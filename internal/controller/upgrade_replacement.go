package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// API-server precondition errors can contain Pod UIDs. Preserve their type for
// retry classification while keeping the controller's automatic error log safe.
type mysqlUpgradeDeleteError struct{ cause error }

func (e *mysqlUpgradeDeleteError) Error() string { return "upgrade replica delete request failed" }
func (e *mysqlUpgradeDeleteError) Unwrap() error { return e.cause }

// Dynamic user intent is an interlock, never an intrinsic status validity rule.
// Let the existing runtime persist and finish replica/HA transitions first.
func mysqlUpgradeReplacementAllowed(cluster *databasev1.MysqlCluster) bool {
	return cluster.Status.Upgrade != nil && (cluster.Status.Upgrade.Stage == databasev1.MysqlClusterUpgradeStageTemplateReady || cluster.Status.Upgrade.Stage == databasev1.MysqlClusterUpgradeStagePrimaryReady) &&
		cluster.Spec.Image == cluster.Status.Upgrade.TargetImage && mysqlUpgradeHAHealthy(cluster) &&
		cluster.Status.ReplicaTransition == nil && cluster.Status.LastConvergedReplicas != nil &&
		*cluster.Status.LastConvergedReplicas == desiredReplicas(cluster)
}

func validateMysqlReplacementMembership(cluster *databasev1.MysqlCluster) error {
	replacement := cluster.Status.Upgrade.Replacement
	if cluster.Status.Upgrade.Stage == databasev1.MysqlClusterUpgradeStagePrimaryReady && replacement != nil {
		h := cluster.Status.Upgrade.Handoff
		if h == nil || replacement.PodName != h.OldPrimary || replacement.OldPodUID != h.OldPrimaryUID {
			return fmt.Errorf("PrimaryReady replacement must bind the durable former primary")
		}
	}
	if replacement != nil && (replacement.Ordinal > desiredReplicas(cluster) || replacement.PodName != mysqlStatefulSetPodName(cluster, replacement.Ordinal)) {
		return fmt.Errorf("durable replacement is incompatible with converged membership")
	}
	return nil
}

func (r *MysqlClusterReconciler) recheckMysqlUpgradeSnapshot(ctx context.Context, cluster *databasev1.MysqlCluster) error {
	fresh := &databasev1.MysqlCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
		return err
	}
	if fresh.UID != cluster.UID || fresh.ResourceVersion != cluster.ResourceVersion {
		return fmt.Errorf("upgrade control snapshot changed during observation")
	}
	return nil
}

func (r *MysqlClusterReconciler) observeMysqlUpgradePrimary(ctx context.Context, cluster *databasev1.MysqlCluster) (*corev1.Pod, bool, error) {
	observation, err := r.observeMysqlPrimaryFailure(ctx, cluster)
	if err != nil {
		return nil, false, fmt.Errorf("upgrade primary authority is ambiguous")
	}
	if observation.Classification != mysqlPrimaryHealthy || !mysqlHAIdentityMatches(cluster.Status.HA, observation) {
		return nil, false, nil
	}
	primary, err := r.observeSinglePublishedPrimary(ctx, cluster)
	if err != nil {
		return nil, false, fmt.Errorf("upgrade requires one canonical primary")
	}
	endpoints, err := r.observeMysqlPrimaryRoutingEndpoints(ctx, cluster)
	if err != nil {
		return nil, false, err
	}
	if primary.Name != observation.PrimaryName || string(primary.UID) != observation.PrimaryUID ||
		!primary.DeletionTimestamp.IsZero() || !mysqlStatefulSetPodHealthy(primary) || !mysqlPublishedPrimaryRoutingAvailable(primary, endpoints) {
		return nil, false, nil
	}
	return primary, true, nil
}

func mysqlUpgradeTargetTemplateValid(cluster *databasev1.MysqlCluster, sts *appsv1.StatefulSet) bool {
	image, err := mysqlWorkloadImage(&sts.Spec.Template.Spec)
	return err == nil && image == cluster.Status.Upgrade.TargetImage &&
		sts.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType && sts.Spec.UpdateStrategy.RollingUpdate == nil &&
		mysqlStatefulSetCurrentReplicas(sts) == desiredReplicas(cluster)
}

func (r *MysqlClusterReconciler) persistMysqlReplacement(ctx context.Context, cluster *databasev1.MysqlCluster, replacement *databasev1.MysqlClusterUpgradeReplacementStatus) error {
	primary, safe, err := r.observeMysqlUpgradePrimary(ctx, cluster)
	if err != nil || !safe {
		return fmt.Errorf("upgrade primary authority changed before replacement persistence")
	}
	target := replacement
	if target == nil {
		target = cluster.Status.Upgrade.Replacement
	}
	if target != nil && target.PodName == primary.Name {
		return fmt.Errorf("replacement target is now the current primary")
	}
	if err := r.recheckMysqlUpgradeSnapshot(ctx, cluster); err != nil {
		return err
	}
	upgrade := cluster.Status.Upgrade.DeepCopy()
	upgrade.Replacement = replacement.DeepCopy()
	return r.persistMysqlClusterUpgradeStatus(ctx, cluster, cluster.Status.LastConvergedImage, upgrade)
}

// Pre-runtime barriers do not demand member readiness while waiting for a new
// incarnation. Delete is the sole exception: it needs a fresh full safety proof.
func (r *MysqlClusterReconciler) reconcileMysqlUpgradeReplacementPreRuntime(ctx context.Context, cluster *databasev1.MysqlCluster) (bool, error) {
	if err := validateMysqlClusterUpgradeStatus(&cluster.Status); err != nil {
		return true, err
	}
	if !mysqlUpgradeReplacementAllowed(cluster) {
		return false, nil
	}
	if err := validateMysqlReplacementMembership(cluster); err != nil {
		return true, err
	}
	replacement := cluster.Status.Upgrade.Replacement
	if replacement == nil {
		return false, nil
	}
	sts, _, err := r.observeMysqlUpgradeWorkload(ctx, cluster)
	if apierrors.IsNotFound(err) {
		return false, nil
	} // preserve child recreation
	if err != nil {
		return true, err
	}
	if !mysqlUpgradeTargetTemplateValid(cluster, sts) {
		return false, nil
	}
	primary, safe, err := r.observeMysqlUpgradePrimary(ctx, cluster)
	if err != nil {
		return true, err
	}
	if !safe {
		return false, nil
	} // fresh failure goes to existing HA
	if cluster.Status.Upgrade.Stage == databasev1.MysqlClusterUpgradeStagePrimaryReady {
		h := cluster.Status.Upgrade.Handoff
		if primary.Name != h.Candidate || string(primary.UID) != h.CandidateUID {
			return true, fmt.Errorf("PrimaryReady requires durable candidate authority")
		}
	}
	if replacement.PodName == primary.Name {
		return true, fmt.Errorf("replacement target is now the current primary")
	}
	pod := &corev1.Pod{}
	err = r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: replacement.PodName}, pod)
	missing := apierrors.IsNotFound(err)
	if err != nil && !missing {
		return true, err
	}
	if !missing {
		if err := r.validateMysqlReplacementPod(ctx, cluster, pod, replacement.Ordinal); err != nil {
			return true, err
		}
	}
	switch replacement.Stage {
	case databasev1.MysqlClusterUpgradeReplacementStageDeletePending:
		if missing || string(pod.UID) != replacement.OldPodUID || !pod.DeletionTimestamp.IsZero() {
			next := replacement.DeepCopy()
			next.Stage = databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement
			return true, r.persistMysqlReplacement(ctx, cluster, next)
		}
		image, err := mysqlWorkloadImage(&pod.Spec)
		if err != nil || image != cluster.Status.Upgrade.FromImage {
			return true, fmt.Errorf("delete target does not have the durable source image")
		}
		proof, safe, err := r.proveMysqlUpgradeReplicas(ctx, cluster)
		if err != nil {
			return true, err
		}
		if !safe {
			return false, nil
		}
		selected := mysqlUpgradeMemberByName(proof.members, replacement.PodName)
		if selected == nil || string(selected.Pod.UID) != replacement.OldPodUID || selected.Ordinal != replacement.Ordinal || selected.Pod.Name == proof.primary.Name {
			return true, fmt.Errorf("delete target identity changed during proof")
		}
		selectedImage, _ := mysqlWorkloadImage(&selected.Pod.Spec)
		if selectedImage != cluster.Status.Upgrade.FromImage {
			return true, fmt.Errorf("delete target source image changed during proof")
		}
		if err := r.recheckMysqlUpgradeProof(ctx, cluster, proof); err != nil {
			return true, err
		}
		uid := types.UID(replacement.OldPodUID)
		// This request, not merely a preceding GET, binds delete authority to
		// the exact durably recorded old Pod. A same-name replacement is safe.
		if err := r.Delete(ctx, selected.Pod, &client.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil {
			return true, &mysqlUpgradeDeleteError{cause: err}
		}
		return true, nil // Never append a Waiting status patch to DELETE.
	case databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement:
		if missing || !pod.DeletionTimestamp.IsZero() {
			return false, nil
		}
		if string(pod.UID) == replacement.OldPodUID {
			return true, fmt.Errorf("old replacement identity unexpectedly remains non-terminating")
		}
		image, err := mysqlWorkloadImage(&pod.Spec)
		if err != nil || image != cluster.Status.Upgrade.TargetImage {
			return true, fmt.Errorf("replacement requested image is not the durable target")
		}
		next := replacement.DeepCopy()
		next.NewPodUID = string(pod.UID)
		next.Stage = databasev1.MysqlClusterUpgradeReplacementStageVerifying
		return true, r.persistMysqlReplacement(ctx, cluster, next)
	case databasev1.MysqlClusterUpgradeReplacementStageVerifying:
		if missing {
			return false, nil
		}
		if string(pod.UID) != replacement.NewPodUID {
			return true, fmt.Errorf("verification target no longer has the durable new identity")
		}
		image, err := mysqlWorkloadImage(&pod.Spec)
		if err != nil || image != cluster.Status.Upgrade.TargetImage {
			return true, fmt.Errorf("verification target requested image changed")
		}
		return false, nil // only the existing runtime executes replication
	default:
		return true, fmt.Errorf("unknown durable replacement stage")
	}
}

func (r *MysqlClusterReconciler) validateMysqlReplacementPod(ctx context.Context, cluster *databasev1.MysqlCluster, pod *corev1.Pod, ordinal int32) error {
	if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
		return fmt.Errorf("replacement Pod ownership is invalid")
	}
	actual, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil || actual != ordinal || pod.Name != mysqlStatefulSetPodName(cluster, ordinal) || pod.UID == "" {
		return fmt.Errorf("replacement Pod canonical identity is invalid")
	}
	return nil
}

type mysqlUpgradeReplicaProof struct {
	primary        *corev1.Pod
	members        []mysqlStatefulSetMember
	statefulSetUID types.UID
}

func mysqlUpgradeMemberByName(members []mysqlStatefulSetMember, name string) *mysqlStatefulSetMember {
	for i := range members {
		if members[i].Pod.Name == name {
			return &members[i]
		}
	}
	return nil
}

// Strong proof extends, rather than redefines, the existing channel semantics.
// Server UUID/GTID data is transient and never passed to status or observability.
func (r *MysqlClusterReconciler) proveMysqlUpgradeReplicas(ctx context.Context, cluster *databasev1.MysqlCluster) (*mysqlUpgradeReplicaProof, bool, error) {
	if !mysqlUpgradeReplacementAllowed(cluster) {
		return nil, false, nil
	}
	sts, members, err := r.observeMysqlUpgradeWorkload(ctx, cluster)
	if err != nil {
		return nil, false, err
	}
	if !mysqlUpgradeTargetTemplateValid(cluster, sts) || !mysqlReplicaTransitionFullyConverged(members, desiredReplicas(cluster)) {
		return nil, false, nil
	}
	primary, safe, err := r.observeMysqlUpgradePrimary(ctx, cluster)
	if err != nil || !safe {
		return nil, false, err
	}
	reference, err := r.observeMysqlElectionReference(ctx, primary, cluster)
	if err != nil {
		return nil, false, fmt.Errorf("upgrade primary source identity observation failed")
	}
	for _, member := range members {
		if member.Pod.UID == "" || !member.Pod.DeletionTimestamp.IsZero() {
			return nil, false, nil
		}
		image, err := mysqlWorkloadImage(&member.Pod.Spec)
		if err != nil || (image != cluster.Status.Upgrade.FromImage && image != cluster.Status.Upgrade.TargetImage) {
			return nil, false, fmt.Errorf("upgrade inventory contains an unexpected or malformed workload image")
		}
		if member.Pod.Name == primary.Name {
			continue
		}
		replication, err := r.observeMysqlMemberReplication(ctx, member.Pod, cluster)
		if err != nil {
			// A member may need the existing runtime to recover its channel.
			// Missing proof forbids upgrade mutation, not replication execution.
			return nil, false, nil
		}
		if replication.PublishedRole != mysqlPublishedRoleSlave || !replication.Channel.semanticallyHealthy(cluster.Spec.MasterService) {
			return nil, false, nil
		}
		if replication.Channel.MasterUUID == "" || replication.Channel.MasterUUID != reference.ServerUUID {
			return nil, false, fmt.Errorf("upgrade replica lacks healthy replication with verified primary source identity")
		}
	}
	if cluster.Status.Upgrade.Stage == databasev1.MysqlClusterUpgradeStagePrimaryReady {
		if _, err := r.proveMysqlPrimaryReady(ctx, cluster, false); err != nil {
			return nil, false, err
		}
	}
	return &mysqlUpgradeReplicaProof{primary: primary, members: members, statefulSetUID: sts.UID}, true, nil
}

// A second identity/authority pass closes the observation window before status
// persistence or delete. DELETE additionally has the server-side UID guard.
func (r *MysqlClusterReconciler) recheckMysqlUpgradeProof(ctx context.Context, cluster *databasev1.MysqlCluster, proof *mysqlUpgradeReplicaProof) error {
	if err := r.recheckMysqlUpgradeSnapshot(ctx, cluster); err != nil {
		return err
	}
	sts, members, err := r.observeMysqlUpgradeWorkload(ctx, cluster)
	if err != nil {
		return err
	}
	if sts.UID != proof.statefulSetUID || !mysqlUpgradeTargetTemplateValid(cluster, sts) || len(members) != len(proof.members) {
		return fmt.Errorf("upgrade workload changed during verification")
	}
	primary, safe, err := r.observeMysqlUpgradePrimary(ctx, cluster)
	if err != nil || !safe || primary.UID != proof.primary.UID || primary.Name != proof.primary.Name {
		return fmt.Errorf("upgrade primary authority changed during verification")
	}
	for i, member := range members {
		before := proof.members[i]
		image, imageErr := mysqlWorkloadImage(&member.Pod.Spec)
		oldImage, _ := mysqlWorkloadImage(&before.Pod.Spec)
		role, roleErr := observeMysqlPublishedRole(member.Pod)
		oldRole, _ := observeMysqlPublishedRole(before.Pod)
		if member.Ordinal != before.Ordinal || member.Pod.Name != before.Pod.Name || member.Pod.UID != before.Pod.UID ||
			!member.Pod.DeletionTimestamp.IsZero() || !mysqlStatefulSetPodHealthy(member.Pod) || imageErr != nil || image != oldImage || roleErr != nil || role != oldRole {
			return fmt.Errorf("upgrade member identity or safety changed during verification")
		}
	}
	return r.recheckMysqlUpgradeSnapshot(ctx, cluster)
}

// Called only after the existing runtime completes without a durable status
// write. Selection, verification clear, and ReplicasVerified are separate writes.
func (r *MysqlClusterReconciler) reconcileMysqlUpgradeReplacementPostRuntime(ctx context.Context, cluster *databasev1.MysqlCluster) error {
	if err := validateMysqlClusterUpgradeStatus(&cluster.Status); err != nil {
		return err
	}
	if !mysqlUpgradeReplacementAllowed(cluster) {
		return nil
	}
	if err := validateMysqlReplacementMembership(cluster); err != nil {
		return err
	}
	replacement := cluster.Status.Upgrade.Replacement
	if replacement != nil && replacement.Stage != databasev1.MysqlClusterUpgradeReplacementStageVerifying {
		return nil
	}
	proof, safe, err := r.proveMysqlUpgradeReplicas(ctx, cluster)
	if err != nil || !safe {
		return err
	}
	if replacement != nil {
		member := mysqlUpgradeMemberByName(proof.members, replacement.PodName)
		if member == nil || member.Pod.Name == proof.primary.Name || string(member.Pod.UID) != replacement.NewPodUID || string(member.Pod.UID) == replacement.OldPodUID {
			return fmt.Errorf("replacement verification does not match the durable non-primary identity")
		}
		image, _ := mysqlWorkloadImage(&member.Pod.Spec)
		if image != cluster.Status.Upgrade.TargetImage {
			return fmt.Errorf("replacement verification requires target image")
		}
		if err := r.recheckMysqlUpgradeProof(ctx, cluster, proof); err != nil {
			return err
		}
		return r.persistMysqlReplacement(ctx, cluster, nil)
	}
	var selected *mysqlStatefulSetMember
	for i := range proof.members {
		member := &proof.members[i]
		if member.Pod.Name == proof.primary.Name {
			continue
		}
		image, _ := mysqlWorkloadImage(&member.Pod.Spec)
		if image == cluster.Status.Upgrade.FromImage {
			selected = member
			break
		}
	}
	if err := r.recheckMysqlUpgradeProof(ctx, cluster, proof); err != nil {
		return err
	}
	if selected != nil {
		return r.persistMysqlReplacement(ctx, cluster, &databasev1.MysqlClusterUpgradeReplacementStatus{
			Ordinal: selected.Ordinal, PodName: selected.Pod.Name, OldPodUID: string(selected.Pod.UID), Stage: databasev1.MysqlClusterUpgradeReplacementStageDeletePending,
		})
	}
	if cluster.Status.Upgrade.Stage == databasev1.MysqlClusterUpgradeStagePrimaryReady {
		return r.completeMysqlUpgrade(ctx, cluster)
	}
	upgrade := cluster.Status.Upgrade.DeepCopy()
	upgrade.Stage = databasev1.MysqlClusterUpgradeStageReplicasVerified
	return r.persistMysqlClusterUpgradeStatus(ctx, cluster, cluster.Status.LastConvergedImage, upgrade)
}

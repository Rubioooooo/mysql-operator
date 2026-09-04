package controller

import (
	"context"
	"fmt"
	"strings"

	databasev1 "github.com/egonlin/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func mysqlStatefulSetEffectiveImage(cluster *databasev1.MysqlCluster) (string, error) {
	if err := validateMysqlClusterUpgradeStatus(&cluster.Status); err != nil {
		return "", err
	}
	if upgrade := cluster.Status.Upgrade; upgrade != nil {
		if upgrade.Stage == databasev1.MysqlClusterUpgradeStageTemplateReady || upgrade.Stage == databasev1.MysqlClusterUpgradeStageReplicasVerified {
			return upgrade.TargetImage, nil
		}
		return upgrade.FromImage, nil
	}
	return cluster.Spec.Image, nil
}

func (r *MysqlClusterReconciler) persistMysqlClusterUpgradeStatus(ctx context.Context, cluster *databasev1.MysqlCluster, checkpoint string, upgrade *databasev1.MysqlClusterUpgradeStatus) error {
	updated := cluster.DeepCopy()
	updated.Status.LastConvergedImage = checkpoint
	updated.Status.Upgrade = upgrade.DeepCopy()
	if err := validateMysqlClusterUpgradeStatus(&updated.Status); err != nil {
		return err
	}
	// Do not expose an in-memory transition (or an Event) on a failed write.
	if err := r.Status().Patch(ctx, updated, client.MergeFromWithOptions(cluster, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("failed to persist upgrade status: %w", err)
	}
	r.emitMysqlUpgradeTransition(ctx, updated, cluster.Status.Upgrade, updated.Status.Upgrade)
	*cluster = *updated
	return nil
}

// This wrapper retains the existing runtime as the sole HA/replication executor.
func (r *MysqlClusterReconciler) reconcileMysqlUpgradeRuntime(ctx context.Context, cluster *databasev1.MysqlCluster) (ctrl.Result, bool, error) {
	handled, err := r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
	if handled || err != nil {
		return ctrl.Result{RequeueAfter: mysqlReplicationRuntimeRequeueAfter}, false, err
	}
	before := cluster.DeepCopy()
	result, complete, err := r.reconcileStatefulSetRuntime(ctx, cluster)
	if err != nil || !complete {
		return result, complete, err
	}
	if before.Status.Upgrade == nil {
		return result, complete, nil
	}
	retargeted := before.Spec.Image != before.Status.Upgrade.TargetImage
	if !retargeted && before.Status.Upgrade.Stage != databasev1.MysqlClusterUpgradeStagePreparing && before.Status.Upgrade.Stage != databasev1.MysqlClusterUpgradeStageTemplateReady {
		return result, complete, nil
	}
	// The existing runtime can persist HA through a value copy. A successful
	// runtime return must not append another durable transition to that write.
	fresh := &databasev1.MysqlCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
		return result, false, err
	}
	if fresh.ResourceVersion != before.ResourceVersion {
		return ctrl.Result{Requeue: true}, false, nil
	}
	if !retargeted && fresh.Status.Upgrade.Stage == databasev1.MysqlClusterUpgradeStageTemplateReady {
		err := r.reconcileMysqlUpgradeReplacementPostRuntime(ctx, fresh)
		return ctrl.Result{RequeueAfter: mysqlReplicationRuntimeRequeueAfter}, false, err
	}
	_, safe, err := r.observeMysqlUpgradeSafety(ctx, fresh)
	if err != nil || !safe {
		return ctrl.Result{RequeueAfter: mysqlReplicationRuntimeRequeueAfter}, false, err
	}
	// Retarget freezes upgrade progression, not ordinary lifecycle recovery.
	// Report it only after the existing runtime and fresh safety observations
	// establish that no higher-priority convergence/recovery work remains.
	if fresh.Spec.Image != fresh.Status.Upgrade.TargetImage {
		return result, false, fmt.Errorf("upgrade target changed; original durable plan is retained")
	}
	upgrade := fresh.Status.Upgrade.DeepCopy()
	upgrade.Stage = databasev1.MysqlClusterUpgradeStageTemplatePending
	err = r.persistMysqlClusterUpgradeStatus(ctx, fresh, fresh.Status.LastConvergedImage, upgrade)
	return ctrl.Result{Requeue: true}, false, err
}

func (r *MysqlClusterReconciler) reconcileMysqlUpgradePreRuntime(ctx context.Context, cluster *databasev1.MysqlCluster) (bool, error) {
	if err := validateMysqlClusterUpgradeStatus(&cluster.Status); err != nil {
		return true, err
	}
	if cluster.Status.LastConvergedImage == "" {
		image, err := r.observeMysqlConvergedImage(ctx, cluster)
		if err != nil {
			return true, err
		}
		return true, r.persistMysqlClusterUpgradeStatus(ctx, cluster, image, nil)
	}
	upgrade := cluster.Status.Upgrade
	if upgrade == nil {
		if cluster.Spec.Image == cluster.Status.LastConvergedImage {
			return false, nil
		}
		return true, r.persistMysqlClusterUpgradeStatus(ctx, cluster, cluster.Status.LastConvergedImage, &databasev1.MysqlClusterUpgradeStatus{
			FromImage: cluster.Status.LastConvergedImage, TargetImage: cluster.Spec.Image, Stage: databasev1.MysqlClusterUpgradeStagePreparing,
		})
	}
	if cluster.Spec.Image != upgrade.TargetImage {
		// The durable effective image protects ordinary StatefulSet writes.
		// Always let runtime perform self-healing, replica transitions and HA
		// before diagnosing retarget in post-runtime control. Never enter the
		// upgrade-specific target-template mutation below for a retargeted plan.
		return false, nil
	}
	if upgrade.Stage == databasev1.MysqlClusterUpgradeStageTemplateReady && upgrade.Replacement != nil {
		return r.reconcileMysqlUpgradeReplacementPreRuntime(ctx, cluster)
	}
	if upgrade.Stage != databasev1.MysqlClusterUpgradeStageTemplatePending {
		return false, nil
	}
	statefulSet, safe, err := r.observeMysqlUpgradeSafety(ctx, cluster)
	if err != nil || !safe {
		// Unsafe observations never grant permission to update the target.
		// Existing membership validation and HA recovery remain reachable.
		return false, nil
	}
	image, imageErr := mysqlWorkloadImage(&statefulSet.Spec.Template.Spec)
	if imageErr == nil && image == upgrade.TargetImage && statefulSet.Spec.UpdateStrategy.Type == appsv1.OnDeleteStatefulSetStrategyType && statefulSet.Spec.UpdateStrategy.RollingUpdate == nil {
		next := upgrade.DeepCopy()
		next.Stage = databasev1.MysqlClusterUpgradeStageTemplateReady
		return true, r.persistMysqlClusterUpgradeStatus(ctx, cluster, cluster.Status.LastConvergedImage, next)
	}
	// Update the observed object using its resourceVersion. Do not recreate a
	// missing StatefulSet, change replicas, or mutate any member in this path.
	desired := desiredMysqlStatefulSetWithImage(cluster, upgrade.TargetImage)
	statefulSet.Spec.Template = desired.Spec.Template
	statefulSet.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
	if err := r.Update(ctx, statefulSet); err != nil {
		return true, fmt.Errorf("failed to update upgrade target template: %w", err)
	}
	return true, nil
}

func mysqlUpgradeHAHealthy(cluster *databasev1.MysqlCluster) bool {
	ha := cluster.Status.HA
	return ha != nil && ha.State == databasev1.MysqlClusterHAStateHealthy && ha.Failover == nil
}

func (r *MysqlClusterReconciler) observeMysqlUpgradeWorkload(ctx context.Context, cluster *databasev1.MysqlCluster) (*appsv1.StatefulSet, []mysqlStatefulSetMember, error) {
	statefulSet := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetName(cluster)}, statefulSet); err != nil {
		return nil, nil, fmt.Errorf("upgrade requires an observed controlled StatefulSet: %w", err)
	}
	if err := validateControlledBy(statefulSet, cluster, "StatefulSet"); err != nil {
		return nil, nil, err
	}
	if statefulSet.UID == "" || !statefulSet.DeletionTimestamp.IsZero() {
		return nil, nil, fmt.Errorf("upgrade StatefulSet identity is not usable")
	}
	if err := validateMysqlStatefulSetImmutableFoundation(statefulSet, desiredMysqlStatefulSet(cluster)); err != nil {
		return nil, nil, err
	}
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	return statefulSet, members, err
}

// Read requested images, never image IDs or spec.image as historical authority.
func mysqlWorkloadImage(spec *corev1.PodSpec) (string, error) {
	image, err := mysqlRequestedImage(spec)
	if err != nil {
		return "", err
	}
	initIndex, initOK := uniqueContainerIndexByName(spec.InitContainers, mysqlConfigInitName)
	if !initOK || spec.InitContainers[initIndex].Image != image {
		return "", fmt.Errorf("workload requires a unique mysql-config-init container matching the mysql image")
	}
	return image, nil
}

func mysqlRequestedImage(spec *corev1.PodSpec) (string, error) {
	index, ok := uniqueContainerIndexByName(spec.Containers, mysqlContainerName)
	if !ok {
		return "", fmt.Errorf("workload requires a unique mysql container")
	}
	image := spec.Containers[index].Image
	if strings.TrimSpace(image) == "" || strings.ContainsAny(image, " \t\r\n") {
		return "", fmt.Errorf("workload mysql requested image is empty or malformed")
	}
	return image, nil
}

func (r *MysqlClusterReconciler) observeMysqlConvergedImage(ctx context.Context, cluster *databasev1.MysqlCluster) (string, error) {
	statefulSet, members, err := r.observeMysqlUpgradeWorkload(ctx, cluster)
	if err != nil {
		return "", err
	}
	image, err := mysqlWorkloadImage(&statefulSet.Spec.Template.Spec)
	if err != nil {
		return "", err
	}
	// 7-A.1: this migrates a compatibility checkpoint, not a health proof.
	// The owned template is sufficient even with zero Pods. Existing members
	// have already passed ownership/canonical ordinal validation; gaps,
	// unready members and terminating members do not invalidate that history.
	for _, member := range members {
		if member.Pod.UID == "" {
			return "", fmt.Errorf("cannot bootstrap image from ambiguous membership")
		}
		memberImage, err := mysqlRequestedImage(&member.Pod.Spec)
		if err != nil || memberImage != image {
			return "", fmt.Errorf("cannot bootstrap image from mixed or malformed workload images")
		}
	}
	return image, nil
}

// Reuse the existing read-only HA and replication observations; never SQL mutation.
func (r *MysqlClusterReconciler) observeMysqlUpgradeSafety(ctx context.Context, cluster *databasev1.MysqlCluster) (*appsv1.StatefulSet, bool, error) {
	if !mysqlUpgradeHAHealthy(cluster) || cluster.Status.ReplicaTransition != nil || cluster.Status.LastConvergedReplicas == nil || *cluster.Status.LastConvergedReplicas != desiredReplicas(cluster) {
		return nil, false, nil
	}
	statefulSet, members, err := r.observeMysqlUpgradeWorkload(ctx, cluster)
	if err != nil {
		return nil, false, err
	}
	if mysqlStatefulSetCurrentReplicas(statefulSet) != desiredReplicas(cluster) || !mysqlReplicaTransitionFullyConverged(members, desiredReplicas(cluster)) {
		return statefulSet, false, nil
	}
	observation, err := r.observeMysqlPrimaryFailure(ctx, cluster)
	if err != nil || observation.Classification != mysqlPrimaryHealthy || !mysqlHAIdentityMatches(cluster.Status.HA, observation) {
		return statefulSet, false, err
	}
	for _, member := range members {
		if member.Pod.UID == "" || !member.Pod.DeletionTimestamp.IsZero() {
			return statefulSet, false, nil
		}
		if member.Pod.Name == observation.PrimaryName {
			continue
		}
		replication, err := r.observeMysqlMemberReplication(ctx, member.Pod, cluster)
		if err != nil || replication.PublishedRole != mysqlPublishedRoleSlave || !replication.Channel.semanticallyHealthy(cluster.Spec.MasterService) {
			return statefulSet, false, err
		}
	}
	return statefulSet, true, nil
}

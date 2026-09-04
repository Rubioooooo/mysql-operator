package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	mysqlConditionAvailable   = "Available"
	mysqlConditionProgressing = "Progressing"
	mysqlConditionDegraded    = "Degraded"
)

// reconcileMysqlObservability runs before lifecycle/control reconciliation. A
// changed projection consumes this reconcile: the caller must return and requeue
// before performing any control work. No status patch is appended to a Phase 5
// mutation, and control-state changes are projected on the next reconciliation.
func (r *MysqlClusterReconciler) reconcileMysqlObservability(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	initialized bool,
) (bool, error) {
	// Preserve the existing legacy-lifecycle diagnostic before member validation.
	if err := r.validateNoLegacyRawPodLifecycle(ctx, cluster); err != nil {
		return false, err
	}
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return false, err
	}
	endpoints, err := r.observeMysqlPrimaryRoutingEndpoints(ctx, cluster)
	if err != nil {
		return false, err
	}
	projected := cluster.DeepCopy()
	projectMysqlObservability(projected, initialized, members, endpoints, metav1.Now())
	if apiequality.Semantic.DeepEqual(cluster.Status, projected.Status) {
		r.Metrics.syncStatus(cluster)
		return false, nil
	}
	// Preserve every unrelated status field and reject stale generation/HA
	// snapshots rather than overwriting a concurrent controller status update.
	if err := r.Status().Patch(ctx, projected, client.MergeFromWithOptions(cluster, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, fmt.Errorf("failed to persist observability status on MysqlCluster %s/%s: %w", cluster.Namespace, cluster.Name, err)
	}
	r.Metrics.syncStatus(projected)
	logMysqlObservabilityProjection(ctx, projected)
	return true, nil
}

// projectMysqlObservability consumes only validated StatefulSet members and
// existing durable state. It neither observes SQL nor changes control state.
func projectMysqlObservability(
	cluster *databasev1.MysqlCluster,
	initialized bool,
	members []mysqlStatefulSetMember,
	endpoints *corev1.Endpoints,
	now metav1.Time,
) {
	status := &cluster.Status
	status.ObservedGeneration = cluster.Generation
	status.CurrentReplicas = int32(len(members))
	status.ReadyReplicas = 0
	status.Primary = ""
	var primary *corev1.Pod
	published := 0
	ambiguousRoles := false
	for _, member := range members {
		if mysqlStatefulSetPodHealthy(member.Pod) {
			status.ReadyReplicas++
		}
		role, err := observeMysqlPublishedRole(member.Pod)
		if err != nil {
			ambiguousRoles = true
			continue
		}
		if role == mysqlPublishedRoleMaster {
			published++
			primary = member.Pod
		}
	}
	if !ambiguousRoles && published == 1 && mysqlObservablePrimaryUsable(cluster, primary) {
		status.Primary = primary.Name
	}
	available := status.Primary != "" && mysqlPublishedPrimaryRoutingAvailable(primary, endpoints)
	progressing := !initialized || status.ReplicaTransition != nil || status.Upgrade != nil
	progressReason, progressMessage := "Stable", "No topology transition is in progress."
	if !initialized {
		progressReason, progressMessage = "Initializing", "Initial topology initialization is in progress."
	} else if status.ReplicaTransition != nil {
		progressReason, progressMessage = "ReplicaTransition", "Replica count convergence is in progress."
	}
	degraded := initialized && (!available || mysqlObservabilityMembersDegraded(members, status.ReplicaTransition) || ambiguousRoles || published > 1)
	degradedReason, degradedMessage := "Healthy", "No degradation is observed."
	if status.Upgrade != nil {
		progressReason, progressMessage = "UpgradeInProgress", "A durable image upgrade is in progress."
	}
	if degraded {
		degradedReason, degradedMessage = "MemberUnavailable", "A primary or member is unavailable or has ambiguous role publication."
	}
	if initialized && status.Primary != "" && !available {
		degradedReason, degradedMessage = "PrimaryRoutingUnavailable", "The published primary has no usable routing endpoint."
	}
	if initialized && status.ReplicaTransition == nil && status.CurrentReplicas != desiredReplicas(cluster) {
		progressing = true
		progressReason, progressMessage = "ReplicaConvergence", "Observed membership has not reached the desired replica count."
		// A new desired count is normal progress. A missing member of the
		// already converged desired topology is an observed abnormality.
		if status.LastConvergedReplicas != nil && *status.LastConvergedReplicas == desiredReplicas(cluster) {
			degraded = true
			degradedReason, degradedMessage = "MemberUnavailable", "Observed membership differs from the converged topology."
		}
	}
	if status.Upgrade != nil && cluster.Spec.Image != status.Upgrade.TargetImage {
		degraded = true
		degradedReason, degradedMessage = "UpgradeTargetChanged", "The requested image differs from the active durable upgrade target."
	}
	if ha := status.HA; ha != nil {
		switch ha.State {
		case databasev1.MysqlClusterHAStateSuspected,
			databasev1.MysqlClusterHAStateFailoverRequired,
			databasev1.MysqlClusterHAStateFailoverInProgress,
			databasev1.MysqlClusterHAStateVerifying:
			progressing = true
			progressReason, progressMessage = "HAProgressing", "HA recovery or verification is in progress."
			degraded = true
			degradedReason, degradedMessage = "HAAbnormal", "HA health has not yet been verified as healthy."
		case databasev1.MysqlClusterHAStateDegraded:
			degraded = true
			degradedReason, degradedMessage = "HADegraded", "The authoritative HA state reports degradation."
		}
		if ha.Failover != nil {
			progressing, degraded = true, true
			progressReason, progressMessage = "FailoverProgressing", "Durable failover recovery is in progress."
			degradedReason, degradedMessage = "HAAbnormal", "Failover recovery has not completed."
		}
	}

	switch {
	case !initialized:
		status.Phase = databasev1.MysqlClusterPhaseInitializing
	case degraded:
		status.Phase = databasev1.MysqlClusterPhaseDegraded
	default:
		status.Phase = databasev1.MysqlClusterPhaseRunning
	}
	availableReason, availableMessage := "PrimaryUnavailable", "No single safely published usable primary is observed."
	if available {
		availableReason, availableMessage = "PrimaryAvailable", "A safely published usable primary is exposed by its routing endpoint."
	} else if status.Primary != "" {
		availableReason, availableMessage = "PrimaryRoutingUnavailable", "The published primary has no usable routing endpoint."
	}
	setMysqlObservabilityCondition(status, mysqlConditionAvailable, available, availableReason, availableMessage, now)
	setMysqlObservabilityCondition(status, mysqlConditionProgressing, progressing, progressReason, progressMessage, now)
	setMysqlObservabilityCondition(status, mysqlConditionDegraded, degraded, degradedReason, degradedMessage, now)
}

// Match Phase 4's read semantics: missing Endpoints means unavailable; other
// API failures remain errors. Endpoints never establish member identity/counts.
func (r *MysqlClusterReconciler) observeMysqlPrimaryRoutingEndpoints(ctx context.Context, cluster *databasev1.MysqlCluster) (*corev1.Endpoints, error) {
	endpoints := &corev1.Endpoints{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.MasterService}
	if err := r.Get(ctx, key, endpoints); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to observe primary Endpoints %s: %w", key, err)
	}
	return endpoints, nil
}

func mysqlPublishedPrimaryRoutingAvailable(primary *corev1.Pod, endpoints *corev1.Endpoints) bool {
	if endpoints == nil || !mysqlMasterEndpointAvailable(endpoints) {
		return false
	}
	// Phase 4 uses ready Addresses, not NotReadyAddresses. Bind that routing
	// evidence to the already validated published Pod so a stale old-primary
	// address cannot make a newly published takeover candidate Available.
	for _, subset := range endpoints.Subsets {
		for _, address := range subset.Addresses {
			ref := address.TargetRef
			if ref != nil && ref.Name == primary.Name &&
				(ref.Namespace == "" || ref.Namespace == primary.Namespace) &&
				(ref.Kind == "" || ref.Kind == "Pod") &&
				(ref.UID == "" || ref.UID == primary.UID) {
				return true
			}
		}
	}
	return false
}

func mysqlObservabilityMembersDegraded(members []mysqlStatefulSetMember, transition *databasev1.MysqlClusterReplicaTransitionStatus) bool {
	var stableCoreSize, stableCorePresent int32
	if transition != nil {
		stableCoreSize = min(transition.FromReplicas, transition.TargetReplicas)
	}
	for _, member := range members {
		if transition != nil && member.Ordinal > stableCoreSize {
			// Expected added/removed ordinals may be absent, unready or
			// terminating while the existing transition barrier converges.
			continue
		}
		stableCorePresent++
		if !mysqlStatefulSetPodHealthy(member.Pod) || !member.Pod.DeletionTimestamp.IsZero() {
			return true
		}
	}
	// Validated member ordinals are unique: a short count means a missing
	// member in the common ordinal set, even when the delta is present.
	return transition != nil && stableCorePresent < stableCoreSize
}

func mysqlObservablePrimaryUsable(cluster *databasev1.MysqlCluster, pod *corev1.Pod) bool {
	if pod.UID == "" || !pod.DeletionTimestamp.IsZero() || !mysqlStatefulSetPodHealthy(pod) {
		return false
	}
	if ha := cluster.Status.HA; ha != nil {
		if failover := ha.Failover; failover != nil {
			// Fencing can leave the old primary's role published temporarily.
			// Only an actually published takeover candidate can be exposed;
			// the candidate identity alone is never publication evidence.
			return (failover.Stage == databasev1.MysqlClusterFailoverStagePromoting ||
				failover.Stage == databasev1.MysqlClusterFailoverStageReconfiguring) &&
				pod.Name == failover.Candidate && string(pod.UID) == failover.CandidateUID
		}
		if ha.Primary == pod.Name && ha.PrimaryUID != "" && ha.PrimaryUID != string(pod.UID) {
			return false
		}
	}
	return true
}

func setMysqlObservabilityCondition(status *databasev1.MysqlClusterStatus, conditionType string, value bool, reason, message string, now metav1.Time) {
	conditionStatus := metav1.ConditionFalse
	if value {
		conditionStatus = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type: conditionType, Status: conditionStatus, ObservedGeneration: status.ObservedGeneration,
		Reason: reason, Message: message, LastTransitionTime: now,
	})
}

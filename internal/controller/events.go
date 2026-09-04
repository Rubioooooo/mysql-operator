package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

// EventRecorder delivers best-effort notifications and exposes no delivery
// error to reconciliation. Events are never a persistence or control barrier.
func (r *MysqlClusterReconciler) recordMysqlEvent(cluster *databasev1.MysqlCluster, eventType, reason, message string) {
	if r.Recorder != nil && reason != "" {
		r.Recorder.Event(cluster, eventType, reason, message)
	}
}

// Upgrade Events describe successfully persisted milestones only.
func (r *MysqlClusterReconciler) emitMysqlUpgradeTransition(ctx context.Context, cluster *databasev1.MysqlCluster, before, after *databasev1.MysqlClusterUpgradeStatus) {
	if after == nil || apiequality.Semantic.DeepEqual(before, after) {
		return
	}
	if before == nil && after.Stage == databasev1.MysqlClusterUpgradeStagePreparing {
		r.recordMysqlEvent(cluster, corev1.EventTypeNormal, "UpgradeStarted", "Durable image upgrade intent has been recorded.")
	} else if before != nil && before.Stage == databasev1.MysqlClusterUpgradeStageTemplatePending && after.Stage == databasev1.MysqlClusterUpgradeStageTemplateReady {
		r.recordMysqlEvent(cluster, corev1.EventTypeNormal, "UpgradeTemplateReady", "The target image template has been observed with OnDelete strategy.")
	}
	logMysqlUpgradeTransition(ctx, after)
	reason := mysqlUpgradeReplacementEvent(before, after)
	if reason != "" {
		r.recordMysqlEvent(cluster, corev1.EventTypeNormal, reason, "Durable replica upgrade barrier has been persisted.")
	}
	if after.Replacement != nil {
		logMysqlUpgradeReplacement(ctx, after.Replacement)
	} else if before != nil && before.Replacement != nil {
		logMysqlUpgradeReplacement(ctx, before.Replacement)
	}
}

func mysqlUpgradeReplacementEvent(before, after *databasev1.MysqlClusterUpgradeStatus) string {
	if before == nil || after == nil {
		return ""
	}
	old, next := before.Replacement, after.Replacement
	switch {
	case before.Stage != databasev1.MysqlClusterUpgradeStageReplicasVerified && after.Stage == databasev1.MysqlClusterUpgradeStageReplicasVerified:
		return "UpgradeReplicasVerified"
	case old == nil && next != nil && next.Stage == databasev1.MysqlClusterUpgradeReplacementStageDeletePending:
		return "UpgradeReplicaSelected"
	case old != nil && next != nil && old.Stage == databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement && next.Stage == databasev1.MysqlClusterUpgradeReplacementStageVerifying:
		return "UpgradeReplicaObserved"
	case old != nil && old.Stage == databasev1.MysqlClusterUpgradeReplacementStageVerifying && next == nil && after.Stage == databasev1.MysqlClusterUpgradeStageTemplateReady:
		return "UpgradeReplicaVerified"
	}
	return ""
}

// Called only after the HA status patch succeeds. Classify once so overlapping
// milestones cannot emit multiple events for a single durable status write.
func (r *MysqlClusterReconciler) emitMysqlHAStatusTransitionEvent(ctx context.Context, cluster *databasev1.MysqlCluster, before, after *databasev1.MysqlClusterHAStatus) {
	eventType, reason, message := mysqlHAStatusTransitionEvent(before, after)
	r.Metrics.incrementHA(cluster.Namespace, cluster.Name, reason)
	r.recordMysqlEvent(cluster, eventType, reason, message)
	logMysqlHAStatusTransition(ctx, reason, after)
}

func mysqlHAStatusTransitionEvent(before, after *databasev1.MysqlClusterHAStatus) (string, string, string) {
	if after == nil {
		return "", "", ""
	}
	var beforeState databasev1.MysqlClusterHAState
	var oldFailover *databasev1.MysqlClusterFailoverStatus
	if before != nil {
		beforeState, oldFailover = before.State, before.Failover
	}
	var oldStage databasev1.MysqlClusterFailoverStage
	var oldFence databasev1.MysqlClusterFenceState
	if oldFailover != nil {
		oldStage, oldFence = oldFailover.Stage, oldFailover.FenceState
	}
	switch {
	case beforeState != databasev1.MysqlClusterHAStateFailoverRequired && after.State == databasev1.MysqlClusterHAStateFailoverRequired:
		return corev1.EventTypeWarning, "FailoverRequired", fmt.Sprintf("Failover is required for primary %s.", after.Primary)
	case before != nil && beforeState != databasev1.MysqlClusterHAStateHealthy && after.State == databasev1.MysqlClusterHAStateHealthy:
		return corev1.EventTypeNormal, "HARecovered", "HA health has recovered."
	case after.State == databasev1.MysqlClusterHAStateVerifying && after.Failover == nil:
		switch oldStage {
		case databasev1.MysqlClusterFailoverStageReconfiguring:
			return corev1.EventTypeNormal, "FailoverVerifying", "Failover replica reconfiguration has finished; verifying HA health."
		case databasev1.MysqlClusterFailoverStageFencing:
			return corev1.EventTypeNormal, "FailoverAborted", "Failover was aborted after primary recovery before fencing."
		}
	}
	failover := after.Failover
	if failover == nil {
		return "", "", ""
	}
	sameAttempt := oldFailover != nil && oldFailover.FailedPrimary == failover.FailedPrimary && oldFailover.FailedPrimaryUID == failover.FailedPrimaryUID
	// Loss/blockage and the no-safe-candidate result take precedence over
	// their incidental proof clearing or stage changes. In particular a lost
	// fence is not also a newly started failover or candidate invalidation.
	switch {
	case oldFence == databasev1.MysqlClusterFenceStateVerified && failover.FenceState == databasev1.MysqlClusterFenceStatePending:
		return corev1.EventTypeWarning, "PrimaryFenceLost", fmt.Sprintf("Previously verified fencing for primary %s was lost.", failover.FailedPrimary)
	case failover.FenceState == databasev1.MysqlClusterFenceStateBlocked && (!sameAttempt || oldFence != databasev1.MysqlClusterFenceStateBlocked):
		return corev1.EventTypeWarning, "FailoverBlocked", "Failover is blocked pending safe fencing."
	case mysqlHANoSafeCandidateEventState(after) && (!sameAttempt || !mysqlHANoSafeCandidateEventState(before)):
		return corev1.EventTypeWarning, "NoSafeCandidate", "No safe failover candidate is available."
	case oldStage == databasev1.MysqlClusterFailoverStageCandidateSelected && failover.Stage == databasev1.MysqlClusterFailoverStageFencing &&
		failover.Candidate == "" && failover.CandidateUID == "" && failover.FailedPrimaryServerUUID == "" && failover.FailedPrimaryGTIDSet == nil:
		return corev1.EventTypeWarning, "CandidateInvalidated", fmt.Sprintf("Failover candidate %s is no longer valid; returning to fencing.", oldFailover.Candidate)
	case oldStage != databasev1.MysqlClusterFailoverStageCandidateSelected && failover.Stage == databasev1.MysqlClusterFailoverStageCandidateSelected:
		return corev1.EventTypeNormal, "CandidateSelected", fmt.Sprintf("Selected %s as the failover candidate.", failover.Candidate)
	case oldStage != databasev1.MysqlClusterFailoverStagePromoting && failover.Stage == databasev1.MysqlClusterFailoverStagePromoting:
		return corev1.EventTypeNormal, "PromotionStarted", fmt.Sprintf("Promotion of failover candidate %s has started.", failover.Candidate)
	case oldStage == databasev1.MysqlClusterFailoverStagePromoting && failover.Stage == databasev1.MysqlClusterFailoverStageReconfiguring &&
		failover.Candidate != "" && failover.CandidateUID != "" && after.Primary == failover.Candidate && after.PrimaryUID == failover.CandidateUID:
		return corev1.EventTypeNormal, "PrimaryPromoted", fmt.Sprintf("Primary %s has been promoted and its publication verified.", after.Primary)
	case oldStage == databasev1.MysqlClusterFailoverStageReconfiguring && failover.Stage == databasev1.MysqlClusterFailoverStageReconfiguring &&
		beforeState != databasev1.MysqlClusterHAStateDegraded && after.State == databasev1.MysqlClusterHAStateDegraded:
		return corev1.EventTypeWarning, "UnsafeReplicaRejoin", "Replica rejoin is blocked by unsafe replication history."
	case oldFence != databasev1.MysqlClusterFenceStateVerified && failover.FenceState == databasev1.MysqlClusterFenceStateVerified &&
		failover.FencedPrimaryUID != "" && failover.FencedPrimaryUID == failover.FailedPrimaryUID:
		return corev1.EventTypeNormal, "PrimaryFenced", fmt.Sprintf("Write fencing for primary %s has been verified.", failover.FailedPrimary)
	case after.State == databasev1.MysqlClusterHAStateFailoverInProgress && failover.Stage == databasev1.MysqlClusterFailoverStageFencing &&
		failover.FenceState == databasev1.MysqlClusterFenceStatePending && !sameAttempt:
		return corev1.EventTypeNormal, "FailoverStarted", fmt.Sprintf("Failover fencing has started for primary %s.", failover.FailedPrimary)
	}
	return "", "", ""
}

func mysqlHANoSafeCandidateEventState(status *databasev1.MysqlClusterHAStatus) bool {
	return status != nil && status.State == databasev1.MysqlClusterHAStateDegraded && status.Failover != nil &&
		status.Failover.Stage == databasev1.MysqlClusterFailoverStageFencing &&
		status.Failover.FenceState == databasev1.MysqlClusterFenceStateVerified && status.Failover.Candidate == ""
}

// Called only after the replica-transition patch succeeds. Compatibility
// checkpoint initialization and repeated writes are deliberately silent.
func (r *MysqlClusterReconciler) emitMysqlReplicaTransitionEvent(ctx context.Context, cluster *databasev1.MysqlCluster, before, after *databasev1.MysqlClusterStatus) {
	reason, message := mysqlReplicaTransitionEvent(before, after)
	r.Metrics.incrementReplica(cluster.Namespace, cluster.Name, reason)
	r.recordMysqlEvent(cluster, corev1.EventTypeNormal, reason, message)
	logMysqlReplicaTransition(ctx, reason, before, after)
}

func mysqlReplicaTransitionEvent(before, after *databasev1.MysqlClusterStatus) (string, string) {
	oldTransition, transition := before.ReplicaTransition, after.ReplicaTransition
	switch {
	case oldTransition == nil && transition != nil:
		return "ReplicaTransitionStarted", fmt.Sprintf("Replica transition started from %d to %d.", transition.FromReplicas, transition.TargetReplicas)
	case oldTransition != nil && transition != nil && oldTransition.TargetReplicas != transition.TargetReplicas:
		return "ReplicaTransitionRetargeted", fmt.Sprintf("Replica transition retargeted from %d to %d replicas.", oldTransition.TargetReplicas, transition.TargetReplicas)
	case oldTransition != nil && transition == nil && after.LastConvergedReplicas != nil && *after.LastConvergedReplicas == oldTransition.TargetReplicas:
		return "ReplicaTransitionCompleted", fmt.Sprintf("Replica transition completed at %d replicas.", *after.LastConvergedReplicas)
	}
	return "", ""
}

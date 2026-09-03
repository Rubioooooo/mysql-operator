package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mysqlTakeoverCandidateObservation struct {
	Pod         *corev1.Pod
	Role        mysqlPublishedRole
	WriteSafety mysqlWriteSafetyObservation
	SourceReady bool
	Replication mysqlReplicationChannelObservation
	GTIDEqual   bool
}

func mysqlStopSlaveCommand() string {
	return mysqlRootClientCommand + ` -e "STOP SLAVE;"`
}

func mysqlSetReadOnlyOffCommand() string {
	return mysqlRootClientCommand + ` -e "SET GLOBAL read_only = OFF;"`
}

func mysqlReplicationStopped(replication mysqlReplicationChannelObservation) bool {
	return replication.Configured &&
		replication.IORunning == "No" &&
		replication.SQLRunning == "No"
}

func mysqlTakeoverBarrierResult() (ctrl.Result, bool, error) {
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

func (r *MysqlClusterReconciler) validateNoPublishedMysqlPrimaryBeforeTakeover(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) error {
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return err
	}
	publishedMasters := 0
	for _, member := range members {
		role, err := observeMysqlPublishedRole(member.Pod)
		if err != nil {
			return err
		}
		if role == mysqlPublishedRoleMaster {
			publishedMasters++
		}
	}
	if publishedMasters != 0 {
		return fmt.Errorf(
			"MySQL takeover requires zero published masters before candidate publication, found %d",
			publishedMasters,
		)
	}
	return nil
}

func validateMysqlTakeoverStatus(cluster *databasev1.MysqlCluster, expectedStage databasev1.MysqlClusterFailoverStage) error {
	if cluster.Status.HA == nil || cluster.Status.HA.Failover == nil {
		return fmt.Errorf("MySQL takeover stage %s requires durable failover status", expectedStage)
	}
	failover := cluster.Status.HA.Failover
	if cluster.Status.HA.State != databasev1.MysqlClusterHAStateFailoverInProgress ||
		failover.Stage != expectedStage ||
		failover.FenceState != databasev1.MysqlClusterFenceStateVerified ||
		failover.FencedPrimaryUID == "" ||
		failover.FencedPrimaryUID != failover.FailedPrimaryUID ||
		failover.FailedPrimary == "" || failover.FailedPrimaryUID == "" ||
		failover.Candidate == "" || failover.CandidateUID == "" ||
		failover.FailedPrimaryServerUUID == "" || failover.FailedPrimaryGTIDSet == nil {
		return fmt.Errorf("invalid durable MySQL takeover status for stage %s", expectedStage)
	}
	return nil
}

func (r *MysqlClusterReconciler) getValidatedMysqlTakeoverCandidate(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (*corev1.Pod, mysqlPublishedRole, error) {
	failover := cluster.Status.HA.Failover
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: failover.Candidate}
	if err := r.Get(ctx, key, pod); err != nil {
		return nil, mysqlPublishedRoleNone, err
	}
	if string(pod.UID) != failover.CandidateUID {
		return nil, mysqlPublishedRoleNone, fmt.Errorf("candidate Pod %s was replaced", key)
	}
	if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
		return nil, mysqlPublishedRoleNone, err
	}
	ordinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil {
		return nil, mysqlPublishedRoleNone, err
	}
	if pod.Name != mysqlStatefulSetPodName(cluster, ordinal) {
		return nil, mysqlPublishedRoleNone, fmt.Errorf("candidate Pod %s has non-canonical ordinal identity", key)
	}
	if !mysqlStatefulSetPodHealthy(pod) {
		return nil, mysqlPublishedRoleNone, fmt.Errorf("candidate Pod %s is not healthy", key)
	}
	role, err := observeMysqlPublishedRole(pod)
	if err != nil {
		return nil, mysqlPublishedRoleNone, err
	}
	return pod, role, nil
}

func (r *MysqlClusterReconciler) observeMysqlTakeoverCandidate(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	requireOldGTIDEquality bool,
) (mysqlTakeoverCandidateObservation, error) {
	pod, role, err := r.getValidatedMysqlTakeoverCandidate(ctx, cluster)
	if err != nil {
		return mysqlTakeoverCandidateObservation{}, err
	}
	observation := mysqlTakeoverCandidateObservation{Pod: pod, Role: role}
	writeSafety, err := r.observeMysqlWriteSafety(ctx, pod, cluster)
	if err != nil {
		return observation, err
	}
	observation.WriteSafety = writeSafety
	sourceReady, err := r.observeMysqlSourceCapability(ctx, pod, cluster)
	if err != nil {
		return observation, err
	}
	observation.SourceReady = sourceReady
	replication, err := r.observeMysqlMemberReplication(ctx, pod, cluster)
	if err != nil {
		return observation, err
	}
	observation.Replication = replication.Channel
	observation.GTIDEqual = !requireOldGTIDEquality
	if requireOldGTIDEquality {
		comparison, err := r.compareMysqlCandidateGTID(ctx, pod, cluster, *cluster.Status.HA.Failover.FailedPrimaryGTIDSet)
		if err != nil {
			return observation, err
		}
		observation.GTIDEqual = comparison.Relation == mysqlGTIDRelationEqual
	}
	return observation, nil
}

func mysqlCandidateOldSourceProofValid(
	cluster *databasev1.MysqlCluster,
	observation mysqlTakeoverCandidateObservation,
) bool {
	failover := cluster.Status.HA.Failover
	return observation.WriteSafety.GTIDReady &&
		observation.SourceReady &&
		observation.Replication.Configured &&
		observation.Replication.MasterHost == cluster.Spec.MasterService &&
		observation.Replication.AutoPosition == "1" &&
		observation.Replication.MasterUUID == failover.FailedPrimaryServerUUID &&
		observation.GTIDEqual
}

func (r *MysqlClusterReconciler) validateMysqlTakeoverFailedPrimary(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) error {
	failover := cluster.Status.HA.Failover
	failedPrimary, reference, err := r.validateMysqlFailedPrimaryElectionReference(ctx, cluster)
	if err != nil {
		return err
	}
	if reference.ServerUUID != failover.FailedPrimaryServerUUID {
		return fmt.Errorf("failed primary server_uuid changed during takeover")
	}
	comparison, err := r.compareMysqlCandidateGTID(ctx, failedPrimary, cluster, *failover.FailedPrimaryGTIDSet)
	if err != nil {
		return err
	}
	if comparison.Relation != mysqlGTIDRelationEqual {
		return fmt.Errorf("failed primary GTID changed during takeover")
	}
	return nil
}

func (r *MysqlClusterReconciler) mutateMysqlTakeoverCandidateRole(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	expectedRole mysqlPublishedRole,
	desiredRole mysqlPublishedRole,
) error {
	pod, role, err := r.getValidatedMysqlTakeoverCandidate(ctx, cluster)
	if err != nil {
		return err
	}
	if role != expectedRole {
		return fmt.Errorf("refusing candidate role mutation for Pod %s/%s with role %q; expected %q", pod.Namespace, pod.Name, role, expectedRole)
	}
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	if desiredRole == mysqlPublishedRoleNone {
		delete(pod.Labels, LabelMysqlRole)
		delete(pod.Labels, LegacyLabelRole)
	} else {
		pod.Labels[LabelMysqlRole] = string(desiredRole)
		pod.Labels[LegacyLabelRole] = string(desiredRole)
	}
	if err := r.Update(ctx, pod); err != nil {
		return fmt.Errorf("failed to update takeover candidate role on Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

func (r *MysqlClusterReconciler) refenceMysqlTakeoverCandidate(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	pod, _, err := r.getValidatedMysqlTakeoverCandidate(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if _, err := r.executeCommandOnPod(pod, mysqlSetSuperReadOnlyCommand()); err != nil {
		return ctrl.Result{}, false, fmt.Errorf("failed to re-fence takeover candidate Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return mysqlTakeoverBarrierResult()
}

func (r *MysqlClusterReconciler) quarantinePublishedTakeoverCandidate(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	if err := r.mutateMysqlTakeoverCandidateRole(ctx, cluster, mysqlPublishedRoleMaster, mysqlPublishedRoleNone); err != nil {
		return ctrl.Result{}, false, err
	}
	return mysqlTakeoverBarrierResult()
}

func (r *MysqlClusterReconciler) reconcileMysqlCandidateTakeover(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	if err := validateMysqlTakeoverStatus(cluster, databasev1.MysqlClusterFailoverStagePromoting); err != nil {
		return ctrl.Result{}, false, err
	}

	_, role, err := r.getValidatedMysqlTakeoverCandidate(ctx, cluster)
	if err != nil {
		return mysqlTakeoverBarrierResult()
	}
	if role == mysqlPublishedRoleMaster {
		return r.reconcileMysqlPublishedTakeoverCandidate(ctx, cluster)
	}

	candidate, err := r.observeMysqlTakeoverCandidate(ctx, cluster, true)
	if err != nil {
		if candidate.Pod != nil &&
			candidate.Role != mysqlPublishedRoleMaster &&
			candidate.WriteSafety.WriteRole == mysqlWriteRoleWritable {
			return r.refenceMysqlTakeoverCandidate(ctx, cluster)
		}
		return mysqlTakeoverBarrierResult()
	}
	if candidate.Role != mysqlPublishedRoleSlave && candidate.Role != mysqlPublishedRoleNone {
		return mysqlTakeoverBarrierResult()
	}
	if err := r.validateNoPublishedMysqlPrimaryBeforeTakeover(ctx, cluster); err != nil {
		if candidate.WriteSafety.WriteRole == mysqlWriteRoleWritable {
			return r.refenceMysqlTakeoverCandidate(ctx, cluster)
		}
		return mysqlTakeoverBarrierResult()
	}

	oldSourceProofValid := mysqlCandidateOldSourceProofValid(cluster, candidate)
	exactlyReadOnly := candidate.WriteSafety.ReadOnly && candidate.WriteSafety.SuperReadOnly
	exactlyWritable := !candidate.WriteSafety.ReadOnly && !candidate.WriteSafety.SuperReadOnly
	if !oldSourceProofValid || (!exactlyReadOnly && !exactlyWritable) {
		if candidate.WriteSafety.WriteRole == mysqlWriteRoleWritable {
			return r.refenceMysqlTakeoverCandidate(ctx, cluster)
		}
		return mysqlTakeoverBarrierResult()
	}

	if err := r.validateMysqlTakeoverFailedPrimary(ctx, cluster); err != nil {
		if candidate.WriteSafety.WriteRole == mysqlWriteRoleWritable {
			return r.refenceMysqlTakeoverCandidate(ctx, cluster)
		}
		return mysqlTakeoverBarrierResult()
	}

	if !mysqlReplicationStopped(candidate.Replication) {
		if !exactlyReadOnly {
			return r.refenceMysqlTakeoverCandidate(ctx, cluster)
		}
		fresh, _, err := r.getValidatedMysqlTakeoverCandidate(ctx, cluster)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		if _, err := r.executeCommandOnPod(fresh, mysqlStopSlaveCommand()); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("failed to stop replication on takeover candidate Pod %s/%s: %w", fresh.Namespace, fresh.Name, err)
		}
		return mysqlTakeoverBarrierResult()
	}

	switch candidate.Role {
	case mysqlPublishedRoleSlave:
		if !exactlyReadOnly {
			return r.refenceMysqlTakeoverCandidate(ctx, cluster)
		}
		if err := r.mutateMysqlTakeoverCandidateRole(ctx, cluster, mysqlPublishedRoleSlave, mysqlPublishedRoleNone); err != nil {
			return ctrl.Result{}, false, err
		}
		return mysqlTakeoverBarrierResult()

	case mysqlPublishedRoleNone:
		if exactlyReadOnly {
			fresh, _, err := r.getValidatedMysqlTakeoverCandidate(ctx, cluster)
			if err != nil {
				return ctrl.Result{}, false, err
			}
			if _, err := r.executeCommandOnPod(fresh, mysqlSetReadOnlyOffCommand()); err != nil {
				return ctrl.Result{}, false, fmt.Errorf("failed to enable writes on takeover candidate Pod %s/%s: %w", fresh.Namespace, fresh.Name, err)
			}
			return mysqlTakeoverBarrierResult()
		}
		if !exactlyWritable {
			return r.refenceMysqlTakeoverCandidate(ctx, cluster)
		}
		if err := r.validateNoPublishedMysqlPrimaryBeforeTakeover(ctx, cluster); err != nil {
			return r.refenceMysqlTakeoverCandidate(ctx, cluster)
		}
		if err := r.mutateMysqlTakeoverCandidateRole(ctx, cluster, mysqlPublishedRoleNone, mysqlPublishedRoleMaster); err != nil {
			return ctrl.Result{}, false, err
		}
		return mysqlTakeoverBarrierResult()
	}

	return mysqlTakeoverBarrierResult()
}

func (r *MysqlClusterReconciler) observeMysqlPublishedTakeoverSafety(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (mysqlTakeoverCandidateObservation, error) {
	candidate, err := r.observeMysqlTakeoverCandidate(ctx, cluster, false)
	if err != nil {
		return candidate, err
	}
	if candidate.Role != mysqlPublishedRoleMaster ||
		candidate.WriteSafety.ReadOnly || candidate.WriteSafety.SuperReadOnly ||
		!candidate.WriteSafety.GTIDReady || !candidate.SourceReady ||
		!mysqlReplicationStopped(candidate.Replication) {
		return candidate, fmt.Errorf("published takeover candidate failed writable-primary proof")
	}
	primary, err := r.observeSinglePublishedPrimary(ctx, cluster)
	if err != nil {
		return candidate, err
	}
	if primary.Name != candidate.Pod.Name || string(primary.UID) != string(candidate.Pod.UID) {
		return candidate, fmt.Errorf("published primary does not match the durable takeover candidate")
	}
	if err := r.validateMysqlTakeoverFailedPrimary(ctx, cluster); err != nil {
		return candidate, err
	}
	return candidate, nil
}

func (r *MysqlClusterReconciler) reconcileMysqlPublishedTakeoverCandidate(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	candidate, err := r.observeMysqlPublishedTakeoverSafety(ctx, cluster)
	if err != nil {
		if candidate.Pod == nil {
			return mysqlTakeoverBarrierResult()
		}
		if candidate.WriteSafety.WriteRole == mysqlWriteRoleWritable {
			return r.refenceMysqlTakeoverCandidate(ctx, cluster)
		}
		if candidate.Role == mysqlPublishedRoleMaster {
			return r.quarantinePublishedTakeoverCandidate(ctx, cluster)
		}
		return mysqlTakeoverBarrierResult()
	}

	desired := cluster.Status.HA.DeepCopy()
	desired.State = databasev1.MysqlClusterHAStateFailoverInProgress
	desired.Primary = desired.Failover.Candidate
	desired.PrimaryUID = desired.Failover.CandidateUID
	desired.Failover.Stage = databasev1.MysqlClusterFailoverStageReconfiguring
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, desired); err != nil {
		return ctrl.Result{}, false, err
	}
	return mysqlTakeoverBarrierResult()
}

func (r *MysqlClusterReconciler) reconcileMysqlReconfiguringBarrier(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	return r.reconcileMysqlReconfiguringReplicas(ctx, cluster)
}

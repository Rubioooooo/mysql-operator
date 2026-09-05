package controller

import (
	"context"
	"encoding/base64"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mysqlRejoinPrimaryObservation struct {
	Candidate mysqlTakeoverCandidateObservation
	Reference mysqlElectionReference
}

type mysqlMemberAncestryObservation struct {
	MemberSubsetOfPrimary bool
	CurrentPrimaryGTIDSet string
}

func validateMysqlReconfiguringStatus(cluster *databasev1.MysqlCluster) error {
	if cluster.Status.HA == nil || cluster.Status.HA.Failover == nil {
		return fmt.Errorf("MySQL replica rejoin requires durable failover status")
	}

	ha := cluster.Status.HA
	failover := ha.Failover
	validState := ha.State == databasev1.MysqlClusterHAStateFailoverInProgress ||
		ha.State == databasev1.MysqlClusterHAStateDegraded
	if !validState ||
		failover.Stage != databasev1.MysqlClusterFailoverStageReconfiguring ||
		failover.FenceState != databasev1.MysqlClusterFenceStateVerified ||
		failover.FencedPrimaryUID == "" ||
		failover.FencedPrimaryUID != failover.FailedPrimaryUID ||
		failover.FailedPrimary == "" || failover.FailedPrimaryUID == "" ||
		failover.Candidate == "" || failover.CandidateUID == "" ||
		failover.Candidate == failover.FailedPrimary ||
		failover.CandidateUID == failover.FailedPrimaryUID ||
		failover.FailedPrimaryServerUUID == "" || failover.FailedPrimaryGTIDSet == nil ||
		ha.Primary != failover.Candidate || ha.PrimaryUID != failover.CandidateUID {
		return fmt.Errorf("invalid durable MySQL replica rejoin status")
	}
	return nil
}

func validateMysqlRejoinInventory(
	cluster *databasev1.MysqlCluster,
	members []mysqlStatefulSetMember,
) error {
	desired := desiredReplicas(cluster)
	if desired < 1 || int32(len(members)) != desired {
		return fmt.Errorf(
			"MysqlCluster %s/%s replica rejoin requires exactly %d desired StatefulSet members, found %d",
			cluster.Namespace,
			cluster.Name,
			desired,
			len(members),
		)
	}
	for i, member := range members {
		expectedOrdinal := int32(i + 1)
		if member.Ordinal != expectedOrdinal || member.Pod.Name != mysqlStatefulSetPodName(cluster, expectedOrdinal) {
			return fmt.Errorf("invalid desired StatefulSet member identity at ordinal %d", expectedOrdinal)
		}
		if _, err := observeMysqlPublishedRole(member.Pod); err != nil {
			return err
		}
	}
	return nil
}

func mysqlMemberAncestryAgainstCurrentPrimaryCommand(memberGTIDSet string, trustedBootstrapGTIDSet ...string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(memberGTIDSet))
	trusted := ""
	if len(trustedBootstrapGTIDSet) > 0 {
		trusted = trustedBootstrapGTIDSet[0]
	}
	trustedEncoded := base64.StdEncoding.EncodeToString([]byte(trusted))
	return mysqlRootClientCommand + fmt.Sprintf(
		` -Nse "SELECT GTID_SUBSET(GTID_SUBTRACT(FROM_BASE64('%s'), FROM_BASE64('%s')), GTID_SUBTRACT(@@GLOBAL.gtid_executed, FROM_BASE64('%s'))), REPLACE(TO_BASE64(@@GLOBAL.gtid_executed), CHAR(10), '');"`,
		encoded,
		trustedEncoded,
		trustedEncoded,
	)
}

func parseMysqlMemberAncestryObservation(output string) (mysqlMemberAncestryObservation, error) {
	fields, err := parseMysqlSingleRow(output, "member ancestry against current primary", 2)
	if err != nil {
		return mysqlMemberAncestryObservation{}, err
	}
	subset, err := parseMysqlBoolean("GTID_SUBSET(member,current-primary)", fields[0])
	if err != nil {
		return mysqlMemberAncestryObservation{}, err
	}
	currentPrimaryGTIDSet, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return mysqlMemberAncestryObservation{}, fmt.Errorf("malformed current-primary GTID encoding: %w", err)
	}
	return mysqlMemberAncestryObservation{
		MemberSubsetOfPrimary: subset,
		CurrentPrimaryGTIDSet: string(currentPrimaryGTIDSet),
	}, nil
}

func mysqlStartSlaveCommand() string {
	return mysqlRootClientCommand + ` -e "START SLAVE;"`
}

func (r *MysqlClusterReconciler) observeMysqlRejoinPrimary(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (mysqlRejoinPrimaryObservation, error) {
	observation := mysqlRejoinPrimaryObservation{}
	candidate, err := r.observeMysqlTakeoverCandidate(ctx, cluster, false)
	observation.Candidate = candidate
	if err != nil {
		return observation, err
	}
	if candidate.Role != mysqlPublishedRoleMaster ||
		candidate.WriteSafety.ReadOnly || candidate.WriteSafety.SuperReadOnly ||
		!candidate.WriteSafety.GTIDReady || !candidate.SourceReady ||
		!mysqlReplicationStopped(candidate.Replication) {
		return observation, fmt.Errorf("published takeover candidate failed Phase 5-D primary proof")
	}

	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return observation, err
	}
	if err := validateMysqlRejoinInventory(cluster, members); err != nil {
		return observation, err
	}
	publishedMasters := 0
	for _, member := range members {
		role, err := observeMysqlPublishedRole(member.Pod)
		if err != nil {
			return observation, err
		}
		if role == mysqlPublishedRoleMaster {
			publishedMasters++
			if member.Pod.Name != candidate.Pod.Name || member.Pod.UID != candidate.Pod.UID {
				return observation, fmt.Errorf("published primary does not match durable takeover candidate")
			}
		}
	}
	if publishedMasters != 1 {
		return observation, fmt.Errorf("Phase 5-D primary proof requires exactly one published master, found %d", publishedMasters)
	}

	reference, err := r.observeMysqlElectionReference(ctx, candidate.Pod, cluster)
	if err != nil {
		return observation, err
	}
	if err := r.validateMysqlGTIDBootstrapIdentity(ctx, cluster, candidate.Pod, reference.ServerUUID); err != nil {
		return observation, err
	}
	observation.Reference = reference
	return observation, nil
}

func (r *MysqlClusterReconciler) getFreshMysqlRejoinMember(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	member mysqlStatefulSetMember,
	expectedRole mysqlPublishedRole,
) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: member.Pod.Name}
	if err := r.Get(ctx, key, pod); err != nil {
		return nil, err
	}
	if pod.UID != member.Pod.UID {
		return nil, fmt.Errorf("replica rejoin member Pod %s UID changed", key)
	}
	if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
		return nil, err
	}
	ordinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil {
		return nil, err
	}
	if ordinal != member.Ordinal || pod.Name != mysqlStatefulSetPodName(cluster, ordinal) {
		return nil, fmt.Errorf("replica rejoin member Pod %s changed canonical ordinal identity", key)
	}
	role, err := observeMysqlPublishedRole(pod)
	if err != nil {
		return nil, err
	}
	if role != expectedRole {
		return nil, fmt.Errorf("replica rejoin member Pod %s role changed from %q to %q", key, expectedRole, role)
	}
	return pod, nil
}

func (r *MysqlClusterReconciler) mutateMysqlRejoinMemberRole(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	member mysqlStatefulSetMember,
	expectedRole mysqlPublishedRole,
	desiredRole mysqlPublishedRole,
) error {
	pod, err := r.getFreshMysqlRejoinMember(ctx, cluster, member, expectedRole)
	if err != nil {
		return err
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
		return fmt.Errorf("failed to update replica rejoin role on Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}

func (r *MysqlClusterReconciler) executeMysqlRejoinMemberMutation(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	member mysqlStatefulSetMember,
	expectedRole mysqlPublishedRole,
	command string,
	description string,
) error {
	pod, err := r.getFreshMysqlRejoinMember(ctx, cluster, member, expectedRole)
	if err != nil {
		return err
	}
	if _, err := r.executeCommandOnPod(pod, command); err != nil {
		return fmt.Errorf("failed to %s on replica rejoin Pod %s/%s: %w", description, pod.Namespace, pod.Name, err)
	}
	return nil
}

func (r *MysqlClusterReconciler) observeMysqlMemberAncestryAgainstCurrentPrimary(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	memberGTIDSet string,
) (mysqlMemberAncestryObservation, mysqlRejoinPrimaryObservation, bool, error) {
	primary, err := r.observeMysqlRejoinPrimary(ctx, cluster)
	if err != nil {
		return mysqlMemberAncestryObservation{}, primary, false, err
	}
	candidate := primary.Candidate.Pod
	trusted, err := mysqlTrustedBootstrapGTIDSet(cluster)
	if err != nil {
		return mysqlMemberAncestryObservation{}, primary, true, err
	}
	output, err := r.executeCommandOnPod(candidate, mysqlMemberAncestryAgainstCurrentPrimaryCommand(memberGTIDSet, trusted))
	if err != nil {
		return mysqlMemberAncestryObservation{}, primary, true, fmt.Errorf(
			"failed to prove member ancestry on current primary Pod %s/%s: %w",
			candidate.Namespace,
			candidate.Name,
			err,
		)
	}
	ancestry, err := parseMysqlMemberAncestryObservation(output)
	return ancestry, primary, true, err
}

func (r *MysqlClusterReconciler) failClosedMysqlRejoinPrimary(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	primary mysqlRejoinPrimaryObservation,
) (ctrl.Result, bool, error) {
	candidate := primary.Candidate
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

func (r *MysqlClusterReconciler) persistMysqlUnsafeRejoinHistory(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	desired := cluster.Status.HA.DeepCopy()
	desired.State = databasev1.MysqlClusterHAStateDegraded
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, desired); err != nil {
		return ctrl.Result{}, false, err
	}
	return mysqlTakeoverBarrierResult()
}

func (r *MysqlClusterReconciler) quarantineMysqlRejoinMember(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	member mysqlStatefulSetMember,
) (ctrl.Result, bool, error) {
	if err := r.mutateMysqlRejoinMemberRole(ctx, cluster, member, mysqlPublishedRoleSlave, mysqlPublishedRoleNone); err != nil {
		return ctrl.Result{}, false, err
	}
	return mysqlTakeoverBarrierResult()
}

func (r *MysqlClusterReconciler) reconcileMysqlRejoinMember(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	member mysqlStatefulSetMember,
	primary mysqlRejoinPrimaryObservation,
) (bool, *ctrl.Result, error) {
	role, err := observeMysqlPublishedRole(member.Pod)
	if err != nil {
		return false, nil, err
	}
	if role == mysqlPublishedRoleMaster {
		return false, nil, fmt.Errorf("non-primary replica rejoin member %s is unexpectedly published as master", member.Pod.Name)
	}
	if role != mysqlPublishedRoleNone && role != mysqlPublishedRoleSlave {
		return false, nil, fmt.Errorf("non-primary replica rejoin member %s has invalid role %q", member.Pod.Name, role)
	}

	fresh, err := r.getFreshMysqlRejoinMember(ctx, cluster, member, role)
	if err != nil {
		return false, nil, err
	}
	if !mysqlStatefulSetPodHealthy(fresh) {
		if role == mysqlPublishedRoleSlave {
			result, _, err := r.quarantineMysqlRejoinMember(ctx, cluster, member)
			return false, &result, err
		}
		return false, nil, nil
	}

	writeSafety, err := r.observeMysqlWriteSafety(ctx, fresh, cluster)
	if err != nil {
		if role == mysqlPublishedRoleSlave {
			result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
			return false, &result, quarantineErr
		}
		return false, nil, nil
	}
	if writeSafety.WriteRole == mysqlWriteRoleWritable {
		if err := r.executeMysqlRejoinMemberMutation(
			ctx,
			cluster,
			member,
			role,
			mysqlSetSuperReadOnlyCommand(),
			"enable super_read_only",
		); err != nil {
			return false, nil, err
		}
		result, _, _ := mysqlTakeoverBarrierResult()
		return false, &result, nil
	}
	if !writeSafety.ReadOnly || !writeSafety.SuperReadOnly || !writeSafety.GTIDReady {
		if role == mysqlPublishedRoleSlave {
			result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
			return false, &result, quarantineErr
		}
		return false, nil, nil
	}

	replication, err := r.observeMysqlMemberReplication(ctx, fresh, cluster)
	if err != nil {
		if role == mysqlPublishedRoleSlave {
			result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
			return false, &result, quarantineErr
		}
		return false, nil, nil
	}
	channel := replication.Channel
	configurationCorrect := channel.Configured &&
		channel.MasterHost == cluster.Spec.MasterService &&
		channel.MasterUser == "replica" &&
		channel.AutoPosition == "1" &&
		channel.MasterUUID == primary.Reference.ServerUUID
	semanticallyHealthy := configurationCorrect &&
		channel.IORunning == "Yes" && channel.SQLRunning == "Yes" &&
		channel.LastIOError == "" && channel.LastSQLError == ""
	if role == mysqlPublishedRoleSlave && !semanticallyHealthy {
		result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
		return false, &result, quarantineErr
	}

	var memberReference mysqlElectionReference
	haveMemberReference := false
	if member.Pod.Name == cluster.Status.HA.Failover.FailedPrimary {
		memberReference, err = r.observeMysqlElectionReference(ctx, fresh, cluster)
		if err != nil {
			if role == mysqlPublishedRoleSlave {
				result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
				return false, &result, quarantineErr
			}
			return false, nil, nil
		}
		if memberReference.ServerUUID != cluster.Status.HA.Failover.FailedPrimaryServerUUID {
			if role == mysqlPublishedRoleSlave {
				result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
				return false, &result, quarantineErr
			}
			result, _, persistErr := r.persistMysqlUnsafeRejoinHistory(ctx, cluster)
			return false, &result, persistErr
		}
		haveMemberReference = true
	}

	wrongSource := channel.Configured && !configurationCorrect
	if role == mysqlPublishedRoleNone && wrongSource &&
		(channel.IORunning != "No" || channel.SQLRunning != "No") {
		if err := r.executeMysqlRejoinMemberMutation(
			ctx,
			cluster,
			member,
			role,
			mysqlStopSlaveCommand(),
			"stop stale replication",
		); err != nil {
			return false, nil, err
		}
		result, _, _ := mysqlTakeoverBarrierResult()
		return false, &result, nil
	}

	if !haveMemberReference {
		memberReference, err = r.observeMysqlElectionReference(ctx, fresh, cluster)
		if err != nil {
			if role == mysqlPublishedRoleSlave {
				result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
				return false, &result, quarantineErr
			}
			return false, nil, nil
		}
	}
	if err := r.validateMysqlGTIDBootstrapIdentity(ctx, cluster, fresh, memberReference.ServerUUID); err != nil {
		if role == mysqlPublishedRoleSlave {
			result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
			return false, &result, quarantineErr
		}
		result, _, persistErr := r.persistMysqlUnsafeRejoinHistory(ctx, cluster)
		return false, &result, persistErr
	}
	ancestry, freshPrimary, primaryProven, err := r.observeMysqlMemberAncestryAgainstCurrentPrimary(ctx, cluster, memberReference.GTIDSet)
	if err != nil {
		if !primaryProven {
			result, _, failClosedErr := r.failClosedMysqlRejoinPrimary(ctx, cluster, freshPrimary)
			return false, &result, failClosedErr
		}
		if role == mysqlPublishedRoleSlave {
			result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
			return false, &result, quarantineErr
		}
		return false, nil, nil
	}
	if !ancestry.MemberSubsetOfPrimary {
		if role == mysqlPublishedRoleSlave {
			result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
			return false, &result, quarantineErr
		}
		result, _, persistErr := r.persistMysqlUnsafeRejoinHistory(ctx, cluster)
		return false, &result, persistErr
	}

	configurationCorrect = channel.Configured &&
		channel.MasterHost == cluster.Spec.MasterService &&
		channel.MasterUser == "replica" &&
		channel.AutoPosition == "1" &&
		channel.MasterUUID == freshPrimary.Reference.ServerUUID
	semanticallyHealthy = configurationCorrect &&
		channel.IORunning == "Yes" && channel.SQLRunning == "Yes" &&
		channel.LastIOError == "" && channel.LastSQLError == ""
	wrongSource = channel.Configured && !configurationCorrect
	if role == mysqlPublishedRoleSlave && !semanticallyHealthy {
		result, _, quarantineErr := r.quarantineMysqlRejoinMember(ctx, cluster, member)
		return false, &result, quarantineErr
	}

	switch {
	case !channel.Configured:
		if err := r.executeMysqlRejoinMemberMutation(
			ctx,
			cluster,
			member,
			role,
			mysqlInitializeReplicaCommand(cluster.Spec.MasterService),
			"initialize replication",
		); err != nil {
			return false, nil, err
		}
		result, _, _ := mysqlTakeoverBarrierResult()
		return false, &result, nil

	case wrongSource:
		if err := r.executeMysqlRejoinMemberMutation(
			ctx,
			cluster,
			member,
			role,
			mysqlConfigureReplicaCommand(cluster.Spec.MasterService),
			"reconfigure replication",
		); err != nil {
			return false, nil, err
		}
		result, _, _ := mysqlTakeoverBarrierResult()
		return false, &result, nil

	case role == mysqlPublishedRoleNone && semanticallyHealthy:
		if err := r.mutateMysqlRejoinMemberRole(ctx, cluster, member, role, mysqlPublishedRoleSlave); err != nil {
			return false, nil, err
		}
		result, _, _ := mysqlTakeoverBarrierResult()
		return false, &result, nil

	case role == mysqlPublishedRoleNone && configurationCorrect &&
		channel.IORunning == "No" && channel.SQLRunning == "No" &&
		channel.LastIOError == "" && channel.LastSQLError == "":
		if err := r.executeMysqlRejoinMemberMutation(
			ctx,
			cluster,
			member,
			role,
			mysqlStartSlaveCommand(),
			"restart replication",
		); err != nil {
			return false, nil, err
		}
		result, _, _ := mysqlTakeoverBarrierResult()
		return false, &result, nil

	case role == mysqlPublishedRoleSlave && semanticallyHealthy:
		return true, nil, nil

	default:
		return false, nil, nil
	}
}

func (r *MysqlClusterReconciler) reconcileMysqlReconfiguringReplicas(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	logMysqlControlBarrier(ctx, "rejoin", cluster)
	if err := validateMysqlReconfiguringStatus(cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if err := validateMysqlReplicationMasterHost(cluster); err != nil {
		return ctrl.Result{}, false, err
	}

	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if err := validateMysqlRejoinInventory(cluster, members); err != nil {
		return mysqlTakeoverBarrierResult()
	}

	failedPrimaryFound := false
	for _, member := range members {
		if member.Pod.Name != cluster.Status.HA.Failover.FailedPrimary {
			continue
		}
		failedPrimaryFound = true
		if string(member.Pod.UID) != cluster.Status.HA.Failover.FailedPrimaryUID {
			return r.persistMysqlUnsafeRejoinHistory(ctx, cluster)
		}
	}
	if !failedPrimaryFound {
		return ctrl.Result{}, false, fmt.Errorf("durable failed-primary member is absent from desired StatefulSet inventory")
	}

	primary, err := r.observeMysqlRejoinPrimary(ctx, cluster)
	if err != nil {
		return r.failClosedMysqlRejoinPrimary(ctx, cluster, primary)
	}

	allMembersConverged := true
	for _, member := range members {
		if member.Pod.Name == cluster.Status.HA.Failover.Candidate {
			continue
		}
		converged, mutationResult, err := r.reconcileMysqlRejoinMember(ctx, cluster, member, primary)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		if mutationResult != nil {
			return *mutationResult, false, nil
		}
		if !converged {
			allMembersConverged = false
		}
	}
	if !allMembersConverged {
		return mysqlTakeoverBarrierResult()
	}

	finalPrimary, err := r.observeMysqlRejoinPrimary(ctx, cluster)
	if err != nil {
		return r.failClosedMysqlRejoinPrimary(ctx, cluster, finalPrimary)
	}

	desired := cluster.Status.HA.DeepCopy()
	desired.State = databasev1.MysqlClusterHAStateVerifying
	desired.Primary = desired.Failover.Candidate
	desired.PrimaryUID = desired.Failover.CandidateUID
	desired.Failover = nil
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, desired); err != nil {
		return ctrl.Result{}, false, err
	}
	return mysqlTakeoverBarrierResult()
}

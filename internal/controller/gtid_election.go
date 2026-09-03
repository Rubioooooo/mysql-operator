package controller

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mysqlGTIDRelation string

const (
	mysqlGTIDRelationEqual     mysqlGTIDRelation = "Equal"
	mysqlGTIDRelationSubset    mysqlGTIDRelation = "Subset"
	mysqlGTIDRelationSuperset  mysqlGTIDRelation = "Superset"
	mysqlGTIDRelationDivergent mysqlGTIDRelation = "Divergent"
)

type mysqlElectionReference struct {
	ServerUUID string
	GTIDSet    string
}

type mysqlGTIDComparison struct {
	Relation         mysqlGTIDRelation
	CandidateGTIDSet string
}

var mysqlServerUUIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func mysqlElectionReferenceCommand() string {
	return mysqlRootClientCommand + ` -Nse "SELECT @@GLOBAL.server_uuid, REPLACE(TO_BASE64(@@GLOBAL.gtid_executed), CHAR(10), '');"`
}

func mysqlSourceCapabilityCommand() string {
	return mysqlRootClientCommand + ` -Nse "SELECT @@GLOBAL.log_bin, @@GLOBAL.log_slave_updates;"`
}

func mysqlGTIDComparisonCommand(failedPrimaryGTIDSet string) string {
	encoded := base64.StdEncoding.EncodeToString([]byte(failedPrimaryGTIDSet))
	return mysqlRootClientCommand + fmt.Sprintf(
		` -Nse "SELECT GTID_SUBSET(FROM_BASE64('%s'), @@GLOBAL.gtid_executed), GTID_SUBSET(@@GLOBAL.gtid_executed, FROM_BASE64('%s')), REPLACE(TO_BASE64(@@GLOBAL.gtid_executed), CHAR(10), '');"`,
		encoded,
		encoded,
	)
}

func parseMysqlSingleRow(output, observation string, fieldCount int) ([]string, error) {
	line := strings.TrimSuffix(output, "\n")
	line = strings.TrimSuffix(line, "\r")
	if line == "" || strings.ContainsAny(line, "\r\n") {
		return nil, fmt.Errorf("malformed MySQL %s: expected exactly one row", observation)
	}
	fields := strings.Split(line, "\t")
	if len(fields) != fieldCount {
		return nil, fmt.Errorf("malformed MySQL %s: expected %d tab-separated fields, got %d", observation, fieldCount, len(fields))
	}
	return fields, nil
}

func parseMysqlElectionReference(output string) (mysqlElectionReference, error) {
	fields, err := parseMysqlSingleRow(output, "election reference", 2)
	if err != nil {
		return mysqlElectionReference{}, err
	}
	if !mysqlServerUUIDPattern.MatchString(fields[0]) {
		return mysqlElectionReference{}, fmt.Errorf("malformed MySQL election reference: invalid server_uuid %q", fields[0])
	}
	decoded, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return mysqlElectionReference{}, fmt.Errorf("malformed MySQL election reference GTID encoding: %w", err)
	}
	return mysqlElectionReference{ServerUUID: fields[0], GTIDSet: string(decoded)}, nil
}

func parseMysqlSourceCapability(output string) (bool, error) {
	fields, err := parseMysqlSingleRow(output, "source capability", 2)
	if err != nil {
		return false, err
	}
	logBin, err := parseMysqlBoolean("log_bin", fields[0])
	if err != nil {
		return false, err
	}
	logSlaveUpdates, err := parseMysqlBoolean("log_slave_updates", fields[1])
	if err != nil {
		return false, err
	}
	return logBin && logSlaveUpdates, nil
}

func parseMysqlGTIDComparison(output string) (mysqlGTIDComparison, error) {
	fields, err := parseMysqlSingleRow(output, "GTID comparison", 3)
	if err != nil {
		return mysqlGTIDComparison{}, err
	}
	primaryInCandidate, err := parseMysqlBoolean("GTID_SUBSET(failed-primary,candidate)", fields[0])
	if err != nil {
		return mysqlGTIDComparison{}, err
	}
	candidateInPrimary, err := parseMysqlBoolean("GTID_SUBSET(candidate,failed-primary)", fields[1])
	if err != nil {
		return mysqlGTIDComparison{}, err
	}
	decoded, err := base64.StdEncoding.DecodeString(fields[2])
	if err != nil {
		return mysqlGTIDComparison{}, fmt.Errorf("malformed MySQL candidate GTID encoding: %w", err)
	}
	relation := mysqlGTIDRelationDivergent
	switch {
	case primaryInCandidate && candidateInPrimary:
		relation = mysqlGTIDRelationEqual
	case !primaryInCandidate && candidateInPrimary:
		relation = mysqlGTIDRelationSubset
	case primaryInCandidate && !candidateInPrimary:
		relation = mysqlGTIDRelationSuperset
	}
	return mysqlGTIDComparison{Relation: relation, CandidateGTIDSet: string(decoded)}, nil
}

func clearMysqlElectionProof(failover *databasev1.MysqlClusterFailoverStatus) {
	if failover == nil {
		return
	}
	failover.Stage = databasev1.MysqlClusterFailoverStageFencing
	failover.Candidate = ""
	failover.CandidateUID = ""
	failover.FailedPrimaryServerUUID = ""
	failover.FailedPrimaryGTIDSet = nil
}

func (r *MysqlClusterReconciler) observeMysqlElectionReference(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
) (mysqlElectionReference, error) {
	if err := r.validateMysqlPodBeforeSQL(ctx, pod, cluster, "election-reference SQL"); err != nil {
		return mysqlElectionReference{}, err
	}
	output, err := r.executeCommandOnPod(pod, mysqlElectionReferenceCommand())
	if err != nil {
		return mysqlElectionReference{}, fmt.Errorf("failed to observe election reference on Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return parseMysqlElectionReference(output)
}

func (r *MysqlClusterReconciler) observeMysqlSourceCapability(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
) (bool, error) {
	if err := r.validateMysqlPodBeforeSQL(ctx, pod, cluster, "source-capability SQL"); err != nil {
		return false, err
	}
	output, err := r.executeCommandOnPod(pod, mysqlSourceCapabilityCommand())
	if err != nil {
		return false, fmt.Errorf("failed to observe source capability on Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return parseMysqlSourceCapability(output)
}

func (r *MysqlClusterReconciler) compareMysqlCandidateGTID(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
	failedPrimaryGTIDSet string,
) (mysqlGTIDComparison, error) {
	if err := r.validateMysqlPodBeforeSQL(ctx, pod, cluster, "GTID comparison SQL"); err != nil {
		return mysqlGTIDComparison{}, err
	}
	output, err := r.executeCommandOnPod(pod, mysqlGTIDComparisonCommand(failedPrimaryGTIDSet))
	if err != nil {
		return mysqlGTIDComparison{}, fmt.Errorf("failed to compare GTID sets on Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return parseMysqlGTIDComparison(output)
}

func (r *MysqlClusterReconciler) getFreshMysqlElectionPod(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	member mysqlStatefulSetMember,
) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: member.Pod.Name}
	if err := r.Get(ctx, key, pod); err != nil {
		return nil, err
	}
	if string(pod.UID) != string(member.Pod.UID) {
		return nil, fmt.Errorf("candidate Pod %s UID changed during election", key)
	}
	if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
		return nil, err
	}
	ordinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil {
		return nil, err
	}
	if ordinal != member.Ordinal || pod.Name != mysqlStatefulSetPodName(cluster, ordinal) {
		return nil, fmt.Errorf("candidate Pod %s has changed canonical ordinal identity", key)
	}
	role, err := observeMysqlPublishedRole(pod)
	if err != nil {
		return nil, err
	}
	if role != mysqlPublishedRoleSlave {
		return nil, fmt.Errorf("candidate Pod %s is no longer published as a replica", key)
	}
	return pod, nil
}

func (r *MysqlClusterReconciler) validateMysqlFailedPrimaryElectionReference(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (*corev1.Pod, mysqlElectionReference, error) {
	failover := cluster.Status.HA.Failover
	pod, role, err := r.getValidatedMysqlFailedPrimary(ctx, cluster, failover)
	if err != nil {
		return nil, mysqlElectionReference{}, err
	}
	if role != mysqlPublishedRoleNone {
		return nil, mysqlElectionReference{}, fmt.Errorf("failed primary Pod %s remains published with role %q", pod.Name, role)
	}
	writeSafety, err := r.observeMysqlWriteSafety(ctx, pod, cluster)
	if err != nil {
		return nil, mysqlElectionReference{}, err
	}
	if writeSafety.WriteRole != mysqlWriteRoleReadOnly || !writeSafety.GTIDReady {
		return nil, mysqlElectionReference{}, fmt.Errorf("failed primary Pod %s no longer has a GTID-ready verified write fence", pod.Name)
	}
	reference, err := r.observeMysqlElectionReference(ctx, pod, cluster)
	if err != nil {
		return nil, mysqlElectionReference{}, err
	}
	return pod, reference, nil
}

func (r *MysqlClusterReconciler) mysqlCandidateMatchesReference(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
	reference mysqlElectionReference,
) (bool, error) {
	writeSafety, err := r.observeMysqlWriteSafety(ctx, pod, cluster)
	if err != nil {
		return false, err
	}
	if writeSafety.WriteRole != mysqlWriteRoleReadOnly || !writeSafety.GTIDReady {
		return false, nil
	}
	sourceReady, err := r.observeMysqlSourceCapability(ctx, pod, cluster)
	if err != nil {
		return false, err
	}
	if !sourceReady {
		return false, nil
	}
	replication, err := r.observeMysqlMemberReplication(ctx, pod, cluster)
	if err != nil {
		return false, err
	}
	if replication.PublishedRole != mysqlPublishedRoleSlave ||
		!replication.Channel.Configured ||
		replication.Channel.MasterHost != cluster.Spec.MasterService ||
		replication.Channel.AutoPosition != "1" ||
		replication.Channel.MasterUUID != reference.ServerUUID {
		return false, nil
	}
	comparison, err := r.compareMysqlCandidateGTID(ctx, pod, cluster, reference.GTIDSet)
	if err != nil {
		return false, err
	}
	return comparison.Relation == mysqlGTIDRelationEqual, nil
}

func (r *MysqlClusterReconciler) persistMysqlNoSafeCandidate(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	desired := cluster.Status.HA.DeepCopy()
	desired.State = databasev1.MysqlClusterHAStateDegraded
	clearMysqlElectionProof(desired.Failover)
	desired.Failover.FenceState = databasev1.MysqlClusterFenceStateVerified
	desired.Failover.FencedPrimaryUID = desired.Failover.FailedPrimaryUID
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, desired); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

func (r *MysqlClusterReconciler) quarantineMysqlElectionCandidate(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	member mysqlStatefulSetMember,
) (ctrl.Result, bool, error) {
	fresh, err := r.getFreshMysqlElectionPod(ctx, cluster, member)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if !mysqlStatefulSetPodHealthy(fresh) {
		return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
	}
	if _, err := r.executeCommandOnPod(fresh, mysqlSetSuperReadOnlyCommand()); err != nil {
		return ctrl.Result{}, false, fmt.Errorf("failed to quarantine election candidate Pod %s/%s: %w", fresh.Namespace, fresh.Name, err)
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

func (r *MysqlClusterReconciler) reconcileMysqlGTIDElection(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	failover := cluster.Status.HA.Failover
	if failover.FenceState != databasev1.MysqlClusterFenceStateVerified ||
		failover.FencedPrimaryUID != failover.FailedPrimaryUID {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}

	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	candidates := make([]mysqlStatefulSetMember, 0, len(members))
	for _, member := range members {
		role, err := observeMysqlPublishedRole(member.Pod)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		if role == mysqlPublishedRoleSlave {
			candidates = append(candidates, member)
		}
	}

	_, reference, err := r.validateMysqlFailedPrimaryElectionReference(ctx, cluster)
	if err != nil {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}

	var selected *mysqlStatefulSetMember
	for i := range candidates {
		candidate := candidates[i]
		if !mysqlStatefulSetPodHealthy(candidate.Pod) {
			continue
		}
		fresh, err := r.getFreshMysqlElectionPod(ctx, cluster, candidate)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		if !mysqlStatefulSetPodHealthy(fresh) {
			continue
		}
		writeSafety, err := r.observeMysqlWriteSafety(ctx, fresh, cluster)
		if err != nil {
			continue
		}
		switch writeSafety.WriteRole {
		case mysqlWriteRoleWritable:
			return r.quarantineMysqlElectionCandidate(ctx, cluster, candidate)
		case mysqlWriteRoleReadOnly:
			if !writeSafety.GTIDReady {
				continue
			}
		default:
			continue
		}

		matches, err := r.mysqlCandidateMatchesReference(ctx, fresh, cluster, reference)
		if err != nil {
			continue
		}
		if matches {
			selected = &candidate
			break
		}
	}
	if selected == nil {
		return r.persistMysqlNoSafeCandidate(ctx, cluster)
	}

	_, finalReference, err := r.validateMysqlFailedPrimaryElectionReference(ctx, cluster)
	if err != nil {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}
	selectedPod, err := r.getFreshMysqlElectionPod(ctx, cluster, *selected)
	if err != nil {
		return r.persistMysqlNoSafeCandidate(ctx, cluster)
	}
	if !mysqlStatefulSetPodHealthy(selectedPod) {
		return r.persistMysqlNoSafeCandidate(ctx, cluster)
	}
	matches, err := r.mysqlCandidateMatchesReference(ctx, selectedPod, cluster, finalReference)
	if err != nil || !matches {
		return r.persistMysqlNoSafeCandidate(ctx, cluster)
	}

	desired := cluster.Status.HA.DeepCopy()
	desired.State = databasev1.MysqlClusterHAStateFailoverInProgress
	desired.Failover.Stage = databasev1.MysqlClusterFailoverStageCandidateSelected
	desired.Failover.Candidate = selectedPod.Name
	desired.Failover.CandidateUID = string(selectedPod.UID)
	desired.Failover.FailedPrimaryServerUUID = finalReference.ServerUUID
	desired.Failover.FailedPrimaryGTIDSet = &finalReference.GTIDSet
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, desired); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

func (r *MysqlClusterReconciler) invalidateMysqlCandidateSelection(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	desired := cluster.Status.HA.DeepCopy()
	desired.State = databasev1.MysqlClusterHAStateFailoverInProgress
	clearMysqlElectionProof(desired.Failover)
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, desired); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

func (r *MysqlClusterReconciler) reconcileMysqlCandidateSelected(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	if cluster.Status.HA == nil || cluster.Status.HA.Failover == nil {
		return ctrl.Result{}, false, fmt.Errorf("CandidateSelected requires durable failover status")
	}
	failover := cluster.Status.HA.Failover
	if failover.FenceState != databasev1.MysqlClusterFenceStateVerified ||
		failover.FencedPrimaryUID != failover.FailedPrimaryUID ||
		failover.Candidate == "" || failover.CandidateUID == "" ||
		failover.FailedPrimaryServerUUID == "" || failover.FailedPrimaryGTIDSet == nil {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}

	failedPrimary, observedReference, err := r.validateMysqlFailedPrimaryElectionReference(ctx, cluster)
	if err != nil {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}
	if observedReference.ServerUUID != failover.FailedPrimaryServerUUID {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}
	primaryComparison, err := r.compareMysqlCandidateGTID(ctx, failedPrimary, cluster, *failover.FailedPrimaryGTIDSet)
	if err != nil || primaryComparison.Relation != mysqlGTIDRelationEqual {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}
	reference := mysqlElectionReference{
		ServerUUID: failover.FailedPrimaryServerUUID,
		GTIDSet:    *failover.FailedPrimaryGTIDSet,
	}

	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: failover.Candidate}
	if err := r.Get(ctx, key, pod); err != nil {
		return r.invalidateMysqlCandidateSelection(ctx, cluster)
	}
	ordinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil {
		return r.invalidateMysqlCandidateSelection(ctx, cluster)
	}
	member := mysqlStatefulSetMember{Ordinal: ordinal, Pod: pod.DeepCopy()}
	if string(pod.UID) != failover.CandidateUID ||
		pod.Name != mysqlStatefulSetPodName(cluster, ordinal) ||
		r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster) != nil ||
		!mysqlStatefulSetPodHealthy(pod) {
		return r.invalidateMysqlCandidateSelection(ctx, cluster)
	}
	fresh, err := r.getFreshMysqlElectionPod(ctx, cluster, member)
	if err != nil || string(fresh.UID) != failover.CandidateUID || !mysqlStatefulSetPodHealthy(fresh) {
		return r.invalidateMysqlCandidateSelection(ctx, cluster)
	}
	matches, err := r.mysqlCandidateMatchesReference(ctx, fresh, cluster, reference)
	if err != nil || !matches {
		return r.invalidateMysqlCandidateSelection(ctx, cluster)
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

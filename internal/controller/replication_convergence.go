package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mysqlReplicaConvergenceAction string

const (
	mysqlReplicaConvergenceNoop        mysqlReplicaConvergenceAction = "noop"
	mysqlReplicaConvergenceConfigure   mysqlReplicaConvergenceAction = "configure"
	mysqlReplicaConvergenceReconfigure mysqlReplicaConvergenceAction = "reconfigure"
	mysqlReplicaConvergenceWait        mysqlReplicaConvergenceAction = "wait"
	mysqlReplicaConvergenceWriteSafety mysqlReplicaConvergenceAction = "write-safety"
)

type mysqlReplicaConvergenceResult struct {
	Action    mysqlReplicaConvergenceAction
	Converged bool
	Mutated   bool
}

func classifyMysqlReplicaConvergence(
	channel mysqlReplicationChannelObservation,
	expectedMasterHost string,
) mysqlReplicaConvergenceAction {
	switch {
	case !channel.Configured:
		return mysqlReplicaConvergenceConfigure
	case !channel.configurationMatches(expectedMasterHost):
		return mysqlReplicaConvergenceReconfigure
	case channel.semanticallyHealthy(expectedMasterHost):
		return mysqlReplicaConvergenceNoop
	default:
		return mysqlReplicaConvergenceWait
	}
}

func (r *MysqlClusterReconciler) reconcileMysqlReplicaChannel(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
) (mysqlReplicaConvergenceResult, error) {
	if err := validateMysqlReplicationMasterHost(cluster); err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}

	freshPod, err := r.getFreshMysqlReplicaConvergencePod(ctx, pod, cluster, "write-safety observation")
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	writeSafety, err := r.observeMysqlWriteSafety(ctx, freshPod, cluster)
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	if !writeSafety.GTIDReady {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"replica Pod %s/%s is not GTID-ready for replication convergence: gtid_mode=%q, enforce_gtid_consistency=%q",
			freshPod.Namespace,
			freshPod.Name,
			writeSafety.GTIDMode,
			writeSafety.EnforceGTIDConsistency,
		)
	}
	if !writeSafety.ReadOnly || !writeSafety.SuperReadOnly {
		freshPod, err = r.getFreshMysqlReplicaConvergencePod(ctx, pod, cluster, "write-safety mutation")
		if err != nil {
			return mysqlReplicaConvergenceResult{}, err
		}
		if _, err := r.executeCommandOnPod(freshPod, mysqlSetSuperReadOnlyCommand()); err != nil {
			return mysqlReplicaConvergenceResult{}, fmt.Errorf(
				"failed to enable super_read_only on replica Pod %s/%s: %w",
				freshPod.Namespace,
				freshPod.Name,
				err,
			)
		}
		return mysqlReplicaConvergenceResult{Action: mysqlReplicaConvergenceWriteSafety, Mutated: true}, nil
	}

	observation, err := r.observeMysqlMemberReplication(ctx, freshPod, cluster)
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}

	action := classifyMysqlReplicaConvergence(observation.Channel, cluster.Spec.MasterService)
	switch action {
	case mysqlReplicaConvergenceNoop:
		return mysqlReplicaConvergenceResult{Action: action, Converged: true}, nil
	case mysqlReplicaConvergenceWait:
		return mysqlReplicaConvergenceResult{Action: action}, nil
	}

	currentPod, err := r.getFreshMysqlReplicaConvergencePod(ctx, pod, cluster, string(action)+" mutation")
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	currentOrdinal, err := mysqlStatefulSetPodOrdinal(currentPod)
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	if currentOrdinal != observation.Ordinal || currentPod.Name != observation.PodName {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"refusing %s for replica Pod %s: observed member identity %s ordinal %d changed to %s ordinal %d",
			action,
			client.ObjectKeyFromObject(pod),
			observation.PodName,
			observation.Ordinal,
			currentPod.Name,
			currentOrdinal,
		)
	}

	var command string
	switch action {
	case mysqlReplicaConvergenceConfigure:
		command = mysqlInitializeReplicaCommand(cluster.Spec.MasterService)
	case mysqlReplicaConvergenceReconfigure:
		command = mysqlConfigureReplicaCommand(cluster.Spec.MasterService)
	default:
		return mysqlReplicaConvergenceResult{}, fmt.Errorf("unsupported replica convergence action %q", action)
	}
	if _, err := r.executeCommandOnPod(currentPod, command); err != nil {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"failed to %s replication channel on Pod %s/%s: %w",
			action,
			currentPod.Namespace,
			currentPod.Name,
			err,
		)
	}

	return mysqlReplicaConvergenceResult{Action: action, Mutated: true}, nil
}

func (r *MysqlClusterReconciler) getFreshMysqlReplicaConvergencePod(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
	purpose string,
) (*corev1.Pod, error) {
	observedOrdinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil {
		return nil, err
	}
	if expectedName := mysqlStatefulSetPodName(cluster, observedOrdinal); pod.Name != expectedName {
		return nil, fmt.Errorf(
			"Pod %s/%s ordinal identity does not match %s label %d: expected name %s",
			pod.Namespace,
			pod.Name,
			statefulSetPodIndexLabel,
			observedOrdinal,
			expectedName,
		)
	}

	podKey := client.ObjectKeyFromObject(pod)
	freshPod := &corev1.Pod{}
	if err := r.Get(ctx, podKey, freshPod); err != nil {
		return nil, fmt.Errorf("failed to re-fetch replica Pod %s before %s: %w", podKey, purpose, err)
	}
	if freshPod.UID != pod.UID {
		return nil, fmt.Errorf(
			"refusing %s for replica Pod %s: observed UID %q changed to %q",
			purpose,
			podKey,
			pod.UID,
			freshPod.UID,
		)
	}
	if err := r.validateMysqlPodBeforeSQL(ctx, freshPod, cluster, purpose+" SQL"); err != nil {
		return nil, err
	}
	freshOrdinal, err := mysqlStatefulSetPodOrdinal(freshPod)
	if err != nil {
		return nil, err
	}
	if expectedName := mysqlStatefulSetPodName(cluster, freshOrdinal); freshPod.Name != expectedName {
		return nil, fmt.Errorf(
			"Pod %s/%s ordinal identity does not match %s label %d: expected name %s",
			freshPod.Namespace,
			freshPod.Name,
			statefulSetPodIndexLabel,
			freshOrdinal,
			expectedName,
		)
	}
	if freshOrdinal != observedOrdinal {
		return nil, fmt.Errorf(
			"refusing %s for replica Pod %s: observed ordinal %d changed to %d",
			purpose,
			podKey,
			observedOrdinal,
			freshOrdinal,
		)
	}
	return freshPod, nil
}

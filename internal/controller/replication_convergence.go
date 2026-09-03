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

	observation, err := r.observeMysqlMemberReplication(ctx, pod, cluster)
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

	currentPod := &corev1.Pod{}
	podKey := client.ObjectKeyFromObject(pod)
	if err := r.Get(ctx, podKey, currentPod); err != nil {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"failed to re-fetch replica Pod %s before %s: %w",
			podKey,
			action,
			err,
		)
	}
	if currentPod.UID != pod.UID {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"refusing %s for replica Pod %s: observed UID %q changed to %q",
			action,
			podKey,
			pod.UID,
			currentPod.UID,
		)
	}
	if err := r.validateMysqlPodBeforeSQL(ctx, currentPod, cluster, "replica convergence mutation SQL"); err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	currentOrdinal, err := mysqlStatefulSetPodOrdinal(currentPod)
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	expectedPodName := mysqlStatefulSetPodName(cluster, currentOrdinal)
	if currentPod.Name != expectedPodName {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"Pod %s/%s ordinal identity does not match %s label %d: expected name %s",
			currentPod.Namespace,
			currentPod.Name,
			statefulSetPodIndexLabel,
			currentOrdinal,
			expectedPodName,
		)
	}
	if currentOrdinal != observation.Ordinal || currentPod.Name != observation.PodName {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"refusing %s for replica Pod %s: observed member identity %s ordinal %d changed to %s ordinal %d",
			action,
			podKey,
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

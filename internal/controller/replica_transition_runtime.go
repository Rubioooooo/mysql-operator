package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func replicaCountCopy(value int32) *int32 {
	copy := value
	return &copy
}

func replicaTransitionCopy(
	transition *databasev1.MysqlClusterReplicaTransitionStatus,
) *databasev1.MysqlClusterReplicaTransitionStatus {
	if transition == nil {
		return nil
	}
	return &databasev1.MysqlClusterReplicaTransitionStatus{
		FromReplicas:   transition.FromReplicas,
		TargetReplicas: transition.TargetReplicas,
	}
}

func (r *MysqlClusterReconciler) persistMysqlClusterReplicaTransitionStatus(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	lastConvergedReplicas int32,
	transition *databasev1.MysqlClusterReplicaTransitionStatus,
) error {
	base := cluster.DeepCopy()
	cluster.Status.LastConvergedReplicas = replicaCountCopy(lastConvergedReplicas)
	cluster.Status.ReplicaTransition = replicaTransitionCopy(transition)
	if err := r.Status().Patch(ctx, cluster, client.MergeFrom(base)); err != nil {
		return fmt.Errorf(
			"failed to persist replica transition status on MysqlCluster %s/%s: %w",
			cluster.Namespace,
			cluster.Name,
			err,
		)
	}
	return nil
}

func mysqlReplicaTransitionDeltaReady(
	members []mysqlStatefulSetMember,
	transition *databasev1.MysqlClusterReplicaTransitionStatus,
) bool {
	if transition.TargetReplicas > transition.FromReplicas {
		membersByOrdinal := make(map[int32]*mysqlStatefulSetMember, len(members))
		for i := range members {
			member := &members[i]
			membersByOrdinal[member.Ordinal] = member
		}
		for ordinal := transition.FromReplicas + 1; ordinal <= transition.TargetReplicas; ordinal++ {
			member, found := membersByOrdinal[ordinal]
			if !found || !mysqlStatefulSetPodHealthy(member.Pod) {
				return false
			}
		}
		return true
	}

	// Scale-down and return-to-from transitions must wait for every member
	// outside the current target to disappear. Retained member health is a
	// domain/HA input, not a lifecycle prerequisite.
	for _, member := range members {
		if member.Ordinal > transition.TargetReplicas {
			return false
		}
	}
	return true
}

func mysqlReplicaTransitionFullyConverged(
	members []mysqlStatefulSetMember,
	targetReplicas int32,
) bool {
	if int32(len(members)) != targetReplicas {
		return false
	}
	for i := range members {
		member := &members[i]
		if member.Ordinal != int32(i+1) || !mysqlStatefulSetPodHealthy(member.Pod) {
			return false
		}
	}
	return true
}

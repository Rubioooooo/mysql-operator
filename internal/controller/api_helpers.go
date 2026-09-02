package controller

import (
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
)

const defaultMysqlReplicas int32 = 3

// desiredReplicas returns the replica count requested by the API object.
//
// The API server defaults spec.replicas to 3. The nil fallback keeps the
// controller defensive when handling objects constructed directly in tests
// or by callers that have not passed through API-server defaulting.
func desiredReplicas(cluster *databasev1.MysqlCluster) int32 {
	if cluster.Spec.Replicas == nil {
		return defaultMysqlReplicas
	}

	return *cluster.Spec.Replicas
}

func validateMysqlClusterReplicaTransitionStatus(status *databasev1.MysqlClusterStatus) error {
	if status.LastConvergedReplicas != nil && *status.LastConvergedReplicas <= 0 {
		return fmt.Errorf(
			"lastConvergedReplicas must be greater than zero, got %d",
			*status.LastConvergedReplicas,
		)
	}

	transition := status.ReplicaTransition
	if transition == nil {
		return nil
	}
	if status.LastConvergedReplicas == nil {
		return fmt.Errorf("replicaTransition requires lastConvergedReplicas")
	}
	if transition.FromReplicas <= 0 {
		return fmt.Errorf(
			"replicaTransition.fromReplicas must be greater than zero, got %d",
			transition.FromReplicas,
		)
	}
	if transition.TargetReplicas <= 0 {
		return fmt.Errorf(
			"replicaTransition.targetReplicas must be greater than zero, got %d",
			transition.TargetReplicas,
		)
	}
	if transition.FromReplicas != *status.LastConvergedReplicas {
		return fmt.Errorf(
			"replicaTransition.fromReplicas %d does not match lastConvergedReplicas %d",
			transition.FromReplicas,
			*status.LastConvergedReplicas,
		)
	}

	return nil
}

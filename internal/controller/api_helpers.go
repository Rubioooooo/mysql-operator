package controller

import databasev1 "github.com/egonlin/api/v1"

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

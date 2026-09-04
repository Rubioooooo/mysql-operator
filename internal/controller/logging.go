package controller

import (
	"context"

	databasev1 "github.com/egonlin/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func mysqlSafeUpgradeLogValues(upgrade *databasev1.MysqlClusterUpgradeStatus) []interface{} {
	if upgrade == nil {
		return nil
	}
	return []interface{}{"operation", "upgrade_transition", "upgrade_stage", upgrade.Stage, "from_image", upgrade.FromImage, "target_image", upgrade.TargetImage}
}

func logMysqlUpgradeTransition(ctx context.Context, upgrade *databasev1.MysqlClusterUpgradeStatus) {
	log.FromContext(ctx).Info("mysqlcluster upgrade barrier persisted", mysqlSafeUpgradeLogValues(upgrade)...)
}

// Explicit allowlist: never pass an HA/failover object to the logger. Those
// objects also contain UID, server identity and replication proof material.
func mysqlSafeHALogValues(status *databasev1.MysqlClusterHAStatus) []interface{} {
	if status == nil {
		return nil
	}
	values := []interface{}{"ha_state", status.State, "primary", status.Primary}
	if failover := status.Failover; failover != nil {
		values = append(values, "failover_stage", failover.Stage, "fence_state", failover.FenceState,
			"candidate", failover.Candidate, "failed_primary", failover.FailedPrimary)
	}
	return values
}

// The caller supplies the already classified Gate 2-B reason after persistence.
// Logging neither reclassifies the transition nor returns a control-path error.
func logMysqlHAStatusTransition(ctx context.Context, reason string, after *databasev1.MysqlClusterHAStatus) {
	if reason == "" {
		return
	}
	values := append([]interface{}{"operation", "ha_transition", "reason", reason}, mysqlSafeHALogValues(after)...)
	log.FromContext(ctx).Info("mysqlcluster HA transition persisted", values...)
}

func logMysqlReplicaTransition(ctx context.Context, reason string, before, after *databasev1.MysqlClusterStatus) {
	if reason == "" {
		return
	}
	transition := after.ReplicaTransition
	if transition == nil {
		transition = before.ReplicaTransition
	}
	if transition == nil {
		return
	}
	log.FromContext(ctx).Info("mysqlcluster replica transition persisted",
		"operation", "replica_transition", "reason", reason,
		"from_replicas", transition.FromReplicas, "target_replicas", transition.TargetReplicas)
}

func logMysqlObservabilityProjection(ctx context.Context, cluster *databasev1.MysqlCluster) {
	log.FromContext(ctx).V(1).Info("mysqlcluster observability status projected",
		"operation", "observability", "phase", cluster.Status.Phase,
		"desired_replicas", desiredReplicas(cluster), "current_replicas", cluster.Status.CurrentReplicas,
		"ready_replicas", cluster.Status.ReadyReplicas, "primary", cluster.Status.Primary)
}

func logMysqlControlBarrier(ctx context.Context, operation string, cluster *databasev1.MysqlCluster) {
	values := append([]interface{}{"operation", operation}, mysqlSafeHALogValues(cluster.Status.HA)...)
	log.FromContext(ctx).V(1).Info("mysqlcluster control barrier entered", values...)
}

// Accept only the outcome: the snapshot and observation error cannot
// accidentally become log values here. The periodic control loop is unchanged.
func logMysqlGTIDSnapshotRefresh(ctx context.Context, succeeded bool) {
	if succeeded {
		log.FromContext(ctx).V(1).Info("primary GTID snapshot refreshed", "operation", "gtid_snapshot")
	} else {
		log.FromContext(ctx).V(1).Info("primary GTID snapshot refresh failed", "operation", "gtid_snapshot", "reason", "mysql_observation_failed")
	}
}

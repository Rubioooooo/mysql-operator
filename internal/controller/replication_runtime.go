package controller

import (
	"context"
	"fmt"
	"time"

	databasev1 "github.com/egonlin/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const mysqlReplicationRuntimeRequeueAfter = 2 * time.Second

func (r *MysqlClusterReconciler) reconcileMysqlHealthyPrimaryRuntime(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}

	publishedRoles := make(map[string]mysqlPublishedRole, len(members))
	primaryName := ""
	primaryCount := 0
	for _, member := range members {
		publishedRole, err := observeMysqlPublishedRole(member.Pod)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		publishedRoles[member.Pod.Name] = publishedRole
		if publishedRole == mysqlPublishedRoleMaster {
			primaryName = member.Pod.Name
			primaryCount++
		}
	}
	if primaryCount != 1 {
		return ctrl.Result{}, false, fmt.Errorf(
			"MysqlCluster %s/%s healthy-primary runtime requires exactly one published primary, found %d",
			cluster.Namespace,
			cluster.Name,
			primaryCount,
		)
	}

	allReplicasConverged := true
	for _, member := range members {
		if member.Pod.Name == primaryName {
			continue
		}
		result, err := r.reconcileMysqlReplicaChannel(ctx, member.Pod, cluster)
		if err != nil {
			return ctrl.Result{}, false, fmt.Errorf(
				"failed to converge replica Pod %s/%s: %w",
				member.Pod.Namespace,
				member.Pod.Name,
				err,
			)
		}
		if !result.Converged {
			allReplicasConverged = false
		}
	}

	if !allReplicasConverged {
		return ctrl.Result{RequeueAfter: mysqlReplicationRuntimeRequeueAfter}, false, nil
	}

	for _, member := range members {
		if member.Pod.Name == primaryName || publishedRoles[member.Pod.Name] == mysqlPublishedRoleSlave {
			continue
		}
		if err := r.labelPod(ctx, member.Pod.Name, string(mysqlPublishedRoleSlave), *cluster); err != nil {
			return ctrl.Result{}, false, fmt.Errorf(
				"failed to publish slave role for converged replica Pod %s/%s: %w",
				member.Pod.Namespace,
				member.Pod.Name,
				err,
			)
		}
	}

	return ctrl.Result{}, true, nil
}

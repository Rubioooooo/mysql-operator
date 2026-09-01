package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *MysqlClusterReconciler) ensureMysqlRoutingServices(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) error {
	if _, err := r.ensureMysqlRoutingService(ctx, desiredMysqlRoutingService(cluster, cluster.Spec.MasterService, "master"), cluster); err != nil {
		return fmt.Errorf("failed to ensure primary routing Service for MysqlCluster %s: %w", cluster.Name, err)
	}
	if _, err := r.ensureMysqlRoutingService(ctx, desiredMysqlRoutingService(cluster, cluster.Spec.SlaveService, "slave"), cluster); err != nil {
		return fmt.Errorf("failed to ensure replica routing Service for MysqlCluster %s: %w", cluster.Name, err)
	}
	return nil
}

func mysqlStatefulSetInitialTopology(cluster *databasev1.MysqlCluster) (string, []string) {
	replicas := desiredReplicas(cluster)
	primaryName := mysqlStatefulSetPodName(cluster, 1)
	replicaNames := make([]string, 0, max(replicas-1, 0))
	for ordinal := int32(2); ordinal <= replicas; ordinal++ {
		replicaNames = append(replicaNames, mysqlStatefulSetPodName(cluster, ordinal))
	}
	return primaryName, replicaNames
}

func mysqlStatefulSetCurrentReplicas(statefulSet *appsv1.StatefulSet) int32 {
	if statefulSet.Spec.Replicas == nil {
		return 1
	}
	return *statefulSet.Spec.Replicas
}

func (r *MysqlClusterReconciler) markMysqlClusterInitialized(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) error {
	if cluster.Annotations["initialized"] == "true" {
		return nil
	}

	base := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations["initialized"] = "true"
	if err := r.Patch(ctx, cluster, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("failed to mark MysqlCluster %s initialized: %w", cluster.Name, err)
	}
	return nil
}

func (r *MysqlClusterReconciler) reconcileStatefulSetInitialization(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	if err := r.ensureMysqlRoutingServices(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if err := r.validateNoLegacyRawPodLifecycle(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if _, err := r.ensureMysqlHeadlessService(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if _, err := r.ensureMysqlSharedConfigMap(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if _, err := r.ensureMysqlStatefulSet(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}

	ready, err := r.mysqlStatefulSetMembersReady(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if !ready {
		return ctrl.Result{}, false, nil
	}

	primaryName, replicaNames := mysqlStatefulSetInitialTopology(cluster)
	if err := r.setupMasterSlaveReplication(ctx, primaryName, replicaNames, *cluster); err != nil {
		return ctrl.Result{}, false, fmt.Errorf("failed to initialize StatefulSet MySQL topology: %w", err)
	}

	if err := r.markMysqlClusterInitialized(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}

	return ctrl.Result{}, true, nil
}

func (r *MysqlClusterReconciler) reconcileStatefulSetRuntime(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	if err := r.validateNoLegacyRawPodLifecycle(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}

	existingStatefulSet := &appsv1.StatefulSet{}
	statefulSetKey := client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetName(cluster)}
	statefulSetExists := true
	if err := r.Get(ctx, statefulSetKey, existingStatefulSet); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, false, fmt.Errorf("failed to get StatefulSet %s before replica transition: %w", statefulSetKey, err)
		}
		statefulSetExists = false
	} else if err := validateControlledBy(existingStatefulSet, cluster, "StatefulSet"); err != nil {
		return ctrl.Result{}, false, err
	}

	if statefulSetExists {
		currentReplicas := mysqlStatefulSetCurrentReplicas(existingStatefulSet)
		if err := r.validateMysqlStatefulSetScaleDownSafety(
			ctx,
			cluster,
			currentReplicas,
			desiredReplicas(cluster),
		); err != nil {
			return ctrl.Result{}, false, err
		}
	}

	if err := r.ensureMysqlRoutingServices(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if _, err := r.ensureMysqlHeadlessService(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if _, err := r.ensureMysqlSharedConfigMap(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if _, err := r.ensureMysqlStatefulSet(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}

	ready, err := r.mysqlStatefulSetMembersReady(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if !ready {
		return ctrl.Result{}, false, nil
	}

	result, err := r.reconcileMasterSlave(ctx, *cluster)
	if err != nil {
		return result, false, err
	}
	return result, true, nil
}

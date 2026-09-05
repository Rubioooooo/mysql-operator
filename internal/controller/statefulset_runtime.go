package controller

import (
	"context"
	"fmt"
	"time"

	databasev1 "github.com/egonlin/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const mysqlInitializationConvergenceRequeueAfter = 2 * time.Second

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
	if cluster.Annotations[mysqlClusterInitializedAnnotation] == "true" {
		return nil
	}

	base := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations[mysqlClusterInitializedAnnotation] = "true"
	if err := r.Patch(ctx, cluster, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("failed to mark MysqlCluster %s initialized: %w", cluster.Name, err)
	}
	return nil
}

func (r *MysqlClusterReconciler) reconcileStatefulSetInitialization(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	if err := validateMysqlReplicationMasterHost(cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if err := r.validateNoLegacyRawPodLifecycle(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if err := r.ensureMysqlCredentials(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
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
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	topologyStarted := false
	for _, member := range members {
		role, err := observeMysqlPublishedRole(member.Pod)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		topologyStarted = topologyStarted || role != mysqlPublishedRoleNone
	}
	if topologyStarted {
		if len(cluster.Status.GTIDBootstrap) != len(members) {
			return ctrl.Result{}, false, fmt.Errorf("initial topology was published without complete GTID bootstrap provenance")
		}
		if err := validateMysqlGTIDBootstrapStatus(cluster.Status.GTIDBootstrap); err != nil {
			return ctrl.Result{}, false, err
		}
		if err := r.validateMysqlGTIDBootstrapMembers(ctx, cluster, members); err != nil {
			return ctrl.Result{}, false, fmt.Errorf("initial topology GTID bootstrap identity changed: %w", err)
		}
	} else {
		provenanceReady, provenanceBarrier, err := r.reconcileMysqlInitialGTIDBootstrap(ctx, cluster)
		if err != nil {
			return ctrl.Result{}, false, fmt.Errorf("failed to establish initial GTID bootstrap provenance: %w", err)
		}
		if provenanceBarrier || !provenanceReady {
			return ctrl.Result{RequeueAfter: mysqlInitializationConvergenceRequeueAfter}, false, nil
		}
	}

	primaryName, replicaNames := mysqlStatefulSetInitialTopology(cluster)
	topologyConverged, err := r.reconcileMysqlInitializationTopology(ctx, primaryName, replicaNames, *cluster)
	if err != nil {
		return ctrl.Result{}, false, fmt.Errorf("failed to initialize StatefulSet MySQL topology: %w", err)
	}
	if !topologyConverged {
		return ctrl.Result{RequeueAfter: mysqlInitializationConvergenceRequeueAfter}, false, nil
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
	if err := validateMysqlReplicationMasterHost(cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if err := validateMysqlClusterReplicaTransitionStatus(&cluster.Status); err != nil {
		return ctrl.Result{}, false, fmt.Errorf(
			"MysqlCluster %s/%s has invalid replica transition status: %w",
			cluster.Namespace,
			cluster.Name,
			err,
		)
	}
	if err := r.validateNoLegacyRawPodLifecycle(ctx, cluster); err != nil {
		return ctrl.Result{}, false, err
	}
	if err := r.ensureMysqlCredentials(ctx, cluster); err != nil {
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

	// Ownership ambiguity is globally unsafe even though member readiness is
	// intentionally not a stable-runtime prerequisite.
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}

	desiredReplicaCount := desiredReplicas(cluster)
	if cluster.Status.LastConvergedReplicas == nil {
		// An initialized pre-B2 object has no durable checkpoint. An existing
		// owned StatefulSet is the authoritative compatibility checkpoint. If
		// the child is missing, recording the current desired count preserves
		// the pre-B2 initialized child-recreation behavior without inventing a
		// different historical replica count.
		checkpoint := desiredReplicaCount
		if statefulSetExists {
			checkpoint = mysqlStatefulSetCurrentReplicas(existingStatefulSet)
		}
		var transition *databasev1.MysqlClusterReplicaTransitionStatus
		if checkpoint != desiredReplicaCount {
			transition = &databasev1.MysqlClusterReplicaTransitionStatus{
				FromReplicas:   checkpoint,
				TargetReplicas: desiredReplicaCount,
			}
		}
		if err := r.persistMysqlClusterReplicaTransitionStatus(ctx, cluster, checkpoint, transition); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{}, false, nil
	}

	transition := cluster.Status.ReplicaTransition
	if transition != nil && transition.TargetReplicas != desiredReplicaCount {
		updatedTransition := &databasev1.MysqlClusterReplicaTransitionStatus{
			FromReplicas:   transition.FromReplicas,
			TargetReplicas: desiredReplicaCount,
		}
		if err := r.persistMysqlClusterReplicaTransitionStatus(
			ctx,
			cluster,
			*cluster.Status.LastConvergedReplicas,
			updatedTransition,
		); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{}, false, nil
	}

	if transition == nil && *cluster.Status.LastConvergedReplicas != desiredReplicaCount {
		if statefulSetExists {
			if err := r.validateMysqlStatefulSetScaleDownSafety(
				ctx,
				cluster,
				mysqlStatefulSetCurrentReplicas(existingStatefulSet),
				desiredReplicaCount,
			); err != nil {
				return ctrl.Result{}, false, err
			}
		}
		transition = &databasev1.MysqlClusterReplicaTransitionStatus{
			FromReplicas:   *cluster.Status.LastConvergedReplicas,
			TargetReplicas: desiredReplicaCount,
		}
		if err := r.persistMysqlClusterReplicaTransitionStatus(
			ctx,
			cluster,
			*cluster.Status.LastConvergedReplicas,
			transition,
		); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{}, false, nil
	}

	effectiveTarget := desiredReplicaCount
	if transition != nil {
		effectiveTarget = transition.TargetReplicas
	}
	if statefulSetExists {
		if err := r.validateMysqlStatefulSetScaleDownSafety(
			ctx,
			cluster,
			mysqlStatefulSetCurrentReplicas(existingStatefulSet),
			effectiveTarget,
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
	if transition != nil {
		members, err = r.listMysqlStatefulSetPods(ctx, cluster)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		if !mysqlReplicaTransitionDeltaReady(members, transition) {
			return ctrl.Result{}, false, nil
		}
	}

	result, replicaDomainConverged, err := r.reconcileMasterSlave(ctx, *cluster)
	if err != nil {
		return result, false, err
	}
	if transition != nil &&
		replicaDomainConverged &&
		mysqlReplicaTransitionFullyConverged(members, transition.TargetReplicas) {
		if err := r.persistMysqlClusterReplicaTransitionStatus(
			ctx,
			cluster,
			transition.TargetReplicas,
			nil,
		); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{}, false, nil
	}
	if !replicaDomainConverged && result.RequeueAfter > 0 {
		return result, false, nil
	}
	return result, true, nil
}

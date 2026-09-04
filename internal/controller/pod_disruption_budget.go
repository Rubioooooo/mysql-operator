package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	policyv1 "k8s.io/api/policy/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// This budget only constrains voluntary disruptions through the Eviction API.
// It does not replace HA safety or constrain direct Pod deletion/replacement.
func mysqlPodDisruptionBudgetMaxUnavailable(cluster *databasev1.MysqlCluster) int32 {
	desired := desiredReplicas(cluster)
	if desired <= 1 || cluster.Status.LastConvergedReplicas == nil ||
		*cluster.Status.LastConvergedReplicas != desired || cluster.Status.ReplicaTransition != nil {
		return 0
	}
	// Spec intent must tighten the budget before durable transition/upgrade
	// status is written in a later reconciliation. Unknown state fails closed.
	if cluster.Status.LastConvergedImage == "" || cluster.Spec.Image != cluster.Status.LastConvergedImage ||
		cluster.Status.Upgrade != nil {
		return 0
	}
	if cluster.Status.HA == nil || cluster.Status.HA.State != databasev1.MysqlClusterHAStateHealthy ||
		cluster.Status.HA.Failover != nil {
		return 0
	}
	return 1
}

func desiredMysqlPodDisruptionBudget(cluster *databasev1.MysqlCluster) *policyv1.PodDisruptionBudget {
	maxUnavailable := intstr.FromInt32(mysqlPodDisruptionBudgetMaxUnavailable(cluster))
	unhealthyPolicy := policyv1.IfHealthyBudget
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name: mysqlPodDisruptionBudgetName(cluster), Namespace: cluster.Namespace,
			Labels: mysqlIdentityLabels(cluster),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector:                   &metav1.LabelSelector{MatchLabels: mysqlStatefulSetSelectorLabels(cluster)},
			MaxUnavailable:             &maxUnavailable,
			UnhealthyPodEvictionPolicy: &unhealthyPolicy,
		},
	}
}

func (r *MysqlClusterReconciler) reconcileMysqlPodDisruptionBudget(ctx context.Context, cluster *databasev1.MysqlCluster) (bool, error) {
	desired := desiredMysqlPodDisruptionBudget(cluster)
	existing := &policyv1.PodDisruptionBudget{}
	key := client.ObjectKeyFromObject(desired)
	if err := r.Get(ctx, key, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("failed to get PodDisruptionBudget %s: %w", key, err)
		}
		if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
			return false, fmt.Errorf("failed to set MysqlCluster %s as controller of PodDisruptionBudget %s: %w", cluster.Name, key, err)
		}
		if err := r.Create(ctx, desired); err != nil {
			return false, fmt.Errorf("failed to create PodDisruptionBudget %s: %w", key, err)
		}
		return true, nil
	}
	if err := validateControlledBy(existing, cluster, "PodDisruptionBudget"); err != nil {
		return false, err
	}
	// All controlled spec values are explicit (including IfHealthyBudget).
	// Semantic equality handles API roundtrip nil/empty selector collections.
	if apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) &&
		apiequality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) &&
		apiequality.Semantic.DeepEqual(existing.Spec.MinAvailable, desired.Spec.MinAvailable) &&
		apiequality.Semantic.DeepEqual(existing.Spec.MaxUnavailable, desired.Spec.MaxUnavailable) &&
		apiequality.Semantic.DeepEqual(existing.Spec.UnhealthyPodEvictionPolicy, desired.Spec.UnhealthyPodEvictionPolicy) {
		return false, nil
	}
	existing.Labels = desired.Labels
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.MinAvailable = desired.Spec.MinAvailable
	existing.Spec.MaxUnavailable = desired.Spec.MaxUnavailable
	existing.Spec.UnhealthyPodEvictionPolicy = desired.Spec.UnhealthyPodEvictionPolicy
	if err := r.Update(ctx, existing); err != nil {
		return false, fmt.Errorf("failed to update PodDisruptionBudget %s: %w", key, err)
	}
	return true, nil
}

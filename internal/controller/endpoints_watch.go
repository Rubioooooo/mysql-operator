package controller

import (
	"context"
	"sort"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// mapMysqlPrimaryEndpointsToMysqlClusters only schedules reconciliation. Routing
// Endpoints are matched by Service name, not owner references or Pod membership.
func (r *MysqlClusterReconciler) mapMysqlPrimaryEndpointsToMysqlClusters(ctx context.Context, object client.Object) []reconcile.Request {
	endpoints, ok := object.(*corev1.Endpoints)
	if !ok {
		return nil
	}
	clusters := &databasev1.MysqlClusterList{}
	if err := r.List(ctx, clusters, client.InNamespace(endpoints.Namespace)); err != nil {
		log.FromContext(ctx).Error(err, "failed to list MysqlClusters referencing primary Endpoints", "namespace", endpoints.Namespace, "endpoints", endpoints.Name)
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range clusters.Items {
		cluster := &clusters.Items[i]
		if cluster.Spec.MasterService == endpoints.Name {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
		}
	}
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].NamespacedName.String() < requests[j].NamespacedName.String()
	})
	return requests
}

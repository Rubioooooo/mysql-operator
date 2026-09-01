package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func mysqlRoutingSelectorLabels(cluster *databasev1.MysqlCluster, role string) map[string]string {
	labels := mysqlStatefulSetSelectorLabels(cluster)
	labels[LabelMysqlRole] = role
	return labels
}

func desiredMysqlRoutingService(cluster *databasev1.MysqlCluster, name, role string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    mysqlRoleLabels(cluster, role),
		},
		Spec: corev1.ServiceSpec{
			Selector: mysqlRoutingSelectorLabels(cluster, role),
			Ports: []corev1.ServicePort{
				{
					Name:       "mysql",
					Protocol:   corev1.ProtocolTCP,
					Port:       3306,
					TargetPort: intstr.FromInt32(3306),
				},
			},
			Type: corev1.ServiceTypeClusterIP,
		},
	}
}

func (r *MysqlClusterReconciler) ensureMysqlRoutingService(
	ctx context.Context,
	desired *corev1.Service,
	cluster *databasev1.MysqlCluster,
) (*corev1.Service, error) {
	existing := &corev1.Service{}
	key := client.ObjectKeyFromObject(desired)

	if err := r.Get(ctx, key, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get routing Service %s: %w", key, err)
		}

		if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
			return nil, fmt.Errorf("failed to set MysqlCluster %s as controller of routing Service %s: %w", cluster.Name, key, err)
		}
		if err := r.Create(ctx, desired); err != nil {
			return nil, fmt.Errorf("failed to create routing Service %s: %w", key, err)
		}
		return desired, nil
	}

	if err := validateControlledBy(existing, cluster, "Service"); err != nil {
		return nil, err
	}
	if existing.Spec.ClusterIP == corev1.ClusterIPNone {
		return nil, fmt.Errorf(
			"routing Service %s/%s controlled by MysqlCluster %s is headless and cannot be reconciled in place",
			existing.Namespace,
			existing.Name,
			cluster.Name,
		)
	}

	changed := false
	if !apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		existing.Labels = desired.Labels
		changed = true
	}
	if !apiequality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		existing.Spec.Selector = desired.Spec.Selector
		changed = true
	}
	if !apiequality.Semantic.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) {
		existing.Spec.Ports = desired.Spec.Ports
		changed = true
	}
	if existing.Spec.Type != desired.Spec.Type {
		existing.Spec.Type = desired.Spec.Type
		changed = true
	}

	if changed {
		if err := r.Update(ctx, existing); err != nil {
			return nil, fmt.Errorf("failed to update routing Service %s: %w", key, err)
		}
	}

	return existing, nil
}

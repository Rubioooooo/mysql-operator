package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	databasev1 "github.com/egonlin/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const statefulSetPodIndexLabel = "apps.kubernetes.io/pod-index"

type mysqlStatefulSetMember struct {
	Ordinal int32
	Pod     *corev1.Pod
}

func mysqlStatefulSetPodOrdinal(pod *corev1.Pod) (int32, error) {
	rawOrdinal, found := pod.Labels[statefulSetPodIndexLabel]
	if !found || rawOrdinal == "" {
		return 0, fmt.Errorf("Pod %s/%s is missing %s label", pod.Namespace, pod.Name, statefulSetPodIndexLabel)
	}

	ordinal, err := strconv.ParseInt(rawOrdinal, 10, 32)
	if err != nil {
		return 0, fmt.Errorf(
			"Pod %s/%s has invalid %s label %q: expected a decimal integer: %w",
			pod.Namespace,
			pod.Name,
			statefulSetPodIndexLabel,
			rawOrdinal,
			err,
		)
	}
	if ordinal < 1 {
		return 0, fmt.Errorf(
			"Pod %s/%s has invalid %s ordinal %d: ordinal must be at least 1",
			pod.Namespace,
			pod.Name,
			statefulSetPodIndexLabel,
			ordinal,
		)
	}
	if rawOrdinal != strconv.FormatInt(ordinal, 10) {
		return 0, fmt.Errorf(
			"Pod %s/%s has non-canonical %s label %q: expected %q",
			pod.Namespace,
			pod.Name,
			statefulSetPodIndexLabel,
			rawOrdinal,
			strconv.FormatInt(ordinal, 10),
		)
	}

	return int32(ordinal), nil
}

func (r *MysqlClusterReconciler) validateAndSortMysqlStatefulSetPods(
	ctx context.Context,
	pods []corev1.Pod,
	cluster *databasev1.MysqlCluster,
) ([]mysqlStatefulSetMember, error) {
	members := make([]mysqlStatefulSetMember, 0, len(pods))
	seenOrdinals := make(map[int32]string, len(pods))

	for i := range pods {
		pod := &pods[i]
		if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
			return nil, fmt.Errorf("invalid StatefulSet member Pod %s/%s: %w", pod.Namespace, pod.Name, err)
		}

		ordinal, err := mysqlStatefulSetPodOrdinal(pod)
		if err != nil {
			return nil, err
		}
		if existingPodName, duplicate := seenOrdinals[ordinal]; duplicate {
			return nil, fmt.Errorf(
				"duplicate StatefulSet member ordinal %d for Pods %s and %s",
				ordinal,
				existingPodName,
				pod.Name,
			)
		}
		seenOrdinals[ordinal] = pod.Name
		members = append(members, mysqlStatefulSetMember{Ordinal: ordinal, Pod: pod.DeepCopy()})
	}

	for i := range members {
		member := &members[i]
		expectedPodName := mysqlStatefulSetPodName(cluster, member.Ordinal)
		if member.Pod.Name != expectedPodName {
			return nil, fmt.Errorf(
				"Pod %s/%s ordinal identity does not match %s label %d: expected name %s",
				member.Pod.Namespace,
				member.Pod.Name,
				statefulSetPodIndexLabel,
				member.Ordinal,
				expectedPodName,
			)
		}
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i].Ordinal < members[j].Ordinal
	})
	return members, nil
}

func (r *MysqlClusterReconciler) listMysqlStatefulSetPods(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) ([]mysqlStatefulSetMember, error) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, mysqlClusterPodListOptions(cluster, "")...); err != nil {
		return nil, fmt.Errorf("failed to list StatefulSet member Pods for MysqlCluster %s: %w", cluster.Name, err)
	}

	return r.validateAndSortMysqlStatefulSetPods(ctx, podList.Items, cluster)
}

func mysqlStatefulSetPodHealthy(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == mysqlContainerName {
			return containerStatus.Ready
		}
	}
	return false
}

func (r *MysqlClusterReconciler) mysqlStatefulSetMembersReady(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (bool, error) {
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return false, err
	}

	desired := desiredReplicas(cluster)
	if int32(len(members)) != desired {
		return false, nil
	}
	for i, member := range members {
		expectedOrdinal := int32(i + 1)
		if member.Ordinal != expectedOrdinal {
			return false, nil
		}
		if !mysqlStatefulSetPodHealthy(member.Pod) {
			return false, nil
		}
	}

	return true, nil
}

func (r *MysqlClusterReconciler) validateNoLegacyRawPodLifecycle(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) error {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, mysqlClusterPodListOptions(cluster, "")...); err != nil {
		return fmt.Errorf("failed to inspect legacy Pods for MysqlCluster %s: %w", cluster.Name, err)
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		if metav1.IsControlledBy(pod, cluster) {
			return fmt.Errorf(
				"legacy raw-Pod lifecycle detected for MysqlCluster %s at Pod %s/%s; automatic in-place migration to StatefulSet is unsupported",
				cluster.Name,
				pod.Namespace,
				pod.Name,
			)
		}
	}

	return nil
}

func (r *MysqlClusterReconciler) validateMysqlClusterPodForRoleMutation(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
) error {
	if err := validateControlledBy(pod, cluster, "Pod"); err == nil {
		return nil
	}
	if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
		return fmt.Errorf(
			"Pod %s/%s is not safe for MysqlCluster %s role mutation: %w",
			pod.Namespace,
			pod.Name,
			cluster.Name,
			err,
		)
	}

	return nil
}

func (r *MysqlClusterReconciler) validateMysqlStatefulSetScaleDownSafety(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	currentReplicas int32,
	desiredReplicas int32,
) error {
	if desiredReplicas >= currentReplicas {
		return nil
	}

	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return err
	}

	primaries := make([]mysqlStatefulSetMember, 0, 1)
	for _, member := range members {
		canonicalRole := member.Pod.Labels[LabelMysqlRole]
		legacyRole := member.Pod.Labels[LegacyLabelRole]
		if canonicalRole == "" && legacyRole != "" {
			return fmt.Errorf(
				"Pod %s/%s has legacy MySQL role %q but is missing authoritative canonical role label %s",
				member.Pod.Namespace,
				member.Pod.Name,
				legacyRole,
				LabelMysqlRole,
			)
		}
		if canonicalRole != "" && legacyRole != "" && canonicalRole != legacyRole {
			return fmt.Errorf(
				"Pod %s/%s has conflicting MySQL role labels: %s=%q, %s=%q",
				member.Pod.Namespace,
				member.Pod.Name,
				LabelMysqlRole,
				canonicalRole,
				LegacyLabelRole,
				legacyRole,
			)
		}
		if canonicalRole == "master" {
			primaries = append(primaries, member)
		}
	}

	if len(primaries) != 1 {
		return fmt.Errorf(
			"cannot safely scale MysqlCluster %s from %d to %d replicas: expected exactly one Primary, found %d",
			cluster.Name,
			currentReplicas,
			desiredReplicas,
			len(primaries),
		)
	}

	primaryOrdinal := primaries[0].Ordinal
	if primaryOrdinal > desiredReplicas {
		return fmt.Errorf(
			"cannot safely scale MysqlCluster %s from %d to %d replicas: scale-down would remove current Primary ordinal %d",
			cluster.Name,
			currentReplicas,
			desiredReplicas,
			primaryOrdinal,
		)
	}

	return nil
}

func (r *MysqlClusterReconciler) mapMysqlStatefulSetPodToMysqlCluster(
	ctx context.Context,
	object client.Object,
) []reconcile.Request {
	pod, ok := object.(*corev1.Pod)
	if !ok {
		return nil
	}

	podController := metav1.GetControllerOf(pod)
	if podController == nil ||
		podController.APIVersion != appsv1.SchemeGroupVersion.String() ||
		podController.Kind != "StatefulSet" ||
		podController.Name == "" ||
		podController.UID == "" {
		return nil
	}

	statefulSet := &appsv1.StatefulSet{}
	key := client.ObjectKey{Namespace: pod.Namespace, Name: podController.Name}
	if err := r.Get(ctx, key, statefulSet); err != nil {
		if !apierrors.IsNotFound(err) {
			log.FromContext(ctx).Error(err, "failed to map StatefulSet Pod to MysqlCluster", "pod", client.ObjectKeyFromObject(pod), "statefulSet", key)
		}
		return nil
	}
	if statefulSet.UID != podController.UID {
		return nil
	}

	clusterController := metav1.GetControllerOf(statefulSet)
	if clusterController == nil ||
		clusterController.APIVersion != databasev1.GroupVersion.String() ||
		clusterController.Kind != "MysqlCluster" ||
		clusterController.Name == "" ||
		clusterController.UID == "" {
		return nil
	}

	return []reconcile.Request{
		{NamespacedName: client.ObjectKey{Namespace: statefulSet.Namespace, Name: clusterController.Name}},
	}
}

package controller

import (
	"context"
	"fmt"
	"strings"

	databasev1 "github.com/egonlin/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (r *MysqlClusterReconciler) ensureMysqlHeadlessService(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (*corev1.Service, error) {
	desired := desiredMysqlHeadlessService(cluster)
	existing := &corev1.Service{}
	key := client.ObjectKeyFromObject(desired)

	if err := r.Get(ctx, key, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get headless Service %s: %w", key, err)
		}

		if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
			return nil, fmt.Errorf("failed to set MysqlCluster %s as controller of Service %s: %w", cluster.Name, key, err)
		}
		if err := r.Create(ctx, desired); err != nil {
			return nil, fmt.Errorf("failed to create headless Service %s: %w", key, err)
		}
		return desired, nil
	}

	if err := validateControlledBy(existing, cluster, "Service"); err != nil {
		return nil, err
	}
	if existing.Spec.ClusterIP != corev1.ClusterIPNone {
		return nil, fmt.Errorf(
			"Service %s/%s controlled by MysqlCluster %s is not headless: clusterIP=%q",
			existing.Namespace,
			existing.Name,
			cluster.Name,
			existing.Spec.ClusterIP,
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

	if changed {
		if err := r.Update(ctx, existing); err != nil {
			return nil, fmt.Errorf("failed to update headless Service %s: %w", key, err)
		}
	}

	return existing, nil
}

func (r *MysqlClusterReconciler) ensureMysqlSharedConfigMap(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (*corev1.ConfigMap, error) {
	desired := desiredMysqlSharedConfigMap(cluster)
	existing := &corev1.ConfigMap{}
	key := client.ObjectKeyFromObject(desired)

	if err := r.Get(ctx, key, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get shared ConfigMap %s: %w", key, err)
		}

		if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
			return nil, fmt.Errorf("failed to set MysqlCluster %s as controller of ConfigMap %s: %w", cluster.Name, key, err)
		}
		if err := r.Create(ctx, desired); err != nil {
			return nil, fmt.Errorf("failed to create shared ConfigMap %s: %w", key, err)
		}
		return desired, nil
	}

	if err := validateControlledBy(existing, cluster, "ConfigMap"); err != nil {
		return nil, err
	}

	changed := false
	if !apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		existing.Labels = desired.Labels
		changed = true
	}
	if !apiequality.Semantic.DeepEqual(existing.Data, desired.Data) {
		existing.Data = desired.Data
		changed = true
	}

	if changed {
		if err := r.Update(ctx, existing); err != nil {
			return nil, fmt.Errorf("failed to update shared ConfigMap %s: %w", key, err)
		}
	}

	return existing, nil
}

func validateMysqlStatefulSetImmutableFoundation(
	existing *appsv1.StatefulSet,
	desired *appsv1.StatefulSet,
) error {
	driftError := func(field string) error {
		return fmt.Errorf(
			"StatefulSet %s/%s immutable foundation drift: %s",
			existing.Namespace,
			existing.Name,
			field,
		)
	}

	if existing.Spec.ServiceName != desired.Spec.ServiceName {
		return driftError("spec.serviceName")
	}
	if !apiequality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		return driftError("spec.selector")
	}
	if existing.Spec.Ordinals == nil || desired.Spec.Ordinals == nil || existing.Spec.Ordinals.Start != desired.Spec.Ordinals.Start {
		return driftError("spec.ordinals.start")
	}
	if existing.Spec.PodManagementPolicy != desired.Spec.PodManagementPolicy {
		return driftError("spec.podManagementPolicy")
	}

	if len(existing.Spec.VolumeClaimTemplates) != 1 || len(desired.Spec.VolumeClaimTemplates) != 1 {
		return driftError("spec.volumeClaimTemplates count")
	}
	existingClaim := &existing.Spec.VolumeClaimTemplates[0]
	desiredClaim := &desired.Spec.VolumeClaimTemplates[0]
	if existingClaim.Name != desiredClaim.Name {
		return driftError("spec.volumeClaimTemplates[0].metadata.name")
	}
	if !apiequality.Semantic.DeepEqual(existingClaim.Spec.AccessModes, desiredClaim.Spec.AccessModes) {
		return driftError("spec.volumeClaimTemplates[0].spec.accessModes")
	}
	if !apiequality.Semantic.DeepEqual(existingClaim.Spec.StorageClassName, desiredClaim.Spec.StorageClassName) {
		return driftError("spec.volumeClaimTemplates[0].spec.storageClassName")
	}

	existingStorage, existingHasStorage := existingClaim.Spec.Resources.Requests[corev1.ResourceStorage]
	desiredStorage, desiredHasStorage := desiredClaim.Spec.Resources.Requests[corev1.ResourceStorage]
	if !existingHasStorage || !desiredHasStorage || existingStorage.Cmp(desiredStorage) != 0 {
		return driftError("spec.volumeClaimTemplates[0].spec.resources.requests.storage")
	}

	return nil
}

func statefulSetPodTemplateSemanticallyEqual(
	scheme *runtime.Scheme,
	existing *appsv1.StatefulSet,
	desired *appsv1.StatefulSet,
) bool {
	existingTemplate := normalizeStatefulSetPodTemplate(scheme, &existing.Spec.Template)
	desiredTemplate := normalizeStatefulSetPodTemplate(scheme, &desired.Spec.Template)
	clearUnownedPodTemplateAPIDefaults(
		&existingTemplate,
		&desiredTemplate,
		&desired.Spec.Template,
	)

	return apiequality.Semantic.DeepEqual(
		existingTemplate,
		desiredTemplate,
	)
}

func normalizeStatefulSetPodTemplate(
	scheme *runtime.Scheme,
	template *corev1.PodTemplateSpec,
) corev1.PodTemplateSpec {
	normalized := template.DeepCopy()
	if scheme == nil {
		return *normalized
	}

	pod := &corev1.Pod{Spec: *normalized.Spec.DeepCopy()}
	scheme.Default(pod)
	normalized.Spec = pod.Spec
	return *normalized
}

func clearUnownedPodTemplateAPIDefaults(
	existing *corev1.PodTemplateSpec,
	desired *corev1.PodTemplateSpec,
	desiredContract *corev1.PodTemplateSpec,
) {
	if desiredContract.Spec.RestartPolicy == "" {
		if existing.Spec.RestartPolicy == corev1.RestartPolicyAlways {
			existing.Spec.RestartPolicy = ""
		}
		if desired.Spec.RestartPolicy == corev1.RestartPolicyAlways {
			desired.Spec.RestartPolicy = ""
		}
	}
	if desiredContract.Spec.TerminationGracePeriodSeconds == nil {
		if existing.Spec.TerminationGracePeriodSeconds != nil &&
			*existing.Spec.TerminationGracePeriodSeconds == corev1.DefaultTerminationGracePeriodSeconds {
			existing.Spec.TerminationGracePeriodSeconds = nil
		}
		if desired.Spec.TerminationGracePeriodSeconds != nil &&
			*desired.Spec.TerminationGracePeriodSeconds == corev1.DefaultTerminationGracePeriodSeconds {
			desired.Spec.TerminationGracePeriodSeconds = nil
		}
	}
	if desiredContract.Spec.DNSPolicy == "" {
		if existing.Spec.DNSPolicy == corev1.DNSClusterFirst {
			existing.Spec.DNSPolicy = ""
		}
		if desired.Spec.DNSPolicy == corev1.DNSClusterFirst {
			desired.Spec.DNSPolicy = ""
		}
	}
	if desiredContract.Spec.SecurityContext == nil {
		emptySecurityContext := &corev1.PodSecurityContext{}
		if apiequality.Semantic.DeepEqual(existing.Spec.SecurityContext, emptySecurityContext) {
			existing.Spec.SecurityContext = nil
		}
		if apiequality.Semantic.DeepEqual(desired.Spec.SecurityContext, emptySecurityContext) {
			desired.Spec.SecurityContext = nil
		}
	}
	if desiredContract.Spec.SchedulerName == "" {
		if existing.Spec.SchedulerName == corev1.DefaultSchedulerName {
			existing.Spec.SchedulerName = ""
		}
		if desired.Spec.SchedulerName == corev1.DefaultSchedulerName {
			desired.Spec.SchedulerName = ""
		}
	}

	clearUnownedContainerAPIDefaults(
		existing.Spec.InitContainers,
		desired.Spec.InitContainers,
		desiredContract.Spec.InitContainers,
	)
	clearUnownedContainerAPIDefaults(
		existing.Spec.Containers,
		desired.Spec.Containers,
		desiredContract.Spec.Containers,
	)
	for i := range desiredContract.Spec.Volumes {
		contractVolume := &desiredContract.Spec.Volumes[i]
		contractConfigMap := contractVolume.ConfigMap
		if contractConfigMap == nil || contractConfigMap.DefaultMode != nil {
			continue
		}
		existingIndex, existingFound := uniqueVolumeIndexByName(existing.Spec.Volumes, contractVolume.Name)
		desiredIndex, desiredFound := uniqueVolumeIndexByName(desired.Spec.Volumes, contractVolume.Name)
		if !existingFound || !desiredFound {
			continue
		}
		existingConfigMap := existing.Spec.Volumes[existingIndex].ConfigMap
		desiredConfigMap := desired.Spec.Volumes[desiredIndex].ConfigMap
		if existingConfigMap != nil && existingConfigMap.DefaultMode != nil &&
			*existingConfigMap.DefaultMode == corev1.ConfigMapVolumeSourceDefaultMode {
			existingConfigMap.DefaultMode = nil
		}
		if desiredConfigMap != nil && desiredConfigMap.DefaultMode != nil &&
			*desiredConfigMap.DefaultMode == corev1.ConfigMapVolumeSourceDefaultMode {
			desiredConfigMap.DefaultMode = nil
		}
	}
}

func clearUnownedContainerAPIDefaults(
	existing []corev1.Container,
	desired []corev1.Container,
	desiredContract []corev1.Container,
) {
	for i := range desiredContract {
		contractContainer := &desiredContract[i]
		existingIndex, existingFound := uniqueContainerIndexByName(existing, contractContainer.Name)
		desiredIndex, desiredFound := uniqueContainerIndexByName(desired, contractContainer.Name)
		if !existingFound || !desiredFound {
			continue
		}
		existingContainer := &existing[existingIndex]
		desiredContainer := &desired[desiredIndex]
		if contractContainer.ImagePullPolicy == "" {
			defaultPolicy := defaultImagePullPolicy(contractContainer.Image)
			if existingContainer.ImagePullPolicy == defaultPolicy {
				existingContainer.ImagePullPolicy = ""
			}
			if desiredContainer.ImagePullPolicy == defaultPolicy {
				desiredContainer.ImagePullPolicy = ""
			}
		}
		if contractContainer.TerminationMessagePath == "" {
			if existingContainer.TerminationMessagePath == corev1.TerminationMessagePathDefault {
				existingContainer.TerminationMessagePath = ""
			}
			if desiredContainer.TerminationMessagePath == corev1.TerminationMessagePathDefault {
				desiredContainer.TerminationMessagePath = ""
			}
		}
		if contractContainer.TerminationMessagePolicy == "" {
			if existingContainer.TerminationMessagePolicy == corev1.TerminationMessageReadFile {
				existingContainer.TerminationMessagePolicy = ""
			}
			if desiredContainer.TerminationMessagePolicy == corev1.TerminationMessageReadFile {
				desiredContainer.TerminationMessagePolicy = ""
			}
		}
	}
}

func defaultImagePullPolicy(image string) corev1.PullPolicy {
	if strings.Contains(image, "@") {
		return corev1.PullIfNotPresent
	}

	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon <= lastSlash || image[lastColon+1:] == "latest" {
		return corev1.PullAlways
	}
	return corev1.PullIfNotPresent
}

func uniqueContainerIndexByName(containers []corev1.Container, name string) (int, bool) {
	index := -1
	for i := range containers {
		if containers[i].Name != name {
			continue
		}
		if index != -1 {
			return -1, false
		}
		index = i
	}
	return index, index != -1
}

func uniqueVolumeIndexByName(volumes []corev1.Volume, name string) (int, bool) {
	index := -1
	for i := range volumes {
		if volumes[i].Name != name {
			continue
		}
		if index != -1 {
			return -1, false
		}
		index = i
	}
	return index, index != -1
}

func (r *MysqlClusterReconciler) ensureMysqlStatefulSet(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (*appsv1.StatefulSet, error) {
	desired := desiredMysqlStatefulSet(cluster)
	existing := &appsv1.StatefulSet{}
	key := client.ObjectKeyFromObject(desired)

	if err := r.Get(ctx, key, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get StatefulSet %s: %w", key, err)
		}

		if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
			return nil, fmt.Errorf("failed to set MysqlCluster %s as controller of StatefulSet %s: %w", cluster.Name, key, err)
		}
		if err := r.Create(ctx, desired); err != nil {
			return nil, fmt.Errorf("failed to create StatefulSet %s: %w", key, err)
		}
		return desired, nil
	}

	if err := validateControlledBy(existing, cluster, "StatefulSet"); err != nil {
		return nil, err
	}
	if err := validateMysqlStatefulSetImmutableFoundation(existing, desired); err != nil {
		return nil, err
	}

	changed := false
	if !apiequality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		existing.Labels = desired.Labels
		changed = true
	}
	if !apiequality.Semantic.DeepEqual(existing.Spec.Replicas, desired.Spec.Replicas) {
		existing.Spec.Replicas = desired.Spec.Replicas
		changed = true
	}
	if !statefulSetPodTemplateSemanticallyEqual(r.Scheme, existing, desired) {
		existing.Spec.Template = desired.Spec.Template
		changed = true
	}
	if !apiequality.Semantic.DeepEqual(existing.Spec.UpdateStrategy, desired.Spec.UpdateStrategy) {
		existing.Spec.UpdateStrategy = desired.Spec.UpdateStrategy
		changed = true
	}

	if changed {
		if err := r.Update(ctx, existing); err != nil {
			return nil, fmt.Errorf("failed to update StatefulSet %s: %w", key, err)
		}
	}

	return existing, nil
}

func (r *MysqlClusterReconciler) validateStatefulSetManagedMysqlPod(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
) error {
	if pod.Namespace != cluster.Namespace {
		return fmt.Errorf(
			"Pod %s/%s namespace does not match MysqlCluster %s namespace %s",
			pod.Namespace,
			pod.Name,
			cluster.Name,
			cluster.Namespace,
		)
	}
	if pod.Labels[LabelAppInstance] != string(cluster.UID) {
		return fmt.Errorf(
			"Pod %s/%s instance label %q does not match MysqlCluster %s UID %q",
			pod.Namespace,
			pod.Name,
			pod.Labels[LabelAppInstance],
			cluster.Name,
			cluster.UID,
		)
	}

	controllerRef := metav1.GetControllerOf(pod)
	if controllerRef == nil {
		return fmt.Errorf("Pod %s/%s has no controller owner", pod.Namespace, pod.Name)
	}
	if controllerRef.APIVersion != appsv1.SchemeGroupVersion.String() || controllerRef.Kind != "StatefulSet" {
		return fmt.Errorf(
			"Pod %s/%s controller is %s %s, expected apps/v1 StatefulSet",
			pod.Namespace,
			pod.Name,
			controllerRef.APIVersion,
			controllerRef.Kind,
		)
	}

	expectedStatefulSetName := mysqlStatefulSetName(cluster)
	if controllerRef.Name != expectedStatefulSetName {
		return fmt.Errorf(
			"Pod %s/%s controller StatefulSet %s does not match MysqlCluster %s StatefulSet %s",
			pod.Namespace,
			pod.Name,
			controllerRef.Name,
			cluster.Name,
			expectedStatefulSetName,
		)
	}

	statefulSet := &appsv1.StatefulSet{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: expectedStatefulSetName}
	if err := r.Get(ctx, key, statefulSet); err != nil {
		return fmt.Errorf("failed to get controller StatefulSet %s for Pod %s/%s: %w", key, pod.Namespace, pod.Name, err)
	}
	if controllerRef.UID != statefulSet.UID {
		return fmt.Errorf(
			"Pod %s/%s controller UID %q does not match StatefulSet %s UID %q",
			pod.Namespace,
			pod.Name,
			controllerRef.UID,
			key,
			statefulSet.UID,
		)
	}
	if err := validateControlledBy(statefulSet, cluster, "StatefulSet"); err != nil {
		return err
	}

	return nil
}

package controller

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	"github.com/go-logr/logr"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type statefulSetReconcileMemoryClient struct {
	client.Client
	objects          map[string]client.Object
	updateCount      int
	statusPatchCount int
	statusPatchError error
}

func newStatefulSetReconcileMemoryClient(objects ...client.Object) *statefulSetReconcileMemoryClient {
	memoryClient := &statefulSetReconcileMemoryClient{objects: make(map[string]client.Object)}
	for _, object := range objects {
		stored := object.DeepCopyObject().(client.Object)
		if stored.GetResourceVersion() == "" {
			stored.SetResourceVersion("1")
		}
		memoryClient.objects[memoryClient.objectKey(stored)] = stored
	}
	return memoryClient
}

func (c *statefulSetReconcileMemoryClient) objectKey(object client.Object) string {
	return fmt.Sprintf("%T/%s/%s", object, object.GetNamespace(), object.GetName())
}

func (c *statefulSetReconcileMemoryClient) getKey(key client.ObjectKey, object client.Object) string {
	return fmt.Sprintf("%T/%s/%s", object, key.Namespace, key.Name)
}

func copyStatefulSetReconcileObject(destination, source client.Object) {
	switch destination := destination.(type) {
	case *corev1.Service:
		*destination = *source.(*corev1.Service).DeepCopy()
	case *corev1.Pod:
		*destination = *source.(*corev1.Pod).DeepCopy()
	case *corev1.Secret:
		*destination = *source.(*corev1.Secret).DeepCopy()
	case *corev1.ConfigMap:
		*destination = *source.(*corev1.ConfigMap).DeepCopy()
	case *corev1.Endpoints:
		*destination = *source.(*corev1.Endpoints).DeepCopy()
	case *appsv1.StatefulSet:
		*destination = *source.(*appsv1.StatefulSet).DeepCopy()
	case *databasev1.MysqlCluster:
		*destination = *source.(*databasev1.MysqlCluster).DeepCopy()
	default:
		panic(fmt.Sprintf("unsupported memory client object type %T", destination))
	}
}

func (c *statefulSetReconcileMemoryClient) List(
	_ context.Context,
	list client.ObjectList,
	options ...client.ListOption,
) error {
	listOptions := &client.ListOptions{}
	for _, option := range options {
		option.ApplyToList(listOptions)
	}

	switch typedList := list.(type) {
	case *corev1.PodList:
		for _, object := range c.objects {
			pod, ok := object.(*corev1.Pod)
			if !ok || listOptions.Namespace != "" && pod.Namespace != listOptions.Namespace {
				continue
			}
			if listOptions.LabelSelector != nil && !listOptions.LabelSelector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			typedList.Items = append(typedList.Items, *pod.DeepCopy())
		}
	case *databasev1.MysqlClusterList:
		for _, object := range c.objects {
			cluster, ok := object.(*databasev1.MysqlCluster)
			if !ok || listOptions.Namespace != "" && cluster.Namespace != listOptions.Namespace {
				continue
			}
			typedList.Items = append(typedList.Items, *cluster.DeepCopy())
		}
	default:
		return fmt.Errorf("unsupported reconcile memory list type %T", list)
	}
	return nil
}

func (c *statefulSetReconcileMemoryClient) Get(
	_ context.Context,
	key client.ObjectKey,
	object client.Object,
	_ ...client.GetOption,
) error {
	stored, found := c.objects[c.getKey(key, object)]
	if !found {
		return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", object)}, key.Name)
	}
	copyStatefulSetReconcileObject(object, stored)
	return nil
}

func (c *statefulSetReconcileMemoryClient) Create(
	_ context.Context,
	object client.Object,
	_ ...client.CreateOption,
) error {
	key := c.objectKey(object)
	if _, found := c.objects[key]; found {
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: fmt.Sprintf("%T", object)}, object.GetName())
	}
	if object.GetResourceVersion() == "" {
		object.SetResourceVersion("1")
	}
	c.objects[key] = object.DeepCopyObject().(client.Object)
	return nil
}

func (c *statefulSetReconcileMemoryClient) Update(
	_ context.Context,
	object client.Object,
	_ ...client.UpdateOption,
) error {
	key := c.objectKey(object)
	if _, found := c.objects[key]; !found {
		return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", object)}, object.GetName())
	}
	resourceVersion, _ := strconv.Atoi(object.GetResourceVersion())
	object.SetResourceVersion(strconv.Itoa(resourceVersion + 1))
	c.objects[key] = object.DeepCopyObject().(client.Object)
	c.updateCount++
	return nil
}

type statefulSetReconcileStatusWriter struct {
	client *statefulSetReconcileMemoryClient
}

func (c *statefulSetReconcileMemoryClient) Status() client.SubResourceWriter {
	return &statefulSetReconcileStatusWriter{client: c}
}

func (w *statefulSetReconcileStatusWriter) Create(
	_ context.Context,
	_ client.Object,
	_ client.Object,
	_ ...client.SubResourceCreateOption,
) error {
	return fmt.Errorf("status create is unsupported by reconcile memory client")
}

func (w *statefulSetReconcileStatusWriter) Update(
	_ context.Context,
	object client.Object,
	_ ...client.SubResourceUpdateOption,
) error {
	return w.writeMysqlClusterStatus(object, false)
}

func (w *statefulSetReconcileStatusWriter) Patch(
	_ context.Context,
	object client.Object,
	_ client.Patch,
	_ ...client.SubResourcePatchOption,
) error {
	return w.writeMysqlClusterStatus(object, true)
}

func (w *statefulSetReconcileStatusWriter) writeMysqlClusterStatus(object client.Object, patch bool) error {
	if w.client.statusPatchError != nil {
		return w.client.statusPatchError
	}
	cluster, ok := object.(*databasev1.MysqlCluster)
	if !ok {
		return fmt.Errorf("unsupported reconcile memory status object %T", object)
	}
	key := w.client.objectKey(cluster)
	storedObject, found := w.client.objects[key]
	if !found {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "MysqlCluster"}, cluster.Name)
	}
	stored := storedObject.(*databasev1.MysqlCluster).DeepCopy()
	stored.Status = cluster.Status
	resourceVersion, _ := strconv.Atoi(stored.ResourceVersion)
	stored.ResourceVersion = strconv.Itoa(resourceVersion + 1)
	cluster.ResourceVersion = stored.ResourceVersion
	w.client.objects[key] = stored
	if patch {
		w.client.statusPatchCount++
	}
	return nil
}

func newStatefulSetReconcileTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	g := NewWithT(t)
	scheme := runtime.NewScheme()
	g.Expect(databasev1.AddToScheme(scheme)).To(Succeed())
	g.Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	return scheme
}

func newStatefulSetReconcileTestReconciler(
	t *testing.T,
	scheme *runtime.Scheme,
	objects ...client.Object,
) *MysqlClusterReconciler {
	t.Helper()
	return &MysqlClusterReconciler{
		Client: newStatefulSetReconcileMemoryClient(objects...),
		Scheme: scheme,
		Log:    logr.Discard(),
	}
}

func setControllerReferenceForTest(
	t *testing.T,
	scheme *runtime.Scheme,
	owner client.Object,
	object client.Object,
) {
	t.Helper()
	g := NewWithT(t)
	g.Expect(controllerutil.SetControllerReference(owner, object, scheme)).To(Succeed())
}

func getObjectForTest(t *testing.T, reconciler *MysqlClusterReconciler, key client.ObjectKey, object client.Object) {
	t.Helper()
	g := NewWithT(t)
	g.Expect(reconciler.Get(context.Background(), key, object)).To(Succeed())
}

func applyObservedPodTemplateAPIDefaults(statefulSet *appsv1.StatefulSet) {
	defaultMode := int32(420)
	for i := range statefulSet.Spec.Template.Spec.Volumes {
		if statefulSet.Spec.Template.Spec.Volumes[i].ConfigMap != nil {
			statefulSet.Spec.Template.Spec.Volumes[i].ConfigMap.DefaultMode = &defaultMode
		}
	}
	for i := range statefulSet.Spec.Template.Spec.InitContainers {
		container := &statefulSet.Spec.Template.Spec.InitContainers[i]
		container.ImagePullPolicy = corev1.PullIfNotPresent
		container.TerminationMessagePath = "/dev/termination-log"
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	for i := range statefulSet.Spec.Template.Spec.Containers {
		container := &statefulSet.Spec.Template.Spec.Containers[i]
		container.ImagePullPolicy = corev1.PullIfNotPresent
		container.TerminationMessagePath = "/dev/termination-log"
		container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	terminationGracePeriodSeconds := int64(30)
	statefulSet.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds = &terminationGracePeriodSeconds
	statefulSet.Spec.Template.Spec.DNSPolicy = corev1.DNSClusterFirst
	statefulSet.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{}
	statefulSet.Spec.Template.Spec.SchedulerName = corev1.DefaultSchedulerName
}

func TestStatefulSetReconciliationPrimitives(t *testing.T) {
	ctx := context.Background()

	t.Run("creates and owns a missing headless Service", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-service-create", types.UID("cluster-service-create"))
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme)

		service, err := reconciler.ensureMysqlHeadlessService(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(metav1.IsControlledBy(service, cluster)).To(BeTrue())
		g.Expect(service.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		g.Expect(service.Spec.Selector).To(Equal(mysqlStatefulSetSelectorLabels(cluster)))

		stored := &corev1.Service{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(service), stored)
		g.Expect(metav1.IsControlledBy(stored, cluster)).To(BeTrue())
	})

	t.Run("repairs owned headless Service mutable drift and preserves allocated fields", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-service-repair", types.UID("cluster-service-repair"))
		service := desiredMysqlHeadlessService(cluster)
		setControllerReferenceForTest(t, scheme, cluster, service)
		service.Labels = map[string]string{"drifted": "true"}
		service.Spec.Selector = map[string]string{"drifted": "true"}
		service.Spec.Ports = []corev1.ServicePort{{Name: "wrong", Port: 3307, TargetPort: intstr.FromInt32(3307)}}
		service.Spec.ClusterIPs = []string{corev1.ClusterIPNone}
		service.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
		policy := corev1.IPFamilyPolicySingleStack
		service.Spec.IPFamilyPolicy = &policy
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, service)

		result, err := reconciler.ensureMysqlHeadlessService(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Labels).To(Equal(mysqlIdentityLabels(cluster)))
		g.Expect(result.Spec.Selector).To(Equal(mysqlStatefulSetSelectorLabels(cluster)))
		g.Expect(result.Spec.Ports).To(Equal(desiredMysqlHeadlessService(cluster).Spec.Ports))
		g.Expect(result.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		g.Expect(result.Spec.ClusterIPs).To(Equal([]string{corev1.ClusterIPNone}))
		g.Expect(result.Spec.IPFamilies).To(Equal([]corev1.IPFamily{corev1.IPv4Protocol}))
		g.Expect(result.Spec.IPFamilyPolicy).NotTo(BeNil())
		g.Expect(*result.Spec.IPFamilyPolicy).To(Equal(corev1.IPFamilyPolicySingleStack))
	})

	t.Run("rejects a foreign headless Service without mutation", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-service-foreign", types.UID("cluster-service-foreign"))
		service := desiredMysqlHeadlessService(cluster)
		service.Labels = map[string]string{"sentinel": "unchanged"}
		before := service.DeepCopy()
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, service)

		_, err := reconciler.ensureMysqlHeadlessService(ctx, cluster)
		g.Expect(err).To(HaveOccurred())
		stored := &corev1.Service{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(service), stored)
		g.Expect(apiequality.Semantic.DeepEqual(stored.Labels, before.Labels)).To(BeTrue())
		g.Expect(apiequality.Semantic.DeepEqual(stored.Spec, before.Spec)).To(BeTrue())
	})

	t.Run("rejects an owned non-headless Service without replacement", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-service-not-headless", types.UID("cluster-service-not-headless"))
		service := desiredMysqlHeadlessService(cluster)
		setControllerReferenceForTest(t, scheme, cluster, service)
		service.UID = types.UID("non-headless-service")
		service.Spec.ClusterIP = "10.0.0.10"
		service.Spec.ClusterIPs = []string{"10.0.0.10"}
		before := service.DeepCopy()
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, service)

		_, err := reconciler.ensureMysqlHeadlessService(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("is not headless")))
		stored := &corev1.Service{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(service), stored)
		g.Expect(stored.UID).To(Equal(before.UID))
		g.Expect(stored.Spec.ClusterIP).To(Equal(before.Spec.ClusterIP))
	})

	t.Run("creates, owns, and repairs the shared ConfigMap", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-config", types.UID("cluster-config"))
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme)

		configMap, err := reconciler.ensureMysqlSharedConfigMap(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(metav1.IsControlledBy(configMap, cluster)).To(BeTrue())

		configMap.Labels = map[string]string{"drifted": "true"}
		configMap.Data = map[string]string{"my.cnf": "drifted"}
		g.Expect(reconciler.Update(ctx, configMap)).To(Succeed())

		repaired, err := reconciler.ensureMysqlSharedConfigMap(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(repaired.Labels).To(Equal(mysqlIdentityLabels(cluster)))
		g.Expect(repaired.Data).To(Equal(desiredMysqlSharedConfigMap(cluster).Data))
	})

	t.Run("rejects a foreign shared ConfigMap without mutation", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-config-foreign", types.UID("cluster-config-foreign"))
		configMap := desiredMysqlSharedConfigMap(cluster)
		configMap.Labels = map[string]string{"sentinel": "unchanged"}
		configMap.Data = map[string]string{"sentinel": "unchanged"}
		before := configMap.DeepCopy()
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, configMap)

		_, err := reconciler.ensureMysqlSharedConfigMap(ctx, cluster)
		g.Expect(err).To(HaveOccurred())
		stored := &corev1.ConfigMap{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(configMap), stored)
		g.Expect(stored.Labels).To(Equal(before.Labels))
		g.Expect(stored.Data).To(Equal(before.Data))
	})

	t.Run("creates and owns a missing StatefulSet", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-stateful-create", types.UID("cluster-stateful-create"))
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme)

		statefulSet, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(metav1.IsControlledBy(statefulSet, cluster)).To(BeTrue())
		g.Expect(statefulSet.Spec.Replicas).NotTo(BeNil())
		g.Expect(*statefulSet.Spec.Replicas).To(Equal(desiredReplicas(cluster)))

		stored := &appsv1.StatefulSet{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(statefulSet), stored)
		g.Expect(metav1.IsControlledBy(stored, cluster)).To(BeTrue())
	})

	t.Run("repairs owned StatefulSet mutable drift", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-stateful-repair", types.UID("cluster-stateful-repair"))
		statefulSet := desiredMysqlStatefulSet(cluster)
		setControllerReferenceForTest(t, scheme, cluster, statefulSet)
		replicas := int32(1)
		statefulSet.Spec.Replicas = &replicas
		statefulSet.Labels = map[string]string{"drifted": "true"}
		statefulSet.Spec.Template.Labels = map[string]string{"drifted": "true"}
		statefulSet.Spec.Template.Spec.Containers[0].Image = "example.com/mysql:wrong"
		statefulSet.Spec.Template.Spec.Containers[0].ReadinessProbe.Exec.Command[2] = "exit 1"
		statefulSet.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType}
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSet)

		repaired, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		desired := desiredMysqlStatefulSet(cluster)
		g.Expect(repaired.Labels).To(Equal(desired.Labels))
		g.Expect(repaired.Spec.Replicas).NotTo(BeNil())
		g.Expect(*repaired.Spec.Replicas).To(Equal(desiredReplicas(cluster)))
		g.Expect(repaired.Spec.Template.Spec.Containers[0].ReadinessProbe).To(Equal(desired.Spec.Template.Spec.Containers[0].ReadinessProbe))
		g.Expect(statefulSetPodTemplateSemanticallyEqual(scheme, repaired, desired)).To(BeTrue())
		g.Expect(repaired.Spec.UpdateStrategy).To(Equal(desired.Spec.UpdateStrategy))
	})

	t.Run("normalizes observed Pod API defaults without ignoring real template drift", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-stateful-normalized", types.UID("cluster-stateful-normalized"))
		desired := desiredMysqlStatefulSet(cluster)
		defaulted := desired.DeepCopy()
		applyObservedPodTemplateAPIDefaults(defaulted)

		g.Expect(statefulSetPodTemplateSemanticallyEqual(scheme, defaulted, desired)).To(BeTrue())

		testCases := []struct {
			name   string
			mutate func(*appsv1.StatefulSet)
		}{
			{
				name: "termination grace period",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					value := int64(9)
					statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds = &value
				},
			},
			{
				name: "ConfigMap default mode",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					value := int32(0600)
					statefulSet.Spec.Template.Spec.Volumes[0].ConfigMap.DefaultMode = &value
				},
			},
			{
				name: "scheduler name",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Template.Spec.SchedulerName = "phase1d-custom-scheduler"
				},
			},
			{
				name: "DNS policy",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Template.Spec.DNSPolicy = corev1.DNSDefault
				},
			},
			{
				name: "security context",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					runAsNonRoot := true
					statefulSet.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: &runAsNonRoot}
				},
			},
			{
				name: "restart policy",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
				},
			},
			{
				name: "termination message path",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Template.Spec.Containers[0].TerminationMessagePath = "/tmp/custom-termination-log"
				},
			},
			{
				name: "termination message policy",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
				},
			},
			{
				name: "image pull policy",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways
				},
			},
			{
				name: "MySQL image",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Template.Spec.Containers[0].Image = "example.com/mysql:genuine-drift"
				},
			},
			{
				name: "volume ordering",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Template.Spec.Volumes[0], statefulSet.Spec.Template.Spec.Volumes[1] =
						statefulSet.Spec.Template.Spec.Volumes[1], statefulSet.Spec.Template.Spec.Volumes[0]
				},
			},
		}
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				g := NewWithT(t)
				drifted := defaulted.DeepCopy()
				testCase.mutate(drifted)
				g.Expect(statefulSetPodTemplateSemanticallyEqual(scheme, drifted, desired)).To(BeFalse())
			})
		}
	})

	t.Run("derives strict image pull policy defaults from the desired image reference", func(t *testing.T) {
		testCases := []struct {
			name          string
			image         string
			defaultPolicy corev1.PullPolicy
			wrongPolicy   corev1.PullPolicy
		}{
			{name: "explicit latest", image: "mysql:latest", defaultPolicy: corev1.PullAlways, wrongPolicy: corev1.PullIfNotPresent},
			{name: "implicit latest", image: "mysql", defaultPolicy: corev1.PullAlways, wrongPolicy: corev1.PullIfNotPresent},
			{name: "non-latest tag", image: "mysql:5.7", defaultPolicy: corev1.PullIfNotPresent, wrongPolicy: corev1.PullAlways},
			{name: "digest", image: "mysql@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", defaultPolicy: corev1.PullIfNotPresent, wrongPolicy: corev1.PullAlways},
			{name: "registry port without tag", image: "registry.example.com:5000/mysql", defaultPolicy: corev1.PullAlways, wrongPolicy: corev1.PullIfNotPresent},
			{name: "registry port with tag", image: "registry.example.com:5000/mysql:5.7", defaultPolicy: corev1.PullIfNotPresent, wrongPolicy: corev1.PullAlways},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				g := NewWithT(t)
				scheme := newStatefulSetReconcileTestScheme(t)
				cluster := statefulSetResourceTestCluster("image-policy", types.UID("image-policy-cluster"))
				desired := desiredMysqlStatefulSet(cluster)
				desired.Spec.Template.Spec.Containers[0].Image = testCase.image
				defaulted := desired.DeepCopy()
				applyObservedPodTemplateAPIDefaults(defaulted)
				defaulted.Spec.Template.Spec.Containers[0].ImagePullPolicy = testCase.defaultPolicy

				g.Expect(defaultImagePullPolicy(testCase.image)).To(Equal(testCase.defaultPolicy))
				g.Expect(statefulSetPodTemplateSemanticallyEqual(scheme, defaulted, desired)).To(BeTrue())

				defaulted.Spec.Template.Spec.Containers[0].ImagePullPolicy = testCase.wrongPolicy
				g.Expect(statefulSetPodTemplateSemanticallyEqual(scheme, defaulted, desired)).To(BeFalse())
			})
		}
	})

	t.Run("does not update an API-defaulted StatefulSet without contract drift", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-stateful-defaulted", types.UID("cluster-stateful-defaulted"))
		statefulSet := desiredMysqlStatefulSet(cluster)
		setControllerReferenceForTest(t, scheme, cluster, statefulSet)
		applyObservedPodTemplateAPIDefaults(statefulSet)
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSet)
		memoryClient := reconciler.Client.(*statefulSetReconcileMemoryClient)

		_, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(memoryClient.updateCount).To(Equal(0))
	})

	t.Run("rejects a foreign StatefulSet without mutation", func(t *testing.T) {
		g := NewWithT(t)
		scheme := newStatefulSetReconcileTestScheme(t)
		cluster := statefulSetResourceTestCluster("ensure-stateful-foreign", types.UID("cluster-stateful-foreign"))
		statefulSet := desiredMysqlStatefulSet(cluster)
		statefulSet.UID = types.UID("foreign-statefulset")
		before := statefulSet.DeepCopy()
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSet)

		_, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		g.Expect(err).To(HaveOccurred())
		stored := &appsv1.StatefulSet{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(statefulSet), stored)
		g.Expect(stored.UID).To(Equal(before.UID))
		g.Expect(apiequality.Semantic.DeepEqual(stored.Spec, before.Spec)).To(BeTrue())
	})

	t.Run("rejects immutable StatefulSet foundation drift without replacement", func(t *testing.T) {
		testCases := []struct {
			name   string
			field  string
			mutate func(*appsv1.StatefulSet)
		}{
			{
				name:  "service name",
				field: "spec.serviceName",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.ServiceName = "different-headless"
				},
			},
			{
				name:  "selector",
				field: "spec.selector",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Selector.MatchLabels[LabelAppInstance] = "different-uid"
				},
			},
			{
				name:  "ordinal start",
				field: "spec.ordinals.start",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.Ordinals.Start = 0
				},
			},
			{
				name:  "claim count",
				field: "volumeClaimTemplates count",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.VolumeClaimTemplates = nil
				},
			},
			{
				name:  "claim name",
				field: "metadata.name",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.VolumeClaimTemplates[0].Name = "different-data"
				},
			},
			{
				name:  "claim access mode",
				field: "accessModes",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.VolumeClaimTemplates[0].Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
				},
			},
			{
				name:  "claim storage class",
				field: "storageClassName",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					storageClass := "different-storage"
					statefulSet.Spec.VolumeClaimTemplates[0].Spec.StorageClassName = &storageClass
				},
			},
			{
				name:  "claim storage request",
				field: "requests.storage",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
				},
			},
			{
				name:  "Pod management policy",
				field: "podManagementPolicy",
				mutate: func(statefulSet *appsv1.StatefulSet) {
					statefulSet.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				g := NewWithT(t)
				scheme := newStatefulSetReconcileTestScheme(t)
				cluster := statefulSetResourceTestCluster("ensure-stateful-immutable", types.UID("cluster-stateful-immutable"))
				statefulSet := desiredMysqlStatefulSet(cluster)
				statefulSet.UID = types.UID("immutable-statefulset")
				setControllerReferenceForTest(t, scheme, cluster, statefulSet)
				testCase.mutate(statefulSet)
				before := statefulSet.DeepCopy()
				reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSet)

				_, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
				g.Expect(err).To(MatchError(ContainSubstring(testCase.field)))
				stored := &appsv1.StatefulSet{}
				getObjectForTest(t, reconciler, client.ObjectKeyFromObject(statefulSet), stored)
				g.Expect(stored.UID).To(Equal(before.UID))
				g.Expect(apiequality.Semantic.DeepEqual(stored.Spec, before.Spec)).To(BeTrue())
			})
		}
	})

	t.Run("validates the Pod StatefulSet MysqlCluster ownership chain", func(t *testing.T) {
		newControlledStatefulSet := func(t *testing.T, scheme *runtime.Scheme, cluster *databasev1.MysqlCluster, uid types.UID) *appsv1.StatefulSet {
			t.Helper()
			statefulSet := desiredMysqlStatefulSet(cluster)
			statefulSet.UID = uid
			setControllerReferenceForTest(t, scheme, cluster, statefulSet)
			return statefulSet
		}
		newStatefulSetPod := func(t *testing.T, scheme *runtime.Scheme, cluster *databasev1.MysqlCluster, statefulSet *appsv1.StatefulSet) *corev1.Pod {
			t.Helper()
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      statefulSet.Name + "-1",
					Namespace: cluster.Namespace,
					Labels:    mysqlIdentityLabels(cluster),
				},
			}
			setControllerReferenceForTest(t, scheme, statefulSet, pod)
			return pod
		}

		t.Run("accepts a valid ownership chain", func(t *testing.T) {
			g := NewWithT(t)
			scheme := newStatefulSetReconcileTestScheme(t)
			cluster := statefulSetResourceTestCluster("pod-chain-valid", types.UID("cluster-pod-valid"))
			statefulSet := newControlledStatefulSet(t, scheme, cluster, types.UID("statefulset-pod-valid"))
			pod := newStatefulSetPod(t, scheme, cluster, statefulSet)
			reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSet)

			g.Expect(reconciler.validateStatefulSetManagedMysqlPod(ctx, pod, cluster)).To(Succeed())
		})

		t.Run("rejects a directly MysqlCluster-owned Pod", func(t *testing.T) {
			g := NewWithT(t)
			scheme := newStatefulSetReconcileTestScheme(t)
			cluster := statefulSetResourceTestCluster("pod-chain-direct", types.UID("cluster-pod-direct"))
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "direct-pod", Namespace: cluster.Namespace, Labels: mysqlIdentityLabels(cluster)}}
			setControllerReferenceForTest(t, scheme, cluster, pod)
			reconciler := newStatefulSetReconcileTestReconciler(t, scheme)

			g.Expect(reconciler.validateStatefulSetManagedMysqlPod(ctx, pod, cluster)).NotTo(Succeed())
		})

		t.Run("rejects a foreign-owned StatefulSet", func(t *testing.T) {
			g := NewWithT(t)
			scheme := newStatefulSetReconcileTestScheme(t)
			clusterA := statefulSetResourceTestCluster("pod-chain-foreign", types.UID("cluster-pod-a"))
			clusterB := statefulSetResourceTestCluster("pod-chain-owner-b", types.UID("cluster-pod-b"))
			statefulSet := desiredMysqlStatefulSet(clusterA)
			statefulSet.UID = types.UID("statefulset-pod-foreign")
			setControllerReferenceForTest(t, scheme, clusterB, statefulSet)
			pod := newStatefulSetPod(t, scheme, clusterA, statefulSet)
			reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSet)

			g.Expect(reconciler.validateStatefulSetManagedMysqlPod(ctx, pod, clusterA)).NotTo(Succeed())
		})

		t.Run("rejects another cluster UID label", func(t *testing.T) {
			g := NewWithT(t)
			scheme := newStatefulSetReconcileTestScheme(t)
			clusterA := statefulSetResourceTestCluster("pod-chain-label", types.UID("cluster-label-a"))
			clusterB := statefulSetResourceTestCluster("pod-chain-label-b", types.UID("cluster-label-b"))
			statefulSet := newControlledStatefulSet(t, scheme, clusterA, types.UID("statefulset-pod-label"))
			pod := newStatefulSetPod(t, scheme, clusterA, statefulSet)
			pod.Labels[LabelAppInstance] = string(clusterB.UID)
			reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSet)

			g.Expect(reconciler.validateStatefulSetManagedMysqlPod(ctx, pod, clusterA)).NotTo(Succeed())
		})

		t.Run("rejects the wrong StatefulSet name", func(t *testing.T) {
			g := NewWithT(t)
			scheme := newStatefulSetReconcileTestScheme(t)
			cluster := statefulSetResourceTestCluster("pod-chain-name", types.UID("cluster-pod-name"))
			statefulSet := newControlledStatefulSet(t, scheme, cluster, types.UID("statefulset-pod-name"))
			statefulSet.Name = "wrong-statefulset"
			pod := newStatefulSetPod(t, scheme, cluster, statefulSet)
			reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSet)

			g.Expect(reconciler.validateStatefulSetManagedMysqlPod(ctx, pod, cluster)).NotTo(Succeed())
		})

		t.Run("rejects a missing controller owner", func(t *testing.T) {
			g := NewWithT(t)
			scheme := newStatefulSetReconcileTestScheme(t)
			cluster := statefulSetResourceTestCluster("pod-chain-missing", types.UID("cluster-pod-missing"))
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unowned-pod", Namespace: cluster.Namespace, Labels: mysqlIdentityLabels(cluster)}}
			reconciler := newStatefulSetReconcileTestReconciler(t, scheme)

			g.Expect(reconciler.validateStatefulSetManagedMysqlPod(ctx, pod, cluster)).NotTo(Succeed())
		})

		t.Run("rejects another namespace", func(t *testing.T) {
			g := NewWithT(t)
			scheme := newStatefulSetReconcileTestScheme(t)
			cluster := statefulSetResourceTestCluster("pod-chain-namespace", types.UID("cluster-namespace"))
			statefulSet := newControlledStatefulSet(t, scheme, cluster, types.UID("statefulset-namespace"))
			pod := newStatefulSetPod(t, scheme, cluster, statefulSet)
			pod.Namespace = "another-namespace"
			reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSet)

			g.Expect(reconciler.validateStatefulSetManagedMysqlPod(ctx, pod, cluster)).NotTo(Succeed())
		})

		t.Run("does not accept another MysqlCluster resource in the same namespace", func(t *testing.T) {
			g := NewWithT(t)
			scheme := newStatefulSetReconcileTestScheme(t)
			clusterA := statefulSetResourceTestCluster("pod-chain-multi-a", types.UID("cluster-multi-a"))
			clusterB := statefulSetResourceTestCluster("pod-chain-multi-b", types.UID("cluster-multi-b"))
			statefulSetB := newControlledStatefulSet(t, scheme, clusterB, types.UID("statefulset-multi-b"))
			podB := newStatefulSetPod(t, scheme, clusterB, statefulSetB)
			reconciler := newStatefulSetReconcileTestReconciler(t, scheme, statefulSetB)

			g.Expect(reconciler.validateStatefulSetManagedMysqlPod(ctx, podB, clusterA)).NotTo(Succeed())
		})
	})
}

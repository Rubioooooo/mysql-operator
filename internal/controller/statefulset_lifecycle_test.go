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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type statefulSetLifecycleMemoryClient struct {
	client.Client
	objects map[string]client.Object
}

func newStatefulSetLifecycleMemoryClient(objects ...client.Object) *statefulSetLifecycleMemoryClient {
	memoryClient := &statefulSetLifecycleMemoryClient{objects: make(map[string]client.Object)}
	for _, object := range objects {
		memoryClient.objects[memoryClient.objectKey(object)] = object.DeepCopyObject().(client.Object)
	}
	return memoryClient
}

func (c *statefulSetLifecycleMemoryClient) objectKey(object client.Object) string {
	return fmt.Sprintf("%T/%s/%s", object, object.GetNamespace(), object.GetName())
}

func (c *statefulSetLifecycleMemoryClient) getKey(key client.ObjectKey, object client.Object) string {
	return fmt.Sprintf("%T/%s/%s", object, key.Namespace, key.Name)
}

func copyStatefulSetLifecycleObject(destination, source client.Object) {
	switch destination := destination.(type) {
	case *corev1.Pod:
		*destination = *source.(*corev1.Pod).DeepCopy()
	case *appsv1.StatefulSet:
		*destination = *source.(*appsv1.StatefulSet).DeepCopy()
	default:
		panic(fmt.Sprintf("unsupported lifecycle memory client object type %T", destination))
	}
}

func (c *statefulSetLifecycleMemoryClient) Get(
	_ context.Context,
	key client.ObjectKey,
	object client.Object,
	_ ...client.GetOption,
) error {
	stored, found := c.objects[c.getKey(key, object)]
	if !found {
		return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", object)}, key.Name)
	}
	copyStatefulSetLifecycleObject(object, stored)
	return nil
}

func (c *statefulSetLifecycleMemoryClient) List(
	_ context.Context,
	list client.ObjectList,
	options ...client.ListOption,
) error {
	podList, ok := list.(*corev1.PodList)
	if !ok {
		return fmt.Errorf("unsupported lifecycle memory list type %T", list)
	}
	listOptions := &client.ListOptions{}
	for _, option := range options {
		option.ApplyToList(listOptions)
	}

	for _, object := range c.objects {
		pod, ok := object.(*corev1.Pod)
		if !ok {
			continue
		}
		if listOptions.Namespace != "" && pod.Namespace != listOptions.Namespace {
			continue
		}
		if listOptions.LabelSelector != nil && !listOptions.LabelSelector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		podList.Items = append(podList.Items, *pod.DeepCopy())
	}
	return nil
}

func (c *statefulSetLifecycleMemoryClient) Update(
	_ context.Context,
	object client.Object,
	_ ...client.UpdateOption,
) error {
	key := c.objectKey(object)
	if _, found := c.objects[key]; !found {
		return apierrors.NewNotFound(schema.GroupResource{Resource: fmt.Sprintf("%T", object)}, object.GetName())
	}
	c.objects[key] = object.DeepCopyObject().(client.Object)
	return nil
}

func newStatefulSetLifecycleTestReconciler(
	t *testing.T,
	objects ...client.Object,
) (*MysqlClusterReconciler, *statefulSetLifecycleMemoryClient) {
	t.Helper()
	scheme := newStatefulSetReconcileTestScheme(t)
	memoryClient := newStatefulSetLifecycleMemoryClient(objects...)
	return &MysqlClusterReconciler{Client: memoryClient, Scheme: scheme, Log: logr.Discard()}, memoryClient
}

func controlledStatefulSetForLifecycleTest(
	t *testing.T,
	cluster *databasev1.MysqlCluster,
	uid types.UID,
) *appsv1.StatefulSet {
	t.Helper()
	scheme := newStatefulSetReconcileTestScheme(t)
	statefulSet := desiredMysqlStatefulSet(cluster)
	statefulSet.UID = uid
	setControllerReferenceForTest(t, scheme, cluster, statefulSet)
	return statefulSet
}

func statefulSetPodForLifecycleTest(
	t *testing.T,
	cluster *databasev1.MysqlCluster,
	statefulSet *appsv1.StatefulSet,
	ordinal int32,
) *corev1.Pod {
	t.Helper()
	scheme := newStatefulSetReconcileTestScheme(t)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mysqlStatefulSetPodName(cluster, ordinal),
			Namespace: cluster.Namespace,
			Labels:    mysqlIdentityLabels(cluster),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "sidecar", Ready: false},
				{Name: mysqlContainerName, Ready: true},
			},
		},
	}
	pod.Labels[statefulSetPodIndexLabel] = strconv.FormatInt(int64(ordinal), 10)
	setControllerReferenceForTest(t, scheme, statefulSet, pod)
	return pod
}

func TestStatefulSetLifecycleSafetyHelpers(t *testing.T) {
	ctx := context.Background()

	t.Run("builds unpadded StatefulSet Pod names", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("demo", types.UID("pod-name-cluster"))
		g.Expect(mysqlStatefulSetPodName(cluster, 1)).To(Equal("demo-mysql-1"))
		g.Expect(mysqlStatefulSetPodName(cluster, 12)).To(Equal("demo-mysql-12"))
		g.Expect(mysqlStatefulSetPodName(cluster, 1)).NotTo(ContainSubstring("-01"))
	})

	t.Run("parses the StatefulSet pod-index label", func(t *testing.T) {
		testCases := []struct {
			name        string
			labels      map[string]string
			expected    int32
			expectError bool
		}{
			{name: "valid", labels: map[string]string{statefulSetPodIndexLabel: "12"}, expected: 12},
			{name: "missing", labels: map[string]string{}, expectError: true},
			{name: "non numeric", labels: map[string]string{statefulSetPodIndexLabel: "abc"}, expectError: true},
			{name: "zero", labels: map[string]string{statefulSetPodIndexLabel: "0"}, expectError: true},
			{name: "negative", labels: map[string]string{statefulSetPodIndexLabel: "-1"}, expectError: true},
		}
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				g := NewWithT(t)
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ordinal-pod", Namespace: "mysql-system", Labels: testCase.labels}}
				ordinal, err := mysqlStatefulSetPodOrdinal(pod)
				if testCase.expectError {
					g.Expect(err).To(HaveOccurred())
					return
				}
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ordinal).To(Equal(testCase.expected))
			})
		}
	})

	t.Run("discovers deterministic validated StatefulSet members", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("member-discovery", types.UID("member-cluster"))
		statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("member-statefulset"))
		pod1 := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
		pod2 := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 2)
		pod3 := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 3)
		reconciler, _ := newStatefulSetLifecycleTestReconciler(t, pod3, statefulSet, pod1, pod2)

		members, err := reconciler.listMysqlStatefulSetPods(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(members).To(HaveLen(3))
		g.Expect([]int32{members[0].Ordinal, members[1].Ordinal, members[2].Ordinal}).To(Equal([]int32{1, 2, 3}))
	})

	t.Run("rejects invalid discovery candidates and duplicate ordinals", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("member-invalid", types.UID("member-invalid-cluster"))
		statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("member-invalid-statefulset"))
		validPod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
		reconciler, _ := newStatefulSetLifecycleTestReconciler(t, statefulSet)

		foreignUIDPod := validPod.DeepCopy()
		foreignUIDPod.Labels[LabelAppInstance] = "foreign-uid"
		_, err := reconciler.validateAndSortMysqlStatefulSetPods(ctx, []corev1.Pod{*foreignUIDPod}, cluster)
		g.Expect(err).To(HaveOccurred())

		foreignCluster := statefulSetResourceTestCluster("member-owner-foreign", types.UID("member-owner-foreign"))
		foreignOwnedStatefulSet := desiredMysqlStatefulSet(cluster)
		foreignOwnedStatefulSet.UID = types.UID("foreign-owned-statefulset")
		setControllerReferenceForTest(t, newStatefulSetReconcileTestScheme(t), foreignCluster, foreignOwnedStatefulSet)
		wrongOwnerPod := statefulSetPodForLifecycleTest(t, cluster, foreignOwnedStatefulSet, 1)
		wrongOwnerReconciler, _ := newStatefulSetLifecycleTestReconciler(t, foreignOwnedStatefulSet)
		_, err = wrongOwnerReconciler.validateAndSortMysqlStatefulSetPods(ctx, []corev1.Pod{*wrongOwnerPod}, cluster)
		g.Expect(err).To(HaveOccurred())

		duplicate := validPod.DeepCopy()
		duplicate.Name = "duplicate-ordinal-pod"
		duplicateReconciler, _ := newStatefulSetLifecycleTestReconciler(t, statefulSet, validPod, duplicate)
		_, err = duplicateReconciler.listMysqlStatefulSetPods(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("duplicate StatefulSet member ordinal 1")))
	})

	t.Run("evaluates desired member readiness by exact ordinal and mysql container name", func(t *testing.T) {
		newFixture := func(t *testing.T) (*databasev1.MysqlCluster, *appsv1.StatefulSet, []*corev1.Pod) {
			t.Helper()
			cluster := statefulSetResourceTestCluster("member-ready", types.UID("ready-cluster"))
			statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("ready-statefulset"))
			return cluster, statefulSet, []*corev1.Pod{
				statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1),
				statefulSetPodForLifecycleTest(t, cluster, statefulSet, 2),
				statefulSetPodForLifecycleTest(t, cluster, statefulSet, 3),
			}
		}

		t.Run("all expected members ready with reordered statuses", func(t *testing.T) {
			g := NewWithT(t)
			cluster, statefulSet, pods := newFixture(t)
			reconciler, _ := newStatefulSetLifecycleTestReconciler(t, statefulSet, pods[2], pods[0], pods[1])
			ready, err := reconciler.mysqlStatefulSetMembersReady(ctx, cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ready).To(BeTrue())
		})

		t.Run("missing ordinal", func(t *testing.T) {
			g := NewWithT(t)
			cluster, statefulSet, pods := newFixture(t)
			reconciler, _ := newStatefulSetLifecycleTestReconciler(t, statefulSet, pods[0], pods[2])
			ready, err := reconciler.mysqlStatefulSetMembersReady(ctx, cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ready).To(BeFalse())
		})

		t.Run("Pending Pod", func(t *testing.T) {
			g := NewWithT(t)
			cluster, statefulSet, pods := newFixture(t)
			pods[1].Status.Phase = corev1.PodPending
			reconciler, _ := newStatefulSetLifecycleTestReconciler(t, statefulSet, pods[0], pods[1], pods[2])
			ready, err := reconciler.mysqlStatefulSetMembersReady(ctx, cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ready).To(BeFalse())
		})

		t.Run("mysql container not ready", func(t *testing.T) {
			g := NewWithT(t)
			cluster, statefulSet, pods := newFixture(t)
			pods[1].Status.ContainerStatuses[1].Ready = false
			reconciler, _ := newStatefulSetLifecycleTestReconciler(t, statefulSet, pods[0], pods[1], pods[2])
			ready, err := reconciler.mysqlStatefulSetMembersReady(ctx, cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ready).To(BeFalse())
		})

		t.Run("extra ordinal above desired replicas", func(t *testing.T) {
			g := NewWithT(t)
			cluster, statefulSet, pods := newFixture(t)
			pod4 := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 4)
			reconciler, _ := newStatefulSetLifecycleTestReconciler(t, statefulSet, pods[0], pods[1], pods[2], pod4)
			ready, err := reconciler.mysqlStatefulSetMembersReady(ctx, cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ready).To(BeFalse())
		})
	})

	t.Run("guards legacy raw Pod lifecycle without misclassifying other Pods", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("legacy-guard", types.UID("legacy-guard-cluster"))
		scheme := newStatefulSetReconcileTestScheme(t)
		rawPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-raw", Namespace: cluster.Namespace, Labels: mysqlIdentityLabels(cluster)}}
		setControllerReferenceForTest(t, scheme, cluster, rawPod)
		rawReconciler, _ := newStatefulSetLifecycleTestReconciler(t, rawPod)
		g.Expect(rawReconciler.validateNoLegacyRawPodLifecycle(ctx, cluster)).To(MatchError(ContainSubstring("automatic in-place migration to StatefulSet is unsupported")))

		statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("legacy-guard-statefulset"))
		statefulSetPod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
		statefulReconciler, _ := newStatefulSetLifecycleTestReconciler(t, statefulSet, statefulSetPod)
		g.Expect(statefulReconciler.validateNoLegacyRawPodLifecycle(ctx, cluster)).To(Succeed())

		otherCluster := statefulSetResourceTestCluster("legacy-other", types.UID("legacy-other-cluster"))
		unrelatedPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: cluster.Namespace, Labels: mysqlIdentityLabels(otherCluster)}}
		setControllerReferenceForTest(t, scheme, otherCluster, unrelatedPod)
		unrelatedReconciler, _ := newStatefulSetLifecycleTestReconciler(t, unrelatedPod)
		g.Expect(unrelatedReconciler.validateNoLegacyRawPodLifecycle(ctx, cluster)).To(Succeed())
	})

	t.Run("allows role mutation only for the two explicit ownership models", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("role-mutation", types.UID("role-cluster"))
		statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("role-statefulset"))
		statefulSetPod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
		statefulReconciler, statefulClient := newStatefulSetLifecycleTestReconciler(t, statefulSet, statefulSetPod)
		g.Expect(statefulReconciler.labelPod(ctx, statefulSetPod.Name, "slave", *cluster)).To(Succeed())
		storedStatefulPod := &corev1.Pod{}
		g.Expect(statefulClient.Get(ctx, client.ObjectKeyFromObject(statefulSetPod), storedStatefulPod)).To(Succeed())
		g.Expect(storedStatefulPod.Labels).To(HaveKeyWithValue(LabelMysqlRole, "slave"))
		g.Expect(storedStatefulPod.Labels).To(HaveKeyWithValue(LegacyLabelRole, "slave"))

		scheme := newStatefulSetReconcileTestScheme(t)
		legacyPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "legacy-role", Namespace: cluster.Namespace, Labels: mysqlIdentityLabels(cluster)}}
		setControllerReferenceForTest(t, scheme, cluster, legacyPod)
		legacyReconciler, legacyClient := newStatefulSetLifecycleTestReconciler(t, legacyPod)
		g.Expect(legacyReconciler.labelPod(ctx, legacyPod.Name, "master", *cluster)).To(Succeed())
		storedLegacyPod := &corev1.Pod{}
		g.Expect(legacyClient.Get(ctx, client.ObjectKeyFromObject(legacyPod), storedLegacyPod)).To(Succeed())
		g.Expect(storedLegacyPod.Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
		g.Expect(storedLegacyPod.Labels).To(HaveKeyWithValue(LegacyLabelRole, "master"))

		foreignCluster := statefulSetResourceTestCluster("role-foreign", types.UID("role-foreign-cluster"))
		foreignPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foreign-role", Namespace: cluster.Namespace, Labels: mysqlIdentityLabels(cluster)}}
		setControllerReferenceForTest(t, scheme, foreignCluster, foreignPod)
		foreignReconciler, foreignClient := newStatefulSetLifecycleTestReconciler(t, foreignPod)
		g.Expect(foreignReconciler.labelPod(ctx, foreignPod.Name, "master", *cluster)).NotTo(Succeed())
		storedForeignPod := &corev1.Pod{}
		g.Expect(foreignClient.Get(ctx, client.ObjectKeyFromObject(foreignPod), storedForeignPod)).To(Succeed())
		g.Expect(storedForeignPod.Labels).NotTo(HaveKey(LabelMysqlRole))
		g.Expect(storedForeignPod.Labels).NotTo(HaveKey(LegacyLabelRole))
	})

	t.Run("protects the current Primary during scale-down", func(t *testing.T) {
		newScaleFixture := func(t *testing.T, primaryOrdinals ...int32) (*MysqlClusterReconciler, *databasev1.MysqlCluster) {
			t.Helper()
			cluster := statefulSetResourceTestCluster("scale-safety", types.UID("scale-cluster"))
			statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("scale-statefulset"))
			objects := []client.Object{statefulSet}
			for ordinal := int32(1); ordinal <= 4; ordinal++ {
				pod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, ordinal)
				for _, primaryOrdinal := range primaryOrdinals {
					if ordinal == primaryOrdinal {
						pod.Labels[LabelMysqlRole] = "master"
						pod.Labels[LegacyLabelRole] = "master"
					}
				}
				objects = append(objects, pod)
			}
			reconciler, _ := newStatefulSetLifecycleTestReconciler(t, objects...)
			return reconciler, cluster
		}
		setScalePodRoles := func(
			t *testing.T,
			reconciler *MysqlClusterReconciler,
			cluster *databasev1.MysqlCluster,
			ordinal int32,
			canonicalRole string,
			legacyRole string,
		) {
			t.Helper()
			g := NewWithT(t)
			pod := &corev1.Pod{}
			key := client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetPodName(cluster, ordinal)}
			g.Expect(reconciler.Get(ctx, key, pod)).To(Succeed())
			delete(pod.Labels, LabelMysqlRole)
			delete(pod.Labels, LegacyLabelRole)
			if canonicalRole != "" {
				pod.Labels[LabelMysqlRole] = canonicalRole
			}
			if legacyRole != "" {
				pod.Labels[LegacyLabelRole] = legacyRole
			}
			g.Expect(reconciler.Update(ctx, pod)).To(Succeed())
		}

		t.Run("allows retained Primary ordinal", func(t *testing.T) {
			g := NewWithT(t)
			reconciler, cluster := newScaleFixture(t, 2)
			g.Expect(reconciler.validateMysqlStatefulSetScaleDownSafety(ctx, cluster, 4, 3)).To(Succeed())
		})
		t.Run("rejects removed Primary ordinal", func(t *testing.T) {
			g := NewWithT(t)
			reconciler, cluster := newScaleFixture(t, 4)
			g.Expect(reconciler.validateMysqlStatefulSetScaleDownSafety(ctx, cluster, 4, 3)).To(MatchError(ContainSubstring("scale-down would remove current Primary ordinal 4")))
		})
		t.Run("rejects no Primary", func(t *testing.T) {
			g := NewWithT(t)
			reconciler, cluster := newScaleFixture(t)
			g.Expect(reconciler.validateMysqlStatefulSetScaleDownSafety(ctx, cluster, 4, 3)).To(MatchError(ContainSubstring("expected exactly one Primary, found 0")))
		})
		t.Run("rejects multiple Primaries", func(t *testing.T) {
			g := NewWithT(t)
			reconciler, cluster := newScaleFixture(t, 2, 3)
			g.Expect(reconciler.validateMysqlStatefulSetScaleDownSafety(ctx, cluster, 4, 3)).To(MatchError(ContainSubstring("expected exactly one Primary, found 2")))
		})
		t.Run("rejects legacy-only Primary identity", func(t *testing.T) {
			g := NewWithT(t)
			reconciler, cluster := newScaleFixture(t)
			setScalePodRoles(t, reconciler, cluster, 2, "", "master")
			g.Expect(reconciler.validateMysqlStatefulSetScaleDownSafety(ctx, cluster, 4, 3)).To(
				MatchError(ContainSubstring("missing authoritative canonical role label")),
			)
		})
		t.Run("accepts canonical-only retained Primary identity", func(t *testing.T) {
			g := NewWithT(t)
			reconciler, cluster := newScaleFixture(t)
			setScalePodRoles(t, reconciler, cluster, 2, "master", "")
			g.Expect(reconciler.validateMysqlStatefulSetScaleDownSafety(ctx, cluster, 4, 3)).To(Succeed())
		})
		t.Run("rejects conflicting canonical and legacy roles", func(t *testing.T) {
			g := NewWithT(t)
			reconciler, cluster := newScaleFixture(t)
			setScalePodRoles(t, reconciler, cluster, 2, "slave", "master")
			g.Expect(reconciler.validateMysqlStatefulSetScaleDownSafety(ctx, cluster, 4, 3)).To(
				MatchError(ContainSubstring("conflicting MySQL role labels")),
			)
		})
		t.Run("allows scale-up and equality without lookup", func(t *testing.T) {
			g := NewWithT(t)
			cluster := statefulSetResourceTestCluster("scale-no-lookup", types.UID("scale-no-lookup"))
			reconciler := &MysqlClusterReconciler{}
			g.Expect(reconciler.validateMysqlStatefulSetScaleDownSafety(ctx, cluster, 3, 4)).To(Succeed())
			g.Expect(reconciler.validateMysqlStatefulSetScaleDownSafety(ctx, cluster, 3, 3)).To(Succeed())
		})
	})

	t.Run("maps only the authoritative Pod StatefulSet MysqlCluster owner chain", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("mapper-cluster", types.UID("mapper-cluster-uid"))
		statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("mapper-statefulset"))
		pod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
		reconciler, _ := newStatefulSetLifecycleTestReconciler(t, statefulSet)
		g.Expect(reconciler.mapMysqlStatefulSetPodToMysqlCluster(ctx, pod)).To(Equal([]reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name}}}))

		scheme := newStatefulSetReconcileTestScheme(t)
		wrongKindPod := pod.DeepCopy()
		wrongKindPod.OwnerReferences = nil
		setControllerReferenceForTest(t, scheme, cluster, wrongKindPod)
		g.Expect(reconciler.mapMysqlStatefulSetPodToMysqlCluster(ctx, wrongKindPod)).To(BeEmpty())

		missingStatefulSetPod := pod.DeepCopy()
		missingReconciler, _ := newStatefulSetLifecycleTestReconciler(t)
		g.Expect(missingReconciler.mapMysqlStatefulSetPodToMysqlCluster(ctx, missingStatefulSetPod)).To(BeEmpty())

		unownedStatefulSet := desiredMysqlStatefulSet(cluster)
		unownedStatefulSet.UID = types.UID("mapper-unowned-statefulset")
		unownedPod := statefulSetPodForLifecycleTest(t, cluster, unownedStatefulSet, 1)
		unownedReconciler, _ := newStatefulSetLifecycleTestReconciler(t, unownedStatefulSet)
		g.Expect(unownedReconciler.mapMysqlStatefulSetPodToMysqlCluster(ctx, unownedPod)).To(BeEmpty())

		g.Expect(reconciler.mapMysqlStatefulSetPodToMysqlCluster(ctx, &corev1.ConfigMap{})).To(BeEmpty())
	})
}

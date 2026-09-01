package controller

import (
	"context"
	"errors"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestMysqlRoutingServiceReconciliation(t *testing.T) {
	ctx := context.Background()

	t.Run("builds canonical-only primary and replica selectors", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-desired", types.UID("routing-desired-uid"))

		for _, testCase := range []struct {
			name string
			role string
		}{
			{name: "primary", role: "master"},
			{name: "replica", role: "slave"},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				service := desiredMysqlRoutingService(cluster, "routing-"+testCase.name, testCase.role)
				g.Expect(service.Spec.Selector).To(Equal(map[string]string{
					LabelAppName:     mysqlAppName,
					LabelAppInstance: string(cluster.UID),
					LabelManagedBy:   mysqlManagedBy,
					LabelMysqlRole:   testCase.role,
				}))
				g.Expect(service.Spec.Selector).NotTo(HaveKey(LegacyLabelApp))
				g.Expect(service.Spec.Selector).NotTo(HaveKey(LegacyLabelRole))
			})
		}
	})

	t.Run("creates owned primary and replica routing Services", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-create", types.UID("routing-create-uid"))
		scheme := newStatefulSetReconcileTestScheme(t)
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme)

		for _, testCase := range []struct {
			name string
			role string
		}{
			{name: "routing-primary", role: "master"},
			{name: "routing-replica", role: "slave"},
		} {
			service, err := reconciler.ensureMysqlRoutingService(
				ctx,
				desiredMysqlRoutingService(cluster, testCase.name, testCase.role),
				cluster,
			)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(metav1.IsControlledBy(service, cluster)).To(BeTrue())
			g.Expect(service.Spec.Selector).To(Equal(mysqlRoutingSelectorLabels(cluster, testCase.role)))
			g.Expect(service.Spec.Ports).To(Equal([]corev1.ServicePort{{
				Name: "mysql", Protocol: corev1.ProtocolTCP, Port: 3306, TargetPort: intstr.FromInt32(3306),
			}}))
			g.Expect(service.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
		}
	})

	t.Run("repairs mutable drift while preserving API-assigned network identity", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-drift", types.UID("routing-drift-uid"))
		scheme := newStatefulSetReconcileTestScheme(t)
		service := desiredMysqlRoutingService(cluster, "routing-drift-primary", "master")
		service.Labels = map[string]string{"drifted": "true"}
		service.Spec.Selector = map[string]string{"wrong": "selector"}
		service.Spec.Ports = []corev1.ServicePort{{Name: "wrong", Port: 1234, NodePort: 31234}}
		service.Spec.Type = corev1.ServiceTypeNodePort
		service.Spec.ClusterIP = "10.96.0.25"
		service.Spec.ClusterIPs = []string{"10.96.0.25"}
		service.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
		policy := corev1.IPFamilyPolicySingleStack
		service.Spec.IPFamilyPolicy = &policy
		setControllerReferenceForTest(t, scheme, cluster, service)
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, service)

		result, err := reconciler.ensureMysqlRoutingService(
			ctx,
			desiredMysqlRoutingService(cluster, service.Name, "master"),
			cluster,
		)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Labels).To(Equal(mysqlRoleLabels(cluster, "master")))
		g.Expect(result.Spec.Selector).To(Equal(mysqlRoutingSelectorLabels(cluster, "master")))
		g.Expect(result.Spec.Ports).To(Equal(desiredMysqlRoutingService(cluster, service.Name, "master").Spec.Ports))
		g.Expect(result.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
		g.Expect(result.Spec.ClusterIP).To(Equal("10.96.0.25"))
		g.Expect(result.Spec.ClusterIPs).To(Equal([]string{"10.96.0.25"}))
		g.Expect(result.Spec.IPFamilies).To(Equal([]corev1.IPFamily{corev1.IPv4Protocol}))
		g.Expect(result.Spec.IPFamilyPolicy).To(Equal(&policy))
	})

	t.Run("rejects foreign ownership without mutation", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-foreign", types.UID("routing-foreign-uid"))
		scheme := newStatefulSetReconcileTestScheme(t)
		foreign := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "routing-foreign-primary", Namespace: cluster.Namespace},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"sentinel": "unchanged"}, Ports: []corev1.ServicePort{{Port: 3306}}},
		}
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, foreign)

		_, err := reconciler.ensureMysqlRoutingService(
			ctx,
			desiredMysqlRoutingService(cluster, foreign.Name, "master"),
			cluster,
		)
		g.Expect(err).To(MatchError(ContainSubstring("is not controlled by MysqlCluster")))
		stored := &corev1.Service{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(foreign), stored)
		g.Expect(stored.Spec.Selector).To(Equal(map[string]string{"sentinel": "unchanged"}))
		g.Expect(stored.OwnerReferences).To(BeEmpty())
	})

	t.Run("rejects owned headless routing drift without replacement", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-headless", types.UID("routing-headless-uid"))
		scheme := newStatefulSetReconcileTestScheme(t)
		headless := desiredMysqlRoutingService(cluster, "routing-headless-primary", "master")
		headless.Spec.ClusterIP = corev1.ClusterIPNone
		setControllerReferenceForTest(t, scheme, cluster, headless)
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, headless)

		_, err := reconciler.ensureMysqlRoutingService(
			ctx,
			desiredMysqlRoutingService(cluster, headless.Name, "master"),
			cluster,
		)
		g.Expect(err).To(MatchError(ContainSubstring("is headless and cannot be reconciled in place")))
		stored := &corev1.Service{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(headless), stored)
		g.Expect(stored.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
	})
}

func TestPrimaryRolePublicationOrdering(t *testing.T) {
	ctx := context.Background()
	cluster := statefulSetResourceTestCluster("role-order", types.UID("role-order-uid"))
	cluster.Spec.MasterService = "role-order-primary"
	scheme := newStatefulSetReconcileTestScheme(t)

	newPrimary := func(t *testing.T) *corev1.Pod {
		t.Helper()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "role-order-mysql-1",
				Namespace: cluster.Namespace,
				Labels:    mysqlIdentityLabels(cluster),
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: mysqlContainerName, Image: cluster.Spec.Image}}},
		}
		if err := controllerutil.SetControllerReference(cluster, pod, scheme); err != nil {
			t.Fatalf("set controller reference: %v", err)
		}
		return pod
	}

	t.Run("failed database transition does not publish primary role", func(t *testing.T) {
		g := NewWithT(t)
		pod := newPrimary(t)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(pod),
			Scheme: scheme,
			execCommandOnPodFn: func(*corev1.Pod, string) (string, error) {
				return "", errors.New("database transition failed")
			},
		}

		err := reconciler.setupMasterSlaveReplication(ctx, pod.Name, nil, *cluster)
		g.Expect(err).To(MatchError(ContainSubstring("database transition failed")))
		stored := &corev1.Pod{}
		g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
		g.Expect(stored.Labels).NotTo(HaveKey(LabelMysqlRole))
		g.Expect(stored.Labels).NotTo(HaveKey(LegacyLabelRole))
	})

	t.Run("successful database transition precedes primary role publication", func(t *testing.T) {
		g := NewWithT(t)
		pod := newPrimary(t)
		var reconciler *MysqlClusterReconciler
		reconciler = &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(pod),
			Scheme: scheme,
			execCommandOnPodFn: func(commandPod *corev1.Pod, _ string) (string, error) {
				observed := &corev1.Pod{}
				g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(commandPod), observed)).To(Succeed())
				g.Expect(observed.Labels).NotTo(HaveKey(LabelMysqlRole))
				return "", nil
			},
		}

		g.Expect(reconciler.setupMasterSlaveReplication(ctx, pod.Name, nil, *cluster)).To(Succeed())
		stored := &corev1.Pod{}
		g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
		g.Expect(stored.Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
		g.Expect(stored.Labels).To(HaveKeyWithValue(LegacyLabelRole, "master"))
	})
}

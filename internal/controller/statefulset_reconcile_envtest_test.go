package controller

import (
	"context"

	databasev1 "github.com/egonlin/api/v1"
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func cleanupStatefulSetEnvtestObject(ctx context.Context, object client.Object) {
	err := k8sClient.Delete(ctx, object)
	if apierrors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
}

func createStatefulSetEnvtestCluster(
	ctx context.Context,
	namespaceName string,
	clusterName string,
) *databasev1.MysqlCluster {
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespaceName}}
	Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
	DeferCleanup(func() {
		cleanupStatefulSetEnvtestObject(context.Background(), namespace)
	})

	cluster := validMysqlClusterForAdmission(clusterName)
	cluster.Namespace = namespaceName
	Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
	immutable := true
	credentials := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.CredentialsSecretName, Namespace: namespaceName},
		Immutable:  &immutable,
		Data: map[string][]byte{
			mysqlRootPasswordSecretKey:        []byte("envtest-root-password"),
			mysqlReplicationPasswordSecretKey: []byte("envtest-replication-password"),
		},
	}
	Expect(k8sClient.Create(ctx, credentials)).To(Succeed())
	DeferCleanup(func() {
		cleanupStatefulSetEnvtestObject(context.Background(), credentials)
	})

	stored := &databasev1.MysqlCluster{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
	Expect(stored.UID).NotTo(BeEmpty())
	DeferCleanup(func() {
		cleanupStatefulSetEnvtestObject(context.Background(), stored)
	})

	return stored
}

func statefulSetEnvtestReconciler() *MysqlClusterReconciler {
	return &MysqlClusterReconciler{
		Client: k8sClient,
		Scheme: scheme.Scheme,
		Log:    logr.Discard(),
	}
}

func copyIPFamilyPolicy(policy *corev1.IPFamilyPolicy) *corev1.IPFamilyPolicy {
	if policy == nil {
		return nil
	}
	copied := *policy
	return &copied
}

func observedIPFamilyPolicy(policy *corev1.IPFamilyPolicy) string {
	if policy == nil {
		return "<nil>"
	}
	return string(*policy)
}

func observedInt64(value *int64) interface{} {
	if value == nil {
		return "<nil>"
	}
	return *value
}

func observedInt32(value *int32) interface{} {
	if value == nil {
		return "<nil>"
	}
	return *value
}

var _ = Describe("StatefulSet reconciliation real API-server integration", func() {
	ctx := context.Background()

	It("creates and repairs the governing headless Service while preserving API fields", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b1e-service", "mysql-service")
		reconciler := statefulSetEnvtestReconciler()

		service, err := reconciler.ensureMysqlHeadlessService(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), service)
		})

		stored := &corev1.Service{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(service), stored)).To(Succeed())
		Expect(stored.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		Expect(stored.Spec.Selector).To(Equal(mysqlStatefulSetSelectorLabels(cluster)))
		Expect(metav1.IsControlledBy(stored, cluster)).To(BeTrue())
		Expect(stored.Spec.Ports).To(HaveLen(1))
		Expect(stored.Spec.Ports[0].Protocol).To(Equal(corev1.ProtocolTCP))
		Expect(stored.Spec.Ports[0].Port).To(Equal(int32(3306)))

		GinkgoWriter.Printf(
			"headless Service API fields: clusterIP=%q clusterIPs=%v ipFamilies=%v ipFamilyPolicy=%v\n",
			stored.Spec.ClusterIP,
			stored.Spec.ClusterIPs,
			stored.Spec.IPFamilies,
			observedIPFamilyPolicy(stored.Spec.IPFamilyPolicy),
		)

		beforeClusterIP := stored.Spec.ClusterIP
		beforeClusterIPs := append([]string(nil), stored.Spec.ClusterIPs...)
		beforeIPFamilies := append([]corev1.IPFamily(nil), stored.Spec.IPFamilies...)
		beforeIPFamilyPolicy := copyIPFamilyPolicy(stored.Spec.IPFamilyPolicy)

		stored.Labels = map[string]string{"drifted": "true"}
		stored.Spec.Selector = map[string]string{"drifted": "true"}
		stored.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "drifted",
				Protocol:   corev1.ProtocolTCP,
				Port:       3307,
				TargetPort: intstr.FromInt32(3307),
			},
		}
		Expect(k8sClient.Update(ctx, stored)).To(Succeed())

		_, err = reconciler.ensureMysqlHeadlessService(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		repaired := &corev1.Service{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(service), repaired)).To(Succeed())
		desired := desiredMysqlHeadlessService(cluster)
		Expect(repaired.Labels).To(Equal(desired.Labels))
		Expect(repaired.Spec.Selector).To(Equal(desired.Spec.Selector))
		Expect(apiequality.Semantic.DeepEqual(repaired.Spec.Ports, desired.Spec.Ports)).To(BeTrue())
		Expect(repaired.Spec.ClusterIP).To(Equal(beforeClusterIP))
		Expect(repaired.Spec.ClusterIPs).To(Equal(beforeClusterIPs))
		Expect(repaired.Spec.IPFamilies).To(Equal(beforeIPFamilies))
		Expect(apiequality.Semantic.DeepEqual(repaired.Spec.IPFamilyPolicy, beforeIPFamilyPolicy)).To(BeTrue())
		Expect(metav1.IsControlledBy(repaired, cluster)).To(BeTrue())
	})

	It("creates and repairs the shared ConfigMap with persistent ownership", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b1e-config", "mysql-config")
		reconciler := statefulSetEnvtestReconciler()

		configMap, err := reconciler.ensureMysqlSharedConfigMap(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), configMap)
		})

		stored := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(configMap), stored)).To(Succeed())
		Expect(metav1.IsControlledBy(stored, cluster)).To(BeTrue())

		stored.Labels = map[string]string{"drifted": "true"}
		stored.Data = map[string]string{"my.cnf": "drifted"}
		Expect(k8sClient.Update(ctx, stored)).To(Succeed())

		_, err = reconciler.ensureMysqlSharedConfigMap(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		repaired := &corev1.ConfigMap{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(configMap), repaired)).To(Succeed())
		desired := desiredMysqlSharedConfigMap(cluster)
		Expect(repaired.Labels).To(Equal(desired.Labels))
		Expect(repaired.Data).To(Equal(desired.Data))
		Expect(metav1.IsControlledBy(repaired, cluster)).To(BeTrue())
	})

	It("creates the desired StatefulSet through the real API server", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b1e-stateful-create", "mysql-stateful")
		reconciler := statefulSetEnvtestReconciler()

		statefulSet, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), statefulSet)
		})

		stored := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(statefulSet), stored)).To(Succeed())
		desired := desiredMysqlStatefulSet(cluster)
		Expect(metav1.IsControlledBy(stored, cluster)).To(BeTrue())
		Expect(stored.Spec.Replicas).NotTo(BeNil())
		Expect(*stored.Spec.Replicas).To(Equal(desiredReplicas(cluster)))
		Expect(stored.Spec.ServiceName).To(Equal(desired.Spec.ServiceName))
		Expect(stored.Spec.Ordinals).NotTo(BeNil())
		Expect(stored.Spec.Ordinals.Start).To(Equal(int32(1)))
		Expect(stored.Spec.PodManagementPolicy).To(Equal(appsv1.OrderedReadyPodManagement))
		Expect(stored.Spec.UpdateStrategy.Type).To(Equal(appsv1.OnDeleteStatefulSetStrategyType))
		Expect(apiequality.Semantic.DeepEqual(stored.Spec.Selector, desired.Spec.Selector)).To(BeTrue())
		for key, value := range mysqlStatefulSetSelectorLabels(cluster) {
			Expect(stored.Spec.Template.Labels).To(HaveKeyWithValue(key, value))
		}
		Expect(stored.Spec.VolumeClaimTemplates).To(HaveLen(1))
		claim := stored.Spec.VolumeClaimTemplates[0]
		Expect(claim.Spec.StorageClassName).NotTo(BeNil())
		Expect(*claim.Spec.StorageClassName).To(Equal(cluster.Spec.Storage.StorageClassName))
		storedStorage := claim.Spec.Resources.Requests[corev1.ResourceStorage]
		Expect(storedStorage.Cmp(cluster.Spec.Storage.Size)).To(Equal(0))

		mysqlContainer := findContainerByName(stored.Spec.Template.Spec.Containers, mysqlContainerName)
		Expect(mysqlContainer).NotTo(BeNil())
		configVolume := stored.Spec.Template.Spec.Volumes[0].ConfigMap
		GinkgoWriter.Printf(
			"StatefulSet API defaults: restartPolicy=%q dnsPolicy=%q schedulerName=%q terminationGrace=%v imagePullPolicy=%q terminationMessagePath=%q configDefaultMode=%v\n",
			stored.Spec.Template.Spec.RestartPolicy,
			stored.Spec.Template.Spec.DNSPolicy,
			stored.Spec.Template.Spec.SchedulerName,
			observedInt64(stored.Spec.Template.Spec.TerminationGracePeriodSeconds),
			mysqlContainer.ImagePullPolicy,
			mysqlContainer.TerminationMessagePath,
			observedInt32(configVolume.DefaultMode),
		)
	})

	It("does not update an API-defaulted StatefulSet without desired-state drift", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b1e-stateful-idempotent", "mysql-idempotent")
		reconciler := statefulSetEnvtestReconciler()

		statefulSet, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), statefulSet)
		})

		before := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(statefulSet), before)).To(Succeed())
		desired := desiredMysqlStatefulSet(cluster)
		Expect(before.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyAlways))
		Expect(before.Spec.Template.Spec.TerminationGracePeriodSeconds).NotTo(BeNil())
		Expect(*before.Spec.Template.Spec.TerminationGracePeriodSeconds).To(Equal(int64(30)))
		Expect(before.Spec.Template.Spec.DNSPolicy).To(Equal(corev1.DNSClusterFirst))
		Expect(before.Spec.Template.Spec.SecurityContext).To(Equal(&corev1.PodSecurityContext{}))
		Expect(before.Spec.Template.Spec.SchedulerName).To(Equal(corev1.DefaultSchedulerName))
		Expect(before.Spec.Template.Spec.Volumes[0].ConfigMap.DefaultMode).NotTo(BeNil())
		Expect(*before.Spec.Template.Spec.Volumes[0].ConfigMap.DefaultMode).To(Equal(int32(420)))
		Expect(before.Spec.Template.Spec.Containers[0].ImagePullPolicy).To(Equal(corev1.PullIfNotPresent))
		Expect(before.Spec.Template.Spec.Containers[0].TerminationMessagePath).To(Equal("/dev/termination-log"))
		Expect(before.Spec.Template.Spec.Containers[0].TerminationMessagePolicy).To(Equal(corev1.TerminationMessageReadFile))
		Expect(statefulSetPodTemplateSemanticallyEqual(reconciler.Scheme, before, desired)).To(BeTrue())
		beforeRV := before.ResourceVersion

		_, err = reconciler.ensureMysqlStatefulSet(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		after := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(statefulSet), after)).To(Succeed())
		afterRV := after.ResourceVersion

		GinkgoWriter.Printf("StatefulSet idempotency resourceVersion: before=%s after=%s\n", beforeRV, afterRV)
		Expect(afterRV).To(Equal(beforeRV))
	})

	It("repairs allowed StatefulSet mutable drift without changing the immutable foundation", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b1e-stateful-repair", "mysql-repair")
		reconciler := statefulSetEnvtestReconciler()

		statefulSet, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), statefulSet)
		})

		stored := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(statefulSet), stored)).To(Succeed())
		beforeServiceName := stored.Spec.ServiceName
		beforeSelector := stored.Spec.Selector.DeepCopy()
		beforeOrdinalStart := stored.Spec.Ordinals.Start
		beforeClaim := stored.Spec.VolumeClaimTemplates[0].DeepCopy()

		replicas := int32(1)
		stored.Spec.Replicas = &replicas
		stored.Labels = map[string]string{"drifted": "true"}
		stored.Spec.Template.Spec.Containers[0].Image = "example.com/mysql:drifted"
		Expect(k8sClient.Update(ctx, stored)).To(Succeed())

		_, err = reconciler.ensureMysqlStatefulSet(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		repaired := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(statefulSet), repaired)).To(Succeed())
		Expect(repaired.Spec.Replicas).NotTo(BeNil())
		Expect(*repaired.Spec.Replicas).To(Equal(desiredReplicas(cluster)))
		Expect(repaired.Labels).To(Equal(mysqlIdentityLabels(cluster)))
		Expect(repaired.Spec.Template.Spec.Containers[0].Image).To(Equal(cluster.Spec.Image))
		Expect(repaired.Spec.ServiceName).To(Equal(beforeServiceName))
		Expect(apiequality.Semantic.DeepEqual(repaired.Spec.Selector, beforeSelector)).To(BeTrue())
		Expect(repaired.Spec.Ordinals.Start).To(Equal(beforeOrdinalStart))
		Expect(apiequality.Semantic.DeepEqual(&repaired.Spec.VolumeClaimTemplates[0], beforeClaim)).To(BeTrue())
	})

	It("rejects a foreign StatefulSet collision without adoption or replacement", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b1e-stateful-foreign", "mysql-foreign")
		reconciler := statefulSetEnvtestReconciler()
		foreign := desiredMysqlStatefulSet(cluster)
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), foreign)
		})

		before := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreign), before)).To(Succeed())
		Expect(before.UID).NotTo(BeEmpty())
		Expect(before.OwnerReferences).To(BeEmpty())

		_, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		Expect(err).To(HaveOccurred())
		after := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreign), after)).To(Succeed())
		Expect(after.UID).To(Equal(before.UID))
		Expect(after.ResourceVersion).To(Equal(before.ResourceVersion))
		Expect(after.OwnerReferences).To(BeEmpty())
	})

	It("fails closed on an initially valid but incompatible StatefulSet foundation", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b1e-stateful-immutable", "mysql-immutable")
		reconciler := statefulSetEnvtestReconciler()
		incompatible := desiredMysqlStatefulSet(cluster)
		incompatible.Spec.ServiceName = "different-governing-service"
		Expect(controllerutil.SetControllerReference(cluster, incompatible, scheme.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, incompatible)).To(Succeed())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), incompatible)
		})

		before := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(incompatible), before)).To(Succeed())
		Expect(before.UID).NotTo(BeEmpty())

		_, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		Expect(err).To(MatchError(ContainSubstring("spec.serviceName")))
		after := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(incompatible), after)).To(Succeed())
		Expect(after.UID).To(Equal(before.UID))
		Expect(after.ResourceVersion).To(Equal(before.ResourceVersion))
		Expect(after.Spec.ServiceName).To(Equal("different-governing-service"))
		Expect(metav1.IsControlledBy(after, cluster)).To(BeTrue())
	})
})

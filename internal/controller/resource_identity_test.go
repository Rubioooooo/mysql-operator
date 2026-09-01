package controller

import (
	"context"

	databasev1 "github.com/egonlin/api/v1"
	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func cleanupIdentityObject(ctx context.Context, object client.Object) {
	err := k8sClient.Delete(ctx, object)
	if apierrors.IsNotFound(err) {
		return
	}
	Expect(err).NotTo(HaveOccurred())
}

func createIdentityTestCluster(ctx context.Context, name string) *databasev1.MysqlCluster {
	cluster := validMysqlClusterForAdmission(name)
	Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
	DeferCleanup(func() {
		cleanupIdentityObject(context.Background(), cluster)
	})
	return cluster
}

func expectMysqlClusterController(object metav1.Object, cluster *databasev1.MysqlCluster) {
	controllerRef := metav1.GetControllerOf(object)
	Expect(controllerRef).NotTo(BeNil())
	Expect(controllerRef.APIVersion).To(Equal("apps.egonlin.com/v1"))
	Expect(controllerRef.Kind).To(Equal("MysqlCluster"))
	Expect(controllerRef.Name).To(Equal(cluster.Name))
	Expect(controllerRef.UID).To(Equal(cluster.UID))
	Expect(controllerRef.Controller).NotTo(BeNil())
	Expect(*controllerRef.Controller).To(BeTrue())
}

var _ = Describe("MysqlCluster resource identity", func() {
	ctx := context.Background()

	It("isolates deterministic child names by cluster name", func() {
		clusterA := &databasev1.MysqlCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-a"}}
		clusterB := &databasev1.MysqlCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster-b"}}

		Expect(mysqlPodName(clusterA, 1)).To(Equal("cluster-a-mysql-01"))
		Expect(mysqlPVCName(clusterA, 1)).To(Equal("cluster-a-mysql-01"))
		Expect(mysqlConfigMapName(clusterA, 1)).To(Equal("cluster-a-mysql-config-01"))
		Expect(mysqlPodName(clusterA, 1)).NotTo(Equal(mysqlPodName(clusterB, 1)))
		Expect(mysqlPVCName(clusterA, 1)).NotTo(Equal(mysqlPVCName(clusterB, 1)))
		Expect(mysqlConfigMapName(clusterA, 1)).NotTo(Equal(mysqlConfigMapName(clusterB, 1)))
	})

	It("keeps generated child names within the Kubernetes DNS subdomain limit", func() {
		longName := ""
		for len(longName) < 253 {
			longName += "a"
		}
		longName = longName[:253]

		cluster := &databasev1.MysqlCluster{
			ObjectMeta: metav1.ObjectMeta{Name: longName},
		}

		podName := mysqlPodName(cluster, 1)
		pvcName := mysqlPVCName(cluster, 1)
		configMapName := mysqlConfigMapName(cluster, 1)

		Expect(len(podName)).To(BeNumerically("<=", maxChildResourceNameLength))
		Expect(len(pvcName)).To(BeNumerically("<=", maxChildResourceNameLength))
		Expect(len(configMapName)).To(BeNumerically("<=", maxChildResourceNameLength))

		Expect(podName).To(Equal(mysqlPodName(cluster, 1)))
		Expect(configMapName).To(Equal(mysqlConfigMapName(cluster, 1)))

		other := &databasev1.MysqlCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name: longName[:252] + "b",
			},
		}

		Expect(mysqlPodName(cluster, 1)).NotTo(Equal(mysqlPodName(other, 1)))
		Expect(mysqlConfigMapName(cluster, 1)).NotTo(Equal(mysqlConfigMapName(other, 1)))
	})

	It("uses the MysqlCluster UID as the instance identity", func() {
		clusterA := &databasev1.MysqlCluster{ObjectMeta: metav1.ObjectMeta{UID: types.UID("uid-a")}}
		clusterB := &databasev1.MysqlCluster{ObjectMeta: metav1.ObjectMeta{UID: types.UID("uid-b")}}

		labelsA := mysqlIdentityLabels(clusterA)
		labelsB := mysqlIdentityLabels(clusterB)

		Expect(labelsA[LabelAppName]).To(Equal("mysql"))
		Expect(labelsA[LabelManagedBy]).To(Equal("mysql-operator"))
		Expect(labelsA[LegacyLabelApp]).To(Equal("mysql"))
		Expect(labelsA[LabelAppInstance]).To(Equal("uid-a"))
		Expect(labelsB[LabelAppInstance]).To(Equal("uid-b"))
		Expect(labelsA[LabelAppInstance]).NotTo(Equal(labelsB[LabelAppInstance]))
	})

	It("sets native controller ownership on the legacy routing Service", func() {
		cluster := createIdentityTestCluster(ctx, "identity-owner")
		reconciler := &MysqlClusterReconciler{Client: k8sClient, Scheme: scheme.Scheme, Log: logr.Discard()}

		service, err := reconciler.getOrCreateService(ctx, cluster.Spec.MasterService, "master", cluster.Namespace, cluster)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { cleanupIdentityObject(context.Background(), service) })
		expectMysqlClusterController(service, cluster)
		Expect(service.Spec.Selector).To(HaveKeyWithValue(LabelAppInstance, string(cluster.UID)))
		Expect(service.Spec.Selector).To(HaveKeyWithValue(LabelMysqlRole, "master"))
		Expect(service.Spec.Selector).To(HaveKeyWithValue(LegacyLabelApp, "mysql"))
		Expect(service.Spec.Selector).To(HaveKeyWithValue(LegacyLabelRole, "master"))
	})

	It("rejects a foreign Service collision without mutation", func() {
		cluster := createIdentityTestCluster(ctx, "identity-collision")
		reconciler := &MysqlClusterReconciler{Client: k8sClient, Scheme: scheme.Scheme, Log: logr.Discard()}

		foreignService := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.SlaveService, Namespace: cluster.Namespace},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{"sentinel": "unchanged"},
				Ports:    []corev1.ServicePort{{Port: 3306}},
			},
		}
		Expect(k8sClient.Create(ctx, foreignService)).To(Succeed())
		DeferCleanup(func() { cleanupIdentityObject(context.Background(), foreignService) })

		_, err := reconciler.getOrCreateService(ctx, foreignService.Name, "slave", cluster.Namespace, cluster)
		Expect(err).To(MatchError(ContainSubstring("Service default/" + foreignService.Name)))
		Expect(err).To(MatchError(ContainSubstring("MysqlCluster " + cluster.Name)))
		storedService := &corev1.Service{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreignService), storedService)).To(Succeed())
		Expect(storedService.Spec.Selector).To(Equal(map[string]string{"sentinel": "unchanged"}))
		Expect(storedService.Labels).To(BeEmpty())
		Expect(storedService.OwnerReferences).To(BeEmpty())
	})

	It("lists only Pods carrying the current cluster UID", func() {
		clusterA := createIdentityTestCluster(ctx, "identity-list-a")
		clusterB := createIdentityTestCluster(ctx, "identity-list-b")
		reconciler := &MysqlClusterReconciler{Client: k8sClient, Scheme: scheme.Scheme, Log: logr.Discard()}

		podA := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: mysqlPodName(clusterA, 1), Namespace: clusterA.Namespace, Labels: mysqlIdentityLabels(clusterA)},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:5.7"}}},
		}
		Expect(controllerutil.SetControllerReference(clusterA, podA, scheme.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, podA)).To(Succeed())
		DeferCleanup(func() { cleanupIdentityObject(context.Background(), podA) })

		podB := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: mysqlPodName(clusterB, 1), Namespace: clusterB.Namespace, Labels: mysqlIdentityLabels(clusterB)},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:5.7"}}},
		}
		Expect(controllerutil.SetControllerReference(clusterB, podB, scheme.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, podB)).To(Succeed())
		DeferCleanup(func() { cleanupIdentityObject(context.Background(), podB) })

		count, names := reconciler.getActualReplicaInfo(ctx, *clusterA)
		Expect(count).To(Equal(int32(1)))
		Expect(names).To(ConsistOf(podA.Name))
	})

	It("updates both role labels only on controlled Pods", func() {
		cluster := createIdentityTestCluster(ctx, "identity-label")
		reconciler := &MysqlClusterReconciler{Client: k8sClient, Scheme: scheme.Scheme, Log: logr.Discard()}

		controlledPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      mysqlPodName(cluster, 1),
				Namespace: cluster.Namespace,
				Labels:    mysqlIdentityLabels(cluster),
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:5.7"}}},
		}
		controlledPod.Labels["preserved"] = "true"
		Expect(controllerutil.SetControllerReference(cluster, controlledPod, scheme.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, controlledPod)).To(Succeed())
		DeferCleanup(func() { cleanupIdentityObject(context.Background(), controlledPod) })

		Expect(reconciler.labelPod(ctx, controlledPod.Name, "master", *cluster)).To(Succeed())
		storedControlledPod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(controlledPod), storedControlledPod)).To(Succeed())
		Expect(storedControlledPod.Labels).To(HaveKeyWithValue(LegacyLabelRole, "master"))
		Expect(storedControlledPod.Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
		Expect(storedControlledPod.Labels).To(HaveKeyWithValue(LabelAppInstance, string(cluster.UID)))
		Expect(storedControlledPod.Labels).To(HaveKeyWithValue("preserved", "true"))

		foreignPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "identity-label-foreign",
				Namespace: cluster.Namespace,
				Labels:    map[string]string{"sentinel": "unchanged"},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mysql", Image: "mysql:5.7"}}},
		}
		Expect(k8sClient.Create(ctx, foreignPod)).To(Succeed())
		DeferCleanup(func() { cleanupIdentityObject(context.Background(), foreignPod) })

		err := reconciler.labelPod(ctx, foreignPod.Name, "slave", *cluster)
		Expect(err).To(MatchError(ContainSubstring("Pod default/" + foreignPod.Name)))
		storedForeignPod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreignPod), storedForeignPod)).To(Succeed())
		Expect(storedForeignPod.Labels).To(Equal(map[string]string{"sentinel": "unchanged"}))
	})
})

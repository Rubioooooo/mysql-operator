package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func configureStatefulSetRuntimeCluster(
	ctx context.Context,
	namespaceName string,
	clusterName string,
	replicas int32,
	initialized bool,
) *databasev1.MysqlCluster {
	cluster := createStatefulSetEnvtestCluster(ctx, namespaceName, clusterName)
	cluster.Spec.Replicas = &replicas
	if initialized {
		cluster.Annotations = map[string]string{"initialized": "true"}
	} else {
		cluster.Annotations = nil
	}
	Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
	return cluster
}

func deferStatefulSetRuntimeResourceCleanup(cluster *databasev1.MysqlCluster) {
	DeferCleanup(func() {
		ctx := context.Background()
		objects := []client.Object{
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.MasterService, Namespace: cluster.Namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.SlaveService, Namespace: cluster.Namespace}},
			&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: mysqlHeadlessServiceName(cluster), Namespace: cluster.Namespace}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: mysqlSharedConfigMapName(cluster), Namespace: cluster.Namespace}},
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: mysqlStatefulSetName(cluster), Namespace: cluster.Namespace}},
		}
		for _, object := range objects {
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
				Expect(apierrors.IsNotFound(err)).To(BeTrue(), "unexpected cleanup get error: %v", err)
				continue
			}
			cleanupStatefulSetEnvtestObject(ctx, object)
		}
	})
}

func createStatefulSetRuntimeWorkload(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	currentReplicas int32,
) *appsv1.StatefulSet {
	statefulSet := desiredMysqlStatefulSet(cluster)
	statefulSet.Spec.Replicas = &currentReplicas
	Expect(controllerutil.SetControllerReference(cluster, statefulSet, scheme.Scheme)).To(Succeed())
	Expect(k8sClient.Create(ctx, statefulSet)).To(Succeed())
	stored := &appsv1.StatefulSet{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(statefulSet), stored)).To(Succeed())
	Expect(stored.UID).NotTo(BeEmpty())
	return stored
}

func createStatefulSetRuntimePods(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	statefulSet *appsv1.StatefulSet,
	count int32,
	primaryOrdinal int32,
) {
	for ordinal := int32(1); ordinal <= count; ordinal++ {
		role := "slave"
		if ordinal == primaryOrdinal {
			role = "master"
		}
		createStatefulSetRuntimePod(ctx, cluster, statefulSet, ordinal, role)
	}
}

func createStatefulSetRuntimePod(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	statefulSet *appsv1.StatefulSet,
	ordinal int32,
	role string,
) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mysqlStatefulSetPodName(cluster, ordinal),
			Namespace: cluster.Namespace,
			Labels:    mysqlIdentityLabels(cluster),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: mysqlContainerName, Image: cluster.Spec.Image}},
		},
	}
	pod.Labels[statefulSetPodIndexLabel] = fmt.Sprintf("%d", ordinal)
	pod.Labels[LabelMysqlRole] = role
	pod.Labels[LegacyLabelRole] = role
	Expect(controllerutil.SetControllerReference(statefulSet, pod, scheme.Scheme)).To(Succeed())
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	DeferCleanup(func() {
		cleanupStatefulSetEnvtestObject(context.Background(), pod)
	})

	stored := &corev1.Pod{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
	stored.Status.Phase = corev1.PodRunning
	stored.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  mysqlContainerName,
		Ready: true,
	}}
	Expect(k8sClient.Status().Update(ctx, stored)).To(Succeed())
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(stored), stored)).To(Succeed())
	Expect(mysqlStatefulSetPodHealthy(stored)).To(BeTrue())
	return stored
}

func expectStatefulSetRuntimeTransition(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	lastConverged int32,
	fromReplicas int32,
	targetReplicas int32,
) *databasev1.MysqlCluster {
	stored := &databasev1.MysqlCluster{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
	Expect(stored.Status.LastConvergedReplicas).NotTo(BeNil())
	Expect(*stored.Status.LastConvergedReplicas).To(Equal(lastConverged))
	Expect(stored.Status.ReplicaTransition).To(Equal(&databasev1.MysqlClusterReplicaTransitionStatus{
		FromReplicas:   fromReplicas,
		TargetReplicas: targetReplicas,
	}))
	return stored
}

func expectStatefulSetRuntimeReplicas(
	ctx context.Context,
	statefulSet *appsv1.StatefulSet,
	expected int32,
) *appsv1.StatefulSet {
	stored := &appsv1.StatefulSet{}
	Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(statefulSet), stored)).To(Succeed())
	Expect(stored.Spec.Replicas).NotTo(BeNil())
	Expect(*stored.Spec.Replicas).To(Equal(expected))
	return stored
}

func createLegacyRuntimePod(ctx context.Context, cluster *databasev1.MysqlCluster, name string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: cluster.Namespace,
			Labels:    mysqlIdentityLabels(cluster),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: mysqlContainerName, Image: cluster.Spec.Image}},
		},
	}
	Expect(controllerutil.SetControllerReference(cluster, pod, scheme.Scheme)).To(Succeed())
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	DeferCleanup(func() {
		cleanupStatefulSetEnvtestObject(context.Background(), pod)
	})
	return pod
}

func expectRuntimeObjectNotFound(ctx context.Context, key client.ObjectKey, object client.Object) {
	err := k8sClient.Get(ctx, key, object)
	Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected %T %s to be absent, got: %v", object, key, err)
}

var _ = Describe("StatefulSet runtime cutover real API-server integration", func() {
	ctx := context.Background()

	It("patches initialized on a stale object while preserving concurrent annotations", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b2b-init-patch", "mysql-init-patch")
		reconciler := statefulSetEnvtestReconciler()

		stale := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stale)).To(Succeed())
		Expect(stale.Annotations).NotTo(HaveKey("initialized"))

		concurrent := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), concurrent)).To(Succeed())
		if concurrent.Annotations == nil {
			concurrent.Annotations = make(map[string]string)
		}
		concurrent.Annotations["unrelated"] = "preserved"
		Expect(k8sClient.Update(ctx, concurrent)).To(Succeed())

		Expect(reconciler.markMysqlClusterInitialized(ctx, stale)).To(Succeed())
		stored := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
		Expect(stored.Annotations).To(HaveKeyWithValue("initialized", "true"))
		Expect(stored.Annotations).To(HaveKeyWithValue("unrelated", "preserved"))

		beforeNoOpRV := stored.ResourceVersion
		Expect(reconciler.markMysqlClusterInitialized(ctx, stored)).To(Succeed())
		afterNoOp := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), afterNoOp)).To(Succeed())
		Expect(afterNoOp.ResourceVersion).To(Equal(beforeNoOpRV))
	})

	It("creates only asynchronous StatefulSet foundation for a fresh cluster", func() {
		cluster := configureStatefulSetRuntimeCluster(ctx, "b2b-fresh", "mysql-fresh", 3, false)
		deferStatefulSetRuntimeResourceCleanup(cluster)
		reconciler := statefulSetEnvtestReconciler()

		result, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(ctrl.Result{}))
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())

		storedCluster := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), storedCluster)).To(Succeed())
		Expect(storedCluster.Annotations).NotTo(HaveKey("initialized"))

		for _, serviceName := range []string{cluster.Spec.MasterService, cluster.Spec.SlaveService, mysqlHeadlessServiceName(cluster)} {
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: serviceName}, &corev1.Service{})).To(Succeed())
		}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlSharedConfigMapName(cluster)}, &corev1.ConfigMap{})).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetName(cluster)}, &appsv1.StatefulSet{})).To(Succeed())

		pods := &corev1.PodList{}
		Expect(k8sClient.List(ctx, pods, mysqlClusterPodListOptions(cluster, "")...)).To(Succeed())
		Expect(pods.Items).To(BeEmpty())
		for ordinal := int32(1); ordinal <= desiredReplicas(cluster); ordinal++ {
			expectRuntimeObjectNotFound(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlPVCName(cluster, int(ordinal))}, &corev1.PersistentVolumeClaim{})
			expectRuntimeObjectNotFound(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlConfigMapName(cluster, int(ordinal))}, &corev1.ConfigMap{})
		}
	})

	It("fails closed on uninitialized legacy Raw Pod collision before workload creation", func() {
		cluster := configureStatefulSetRuntimeCluster(ctx, "b2b-legacy-new", "mysql-legacy-new", 3, false)
		deferStatefulSetRuntimeResourceCleanup(cluster)
		createLegacyRuntimePod(ctx, cluster, "legacy-uninitialized")
		reconciler := statefulSetEnvtestReconciler()

		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
		Expect(err).To(MatchError(ContainSubstring("automatic in-place migration to StatefulSet is unsupported")))
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())

		expectRuntimeObjectNotFound(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlHeadlessServiceName(cluster)}, &corev1.Service{})
		expectRuntimeObjectNotFound(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlSharedConfigMapName(cluster)}, &corev1.ConfigMap{})
		expectRuntimeObjectNotFound(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetName(cluster)}, &appsv1.StatefulSet{})
	})

	It("fails closed on initialized legacy Raw Pod collision before parallel workload creation", func() {
		cluster := configureStatefulSetRuntimeCluster(ctx, "b2b-legacy-old", "mysql-legacy-old", 3, true)
		deferStatefulSetRuntimeResourceCleanup(cluster)
		createLegacyRuntimePod(ctx, cluster, "legacy-initialized")
		reconciler := statefulSetEnvtestReconciler()

		_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
		Expect(err).To(MatchError(ContainSubstring("automatic in-place migration to StatefulSet is unsupported")))
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())
		expectRuntimeObjectNotFound(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetName(cluster)}, &appsv1.StatefulSet{})
	})

	It("bootstraps scale-up status before mutation and waits for the new Pod before domain entry", func() {
		cluster := configureStatefulSetRuntimeCluster(ctx, "b2b-scale-up", "mysql-scale-up", 3, true)
		deferStatefulSetRuntimeResourceCleanup(cluster)
		statefulSet := createStatefulSetRuntimeWorkload(ctx, cluster, 2)
		createStatefulSetRuntimePods(ctx, cluster, statefulSet, 2, 1)
		reconciler := statefulSetEnvtestReconciler()
		request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}

		By("persisting the legacy compatibility checkpoint and transition before child mutation")
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())
		expectStatefulSetRuntimeTransition(ctx, cluster, 2, 2, 3)
		statefulSet = expectStatefulSetRuntimeReplicas(ctx, statefulSet, 2)

		By("mutating the StatefulSet only on the next reconciliation")
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())
		expectStatefulSetRuntimeTransition(ctx, cluster, 2, 2, 3)
		statefulSet = expectStatefulSetRuntimeReplicas(ctx, statefulSet, 3)
		ordinal3Key := client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetPodName(cluster, 3)}
		expectRuntimeObjectNotFound(ctx, ordinal3Key, &corev1.Pod{})

		By("simulating StatefulSet-controller creation and readiness of ordinal 3")
		createStatefulSetRuntimePod(ctx, cluster, statefulSet, 3, "slave")

		By("entering domain reconciliation only after the scale-up delta is ready")
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())
		observedCluster := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), observedCluster)).To(Succeed())
		Expect(observedCluster.Status.HA).NotTo(BeNil())
		Expect(observedCluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
		expectStatefulSetRuntimeTransition(ctx, cluster, 2, 2, 3)
		expectStatefulSetRuntimeReplicas(ctx, statefulSet, 3)
	})

	It("bootstraps safe scale-down status and waits for removed Pod deletion before domain entry", func() {
		cluster := configureStatefulSetRuntimeCluster(ctx, "b2b-scale-down-safe", "mysql-down-safe", 3, true)
		deferStatefulSetRuntimeResourceCleanup(cluster)
		statefulSet := createStatefulSetRuntimeWorkload(ctx, cluster, 4)
		createStatefulSetRuntimePods(ctx, cluster, statefulSet, 4, 2)
		reconciler := statefulSetEnvtestReconciler()
		request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}

		By("persisting the legacy compatibility checkpoint and transition before child mutation")
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())
		expectStatefulSetRuntimeTransition(ctx, cluster, 4, 4, 3)
		statefulSet = expectStatefulSetRuntimeReplicas(ctx, statefulSet, 4)

		By("running primary safety and reducing the StatefulSet on the next reconciliation")
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())
		expectStatefulSetRuntimeTransition(ctx, cluster, 4, 4, 3)
		statefulSet = expectStatefulSetRuntimeReplicas(ctx, statefulSet, 3)
		ordinal4 := &corev1.Pod{}
		ordinal4Key := client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetPodName(cluster, 4)}
		Expect(k8sClient.Get(ctx, ordinal4Key, ordinal4)).To(Succeed())

		By("simulating StatefulSet-controller deletion of ordinal 4")
		Expect(k8sClient.Delete(ctx, ordinal4)).To(Succeed())
		expectRuntimeObjectNotFound(ctx, ordinal4Key, &corev1.Pod{})

		By("entering domain reconciliation only after the scale-down delta is gone")
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())
		observedCluster := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), observedCluster)).To(Succeed())
		Expect(observedCluster.Status.HA).NotTo(BeNil())
		Expect(observedCluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
		expectStatefulSetRuntimeTransition(ctx, cluster, 4, 4, 3)
		expectStatefulSetRuntimeReplicas(ctx, statefulSet, 3)
	})

	It("persists unsafe scale-down intent before primary safety blocks StatefulSet mutation", func() {
		cluster := configureStatefulSetRuntimeCluster(ctx, "b2b-scale-down-unsafe", "mysql-down-unsafe", 3, true)
		deferStatefulSetRuntimeResourceCleanup(cluster)
		statefulSet := createStatefulSetRuntimeWorkload(ctx, cluster, 4)
		createStatefulSetRuntimePods(ctx, cluster, statefulSet, 4, 4)
		reconciler := statefulSetEnvtestReconciler()
		request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}

		By("persisting non-destructive transition intent on the first reconciliation")
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())
		expectStatefulSetRuntimeTransition(ctx, cluster, 4, 4, 3)
		statefulSet = expectStatefulSetRuntimeReplicas(ctx, statefulSet, 4)

		By("rejecting primary removal before child mutation on the second reconciliation")
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).To(MatchError(ContainSubstring("scale-down would remove current Primary ordinal 4")))
		Expect(reconciler.SnapGoIsEnabled).To(BeFalse())
		expectStatefulSetRuntimeTransition(ctx, cluster, 4, 4, 3)
		expectStatefulSetRuntimeReplicas(ctx, statefulSet, 4)
	})
})

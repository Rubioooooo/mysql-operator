package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ = Describe("StatefulSet lifecycle helpers real API-server integration", func() {
	ctx := context.Background()

	It("discovers, maps, and safely role-labels a persisted StatefulSet-owned Pod", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b2a-stateful-pod", "mysql-lifecycle")
		reconciler := statefulSetEnvtestReconciler()

		createdStatefulSet, err := reconciler.ensureMysqlStatefulSet(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), createdStatefulSet)
		})

		statefulSet := &appsv1.StatefulSet{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(createdStatefulSet), statefulSet)).To(Succeed())
		Expect(statefulSet.UID).NotTo(BeEmpty())

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      mysqlStatefulSetPodName(cluster, 1),
				Namespace: cluster.Namespace,
				Labels:    mysqlIdentityLabels(cluster),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: mysqlContainerName, Image: cluster.Spec.Image}},
			},
		}
		pod.Labels[statefulSetPodIndexLabel] = "1"
		Expect(controllerutil.SetControllerReference(statefulSet, pod, reconciler.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), pod)
		})

		storedPod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), storedPod)).To(Succeed())
		members, err := reconciler.listMysqlStatefulSetPods(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		Expect(members).To(HaveLen(1))
		Expect(members[0].Ordinal).To(Equal(int32(1)))
		Expect(members[0].Pod.UID).To(Equal(storedPod.UID))

		requests := reconciler.mapMysqlStatefulSetPodToMysqlCluster(ctx, storedPod)
		Expect(requests).To(Equal([]reconcile.Request{
			{NamespacedName: client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Name}},
		}))

		Expect(reconciler.labelPod(ctx, storedPod.Name, "slave", *cluster)).To(Succeed())
		labeledPod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), labeledPod)).To(Succeed())
		Expect(labeledPod.Labels).To(HaveKeyWithValue(LabelMysqlRole, "slave"))
		Expect(labeledPod.Labels).To(HaveKeyWithValue(LegacyLabelRole, "slave"))
	})

	It("detects a persisted legacy raw Pod while retaining transitional role mutation", func() {
		cluster := createStatefulSetEnvtestCluster(ctx, "b2a-legacy-pod", "mysql-legacy")
		reconciler := statefulSetEnvtestReconciler()

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "legacy-raw-pod",
				Namespace: cluster.Namespace,
				Labels:    mysqlIdentityLabels(cluster),
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: mysqlContainerName, Image: cluster.Spec.Image}},
			},
		}
		Expect(controllerutil.SetControllerReference(cluster, pod, reconciler.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() {
			cleanupStatefulSetEnvtestObject(context.Background(), pod)
		})

		Expect(reconciler.validateNoLegacyRawPodLifecycle(ctx, cluster)).To(
			MatchError(ContainSubstring("automatic in-place migration to StatefulSet is unsupported")),
		)
		Expect(reconciler.labelPod(ctx, pod.Name, "master", *cluster)).To(Succeed())

		stored := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
		Expect(metav1.IsControlledBy(stored, cluster)).To(BeTrue())
		Expect(stored.Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
		Expect(stored.Labels).To(HaveKeyWithValue(LegacyLabelRole, "master"))
	})
})

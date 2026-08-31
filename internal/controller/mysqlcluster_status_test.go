package controller

import (
	"context"

	databasev1 "github.com/egonlin/api/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("MysqlCluster status subresource contract", func() {
	ctx := context.Background()

	It("keeps spec and status updates separated by the status subresource", func() {
		cluster := validMysqlClusterForAdmission("status-contract")

		By("creating a valid MysqlCluster")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), cluster)
		})

		key := types.NamespacedName{
			Namespace: cluster.Namespace,
			Name:      cluster.Name,
		}

		stored := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, key, stored)).To(Succeed())

		originalImage := stored.Spec.Image

		By("updating status through the status subresource")
		stored.Status = databasev1.MysqlClusterStatus{
			ObservedGeneration: stored.Generation,
			Phase:              databasev1.MysqlClusterPhaseRunning,
			Primary:            "mysql-01",
			CurrentReplicas:    3,
			ReadyReplicas:      2,
			Conditions: []metav1.Condition{
				{
					Type:               "Available",
					Status:             metav1.ConditionTrue,
					Reason:             "MinimumReplicasReady",
					Message:            "The cluster has ready MySQL members.",
					ObservedGeneration: stored.Generation,
					LastTransitionTime: metav1.Now(),
				},
			},
		}

		Expect(k8sClient.Status().Update(ctx, stored)).To(Succeed())

		afterStatusUpdate := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, key, afterStatusUpdate)).To(Succeed())

		Expect(afterStatusUpdate.Spec.Image).To(Equal(originalImage))
		Expect(afterStatusUpdate.Status.Phase).To(Equal(databasev1.MysqlClusterPhaseRunning))
		Expect(afterStatusUpdate.Status.Primary).To(Equal("mysql-01"))
		Expect(afterStatusUpdate.Status.CurrentReplicas).To(Equal(int32(3)))
		Expect(afterStatusUpdate.Status.ReadyReplicas).To(Equal(int32(2)))
		Expect(afterStatusUpdate.Status.ObservedGeneration).To(
			Equal(afterStatusUpdate.Generation),
		)
		Expect(afterStatusUpdate.Status.Conditions).To(HaveLen(1))
		Expect(afterStatusUpdate.Status.Conditions[0].Type).To(Equal("Available"))

		By("attempting to modify status through the normal resource update")
		afterStatusUpdate.Status.Phase = databasev1.MysqlClusterPhaseFailed
		afterStatusUpdate.Spec.Image = "example.com/mysql:8.0"

		Expect(k8sClient.Update(ctx, afterStatusUpdate)).To(Succeed())

		final := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, key, final)).To(Succeed())

		By("confirming the spec update persisted")
		Expect(final.Spec.Image).To(Equal("example.com/mysql:8.0"))

		By("confirming the normal update did not overwrite the status subresource")
		Expect(final.Status.Phase).To(Equal(databasev1.MysqlClusterPhaseRunning))
		Expect(final.Status.Primary).To(Equal("mysql-01"))
	})
})

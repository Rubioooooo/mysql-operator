package controller

import (
	"context"

	databasev1 "github.com/egonlin/api/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func mustQuantityForTest(value string) resource.Quantity {
	return resource.MustParse(value)
}

func int32PtrForTest(value int32) *int32 {
	return &value
}

func validMysqlClusterForAdmission(name string) *databasev1.MysqlCluster {
	return &databasev1.MysqlCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: databasev1.MysqlClusterSpec{
			Image:         "example.com/mysql:5.7",
			Replicas:      int32PtrForTest(3),
			MasterService: name + "-master",
			SlaveService:  name + "-replica",
			Storage: databasev1.StorageConfig{
				StorageClassName: "test-storage",
				Size:             mustQuantityForTest("1Gi"),
			},
			Resources: databasev1.ResourceRequirements{
				Requests: databasev1.ResourceRequests{
					CPU:    mustQuantityForTest("100m"),
					Memory: mustQuantityForTest("128Mi"),
				},
				Limits: databasev1.ResourceLimits{
					CPU:    mustQuantityForTest("500m"),
					Memory: mustQuantityForTest("512Mi"),
				},
			},
		},
	}
}

func cleanupMysqlClusterForAdmission(ctx context.Context, cluster *databasev1.MysqlCluster) {
	err := k8sClient.Delete(ctx, cluster)
	if apierrors.IsNotFound(err) {
		return
	}

	Expect(err).NotTo(HaveOccurred())
}

var _ = Describe("MysqlCluster API admission contract", func() {
	ctx := context.Background()

	It("accepts the Phase 0 compatible API shape", func() {
		cluster := validMysqlClusterForAdmission("api-valid-legacy")

		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), cluster)
		})

		stored := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{
				Namespace: cluster.Namespace,
				Name:      cluster.Name,
			},
			stored,
		)).To(Succeed())

		Expect(stored.Spec.Replicas).NotTo(BeNil())
		Expect(*stored.Spec.Replicas).To(Equal(int32(3)))
		Expect(stored.Spec.MasterService).To(Equal("api-valid-legacy-master"))
		Expect(stored.Spec.SlaveService).To(Equal("api-valid-legacy-replica"))
	})

	It("defaults replicas to three when omitted", func() {
		cluster := validMysqlClusterForAdmission("api-default-replicas")
		cluster.Spec.Replicas = nil

		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())

		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), cluster)
		})

		stored := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(
			ctx,
			types.NamespacedName{
				Namespace: cluster.Namespace,
				Name:      cluster.Name,
			},
			stored,
		)).To(Succeed())

		Expect(stored.Spec.Replicas).NotTo(BeNil())
		Expect(*stored.Spec.Replicas).To(Equal(int32(3)))
	})

	It("rejects replicas equal to zero", func() {
		cluster := validMysqlClusterForAdmission("api-replicas-zero")
		cluster.Spec.Replicas = int32PtrForTest(0)

		err := k8sClient.Create(ctx, cluster)

		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("rejects negative replicas", func() {
		cluster := validMysqlClusterForAdmission("api-replicas-negative")
		cluster.Spec.Replicas = int32PtrForTest(-1)

		err := k8sClient.Create(ctx, cluster)

		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("rejects an empty image", func() {
		cluster := validMysqlClusterForAdmission("api-empty-image")
		cluster.Spec.Image = ""

		err := k8sClient.Create(ctx, cluster)

		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("rejects an invalid master service name", func() {
		cluster := validMysqlClusterForAdmission("api-invalid-service")
		cluster.Spec.MasterService = "Invalid_Service"

		err := k8sClient.Create(ctx, cluster)

		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("rejects identical master and slave service names", func() {
		cluster := validMysqlClusterForAdmission("api-same-services")
		cluster.Spec.MasterService = "same-service"
		cluster.Spec.SlaveService = "same-service"

		err := k8sClient.Create(ctx, cluster)

		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("rejects zero storage capacity", func() {
		cluster := validMysqlClusterForAdmission("api-zero-storage")
		cluster.Spec.Storage.Size = mustQuantityForTest("0")

		err := k8sClient.Create(ctx, cluster)

		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("rejects negative storage capacity", func() {
		cluster := validMysqlClusterForAdmission("api-negative-storage")
		cluster.Spec.Storage.Size = mustQuantityForTest("-1Gi")

		err := k8sClient.Create(ctx, cluster)

		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("rejects a negative CPU request", func() {
		cluster := validMysqlClusterForAdmission("api-negative-cpu")
		cluster.Spec.Resources.Requests.CPU = mustQuantityForTest("-100m")

		err := k8sClient.Create(ctx, cluster)

		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("rejects malformed Quantity syntax at the API boundary", func() {
		cluster := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps.egonlin.com/v1",
				"kind":       "MysqlCluster",
				"metadata": map[string]interface{}{
					"name":      "api-malformed-quantity",
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"image":         "example.com/mysql:5.7",
					"replicas":      int64(3),
					"masterService": "api-malformed-master",
					"slaveService":  "api-malformed-replica",
					"storage": map[string]interface{}{
						"storageClassName": "test-storage",
						"size":             "1Gi",
					},
					"resources": map[string]interface{}{
						"requests": map[string]interface{}{
							"cpu":    "definitely-not-a-quantity",
							"memory": "128Mi",
						},
						"limits": map[string]interface{}{
							"cpu":    "500m",
							"memory": "512Mi",
						},
					},
				},
			},
		}

		err := k8sClient.Create(ctx, cluster)

		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})
})

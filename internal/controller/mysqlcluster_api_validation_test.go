package controller

import (
	"context"
	"fmt"
	"strings"

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
			Image:                 "example.com/mysql:5.7",
			Replicas:              int32PtrForTest(3),
			MasterService:         name + "-master",
			SlaveService:          name + "-replica",
			CredentialsSecretName: name + "-credentials",
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

func phase5BCandidateSelectedStatusForAdmission(emptyGTID bool) *databasev1.MysqlClusterHAStatus {
	gtidSet := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10"
	if emptyGTID {
		gtidSet = ""
	}
	return &databasev1.MysqlClusterHAStatus{
		State:      databasev1.MysqlClusterHAStateFailoverInProgress,
		Primary:    "api-election-mysql-1",
		PrimaryUID: "failed-primary-uid",
		Failover: &databasev1.MysqlClusterFailoverStatus{
			Stage:                   databasev1.MysqlClusterFailoverStageCandidateSelected,
			FailedPrimary:           "api-election-mysql-1",
			FailedPrimaryUID:        "failed-primary-uid",
			FenceState:              databasev1.MysqlClusterFenceStateVerified,
			FenceMethod:             databasev1.MysqlClusterFenceMethodMySQLSuperReadOnly,
			FencedPrimaryUID:        "failed-primary-uid",
			Candidate:               "api-election-mysql-2",
			CandidateUID:            "candidate-uid",
			FailedPrimaryServerUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
			FailedPrimaryGTIDSet:    &gtidSet,
		},
	}
}

func persistHAStatusForAdmission(ctx context.Context, cluster *databasev1.MysqlCluster, status *databasev1.MysqlClusterHAStatus) error {
	stored := &databasev1.MysqlCluster{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored); err != nil {
		return err
	}
	stored.Status.HA = status
	return k8sClient.Status().Update(ctx, stored)
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
		Expect(stored.Spec.CredentialsSecretName).To(Equal("api-valid-legacy-credentials"))
	})

	It("requires a credential Secret name", func() {
		cluster := validMysqlClusterForAdmission("api-missing-credentials")
		cluster.Spec.CredentialsSecretName = ""

		err := k8sClient.Create(ctx, cluster)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("enforces the MySQL 5.7 replication host limit only on masterService", func() {
		acceptedMaster := validMysqlClusterForAdmission("api-master-service-60")
		acceptedMaster.Spec.MasterService = strings.Repeat("m", 60)
		Expect(k8sClient.Create(ctx, acceptedMaster)).To(Succeed())
		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), acceptedMaster)
		})

		rejectedMaster := validMysqlClusterForAdmission("api-master-service-61")
		rejectedMaster.Spec.MasterService = strings.Repeat("m", 61)
		err := k8sClient.Create(ctx, rejectedMaster)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)

		acceptedSlave := validMysqlClusterForAdmission("api-slave-service-63")
		acceptedSlave.Spec.SlaveService = strings.Repeat("s", 63)
		Expect(k8sClient.Create(ctx, acceptedSlave)).To(Succeed())
		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), acceptedSlave)
		})
	})

	It("rejects an invalid credential Secret name", func() {
		cluster := validMysqlClusterForAdmission("api-invalid-credentials")
		cluster.Spec.CredentialsSecretName = "Invalid_Secret"

		err := k8sClient.Create(ctx, cluster)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("keeps the credential Secret name immutable after creation", func() {
		cluster := validMysqlClusterForAdmission("api-immutable-credentials")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), cluster)
		})

		stored := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored)).To(Succeed())
		stored.Spec.CredentialsSecretName = "changed-credentials"
		err := k8sClient.Update(ctx, stored)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("keeps routing Service names immutable after creation", func() {
		cluster := validMysqlClusterForAdmission("api-immutable-services")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), cluster)
		})

		stored := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored)).To(Succeed())
		stored.Spec.MasterService = "changed-master"
		err := k8sClient.Update(ctx, stored)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)

		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored)).To(Succeed())
		Expect(stored.Spec.MasterService).To(Equal(cluster.Spec.MasterService))
		stored.Spec.SlaveService = "changed-replica"
		err = k8sClient.Update(ctx, stored)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)

		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, stored)).To(Succeed())
		Expect(stored.Spec.SlaveService).To(Equal(cluster.Spec.SlaveService))
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
					"image":                 "example.com/mysql:5.7",
					"replicas":              int64(3),
					"masterService":         "api-malformed-master",
					"slaveService":          "api-malformed-replica",
					"credentialsSecretName": "api-malformed-credentials",
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

	It("accepts complete CandidateSelected proof including an authoritative empty GTID set", func() {
		cluster := validMysqlClusterForAdmission("api-election-empty-gtid")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), cluster)
		})

		Expect(persistHAStatusForAdmission(ctx, cluster, phase5BCandidateSelectedStatusForAdmission(true))).To(Succeed())
	})

	It("rejects CandidateSelected without complete verified election proof", func() {
		mutations := []struct {
			name   string
			mutate func(*databasev1.MysqlClusterFailoverStatus)
		}{
			{name: "verified fence", mutate: func(status *databasev1.MysqlClusterFailoverStatus) {
				status.FenceState = databasev1.MysqlClusterFenceStatePending
				status.FencedPrimaryUID = ""
			}},
			{name: "matching fenced UID", mutate: func(status *databasev1.MysqlClusterFailoverStatus) { status.FencedPrimaryUID = "other-uid" }},
			{name: "candidate", mutate: func(status *databasev1.MysqlClusterFailoverStatus) { status.Candidate = "" }},
			{name: "candidate UID", mutate: func(status *databasev1.MysqlClusterFailoverStatus) { status.CandidateUID = "" }},
			{name: "failed primary server UUID", mutate: func(status *databasev1.MysqlClusterFailoverStatus) { status.FailedPrimaryServerUUID = "" }},
			{name: "failed primary GTID presence", mutate: func(status *databasev1.MysqlClusterFailoverStatus) { status.FailedPrimaryGTIDSet = nil }},
		}
		for index, mutation := range mutations {
			cluster := validMysqlClusterForAdmission(fmt.Sprintf("api-election-missing-%d", index))
			Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
			status := phase5BCandidateSelectedStatusForAdmission(false)
			mutation.mutate(status.Failover)
			err := persistHAStatusForAdmission(ctx, cluster, status)
			Expect(err).To(HaveOccurred(), "missing %s was accepted", mutation.name)
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error for missing %s: %v", mutation.name, err)
			cleanupMysqlClusterForAdmission(ctx, cluster)
		}
	})

	It("rejects selecting the failed primary identity", func() {
		cluster := validMysqlClusterForAdmission("api-election-same-primary")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), cluster)
		})
		status := phase5BCandidateSelectedStatusForAdmission(false)
		status.Failover.Candidate = status.Failover.FailedPrimary
		status.Failover.CandidateUID = status.Failover.FailedPrimaryUID

		err := persistHAStatusForAdmission(ctx, cluster, status)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})

	It("rejects stale candidate-selection proof while Stage is Fencing", func() {
		cluster := validMysqlClusterForAdmission("api-election-stale-fencing")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() {
			cleanupMysqlClusterForAdmission(context.Background(), cluster)
		})
		status := phase5BCandidateSelectedStatusForAdmission(false)
		status.Failover.Stage = databasev1.MysqlClusterFailoverStageFencing

		err := persistHAStatusForAdmission(ctx, cluster, status)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "unexpected error: %v", err)
	})
})

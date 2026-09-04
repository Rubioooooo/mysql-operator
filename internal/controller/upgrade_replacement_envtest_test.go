package controller

import (
	"context"
	"fmt"
	"time"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type replacementEnvDeleteClient struct {
	client.Client
	beforeDelete func(*corev1.Pod)
	calls        []upgradeDeleteEvidence
}

func (c *replacementEnvDeleteClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	pod, ok := object.(*corev1.Pod)
	Expect(ok).To(BeTrue())
	opts := &client.DeleteOptions{}
	for _, option := range options {
		option.ApplyToDelete(opts)
	}
	Expect(opts.Preconditions).NotTo(BeNil())
	Expect(opts.Preconditions.UID).NotTo(BeNil())
	Expect(*opts.Preconditions.UID).To(Equal(pod.UID))
	c.calls = append(c.calls, upgradeDeleteEvidence{pod.Name, pod.UID, *opts.Preconditions.UID})
	if c.beforeDelete != nil {
		c.beforeDelete(pod)
	}
	return c.Client.Delete(ctx, object, options...)
}

func replacementEnvFixture(ctx context.Context, name string) (*MysqlClusterReconciler, *databasev1.MysqlCluster) {
	cluster := configureStatefulSetRuntimeCluster(ctx, name, "mysql-replacement", 3, true)
	deferStatefulSetRuntimeResourceCleanup(cluster)
	r := statefulSetEnvtestReconciler()
	sts := createStatefulSetRuntimeWorkload(ctx, cluster, 3)
	var primary *corev1.Pod
	for ordinal := int32(1); ordinal <= 3; ordinal++ {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: mysqlStatefulSetPodName(cluster, ordinal), Namespace: cluster.Namespace, Labels: mysqlIdentityLabels(cluster)}, Spec: *sts.Spec.Template.Spec.DeepCopy()}
		// Simulate the volume reference normally added by the StatefulSet
		// controller from volumeClaimTemplates; no PVC/PV is created here.
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: mysqlDataVolume, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: mysqlDataVolume + "-" + pod.Name}}})
		pod.Labels[statefulSetPodIndexLabel] = fmt.Sprint(ordinal)
		role := "slave"
		if ordinal == 1 {
			role = "master"
		}
		pod.Labels[LabelMysqlRole], pod.Labels[LegacyLabelRole] = role, role
		Expect(controllerutil.SetControllerReference(sts, pod, r.Scheme)).To(Succeed())
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() { cleanupStatefulSetEnvtestObject(ctx, pod) })
		pod.Status.Phase = corev1.PodRunning
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: mysqlContainerName, Ready: true}}
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
		if ordinal == 1 {
			primary = pod
		}
	}
	endpoints := phase1HEndpoints(cluster, primary)
	endpoints.Subsets[0].Addresses[0].IP = "10.0.0.1"
	endpoints.Subsets[0].Ports = []corev1.EndpointPort{{Port: 3306}}
	Expect(k8sClient.Create(ctx, endpoints)).To(Succeed())
	DeferCleanup(func() { cleanupStatefulSetEnvtestObject(ctx, endpoints) })
	oldImage := cluster.Spec.Image
	cluster.Spec.Image = "mysql:8.0"
	Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
	cluster.Status.LastConvergedImage = oldImage
	cluster.Status.LastConvergedReplicas = replicaCountCopy(3)
	cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, primary)
	cluster.Status.Upgrade = &databasev1.MysqlClusterUpgradeStatus{FromImage: oldImage, TargetImage: cluster.Spec.Image, Stage: databasev1.MysqlClusterUpgradeStageTemplateReady}
	Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
	sts.Spec.Template = desiredMysqlStatefulSetWithImage(cluster, cluster.Spec.Image).Spec.Template
	Expect(k8sClient.Update(ctx, sts)).To(Succeed())
	r.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		switch command {
		case mysqlElectionReferenceCommand():
			return upgradePrimaryUUID + "\t\n", nil
		case mysqlShowSlaveStatusCommand():
			return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", "") + "\nMaster_UUID: " + upgradePrimaryUUID + "\n", nil
		default:
			return "", fmt.Errorf("unexpected mutation SQL in envtest")
		}
	}
	return r, cluster
}

var _ = Describe("Phase 7-B replica replacement API-server contract", func() {
	It("round-trips every durable stage and rejects intrinsic invalid states", func() {
		ctx := context.Background()
		cluster := validMysqlClusterForAdmission("p7b-api")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() { cleanupMysqlClusterForAdmission(ctx, cluster) })
		cluster.Status.LastConvergedImage = "mysql:old"
		upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStageTemplateReady)
		for _, stage := range []databasev1.MysqlClusterUpgradeReplacementStage{databasev1.MysqlClusterUpgradeReplacementStageDeletePending, databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement, databasev1.MysqlClusterUpgradeReplacementStageVerifying} {
			cluster.Status.Upgrade.Replacement = &databasev1.MysqlClusterUpgradeReplacementStatus{Ordinal: 2, PodName: "p7b-api-mysql-2", OldPodUID: "old", Stage: stage}
			if stage == databasev1.MysqlClusterUpgradeReplacementStageVerifying {
				cluster.Status.Upgrade.Replacement.NewPodUID = "new"
			}
			Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
			stored := &databasev1.MysqlCluster{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
			Expect(stored.Status.Upgrade).To(Equal(cluster.Status.Upgrade))
			cluster = stored
		}
		for _, scenario := range []string{"future-stage", "empty-old", "empty-name", "zero-ordinal", "missing-new", "same-new", "early-new", "preparing", "pending", "replicas-verified"} {
			invalid := cluster.DeepCopy()
			replacement := invalid.Status.Upgrade.Replacement
			switch scenario {
			case "future-stage":
				replacement.Stage = "Future"
			case "empty-old":
				replacement.OldPodUID = ""
			case "empty-name":
				replacement.PodName = ""
			case "zero-ordinal":
				replacement.Ordinal = 0
			case "missing-new":
				replacement.NewPodUID = ""
			case "same-new":
				replacement.NewPodUID = replacement.OldPodUID
			case "early-new":
				replacement.Stage = databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement
			case "preparing":
				invalid.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStagePreparing
			case "pending":
				invalid.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageTemplatePending
			case "replicas-verified":
				invalid.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageReplicasVerified
			}
			Expect(apierrors.IsInvalid(k8sClient.Status().Update(ctx, invalid))).To(BeTrue(), scenario)
		}
		// User intent may change during replacement; schema must not block it.
		cluster.Spec.Replicas = replicaCountCopy(1)
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		cluster.Status.Upgrade.Replacement = nil
		cluster.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageReplicasVerified
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
	})
	It("rejects stale optimistic status writes without emitting a milestone", func() {
		ctx := context.Background()
		r, cluster := replacementEnvFixture(ctx, "p7b-conflict")
		stale := cluster.DeepCopy()
		cluster.Spec.Replicas = replicaCountCopy(4)
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		recorder := record.NewFakeRecorder(5)
		r.Recorder = recorder
		upgrade := stale.Status.Upgrade.DeepCopy()
		upgrade.Replacement = &databasev1.MysqlClusterUpgradeReplacementStatus{Ordinal: 2, PodName: mysqlStatefulSetPodName(cluster, 2), OldPodUID: "old", Stage: databasev1.MysqlClusterUpgradeReplacementStageDeletePending}
		err := r.persistMysqlClusterUpgradeStatus(ctx, stale, stale.Status.LastConvergedImage, upgrade)
		Expect(apierrors.IsConflict(err)).To(BeTrue())
		Expect(stale.Status.Upgrade.Replacement).To(BeNil())
		Expect(recorder.Events).To(BeEmpty())
	})
	for _, race := range []bool{false, true} {
		It(fmt.Sprintf("enforces an exact server UID precondition with same-name race=%t", race), func() {
			ctx := context.Background()
			r, cluster := replacementEnvFixture(ctx, fmt.Sprintf("p7b-delete-%t", race))
			wrapped := &replacementEnvDeleteClient{Client: r.Client}
			r.Client = wrapped
			Expect(r.reconcileMysqlUpgradeReplacementPostRuntime(ctx, cluster)).To(Succeed())
			Expect(wrapped.calls).To(BeEmpty())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
			plan := cluster.Status.Upgrade.Replacement.DeepCopy()
			Expect(plan.Ordinal).To(Equal(int32(2)))
			version := cluster.ResourceVersion
			if race {
				wrapped.beforeDelete = func(old *corev1.Pod) {
					Expect(k8sClient.Delete(ctx, old, client.GracePeriodSeconds(0))).To(Succeed())
					Eventually(func() bool {
						return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(old), &corev1.Pod{}))
					}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
					replacement := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: old.Name, Namespace: old.Namespace, Labels: old.Labels, OwnerReferences: old.OwnerReferences}, Spec: *old.Spec.DeepCopy()}
					replacement.Spec.Containers[0].Image = cluster.Status.Upgrade.TargetImage
					replacement.Spec.InitContainers[0].Image = cluster.Status.Upgrade.TargetImage
					Expect(k8sClient.Create(ctx, replacement)).To(Succeed())
				}
			}
			handled, err := r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
			Expect(handled).To(BeTrue())
			if race {
				Expect(apierrors.IsConflict(err)).To(BeTrue())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(wrapped.calls).To(HaveLen(1))
			Expect(string(wrapped.calls[0].precondition)).To(Equal(plan.OldPodUID))
			Expect(wrapped.calls[0].name).To(Equal(plan.PodName))
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
			Expect(cluster.ResourceVersion).To(Equal(version))
			Expect(cluster.Status.Upgrade.Replacement).To(Equal(plan))
			if race {
				pod := &corev1.Pod{}
				Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: plan.PodName}, pod)).To(Succeed())
				Expect(string(pod.UID)).NotTo(Equal(plan.OldPodUID))
				Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
			}
		})
	}
})

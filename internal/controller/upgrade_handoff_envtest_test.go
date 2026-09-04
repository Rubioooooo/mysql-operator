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
)

type handoffEnvClient struct {
	*replacementEnvDeleteClient
	patches, roles int
}
type handoffEnvStatus struct {
	client.SubResourceWriter
	parent *handoffEnvClient
}

func (c *handoffEnvClient) Status() client.SubResourceWriter {
	return &handoffEnvStatus{SubResourceWriter: c.Client.Status(), parent: c}
}
func (s *handoffEnvStatus) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	err := s.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	if err == nil {
		s.parent.patches++
	}
	return err
}
func (c *handoffEnvClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	err := c.Client.Update(ctx, obj, opts...)
	if _, ok := obj.(*corev1.Pod); ok && err == nil {
		c.roles++
	}
	return err
}

func handoffEnvFixture(ctx context.Context, name string) (*MysqlClusterReconciler, *databasev1.MysqlCluster, *handoffTestFixture, *handoffEnvClient) {
	r, cluster := replacementEnvFixture(ctx, name)
	c := &handoffEnvClient{replacementEnvDeleteClient: &replacementEnvDeleteClient{Client: r.Client}}
	r.Client = c
	sim := &handoffTestFixture{replacementFixture: &replacementFixture{cluster: cluster}, nodes: map[string]*handoffTestNode{}, ancestry: true}
	for ordinal := int32(1); ordinal <= 3; ordinal++ {
		pod := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetPodName(cluster, ordinal)}, pod)).To(Succeed())
		replica := ordinal != 1
		n := &handoffTestNode{ro: replica, sro: replica, gtidReady: true, source: true, uuid: fmt.Sprintf("%08d-bbbb-cccc-dddd-eeeeeeeeeeee", ordinal), gtid: "00000001-bbbb-cccc-dddd-eeeeeeeeeeee:1-10"}
		if replica {
			pod.Spec.Containers[0].Image = cluster.Status.Upgrade.TargetImage
			pod.Spec.InitContainers[0].Image = cluster.Status.Upgrade.TargetImage
			Expect(k8sClient.Update(ctx, pod)).To(Succeed())
			n.channel = mysqlReplicationChannelObservation{Configured: true, MasterHost: cluster.Spec.MasterService, MasterUUID: "00000001-bbbb-cccc-dddd-eeeeeeeeeeee", MasterUser: "replica", AutoPosition: "1", IORunning: "Yes", SQLRunning: "Yes"}
		}
		sim.nodes[pod.Name] = n
	}
	cluster.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageReplicasVerified
	Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
	sim.currentPrimary = func() string {
		fresh := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), fresh)).To(Succeed())
		return fresh.Status.HA.Primary
	}
	r.execCommandOnPodFn = sim.exec
	return r, cluster, sim, c
}

func handoffEnvRoute(ctx context.Context, r *MysqlClusterReconciler, cluster *databasev1.MysqlCluster) {
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	Expect(err).NotTo(HaveOccurred())
	var primary *corev1.Pod
	for _, member := range members {
		role, _ := observeMysqlPublishedRole(member.Pod)
		if role == mysqlPublishedRoleMaster {
			primary = member.Pod
		}
	}
	current := &corev1.Endpoints{}
	Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.MasterService}, current)).To(Succeed())
	desired := phase1HEndpoints(cluster, primary)
	if primary != nil {
		desired.Subsets[0].Addresses[0].IP = "10.0.0.2"
		desired.Subsets[0].Ports = []corev1.EndpointPort{{Port: 3306}}
	}
	current.Subsets = desired.Subsets
	Expect(k8sClient.Update(ctx, current)).To(Succeed())
}

var _ = Describe("Phase 7-C handoff API-server contract", func() {
	It("round-trips handoff stages and an empty durable GTID pointer, rejecting inconsistent state", func() {
		ctx := context.Background()
		cluster := validMysqlClusterForAdmission("p7c-api")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() { cleanupMysqlClusterForAdmission(ctx, cluster) })
		cluster.Status.LastConvergedImage = "mysql:old"
		upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStageReplicasVerified)
		for _, stage := range []databasev1.MysqlClusterUpgradeHandoffStage{databasev1.MysqlClusterUpgradeHandoffStageFencing, databasev1.MysqlClusterUpgradeHandoffStageFenceVerified, databasev1.MysqlClusterUpgradeHandoffStageCandidateReady, databasev1.MysqlClusterUpgradeHandoffStagePromoting, databasev1.MysqlClusterUpgradeHandoffStageReconfiguring, databasev1.MysqlClusterUpgradeHandoffStageCompleted} {
			h := &databasev1.MysqlClusterUpgradeHandoffStatus{Stage: stage, OldPrimary: "p7c-api-mysql-1", OldPrimaryUID: "old", Candidate: "p7c-api-mysql-2", CandidateUID: "candidate"}
			if stage != databasev1.MysqlClusterUpgradeHandoffStageFencing {
				empty := ""
				h.OldPrimaryGTIDSet = &empty
				h.OldPrimaryServerUUID = upgradePrimaryUUID
			}
			if stage == databasev1.MysqlClusterUpgradeHandoffStageCompleted {
				cluster.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStagePrimaryReady
			}
			cluster.Status.Upgrade.Handoff = h
			Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
			stored := &databasev1.MysqlCluster{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
			Expect(stored.Status.Upgrade.Handoff).To(Equal(h))
			cluster = stored
		}
		for _, bad := range []string{"unknown", "same-name", "same-uid", "missing-uid", "missing-proof", "early-proof", "wrong-top", "incomplete-ready", "completed-at-replicas", "missing-handoff", "replacement-at-replicas"} {
			invalid := cluster.DeepCopy()
			h := invalid.Status.Upgrade.Handoff
			switch bad {
			case "unknown":
				h.Stage = "Unknown"
			case "same-name":
				h.Candidate = h.OldPrimary
			case "same-uid":
				h.CandidateUID = h.OldPrimaryUID
			case "missing-uid":
				h.OldPrimaryUID = ""
			case "missing-proof":
				h.OldPrimaryGTIDSet = nil
			case "early-proof":
				h.Stage = databasev1.MysqlClusterUpgradeHandoffStageFencing
				invalid.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageReplicasVerified
			case "wrong-top":
				invalid.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageTemplateReady
			case "incomplete-ready":
				h.Stage = databasev1.MysqlClusterUpgradeHandoffStageReconfiguring
			case "completed-at-replicas":
				invalid.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageReplicasVerified
			case "missing-handoff":
				invalid.Status.Upgrade.Handoff = nil
			case "replacement-at-replicas":
				invalid.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageReplicasVerified
				h.Stage = databasev1.MysqlClusterUpgradeHandoffStageReconfiguring
				invalid.Status.Upgrade.Replacement = &databasev1.MysqlClusterUpgradeReplacementStatus{Stage: databasev1.MysqlClusterUpgradeReplacementStageDeletePending, Ordinal: 1, PodName: h.OldPrimary, OldPodUID: h.OldPrimaryUID}
			}
			Expect(validateMysqlClusterUpgradeStatus(&invalid.Status)).NotTo(Succeed())
			Expect(apierrors.IsInvalid(k8sClient.Status().Update(ctx, invalid))).To(BeTrue(), bad)
		}
		cluster.Spec.Replicas = replicaCountCopy(1)
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
	})
	for _, race := range []bool{false, true} {
		It(fmt.Sprintf("persists planned barriers and UID-protects former-primary delete, race=%t", race), func() {
			ctx := context.Background()
			r, cluster, sim, c := handoffEnvFixture(ctx, fmt.Sprintf("p7c-run-%t", race))
			recorder := record.NewFakeRecorder(100)
			r.Recorder = recorder
			Expect(r.reconcileMysqlHandoffEntry(ctx, cluster)).To(Succeed())
			Expect(sim.mutations).To(BeEmpty())
			Expect(c.calls).To(BeEmpty())
			for i := 0; i < 35; i++ {
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
				if cluster.Status.Upgrade.Stage == databasev1.MysqlClusterUpgradeStagePrimaryReady {
					break
				}
				patches, roles, sql := c.patches, c.roles, len(sim.mutations)
				r = &MysqlClusterReconciler{Client: c, Scheme: r.Scheme, Recorder: recorder, execCommandOnPodFn: sim.exec}
				handled, err := r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
				Expect(err).NotTo(HaveOccurred())
				Expect(handled).To(BeTrue())
				Expect(c.patches - patches + c.roles - roles + len(sim.mutations) - sql).To(BeNumerically("<=", 1))
				Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
				Expect(cluster.Status.HA.Failover).To(BeNil())
				handoffEnvRoute(ctx, r, cluster)
			}
			Expect(cluster.Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStagePrimaryReady))
			Expect(cluster.Status.Upgrade.Handoff.Stage).To(Equal(databasev1.MysqlClusterUpgradeHandoffStageCompleted))
			Expect(c.calls).To(BeEmpty())
			Expect(r.reconcileMysqlUpgradeReplacementPostRuntime(ctx, cluster)).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
			plan := cluster.Status.Upgrade.Replacement.DeepCopy()
			Expect(plan.PodName).To(Equal(cluster.Status.Upgrade.Handoff.OldPrimary))
			version := cluster.ResourceVersion
			old := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: plan.PodName}, old)).To(Succeed())
			recreate := func() {
				Eventually(func() bool {
					return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(old), &corev1.Pod{}))
				}, 5*time.Second, 50*time.Millisecond).Should(BeTrue())
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: old.Name, Namespace: old.Namespace, Labels: old.Labels, OwnerReferences: old.OwnerReferences}, Spec: *old.Spec.DeepCopy()}
				pod.Spec.Containers[0].Image = cluster.Status.Upgrade.TargetImage
				pod.Spec.InitContainers[0].Image = cluster.Status.Upgrade.TargetImage
				Expect(k8sClient.Create(ctx, pod)).To(Succeed())
				pod.Status = old.Status
				Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
				sim.nodes[pod.Name].uuid = "99999999-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
			if race {
				c.beforeDelete = func(pod *corev1.Pod) {
					Expect(k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0))).To(Succeed())
					recreate()
				}
			}
			_, err := r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
			if race {
				Expect(apierrors.IsConflict(err)).To(BeTrue())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(c.calls).To(HaveLen(1))
			Expect(string(c.calls[0].precondition)).To(Equal(plan.OldPodUID))
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
			Expect(cluster.ResourceVersion).To(Equal(version))
			_, err = r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(cluster.Status.Upgrade.Replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement))
			if !race {
				recreate()
			}
			_, err = r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(cluster.Status.Upgrade.Replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageVerifying))
			Expect(r.reconcileMysqlUpgradeReplacementPostRuntime(ctx, cluster)).To(Succeed())
			Expect(cluster.Status.Upgrade.Replacement).To(BeNil())
			Expect(cluster.Status.LastConvergedImage).To(Equal(cluster.Status.Upgrade.FromImage))
			for len(recorder.Events) > 0 {
				<-recorder.Events
			}
			stale := cluster.DeepCopy()
			cluster.Spec.Resources.Requests.CPU = mustQuantityForTest("150m")
			Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
			Expect(r.completeMysqlUpgrade(ctx, stale)).NotTo(Succeed())
			Expect(recorder.Events).To(BeEmpty())
			Expect(stale.Status.Upgrade).NotTo(BeNil())
			conflict := r.persistMysqlClusterUpgradeStatus(ctx, stale, stale.Status.Upgrade.TargetImage, nil)
			Expect(apierrors.IsConflict(conflict)).To(BeTrue())
			Expect(recorder.Events).To(BeEmpty())
			Expect(stale.Status.Upgrade).NotTo(BeNil())
			Expect(r.completeMysqlUpgrade(ctx, cluster)).To(Succeed())
			Expect(cluster.Status.Upgrade).To(BeNil())
			Expect(cluster.Status.LastConvergedImage).To(Equal(cluster.Spec.Image))
			Expect(recorder.Events).To(Receive(ContainSubstring("UpgradeCompleted")))
			Expect(c.calls).To(HaveLen(1))
		})
	}
})

package controller

import (
	"context"
	"errors"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type upgradeTestClient struct {
	*statefulSetReconcileMemoryClient
	statefulSetWrites                     []*appsv1.StatefulSet
	podDeletes, podWrites, templateWrites int
	statefulSetCreates                    int
	updateError                           error
}

func (c *upgradeTestClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if sts, ok := obj.(*appsv1.StatefulSet); ok {
		c.statefulSetCreates++
		c.statefulSetWrites = append(c.statefulSetWrites, sts.DeepCopy())
	}
	if _, ok := obj.(*corev1.Pod); ok {
		c.podWrites++
	}
	return c.statefulSetReconcileMemoryClient.Create(ctx, obj, opts...)
}

func (c *upgradeTestClient) Delete(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		c.podDeletes++
	}
	return errors.New("upgrade must never delete an object")
}

func (c *upgradeTestClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if sts, ok := obj.(*appsv1.StatefulSet); ok {
		c.templateWrites++
		c.statefulSetWrites = append(c.statefulSetWrites, sts.DeepCopy())
		if c.updateError != nil {
			return c.updateError
		}
	}
	if _, ok := obj.(*corev1.Pod); ok {
		c.podWrites++
	}
	return c.statefulSetReconcileMemoryClient.Update(ctx, obj, opts...)
}

func newUpgradeTest(t *testing.T) (*MysqlClusterReconciler, *upgradeTestClient, *databasev1.MysqlCluster) {
	t.Helper()
	cluster := phase1HCluster("upgrade", true)
	cluster.Spec.Image = "mysql:old"
	cluster.Status.LastConvergedImage = cluster.Spec.Image
	sts := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, sts, 1, "master", true)
	cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, primary)
	objects := []client.Object{cluster, sts, phase1HCredentialSecret(cluster), phase1HEndpoints(cluster, primary)}
	for ordinal := int32(1); ordinal <= 3; ordinal++ {
		role := "slave"
		if ordinal == 1 {
			role = "master"
		}
		pod := phase1HPod(t, cluster, sts, ordinal, role, true)
		pod.Spec = *sts.Spec.Template.Spec.DeepCopy()
		objects = append(objects, pod)
	}
	c := &upgradeTestClient{statefulSetReconcileMemoryClient: newStatefulSetReconcileMemoryClient(objects...)}
	r := &MysqlClusterReconciler{Client: c, Scheme: newStatefulSetReconcileTestScheme(t), SnapGoIsEnabled: true}
	r.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		if command != mysqlShowSlaveStatusCommand() {
			t.Fatalf("unexpected SQL mutation in upgrade test")
		}
		return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
	}
	t.Cleanup(func() { NewWithT(t).Expect(c.podDeletes).To(BeZero(), "Gate 7-A Pod DELETE count") })
	return r, c, phase4StoredCluster(t, r, cluster)
}

func storeUpgradeTestCluster(t *testing.T, c *upgradeTestClient, cluster *databasev1.MysqlCluster) {
	t.Helper()
	NewWithT(t).Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), cluster)).To(Succeed())
}

func upgradeTestPlan(cluster *databasev1.MysqlCluster, stage databasev1.MysqlClusterUpgradeStage) {
	cluster.Spec.Image = "mysql:new"
	cluster.Status.Upgrade = &databasev1.MysqlClusterUpgradeStatus{FromImage: "mysql:old", TargetImage: "mysql:new", Stage: stage}
}

func upgradeTestSTS(t *testing.T, r *MysqlClusterReconciler, cluster *databasev1.MysqlCluster) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	NewWithT(t).Expect(r.Get(context.Background(), client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetName(cluster)}, sts)).To(Succeed())
	return sts
}

func TestUpgradeBootstrap(t *testing.T) {
	for _, scenario := range []string{"stable", "mixed", "template-mismatch", "empty", "duplicate", "init-mismatch", "missing-init", "empty-template", "missing-mysql", "missing-member", "zero-pods", "not-ready", "terminating", "pod-mysql-only", "bad-ordinal", "bad-owner", "patch-failure"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			r, c, cluster := newUpgradeTest(t)
			cluster.Status.LastConvergedImage = ""
			cluster.Spec.Image = "mysql:requested-new"
			storeUpgradeTestCluster(t, c, cluster)
			pod := &corev1.Pod{}
			g.Expect(r.Get(context.Background(), client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlStatefulSetPodName(cluster, 2)}, pod)).To(Succeed())
			switch scenario {
			case "mixed":
				pod.Spec.Containers[0].Image = "mysql:other"
				pod.Spec.InitContainers[0].Image = "mysql:other"
			case "template-mismatch":
				sts := upgradeTestSTS(t, r, cluster)
				sts.Spec.Template.Spec.Containers[0].Image = "mysql:other"
				sts.Spec.Template.Spec.InitContainers[0].Image = "mysql:other"
				g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), sts)).To(Succeed())
			case "empty":
				pod.Spec.Containers[0].Image = ""
			case "duplicate":
				pod.Spec.Containers = append(pod.Spec.Containers, pod.Spec.Containers[0])
			case "init-mismatch":
				sts := upgradeTestSTS(t, r, cluster)
				sts.Spec.Template.Spec.InitContainers[0].Image = "mysql:other"
				g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), sts)).To(Succeed())
			case "missing-init", "empty-template", "missing-mysql":
				sts := upgradeTestSTS(t, r, cluster)
				if scenario == "missing-init" {
					sts.Spec.Template.Spec.InitContainers = nil
				}
				if scenario == "empty-template" {
					sts.Spec.Template.Spec.Containers[0].Image = ""
				}
				if scenario == "missing-mysql" {
					sts.Spec.Template.Spec.Containers = nil
				}
				g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), sts)).To(Succeed())
			case "not-ready":
				pod.Status.ContainerStatuses[0].Ready = false
			case "terminating":
				now := metav1.Now()
				pod.DeletionTimestamp = &now
			case "pod-mysql-only":
				pod.Spec.InitContainers = nil
			case "bad-ordinal":
				pod.Labels[statefulSetPodIndexLabel] = "99"
			case "bad-owner":
				pod.OwnerReferences[0].UID = "other"
			case "patch-failure":
				c.statusPatchError = errors.New("injected status failure")
			}
			g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), pod)).To(Succeed())
			if scenario == "missing-member" {
				delete(c.objects, c.objectKey(pod))
			}
			if scenario == "zero-pods" {
				for key, object := range c.objects {
					if _, ok := object.(*corev1.Pod); ok {
						delete(c.objects, key)
					}
				}
			}
			handled, err := r.reconcileMysqlUpgradePreRuntime(context.Background(), cluster)
			g.Expect(handled).To(BeTrue())
			stored := phase4StoredCluster(t, r, cluster)
			if scenario == "stable" || scenario == "missing-member" || scenario == "zero-pods" || scenario == "not-ready" || scenario == "terminating" || scenario == "pod-mysql-only" {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(stored.Status.LastConvergedImage).To(Equal("mysql:old"))
				g.Expect(c.statusPatchCount).To(Equal(1))
			} else {
				g.Expect(err).To(HaveOccurred())
				g.Expect(stored.Status.LastConvergedImage).To(BeEmpty())
			}
			g.Expect(stored.Status.Upgrade).To(BeNil())
			g.Expect(stored.Status.ReplicaTransition).To(BeNil())
			g.Expect(c.statefulSetCreates).To(BeZero())
			g.Expect(c.templateWrites).To(BeZero())
			g.Expect(c.podWrites).To(BeZero())
		})
	}
}

func TestUpgradeMissingStatefulSetCheckpointAuthority(t *testing.T) {
	for _, stage := range []string{"no-checkpoint", "checkpoint", "Preparing", "TemplatePending", "TemplateReady"} {
		t.Run(stage, func(t *testing.T) {
			g := NewWithT(t)
			r, c, cluster := newUpgradeTest(t)
			sts := upgradeTestSTS(t, r, cluster)
			delete(c.objects, c.objectKey(sts))
			for key, obj := range c.objects {
				if _, ok := obj.(*corev1.Pod); ok {
					delete(c.objects, key)
				}
			}
			g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), phase1HEndpoints(cluster, nil))).To(Succeed())
			wantImage := "mysql:old"
			switch stage {
			case "no-checkpoint":
				cluster.Status.LastConvergedImage = ""
				cluster.Spec.Image = "mysql:changed-during-downtime"
			case "checkpoint":
			default:
				upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStage(stage))
				if stage == "TemplateReady" {
					wantImage = "mysql:new"
				}
			}
			storeUpgradeTestCluster(t, c, cluster)
			_, _, err := r.reconcileMysqlUpgradeRuntime(context.Background(), cluster)
			stored := phase4StoredCluster(t, r, cluster)
			if stage == "no-checkpoint" {
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
				g.Expect(c.statefulSetCreates).To(BeZero())
				g.Expect(stored.Status.LastConvergedImage).To(BeEmpty())
				g.Expect(stored.Status.Upgrade).To(BeNil())
				g.Expect(c.statusPatchCount).To(BeZero())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(c.statefulSetCreates).To(Equal(1))
				recreated := upgradeTestSTS(t, r, cluster)
				image, err := mysqlWorkloadImage(&recreated.Spec.Template.Spec)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(image).To(Equal(wantImage))
				g.Expect(recreated.Spec.UpdateStrategy.Type).To(Equal(appsv1.OnDeleteStatefulSetStrategyType))
				g.Expect(stored.Status.LastConvergedImage).To(Equal("mysql:old"))
				g.Expect(stored.Status.Upgrade).To(Equal(cluster.Status.Upgrade))
			}
			g.Expect(c.podWrites).To(BeZero())
		})
	}
}

func TestUpgradeImageAndReplicaCheckpointBarriers(t *testing.T) {
	g := NewWithT(t)
	r, c, cluster := newUpgradeTest(t)
	cluster.Status.LastConvergedImage = ""
	cluster.Status.LastConvergedReplicas = nil
	cluster.Spec.Replicas = replicaCountCopy(4)
	storeUpgradeTestCluster(t, c, cluster)
	_, complete, err := r.reconcileMysqlUpgradeRuntime(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(complete).To(BeFalse())
	stored := phase4StoredCluster(t, r, cluster)
	g.Expect(stored.Status.LastConvergedImage).To(Equal("mysql:old"))
	g.Expect(stored.Status.LastConvergedReplicas).To(BeNil())
	g.Expect(stored.Status.ReplicaTransition).To(BeNil())
	g.Expect(stored.Status.Upgrade).To(BeNil())
	g.Expect(c.statusPatchCount).To(Equal(1))
	g.Expect(c.templateWrites).To(BeZero())
	_, complete, err = r.reconcileMysqlUpgradeRuntime(context.Background(), stored)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(complete).To(BeFalse())
	stored = phase4StoredCluster(t, r, cluster)
	g.Expect(stored.Status.LastConvergedReplicas).To(Equal(replicaCountCopy(3)))
	g.Expect(stored.Status.ReplicaTransition).To(Equal(&databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 4}))
	g.Expect(c.statusPatchCount).To(Equal(2))
	g.Expect(c.templateWrites).To(BeZero())
	g.Expect(c.podWrites).To(BeZero())
}

func TestUpgradeDurableBarriersAndRestart(t *testing.T) {
	g := NewWithT(t)
	r, c, cluster := newUpgradeTest(t)
	cluster.Spec.Image = "mysql:new"
	storeUpgradeTestCluster(t, c, cluster)
	recorder := record.NewFakeRecorder(10)
	r.Recorder = recorder
	for _, want := range []databasev1.MysqlClusterUpgradeStage{databasev1.MysqlClusterUpgradeStagePreparing, databasev1.MysqlClusterUpgradeStageTemplatePending, databasev1.MysqlClusterUpgradeStageTemplatePending, databasev1.MysqlClusterUpgradeStageTemplateReady} {
		cluster = phase4StoredCluster(t, r, cluster)
		// A new process has only persisted API state, never a template-write flag.
		r = &MysqlClusterReconciler{Client: c, Scheme: r.Scheme, Recorder: recorder, execCommandOnPodFn: r.execCommandOnPodFn, SnapGoIsEnabled: true}
		_, complete, err := r.reconcileMysqlUpgradeRuntime(context.Background(), cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		stored := phase4StoredCluster(t, r, cluster)
		g.Expect(stored.Status.Upgrade.Stage).To(Equal(want))
		g.Expect(stored.Status.LastConvergedImage).To(Equal("mysql:old"))
		g.Expect(stored.Status.Upgrade.TargetImage).To(Equal("mysql:new"))
		sts := upgradeTestSTS(t, r, cluster)
		g.Expect(sts.Spec.UpdateStrategy.Type).To(Equal(appsv1.OnDeleteStatefulSetStrategyType))
		g.Expect(sts.Spec.PodManagementPolicy).To(Equal(appsv1.OrderedReadyPodManagement))
		if c.templateWrites == 0 {
			g.Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal("mysql:old"))
		}
	}
	g.Expect(c.templateWrites).To(Equal(1))
	g.Expect(c.statusPatchCount).To(Equal(3))
	g.Expect(c.podWrites).To(BeZero())
	g.Expect(<-recorder.Events).To(ContainSubstring("UpgradeStarted"))
	g.Expect(<-recorder.Events).To(ContainSubstring("UpgradeTemplateReady"))
	g.Expect(recorder.Events).To(BeEmpty())
	// TemplateReady is durable, does not complete, and cannot starve HA.
	endpoints := phase1HEndpoints(cluster, nil)
	g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), endpoints)).To(Succeed())
	cluster = phase4StoredCluster(t, r, cluster)
	_, _, err := r.reconcileMysqlUpgradeRuntime(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
	stored := phase4StoredCluster(t, r, cluster)
	g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
	g.Expect(stored.Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStageTemplateReady))
}

func TestUpgradeManualTemplateDrift(t *testing.T) {
	g := NewWithT(t)
	r, c, cluster := newUpgradeTest(t)
	sts := upgradeTestSTS(t, r, cluster)
	sts.Spec.Template.Spec.Containers[0].Image = "mysql:external-drift"
	g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), sts)).To(Succeed())
	_, _, err := r.reconcileMysqlUpgradeRuntime(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(phase4StoredCluster(t, r, cluster).Status.Upgrade).To(BeNil())
	g.Expect(upgradeTestSTS(t, r, cluster).Spec.Template.Spec.Containers[0].Image).To(Equal("mysql:old"))
	g.Expect(c.templateWrites).To(Equal(1))
}

func TestUpgradeSafetyInterlocks(t *testing.T) {
	for _, stage := range []databasev1.MysqlClusterUpgradeStage{databasev1.MysqlClusterUpgradeStagePreparing, databasev1.MysqlClusterUpgradeStageTemplatePending} {
		for _, scenario := range []string{"ha", "failover", "replica-transition", "unconverged-replicas", "replication", "fresh-primary-failure", "missing-member", "foundation"} {
			t.Run(string(stage)+"/"+scenario, func(t *testing.T) {
				g := NewWithT(t)
				r, c, cluster := newUpgradeTest(t)
				upgradeTestPlan(cluster, stage)
				switch scenario {
				case "ha":
					cluster.Status.HA.State = databasev1.MysqlClusterHAStateSuspected
				case "failover":
					cluster.Status.HA.Failover = &databasev1.MysqlClusterFailoverStatus{Stage: databasev1.MysqlClusterFailoverStageFencing}
				case "replica-transition":
					cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 3}
				case "unconverged-replicas":
					cluster.Status.LastConvergedReplicas = replicaCountCopy(2)
				case "replication":
					r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
						return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "No", "Yes", "", ""), nil
					}
				case "fresh-primary-failure":
					g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), phase1HEndpoints(cluster, nil))).To(Succeed())
				case "missing-member":
					pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: cluster.Namespace, Name: mysqlStatefulSetPodName(cluster, 3)}}
					delete(c.objects, c.objectKey(pod))
				case "foundation":
					sts := upgradeTestSTS(t, r, cluster)
					sts.Spec.ServiceName = "other"
					g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), sts)).To(Succeed())
				}
				storeUpgradeTestCluster(t, c, cluster)
				_, safe, _ := r.observeMysqlUpgradeSafety(context.Background(), cluster)
				g.Expect(safe).To(BeFalse())
				if stage == databasev1.MysqlClusterUpgradeStageTemplatePending {
					handled, err := r.reconcileMysqlUpgradePreRuntime(context.Background(), cluster)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(handled).To(BeFalse(), "existing runtime must remain reachable")
				}
				g.Expect(c.templateWrites).To(BeZero())
				g.Expect(c.statusPatchCount).To(BeZero())
			})
		}
	}
}

func TestUpgradeRetargetAndInvalidStatus(t *testing.T) {
	for _, scenario := range []string{"retarget", "retarget-ha", "retarget-fresh-failure", "unknown-stage", "empty-source", "same-images", "wrong-checkpoint", "no-checkpoint"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			r, c, cluster := newUpgradeTest(t)
			upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStageTemplatePending)
			switch scenario {
			case "retarget", "retarget-ha", "retarget-fresh-failure":
				cluster.Spec.Image = "mysql:retarget"
			case "unknown-stage":
				cluster.Status.Upgrade.Stage = "FutureStage"
			case "empty-source":
				cluster.Status.Upgrade.FromImage = ""
			case "same-images":
				cluster.Status.Upgrade.TargetImage = "mysql:old"
			case "wrong-checkpoint":
				cluster.Status.LastConvergedImage = "mysql:wrong"
			case "no-checkpoint":
				cluster.Status.LastConvergedImage = ""
			}
			if scenario == "retarget-ha" {
				cluster.Status.HA.State = databasev1.MysqlClusterHAStateSuspected
			}
			if scenario == "retarget-ha" || scenario == "retarget-fresh-failure" {
				g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), phase1HEndpoints(cluster, nil))).To(Succeed())
			}
			storeUpgradeTestCluster(t, c, cluster)
			plan := cluster.Status.Upgrade.DeepCopy()
			_, _, err := r.reconcileMysqlUpgradeRuntime(context.Background(), cluster)
			if scenario == "retarget-ha" || scenario == "retarget-fresh-failure" {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(phase4StoredCluster(t, r, cluster).Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
			} else {
				g.Expect(err).To(HaveOccurred())
			}
			g.Expect(phase4StoredCluster(t, r, cluster).Status.Upgrade).To(Equal(plan))
			g.Expect(c.templateWrites).To(BeZero())
		})
	}
}

func TestUpgradeRetargetRuntimePreemption(t *testing.T) {
	ctx := context.Background()
	for _, stage := range []databasev1.MysqlClusterUpgradeStage{
		databasev1.MysqlClusterUpgradeStagePreparing,
		databasev1.MysqlClusterUpgradeStageTemplatePending,
		databasev1.MysqlClusterUpgradeStageTemplateReady,
	} {
		for _, scenario := range []string{"missing-statefulset", "template-drift", "ha-fault-with-drift", "replica-transition", "replica-intent", "stable"} {
			t.Run(string(stage)+"/"+scenario, func(t *testing.T) {
				g := NewWithT(t)
				r, c, cluster := newUpgradeTest(t)
				upgradeTestPlan(cluster, stage)
				cluster.Spec.Image = "mysql:retarget"
				plan := cluster.Status.Upgrade.DeepCopy()
				wantImage := plan.FromImage
				if stage == databasev1.MysqlClusterUpgradeStageTemplateReady {
					wantImage = plan.TargetImage
				}
				sts := upgradeTestSTS(t, r, cluster)
				sts.Spec.Template = desiredMysqlStatefulSetWithImage(cluster, wantImage).Spec.Template
				if scenario == "template-drift" || scenario == "ha-fault-with-drift" {
					sts.Spec.Template.Spec.Containers[0].Image = "mysql:external-drift"
					sts.Spec.Template.Spec.InitContainers[0].Image = "mysql:external-drift"
				}
				g.Expect(c.statefulSetReconcileMemoryClient.Update(ctx, sts)).To(Succeed())
				switch scenario {
				case "missing-statefulset":
					delete(c.objects, c.objectKey(sts))
					for key, obj := range c.objects {
						if _, ok := obj.(*corev1.Pod); ok {
							delete(c.objects, key)
						}
					}
					g.Expect(c.statefulSetReconcileMemoryClient.Update(ctx, phase1HEndpoints(cluster, nil))).To(Succeed())
				case "ha-fault-with-drift":
					cluster.Status.HA.State = databasev1.MysqlClusterHAStateSuspected
					g.Expect(c.statefulSetReconcileMemoryClient.Update(ctx, phase1HEndpoints(cluster, nil))).To(Succeed())
				case "replica-transition", "replica-intent":
					cluster.Spec.Replicas = replicaCountCopy(4)
					if scenario == "replica-transition" {
						cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 4}
					}
				}
				storeUpgradeTestCluster(t, c, cluster)
				handled, err := r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(handled).To(BeFalse(), "retarget must leave existing runtime reachable")
				g.Expect(c.templateWrites).To(BeZero())
				g.Expect(c.statusPatchCount).To(BeZero())
				_, _, err = r.reconcileMysqlUpgradeRuntime(ctx, cluster)
				if scenario == "stable" || scenario == "template-drift" {
					g.Expect(err).To(MatchError(ContainSubstring("upgrade target changed; original durable plan is retained")))
					g.Expect(c.statusPatchCount).To(BeZero())
				} else {
					g.Expect(err).NotTo(HaveOccurred())
				}
				stored := phase4StoredCluster(t, r, cluster)
				switch scenario {
				case "missing-statefulset":
					g.Expect(c.statefulSetCreates).To(Equal(1))
					g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverRequired))
				case "ha-fault-with-drift":
					g.Expect(c.templateWrites).To(Equal(1))
					g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
				case "template-drift":
					g.Expect(c.templateWrites).To(Equal(1))
				case "stable":
					g.Expect(c.statefulSetWrites).To(BeEmpty())
				case "replica-intent":
					g.Expect(stored.Status.ReplicaTransition).To(Equal(&databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 4}))
					g.Expect(c.statusPatchCount).To(Equal(1))
					g.Expect(c.templateWrites).To(BeZero(), "replica intent must persist before the workload update")
					_, _, err = r.reconcileMysqlUpgradeRuntime(ctx, stored)
					g.Expect(err).NotTo(HaveOccurred())
				}
				if scenario == "replica-transition" || scenario == "replica-intent" {
					sts = upgradeTestSTS(t, r, cluster)
					g.Expect(*sts.Spec.Replicas).To(Equal(int32(4)))
					g.Expect(c.templateWrites).To(Equal(1))
					patches := c.statusPatchCount
					// Simulate the StatefulSet controller adding the requested member.
					pod := phase1HPod(t, cluster, sts, 4, "slave", true)
					pod.Spec = *sts.Spec.Template.Spec.DeepCopy()
					g.Expect(c.statefulSetReconcileMemoryClient.Create(ctx, pod)).To(Succeed())
					stored = phase4StoredCluster(t, r, cluster)
					_, _, err = r.reconcileMysqlUpgradeRuntime(ctx, stored)
					g.Expect(err).NotTo(HaveOccurred())
					stored = phase4StoredCluster(t, r, cluster)
					g.Expect(stored.Status.ReplicaTransition).To(BeNil())
					g.Expect(stored.Status.LastConvergedReplicas).To(Equal(replicaCountCopy(4)))
					g.Expect(c.statusPatchCount).To(Equal(patches + 1))
					_, _, err = r.reconcileMysqlUpgradeRuntime(ctx, stored)
					g.Expect(err).To(MatchError(ContainSubstring("upgrade target changed")))
					g.Expect(c.statusPatchCount).To(Equal(patches + 1))
				}
				stored = phase4StoredCluster(t, r, cluster)
				g.Expect(stored.Status.Upgrade).To(Equal(plan))
				g.Expect(stored.Status.LastConvergedImage).To(Equal("mysql:old"))
				g.Expect(c.podWrites).To(BeZero())
				// Inspect every attempted create/update, not just the final object.
				for _, written := range append(c.statefulSetWrites, upgradeTestSTS(t, r, cluster)) {
					image, err := mysqlWorkloadImage(&written.Spec.Template.Spec)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(image).To(Equal(wantImage))
					g.Expect(image).NotTo(Equal(cluster.Spec.Image))
					g.Expect(written.Spec.UpdateStrategy.Type).To(Equal(appsv1.OnDeleteStatefulSetStrategyType))
					g.Expect(written.Spec.UpdateStrategy.RollingUpdate).To(BeNil())
				}
			})
		}
	}
}

func TestUpgradePersistenceFailures(t *testing.T) {
	for _, scenario := range []string{"intent", "pending", "template-update", "ready"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			r, c, cluster := newUpgradeTest(t)
			cluster.Spec.Image = "mysql:new"
			if scenario != "intent" {
				upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStagePreparing)
			}
			if scenario == "template-update" || scenario == "ready" {
				cluster.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageTemplatePending
			}
			if scenario == "ready" {
				sts := upgradeTestSTS(t, r, cluster)
				sts.Spec.Template = desiredMysqlStatefulSet(cluster).Spec.Template
				g.Expect(c.statefulSetReconcileMemoryClient.Update(context.Background(), sts)).To(Succeed())
			}
			storeUpgradeTestCluster(t, c, cluster)
			before := cluster.Status.DeepCopy()
			if scenario == "template-update" {
				c.updateError = errors.New("injected update failure")
			} else {
				c.statusPatchError = errors.New("injected status failure")
			}
			recorder := record.NewFakeRecorder(10)
			r.Recorder = recorder
			_, _, err := r.reconcileMysqlUpgradeRuntime(context.Background(), cluster)
			g.Expect(err).To(HaveOccurred())
			g.Expect(phase4StoredCluster(t, r, cluster).Status.Upgrade).To(Equal(before.Upgrade))
			g.Expect(cluster.Status.Upgrade).To(Equal(before.Upgrade))
			g.Expect(recorder.Events).To(BeEmpty())
		})
	}
}

func TestUpgradeReconcileObservabilityBarrier(t *testing.T) {
	g := NewWithT(t)
	r, c, cluster := newUpgradeTest(t)
	cluster.Spec.Image = "mysql:new"
	storeUpgradeTestCluster(t, c, cluster)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(c.statusPatchCount).To(Equal(1))
	g.Expect(phase4StoredCluster(t, r, cluster).Status.Upgrade).To(BeNil())
	g.Expect(c.templateWrites).To(BeZero())
	_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(phase4StoredCluster(t, r, cluster).Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStagePreparing))
	g.Expect(c.templateWrites).To(BeZero())
}

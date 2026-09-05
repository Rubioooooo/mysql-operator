package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const upgradePrimaryUUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

func TestUpgradeReplacementStatusValidation(t *testing.T) {
	for _, scenario := range []string{"delete", "waiting", "verifying", "future", "empty-old", "empty-name", "zero-ordinal", "missing-new", "same-new", "early-new", "wrong-top", "unknown-top", "dynamic-membership"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("replacement-status", true)
			cluster.Status.LastConvergedImage = "mysql:old"
			upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStageTemplateReady)
			replacement := &databasev1.MysqlClusterUpgradeReplacementStatus{Ordinal: 2, PodName: "replacement-status-mysql-2", OldPodUID: "old", Stage: databasev1.MysqlClusterUpgradeReplacementStageDeletePending}
			cluster.Status.Upgrade.Replacement = replacement
			switch scenario {
			case "waiting":
				replacement.Stage = databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement
			case "verifying":
				replacement.Stage = databasev1.MysqlClusterUpgradeReplacementStageVerifying
				replacement.NewPodUID = "new"
			case "future":
				replacement.Stage = "Future"
			case "empty-old":
				replacement.OldPodUID = ""
			case "empty-name":
				replacement.PodName = ""
			case "zero-ordinal":
				replacement.Ordinal = 0
			case "missing-new":
				replacement.Stage = databasev1.MysqlClusterUpgradeReplacementStageVerifying
			case "same-new":
				replacement.Stage = databasev1.MysqlClusterUpgradeReplacementStageVerifying
				replacement.NewPodUID = "old"
			case "early-new":
				replacement.NewPodUID = "new"
			case "wrong-top":
				cluster.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageReplicasVerified
			case "unknown-top":
				cluster.Status.Upgrade.Stage = "Future"
			case "dynamic-membership":
				cluster.Spec.Replicas = replicaCountCopy(1)
				replacement.Ordinal = 9
			}
			err := validateMysqlClusterUpgradeStatus(&cluster.Status)
			if scenario == "delete" || scenario == "waiting" || scenario == "verifying" || scenario == "dynamic-membership" {
				g.Expect(err).NotTo(HaveOccurred())
			} else {
				g.Expect(err).To(HaveOccurred())
			}
		})
	}
}

func TestUpgradeReplicasVerifiedEffectiveImage(t *testing.T) {
	g := NewWithT(t)
	r, c, cluster := newUpgradeTest(t)
	upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStageReplicasVerified)
	storeUpgradeTestCluster(t, c, cluster)
	image, err := mysqlStatefulSetEffectiveImage(cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(image).To(Equal("mysql:new"))
	_, err = r.ensureMysqlStatefulSet(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(upgradeTestSTS(t, r, cluster).Spec.Template.Spec.Containers[0].Image).To(Equal("mysql:new"))
	// ReplicasVerified now hands off to Gate 7-C. This Gate 7-B regression
	// exercises ordinary runtime image authority without entering planned handoff.
	_, _, err = r.reconcileStatefulSetRuntime(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(phase4StoredCluster(t, r, cluster).Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStageReplicasVerified))
	g.Expect(c.podDeletes).To(BeZero())
}

type upgradeDeleteEvidence struct {
	name              string
	uid, precondition types.UID
}

// Only Gate 7-B fixtures can arm a single exact delete. Gate 7-A's default
// client still rejects every Delete; even this client rejects unarmed calls.
type replacementTestClient struct {
	*upgradeTestClient
	t           *testing.T
	armed       *databasev1.MysqlClusterUpgradeReplacementStatus
	deletes     []upgradeDeleteEvidence
	deleteError error
	keepOld     bool
}

func (c *replacementTestClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	c.t.Helper()
	g := NewWithT(c.t)
	pod, ok := object.(*corev1.Pod)
	g.Expect(ok).To(BeTrue(), "only a Pod may be deleted")
	g.Expect(c.armed).NotTo(BeNil(), "unexpected/unarmed upgrade delete")
	opts := &client.DeleteOptions{}
	for _, option := range options {
		option.ApplyToDelete(opts)
	}
	g.Expect(opts.Preconditions).NotTo(BeNil())
	g.Expect(opts.Preconditions.UID).NotTo(BeNil())
	g.Expect(pod.Name).To(Equal(c.armed.PodName))
	g.Expect(string(pod.UID)).To(Equal(c.armed.OldPodUID))
	g.Expect(string(*opts.Preconditions.UID)).To(Equal(c.armed.OldPodUID))
	c.deletes = append(c.deletes, upgradeDeleteEvidence{pod.Name, pod.UID, *opts.Preconditions.UID})
	c.armed = nil
	if c.deleteError != nil {
		return c.deleteError
	}
	if c.keepOld {
		return nil
	} // simulate lost response/cache observation
	current := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
		return err
	}
	if current.UID != *opts.Preconditions.UID {
		return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, pod.Name, errors.New("UID precondition"))
	}
	current.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	return c.statefulSetReconcileMemoryClient.Update(ctx, current)
}

type replacementFixture struct {
	t        *testing.T
	r        *MysqlClusterReconciler
	c        *replacementTestClient
	cluster  *databasev1.MysqlCluster
	channels map[string]string
	source   string
	sqlHook  func(*corev1.Pod, string)
	recorder *record.FakeRecorder
}

func newReplacementFixture(t *testing.T, primaryOrdinal int32) *replacementFixture {
	t.Helper()
	g := NewWithT(t)
	r, base, cluster := newUpgradeTest(t)
	upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStageTemplateReady)
	sts := upgradeTestSTS(t, r, cluster)
	sts.Spec.Template = desiredMysqlStatefulSetWithImage(cluster, cluster.Status.Upgrade.TargetImage).Spec.Template
	g.Expect(base.statefulSetReconcileMemoryClient.Update(context.Background(), sts)).To(Succeed())
	c := &replacementTestClient{upgradeTestClient: base, t: t}
	f := &replacementFixture{t: t, r: r, c: c, cluster: cluster, channels: map[string]string{}, source: upgradePrimaryUUID, recorder: record.NewFakeRecorder(50)}
	r.Client, r.Recorder = c, f.recorder
	for ordinal := int32(1); ordinal <= 3; ordinal++ {
		pod := f.pod(ordinal)
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: mysqlContainerName, Ready: true}}
		role := "slave"
		if ordinal == primaryOrdinal {
			role = "master"
			cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, pod)
			g.Expect(base.statefulSetReconcileMemoryClient.Update(context.Background(), phase1HEndpoints(cluster, pod))).To(Succeed())
		}
		pod.Labels[LabelMysqlRole], pod.Labels[LegacyLabelRole] = role, role
		f.put(pod)
	}
	storeUpgradeTestCluster(t, base, cluster)
	r.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		if f.sqlHook != nil {
			f.sqlHook(pod, command)
		}
		switch command {
		case mysqlWriteSafetyObservationCommand():
			return "1\t1\tON\tON\n", nil
		case mysqlElectionReferenceCommand():
			return f.source + "\t\n", nil
		case mysqlShowSlaveStatusCommand():
			if channel, ok := f.channels[pod.Name]; ok {
				return channel, nil
			}
			return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", "") + "\nMaster_UUID: " + upgradePrimaryUUID + "\n", nil
		default:
			t.Fatalf("unexpected replication mutation in replacement proof test")
			return "", nil
		}
	}
	return f
}

func (f *replacementFixture) pod(ordinal int32) *corev1.Pod {
	f.t.Helper()
	pod := &corev1.Pod{}
	NewWithT(f.t).Expect(f.r.Get(context.Background(), client.ObjectKey{Namespace: f.cluster.Namespace, Name: mysqlStatefulSetPodName(f.cluster, ordinal)}, pod)).To(Succeed())
	return pod
}
func (f *replacementFixture) put(object client.Object) {
	f.t.Helper()
	NewWithT(f.t).Expect(f.c.statefulSetReconcileMemoryClient.Update(context.Background(), object)).To(Succeed())
}
func (f *replacementFixture) stored() *databasev1.MysqlCluster {
	return phase4StoredCluster(f.t, f.r, f.cluster)
}
func (f *replacementFixture) run() error {
	_, _, err := f.r.reconcileMysqlUpgradeRuntime(context.Background(), f.stored())
	return err
}
func (f *replacementFixture) pre() (bool, error) {
	return f.r.reconcileMysqlUpgradePreRuntime(context.Background(), f.stored())
}
func (f *replacementFixture) post() error {
	return f.r.reconcileMysqlUpgradeReplacementPostRuntime(context.Background(), f.stored())
}
func (f *replacementFixture) plan(stage databasev1.MysqlClusterUpgradeReplacementStage, ordinal int32) {
	cluster := f.stored()
	pod := f.pod(ordinal)
	cluster.Status.Upgrade.Replacement = &databasev1.MysqlClusterUpgradeReplacementStatus{Ordinal: ordinal, PodName: pod.Name, OldPodUID: string(pod.UID), Stage: stage}
	if stage == databasev1.MysqlClusterUpgradeReplacementStageVerifying {
		cluster.Status.Upgrade.Replacement.OldPodUID = "old-incarnation"
		cluster.Status.Upgrade.Replacement.NewPodUID = string(pod.UID)
		f.image(ordinal, cluster.Status.Upgrade.TargetImage)
	}
	f.put(cluster)
}
func (f *replacementFixture) image(ordinal int32, image string) {
	pod := f.pod(ordinal)
	pod.Spec.Containers[0].Image = image
	pod.Spec.InitContainers[0].Image = image
	f.put(pod)
}
func (f *replacementFixture) restart() {
	old := f.r
	f.r = &MysqlClusterReconciler{Client: f.c, Scheme: old.Scheme, Recorder: old.Recorder, execCommandOnPodFn: old.execCommandOnPodFn, SnapGoIsEnabled: true}
}

func TestUpgradeReplacementSelection(t *testing.T) {
	for _, primary := range []int32{1, 2, 3} {
		t.Run(fmt.Sprint(primary), func(t *testing.T) {
			g := NewWithT(t)
			f := newReplacementFixture(t, primary)
			g.Expect(f.run()).To(Succeed())
			want := int32(1)
			if primary == 1 {
				want = 2
			}
			plan := f.stored().Status.Upgrade.Replacement
			g.Expect(plan.Ordinal).To(Equal(want))
			g.Expect(plan.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageDeletePending))
			g.Expect(plan.OldPodUID).To(Equal(string(f.pod(want).UID)))
			g.Expect(f.c.deletes).To(BeEmpty())
			g.Expect(f.c.statusPatchCount).To(Equal(1))
			g.Expect(<-f.recorder.Events).To(ContainSubstring("UpgradeReplicaSelected"))
			f.restart()
			g.Expect(f.stored().Status.Upgrade.Replacement).To(Equal(plan))
		})
	}
	for _, scenario := range []string{"skip-target", "third-image", "init-mismatch", "source-empty", "source-wrong"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			f := newReplacementFixture(t, 3)
			switch scenario {
			case "skip-target":
				f.image(1, "mysql:new")
			case "third-image":
				f.image(2, "mysql:unexpected")
			case "init-mismatch":
				pod := f.pod(2)
				pod.Spec.InitContainers[0].Image = "mysql:other"
				f.put(pod)
			case "source-empty":
				f.channels[f.pod(2).Name] = mysqlSlaveStatusOutputForTest(f.cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", "")
			case "source-wrong":
				f.source = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
			}
			err := f.post()
			if scenario == "skip-target" {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(f.stored().Status.Upgrade.Replacement.Ordinal).To(Equal(int32(2)))
			} else {
				g.Expect(err).To(HaveOccurred())
				g.Expect(f.stored().Status.Upgrade.Replacement).To(BeNil())
			}
			g.Expect(f.c.deletes).To(BeEmpty())
		})
	}
}

func TestUpgradeReplacementDeleteCrashRecovery(t *testing.T) {
	g := NewWithT(t)
	f := newReplacementFixture(t, 1)
	f.plan(databasev1.MysqlClusterUpgradeReplacementStageDeletePending, 2)
	plan := f.stored().Status.Upgrade.Replacement.DeepCopy()
	f.c.keepOld = true
	for i := 0; i < 2; i++ {
		f.restart()
		f.c.armed = plan.DeepCopy()
		handled, err := f.pre()
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(handled).To(BeTrue())
		g.Expect(f.c.statusPatchCount).To(BeZero())
		g.Expect(f.stored().Status.Upgrade.Replacement).To(Equal(plan))
	}
	g.Expect(f.c.deletes).To(HaveLen(2))
	for _, call := range f.c.deletes {
		g.Expect(string(call.precondition)).To(Equal(plan.OldPodUID))
		g.Expect(call.name).To(Equal(plan.PodName))
	}
	f.c.keepOld = false
	f.c.armed = plan.DeepCopy()
	_, err := f.pre()
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(f.c.statusPatchCount).To(BeZero())
	f.restart()
	_, err = f.pre()
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(f.stored().Status.Upgrade.Replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement))
	g.Expect(f.c.statusPatchCount).To(Equal(1))
	for _, scenario := range []string{"missing", "new-uid"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			f := newReplacementFixture(t, 1)
			f.plan(databasev1.MysqlClusterUpgradeReplacementStageDeletePending, 2)
			pod := f.pod(2)
			if scenario == "missing" {
				delete(f.c.objects, f.c.objectKey(pod))
			} else {
				pod.UID = "new-uid"
				f.put(pod)
			}
			handled, err := f.pre()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(handled).To(BeTrue())
			g.Expect(f.c.deletes).To(BeEmpty())
			g.Expect(f.stored().Status.Upgrade.Replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement))
		})
	}
}

func TestUpgradeReplacementDeleteSafety(t *testing.T) {
	for _, scenario := range []string{"primary", "ordinal", "owner", "role-master", "retarget", "ha", "failover", "transition", "replica-intent", "unready", "unknown-stage", "wrong-source", "empty-source", "third-image", "uid-race", "primary-race", "retarget-race", "delete-error"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			f := newReplacementFixture(t, 1)
			f.plan(databasev1.MysqlClusterUpgradeReplacementStageDeletePending, 2)
			cluster := f.stored()
			pod := f.pod(2)
			switch scenario {
			case "primary":
				f.plan(databasev1.MysqlClusterUpgradeReplacementStageDeletePending, 1)
				cluster = f.stored()
			case "ordinal":
				cluster.Status.Upgrade.Replacement.Ordinal = 3
			case "owner":
				pod.OwnerReferences[0].UID = "wrong-owner"
				f.put(pod)
			case "role-master":
				pod.Labels[LabelMysqlRole] = "master"
				pod.Labels[LegacyLabelRole] = "master"
				f.put(pod)
			case "retarget":
				cluster.Spec.Image = "mysql:retarget"
			case "ha":
				cluster.Status.HA.State = databasev1.MysqlClusterHAStateSuspected
			case "failover":
				cluster.Status.HA.Failover = &databasev1.MysqlClusterFailoverStatus{Stage: databasev1.MysqlClusterFailoverStageFencing}
			case "transition":
				cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 3}
			case "replica-intent":
				cluster.Spec.Replicas = replicaCountCopy(4)
			case "unready":
				pod.Status.ContainerStatuses[0].Ready = false
				f.put(pod)
			case "unknown-stage":
				cluster.Status.Upgrade.Replacement.Stage = "Future"
			case "wrong-source":
				f.source = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
			case "empty-source":
				f.channels[pod.Name] = mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", "")
			case "third-image":
				f.image(2, "mysql:unexpected")
			case "uid-race", "primary-race", "retarget-race":
				f.sqlHook = func(_ *corev1.Pod, cmd string) {
					if cmd != mysqlShowSlaveStatusCommand() {
						return
					}
					f.sqlHook = nil
					if scenario == "retarget-race" {
						current := f.stored()
						current.Spec.Image = "mysql:retarget"
						f.put(current)
					} else {
						ordinal := int32(2)
						if scenario == "primary-race" {
							ordinal = 1
						}
						current := f.pod(ordinal)
						current.UID = "changed-during-proof"
						f.put(current)
					}
				}
			case "delete-error":
				f.c.armed = cluster.Status.Upgrade.Replacement.DeepCopy()
				f.c.deleteError = errors.New("injected delete failure")
			}
			f.put(cluster)
			before := cluster.Status.Upgrade.DeepCopy()
			handled, err := f.pre()
			if scenario == "retarget" || scenario == "ha" || scenario == "failover" || scenario == "transition" || scenario == "replica-intent" || scenario == "unready" {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(handled).To(BeFalse())
			} else {
				g.Expect(err).To(HaveOccurred())
			}
			if scenario == "delete-error" {
				g.Expect(f.c.deletes).To(HaveLen(1))
			} else {
				g.Expect(f.c.deletes).To(BeEmpty())
			}
			g.Expect(f.stored().Status.Upgrade).To(Equal(before))
			g.Expect(f.c.statusPatchCount).To(BeZero())
		})
	}
}

func TestUpgradeReplacementWaiting(t *testing.T) {
	for _, scenario := range []string{"missing", "old-terminating", "old-live", "new-unready", "new-wrong-image"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			f := newReplacementFixture(t, 1)
			f.plan(databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement, 2)
			pod := f.pod(2)
			switch scenario {
			case "missing":
				delete(f.c.objects, f.c.objectKey(pod))
			case "old-terminating":
				now := metav1.Now()
				pod.DeletionTimestamp = &now
				f.put(pod)
			case "new-unready", "new-wrong-image":
				pod.UID = "new-incarnation"
				pod.Status.ContainerStatuses[0].Ready = false
				if scenario == "new-unready" {
					pod.Spec.Containers[0].Image = "mysql:new"
					pod.Spec.InitContainers[0].Image = "mysql:new"
				}
				f.put(pod)
			}
			handled, err := f.pre()
			switch scenario {
			case "missing", "old-terminating":
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(handled).To(BeFalse())
				g.Expect(f.c.statusPatchCount).To(BeZero())
			case "old-live", "new-wrong-image":
				g.Expect(err).To(HaveOccurred())
				g.Expect(f.c.statusPatchCount).To(BeZero())
			case "new-unready":
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(mysqlStatefulSetPodHealthy(f.pod(2))).To(BeFalse())
				g.Expect(handled).To(BeTrue())
				replacement := f.stored().Status.Upgrade.Replacement
				g.Expect(replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageVerifying))
				g.Expect(replacement.NewPodUID).To(Equal("new-incarnation"))
				g.Expect(f.c.statusPatchCount).To(Equal(1))
			}
			g.Expect(f.c.deletes).To(BeEmpty())
		})
	}
}

func TestUpgradeReplacementVerification(t *testing.T) {
	for _, scenario := range []string{"success", "unready", "old-uid", "wrong-image", "init-image", "master", "role-none", "io-stopped", "wrong-host", "empty-source", "wrong-source", "primary-changed", "target-race"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			f := newReplacementFixture(t, 1)
			f.plan(databasev1.MysqlClusterUpgradeReplacementStageVerifying, 2)
			cluster := f.stored()
			pod := f.pod(2)
			switch scenario {
			case "unready":
				pod.Status.ContainerStatuses[0].Ready = false
				f.put(pod)
			case "old-uid":
				pod.UID = types.UID(cluster.Status.Upgrade.Replacement.OldPodUID)
				f.put(pod)
			case "wrong-image":
				f.image(2, "mysql:old")
			case "init-image":
				pod.Spec.InitContainers[0].Image = "mysql:old"
				f.put(pod)
			case "master":
				pod.Labels[LabelMysqlRole] = "master"
				pod.Labels[LegacyLabelRole] = "master"
				f.put(pod)
			case "role-none":
				delete(pod.Labels, LabelMysqlRole)
				delete(pod.Labels, LegacyLabelRole)
				f.put(pod)
			case "io-stopped":
				f.channels[pod.Name] = mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "No", "Yes", "", "") + "\nMaster_UUID: " + upgradePrimaryUUID
			case "wrong-host":
				f.channels[pod.Name] = mysqlSlaveStatusOutputForTest("wrong-host", "replica", "1", "Yes", "Yes", "", "") + "\nMaster_UUID: " + upgradePrimaryUUID
			case "empty-source":
				f.channels[pod.Name] = mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", "")
			case "wrong-source":
				f.source = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
			case "primary-changed":
				primary := f.pod(1)
				primary.UID = "changed-primary"
				f.put(primary)
			case "target-race":
				f.sqlHook = func(_ *corev1.Pod, cmd string) {
					if cmd == mysqlShowSlaveStatusCommand() {
						f.sqlHook = nil
						current := f.pod(2)
						current.UID = "changed-new-pod"
						f.put(current)
					}
				}
			}
			err := f.post()
			if scenario == "success" {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(f.stored().Status.Upgrade.Replacement).To(BeNil())
				g.Expect(f.c.statusPatchCount).To(Equal(1))
			} else {
				g.Expect(f.stored().Status.Upgrade.Replacement).NotTo(BeNil())
				g.Expect(f.c.statusPatchCount).To(BeZero())
			}
			g.Expect(f.stored().Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStageTemplateReady))
			g.Expect(f.c.deletes).To(BeEmpty())
		})
	}
}

func TestUpgradeReplacementMultipleReplicas(t *testing.T) {
	g := NewWithT(t)
	f := newReplacementFixture(t, 3)
	primary := f.pod(3).DeepCopy()
	for _, ordinal := range []int32{1, 2} {
		g.Expect(f.run()).To(Succeed())
		selected := f.stored().Status.Upgrade.Replacement.DeepCopy()
		g.Expect(selected.Ordinal).To(Equal(ordinal))
		before := len(f.c.deletes)
		f.c.armed = selected.DeepCopy()
		f.restart()
		g.Expect(f.run()).To(Succeed())
		g.Expect(len(f.c.deletes)).To(Equal(before + 1))
		g.Expect(f.stored().Status.Upgrade.Replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageDeletePending))
		g.Expect(f.run()).To(Succeed())
		g.Expect(f.stored().Status.Upgrade.Replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement))
		pod := f.pod(ordinal)
		pod.UID = types.UID(fmt.Sprintf("new-%d", ordinal))
		pod.DeletionTimestamp = nil
		pod.Spec.Containers[0].Image = "mysql:new"
		pod.Spec.InitContainers[0].Image = "mysql:new"
		f.put(pod)
		g.Expect(f.run()).To(Succeed())
		g.Expect(f.stored().Status.Upgrade.Replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageVerifying))
		g.Expect(f.run()).To(Succeed())
		g.Expect(f.stored().Status.Upgrade.Replacement).To(BeNil())
		g.Expect(f.stored().Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStageTemplateReady))
	}
	g.Expect(f.run()).To(Succeed())
	g.Expect(f.stored().Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStageReplicasVerified))
	g.Expect(f.stored().Status.Upgrade).NotTo(BeNil())
	g.Expect(f.stored().Status.LastConvergedImage).To(Equal("mysql:old"))
	g.Expect(f.c.deletes).To(HaveLen(2))
	g.Expect(f.pod(3)).To(Equal(primary))
	// Gate 7-B ends at ReplicasVerified; Gate 7-C owns the next upgrade barrier.
	_, _, runtimeErr := f.r.reconcileStatefulSetRuntime(context.Background(), f.stored())
	g.Expect(runtimeErr).NotTo(HaveOccurred())
	g.Expect(upgradeTestSTS(t, f.r, f.cluster).Spec.Template.Spec.Containers[0].Image).To(Equal("mysql:new"))
	for _, reason := range []string{"UpgradeReplicaSelected", "UpgradeReplicaObserved", "UpgradeReplicaVerified", "UpgradeReplicaSelected", "UpgradeReplicaObserved", "UpgradeReplicaVerified", "UpgradeReplicasVerified"} {
		g.Expect(f.recorder.Events).To(Receive(ContainSubstring(reason)))
	}
	g.Expect(f.recorder.Events).To(BeEmpty())
	status := f.stored().Status.Upgrade
	f.r.emitMysqlUpgradeTransition(context.Background(), f.stored(), status, status.DeepCopy())
	g.Expect(f.recorder.Events).To(BeEmpty(), "re-observing the same durable state must not repeat milestones")
}

func TestUpgradeReplacementPersistenceFailures(t *testing.T) {
	for _, barrier := range []string{"selection", "waiting", "verifying", "clear", "replicas-verified"} {
		t.Run(barrier, func(t *testing.T) {
			g := NewWithT(t)
			f := newReplacementFixture(t, 1)
			switch barrier {
			case "waiting":
				f.plan(databasev1.MysqlClusterUpgradeReplacementStageDeletePending, 2)
				pod := f.pod(2)
				delete(f.c.objects, f.c.objectKey(pod))
			case "verifying":
				f.plan(databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement, 2)
				pod := f.pod(2)
				pod.UID = "new-member"
				f.put(pod)
				f.image(2, "mysql:new")
			case "clear":
				f.plan(databasev1.MysqlClusterUpgradeReplacementStageVerifying, 2)
			case "replicas-verified":
				f.image(2, "mysql:new")
				f.image(3, "mysql:new")
			}
			before := f.stored().Status.Upgrade.DeepCopy()
			f.c.statusPatchError = errors.New("injected status failure")
			var err error
			if barrier == "waiting" || barrier == "verifying" {
				_, err = f.pre()
			} else {
				err = f.post()
			}
			g.Expect(err).To(HaveOccurred())
			g.Expect(f.stored().Status.Upgrade).To(Equal(before))
			g.Expect(f.c.deletes).To(BeEmpty())
			g.Expect(f.recorder.Events).To(BeEmpty())
		})
	}
}

func TestUpgradeReplacementPreemption(t *testing.T) {
	for _, stage := range []databasev1.MysqlClusterUpgradeReplacementStage{databasev1.MysqlClusterUpgradeReplacementStageDeletePending, databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement, databasev1.MysqlClusterUpgradeReplacementStageVerifying} {
		for _, scenario := range []string{"ha", "transition", "retarget", "incompatible", "became-primary"} {
			t.Run(string(stage)+"/"+scenario, func(t *testing.T) {
				g := NewWithT(t)
				f := newReplacementFixture(t, 1)
				f.plan(stage, 3)
				cluster := f.stored()
				plan := cluster.Status.Upgrade.DeepCopy()
				switch scenario {
				case "ha":
					cluster.Status.HA.State = databasev1.MysqlClusterHAStateSuspected
					f.put(phase1HEndpoints(cluster, nil))
				case "transition":
					cluster.Spec.Replicas = replicaCountCopy(4)
					cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 4}
				case "retarget":
					cluster.Spec.Image = "mysql:retarget"
					f.put(phase1HEndpoints(cluster, nil))
				case "incompatible":
					cluster.Spec.Replicas = replicaCountCopy(2)
					cluster.Status.LastConvergedReplicas = replicaCountCopy(2)
				case "became-primary":
					old := f.pod(1)
					old.Labels[LabelMysqlRole] = "slave"
					old.Labels[LegacyLabelRole] = "slave"
					f.put(old)
					pod := f.pod(3)
					pod.Labels[LabelMysqlRole] = "master"
					pod.Labels[LegacyLabelRole] = "master"
					f.put(pod)
					cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, pod)
					f.put(phase1HEndpoints(cluster, pod))
				}
				f.put(cluster)
				err := f.run()
				if scenario == "incompatible" || scenario == "became-primary" {
					g.Expect(err).To(HaveOccurred())
				} else {
					g.Expect(err).NotTo(HaveOccurred())
				}
				if scenario == "ha" || scenario == "retarget" {
					g.Expect(f.stored().Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
				}
				if scenario == "transition" {
					g.Expect(*upgradeTestSTS(t, f.r, cluster).Spec.Replicas).To(Equal(int32(4)))
				}
				g.Expect(f.stored().Status.Upgrade).To(Equal(plan))
				g.Expect(f.c.deletes).To(BeEmpty())
			})
		}
	}
}

func TestUpgradeReplacementExternalTargetNeedsProof(t *testing.T) {
	g := NewWithT(t)
	f := newReplacementFixture(t, 1)
	f.image(2, "mysql:new")
	f.image(3, "mysql:new")
	f.channels[f.pod(3).Name] = mysqlSlaveStatusOutputForTest(f.cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", "")
	g.Expect(f.post()).NotTo(Succeed())
	g.Expect(f.stored().Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStageTemplateReady))
	g.Expect(f.c.deletes).To(BeEmpty())
	delete(f.channels, f.pod(3).Name)
	g.Expect(f.post()).To(Succeed())
	g.Expect(f.stored().Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStageReplicasVerified))
	g.Expect(f.stored().Status.LastConvergedImage).To(Equal("mysql:old"))
	g.Expect(f.c.deletes).To(BeEmpty())
}

func TestUpgradeReplacementHARecoveryResumesExactDelete(t *testing.T) {
	g := NewWithT(t)
	f := newReplacementFixture(t, 1)
	f.plan(databasev1.MysqlClusterUpgradeReplacementStageDeletePending, 2)
	cluster := f.stored()
	cluster.Status.HA.State = databasev1.MysqlClusterHAStateSuspected
	f.put(cluster)
	f.put(phase1HEndpoints(cluster, nil))
	plan := cluster.Status.Upgrade.DeepCopy()
	g.Expect(f.run()).To(Succeed())
	g.Expect(f.c.deletes).To(BeEmpty())
	g.Expect(f.stored().Status.Upgrade).To(Equal(plan))
	f.put(phase1HEndpoints(cluster, f.pod(1)))
	g.Expect(f.run()).To(Succeed())
	g.Expect(f.c.deletes).To(BeEmpty())
	g.Expect(f.stored().Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateHealthy))
	f.c.armed = plan.Replacement.DeepCopy()
	g.Expect(f.run()).To(Succeed())
	g.Expect(f.c.deletes).To(HaveLen(1))
	g.Expect(f.stored().Status.Upgrade).To(Equal(plan))
}

func TestUpgradeReplacementUsesExistingReplicationExecutor(t *testing.T) {
	g := NewWithT(t)
	f := newReplacementFixture(t, 1)
	f.plan(databasev1.MysqlClusterUpgradeReplacementStageVerifying, 2)
	target := f.pod(2)
	f.channels[target.Name] = mysqlSlaveStatusOutputForTest("wrong-host", "replica", "1", "Yes", "Yes", "", "") + "\nMaster_UUID: " + upgradePrimaryUUID
	readOnly := f.r.execCommandOnPodFn
	mutations := 0
	f.r.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		if command == mysqlConfigureReplicaCommand(f.cluster.Spec.MasterService) {
			g.Expect(pod.Name).To(Equal(target.Name))
			mutations++
			delete(f.channels, pod.Name)
			return "", nil
		}
		return readOnly(pod, command)
	}
	g.Expect(f.run()).To(Succeed())
	g.Expect(mutations).To(Equal(1))
	g.Expect(f.stored().Status.Upgrade.Replacement).NotTo(BeNil())
	g.Expect(f.c.statusPatchCount).To(BeZero())
	g.Expect(f.run()).To(Succeed())
	g.Expect(f.stored().Status.Upgrade.Replacement).To(BeNil())
	g.Expect(f.c.statusPatchCount).To(Equal(1))
	g.Expect(f.c.deletes).To(BeEmpty())
}

func TestUpgradeReplacementDeletePendingAllowsReplicationRecovery(t *testing.T) {
	g := NewWithT(t)
	f := newReplacementFixture(t, 1)
	f.plan(databasev1.MysqlClusterUpgradeReplacementStageDeletePending, 2)
	plan := f.stored().Status.Upgrade.DeepCopy()
	target := f.pod(2)
	f.channels[target.Name] = mysqlSlaveStatusOutputForTest("wrong-host", "replica", "1", "Yes", "Yes", "", "") + "\nMaster_UUID: " + upgradePrimaryUUID
	readOnly := f.r.execCommandOnPodFn
	mutations := 0
	f.r.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		if command == mysqlConfigureReplicaCommand(f.cluster.Spec.MasterService) {
			g.Expect(pod.Name).To(Equal(target.Name))
			mutations++
			delete(f.channels, pod.Name)
			return "", nil
		}
		return readOnly(pod, command)
	}
	g.Expect(f.run()).To(Succeed())
	g.Expect(mutations).To(Equal(1))
	g.Expect(f.c.deletes).To(BeEmpty())
	g.Expect(f.stored().Status.Upgrade).To(Equal(plan))
	g.Expect(f.c.statusPatchCount).To(BeZero())
	f.c.armed = plan.Replacement.DeepCopy()
	g.Expect(f.run()).To(Succeed())
	g.Expect(f.c.deletes).To(HaveLen(1))
	g.Expect(f.stored().Status.Upgrade).To(Equal(plan))
}

func TestUpgradeReplacementScaleDownIsNotUpgradeReplacement(t *testing.T) {
	g := NewWithT(t)
	f := newReplacementFixture(t, 1)
	f.plan(databasev1.MysqlClusterUpgradeReplacementStageDeletePending, 3)
	cluster := f.stored()
	plan := cluster.Status.Upgrade.DeepCopy()
	cluster.Spec.Replicas = replicaCountCopy(2)
	cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 2}
	f.put(cluster)
	g.Expect(f.run()).To(Succeed())
	g.Expect(*upgradeTestSTS(t, f.r, cluster).Spec.Replicas).To(Equal(int32(2)))
	g.Expect(f.stored().Status.Upgrade).To(Equal(plan))
	pod := f.pod(3)
	delete(f.c.objects, f.c.objectKey(pod)) // external StatefulSet-controller scale-down
	g.Expect(f.run()).To(Succeed())
	g.Expect(f.stored().Status.ReplicaTransition).To(BeNil())
	g.Expect(f.stored().Status.LastConvergedReplicas).To(Equal(replicaCountCopy(2)))
	g.Expect(f.run()).To(MatchError(ContainSubstring("incompatible with converged membership")))
	g.Expect(f.stored().Status.Upgrade).To(Equal(plan))
	g.Expect(f.c.deletes).To(BeEmpty())
}

func TestUpgradeReplacementAllowsActiveFailoverRuntime(t *testing.T) {
	g := NewWithT(t)
	f := newReplacementFixture(t, 1)
	f.plan(databasev1.MysqlClusterUpgradeReplacementStageDeletePending, 2)
	cluster := f.stored()
	plan := cluster.Status.Upgrade.DeepCopy()
	primary := f.pod(1)
	primary.Status.ContainerStatuses[0].Ready = false
	f.put(primary)
	cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStatePending)
	f.put(cluster)
	f.put(phase1HEndpoints(cluster, nil))
	observations := 0
	f.r.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		g.Expect(command).To(Equal(mysqlWriteSafetyObservationCommand()))
		observations++
		return "", errors.New("primary observation unavailable")
	}
	_ = f.run()
	g.Expect(observations).To(BeNumerically(">", 0))
	g.Expect(f.stored().Status.Upgrade).To(Equal(plan))
	g.Expect(f.c.deletes).To(BeEmpty())
}

package controller

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type handoffTestNode struct {
	ro, sro, gtidReady, source bool
	uuid, gtid                 string
	channel                    mysqlReplicationChannelObservation
	relation                   mysqlGTIDRelation
}
type handoffSQLMutation struct{ name, command string }
type handoffTestFixture struct {
	*replacementFixture
	nodes          map[string]*handoffTestNode
	mutations      []handoffSQLMutation
	hook           func(*corev1.Pod, string)
	ancestry       bool
	autoRoute      bool
	currentPrimary func() string
}

var handoffEncodedArgument = regexp.MustCompile(`FROM_BASE64\('([^']*)'\)`)

func newHandoffTest(t *testing.T, primary int32) *handoffTestFixture {
	t.Helper()
	f := newReplacementFixture(t, primary)
	h := &handoffTestFixture{replacementFixture: f, nodes: map[string]*handoffTestNode{}, ancestry: true, autoRoute: true}
	primaryUUID := fmt.Sprintf("%08d-bbbb-cccc-dddd-eeeeeeeeeeee", primary)
	for ordinal := int32(1); ordinal <= 3; ordinal++ {
		pod := f.pod(ordinal)
		replica := ordinal != primary
		node := &handoffTestNode{ro: replica, sro: replica, gtidReady: true, source: true, uuid: fmt.Sprintf("%08d-bbbb-cccc-dddd-eeeeeeeeeeee", ordinal), gtid: primaryUUID + ":1-10"}
		if replica {
			f.image(ordinal, "mysql:new")
			node.channel = mysqlReplicationChannelObservation{Configured: true, MasterHost: f.cluster.Spec.MasterService, MasterUUID: primaryUUID, MasterUser: "replica", AutoPosition: "1", IORunning: "Yes", SQLRunning: "Yes"}
		}
		h.nodes[pod.Name] = node
	}
	cluster := f.stored()
	cluster.Status.Upgrade.Stage = databasev1.MysqlClusterUpgradeStageReplicasVerified
	f.put(cluster)
	f.r.execCommandOnPodFn = h.exec
	return h
}

func (h *handoffTestFixture) exec(pod *corev1.Pod, command string) (string, error) {
	if h.hook != nil {
		h.hook(pod, command)
	}
	n := h.nodes[pod.Name]
	if n == nil {
		return "", errors.New("unknown fake member")
	}
	b := func(v bool) string {
		if v {
			return "1"
		}
		return "0"
	}
	switch command {
	case mysqlWriteSafetyObservationCommand():
		mode := "ON"
		if !n.gtidReady {
			mode = "OFF"
		}
		return b(n.ro) + "\t" + b(n.sro) + "\t" + mode + "\tON\n", nil
	case mysqlElectionReferenceCommand():
		return n.uuid + "\t" + base64.StdEncoding.EncodeToString([]byte(n.gtid)) + "\n", nil
	case mysqlSourceCapabilityCommand():
		return b(n.source) + "\t1\n", nil
	case mysqlShowSlaveStatusCommand():
		c := n.channel
		if !c.Configured {
			return "", nil
		}
		return mysqlSlaveStatusOutputForTest(c.MasterHost, c.MasterUser, c.AutoPosition, c.IORunning, c.SQLRunning, c.LastIOError, c.LastSQLError) + "\nMaster_UUID: " + c.MasterUUID + "\n", nil
	case mysqlSetSuperReadOnlyCommand():
		n.ro, n.sro = true, true
	case mysqlSetReadOnlyOffCommand():
		n.ro, n.sro = false, false
	case mysqlStopSlaveCommand():
		n.channel.IORunning, n.channel.SQLRunning = "No", "No"
	case mysqlStartSlaveCommand():
		n.channel.IORunning, n.channel.SQLRunning = "Yes", "Yes"
	case mysqlInitializeReplicaCommand(h.cluster.Spec.MasterService), mysqlConfigureReplicaCommand(h.cluster.Spec.MasterService):
		primaryName := ""
		if h.currentPrimary != nil {
			primaryName = h.currentPrimary()
		} else {
			primaryName = h.stored().Status.HA.Primary
		}
		primary := h.nodes[primaryName]
		n.channel = mysqlReplicationChannelObservation{Configured: true, MasterHost: h.cluster.Spec.MasterService, MasterUUID: primary.uuid, MasterUser: "replica", AutoPosition: "1", IORunning: "Yes", SQLRunning: "Yes"}
	default:
		if strings.Contains(command, "GTID_SUBSET") {
			args := handoffEncodedArgument.FindStringSubmatch(command)
			if len(args) != 2 {
				return "", errors.New("invalid fake GTID query")
			}
			raw, err := base64.StdEncoding.DecodeString(args[1])
			if err != nil {
				return "", err
			}
			if strings.Count(command, "GTID_SUBSET") == 1 {
				return b(h.ancestry) + "\t" + base64.StdEncoding.EncodeToString([]byte(n.gtid)) + "\n", nil
			}
			relation := n.relation
			if relation == "" {
				relation = mysqlGTIDRelationEqual
				if string(raw) != n.gtid {
					relation = mysqlGTIDRelationSuperset
				}
			}
			left, right := "1", "1"
			switch relation {
			case mysqlGTIDRelationSubset:
				left = "0"
			case mysqlGTIDRelationSuperset:
				right = "0"
			case mysqlGTIDRelationDivergent:
				left, right = "0", "0"
			}
			return left + "\t" + right + "\t" + base64.StdEncoding.EncodeToString([]byte(n.gtid)) + "\n", nil
		}
		return "", fmt.Errorf("unexpected fake SQL command")
	}
	h.mutations = append(h.mutations, handoffSQLMutation{pod.Name, command})
	return "", nil
}

func (h *handoffTestFixture) route() {
	members, err := h.r.listMysqlStatefulSetPods(context.Background(), h.stored())
	NewWithT(h.t).Expect(err).NotTo(HaveOccurred())
	var primary *corev1.Pod
	for _, m := range members {
		role, _ := observeMysqlPublishedRole(m.Pod)
		if role == mysqlPublishedRoleMaster {
			primary = m.Pod
		}
	}
	h.put(phase1HEndpoints(h.cluster, primary))
}

func (h *handoffTestFixture) step() error {
	h.t.Helper()
	g := NewWithT(h.t)
	metrics := h.r.Metrics
	h.restart()
	h.r.Metrics = metrics
	patches, writes, sql, deletes := h.c.statusPatchCount, h.c.podWrites, len(h.mutations), len(h.c.deletes)
	err := h.run()
	g.Expect((h.c.statusPatchCount-patches)+(h.c.podWrites-writes)+(len(h.mutations)-sql)+(len(h.c.deletes)-deletes)).To(BeNumerically("<=", 1), "one handoff control barrier per reconcile")
	g.Expect(h.stored().Status.HA.Failover).To(BeNil(), "planned handoff must not synthesize HA.Failover")
	if h.autoRoute {
		h.route()
	}
	return err
}

func (h *handoffTestFixture) until(predicate func(*databasev1.MysqlCluster) bool) {
	h.t.Helper()
	for i := 0; i < 60; i++ {
		if predicate(h.stored()) {
			return
		}
		NewWithT(h.t).Expect(h.step()).To(Succeed())
	}
	h.t.Fatal("handoff barrier did not converge")
}
func (h *handoffTestFixture) stage(stage databasev1.MysqlClusterUpgradeHandoffStage) {
	h.until(func(c *databasev1.MysqlCluster) bool {
		return c.Status.Upgrade != nil && c.Status.Upgrade.Handoff != nil && c.Status.Upgrade.Handoff.Stage == stage
	})
}

func TestHandoffCompletePrimaryLastRestartEveryBarrier(t *testing.T) {
	for _, ordinal := range []int32{1, 2, 3} {
		t.Run(fmt.Sprint(ordinal), func(t *testing.T) {
			g := NewWithT(t)
			h := newHandoffTest(t, ordinal)
			m, registry := newMysqlMetricsForTest(t)
			h.r.Metrics = m
			g.Expect(h.step()).To(Succeed())
			initial := h.stored().Status.Upgrade.Handoff.DeepCopy()
			want := int32(1)
			if ordinal == 1 {
				want = 2
			}
			g.Expect(initial.Candidate).To(Equal(mysqlStatefulSetPodName(h.cluster, want)))
			g.Expect(h.mutations).To(BeEmpty())
			g.Expect(h.c.deletes).To(BeEmpty())
			h.stage(databasev1.MysqlClusterUpgradeHandoffStageCompleted)
			ready := h.stored()
			g.Expect(ready.Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStagePrimaryReady))
			g.Expect(ready.Status.LastConvergedImage).To(Equal("mysql:old"))
			g.Expect(ready.Status.HA.Primary).To(Equal(initial.Candidate))
			g.Expect(h.c.deletes).To(BeEmpty())
			old := h.pod(ordinal)
			role, _ := observeMysqlPublishedRole(old)
			g.Expect(role).To(Equal(mysqlPublishedRoleSlave))
			g.Expect(h.nodes[old.Name].sro).To(BeTrue())
			g.Expect(h.step()).To(Succeed())
			plan := h.stored().Status.Upgrade.Replacement.DeepCopy()
			g.Expect(plan.PodName).To(Equal(initial.OldPrimary))
			g.Expect(plan.OldPodUID).To(Equal(initial.OldPrimaryUID))
			g.Expect(h.c.deletes).To(BeEmpty())
			h.c.armed = plan.DeepCopy()
			g.Expect(h.step()).To(Succeed())
			g.Expect(h.c.deletes).To(HaveLen(1))
			g.Expect(h.stored().Status.Upgrade.Replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageDeletePending))
			g.Expect(h.step()).To(Succeed())
			g.Expect(h.stored().Status.Upgrade.Replacement.Stage).To(Equal(databasev1.MysqlClusterUpgradeReplacementStageWaitingForReplacement))
			old = h.pod(ordinal)
			old.UID = types.UID("new-former-primary")
			old.DeletionTimestamp = nil
			old.Spec.Containers[0].Image = "mysql:new"
			old.Spec.InitContainers[0].Image = "mysql:new"
			h.put(old)
			h.nodes[old.Name].uuid = "99999999-bbbb-cccc-dddd-eeeeeeeeeeee"
			g.Expect(h.step()).To(Succeed())
			g.Expect(h.stored().Status.Upgrade.Replacement.NewPodUID).To(Equal(string(old.UID)))
			g.Expect(h.step()).To(Succeed())
			g.Expect(h.stored().Status.Upgrade.Replacement).To(BeNil())
			g.Expect(h.stored().Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStagePrimaryReady))
			g.Expect(h.stored().Status.LastConvergedImage).To(Equal("mysql:old"))
			g.Expect(h.step()).To(Succeed())
			g.Expect(h.stored().Status.Upgrade).To(BeNil())
			g.Expect(h.stored().Status.LastConvergedImage).To(Equal("mysql:new"))
			g.Expect(h.c.deletes).To(HaveLen(1))
			g.Expect(h.c.deletes[0].name).NotTo(Equal(initial.Candidate))
			var events []string
			for len(h.recorder.Events) > 0 {
				events = append(events, <-h.recorder.Events)
			}
			g.Expect(events[len(events)-1]).To(ContainSubstring("UpgradeCompleted"))
			for _, event := range events {
				g.Expect(event).NotTo(ContainSubstring("Failover"))
				g.Expect(event).NotTo(ContainSubstring(initial.OldPrimaryUID))
				g.Expect(event).NotTo(ContainSubstring(initial.CandidateUID))
				g.Expect(event).NotTo(ContainSubstring("mysql:new"))
			}
			for _, sample := range gatherMysqlMetrics(t, registry) {
				if sample.name == mysqlTestMetricPrefix+"ha_transitions_total" {
					g.Expect(sample.value).To(BeZero())
				}
			}
		})
	}
}

func TestHandoffNoCandidateAndTargetShortcut(t *testing.T) {
	for _, scenario := range []string{"single", "no-capability", "not-equal", "shortcut", "shortcut-not-sro", "shortcut-not-writable", "shortcut-routing", "shortcut-source", "shortcut-unready"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			h := newHandoffTest(t, 1)
			if scenario == "single" {
				c := h.stored()
				c.Spec.Replicas = replicaCountCopy(1)
				c.Status.LastConvergedReplicas = replicaCountCopy(1)
				h.put(c)
				sts := upgradeTestSTS(t, h.r, c)
				sts.Spec.Replicas = replicaCountCopy(1)
				h.put(sts)
				for _, i := range []int32{2, 3} {
					pod := h.pod(i)
					delete(h.c.objects, h.c.objectKey(pod))
				}
			}
			if scenario == "no-capability" {
				h.nodes[h.pod(2).Name].source = false
				h.nodes[h.pod(3).Name].source = false
			}
			if scenario == "not-equal" {
				h.nodes[h.pod(2).Name].relation = mysqlGTIDRelationSubset
				h.nodes[h.pod(3).Name].relation = mysqlGTIDRelationSubset
			}
			if strings.HasPrefix(scenario, "shortcut") {
				h.image(1, "mysql:new")
			}
			switch scenario {
			case "shortcut-not-sro":
				h.nodes[h.pod(2).Name].ro, h.nodes[h.pod(2).Name].sro = false, false
			case "shortcut-not-writable":
				h.nodes[h.pod(1).Name].ro, h.nodes[h.pod(1).Name].sro = true, true
			case "shortcut-routing":
				h.put(phase1HEndpoints(h.cluster, nil))
			case "shortcut-source":
				h.nodes[h.pod(2).Name].channel.MasterUUID = "wrong"
			case "shortcut-unready":
				pod := h.pod(2)
				pod.Status.ContainerStatuses[0].Ready = false
				h.put(pod)
			}
			err := h.r.reconcileMysqlHandoffEntry(context.Background(), h.stored())
			if scenario == "shortcut" {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(h.stored().Status.Upgrade).To(BeNil())
				g.Expect(h.stored().Status.LastConvergedImage).To(Equal("mysql:new"))
			} else {
				g.Expect(err).To(HaveOccurred())
				g.Expect(h.stored().Status.Upgrade.Handoff).To(BeNil())
				g.Expect(h.stored().Status.LastConvergedImage).To(Equal("mysql:old"))
			}
			g.Expect(h.mutations).To(BeEmpty())
			g.Expect(h.c.podWrites).To(BeZero())
			g.Expect(h.c.deletes).To(BeEmpty())
			g.Expect(h.stored().Status.HA.Failover).To(BeNil())
		})
	}
}

func TestHandoffDurableFenceAndIdentityFailures(t *testing.T) {
	for _, scenario := range []string{"old-uid-before", "old-uid-after", "old-uuid", "old-gtid", "old-writable", "candidate-uid", "candidate-master", "candidate-writable", "candidate-source", "candidate-channel-source", "candidate-subset", "candidate-divergent", "multiple-masters", "proof-race"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			h := newHandoffTest(t, 1)
			h.stage(databasev1.MysqlClusterUpgradeHandoffStageFencing)
			if scenario != "old-uid-before" {
				h.stage(databasev1.MysqlClusterUpgradeHandoffStageFenceVerified)
			}
			plan := h.stored().Status.Upgrade.DeepCopy()
			old, candidate := h.pod(1), h.pod(2)
			switch scenario {
			case "old-uid-before", "old-uid-after":
				old.UID = "changed-old"
				h.put(old)
			case "old-uuid":
				h.nodes[old.Name].uuid = "99999999-bbbb-cccc-dddd-eeeeeeeeeeee"
			case "old-gtid":
				h.nodes[old.Name].gtid += "1"
			case "old-writable":
				h.nodes[old.Name].ro, h.nodes[old.Name].sro = false, false
			case "candidate-uid":
				candidate.UID = "changed-candidate"
				h.put(candidate)
			case "candidate-master":
				candidate.Labels[LabelMysqlRole], candidate.Labels[LegacyLabelRole] = "master", "master"
				h.put(candidate)
			case "candidate-writable":
				h.nodes[candidate.Name].ro, h.nodes[candidate.Name].sro = false, false
			case "candidate-source":
				h.nodes[candidate.Name].source = false
			case "candidate-channel-source":
				h.nodes[candidate.Name].channel.MasterUUID = "wrong-source"
			case "candidate-subset":
				h.nodes[candidate.Name].relation = mysqlGTIDRelationSubset
			case "candidate-divergent":
				h.nodes[candidate.Name].relation = mysqlGTIDRelationDivergent
			case "multiple-masters":
				pod := h.pod(3)
				pod.Labels[LabelMysqlRole], pod.Labels[LegacyLabelRole] = "master", "master"
				h.put(pod)
			case "proof-race":
				h.hook = func(pod *corev1.Pod, command string) {
					if pod.Name == candidate.Name && strings.Contains(command, "GTID_SUBSET") {
						h.hook = nil
						changed := h.pod(2)
						changed.UID = "race-candidate"
						h.put(changed)
					}
				}
			}
			mutations, writes, patches := len(h.mutations), h.c.podWrites, h.c.statusPatchCount
			g.Expect(h.step()).To(HaveOccurred())
			g.Expect(h.stored().Status.Upgrade).To(Equal(plan))
			g.Expect(len(h.mutations)).To(Equal(mutations))
			g.Expect(h.c.podWrites).To(Equal(writes))
			g.Expect(h.c.statusPatchCount).To(Equal(patches))
			g.Expect(h.c.deletes).To(BeEmpty())
		})
	}
}

func TestHandoffRoutingBarriers(t *testing.T) {
	g := NewWithT(t)
	h := newHandoffTest(t, 1)
	h.stage(databasev1.MysqlClusterUpgradeHandoffStageCandidateReady)
	h.autoRoute = false
	g.Expect(h.step()).To(Succeed())
	g.Expect(h.pod(1).Labels).NotTo(HaveKey(LabelMysqlRole))
	before := h.stored().Status.Upgrade.DeepCopy()
	patches := h.c.statusPatchCount
	g.Expect(h.step()).To(Succeed())
	g.Expect(h.stored().Status.Upgrade).To(Equal(before))
	g.Expect(h.c.statusPatchCount).To(Equal(patches))
	h.route()
	h.stage(databasev1.MysqlClusterUpgradeHandoffStagePromoting)
	h.until(func(c *databasev1.MysqlCluster) bool {
		role, _ := observeMysqlPublishedRole(h.pod(2))
		return role == mysqlPublishedRoleMaster
	})
	patches = h.c.statusPatchCount
	g.Expect(h.step()).To(Succeed())
	g.Expect(h.c.statusPatchCount).To(Equal(patches))
	g.Expect(h.stored().Status.HA.Primary).To(Equal(h.pod(1).Name))
	h.route()
	g.Expect(h.step()).To(Succeed())
	g.Expect(h.stored().Status.HA.Primary).To(Equal(h.pod(2).Name))
	g.Expect(h.stored().Status.Upgrade.Handoff.Stage).To(Equal(databasev1.MysqlClusterUpgradeHandoffStagePromoting))
	g.Expect(h.step()).To(Succeed())
	g.Expect(h.stored().Status.Upgrade.Handoff.Stage).To(Equal(databasev1.MysqlClusterUpgradeHandoffStageReconfiguring))
}

func TestHandoffRetargetAndReplicaPriority(t *testing.T) {
	for _, afterFence := range []bool{false, true} {
		for _, kind := range []string{"retarget", "replicas"} {
			t.Run(fmt.Sprintf("%s/%t", kind, afterFence), func(t *testing.T) {
				g := NewWithT(t)
				h := newHandoffTest(t, 1)
				h.stage(databasev1.MysqlClusterUpgradeHandoffStageFencing)
				if afterFence {
					g.Expect(h.step()).To(Succeed())
				} // SQL persisted in MySQL, status still Fencing
				c := h.stored()
				plan := c.Status.Upgrade.DeepCopy()
				if kind == "retarget" {
					c.Spec.Image = "mysql:retarget"
				} else {
					c.Spec.Replicas = replicaCountCopy(4)
				}
				h.put(c)
				if !afterFence {
					mutations := len(h.mutations)
					_, _, _ = h.r.reconcileMysqlUpgradeRuntime(context.Background(), h.stored())
					g.Expect(len(h.mutations)).To(Equal(mutations))
					g.Expect(h.stored().Status.Upgrade).To(Equal(plan))
					if kind == "replicas" {
						g.Expect(h.stored().Status.ReplicaTransition).NotTo(BeNil())
					}
				} else {
					h.stage(databasev1.MysqlClusterUpgradeHandoffStageCompleted)
					g.Expect(h.stored().Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStagePrimaryReady))
					g.Expect(h.stored().Status.Upgrade.TargetImage).To(Equal(plan.TargetImage))
					g.Expect(*upgradeTestSTS(t, h.r, c).Spec.Replicas).To(Equal(int32(3)))
					g.Expect(h.c.deletes).To(BeEmpty())
					_, _, _ = h.r.reconcileMysqlUpgradeRuntime(context.Background(), h.stored())
					g.Expect(h.stored().Status.Upgrade.Replacement).To(BeNil())
					g.Expect(h.c.deletes).To(BeEmpty())
					if kind == "replicas" {
						g.Expect(h.stored().Status.ReplicaTransition).NotTo(BeNil())
					}
				}
			})
		}
	}
}

func TestHandoffPersistenceFailures(t *testing.T) {
	for _, barrier := range []string{"selection", "checkpoint", "candidate-ready", "promoting", "ha", "reconfiguring", "primary-ready", "completion"} {
		t.Run(barrier, func(t *testing.T) {
			g := NewWithT(t)
			h := newHandoffTest(t, 1)
			switch barrier {
			case "checkpoint":
				h.stage(databasev1.MysqlClusterUpgradeHandoffStageFencing)
				g.Expect(h.step()).To(Succeed())
			case "candidate-ready":
				h.stage(databasev1.MysqlClusterUpgradeHandoffStageFenceVerified)
			case "promoting":
				h.stage(databasev1.MysqlClusterUpgradeHandoffStageCandidateReady)
				g.Expect(h.step()).To(Succeed())
			case "ha":
				h.stage(databasev1.MysqlClusterUpgradeHandoffStagePromoting)
				h.until(func(c *databasev1.MysqlCluster) bool {
					role, _ := observeMysqlPublishedRole(h.pod(2))
					return role == mysqlPublishedRoleMaster
				})
			case "reconfiguring":
				h.stage(databasev1.MysqlClusterUpgradeHandoffStagePromoting)
				h.until(func(c *databasev1.MysqlCluster) bool { return c.Status.HA.Primary == h.pod(2).Name })
			case "primary-ready":
				h.stage(databasev1.MysqlClusterUpgradeHandoffStageReconfiguring)
				h.until(func(c *databasev1.MysqlCluster) bool {
					o, err := h.r.observeMysqlHandoff(context.Background(), c, true)
					if err != nil {
						return false
					}
					a, err := h.r.planMysqlHandoffAction(context.Background(), c, o)
					return err == nil && a.kind == "primary-ready"
				})
			case "completion":
				h.image(1, "mysql:new")
			}
			for len(h.recorder.Events) > 0 {
				<-h.recorder.Events
			}
			before := h.stored()
			h.c.statusPatchError = errors.New("injected status failure")
			mutations, writes := len(h.mutations), h.c.podWrites
			_, _, err := h.r.reconcileMysqlUpgradeRuntime(context.Background(), before)
			g.Expect(err).To(HaveOccurred())
			g.Expect(before.Status.Upgrade).To(Equal(h.stored().Status.Upgrade))
			g.Expect(before.Status.HA).To(Equal(h.stored().Status.HA))
			g.Expect(len(h.mutations)).To(Equal(mutations))
			g.Expect(h.c.podWrites).To(Equal(writes))
			g.Expect(h.recorder.Events).To(BeEmpty())
		})
	}
}

func TestHandoffUnsafeRejoin(t *testing.T) {
	for _, scenario := range []string{"old-uuid", "old-uid", "candidate-uid", "ancestry", "other-writable"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			h := newHandoffTest(t, 1)
			h.stage(databasev1.MysqlClusterUpgradeHandoffStageReconfiguring)
			old := h.pod(1)
			switch scenario {
			case "old-uuid":
				h.nodes[old.Name].uuid = "99999999-bbbb-cccc-dddd-eeeeeeeeeeee"
			case "old-uid":
				old.UID = "new-old"
				h.put(old)
			case "candidate-uid":
				p := h.pod(2)
				p.UID = "new-candidate"
				h.put(p)
			case "ancestry":
				h.ancestry = false
			case "other-writable":
				h.nodes[h.pod(3).Name].ro, h.nodes[h.pod(3).Name].sro = false, false
			}
			before := h.stored().Status.Upgrade.DeepCopy()
			mutations := len(h.mutations)
			if scenario == "other-writable" {
				g.Expect(h.step()).To(Succeed())
				g.Expect(len(h.mutations)).To(Equal(mutations + 1))
			} else {
				g.Expect(h.step()).To(HaveOccurred())
				g.Expect(len(h.mutations)).To(Equal(mutations))
			}
			g.Expect(h.stored().Status.Upgrade).To(Equal(before))
			g.Expect(h.c.deletes).To(BeEmpty())
		})
	}
}

func (h *handoffTestFixture) recreateFormer() {
	h.t.Helper()
	cluster := h.stored()
	name := cluster.Status.Upgrade.Handoff.OldPrimary
	pod := &corev1.Pod{}
	NewWithT(h.t).Expect(h.r.Get(context.Background(), client.ObjectKey{Namespace: cluster.Namespace, Name: name}, pod)).To(Succeed())
	pod.UID = "recreated-former"
	pod.DeletionTimestamp = nil
	pod.Spec.Containers[0].Image = cluster.Status.Upgrade.TargetImage
	pod.Spec.InitContainers[0].Image = cluster.Status.Upgrade.TargetImage
	h.put(pod)
	h.nodes[name].uuid = "99999999-bbbb-cccc-dddd-eeeeeeeeeeee"
}

func TestHandoffFinalProofAndFormerReplacementFailures(t *testing.T) {
	for _, scenario := range []string{"not-sro", "not-ready", "wrong-source", "primary-not-writable", "primary-uid", "other-old-image", "same-former-uid", "final-race", "completion-patch", "delete-intent-patch", "waiting-patch", "verifying-patch", "clear-patch"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			h := newHandoffTest(t, 1)
			h.stage(databasev1.MysqlClusterUpgradeHandoffStageCompleted)
			if scenario == "delete-intent-patch" {
				h.c.statusPatchError = errors.New("injected")
			} else if scenario == "waiting-patch" || scenario == "verifying-patch" || scenario == "clear-patch" {
				g.Expect(h.step()).To(Succeed())
				h.c.armed = h.stored().Status.Upgrade.Replacement.DeepCopy()
				g.Expect(h.step()).To(Succeed())
				if scenario != "waiting-patch" {
					g.Expect(h.step()).To(Succeed())
					h.recreateFormer()
					if scenario == "clear-patch" {
						g.Expect(h.step()).To(Succeed())
					}
				}
				h.c.statusPatchError = errors.New("injected")
			} else {
				h.recreateFormer()
			}
			old, candidate := h.pod(1), h.pod(2)
			switch scenario {
			case "not-sro":
				h.nodes[old.Name].ro, h.nodes[old.Name].sro = false, false
			case "not-ready":
				old.Status.ContainerStatuses[0].Ready = false
				h.put(old)
			case "wrong-source":
				h.nodes[old.Name].channel.MasterUUID = "wrong"
			case "primary-not-writable":
				h.nodes[candidate.Name].ro, h.nodes[candidate.Name].sro = true, true
			case "primary-uid":
				candidate.UID = "wrong-primary"
				h.put(candidate)
			case "other-old-image":
				h.image(3, "mysql:old")
			case "same-former-uid":
				old.UID = types.UID(h.stored().Status.Upgrade.Handoff.OldPrimaryUID)
				h.put(old)
			case "final-race":
				h.hook = func(pod *corev1.Pod, command string) {
					if pod.Name == candidate.Name && command == mysqlSourceCapabilityCommand() {
						h.hook = nil
						changed := h.pod(2)
						changed.UID = "raced-primary"
						h.put(changed)
					}
				}
			case "completion-patch":
				h.c.statusPatchError = errors.New("injected")
			}
			before := h.stored().Status.Upgrade.DeepCopy()
			for len(h.recorder.Events) > 0 {
				<-h.recorder.Events
			}
			var err error
			if strings.HasSuffix(scenario, "-patch") && scenario != "completion-patch" {
				_, _, err = h.r.reconcileMysqlUpgradeRuntime(context.Background(), h.stored())
			} else {
				err = h.r.completeMysqlUpgrade(context.Background(), h.stored())
			}
			g.Expect(err).To(HaveOccurred())
			g.Expect(h.stored().Status.Upgrade).To(Equal(before))
			g.Expect(h.stored().Status.LastConvergedImage).To(Equal("mysql:old"))
			g.Expect(h.recorder.Events).To(BeEmpty())
		})
	}
}

func TestHandoffRejoinActionsAndNewWrites(t *testing.T) {
	for _, scenario := range []string{"no-channel", "wrong-running", "wrong-stopped", "correct-stopped", "publish-slave", "new-writes"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			h := newHandoffTest(t, 1)
			h.stage(databasev1.MysqlClusterUpgradeHandoffStageReconfiguring)
			old := h.nodes[h.pod(1).Name]
			candidate := h.nodes[h.pod(2).Name]
			if scenario != "no-channel" {
				old.channel = mysqlReplicationChannelObservation{Configured: true, MasterHost: h.cluster.Spec.MasterService, MasterUUID: candidate.uuid, MasterUser: "replica", AutoPosition: "1", IORunning: "Yes", SQLRunning: "Yes"}
			}
			switch scenario {
			case "wrong-running":
				old.channel.MasterUUID = "wrong"
			case "wrong-stopped":
				old.channel.MasterUUID = "wrong"
				old.channel.IORunning, old.channel.SQLRunning = "No", "No"
			case "correct-stopped":
				old.channel.IORunning, old.channel.SQLRunning = "No", "No"
			case "new-writes":
				candidate.gtid += "1"
			}
			beforeSQL, beforeRoles := len(h.mutations), h.c.podWrites
			g.Expect(h.step()).To(Succeed())
			if scenario == "publish-slave" || scenario == "new-writes" {
				g.Expect(h.c.podWrites).To(Equal(beforeRoles + 1))
			} else {
				g.Expect(len(h.mutations)).To(Equal(beforeSQL + 1))
				want := mysqlInitializeReplicaCommand(h.cluster.Spec.MasterService)
				switch scenario {
				case "wrong-running":
					want = mysqlStopSlaveCommand()
				case "wrong-stopped":
					want = mysqlConfigureReplicaCommand(h.cluster.Spec.MasterService)
				case "correct-stopped":
					want = mysqlStartSlaveCommand()
				}
				g.Expect(h.mutations[len(h.mutations)-1].command).To(Equal(want))
			}
		})
	}
}

func TestHandoffRealHAPreemption(t *testing.T) {
	for _, stage := range []databasev1.MysqlClusterUpgradeHandoffStage{databasev1.MysqlClusterUpgradeHandoffStageFencing, databasev1.MysqlClusterUpgradeHandoffStageCandidateReady, databasev1.MysqlClusterUpgradeHandoffStagePromoting} {
		t.Run(string(stage), func(t *testing.T) {
			g := NewWithT(t)
			h := newHandoffTest(t, 1)
			h.stage(stage)
			cluster := h.stored()
			plan := cluster.Status.Upgrade.DeepCopy()
			cluster.Status.HA = phase5FencingHA(h.pod(1), databasev1.MysqlClusterFenceStatePending)
			h.put(cluster)
			mutations := len(h.mutations)
			handled, err := h.r.reconcileMysqlUpgradePreRuntime(context.Background(), h.stored())
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(handled).To(BeFalse())
			g.Expect(len(h.mutations)).To(Equal(mutations))
			g.Expect(h.stored().Status.Upgrade).To(Equal(plan))
		})
	}
}

func TestHandoffZeroMasterThroughMainReconcile(t *testing.T) {
	g := NewWithT(t)
	h := newHandoffTest(t, 1)
	h.stage(databasev1.MysqlClusterUpgradeHandoffStageCandidateReady)
	g.Expect(h.step()).To(Succeed())
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(h.stored())}
	for i := 0; i < 2; i++ {
		_, err := h.r.Reconcile(context.Background(), request)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(h.stored().Status.HA.Failover).To(BeNil())
	}
	g.Expect(h.stored().Status.Upgrade.Handoff.Stage).To(Equal(databasev1.MysqlClusterUpgradeHandoffStagePromoting))
	g.Expect(h.stored().Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateHealthy))
}

func TestHandoffFencesUnsafeWritableReplicaBeforeAncestry(t *testing.T) {
	g := NewWithT(t)
	h := newHandoffTest(t, 1)
	h.stage(databasev1.MysqlClusterUpgradeHandoffStageReconfiguring)
	member := h.nodes[h.pod(3).Name]
	member.ro, member.sro = false, false
	h.ancestry = false
	before := len(h.mutations)
	g.Expect(h.step()).To(Succeed())
	g.Expect(len(h.mutations)).To(Equal(before + 1))
	g.Expect(h.mutations[len(h.mutations)-1].command).To(Equal(mysqlSetSuperReadOnlyCommand()))
	g.Expect(member.sro).To(BeTrue())
	g.Expect(h.step()).To(HaveOccurred())
	g.Expect(len(h.mutations)).To(Equal(before + 1))
	g.Expect(h.stored().Status.Upgrade.Handoff.Stage).To(Equal(databasev1.MysqlClusterUpgradeHandoffStageReconfiguring))
}

func TestHandoffUnknownFenceCannotReleaseTopology(t *testing.T) {
	for _, intent := range []string{"image", "replicas", "unsupported-observation"} {
		t.Run(intent, func(t *testing.T) {
			g := NewWithT(t)
			h := newHandoffTest(t, 1)
			h.stage(databasev1.MysqlClusterUpgradeHandoffStageFencing)
			g.Expect(h.step()).To(Succeed())
			cluster := h.stored()
			if intent == "image" {
				cluster.Spec.Image = "mysql:retarget"
			} else {
				cluster.Spec.Replicas = replicaCountCopy(4)
			}
			h.put(cluster)
			plan := cluster.Status.Upgrade.DeepCopy()
			h.r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
				if intent == "unsupported-observation" {
					return "", errors.New("unknown system variable super_read_only")
				}
				return "", errors.New("observation unavailable")
			}
			handled, err := h.r.reconcileMysqlUpgradePreRuntime(context.Background(), h.stored())
			g.Expect(err).To(HaveOccurred())
			g.Expect(handled).To(BeTrue())
			g.Expect(h.stored().Status.Upgrade).To(Equal(plan))
			g.Expect(h.stored().Status.ReplicaTransition).To(BeNil())
			g.Expect(*upgradeTestSTS(t, h.r, cluster).Spec.Replicas).To(Equal(int32(3)))
		})
	}
}

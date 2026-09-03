package controller

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	phase5DNewPrimaryUUID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	phase5DOldPrimaryUUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	phase5DPrimaryGTID    = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff:1-20"
)

type phase5DMemberState struct {
	readOnly   bool
	superRO    bool
	gtidReady  bool
	source     bool
	serverUUID string
	gtidSet    string
	channel    mysqlReplicationChannelObservation
	subset     bool
}

type phase5DExecPlan struct {
	cluster       *databasev1.MysqlCluster
	candidate     string
	states        map[string]*phase5DMemberState
	commands      []string
	mutations     []string
	beforeCommand func(*corev1.Pod, string)
}

func phase5DBool(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func phase5DChannelOutput(channel mysqlReplicationChannelObservation) string {
	if !channel.Configured {
		return ""
	}
	output := mysqlSlaveStatusOutputForTest(
		channel.MasterHost,
		channel.MasterUser,
		channel.AutoPosition,
		channel.IORunning,
		channel.SQLRunning,
		channel.LastIOError,
		channel.LastSQLError,
	)
	return strings.Replace(
		output,
		"               Master_User: "+channel.MasterUser+"\n",
		"               Master_User: "+channel.MasterUser+"\n               Master_UUID: "+channel.MasterUUID+"\n",
		1,
	)
}

func phase5DHealthyChannel(cluster *databasev1.MysqlCluster) mysqlReplicationChannelObservation {
	return mysqlReplicationChannelObservation{
		Configured:   true,
		MasterHost:   cluster.Spec.MasterService,
		MasterUUID:   phase5DNewPrimaryUUID,
		MasterUser:   "replica",
		AutoPosition: "1",
		IORunning:    "Yes",
		SQLRunning:   "Yes",
	}
}

func phase5DStaleChannel(cluster *databasev1.MysqlCluster, ioRunning, sqlRunning string) mysqlReplicationChannelObservation {
	channel := phase5DHealthyChannel(cluster)
	channel.MasterUUID = phase5DOldPrimaryUUID
	channel.IORunning = ioRunning
	channel.SQLRunning = sqlRunning
	return channel
}

func (p *phase5DExecPlan) execute(pod *corev1.Pod, command string) (string, error) {
	if p.beforeCommand != nil {
		p.beforeCommand(pod, command)
	}
	p.commands = append(p.commands, pod.Name+":"+command)
	state := p.states[pod.Name]
	if state == nil {
		return "", fmt.Errorf("unexpected Phase 5-D Pod %s", pod.Name)
	}
	switch command {
	case mysqlWriteSafetyObservationCommand():
		gtidMode, consistency := "OFF", "WARN"
		if state.gtidReady {
			gtidMode, consistency = "ON", "ON"
		}
		return fmt.Sprintf("%s\t%s\t%s\t%s\n", phase5DBool(state.readOnly), phase5DBool(state.superRO), gtidMode, consistency), nil
	case mysqlSourceCapabilityCommand():
		return phase5DBool(state.source) + "\t" + phase5DBool(state.source) + "\n", nil
	case mysqlShowSlaveStatusCommand():
		return phase5DChannelOutput(state.channel), nil
	case mysqlElectionReferenceCommand():
		return phase5BElectionReferenceOutput(state.serverUUID, state.gtidSet), nil
	case mysqlSetSuperReadOnlyCommand():
		p.mutations = append(p.mutations, pod.Name+":SUPER READ ONLY ON")
		state.readOnly = true
		state.superRO = true
		return "", nil
	case mysqlStopSlaveCommand():
		p.mutations = append(p.mutations, pod.Name+":STOP SLAVE")
		state.channel.IORunning = "No"
		state.channel.SQLRunning = "No"
		return "", nil
	case mysqlStartSlaveCommand():
		p.mutations = append(p.mutations, pod.Name+":START SLAVE")
		state.channel.IORunning = "Yes"
		state.channel.SQLRunning = "Yes"
		return "", nil
	case mysqlInitializeReplicaCommand(p.cluster.Spec.MasterService), mysqlConfigureReplicaCommand(p.cluster.Spec.MasterService):
		action := "INITIALIZE"
		if command == mysqlConfigureReplicaCommand(p.cluster.Spec.MasterService) {
			action = "RECONFIGURE"
		}
		p.mutations = append(p.mutations, pod.Name+":"+action)
		state.channel = phase5DHealthyChannel(p.cluster)
		return "", nil
	}
	if pod.Name == p.candidate {
		for _, memberState := range p.states {
			if command != mysqlMemberAncestryAgainstCurrentPrimaryCommand(memberState.gtidSet) {
				continue
			}
			return fmt.Sprintf(
				"%s\t%s\n",
				phase5DBool(memberState.subset),
				base64.StdEncoding.EncodeToString([]byte(state.gtidSet)),
			), nil
		}
	}
	return "", fmt.Errorf("unexpected Phase 5-D command on %s: %s", pod.Name, command)
}

type phase5DInventoryDriftClient struct {
	client.Client
	driftKey       client.ObjectKey
	driftOnList    int
	inventoryCalls int
}

func (c *phase5DInventoryDriftClient) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	if _, isPodList := list.(*corev1.PodList); isPodList {
		c.inventoryCalls++
		if c.inventoryCalls == c.driftOnList {
			pod := &corev1.Pod{}
			if err := c.Client.Get(ctx, c.driftKey, pod); err != nil {
				return err
			}
			pod.Labels[LabelMysqlRole] = string(mysqlPublishedRoleMaster)
			pod.Labels[LegacyLabelRole] = string(mysqlPublishedRoleMaster)
			if err := c.Client.Update(ctx, pod); err != nil {
				return err
			}
		}
	}
	return c.Client.List(ctx, list, opts...)
}

type phase5DFixture struct {
	cluster     *databasev1.MysqlCluster
	statefulSet *appsv1.StatefulSet
	former      *corev1.Pod
	candidate   *corev1.Pod
	ordinary    *corev1.Pod
	plan        *phase5DExecPlan
	reconciler  *MysqlClusterReconciler
}

func newPhase5DFixture(t *testing.T, name string) *phase5DFixture {
	t.Helper()
	cluster, statefulSet, former, replicas := phase5BFixture(t, name, 2, 3)
	candidate, ordinary := replicas[0], replicas[1]
	former.Labels[LabelMysqlRole] = string(mysqlPublishedRoleSlave)
	former.Labels[LegacyLabelRole] = string(mysqlPublishedRoleSlave)
	former.Status = candidate.Status
	candidate.Labels[LabelMysqlRole] = string(mysqlPublishedRoleMaster)
	candidate.Labels[LegacyLabelRole] = string(mysqlPublishedRoleMaster)

	gtidSnapshot := phase5CGTIDSet
	cluster.Status.HA = &databasev1.MysqlClusterHAStatus{
		State:      databasev1.MysqlClusterHAStateFailoverInProgress,
		Primary:    candidate.Name,
		PrimaryUID: string(candidate.UID),
		Failover: &databasev1.MysqlClusterFailoverStatus{
			Stage:                   databasev1.MysqlClusterFailoverStageReconfiguring,
			FailedPrimary:           former.Name,
			FailedPrimaryUID:        string(former.UID),
			FenceState:              databasev1.MysqlClusterFenceStateVerified,
			FenceMethod:             databasev1.MysqlClusterFenceMethodMySQLSuperReadOnly,
			FencedPrimaryUID:        string(former.UID),
			Candidate:               candidate.Name,
			CandidateUID:            string(candidate.UID),
			FailedPrimaryServerUUID: phase5DOldPrimaryUUID,
			FailedPrimaryGTIDSet:    &gtidSnapshot,
		},
	}

	states := map[string]*phase5DMemberState{
		candidate.Name: {
			gtidReady:  true,
			source:     true,
			serverUUID: phase5DNewPrimaryUUID,
			gtidSet:    phase5DPrimaryGTID,
			channel:    mysqlReplicationChannelObservation{Configured: true, IORunning: "No", SQLRunning: "No"},
			subset:     true,
		},
		former.Name: {
			readOnly:   true,
			superRO:    true,
			gtidReady:  true,
			serverUUID: phase5DOldPrimaryUUID,
			gtidSet:    phase5CGTIDSet,
			channel:    phase5DHealthyChannel(cluster),
			subset:     true,
		},
		ordinary.Name: {
			readOnly:   true,
			superRO:    true,
			gtidReady:  true,
			serverUUID: "cccccccc-dddd-eeee-ffff-000000000000",
			gtidSet:    "cccccccc-dddd-eeee-ffff-000000000000:1-8",
			channel:    phase5DHealthyChannel(cluster),
			subset:     true,
		},
	}
	plan := &phase5DExecPlan{cluster: cluster, candidate: candidate.Name, states: states}
	objects := phase5BObjects(cluster, statefulSet, former, []*corev1.Pod{candidate, ordinary})
	objects = append(objects, phase1HEndpoints(cluster, candidate))
	reconciler := phase1HReconciler(t, objects...)
	reconciler.execCommandOnPodFn = plan.execute
	return &phase5DFixture{
		cluster: cluster, statefulSet: statefulSet, former: former, candidate: candidate,
		ordinary: ordinary, plan: plan, reconciler: reconciler,
	}
}

func phase5DStoredRole(t *testing.T, fixture *phase5DFixture, pod *corev1.Pod) mysqlPublishedRole {
	t.Helper()
	role, err := observeMysqlPublishedRole(phase5StoredPod(t, fixture.reconciler, pod))
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return role
}

func phase5DReconcile(t *testing.T, fixture *phase5DFixture) {
	t.Helper()
	_, converged, err := fixture.reconciler.reconcileMasterSlave(context.Background(), *fixture.cluster)
	g := NewWithT(t)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(converged).To(BeFalse())
	fixture.cluster = phase4StoredCluster(t, fixture.reconciler, fixture.cluster)
}

func phase5DRestart(t *testing.T, fixture *phase5DFixture) {
	t.Helper()
	objects := []client.Object{
		phase4StoredCluster(t, fixture.reconciler, fixture.cluster),
		fixture.statefulSet.DeepCopy(),
		phase5StoredPod(t, fixture.reconciler, fixture.former),
		phase5StoredPod(t, fixture.reconciler, fixture.candidate),
		phase5StoredPod(t, fixture.reconciler, fixture.ordinary),
		phase1HEndpoints(fixture.cluster, fixture.candidate),
	}
	fixture.cluster = objects[0].(*databasev1.MysqlCluster)
	fixture.reconciler = phase1HReconciler(t, objects...)
	fixture.reconciler.execCommandOnPodFn = fixture.plan.execute
}

func TestPhase5DCommandsAndMemberFirstGTIDSubsetProof(t *testing.T) {
	g := NewWithT(t)
	memberGTID := "member-uuid:1-7' unsafe raw text"
	command := mysqlMemberAncestryAgainstCurrentPrimaryCommand(memberGTID)
	g.Expect(command).NotTo(ContainSubstring(memberGTID))
	g.Expect(command).To(ContainSubstring(base64.StdEncoding.EncodeToString([]byte(memberGTID))))
	g.Expect(command).To(ContainSubstring("GTID_SUBSET(FROM_BASE64("))
	g.Expect(command).To(ContainSubstring("@@GLOBAL.gtid_executed"))
	g.Expect(mysqlStartSlaveCommand()).To(Equal(mysqlRootClientCommand + ` -e "START SLAVE;"`))

	parsed, err := parseMysqlMemberAncestryObservation("1\t" + base64.StdEncoding.EncodeToString([]byte("primary:1-9")) + "\n")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(parsed.MemberSubsetOfPrimary).To(BeTrue())
	g.Expect(parsed.CurrentPrimaryGTIDSet).To(Equal("primary:1-9"))
}

func TestPhase5DStaleSameHostReplicaCrossesQuarantineStopAndReconfigureBarriers(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-stale")
	f.plan.states[f.ordinary.Name].channel = phase5DStaleChannel(f.cluster, "Yes", "Yes")

	phase5DReconcile(t, f)
	g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleNone))
	g.Expect(f.plan.mutations).To(BeEmpty(), "routing quarantine must be the only first mutation")

	phase5DRestart(t, f)
	phase5DReconcile(t, f)
	g.Expect(f.plan.mutations).To(Equal([]string{f.ordinary.Name + ":STOP SLAVE"}))
	g.Expect(f.plan.states[f.ordinary.Name].channel.MasterHost).To(Equal(f.cluster.Spec.MasterService))
	g.Expect(f.plan.states[f.ordinary.Name].channel.MasterUUID).To(Equal(phase5DOldPrimaryUUID))

	phase5DRestart(t, f)
	phase5DReconcile(t, f)
	g.Expect(f.plan.mutations).To(Equal([]string{
		f.ordinary.Name + ":STOP SLAVE",
		f.ordinary.Name + ":RECONFIGURE",
	}))
	g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleNone))

	phase5DRestart(t, f)
	phase5DReconcile(t, f)
	g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleSlave))
	g.Expect(f.plan.mutations).To(HaveLen(2), "fresh semantic proof must precede role publication")
}

func TestPhase5DNoChannelInitializesAndPartialConfigurationRestartsSafely(t *testing.T) {
	g := NewWithT(t)

	t.Run("no channel", func(t *testing.T) {
		f := newPhase5DFixture(t, "phase5d-no-channel")
		delete(f.ordinary.Labels, LabelMysqlRole)
		delete(f.ordinary.Labels, LegacyLabelRole)
		f.plan.states[f.ordinary.Name].channel = mysqlReplicationChannelObservation{}
		f.reconciler = phase1HReconciler(t, phase5BObjects(f.cluster, f.statefulSet, f.former, []*corev1.Pod{f.candidate, f.ordinary})...)
		f.reconciler.execCommandOnPodFn = f.plan.execute

		phase5DReconcile(t, f)
		g.Expect(f.plan.mutations).To(Equal([]string{f.ordinary.Name + ":INITIALIZE"}))
		g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleNone))
	})

	t.Run("partial correct channel", func(t *testing.T) {
		f := newPhase5DFixture(t, "phase5d-partial")
		delete(f.ordinary.Labels, LabelMysqlRole)
		delete(f.ordinary.Labels, LegacyLabelRole)
		channel := phase5DHealthyChannel(f.cluster)
		channel.IORunning, channel.SQLRunning = "No", "No"
		f.plan.states[f.ordinary.Name].channel = channel
		f.reconciler = phase1HReconciler(t, phase5BObjects(f.cluster, f.statefulSet, f.former, []*corev1.Pod{f.candidate, f.ordinary})...)
		f.reconciler.execCommandOnPodFn = f.plan.execute

		phase5DReconcile(t, f)
		g.Expect(f.plan.mutations).To(Equal([]string{f.ordinary.Name + ":START SLAVE"}))
		g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleNone))
		memberSnapshotIndex := indexOfPhase5DCommand(f.plan.commands, f.ordinary.Name+":"+mysqlElectionReferenceCommand())
		freshWriteIndex := indexOfPhase5DCommandAfter(f.plan.commands, f.candidate.Name+":"+mysqlWriteSafetyObservationCommand(), memberSnapshotIndex)
		freshSourceIndex := indexOfPhase5DCommandAfter(f.plan.commands, f.candidate.Name+":"+mysqlSourceCapabilityCommand(), memberSnapshotIndex)
		freshChannelIndex := indexOfPhase5DCommandAfter(f.plan.commands, f.candidate.Name+":"+mysqlShowSlaveStatusCommand(), memberSnapshotIndex)
		freshIdentityIndex := indexOfPhase5DCommandAfter(f.plan.commands, f.candidate.Name+":"+mysqlElectionReferenceCommand(), memberSnapshotIndex)
		ancestryIndex := indexOfPhase5DCommand(f.plan.commands, f.candidate.Name+":"+mysqlMemberAncestryAgainstCurrentPrimaryCommand(f.plan.states[f.ordinary.Name].gtidSet))
		startIndex := indexOfPhase5DCommand(f.plan.commands, f.ordinary.Name+":"+mysqlStartSlaveCommand())
		for _, proofIndex := range []int{freshWriteIndex, freshSourceIndex, freshChannelIndex, freshIdentityIndex} {
			g.Expect(proofIndex).To(BeNumerically(">", memberSnapshotIndex))
			g.Expect(proofIndex).To(BeNumerically("<", ancestryIndex))
		}
		g.Expect(ancestryIndex).To(BeNumerically("<", startIndex))
		phase5DRestart(t, f)
		phase5DReconcile(t, f)
		g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleSlave))
		g.Expect(f.plan.mutations).To(HaveLen(1))
	})
}

func TestPhase5DWaitDoesNotBlockIndependentRepair(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-wait")
	f.former.Labels[LabelMysqlRole] = string(mysqlPublishedRoleSlave)
	f.former.Labels[LegacyLabelRole] = string(mysqlPublishedRoleSlave)
	formerChannel := phase5DHealthyChannel(f.cluster)
	formerChannel.IORunning = "Connecting"
	f.plan.states[f.former.Name].channel = formerChannel
	delete(f.ordinary.Labels, LabelMysqlRole)
	delete(f.ordinary.Labels, LegacyLabelRole)
	f.plan.states[f.ordinary.Name].channel = phase5DStaleChannel(f.cluster, "Yes", "Yes")
	f.reconciler = phase1HReconciler(t, phase5BObjects(f.cluster, f.statefulSet, f.former, []*corev1.Pod{f.candidate, f.ordinary})...)
	f.reconciler.execCommandOnPodFn = f.plan.execute

	phase5DReconcile(t, f)
	g.Expect(phase5DStoredRole(t, f, f.former)).To(Equal(mysqlPublishedRoleNone))
	g.Expect(f.plan.mutations).To(BeEmpty())
	phase5DRestart(t, f)
	phase5DReconcile(t, f)
	g.Expect(f.plan.mutations).To(Equal([]string{f.ordinary.Name + ":STOP SLAVE"}))
}

func TestPhase5DWritableMemberIsRefencedAsOnlyMutation(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-writable")
	state := f.plan.states[f.former.Name]
	state.readOnly, state.superRO = false, false

	phase5DReconcile(t, f)
	g.Expect(f.plan.mutations).To(Equal([]string{f.former.Name + ":SUPER READ ONLY ON"}))
	g.Expect(phase5DStoredRole(t, f, f.former)).To(Equal(mysqlPublishedRoleSlave))
}

func TestPhase5DUnsafeHistoryQuarantinesThenPersistsDegraded(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-unsafe")
	f.plan.states[f.ordinary.Name].gtidSet = "dddddddd-eeee-ffff-0000-111111111111:1"
	f.plan.states[f.ordinary.Name].subset = false

	phase5DReconcile(t, f)
	g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleNone))
	g.Expect(f.plan.mutations).To(BeEmpty())
	phase5DRestart(t, f)
	phase5DReconcile(t, f)
	g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
	g.Expect(f.cluster.Status.HA.Failover).NotTo(BeNil())
	g.Expect(f.cluster.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageReconfiguring))
	g.Expect(f.plan.mutations).To(BeEmpty())
}

func TestPhase5DFormerPrimaryUsesSameSafeRejoinAndUnsafeHistoryBarriers(t *testing.T) {
	g := NewWithT(t)

	t.Run("safe role-none former primary initializes then publishes", func(t *testing.T) {
		f := newPhase5DFixture(t, "phase5d-former-safe")
		delete(f.former.Labels, LabelMysqlRole)
		delete(f.former.Labels, LegacyLabelRole)
		f.plan.states[f.former.Name].channel = mysqlReplicationChannelObservation{}
		f.reconciler = phase1HReconciler(t, phase5BObjects(f.cluster, f.statefulSet, f.former, []*corev1.Pod{f.candidate, f.ordinary})...)
		f.reconciler.execCommandOnPodFn = f.plan.execute

		phase5DReconcile(t, f)
		g.Expect(f.plan.mutations).To(Equal([]string{f.former.Name + ":INITIALIZE"}))
		g.Expect(phase5DStoredRole(t, f, f.former)).To(Equal(mysqlPublishedRoleNone))
		phase5DRestart(t, f)
		phase5DReconcile(t, f)
		g.Expect(phase5DStoredRole(t, f, f.former)).To(Equal(mysqlPublishedRoleSlave))
		g.Expect(f.plan.mutations).To(HaveLen(1))
	})

	t.Run("unsafe former primary quarantines then degrades", func(t *testing.T) {
		f := newPhase5DFixture(t, "phase5d-former-unsafe")
		f.plan.states[f.former.Name].gtidSet = phase5DOldPrimaryUUID + ":1-99"
		f.plan.states[f.former.Name].subset = false

		phase5DReconcile(t, f)
		g.Expect(phase5DStoredRole(t, f, f.former)).To(Equal(mysqlPublishedRoleNone))
		g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverInProgress))
		phase5DRestart(t, f)
		phase5DReconcile(t, f)
		g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
		g.Expect(f.cluster.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageReconfiguring))
		g.Expect(f.plan.mutations).To(BeEmpty())
	})
}

func TestPhase5DFormerPrimaryIdentityAndAdvancingGTID(t *testing.T) {
	g := NewWithT(t)

	t.Run("UID replacement performs zero SQL", func(t *testing.T) {
		f := newPhase5DFixture(t, "phase5d-former-uid")
		f.former.UID = types.UID("replacement-former-uid")
		f.reconciler = phase1HReconciler(t, phase5BObjects(f.cluster, f.statefulSet, f.former, []*corev1.Pod{f.candidate, f.ordinary})...)
		f.reconciler.execCommandOnPodFn = f.plan.execute
		phase5DReconcile(t, f)
		g.Expect(f.plan.commands).To(BeEmpty())
		g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
		g.Expect(f.cluster.Status.HA.Failover).NotTo(BeNil())
	})

	t.Run("server_uuid replacement never mutates replication", func(t *testing.T) {
		f := newPhase5DFixture(t, "phase5d-former-server-uuid")
		f.plan.states[f.former.Name].serverUUID = "eeeeeeee-ffff-0000-1111-222222222222"
		phase5DReconcile(t, f)
		g.Expect(phase5DStoredRole(t, f, f.former)).To(Equal(mysqlPublishedRoleNone))
		g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverInProgress))
		phase5DRestart(t, f)
		phase5DReconcile(t, f)
		g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
		g.Expect(f.plan.mutations).To(BeEmpty())
		g.Expect(f.cluster.Status.HA.Failover.FailedPrimaryUID).To(Equal(string(f.former.UID)))
	})

	t.Run("current safe GTID may advance beyond election snapshot", func(t *testing.T) {
		f := newPhase5DFixture(t, "phase5d-former-advanced")
		advanced := phase5DOldPrimaryUUID + ":1-18"
		f.plan.states[f.former.Name].gtidSet = advanced
		phase5DReconcile(t, f)
		g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateVerifying))
		g.Expect(f.plan.commands).To(ContainElement(f.candidate.Name + ":" + mysqlMemberAncestryAgainstCurrentPrimaryCommand(advanced)))
		g.Expect(f.plan.commands).NotTo(ContainElement(f.former.Name + ":" + mysqlGTIDComparisonCommand(phase5CGTIDSet)))
	})
}

func TestPhase5DMultiplePublishedMastersFailClosedBeforeReplicaRepair(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-multiple-master")
	f.ordinary.Labels[LabelMysqlRole] = string(mysqlPublishedRoleMaster)
	f.ordinary.Labels[LegacyLabelRole] = string(mysqlPublishedRoleMaster)
	f.reconciler = phase1HReconciler(t, phase5BObjects(f.cluster, f.statefulSet, f.former, []*corev1.Pod{f.candidate, f.ordinary})...)
	f.reconciler.execCommandOnPodFn = f.plan.execute

	phase5DReconcile(t, f)
	g.Expect(f.plan.mutations).To(Equal([]string{f.candidate.Name + ":SUPER READ ONLY ON"}))
	g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleMaster))
}

func phase5DRebuildWithOrdinaryRoleNone(t *testing.T, f *phase5DFixture) {
	t.Helper()
	delete(f.ordinary.Labels, LabelMysqlRole)
	delete(f.ordinary.Labels, LegacyLabelRole)
	f.reconciler = phase1HReconciler(t, phase5BObjects(f.cluster, f.statefulSet, f.former, []*corev1.Pod{f.candidate, f.ordinary})...)
	f.reconciler.execCommandOnPodFn = f.plan.execute
}

func phase5DExpectNoSourceAttachment(t *testing.T, commands []string) {
	t.Helper()
	g := NewWithT(t)
	for _, command := range commands {
		g.Expect(command).NotTo(ContainSubstring("CHANGE MASTER TO"))
		g.Expect(command).NotTo(HaveSuffix(":" + mysqlStartSlaveCommand()))
	}
}

func TestPhase5DSameReconcileMasterDriftBlocksSourceAttachmentAndSlavePublication(t *testing.T) {
	testCases := []struct {
		name      string
		configure func(*phase5DFixture)
	}{
		{
			name: "before stale source reconfiguration",
			configure: func(f *phase5DFixture) {
				f.plan.states[f.ordinary.Name].channel = phase5DStaleChannel(f.cluster, "No", "No")
			},
		},
		{
			name: "before role publication",
			configure: func(f *phase5DFixture) {
				f.plan.states[f.ordinary.Name].channel = phase5DHealthyChannel(f.cluster)
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			fixtureName := "phase5d-drift-publish"
			if strings.Contains(testCase.name, "reconfiguration") {
				fixtureName = "phase5d-drift-configure"
			}
			f := newPhase5DFixture(t, fixtureName)
			testCase.configure(f)
			phase5DRebuildWithOrdinaryRoleNone(t, f)
			driftClient := &phase5DInventoryDriftClient{
				Client:      f.reconciler.Client,
				driftKey:    client.ObjectKeyFromObject(f.former),
				driftOnList: 4,
			}
			f.reconciler.Client = driftClient

			phase5DReconcile(t, f)
			g.Expect(driftClient.inventoryCalls).To(Equal(4))
			g.Expect(f.plan.mutations).To(Equal([]string{f.candidate.Name + ":SUPER READ ONLY ON"}))
			phase5DExpectNoSourceAttachment(t, f.plan.commands)
			g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleNone))
			g.Expect(phase5DStoredRole(t, f, f.former)).To(Equal(mysqlPublishedRoleMaster))
			g.Expect(f.cluster.Status.HA.Failover).NotTo(BeNil())
			g.Expect(f.cluster.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageReconfiguring))
		})
	}
}

func TestPhase5DFinalAuthorityRecheckBlocksVerifyingOnSameReconcileMasterDrift(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-final-master-drift")
	driftClient := &phase5DInventoryDriftClient{
		Client:      f.reconciler.Client,
		driftKey:    client.ObjectKeyFromObject(f.ordinary),
		driftOnList: 5,
	}
	f.reconciler.Client = driftClient

	phase5DReconcile(t, f)
	g.Expect(driftClient.inventoryCalls).To(Equal(5))
	g.Expect(f.plan.mutations).To(Equal([]string{f.candidate.Name + ":SUPER READ ONLY ON"}))
	g.Expect(f.cluster.Status.HA.State).NotTo(Equal(databasev1.MysqlClusterHAStateVerifying))
	g.Expect(f.cluster.Status.HA.Failover).NotTo(BeNil())
	g.Expect(f.cluster.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageReconfiguring))
	g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleMaster))
}

func TestPhase5DPrimarySourceSafetyDriftBlocksSourceAttachment(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-primary-source-drift")
	f.plan.states[f.ordinary.Name].channel = mysqlReplicationChannelObservation{}
	phase5DRebuildWithOrdinaryRoleNone(t, f)
	sourceObservations := 0
	f.plan.beforeCommand = func(pod *corev1.Pod, command string) {
		if pod.Name != f.candidate.Name || command != mysqlSourceCapabilityCommand() {
			return
		}
		sourceObservations++
		if sourceObservations == 3 {
			f.plan.states[f.candidate.Name].source = false
		}
	}

	phase5DReconcile(t, f)
	g.Expect(sourceObservations).To(Equal(3))
	g.Expect(f.plan.mutations).To(Equal([]string{f.candidate.Name + ":SUPER READ ONLY ON"}))
	phase5DExpectNoSourceAttachment(t, f.plan.commands)
	g.Expect(phase5DStoredRole(t, f, f.ordinary)).To(Equal(mysqlPublishedRoleNone))
	g.Expect(f.cluster.Status.HA.Failover).NotTo(BeNil())
}

func TestPhase5DCompletionIsStatusOnlyThenExistingVerifierReachesHealthy(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-complete")
	f.cluster.Status.HA.State = databasev1.MysqlClusterHAStateDegraded

	phase5DReconcile(t, f)
	g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateVerifying))
	g.Expect(f.cluster.Status.HA.Primary).To(Equal(f.candidate.Name))
	g.Expect(f.cluster.Status.HA.PrimaryUID).To(Equal(string(f.candidate.UID)))
	g.Expect(f.cluster.Status.HA.Failover).To(BeNil())
	g.Expect(f.plan.mutations).To(BeEmpty())

	phase5DReconcile(t, f)
	g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateHealthy))
	g.Expect(f.plan.mutations).To(BeEmpty())
}

func TestPhase5DMalformedInventoryFailsClosedBeforeSQL(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-malformed")
	delete(f.ordinary.Labels, LegacyLabelRole)
	f.reconciler = phase1HReconciler(t, phase5BObjects(f.cluster, f.statefulSet, f.former, []*corev1.Pod{f.candidate, f.ordinary})...)
	f.reconciler.execCommandOnPodFn = f.plan.execute

	_, _, err := f.reconciler.reconcileMasterSlave(context.Background(), *f.cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(f.plan.commands).To(BeEmpty())
	g.Expect(f.plan.mutations).To(BeEmpty())
}

func TestPhase5DValidatorSupportsRecoverableDegradedAndRejectsMalformedStatus(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-validator")
	f.cluster.Status.HA.State = databasev1.MysqlClusterHAStateDegraded
	g.Expect(validateMysqlReconfiguringStatus(f.cluster)).To(Succeed())
	f.cluster.Status.HA.Primary = f.former.Name
	g.Expect(validateMysqlReconfiguringStatus(f.cluster)).NotTo(Succeed())
}

func TestPhase5DNewPrimaryMayAdvanceAfterMemberSnapshot(t *testing.T) {
	g := NewWithT(t)
	f := newPhase5DFixture(t, "phase5d-primary-advances")
	memberSnapshot := f.plan.states[f.ordinary.Name].gtidSet
	f.plan.states[f.candidate.Name].gtidSet = phase5DPrimaryGTID + ":21-25"

	phase5DReconcile(t, f)
	g.Expect(f.cluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateVerifying))
	memberObservation := f.ordinary.Name + ":" + mysqlElectionReferenceCommand()
	ancestryProof := f.candidate.Name + ":" + mysqlMemberAncestryAgainstCurrentPrimaryCommand(memberSnapshot)
	g.Expect(f.plan.commands).To(ContainElement(memberObservation))
	g.Expect(f.plan.commands).To(ContainElement(ancestryProof))
	g.Expect(indexOfPhase5DCommand(f.plan.commands, memberObservation)).To(BeNumerically("<", indexOfPhase5DCommand(f.plan.commands, ancestryProof)))
}

func indexOfPhase5DCommand(commands []string, expected string) int {
	for i, command := range commands {
		if command == expected {
			return i
		}
	}
	return -1
}

func indexOfPhase5DCommandAfter(commands []string, expected string, after int) int {
	for i := after + 1; i < len(commands); i++ {
		if commands[i] == expected {
			return i
		}
	}
	return -1
}

func TestPhase5DActivePathHasNoLegacyOrForbiddenReset(t *testing.T) {
	g := NewWithT(t)
	source, err := os.ReadFile("replica_rejoin.go")
	g.Expect(err).NotTo(HaveOccurred())
	activePath := string(source)
	for _, forbidden := range []string{
		"handleMasterFailure(",
		"electNewMaster(",
		"setupMysqlPrimary(",
		"setupMysqlReplicas(",
		"setupMasterSlaveReplication(",
		"mysqlPreparePrimaryCommand(",
		"RESET MASTER",
		"RESET SLAVE",
	} {
		g.Expect(activePath).NotTo(ContainSubstring(forbidden))
	}
}

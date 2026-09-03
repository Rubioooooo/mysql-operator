package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const phase5CGTIDSet = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10"

type phase5CExecPlan struct {
	primaryName         string
	candidateName       string
	primaryFenced       bool
	primaryGTIDEqual    bool
	candidateReadOnly   bool
	candidateSuperRO    bool
	candidateGTIDReady  bool
	candidateSource     bool
	candidateGTIDEqual  bool
	channelConfigured   bool
	channelMasterHost   string
	channelMasterUUID   string
	channelAutoPosition string
	ioRunning           string
	sqlRunning          string
	commands            []string
	mutations           []string
	errors              map[string]error
}

func newPhase5CExecPlan(cluster *databasev1.MysqlCluster, primary, candidate *corev1.Pod) *phase5CExecPlan {
	return &phase5CExecPlan{
		primaryName:         primary.Name,
		candidateName:       candidate.Name,
		primaryFenced:       true,
		primaryGTIDEqual:    true,
		candidateReadOnly:   true,
		candidateSuperRO:    true,
		candidateGTIDReady:  true,
		candidateSource:     true,
		candidateGTIDEqual:  true,
		channelConfigured:   true,
		channelMasterHost:   cluster.Spec.MasterService,
		channelMasterUUID:   phase5BPrimaryServerUUID,
		channelAutoPosition: "1",
		ioRunning:           "Yes",
		sqlRunning:          "Yes",
		errors:              make(map[string]error),
	}
}

func phase5CBoolean(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func phase5CSlaveStatus(plan *phase5CExecPlan) string {
	if !plan.channelConfigured {
		return ""
	}
	output := mysqlSlaveStatusOutputForTest(
		plan.channelMasterHost,
		"replica",
		plan.channelAutoPosition,
		plan.ioRunning,
		plan.sqlRunning,
		"",
		"",
	)
	return strings.Replace(
		output,
		"               Master_User: replica\n",
		"               Master_User: replica\n               Master_UUID: "+plan.channelMasterUUID+"\n",
		1,
	)
}

func (p *phase5CExecPlan) execute(pod *corev1.Pod, command string) (string, error) {
	p.commands = append(p.commands, pod.Name+":"+command)
	if err := p.errors[pod.Name+"\x00"+command]; err != nil {
		return "", err
	}
	switch command {
	case mysqlWriteSafetyObservationCommand():
		if pod.Name == p.primaryName {
			if p.primaryFenced {
				return "1\t1\tON\tON\n", nil
			}
			return "0\t0\tON\tON\n", nil
		}
		gtidMode := "ON"
		enforce := "ON"
		if !p.candidateGTIDReady {
			gtidMode = "OFF"
			enforce = "WARN"
		}
		return fmt.Sprintf("%s\t%s\t%s\t%s\n", phase5CBoolean(p.candidateReadOnly), phase5CBoolean(p.candidateSuperRO), gtidMode, enforce), nil
	case mysqlElectionReferenceCommand():
		if pod.Name == p.primaryName {
			return phase5BElectionReferenceOutput(phase5BPrimaryServerUUID, phase5CGTIDSet), nil
		}
		if pod.Name == p.candidateName && p.primaryFenced {
			return phase5BElectionReferenceOutput("bbbbbbbb-cccc-dddd-eeee-ffffffffffff", phase5CGTIDSet), nil
		}
	case mysqlSourceCapabilityCommand():
		return phase5CBoolean(p.candidateSource) + "\t" + phase5CBoolean(p.candidateSource) + "\n", nil
	case mysqlShowSlaveStatusCommand():
		return phase5CSlaveStatus(p), nil
	case mysqlStopSlaveCommand():
		p.mutations = append(p.mutations, "STOP SLAVE")
		p.ioRunning = "No"
		p.sqlRunning = "No"
		return "", nil
	case mysqlSetReadOnlyOffCommand():
		p.mutations = append(p.mutations, "READ ONLY OFF")
		p.candidateReadOnly = false
		p.candidateSuperRO = false
		return "", nil
	case mysqlSetSuperReadOnlyCommand():
		p.mutations = append(p.mutations, "SUPER READ ONLY ON")
		p.candidateReadOnly = true
		p.candidateSuperRO = true
		return "", nil
	}
	if strings.Contains(command, "GTID_SUBSET(") {
		if pod.Name == p.primaryName {
			return phase5BGTIDComparisonOutput(p.primaryGTIDEqual, p.primaryGTIDEqual, phase5CGTIDSet), nil
		}
		return phase5BGTIDComparisonOutput(p.candidateGTIDEqual, p.candidateGTIDEqual, phase5CGTIDSet), nil
	}
	return "", fmt.Errorf("unexpected Phase 5-C command on %s: %s", pod.Name, command)
}

func phase5CTakeoverFixture(
	t *testing.T,
	name string,
	stage databasev1.MysqlClusterFailoverStage,
) (*databasev1.MysqlCluster, *appsv1.StatefulSet, *corev1.Pod, *corev1.Pod, *phase5CExecPlan) {
	t.Helper()
	cluster, statefulSet, primary, replicas := phase5BFixture(t, name, 2)
	desiredReplicas := int32(2)
	cluster.Spec.Replicas = &desiredReplicas
	candidate := replicas[0]
	cluster.Status.HA.Failover.Stage = stage
	cluster.Status.HA.Failover.Candidate = candidate.Name
	cluster.Status.HA.Failover.CandidateUID = string(candidate.UID)
	cluster.Status.HA.Failover.FailedPrimaryServerUUID = phase5BPrimaryServerUUID
	gtidSet := phase5CGTIDSet
	cluster.Status.HA.Failover.FailedPrimaryGTIDSet = &gtidSet
	if stage == databasev1.MysqlClusterFailoverStageReconfiguring {
		cluster.Status.HA.Primary = candidate.Name
		cluster.Status.HA.PrimaryUID = string(candidate.UID)
	}
	plan := newPhase5CExecPlan(cluster, primary, candidate)
	return cluster, statefulSet, primary, candidate, plan
}

func phase5CStoredRole(t *testing.T, reconciler *MysqlClusterReconciler, candidate *corev1.Pod) mysqlPublishedRole {
	t.Helper()
	role, err := observeMysqlPublishedRole(phase5StoredPod(t, reconciler, candidate))
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return role
}

func phase5CReconcile(t *testing.T, reconciler *MysqlClusterReconciler, cluster *databasev1.MysqlCluster) *databasev1.MysqlCluster {
	t.Helper()
	_, converged, err := reconciler.reconcileMasterSlave(context.Background(), *cluster)
	g := NewWithT(t)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(converged).To(BeFalse())
	return phase4StoredCluster(t, reconciler, cluster)
}

func phase5CRestart(
	t *testing.T,
	reconciler *MysqlClusterReconciler,
	cluster *databasev1.MysqlCluster,
	statefulSet *appsv1.StatefulSet,
	primary *corev1.Pod,
	candidate *corev1.Pod,
	plan *phase5CExecPlan,
) (*MysqlClusterReconciler, *databasev1.MysqlCluster) {
	t.Helper()
	storedCluster := phase4StoredCluster(t, reconciler, cluster)
	storedPrimary := phase5StoredPod(t, reconciler, primary)
	storedCandidate := phase5StoredPod(t, reconciler, candidate)
	restarted := phase1HReconciler(t, storedCluster, statefulSet.DeepCopy(), storedPrimary, storedCandidate)
	restarted.execCommandOnPodFn = plan.execute
	return restarted, storedCluster
}

func TestPhase5CMutationCommandsAreMinimalMySQL57Barriers(t *testing.T) {
	g := NewWithT(t)
	g.Expect(mysqlStopSlaveCommand()).To(Equal(mysqlRootClientCommand + ` -e "STOP SLAVE;"`))
	g.Expect(mysqlSetReadOnlyOffCommand()).To(Equal(mysqlRootClientCommand + ` -e "SET GLOBAL read_only = OFF;"`))
	g.Expect(mysqlSetReadOnlyOffCommand()).NotTo(ContainSubstring("super_read_only"))
	phase5CExpectNoForbiddenSQL(t, []string{mysqlStopSlaveCommand(), mysqlSetReadOnlyOffCommand(), mysqlSetSuperReadOnlyCommand()})
}

func TestPhase5CUnexpectedMasterBlocksTakeoverBeforeWriteEnable(t *testing.T) {
	g := NewWithT(t)
	cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-zero-master-readonly", databasev1.MysqlClusterFailoverStagePromoting)
	delete(candidate.Labels, LabelMysqlRole)
	delete(candidate.Labels, LegacyLabelRole)
	third := phase1HPod(t, cluster, statefulSet, 3, "master", true)
	plan.ioRunning = "No"
	plan.sqlRunning = "No"
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate, third})...)
	reconciler.execCommandOnPodFn = plan.execute

	stored := phase5CReconcile(t, reconciler, cluster)
	g.Expect(plan.mutations).To(BeEmpty())
	g.Expect(plan.candidateReadOnly).To(BeTrue())
	g.Expect(plan.candidateSuperRO).To(BeTrue())
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleNone))
	g.Expect(phase5CStoredRole(t, reconciler, third)).To(Equal(mysqlPublishedRoleMaster))
	g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStagePromoting))
}

func TestPhase5CUnexpectedMasterRefencesWritableCandidate(t *testing.T) {
	g := NewWithT(t)
	cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-zero-master-writable", databasev1.MysqlClusterFailoverStagePromoting)
	delete(candidate.Labels, LabelMysqlRole)
	delete(candidate.Labels, LegacyLabelRole)
	third := phase1HPod(t, cluster, statefulSet, 3, "master", true)
	plan.ioRunning = "No"
	plan.sqlRunning = "No"
	plan.candidateReadOnly = false
	plan.candidateSuperRO = false
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate, third})...)
	reconciler.execCommandOnPodFn = plan.execute

	stored := phase5CReconcile(t, reconciler, cluster)
	g.Expect(plan.mutations).To(Equal([]string{"SUPER READ ONLY ON"}))
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleNone))
	g.Expect(phase5CStoredRole(t, reconciler, third)).To(Equal(mysqlPublishedRoleMaster))
	g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStagePromoting))
}

type phase5CSecondInventoryMasterClient struct {
	client.Client
	thirdKey       client.ObjectKey
	inventoryCalls int
}

func (c *phase5CSecondInventoryMasterClient) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	if _, isPodList := list.(*corev1.PodList); isPodList {
		c.inventoryCalls++
		if c.inventoryCalls == 2 {
			third := &corev1.Pod{}
			if err := c.Client.Get(ctx, c.thirdKey, third); err != nil {
				return err
			}
			third.Labels[LabelMysqlRole] = string(mysqlPublishedRoleMaster)
			third.Labels[LegacyLabelRole] = string(mysqlPublishedRoleMaster)
			if err := c.Client.Update(ctx, third); err != nil {
				return err
			}
		}
	}
	return c.Client.List(ctx, list, opts...)
}

func TestPhase5CFinalZeroMasterRecheckCatchesSameReconcileDrift(t *testing.T) {
	g := NewWithT(t)
	cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-final-zero-master", databasev1.MysqlClusterFailoverStagePromoting)
	delete(candidate.Labels, LabelMysqlRole)
	delete(candidate.Labels, LegacyLabelRole)
	third := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
	plan.ioRunning = "No"
	plan.sqlRunning = "No"
	plan.candidateReadOnly = false
	plan.candidateSuperRO = false
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate, third})...)
	guardedClient := &phase5CSecondInventoryMasterClient{
		Client:   reconciler.Client,
		thirdKey: client.ObjectKeyFromObject(third),
	}
	reconciler.Client = guardedClient
	reconciler.execCommandOnPodFn = plan.execute

	stored := phase5CReconcile(t, reconciler, cluster)
	g.Expect(guardedClient.inventoryCalls).To(Equal(2))
	g.Expect(plan.mutations).To(Equal([]string{"SUPER READ ONLY ON"}))
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleNone))
	g.Expect(phase5CStoredRole(t, reconciler, third)).To(Equal(mysqlPublishedRoleMaster))
	g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStagePromoting))
}

func TestPhase5CTakeoverBarriersAreDurableAndOrdered(t *testing.T) {
	g := NewWithT(t)
	cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-ordered", databasev1.MysqlClusterFailoverStagePromoting)
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate})...)
	reconciler.execCommandOnPodFn = plan.execute

	cluster = phase5CReconcile(t, reconciler, cluster)
	g.Expect(plan.mutations).To(Equal([]string{"STOP SLAVE"}))
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleSlave))
	g.Expect(plan.candidateReadOnly).To(BeTrue())

	reconciler, cluster = phase5CRestart(t, reconciler, cluster, statefulSet, primary, candidate, plan)
	cluster = phase5CReconcile(t, reconciler, cluster)
	g.Expect(plan.mutations).To(Equal([]string{"STOP SLAVE"}), "role quarantine must be the only second-barrier mutation")
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleNone))

	reconciler, cluster = phase5CRestart(t, reconciler, cluster, statefulSet, primary, candidate, plan)
	cluster = phase5CReconcile(t, reconciler, cluster)
	g.Expect(plan.mutations).To(Equal([]string{"STOP SLAVE", "READ ONLY OFF"}))
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleNone), "write enable must not publish master")

	reconciler, cluster = phase5CRestart(t, reconciler, cluster, statefulSet, primary, candidate, plan)
	cluster = phase5CReconcile(t, reconciler, cluster)
	g.Expect(plan.mutations).To(Equal([]string{"STOP SLAVE", "READ ONLY OFF"}), "master publication must not execute SQL")
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleMaster))
	g.Expect(cluster.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStagePromoting))
	publishedPrimary, err := reconciler.observeSinglePublishedPrimary(context.Background(), cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(publishedPrimary.Name).To(Equal(candidate.Name))
	g.Expect(string(publishedPrimary.UID)).To(Equal(string(candidate.UID)))

	plan.candidateGTIDEqual = false
	reconciler, cluster = phase5CRestart(t, reconciler, cluster, statefulSet, primary, candidate, plan)
	commandsBefore := len(plan.commands)
	cluster = phase5CReconcile(t, reconciler, cluster)
	g.Expect(cluster.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageReconfiguring))
	g.Expect(cluster.Status.HA.Primary).To(Equal(candidate.Name))
	g.Expect(cluster.Status.HA.PrimaryUID).To(Equal(string(candidate.UID)))
	for _, command := range plan.commands[commandsBefore:] {
		if strings.HasPrefix(command, candidate.Name+":") {
			g.Expect(command).NotTo(ContainSubstring("GTID_SUBSET("), "post-publication candidate GTID must not be compared with the old snapshot")
		}
	}

	reconciler, cluster = phase5CRestart(t, reconciler, cluster, statefulSet, primary, candidate, plan)
	commandsBefore = len(plan.commands)
	cluster = phase5CReconcile(t, reconciler, cluster)
	g.Expect(cluster.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageReconfiguring))
	g.Expect(plan.mutations).To(Equal([]string{"STOP SLAVE", "READ ONLY OFF"}))
	phase5CExpectNoForbiddenSQL(t, plan.commands[commandsBefore:])
}

func TestPhase5CPrePublicationGTIDDriftRefencesAndNeverPublishes(t *testing.T) {
	g := NewWithT(t)
	cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-drift", databasev1.MysqlClusterFailoverStagePromoting)
	delete(candidate.Labels, LabelMysqlRole)
	delete(candidate.Labels, LegacyLabelRole)
	plan.ioRunning = "No"
	plan.sqlRunning = "No"
	plan.candidateReadOnly = false
	plan.candidateSuperRO = false
	plan.candidateGTIDEqual = false
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate})...)
	reconciler.execCommandOnPodFn = plan.execute

	stored := phase5CReconcile(t, reconciler, cluster)
	g.Expect(plan.mutations).To(Equal([]string{"SUPER READ ONLY ON"}))
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleNone))
	g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStagePromoting))
}

func TestPhase5CFailedPrimaryFenceLossBlocksEveryTakeoverBarrier(t *testing.T) {
	testCases := []struct {
		name      string
		role      mysqlPublishedRole
		stopped   bool
		writable  bool
		stage     databasev1.MysqlClusterFailoverStage
		expectSRO bool
	}{
		{name: "before stop", role: mysqlPublishedRoleSlave},
		{name: "before replica quarantine", role: mysqlPublishedRoleSlave, stopped: true},
		{name: "before write enable", role: mysqlPublishedRoleNone, stopped: true},
		{name: "before master publication", role: mysqlPublishedRoleNone, stopped: true, writable: true, expectSRO: true},
		{name: "post publication verification", role: mysqlPublishedRoleMaster, stopped: true, writable: true, expectSRO: true},
		{name: "reconfiguring", role: mysqlPublishedRoleMaster, stopped: true, writable: true, stage: databasev1.MysqlClusterFailoverStageReconfiguring, expectSRO: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			stage := testCase.stage
			if stage == "" {
				stage = databasev1.MysqlClusterFailoverStagePromoting
			}
			cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-fence-loss-"+strings.ReplaceAll(testCase.name, " ", "-"), stage)
			candidate.Labels[LabelMysqlRole] = string(testCase.role)
			candidate.Labels[LegacyLabelRole] = string(testCase.role)
			if testCase.role == mysqlPublishedRoleNone {
				delete(candidate.Labels, LabelMysqlRole)
				delete(candidate.Labels, LegacyLabelRole)
			}
			if testCase.stopped {
				plan.ioRunning = "No"
				plan.sqlRunning = "No"
			}
			if testCase.writable {
				plan.candidateReadOnly = false
				plan.candidateSuperRO = false
			}
			plan.primaryFenced = false
			reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate})...)
			reconciler.execCommandOnPodFn = plan.execute

			stored := phase5CReconcile(t, reconciler, cluster)
			g.Expect(stored.Status.HA.Failover.Stage).To(Equal(stage))
			if testCase.role == mysqlPublishedRoleMaster {
				g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleMaster), "the first fail-closed mutation must re-fence, not quarantine")
			} else {
				g.Expect(phase5CStoredRole(t, reconciler, candidate)).NotTo(Equal(mysqlPublishedRoleMaster), "fence loss must never publish master")
			}
			if testCase.expectSRO {
				g.Expect(plan.mutations).To(Equal([]string{"SUPER READ ONLY ON"}))
			} else {
				g.Expect(plan.mutations).To(BeEmpty())
			}
		})
	}
}

func TestPhase5CCandidateIdentityFailurePrecedesUnsafeSQL(t *testing.T) {
	testCases := []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{name: "UID replacement", mutate: func(candidate *corev1.Pod) { candidate.UID = types.UID("replacement-candidate") }},
		{name: "ownership ambiguity", mutate: func(candidate *corev1.Pod) {
			candidate.OwnerReferences[0].Name = "other-statefulset"
			candidate.OwnerReferences[0].UID = types.UID("other-statefulset-uid")
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-identity-"+strings.ReplaceAll(testCase.name, " ", "-"), databasev1.MysqlClusterFailoverStagePromoting)
			testCase.mutate(candidate)
			reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate})...)
			reconciler.execCommandOnPodFn = plan.execute

			stored := phase5CReconcile(t, reconciler, cluster)
			g.Expect(plan.commands).To(BeEmpty())
			g.Expect(plan.mutations).To(BeEmpty())
			g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStagePromoting))
		})
	}
}

func TestPhase5CMutationErrorNeverCrossesBarrier(t *testing.T) {
	g := NewWithT(t)
	cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-stop-error", databasev1.MysqlClusterFailoverStagePromoting)
	plan.errors[candidate.Name+"\x00"+mysqlStopSlaveCommand()] = errors.New("outcome unknown")
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate})...)
	reconciler.execCommandOnPodFn = plan.execute

	_, _, err := reconciler.reconcileMasterSlave(context.Background(), *cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(plan.mutations).To(BeEmpty())
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleSlave))
	g.Expect(phase4StoredCluster(t, reconciler, cluster).Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStagePromoting))
}

func TestPhase5CReconfiguringFenceLossRefencesBeforeQuarantine(t *testing.T) {
	g := NewWithT(t)
	cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-reconfig-refence", databasev1.MysqlClusterFailoverStageReconfiguring)
	candidate.Labels[LabelMysqlRole] = string(mysqlPublishedRoleMaster)
	candidate.Labels[LegacyLabelRole] = string(mysqlPublishedRoleMaster)
	plan.ioRunning = "No"
	plan.sqlRunning = "No"
	plan.candidateReadOnly = false
	plan.candidateSuperRO = false
	plan.primaryFenced = false
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate})...)
	reconciler.execCommandOnPodFn = plan.execute

	cluster = phase5CReconcile(t, reconciler, cluster)
	g.Expect(plan.mutations).To(Equal([]string{"SUPER READ ONLY ON"}))
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleMaster))

	cluster = phase5CReconcile(t, reconciler, cluster)
	g.Expect(plan.mutations).To(Equal([]string{"SUPER READ ONLY ON"}))
	g.Expect(phase5CStoredRole(t, reconciler, candidate)).To(Equal(mysqlPublishedRoleNone))
	g.Expect(cluster.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageReconfiguring))
}

func phase5CExpectNoForbiddenSQL(t *testing.T, commands []string) {
	t.Helper()
	g := NewWithT(t)
	for _, command := range commands {
		upper := strings.ToUpper(command)
		for _, forbidden := range []string{"RESET SLAVE", "RESET MASTER", "CHANGE MASTER", "START SLAVE"} {
			g.Expect(upper).NotTo(ContainSubstring(forbidden))
		}
		g.Expect(command).NotTo(Equal(mysqlPreparePrimaryCommand()))
		g.Expect(command).NotTo(Equal(mysqlConfigureReplicaCommand("")))
	}
}

func TestPhase5CPathContainsNoLegacyOrForbiddenSQL(t *testing.T) {
	cluster, statefulSet, primary, candidate, plan := phase5CTakeoverFixture(t, "phase5c-forbidden", databasev1.MysqlClusterFailoverStagePromoting)
	objects := phase5BObjects(cluster, statefulSet, primary, []*corev1.Pod{candidate})
	reconciler := phase1HReconciler(t, objects...)
	reconciler.execCommandOnPodFn = plan.execute

	for i := 0; i < 5; i++ {
		cluster = phase5CReconcile(t, reconciler, cluster)
	}
	phase5CExpectNoForbiddenSQL(t, plan.commands)
}

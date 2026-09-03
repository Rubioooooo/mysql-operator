package controller

import (
	"context"
	"encoding/base64"
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

const phase5BPrimaryServerUUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

func phase5BElectionReferenceOutput(serverUUID, gtidSet string) string {
	return serverUUID + "\t" + base64.StdEncoding.EncodeToString([]byte(gtidSet)) + "\n"
}

func phase5BGTIDComparisonOutput(primaryInCandidate, candidateInPrimary bool, candidateGTIDSet string) string {
	boolField := func(value bool) string {
		if value {
			return "1"
		}
		return "0"
	}
	return fmt.Sprintf(
		"%s\t%s\t%s\n",
		boolField(primaryInCandidate),
		boolField(candidateInPrimary),
		base64.StdEncoding.EncodeToString([]byte(candidateGTIDSet)),
	)
}

func phase5BSlaveStatusOutput(masterHost, autoPosition, masterUUID string) string {
	output := mysqlSlaveStatusOutputForTest(masterHost, "replica", autoPosition, "No", "No", "", "")
	return strings.Replace(output, "               Master_User: replica\n", "               Master_User: replica\n               Master_UUID: "+masterUUID+"\n", 1)
}

func phase5BFixture(
	t *testing.T,
	name string,
	replicaOrdinals ...int32,
) (*databasev1.MysqlCluster, *appsv1.StatefulSet, *corev1.Pod, []*corev1.Pod) {
	t.Helper()
	cluster := phase1HCluster(name, true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	delete(primary.Labels, LabelMysqlRole)
	delete(primary.Labels, LegacyLabelRole)
	cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStateVerified)
	replicas := make([]*corev1.Pod, 0, len(replicaOrdinals))
	for _, ordinal := range replicaOrdinals {
		replicas = append(replicas, phase1HPod(t, cluster, statefulSet, ordinal, "slave", true))
	}
	return cluster, statefulSet, primary, replicas
}

type phase5BExecPlan struct {
	t                  *testing.T
	primaryName        string
	primaryGTIDSet     string
	writeSafety        map[string]string
	sourceCapability   map[string]string
	slaveStatus        map[string]string
	gtidComparison     map[string]string
	errors             map[string]error
	commandsByPod      map[string][]string
	quarantineCalls    []string
	promotionMutations int
}

func newPhase5BExecPlan(t *testing.T, primaryName, primaryGTIDSet string) *phase5BExecPlan {
	return &phase5BExecPlan{
		t:                t,
		primaryName:      primaryName,
		primaryGTIDSet:   primaryGTIDSet,
		writeSafety:      map[string]string{primaryName: "1\t1\tON\tON\n"},
		sourceCapability: make(map[string]string),
		slaveStatus:      make(map[string]string),
		gtidComparison:   make(map[string]string),
		errors:           make(map[string]error),
		commandsByPod:    make(map[string][]string),
	}
}

func (p *phase5BExecPlan) configureSafeCandidate(pod *corev1.Pod) {
	p.writeSafety[pod.Name] = "1\t1\tON\tON\n"
	p.sourceCapability[pod.Name] = "1\t1\n"
	p.slaveStatus[pod.Name] = phase5BSlaveStatusOutput("", "1", phase5BPrimaryServerUUID)
	p.gtidComparison[pod.Name] = phase5BGTIDComparisonOutput(true, true, p.primaryGTIDSet)
}

func (p *phase5BExecPlan) execute(pod *corev1.Pod, command string) (string, error) {
	p.commandsByPod[pod.Name] = append(p.commandsByPod[pod.Name], command)
	if command == mysqlPreparePrimaryCommand() || strings.Contains(command, "STOP SLAVE") || strings.Contains(command, "CHANGE MASTER") {
		p.promotionMutations++
		return "", fmt.Errorf("forbidden promotion or replica mutation: %s", command)
	}
	if err := p.errors[pod.Name+"\x00"+command]; err != nil {
		return "", err
	}
	switch command {
	case mysqlWriteSafetyObservationCommand():
		if output, found := p.writeSafety[pod.Name]; found {
			return output, nil
		}
	case mysqlElectionReferenceCommand():
		if pod.Name == p.primaryName {
			return phase5BElectionReferenceOutput(phase5BPrimaryServerUUID, p.primaryGTIDSet), nil
		}
	case mysqlSourceCapabilityCommand():
		if output, found := p.sourceCapability[pod.Name]; found {
			return output, nil
		}
	case mysqlShowSlaveStatusCommand():
		if output, found := p.slaveStatus[pod.Name]; found {
			return output, nil
		}
	case mysqlSetSuperReadOnlyCommand():
		p.quarantineCalls = append(p.quarantineCalls, pod.Name)
		return "", nil
	}
	if strings.Contains(command, "GTID_SUBSET(") {
		if pod.Name == p.primaryName {
			return phase5BGTIDComparisonOutput(true, true, p.primaryGTIDSet), nil
		}
		if output, found := p.gtidComparison[pod.Name]; found {
			return output, nil
		}
	}
	return "", fmt.Errorf("unexpected Phase 5-B command on %s: %s", pod.Name, command)
}

func phase5BInstallMasterHost(plan *phase5BExecPlan, cluster *databasev1.MysqlCluster, pod *corev1.Pod) {
	plan.slaveStatus[pod.Name] = phase5BSlaveStatusOutput(cluster.Spec.MasterService, "1", phase5BPrimaryServerUUID)
}

func phase5BObjects(cluster *databasev1.MysqlCluster, statefulSet *appsv1.StatefulSet, primary *corev1.Pod, replicas []*corev1.Pod) []client.Object {
	objects := []client.Object{cluster, statefulSet, primary}
	for _, replica := range replicas {
		objects = append(objects, replica)
	}
	return objects
}

func TestPhase5BGTIDRelationSemantics(t *testing.T) {
	testCases := []struct {
		name               string
		primaryInCandidate bool
		candidateInPrimary bool
		expected           mysqlGTIDRelation
	}{
		{name: "equal", primaryInCandidate: true, candidateInPrimary: true, expected: mysqlGTIDRelationEqual},
		{name: "strict subset", primaryInCandidate: false, candidateInPrimary: true, expected: mysqlGTIDRelationSubset},
		{name: "strict superset", primaryInCandidate: true, candidateInPrimary: false, expected: mysqlGTIDRelationSuperset},
		{name: "divergent", expected: mysqlGTIDRelationDivergent},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			comparison, err := parseMysqlGTIDComparison(phase5BGTIDComparisonOutput(
				testCase.primaryInCandidate,
				testCase.candidateInPrimary,
				"uuid:1-7",
			))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(comparison.Relation).To(Equal(testCase.expected))
			g.Expect(comparison.CandidateGTIDSet).To(Equal("uuid:1-7"))
		})
	}

	t.Run("valid empty set", func(t *testing.T) {
		g := NewWithT(t)
		reference, err := parseMysqlElectionReference(phase5BElectionReferenceOutput(phase5BPrimaryServerUUID, ""))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(reference.GTIDSet).To(BeEmpty())
		comparison, err := parseMysqlGTIDComparison(phase5BGTIDComparisonOutput(true, true, ""))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(comparison.Relation).To(Equal(mysqlGTIDRelationEqual))
		g.Expect(comparison.CandidateGTIDSet).To(BeEmpty())
	})

	t.Run("failed primary GTID is safely base64 encoded", func(t *testing.T) {
		g := NewWithT(t)
		unsafeText := `uuid:1-2'); DROP USER root; --`
		command := mysqlGTIDComparisonCommand(unsafeText)
		g.Expect(command).NotTo(ContainSubstring(unsafeText))
		g.Expect(command).To(ContainSubstring(base64.StdEncoding.EncodeToString([]byte(unsafeText))))
	})

	for _, output := range []string{"", "not-a-row\n", "1\t1\t%%%\n", "1\t1\t\nsecond\n"} {
		t.Run("malformed output fails closed "+fmt.Sprintf("%q", output), func(t *testing.T) {
			_, err := parseMysqlGTIDComparison(output)
			NewWithT(t).Expect(err).To(HaveOccurred())
		})
	}
}

func TestPhase5BRoleNoneStartsOnlyAfterPhase5ABarrier(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase5b-stage-barrier", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
	cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStateVerified)
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary, replica)
	commands := make([]string, 0)
	reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		commands = append(commands, pod.Name+":"+command)
		if pod.Name != primary.Name || command != mysqlWriteSafetyObservationCommand() {
			return "", fmt.Errorf("Phase 5-A barrier crossed in role quarantine reconcile")
		}
		return "1\t1\tON\tON\n", nil
	}

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(commands).To(HaveLen(1))
	g.Expect(phase5StoredPod(t, reconciler, primary).Labels).NotTo(HaveKey(LabelMysqlRole))
	g.Expect(phase4StoredCluster(t, reconciler, cluster).Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageFencing))
}

func TestPhase5BCandidateInventoryRejectsSpoofBeforeSQL(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster, statefulSet, primary, replicas := phase5BFixture(t, "phase5b-spoof", 2)
	replicas[0].OwnerReferences[0].Name = "foreign-statefulset"
	replicas[0].OwnerReferences[0].UID = types.UID("foreign-statefulset-uid")
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, replicas)...)
	execCalls := 0
	reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
		execCalls++
		return "", nil
	}

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(execCalls).To(Equal(0))
}

func TestPhase5BNotReadyCandidateReceivesNoSQL(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster, statefulSet, primary, replicas := phase5BFixture(t, "phase5b-notready", 2)
	for i := range replicas[0].Status.ContainerStatuses {
		if replicas[0].Status.ContainerStatuses[i].Name == mysqlContainerName {
			replicas[0].Status.ContainerStatuses[i].Ready = false
		}
	}
	plan := newPhase5BExecPlan(t, primary.Name, "uuid:1-10")
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, replicas)...)
	reconciler.execCommandOnPodFn = plan.execute

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(plan.commandsByPod[replicas[0].Name]).To(BeEmpty())
	stored := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
	g.Expect(stored.Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStateVerified))
	g.Expect(stored.Status.HA.Failover.Candidate).To(BeEmpty())
}

func TestPhase5BWritableCandidateQuarantineIsAReconcileBarrier(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster, statefulSet, primary, replicas := phase5BFixture(t, "phase5b-quarantine", 2)
	plan := newPhase5BExecPlan(t, primary.Name, "uuid:1-10")
	plan.writeSafety[replicas[0].Name] = "0\t0\tON\tON\n"
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, replicas)...)
	reconciler.execCommandOnPodFn = plan.execute

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(plan.quarantineCalls).To(Equal([]string{replicas[0].Name}))
	stored := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageFencing))
	g.Expect(stored.Status.HA.Failover.Candidate).To(BeEmpty())
	g.Expect(plan.commandsByPod[replicas[0].Name]).NotTo(ContainElement(mysqlSourceCapabilityCommand()))
	g.Expect(plan.promotionMutations).To(Equal(0))
}

func TestPhase5BCandidateCapabilityAndChannelRequirements(t *testing.T) {
	testCases := []struct {
		name        string
		writeSafety string
		source      string
		channel     func(*databasev1.MysqlCluster) string
	}{
		{name: "GTID mode mismatch", writeSafety: "1\t1\tOFF\tON\n", source: "1\t1\n"},
		{name: "enforce GTID mismatch", writeSafety: "1\t1\tON\tWARN\n", source: "1\t1\n"},
		{name: "log bin disabled", writeSafety: "1\t1\tON\tON\n", source: "0\t1\n"},
		{name: "log slave updates disabled", writeSafety: "1\t1\tON\tON\n", source: "1\t0\n"},
		{name: "channel missing", writeSafety: "1\t1\tON\tON\n", source: "1\t1\n", channel: func(*databasev1.MysqlCluster) string { return "" }},
		{name: "master host mismatch", writeSafety: "1\t1\tON\tON\n", source: "1\t1\n", channel: func(*databasev1.MysqlCluster) string {
			return phase5BSlaveStatusOutput("wrong-primary", "1", phase5BPrimaryServerUUID)
		}},
		{name: "auto position mismatch", writeSafety: "1\t1\tON\tON\n", source: "1\t1\n", channel: func(cluster *databasev1.MysqlCluster) string {
			return phase5BSlaveStatusOutput(cluster.Spec.MasterService, "0", phase5BPrimaryServerUUID)
		}},
		{name: "master UUID mismatch", writeSafety: "1\t1\tON\tON\n", source: "1\t1\n", channel: func(cluster *databasev1.MysqlCluster) string {
			return phase5BSlaveStatusOutput(cluster.Spec.MasterService, "1", "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee")
		}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			cluster, statefulSet, primary, replicas := phase5BFixture(t, "phase5b-capability-"+strings.ReplaceAll(testCase.name, " ", "-"), 2)
			candidate := replicas[0]
			plan := newPhase5BExecPlan(t, primary.Name, "uuid:1-10")
			plan.writeSafety[candidate.Name] = testCase.writeSafety
			plan.sourceCapability[candidate.Name] = testCase.source
			if testCase.channel != nil {
				plan.slaveStatus[candidate.Name] = testCase.channel(cluster)
			} else {
				plan.slaveStatus[candidate.Name] = phase5BSlaveStatusOutput(cluster.Spec.MasterService, "1", phase5BPrimaryServerUUID)
			}
			plan.gtidComparison[candidate.Name] = phase5BGTIDComparisonOutput(true, true, "uuid:1-10")
			reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, replicas)...)
			reconciler.execCommandOnPodFn = plan.execute

			_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
			g.Expect(err).NotTo(HaveOccurred())
			stored := phase4StoredCluster(t, reconciler, cluster)
			g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageFencing))
			g.Expect(stored.Status.HA.Failover.Candidate).To(BeEmpty())
			g.Expect(stored.Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStateVerified))
			g.Expect(plan.quarantineCalls).To(BeEmpty())
			g.Expect(plan.promotionMutations).To(Equal(0))
		})
	}
}

func TestPhase5BOnlyExactGTIDEqualityElects(t *testing.T) {
	testCases := []struct {
		name               string
		primaryGTIDSet     string
		candidateGTIDSet   string
		primaryInCandidate bool
		candidateInPrimary bool
		expectSelected     bool
	}{
		{name: "equal", primaryGTIDSet: "uuid:1-10", candidateGTIDSet: "uuid:1-10", primaryInCandidate: true, candidateInPrimary: true, expectSelected: true},
		{name: "strict subset", primaryGTIDSet: "uuid:1-10", candidateGTIDSet: "uuid:1-9", candidateInPrimary: true},
		{name: "strict superset", primaryGTIDSet: "uuid:1-10", candidateGTIDSet: "uuid:1-11", primaryInCandidate: true},
		{name: "divergent", primaryGTIDSet: "uuid:1-10", candidateGTIDSet: "other:1"},
		{name: "valid empty equality", primaryGTIDSet: "", candidateGTIDSet: "", primaryInCandidate: true, candidateInPrimary: true, expectSelected: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			cluster, statefulSet, primary, replicas := phase5BFixture(t, "phase5b-gtid-"+strings.ReplaceAll(testCase.name, " ", "-"), 2)
			candidate := replicas[0]
			plan := newPhase5BExecPlan(t, primary.Name, testCase.primaryGTIDSet)
			plan.configureSafeCandidate(candidate)
			phase5BInstallMasterHost(plan, cluster, candidate)
			plan.gtidComparison[candidate.Name] = phase5BGTIDComparisonOutput(testCase.primaryInCandidate, testCase.candidateInPrimary, testCase.candidateGTIDSet)
			reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, replicas)...)
			reconciler.execCommandOnPodFn = plan.execute

			_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
			g.Expect(err).NotTo(HaveOccurred())
			stored := phase4StoredCluster(t, reconciler, cluster)
			if !testCase.expectSelected {
				g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
				g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageFencing))
				g.Expect(stored.Status.HA.Failover.Candidate).To(BeEmpty())
				return
			}
			g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverInProgress))
			g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageCandidateSelected))
			g.Expect(stored.Status.HA.Failover.Candidate).To(Equal(candidate.Name))
			g.Expect(stored.Status.HA.Failover.CandidateUID).To(Equal(string(candidate.UID)))
			g.Expect(stored.Status.HA.Failover.FailedPrimaryServerUUID).To(Equal(phase5BPrimaryServerUUID))
			g.Expect(stored.Status.HA.Failover.FailedPrimaryGTIDSet).NotTo(BeNil())
			g.Expect(*stored.Status.HA.Failover.FailedPrimaryGTIDSet).To(Equal(testCase.primaryGTIDSet))
			g.Expect(plan.quarantineCalls).To(BeEmpty(), "an already-SRO candidate must not repeat quarantine")
			g.Expect(plan.promotionMutations).To(Equal(0))
		})
	}
}

func TestPhase5BSelectsLowestEqualOrdinal(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster, statefulSet, primary, replicas := phase5BFixture(t, "phase5b-deterministic", 3, 2)
	plan := newPhase5BExecPlan(t, primary.Name, "uuid:1-10")
	for _, replica := range replicas {
		plan.configureSafeCandidate(replica)
		phase5BInstallMasterHost(plan, cluster, replica)
	}
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, replicas)...)
	reconciler.execCommandOnPodFn = plan.execute

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	stored := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(stored.Status.HA.Failover.Candidate).To(Equal(mysqlStatefulSetPodName(cluster, 2)))
	g.Expect(plan.commandsByPod[mysqlStatefulSetPodName(cluster, 3)]).To(BeEmpty())
}

func TestPhase5BSQLFailureNeverPublishesCandidateSelected(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster, statefulSet, primary, replicas := phase5BFixture(t, "phase5b-sql-failure", 2)
	candidate := replicas[0]
	plan := newPhase5BExecPlan(t, primary.Name, "uuid:1-10")
	plan.configureSafeCandidate(candidate)
	phase5BInstallMasterHost(plan, cluster, candidate)
	plan.errors[candidate.Name+"\x00"+mysqlSourceCapabilityCommand()] = errors.New("SQL unavailable")
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, replicas)...)
	reconciler.execCommandOnPodFn = plan.execute

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	stored := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageFencing))
	g.Expect(stored.Status.HA.Failover.Candidate).To(BeEmpty())
	g.Expect(plan.promotionMutations).To(Equal(0))
}

func phase5BSelectCandidate(t *testing.T, name string) (*databasev1.MysqlCluster, *appsv1.StatefulSet, *corev1.Pod, *corev1.Pod, *phase5BExecPlan) {
	t.Helper()
	cluster, statefulSet, primary, replicas := phase5BFixture(t, name, 2)
	candidate := replicas[0]
	plan := newPhase5BExecPlan(t, primary.Name, "uuid:1-10")
	plan.configureSafeCandidate(candidate)
	phase5BInstallMasterHost(plan, cluster, candidate)
	reconciler := phase1HReconciler(t, phase5BObjects(cluster, statefulSet, primary, replicas)...)
	reconciler.execCommandOnPodFn = plan.execute
	_, _, err := reconciler.reconcileMasterSlave(context.Background(), *cluster)
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	selected := phase4StoredCluster(t, reconciler, cluster)
	*cluster = *selected
	return cluster, statefulSet, primary, candidate, plan
}

func TestPhase5BCandidateSelectedCheckpointIsReadOnlyAndRestartSafe(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster, statefulSet, primary, candidate, _ := phase5BSelectCandidate(t, "phase5b-checkpoint")
	plan := newPhase5BExecPlan(t, primary.Name, "uuid:1-10")
	plan.configureSafeCandidate(candidate)
	phase5BInstallMasterHost(plan, cluster, candidate)
	restarted := phase1HReconciler(t, cluster, statefulSet, primary, candidate)
	restarted.execCommandOnPodFn = plan.execute

	_, _, err := restarted.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	stored := phase4StoredCluster(t, restarted, cluster)
	g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageCandidateSelected))
	g.Expect(stored.Status.HA.Failover.Candidate).To(Equal(candidate.Name))
	g.Expect(plan.quarantineCalls).To(BeEmpty())
	g.Expect(plan.promotionMutations).To(Equal(0))
}

func TestPhase5BCandidateSelectedInvalidationMatrix(t *testing.T) {
	testCases := []struct {
		name          string
		mutateObjects func(*databasev1.MysqlCluster, *corev1.Pod, *corev1.Pod)
		mutatePlan    func(*phase5BExecPlan, *databasev1.MysqlCluster, *corev1.Pod, *corev1.Pod)
		expectFence   databasev1.MysqlClusterFenceState
	}{
		{
			name: "candidate UID replacement",
			mutateObjects: func(_ *databasev1.MysqlCluster, _ *corev1.Pod, candidate *corev1.Pod) {
				candidate.UID = types.UID("replacement-candidate-uid")
			},
			expectFence: databasev1.MysqlClusterFenceStateVerified,
		},
		{
			name: "candidate write protection loss",
			mutatePlan: func(plan *phase5BExecPlan, _ *databasev1.MysqlCluster, _ *corev1.Pod, candidate *corev1.Pod) {
				plan.writeSafety[candidate.Name] = "1\t0\tON\tON\n"
			},
			expectFence: databasev1.MysqlClusterFenceStateVerified,
		},
		{
			name: "candidate GTID drift",
			mutatePlan: func(plan *phase5BExecPlan, _ *databasev1.MysqlCluster, _ *corev1.Pod, candidate *corev1.Pod) {
				plan.gtidComparison[candidate.Name] = phase5BGTIDComparisonOutput(true, false, "uuid:1-11")
			},
			expectFence: databasev1.MysqlClusterFenceStateVerified,
		},
		{
			name: "candidate replication identity drift",
			mutatePlan: func(plan *phase5BExecPlan, cluster *databasev1.MysqlCluster, _ *corev1.Pod, candidate *corev1.Pod) {
				plan.slaveStatus[candidate.Name] = phase5BSlaveStatusOutput(cluster.Spec.MasterService, "1", "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee")
			},
			expectFence: databasev1.MysqlClusterFenceStateVerified,
		},
		{
			name: "failed primary fence loss",
			mutatePlan: func(plan *phase5BExecPlan, _ *databasev1.MysqlCluster, primary *corev1.Pod, _ *corev1.Pod) {
				plan.writeSafety[primary.Name] = "1\t0\tON\tON\n"
			},
			expectFence: databasev1.MysqlClusterFenceStateBlocked,
		},
		{
			name: "failed primary UID replacement",
			mutateObjects: func(_ *databasev1.MysqlCluster, primary *corev1.Pod, _ *corev1.Pod) {
				primary.UID = types.UID("replacement-primary-uid")
			},
			expectFence: databasev1.MysqlClusterFenceStateBlocked,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			cluster, statefulSet, primary, candidate, _ := phase5BSelectCandidate(t, "phase5b-invalidate-"+strings.ReplaceAll(testCase.name, " ", "-"))
			if testCase.mutateObjects != nil {
				testCase.mutateObjects(cluster, primary, candidate)
			}
			plan := newPhase5BExecPlan(t, primary.Name, "uuid:1-10")
			plan.configureSafeCandidate(candidate)
			phase5BInstallMasterHost(plan, cluster, candidate)
			if testCase.mutatePlan != nil {
				testCase.mutatePlan(plan, cluster, primary, candidate)
			}
			reconciler := phase1HReconciler(t, cluster, statefulSet, primary, candidate)
			reconciler.execCommandOnPodFn = plan.execute

			_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
			g.Expect(err).NotTo(HaveOccurred())
			stored := phase4StoredCluster(t, reconciler, cluster)
			g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageFencing))
			g.Expect(stored.Status.HA.Failover.Candidate).To(BeEmpty())
			g.Expect(stored.Status.HA.Failover.CandidateUID).To(BeEmpty())
			g.Expect(stored.Status.HA.Failover.FailedPrimaryServerUUID).To(BeEmpty())
			g.Expect(stored.Status.HA.Failover.FailedPrimaryGTIDSet).To(BeNil())
			g.Expect(stored.Status.HA.Failover.FenceState).To(Equal(testCase.expectFence))
			g.Expect(plan.quarantineCalls).To(BeEmpty())
			g.Expect(plan.promotionMutations).To(Equal(0))
		})
	}
}

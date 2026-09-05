package controller

import (
	"context"
	"errors"
	"fmt"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func phase5FencingHA(
	pod *corev1.Pod,
	fenceState databasev1.MysqlClusterFenceState,
) *databasev1.MysqlClusterHAStatus {
	fencedUID := ""
	if fenceState == databasev1.MysqlClusterFenceStateVerified {
		fencedUID = string(pod.UID)
	}
	return &databasev1.MysqlClusterHAStatus{
		State:      databasev1.MysqlClusterHAStateFailoverInProgress,
		Primary:    pod.Name,
		PrimaryUID: string(pod.UID),
		Failover: &databasev1.MysqlClusterFailoverStatus{
			Stage:            databasev1.MysqlClusterFailoverStageFencing,
			FailedPrimary:    pod.Name,
			FailedPrimaryUID: string(pod.UID),
			FenceState:       fenceState,
			FenceMethod:      databasev1.MysqlClusterFenceMethodMySQLSuperReadOnly,
			FencedPrimaryUID: fencedUID,
		},
	}
}

func phase5StoredPod(t *testing.T, reconciler *MysqlClusterReconciler, pod *corev1.Pod) *corev1.Pod {
	t.Helper()
	stored := &corev1.Pod{}
	NewWithT(t).Expect(reconciler.Get(context.Background(), client.ObjectKeyFromObject(pod), stored)).To(Succeed())
	return stored
}

func TestPhase5AWriteSafetyClassification(t *testing.T) {
	g := NewWithT(t)
	g.Expect(mysqlWriteSafetyObservationCommand()).To(Equal(mysqlRootClientCommand + ` -Nse "SELECT @@GLOBAL.read_only, @@GLOBAL.super_read_only, @@GLOBAL.gtid_mode, @@GLOBAL.enforce_gtid_consistency;"`))
	g.Expect(mysqlSetSuperReadOnlyCommand()).To(Equal(mysqlRootClientCommand + ` -e "SET GLOBAL super_read_only = ON;"`))

	testCases := []struct {
		name        string
		output      string
		role        mysqlWriteRole
		gtidReady   bool
		expectError bool
	}{
		{name: "RO0 SRO0 writable", output: "0\t0\tON\tON\n", role: mysqlWriteRoleWritable, gtidReady: true},
		{name: "RO1 SRO0 writable", output: "1\t0\tON\tON\n", role: mysqlWriteRoleWritable, gtidReady: true},
		{name: "RO1 SRO1 read only", output: "1\t1\tON\tON\n", role: mysqlWriteRoleReadOnly, gtidReady: true},
		{name: "RO0 SRO1 unknown", output: "0\t1\tON\tON\n", role: mysqlWriteRoleUnknown, gtidReady: true},
		{name: "malformed field count", output: "1\t1\tON\n", expectError: true},
		{name: "malformed boolean", output: "ON\t1\tON\tON\n", expectError: true},
		{name: "multiple rows", output: "1\t1\tON\tON\n1\t1\tON\tON\n", expectError: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			observation, err := parseMysqlWriteSafetyObservation(testCase.output)
			if testCase.expectError {
				g.Expect(err).To(HaveOccurred())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(observation.WriteRole).To(Equal(testCase.role))
			g.Expect(observation.GTIDReady).To(Equal(testCase.gtidReady))
		})
	}
}

func TestPhase5AGTIDCapabilityObservation(t *testing.T) {
	testCases := []struct {
		gtidMode    string
		consistency string
		ready       bool
	}{
		{gtidMode: "ON", consistency: "ON", ready: true},
		{gtidMode: "OFF", consistency: "ON", ready: false},
		{gtidMode: "OFF_PERMISSIVE", consistency: "ON", ready: false},
		{gtidMode: "ON_PERMISSIVE", consistency: "ON", ready: false},
		{gtidMode: "ON", consistency: "WARN", ready: false},
		{gtidMode: "ON", consistency: "OFF", ready: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.gtidMode+"-"+testCase.consistency, func(t *testing.T) {
			g := NewWithT(t)
			observation, err := parseMysqlWriteSafetyObservation(
				fmt.Sprintf("1\t1\t%s\t%s\n", testCase.gtidMode, testCase.consistency),
			)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(observation.GTIDReady).To(Equal(testCase.ready))
		})
	}
}

func TestPhase5AUnsupportedSuperReadOnly(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase5-unsupported", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary)
	reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
		return "", errors.New("ERROR 1193 (HY000): Unknown system variable 'super_read_only'")
	}

	observation, err := reconciler.observeMysqlWriteSafety(ctx, primary, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(observation.WriteRole).To(Equal(mysqlWriteRoleUnsupported))

	wrapped := &mysqlPodCommandExecutionError{
		cause:  errors.New("command terminated with exit code 1"),
		stderr: "ERROR 1193 (HY000): Unknown system variable 'super_read_only'",
	}
	g.Expect(mysqlSuperReadOnlyUnsupported(wrapped)).To(BeTrue())
	g.Expect(wrapped.Error()).NotTo(ContainSubstring("super_read_only"), "captured SQL stderr must not be exposed through the generic error text")
}

func TestPhase5AFailoverEntryPersistsPendingWithoutSQL(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase5-entry", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
	cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateFailoverRequired, primary)
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary, replica, phase1HEndpoints(cluster, nil))
	execCalls := 0
	reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
		execCalls++
		return "", nil
	}

	_, converged, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(converged).To(BeFalse())
	g.Expect(execCalls).To(Equal(0))
	stored := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverInProgress))
	g.Expect(stored.Status.HA.Failover).To(Equal(phase5FencingHA(primary, databasev1.MysqlClusterFenceStatePending).Failover))
	g.Expect(phase5StoredPod(t, reconciler, primary).Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
}

func TestPhase5APendingMutationThenLaterVerificationAndQuarantine(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase5-barrier", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
	cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStatePending)
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary, replica, phase1HEndpoints(cluster, nil))
	fenced := false
	setCalls := 0
	commands := make([]string, 0, 4)
	reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		commands = append(commands, command)
		g.Expect(pod.Name).To(Equal(primary.Name), "Phase 5-A must never run SQL on an election candidate")
		switch command {
		case mysqlWriteSafetyObservationCommand():
			if fenced {
				return "1\t1\tON\tON\n", nil
			}
			return "0\t0\tOFF\tWARN\n", nil
		case mysqlSetSuperReadOnlyCommand():
			setCalls++
			fenced = true
			return "", nil
		default:
			return "", fmt.Errorf("unexpected Phase 5-A command: %s", command)
		}
	}

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(setCalls).To(Equal(1))
	afterMutation := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(afterMutation.Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStatePending))
	g.Expect(afterMutation.Status.HA.Failover.FencedPrimaryUID).To(BeEmpty())
	g.Expect(phase5StoredPod(t, reconciler, primary).Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))

	_, _, err = reconciler.reconcileMasterSlave(ctx, *afterMutation)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(setCalls).To(Equal(1), "verification must not repeat the fence mutation")
	afterVerification := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(afterVerification.Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStateVerified))
	g.Expect(afterVerification.Status.HA.Failover.FencedPrimaryUID).To(Equal(string(primary.UID)))
	g.Expect(phase5StoredPod(t, reconciler, primary).Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))

	_, _, err = reconciler.reconcileMasterSlave(ctx, *afterVerification)
	g.Expect(err).NotTo(HaveOccurred())
	quarantined := phase5StoredPod(t, reconciler, primary)
	g.Expect(quarantined.Labels).NotTo(HaveKey(LabelMysqlRole))
	g.Expect(quarantined.Labels).NotTo(HaveKey(LegacyLabelRole))
	g.Expect(quarantined.Labels).NotTo(HaveKeyWithValue(LabelMysqlRole, "slave"))
	g.Expect(setCalls).To(Equal(1))
	for _, command := range commands {
		g.Expect(command).NotTo(Equal(mysqlShowSlaveGTIDCommand()))
		g.Expect(command).NotTo(Equal(mysqlPreparePrimaryCommand()))
		g.Expect(command).NotTo(Equal(mysqlConfigureReplicaCommand(cluster.Spec.MasterService)))
	}
	final := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(final.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverInProgress))
	g.Expect(final.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageFencing))
}

func TestPhase5APendingAlreadyFencedCrashRecoveryUsesNoSet(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase5-crash-recovery", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStatePending)
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary)
	setCalls := 0
	reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		if command == mysqlSetSuperReadOnlyCommand() {
			setCalls++
		}
		return "1\t1\tON\tON\n", nil
	}

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(setCalls).To(Equal(0))
	g.Expect(phase4StoredCluster(t, reconciler, cluster).Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStateVerified))
}

func TestPhase5AFencingBlocksUnsafeObservations(t *testing.T) {
	testCases := []struct {
		name       string
		objects    func(*testing.T, *databasev1.MysqlCluster, *appsv1.StatefulSet, *corev1.Pod) []client.Object
		execResult func() (string, error)
	}{
		{
			name: "missing failed primary",
			objects: func(_ *testing.T, cluster *databasev1.MysqlCluster, statefulSet *appsv1.StatefulSet, _ *corev1.Pod) []client.Object {
				return []client.Object{cluster, statefulSet}
			},
		},
		{
			name: "UID replacement",
			objects: func(_ *testing.T, cluster *databasev1.MysqlCluster, statefulSet *appsv1.StatefulSet, primary *corev1.Pod) []client.Object {
				replacement := primary.DeepCopy()
				replacement.UID = types.UID("replacement-uid")
				return []client.Object{cluster, statefulSet, replacement}
			},
		},
		{
			name: "database unreachable",
			objects: func(_ *testing.T, cluster *databasev1.MysqlCluster, statefulSet *appsv1.StatefulSet, primary *corev1.Pod) []client.Object {
				return []client.Object{cluster, statefulSet, primary}
			},
			execResult: func() (string, error) { return "", errors.New("transport unavailable") },
		},
		{
			name: "super read only unsupported",
			objects: func(_ *testing.T, cluster *databasev1.MysqlCluster, statefulSet *appsv1.StatefulSet, primary *corev1.Pod) []client.Object {
				return []client.Object{cluster, statefulSet, primary}
			},
			execResult: func() (string, error) {
				return "", errors.New("Unknown system variable 'super_read_only'")
			},
		},
		{
			name: "unknown write role",
			objects: func(_ *testing.T, cluster *databasev1.MysqlCluster, statefulSet *appsv1.StatefulSet, primary *corev1.Pod) []client.Object {
				return []client.Object{cluster, statefulSet, primary}
			},
			execResult: func() (string, error) { return "0\t1\tON\tON\n", nil },
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			cluster := phase1HCluster("phase5-block-"+fmt.Sprint(len(testCase.name)), true)
			statefulSet := phase1HStatefulSet(t, cluster)
			primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
			cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStateVerified)
			reconciler := phase1HReconciler(t, testCase.objects(t, cluster, statefulSet, primary)...)
			sqlCalls := 0
			setCalls := 0
			reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
				sqlCalls++
				if command == mysqlSetSuperReadOnlyCommand() {
					setCalls++
				}
				if testCase.execResult == nil {
					return "", nil
				}
				return testCase.execResult()
			}

			_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
			g.Expect(err).NotTo(HaveOccurred())
			stored := phase4StoredCluster(t, reconciler, cluster)
			g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
			g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageFencing))
			g.Expect(stored.Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStateBlocked))
			g.Expect(stored.Status.HA.Failover.FencedPrimaryUID).To(BeEmpty())
			g.Expect(setCalls).To(Equal(0))
			if testCase.name == "missing failed primary" || testCase.name == "UID replacement" {
				g.Expect(sqlCalls).To(Equal(0))
			}
		})
	}
}

func TestPhase5ABlockedFenceRetriesThroughPendingBarrier(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase5-blocked-retry", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStateBlocked)
	cluster.Status.HA.State = databasev1.MysqlClusterHAStateDegraded
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary)
	setCalls := 0
	reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		if command == mysqlSetSuperReadOnlyCommand() {
			setCalls++
			return "", nil
		}
		return "0\t0\tON\tON\n", nil
	}

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(setCalls).To(Equal(0), "Blocked must first republish Pending")
	pending := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(pending.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverInProgress))
	g.Expect(pending.Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStatePending))

	_, _, err = reconciler.reconcileMasterSlave(ctx, *pending)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(setCalls).To(Equal(1))
	g.Expect(phase4StoredCluster(t, reconciler, cluster).Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStatePending))
}

func TestPhase5ARecoveryBeforeFenceAbortsToVerifying(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase5-recovered", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
	cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStatePending)
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary, phase1HEndpoints(cluster, primary))
	setCalls := 0
	reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		if command == mysqlSetSuperReadOnlyCommand() {
			setCalls++
		}
		return "0\t0\tON\tON\n", nil
	}

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(setCalls).To(Equal(0))
	stored := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateVerifying))
	g.Expect(stored.Status.HA.Primary).To(Equal(primary.Name))
	g.Expect(stored.Status.HA.PrimaryUID).To(Equal(string(primary.UID)))
	g.Expect(stored.Status.HA.Failover).To(BeNil())
}

func TestPhase5AVerifiedFenceLossInvalidatesProofBeforeMutation(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase5-fence-loss", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStateVerified)
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary)
	setCalls := 0
	reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		if command == mysqlSetSuperReadOnlyCommand() {
			setCalls++
		}
		return "1\t0\tON\tON\n", nil
	}

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(setCalls).To(Equal(0))
	stored := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(stored.Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStatePending))
	g.Expect(stored.Status.HA.Failover.FencedPrimaryUID).To(BeEmpty())
	g.Expect(phase5StoredPod(t, reconciler, primary).Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
}

func TestPhase5AActiveRoleNoneTopologyBypassesOrdinaryZeroPrimaryGuard(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase5-role-none", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	delete(primary.Labels, LabelMysqlRole)
	delete(primary.Labels, LegacyLabelRole)
	cluster.Status.GTIDBootstrap = []databasev1.MysqlClusterGTIDBootstrapStatus{{
		Ordinal: 1, PVCUID: mysqlTestPVCUID(primary), ServerUUID: phase5BPrimaryServerUUID, BootstrapGTIDSet: "",
	}}
	cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStateVerified)
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary)
	commands := make([]string, 0, 3)
	reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		commands = append(commands, command)
		switch command {
		case mysqlWriteSafetyObservationCommand():
			return "1\t1\tON\tON\n", nil
		case mysqlElectionReferenceCommand():
			return phase5BElectionReferenceOutput(phase5BPrimaryServerUUID, "uuid:1-10"), nil
		default:
			return "", fmt.Errorf("unexpected command after the role-none Phase 5-A endpoint: %s", command)
		}
	}

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	stored := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(commands).To(Equal([]string{
		mysqlWriteSafetyObservationCommand(),
		mysqlWriteSafetyObservationCommand(),
		mysqlElectionReferenceCommand(),
	}))
	g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateDegraded))
	g.Expect(stored.Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStateVerified))
	g.Expect(stored.Status.HA.Failover.Candidate).To(BeEmpty())
}

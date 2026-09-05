package controller

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	mysqlGTIDTestUUID1 = "11111111-1111-1111-1111-111111111111"
	mysqlGTIDTestUUID2 = "22222222-2222-2222-2222-222222222222"
	mysqlGTIDTestUUID3 = "33333333-3333-3333-3333-333333333333"
)

func mysqlGTIDTestPVC(pod *corev1.Pod, uid string) *corev1.PersistentVolumeClaim {
	name := mysqlDataVolume + "-" + pod.Name
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: mysqlDataVolume,
		VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
			ClaimName: name,
		}},
	})
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: pod.Namespace, UID: types.UID(uid),
	}}
}

func mysqlGTIDBootstrapOutput(serverUUID, purged, executed string, ownOnly bool) string {
	boolean := "0"
	if ownOnly {
		boolean = "1"
	}
	return fmt.Sprintf("%s\t%s\t%s\t%s\n",
		serverUUID,
		base64.StdEncoding.EncodeToString([]byte(purged)),
		base64.StdEncoding.EncodeToString([]byte(executed)),
		boolean,
	)
}

func TestMysqlGTIDBootstrapObservationUsesMaximumValidGNO(t *testing.T) {
	g := NewWithT(t)
	command := mysqlGTIDBootstrapObservationCommand()
	g.Expect(mysqlGTIDMaxGNO).To(Equal(int64(9223372036854775806)))
	g.Expect(command).To(ContainSubstring("CONCAT(@@GLOBAL.server_uuid, ':1-9223372036854775806')"))
	g.Expect(command).NotTo(ContainSubstring("9223372036854775807"))

	owned, err := parseMysqlGTIDBootstrapObservation(mysqlGTIDBootstrapOutput(
		mysqlGTIDTestUUID1,
		"",
		mysqlGTIDTestUUID1+":1-5",
		true,
	))
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(owned.ExecutedOwnOnly).To(BeTrue())

	foreign, err := parseMysqlGTIDBootstrapObservation(mysqlGTIDBootstrapOutput(
		mysqlGTIDTestUUID1,
		"",
		mysqlGTIDTestUUID2+":1",
		false,
	))
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(foreign.ExecutedOwnOnly).To(BeFalse())
}

func TestMysqlGTIDBootstrapStatusAndNormalization(t *testing.T) {
	g := NewWithT(t)
	cluster := phase1HCluster("gtid-normalization", true)
	cluster.Status.GTIDBootstrap = []databasev1.MysqlClusterGTIDBootstrapStatus{
		{Ordinal: 2, PVCUID: "pvc-2", ServerUUID: mysqlGTIDTestUUID2, BootstrapGTIDSet: mysqlGTIDTestUUID2 + ":1"},
		{Ordinal: 1, PVCUID: "pvc-1", ServerUUID: mysqlGTIDTestUUID1, BootstrapGTIDSet: ""},
	}

	trusted, err := mysqlTrustedBootstrapGTIDSet(cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(trusted).To(Equal(mysqlGTIDTestUUID2 + ":1"))
	command := mysqlGTIDComparisonCommand(mysqlGTIDTestUUID1+":1-10", trusted)
	g.Expect(command).To(ContainSubstring("GTID_SUBTRACT"))
	g.Expect(command).To(ContainSubstring(base64.StdEncoding.EncodeToString([]byte(trusted))))
	g.Expect(command).To(ContainSubstring("@@GLOBAL.gtid_executed"), "normalization must subtract exact GTIDs, not ignore a server UUID")

	duplicateOrdinal := append([]databasev1.MysqlClusterGTIDBootstrapStatus(nil), cluster.Status.GTIDBootstrap...)
	duplicateOrdinal[1].Ordinal = 2
	g.Expect(validateMysqlGTIDBootstrapStatus(duplicateOrdinal)).To(MatchError(ContainSubstring("duplicate GTID bootstrap ordinal")))
	duplicatePVC := append([]databasev1.MysqlClusterGTIDBootstrapStatus(nil), cluster.Status.GTIDBootstrap...)
	duplicatePVC[1].PVCUID = duplicatePVC[0].PVCUID
	g.Expect(validateMysqlGTIDBootstrapStatus(duplicatePVC)).To(MatchError(ContainSubstring("duplicate GTID bootstrap PVC UID")))
	duplicateUUID := append([]databasev1.MysqlClusterGTIDBootstrapStatus(nil), cluster.Status.GTIDBootstrap...)
	duplicateUUID[1].ServerUUID = duplicateUUID[0].ServerUUID
	g.Expect(validateMysqlGTIDBootstrapStatus(duplicateUUID)).To(MatchError(ContainSubstring("duplicate GTID bootstrap server UUID")))

	cluster.Status.GTIDBootstrap = nil
	_, err = mysqlTrustedBootstrapGTIDSet(cluster)
	g.Expect(err).To(MatchError(ContainSubstring("no durable GTID bootstrap provenance")))

	cluster.Status.GTIDBootstrap = []databasev1.MysqlClusterGTIDBootstrapStatus{{
		Ordinal: 1, PVCUID: "deepcopy-pvc", ServerUUID: mysqlGTIDTestUUID1, BootstrapGTIDSet: "",
	}}
	copy := cluster.DeepCopy()
	copy.Status.GTIDBootstrap[0].PVCUID = "changed"
	g.Expect(cluster.Status.GTIDBootstrap[0].PVCUID).To(Equal("deepcopy-pvc"))
}

func TestMysqlInitialGTIDBootstrapCaptureBarriers(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("initial-gtid-bootstrap", false)
	cluster.ResourceVersion = "1"
	cluster.Spec.Replicas = replicaCountCopy(2)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
	primary.UID = "primary-pod"
	replica := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 2)
	replica.UID = "replica-pod"
	primaryPVC := mysqlGTIDTestPVC(primary, "primary-pvc")
	replicaPVC := mysqlGTIDTestPVC(replica, "replica-pvc")
	memoryClient := newStatefulSetReconcileMemoryClient(cluster, statefulSet, primary, replica, primaryPVC, replicaPVC)
	reconciler := &MysqlClusterReconciler{Client: memoryClient}
	replicaFenced := false
	commands := make([]string, 0, 32)
	reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		commands = append(commands, pod.Name+":"+command)
		switch command {
		case mysqlWriteSafetyObservationCommand():
			if pod.Name == replica.Name && !replicaFenced {
				return "0\t0\tON\tON\n", nil
			}
			if pod.Name == primary.Name {
				return "0\t0\tON\tON\n", nil
			}
			return "1\t1\tON\tON\n", nil
		case mysqlSetSuperReadOnlyCommand():
			replicaFenced = true
			return "", nil
		case mysqlSourceCapabilityCommand():
			return "1\t1\n", nil
		case mysqlShowSlaveStatusCommand():
			return "", nil
		case mysqlGTIDBootstrapObservationCommand():
			if pod.Name == primary.Name {
				return mysqlGTIDBootstrapOutput(mysqlGTIDTestUUID1, "", mysqlGTIDTestUUID1+":1", true), nil
			}
			return mysqlGTIDBootstrapOutput(mysqlGTIDTestUUID2, "", mysqlGTIDTestUUID2+":1", true), nil
		default:
			return "", fmt.Errorf("unexpected command %s", command)
		}
	}

	ready, barrier, err := reconciler.reconcileMysqlInitialGTIDBootstrap(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(ready).To(BeFalse())
	g.Expect(barrier).To(BeTrue())
	g.Expect(cluster.Status.GTIDBootstrap).To(BeEmpty())
	g.Expect(commands).To(ContainElement(replica.Name + ":" + mysqlSetSuperReadOnlyCommand()))
	g.Expect(strings.Join(commands, "\n")).NotTo(ContainSubstring(mysqlPreparePrimaryCommand()))
	g.Expect(strings.Join(commands, "\n")).NotTo(ContainSubstring("CHANGE MASTER"))

	ready, barrier, err = reconciler.reconcileMysqlInitialGTIDBootstrap(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(ready).To(BeFalse())
	g.Expect(barrier).To(BeTrue(), "atomic provenance persistence is its own reconcile barrier")
	g.Expect(cluster.Status.GTIDBootstrap).To(HaveLen(2))
	g.Expect(cluster.Status.GTIDBootstrap[0]).To(Equal(databasev1.MysqlClusterGTIDBootstrapStatus{
		Ordinal: 1, PVCUID: "primary-pvc", ServerUUID: mysqlGTIDTestUUID1, BootstrapGTIDSet: mysqlGTIDTestUUID1 + ":1",
	}))

	ready, barrier, err = reconciler.reconcileMysqlInitialGTIDBootstrap(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(ready).To(BeTrue())
	g.Expect(barrier).To(BeFalse())
}

func TestMysqlScaleUpGTIDBootstrapBeforeConfigure(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("scaleup-gtid-bootstrap", true)
	cluster.ResourceVersion = "1"
	cluster.Spec.Replicas = replicaCountCopy(3)
	cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3}
	cluster.Status.GTIDBootstrap = []databasev1.MysqlClusterGTIDBootstrapStatus{
		{Ordinal: 1, PVCUID: "pvc-1", ServerUUID: mysqlGTIDTestUUID1, BootstrapGTIDSet: ""},
		{Ordinal: 2, PVCUID: "pvc-2", ServerUUID: mysqlGTIDTestUUID2, BootstrapGTIDSet: ""},
	}
	statefulSet := phase1HStatefulSet(t, cluster)
	replica := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 3)
	replica.UID = "scaleup-pod"
	pvc := mysqlGTIDTestPVC(replica, "scaleup-pvc")
	memoryClient := newStatefulSetReconcileMemoryClient(cluster, statefulSet, replica, pvc)
	commands := make([]string, 0, 24)
	reconciler := &MysqlClusterReconciler{Client: memoryClient}
	reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		commands = append(commands, command)
		switch command {
		case mysqlWriteSafetyObservationCommand():
			return "1\t1\tON\tON\n", nil
		case mysqlShowSlaveStatusCommand():
			return "", nil
		case mysqlSourceCapabilityCommand():
			return "1\t1\n", nil
		case mysqlGTIDBootstrapObservationCommand():
			return mysqlGTIDBootstrapOutput(mysqlGTIDTestUUID3, "", mysqlGTIDTestUUID3+":1", true), nil
		case mysqlElectionReferenceCommand():
			return phase5BElectionReferenceOutput(mysqlGTIDTestUUID3, mysqlGTIDTestUUID3+":1"), nil
		case mysqlInitializeReplicaCommand(cluster.Spec.MasterService):
			return "", nil
		default:
			return "", fmt.Errorf("unexpected command %s", command)
		}
	}

	result, err := reconciler.reconcileMysqlReplicaChannel(ctx, replica, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result).To(Equal(mysqlReplicaConvergenceResult{Action: mysqlReplicaConvergenceProvenance, Mutated: true}))
	g.Expect(commands).NotTo(ContainElement(mysqlInitializeReplicaCommand(cluster.Spec.MasterService)))
	g.Expect(cluster.Status.GTIDBootstrap).To(HaveLen(3))

	commands = nil
	result, err = reconciler.reconcileMysqlReplicaChannel(ctx, replica, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result).To(Equal(mysqlReplicaConvergenceResult{Action: mysqlReplicaConvergenceConfigure, Mutated: true}))
	g.Expect(commands[len(commands)-1]).To(Equal(mysqlInitializeReplicaCommand(cluster.Spec.MasterService)))
}

func TestMysqlScaleUpPersistedBootstrapRequiresExactRawGTID(t *testing.T) {
	const (
		serverUUID = "44444444-4444-4444-4444-444444444444"
		persisted  = "44444444-4444-4444-4444-444444444444:1-5"
	)
	for _, testCase := range []struct {
		name        string
		current     string
		expectReady bool
		expectError string
	}{
		{name: "exact persisted bootstrap is ready", current: persisted, expectReady: true},
		{name: "advanced bootstrap fails closed", current: serverUUID + ":1-6", expectError: "raw GTID changed"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("scaleup-persisted-bootstrap", true)
			cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 4}
			cluster.Status.GTIDBootstrap = []databasev1.MysqlClusterGTIDBootstrapStatus{{
				Ordinal: 4, PVCUID: "scaleup-r4-pvc", ServerUUID: serverUUID, BootstrapGTIDSet: persisted,
			}}
			statefulSet := phase1HStatefulSet(t, cluster)
			replica := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 4)
			replica.UID = "scaleup-r4-pod"
			pvc := mysqlGTIDTestPVC(replica, "scaleup-r4-pvc")
			commands := make([]string, 0, 4)
			reconciler := &MysqlClusterReconciler{
				Client: newStatefulSetReconcileMemoryClient(statefulSet, replica, pvc),
				execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
					commands = append(commands, command)
					switch command {
					case mysqlWriteSafetyObservationCommand():
						return "1\t1\tON\tON\n", nil
					case mysqlSourceCapabilityCommand():
						return "1\t1\n", nil
					case mysqlShowSlaveStatusCommand():
						return "", nil
					case mysqlGTIDBootstrapObservationCommand():
						return mysqlGTIDBootstrapOutput(serverUUID, "", testCase.current, true), nil
					default:
						return "", fmt.Errorf("unexpected command %s", command)
					}
				},
			}

			ready, barrier, err := reconciler.reconcileMysqlScaleUpGTIDBootstrap(
				context.Background(), cluster, mysqlStatefulSetMember{Ordinal: 4, Pod: replica},
			)
			g.Expect(ready).To(Equal(testCase.expectReady))
			g.Expect(barrier).To(BeFalse())
			if testCase.expectError == "" {
				g.Expect(err).NotTo(HaveOccurred())
			} else {
				g.Expect(err).To(MatchError(ContainSubstring(testCase.expectError)))
			}
			g.Expect(cluster.Status.GTIDBootstrap).To(Equal([]databasev1.MysqlClusterGTIDBootstrapStatus{{
				Ordinal: 4, PVCUID: "scaleup-r4-pvc", ServerUUID: serverUUID, BootstrapGTIDSet: persisted,
			}}))
			g.Expect(commands).NotTo(ContainElements(
				mysqlInitializeReplicaCommand(cluster.Spec.MasterService),
				mysqlConfigureReplicaCommand(cluster.Spec.MasterService),
			))
		})
	}
}

func TestMysqlGTIDBootstrapPersistentIdentity(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("gtid-persistent-identity", true)
	cluster.Status.GTIDBootstrap = []databasev1.MysqlClusterGTIDBootstrapStatus{{
		Ordinal: 1, PVCUID: "stable-pvc", ServerUUID: mysqlGTIDTestUUID1, BootstrapGTIDSet: "",
	}}
	statefulSet := phase1HStatefulSet(t, cluster)
	pod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
	pod.UID = "replacement-pod-uid"
	pvc := mysqlGTIDTestPVC(pod, "stable-pvc")
	reconciler := &MysqlClusterReconciler{Client: newStatefulSetReconcileMemoryClient(statefulSet, pod, pvc)}

	g.Expect(reconciler.validateMysqlGTIDBootstrapIdentity(ctx, cluster, pod, mysqlGTIDTestUUID1)).To(Succeed(), "Pod UID replacement is allowed when persistent identity is unchanged")
	g.Expect(reconciler.validateMysqlGTIDBootstrapIdentity(ctx, cluster, pod, mysqlGTIDTestUUID2)).To(MatchError(ContainSubstring("MySQL server identity changed")))
	pvc.UID = "replacement-pvc"
	reconciler = &MysqlClusterReconciler{Client: newStatefulSetReconcileMemoryClient(statefulSet, pod, pvc)}
	g.Expect(reconciler.validateMysqlGTIDBootstrapIdentity(ctx, cluster, pod, mysqlGTIDTestUUID1)).To(MatchError(ContainSubstring("PVC identity changed")))
}

func TestMysqlGTIDBootstrapCapturePreconditionsFailClosed(t *testing.T) {
	testCases := []struct {
		name       string
		write      string
		source     string
		channel    string
		bootstrap  string
		expectText string
	}{
		{name: "GTID not ready", write: "0\t0\tOFF\tON\n", source: "1\t1\n", bootstrap: mysqlGTIDBootstrapOutput(mysqlGTIDTestUUID1, "", "", true), expectText: "not GTID ready"},
		{name: "source capability missing", write: "0\t0\tON\tON\n", source: "1\t0\n", bootstrap: mysqlGTIDBootstrapOutput(mysqlGTIDTestUUID1, "", "", true), expectText: "not source capable"},
		{name: "channel already configured", write: "0\t0\tON\tON\n", source: "1\t1\n", channel: mysqlSlaveStatusOutputForTest("primary", "replica", "1", "Yes", "Yes", "", ""), bootstrap: mysqlGTIDBootstrapOutput(mysqlGTIDTestUUID1, "", "", true), expectText: "already has a replication channel"},
		{name: "purged history", write: "0\t0\tON\tON\n", source: "1\t1\n", bootstrap: mysqlGTIDBootstrapOutput(mysqlGTIDTestUUID1, mysqlGTIDTestUUID1+":1", "", true), expectText: "non-empty gtid_purged"},
		{name: "foreign executed history", write: "0\t0\tON\tON\n", source: "1\t1\n", bootstrap: mysqlGTIDBootstrapOutput(mysqlGTIDTestUUID1, "", mysqlGTIDTestUUID2+":1", false), expectText: "non-local gtid_executed"},
		{name: "malformed observation", write: "0\t0\tON\tON\n", source: "1\t1\n", bootstrap: "malformed", expectText: "malformed MySQL GTID bootstrap provenance"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("bootstrap-precondition-"+strings.ReplaceAll(testCase.name, " ", "-"), false)
			cluster.Spec.Replicas = replicaCountCopy(1)
			statefulSet := phase1HStatefulSet(t, cluster)
			pod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
			pod.UID = "bootstrap-precondition-pod"
			pvc := mysqlGTIDTestPVC(pod, "bootstrap-precondition-pvc")
			commands := make([]string, 0, 5)
			reconciler := &MysqlClusterReconciler{
				Client: newStatefulSetReconcileMemoryClient(statefulSet, pod, pvc),
				execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
					commands = append(commands, command)
					switch command {
					case mysqlWriteSafetyObservationCommand():
						return testCase.write, nil
					case mysqlSourceCapabilityCommand():
						return testCase.source, nil
					case mysqlShowSlaveStatusCommand():
						return testCase.channel, nil
					case mysqlGTIDBootstrapObservationCommand():
						return testCase.bootstrap, nil
					default:
						return "", fmt.Errorf("unexpected command %s", command)
					}
				},
			}
			_, _, err := reconciler.observeMysqlGTIDBootstrapProof(
				context.Background(), cluster, mysqlStatefulSetMember{Ordinal: 1, Pod: pod}, false, true,
			)
			g.Expect(err).To(MatchError(ContainSubstring(testCase.expectText)))
			g.Expect(commands).NotTo(ContainElements(mysqlPreparePrimaryCommand(), mysqlInitializeReplicaCommand(cluster.Spec.MasterService), mysqlConfigureReplicaCommand(cluster.Spec.MasterService)))
		})
	}
}

func TestMysqlGTIDBootstrapLegacyBehavior(t *testing.T) {
	g := NewWithT(t)
	cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
	commands := make([]string, 0, 2)
	reconciler := &MysqlClusterReconciler{
		Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
		execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
			commands = append(commands, command)
			if command == mysqlWriteSafetyObservationCommand() {
				return "1\t1\tON\tON\n", nil
			}
			return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
		},
	}
	result, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.Converged).To(BeTrue(), "configured legacy steady replication remains compatible")

	commands = nil
	_, err = reconciler.compareMysqlCandidateGTID(context.Background(), replica, cluster, "")
	g.Expect(err).To(MatchError(ContainSubstring("no durable GTID bootstrap provenance")))
	g.Expect(commands).To(BeEmpty(), "legacy election/handoff/rejoin comparison must fail before SQL")
}

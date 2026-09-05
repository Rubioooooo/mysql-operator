package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func healthyMysqlChannelObservation(masterHost string) mysqlReplicationChannelObservation {
	return mysqlReplicationChannelObservation{
		Configured:   true,
		MasterHost:   masterHost,
		MasterUser:   "replica",
		AutoPosition: "1",
		IORunning:    "Yes",
		SQLRunning:   "Yes",
	}
}

func newMysqlReplicaConvergenceFixture(
	t *testing.T,
) (*databasev1.MysqlCluster, *appsv1.StatefulSet, *corev1.Pod) {
	t.Helper()
	cluster := statefulSetResourceTestCluster("replica-convergence", types.UID("replica-convergence-cluster-uid"))
	cluster.Spec.MasterService = "replica-convergence-primary"
	statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("replica-convergence-statefulset-uid"))
	replica := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 2)
	replica.UID = types.UID("replica-convergence-pod-a")
	return cluster, statefulSet, replica
}

type mysqlReplicaConvergencePodReplacementClient struct {
	*statefulSetReconcileMemoryClient
	replacement *corev1.Pod
	podGets     int
}

func (c *mysqlReplicaConvergencePodReplacementClient) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if _, isPod := object.(*corev1.Pod); isPod && key == client.ObjectKeyFromObject(c.replacement) {
		c.podGets++
		if c.podGets == 2 {
			copyStatefulSetReconcileObject(object, c.replacement)
			return nil
		}
	}
	return c.statefulSetReconcileMemoryClient.Get(ctx, key, object, options...)
}

func TestMysqlReplicationConvergenceClassifier(t *testing.T) {
	expectedMasterHost := "mysql-primary"
	testCases := []struct {
		name     string
		channel  mysqlReplicationChannelObservation
		expected mysqlReplicaConvergenceAction
	}{
		{name: "absent channel configures", expected: mysqlReplicaConvergenceConfigure},
		{name: "wrong MasterHost reconfigures", channel: healthyMysqlChannelObservation("other-primary"), expected: mysqlReplicaConvergenceReconfigure},
		{name: "wrong MasterUser reconfigures", channel: func() mysqlReplicationChannelObservation {
			channel := healthyMysqlChannelObservation(expectedMasterHost)
			channel.MasterUser = "other-user"
			return channel
		}(), expected: mysqlReplicaConvergenceReconfigure},
		{name: "AutoPosition not one reconfigures", channel: func() mysqlReplicationChannelObservation {
			channel := healthyMysqlChannelObservation(expectedMasterHost)
			channel.AutoPosition = "0"
			return channel
		}(), expected: mysqlReplicaConvergenceReconfigure},
		{name: "healthy channel is a no-op", channel: healthyMysqlChannelObservation(expectedMasterHost), expected: mysqlReplicaConvergenceNoop},
		{name: "IO Connecting waits", channel: func() mysqlReplicationChannelObservation {
			channel := healthyMysqlChannelObservation(expectedMasterHost)
			channel.IORunning = "Connecting"
			return channel
		}(), expected: mysqlReplicaConvergenceWait},
		{name: "SQL No waits", channel: func() mysqlReplicationChannelObservation {
			channel := healthyMysqlChannelObservation(expectedMasterHost)
			channel.SQLRunning = "No"
			return channel
		}(), expected: mysqlReplicaConvergenceWait},
		{name: "LastIOError waits", channel: func() mysqlReplicationChannelObservation {
			channel := healthyMysqlChannelObservation(expectedMasterHost)
			channel.LastIOError = "connection failed"
			return channel
		}(), expected: mysqlReplicaConvergenceWait},
		{name: "LastSQLError waits", channel: func() mysqlReplicationChannelObservation {
			channel := healthyMysqlChannelObservation(expectedMasterHost)
			channel.LastSQLError = "apply failed"
			return channel
		}(), expected: mysqlReplicaConvergenceWait},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(classifyMysqlReplicaConvergence(testCase.channel, expectedMasterHost)).To(Equal(testCase.expected))
		})
	}
}

func TestMysqlReplicationConvergenceEngine(t *testing.T) {
	testCases := []struct {
		name           string
		statusOutput   func(masterHost string) string
		expectedResult mysqlReplicaConvergenceResult
		expectedAction func(masterHost string) string
	}{
		{
			name:         "CONFIGURE executes only the minimal initialize command",
			statusOutput: func(string) string { return "" },
			expectedResult: mysqlReplicaConvergenceResult{
				Action:  mysqlReplicaConvergenceConfigure,
				Mutated: true,
			},
			expectedAction: mysqlInitializeReplicaCommand,
		},
		{
			name: "RECONFIGURE executes only the stop change start command",
			statusOutput: func(string) string {
				return mysqlSlaveStatusOutputForTest("other-primary", "replica", "1", "Yes", "Yes", "", "")
			},
			expectedResult: mysqlReplicaConvergenceResult{
				Action:  mysqlReplicaConvergenceReconfigure,
				Mutated: true,
			},
			expectedAction: mysqlConfigureReplicaCommand,
		},
		{
			name: "NOOP executes SHOW only",
			statusOutput: func(masterHost string) string {
				return mysqlSlaveStatusOutputForTest(masterHost, "replica", "1", "Yes", "Yes", "", "")
			},
			expectedResult: mysqlReplicaConvergenceResult{
				Action:    mysqlReplicaConvergenceNoop,
				Converged: true,
			},
		},
		{
			name: "WAIT executes SHOW only",
			statusOutput: func(masterHost string) string {
				return mysqlSlaveStatusOutputForTest(masterHost, "replica", "1", "Connecting", "Yes", "", "")
			},
			expectedResult: mysqlReplicaConvergenceResult{Action: mysqlReplicaConvergenceWait},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
			memoryClient := newStatefulSetReconcileMemoryClient(statefulSet, replica)
			commands := make([]string, 0, 3)
			reconciler := &MysqlClusterReconciler{
				Client: memoryClient,
				execCommandOnPodFn: func(commandPod *corev1.Pod, command string) (string, error) {
					commands = append(commands, command)
					g.Expect(commandPod.UID).To(Equal(replica.UID))
					if command == mysqlWriteSafetyObservationCommand() {
						return "1\t1\tON\tON\n", nil
					}
					if command == mysqlShowSlaveStatusCommand() {
						return testCase.statusOutput(cluster.Spec.MasterService), nil
					}
					return "", nil
				},
			}

			result, err := reconciler.reconcileMysqlReplicaChannel(ctx, replica, cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result).To(Equal(testCase.expectedResult))
			expectedCommands := []string{mysqlWriteSafetyObservationCommand(), mysqlShowSlaveStatusCommand()}
			if testCase.expectedAction != nil {
				expectedCommands = append(expectedCommands, testCase.expectedAction(cluster.Spec.MasterService))
			}
			g.Expect(commands).To(Equal(expectedCommands))
			g.Expect(commands).NotTo(ContainElement(mysqlPreparePrimaryCommand()))
			g.Expect(memoryClient.updateCount).To(Equal(0))

			storedReplica := &corev1.Pod{}
			g.Expect(memoryClient.Get(ctx, client.ObjectKeyFromObject(replica), storedReplica)).To(Succeed())
			g.Expect(storedReplica.Labels).To(Equal(replica.Labels))
		})
	}
}

func TestMysqlReplicationConvergenceWriteSafetyBarrier(t *testing.T) {
	t.Run("healthy writable replica is fenced without replication mutation or convergence", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		commands := make([]string, 0, 2)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				switch command {
				case mysqlWriteSafetyObservationCommand():
					return "0\t0\tON\tON\n", nil
				case mysqlShowSlaveStatusCommand():
					return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
				default:
					return "", nil
				}
			},
		}

		result, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(mysqlReplicaConvergenceResult{
			Action:  mysqlReplicaConvergenceWriteSafety,
			Mutated: true,
		}))
		g.Expect(result.Converged).To(BeFalse())
		g.Expect(commands).To(Equal([]string{
			mysqlWriteSafetyObservationCommand(),
			mysqlSetSuperReadOnlyCommand(),
		}))
		g.Expect(commands).NotTo(ContainElements(
			mysqlInitializeReplicaCommand(cluster.Spec.MasterService),
			mysqlConfigureReplicaCommand(cluster.Spec.MasterService),
			mysqlShowSlaveStatusCommand(),
		))
	})

	t.Run("later reconcile verifies the fence before healthy channel convergence", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		commands := make([]string, 0, 4)
		writeObservations := 0
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				switch command {
				case mysqlWriteSafetyObservationCommand():
					writeObservations++
					if writeObservations == 1 {
						return "0\t0\tON\tON\n", nil
					}
					return "1\t1\tON\tON\n", nil
				case mysqlShowSlaveStatusCommand():
					return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
				default:
					return "", nil
				}
			},
		}

		first, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(first).To(Equal(mysqlReplicaConvergenceResult{Action: mysqlReplicaConvergenceWriteSafety, Mutated: true}))
		g.Expect(commands).To(Equal([]string{mysqlWriteSafetyObservationCommand(), mysqlSetSuperReadOnlyCommand()}))

		second, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(second).To(Equal(mysqlReplicaConvergenceResult{Action: mysqlReplicaConvergenceNoop, Converged: true}))
		g.Expect(commands).To(Equal([]string{
			mysqlWriteSafetyObservationCommand(),
			mysqlSetSuperReadOnlyCommand(),
			mysqlWriteSafetyObservationCommand(),
			mysqlShowSlaveStatusCommand(),
		}))
	})

	t.Run("writable replica with absent channel stops at one mutation barrier", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		commands := make([]string, 0, 2)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				if command == mysqlWriteSafetyObservationCommand() {
					return "0\t0\tON\tON\n", nil
				}
				return "", nil
			},
		}

		result, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Mutated).To(BeTrue())
		g.Expect(result.Converged).To(BeFalse())
		g.Expect(commands).To(Equal([]string{mysqlWriteSafetyObservationCommand(), mysqlSetSuperReadOnlyCommand()}))
		g.Expect(commands).NotTo(ContainElement(mysqlInitializeReplicaCommand(cluster.Spec.MasterService)))
	})

	t.Run("malformed write-safety observation fails closed", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		commands := make([]string, 0, 1)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				return "malformed", nil
			},
		}

		result, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("malformed MySQL write-safety observation")))
		g.Expect(result).To(Equal(mysqlReplicaConvergenceResult{}))
		g.Expect(commands).To(Equal([]string{mysqlWriteSafetyObservationCommand()}))
	})

	t.Run("failed write-safety observation fails closed", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		observationErr := errors.New("write-safety observation failed")
		commands := make([]string, 0, 1)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				return "", observationErr
			},
		}

		result, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(errors.Is(err, observationErr)).To(BeTrue())
		g.Expect(result).To(Equal(mysqlReplicaConvergenceResult{}))
		g.Expect(commands).To(Equal([]string{mysqlWriteSafetyObservationCommand()}))
	})

	t.Run("GTID capability not ready fails closed before replication observation", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		commands := make([]string, 0, 1)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				return "1\t1\tOFF\tON\n", nil
			},
		}

		result, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("not GTID-ready")))
		g.Expect(result).To(Equal(mysqlReplicaConvergenceResult{}))
		g.Expect(commands).To(Equal([]string{mysqlWriteSafetyObservationCommand()}))
	})

	t.Run("exactly read-only healthy replica avoids redundant fence mutation", func(t *testing.T) {
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
		g.Expect(result).To(Equal(mysqlReplicaConvergenceResult{Action: mysqlReplicaConvergenceNoop, Converged: true}))
		g.Expect(commands).To(Equal([]string{mysqlWriteSafetyObservationCommand(), mysqlShowSlaveStatusCommand()}))
		g.Expect(commands).NotTo(ContainElement(mysqlSetSuperReadOnlyCommand()))
	})
}

func TestMysqlReplicationConvergenceSafety(t *testing.T) {
	t.Run("malformed observation returns error without corrective mutation", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		commands := make([]string, 0, 1)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				if command == mysqlWriteSafetyObservationCommand() {
					return "1\t1\tON\tON\n", nil
				}
				return "*************************** 1. row ***************************", nil
			},
		}

		_, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("missing required fields")))
		g.Expect(commands).To(Equal([]string{mysqlWriteSafetyObservationCommand(), mysqlShowSlaveStatusCommand()}))
	})

	t.Run("spoofed Pod is rejected before SQL", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		spoofed := replica.DeepCopy()
		spoofed.Name = "spoofed-replica"
		execCalls := 0
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet),
			execCommandOnPodFn: func(*corev1.Pod, string) (string, error) {
				execCalls++
				return "", nil
			},
		}

		_, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), spoofed, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("ordinal identity does not match")))
		g.Expect(execCalls).To(Equal(0))
	})

	t.Run("observation SQL error is propagated", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		observationErr := errors.New("observation failed")
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(*corev1.Pod, string) (string, error) {
				return "", observationErr
			},
		}

		_, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(errors.Is(err, observationErr)).To(BeTrue())
	})

	t.Run("mutation SQL error is propagated", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		mutationErr := errors.New("mutation failed")
		commands := make([]string, 0, 3)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				if command == mysqlWriteSafetyObservationCommand() {
					return "1\t1\tON\tON\n", nil
				}
				if command == mysqlShowSlaveStatusCommand() {
					return "", nil
				}
				return "", mutationErr
			},
		}

		_, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(errors.Is(err, mutationErr)).To(BeTrue())
		g.Expect(commands).To(Equal([]string{
			mysqlWriteSafetyObservationCommand(),
			mysqlShowSlaveStatusCommand(),
			mysqlInitializeReplicaCommand(cluster.Spec.MasterService),
		}))
	})

	t.Run("Pod UID replacement between observation and mutation fails closed", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, observedReplica := newMysqlReplicaConvergenceFixture(t)
		replacement := observedReplica.DeepCopy()
		replacement.UID = types.UID("replica-convergence-pod-b")
		commands := make([]string, 0, 1)
		memoryClient := newStatefulSetReconcileMemoryClient(statefulSet, observedReplica)
		reconciler := &MysqlClusterReconciler{
			Client: &mysqlReplicaConvergencePodReplacementClient{
				statefulSetReconcileMemoryClient: memoryClient,
				replacement:                      replacement,
			},
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				return "0\t0\tON\tON\n", nil
			},
		}

		_, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), observedReplica, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("observed UID")))
		g.Expect(commands).To(Equal([]string{mysqlWriteSafetyObservationCommand()}))
		g.Expect(commands).NotTo(ContainElement(mysqlSetSuperReadOnlyCommand()))
	})

	t.Run("ownership change between observation and mutation fails closed", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, observedReplica := newMysqlReplicaConvergenceFixture(t)
		currentReplica := observedReplica.DeepCopy()
		currentReplica.OwnerReferences[0].UID = types.UID("foreign-statefulset-uid")
		commands := make([]string, 0, 1)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, currentReplica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				return "", nil
			},
		}

		_, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), observedReplica, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("controller UID")))
		g.Expect(commands).To(BeEmpty())
	})

	t.Run("oversized MasterService is rejected before any SQL", func(t *testing.T) {
		g := NewWithT(t)
		cluster, statefulSet, replica := newMysqlReplicaConvergenceFixture(t)
		cluster.Spec.MasterService = strings.Repeat("m", mysqlReplicationMasterHostMaxBytes+1)
		execCalls := 0
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(*corev1.Pod, string) (string, error) {
				execCalls++
				return "", nil
			},
		}

		_, err := reconciler.reconcileMysqlReplicaChannel(context.Background(), replica, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("MASTER_HOST limit 60")))
		g.Expect(execCalls).To(Equal(0))
	})
}

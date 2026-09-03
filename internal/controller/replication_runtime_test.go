package controller

import (
	"context"
	"fmt"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

func TestPhase3CReplicationRuntimeActions(t *testing.T) {
	t.Run("healthy NOOP converges without corrective or primary SQL", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase3c-healthy", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		commands := make([]string, 0, 1)
		reconciler := phase1HReconciler(t, statefulSet, primary, replica)
		reconciler.execCommandOnPodFn = func(commandPod *corev1.Pod, command string) (string, error) {
			commands = append(commands, command)
			g.Expect(commandPod.Name).To(Equal(replica.Name))
			return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
		}

		result, converged, err := reconciler.reconcileMysqlHealthyPrimaryRuntime(context.Background(), cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeZero())
		g.Expect(converged).To(BeTrue())
		g.Expect(commands).To(Equal([]string{mysqlShowSlaveStatusCommand()}))
		g.Expect(commands).NotTo(ContainElement(mysqlPreparePrimaryCommand()))
	})

	pendingCases := []struct {
		name               string
		statusOutput       func(masterHost string) string
		expectedCorrection func(masterHost string) string
	}{
		{
			name:               "absent channel CONFIGURE",
			statusOutput:       func(string) string { return "" },
			expectedCorrection: mysqlInitializeReplicaCommand,
		},
		{
			name: "configuration mismatch RECONFIGURE",
			statusOutput: func(string) string {
				return mysqlSlaveStatusOutputForTest("other-primary", "replica", "1", "Yes", "Yes", "", "")
			},
			expectedCorrection: mysqlConfigureReplicaCommand,
		},
		{
			name: "IO Connecting WAIT",
			statusOutput: func(masterHost string) string {
				return mysqlSlaveStatusOutputForTest(masterHost, "replica", "1", "Connecting", "Yes", "", "")
			},
		},
		{
			name: "SQL No WAIT",
			statusOutput: func(masterHost string) string {
				return mysqlSlaveStatusOutputForTest(masterHost, "replica", "1", "Yes", "No", "", "")
			},
		},
		{
			name: "replication error WAIT",
			statusOutput: func(masterHost string) string {
				return mysqlSlaveStatusOutputForTest(masterHost, "replica", "1", "Yes", "Yes", "connection failed", "")
			},
		},
	}
	for _, testCase := range pendingCases {
		t.Run(testCase.name+" remains pending without role publication", func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("phase3c-"+fmt.Sprintf("%d", len(testCase.name)), true)
			statefulSet := phase1HStatefulSet(t, cluster)
			primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
			replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
			memoryClient := newStatefulSetReconcileMemoryClient(statefulSet, primary, replica)
			commands := make([]string, 0, 2)
			reconciler := &MysqlClusterReconciler{
				Client: memoryClient,
				execCommandOnPodFn: func(commandPod *corev1.Pod, command string) (string, error) {
					commands = append(commands, command)
					g.Expect(commandPod.Name).To(Equal(replica.Name))
					if command == mysqlShowSlaveStatusCommand() {
						return testCase.statusOutput(cluster.Spec.MasterService), nil
					}
					return "", nil
				},
			}

			result, converged, err := reconciler.reconcileMysqlHealthyPrimaryRuntime(context.Background(), cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.RequeueAfter).To(Equal(mysqlReplicationRuntimeRequeueAfter))
			g.Expect(converged).To(BeFalse())
			expectedCommands := []string{mysqlShowSlaveStatusCommand()}
			if testCase.expectedCorrection != nil {
				expectedCommands = append(expectedCommands, testCase.expectedCorrection(cluster.Spec.MasterService))
			}
			g.Expect(commands).To(Equal(expectedCommands))
			g.Expect(commands).NotTo(ContainElement(mysqlPreparePrimaryCommand()))
			g.Expect(memoryClient.updateCount).To(Equal(0))
		})
	}

	t.Run("WAIT does not prevent independent replica reconfiguration", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase3c-aggregate", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		replica3 := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		commands := make([]string, 0, 3)
		reconciler := phase1HReconciler(t, statefulSet, primary, replica2, replica3)
		reconciler.execCommandOnPodFn = func(commandPod *corev1.Pod, command string) (string, error) {
			commands = append(commands, commandPod.Name+":"+command)
			switch {
			case command == mysqlShowSlaveStatusCommand() && commandPod.Name == replica2.Name:
				return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Connecting", "Yes", "", ""), nil
			case command == mysqlShowSlaveStatusCommand() && commandPod.Name == replica3.Name:
				return mysqlSlaveStatusOutputForTest("wrong-primary", "replica", "1", "Yes", "Yes", "", ""), nil
			case command == mysqlConfigureReplicaCommand(cluster.Spec.MasterService) && commandPod.Name == replica3.Name:
				return "", nil
			default:
				return "", fmt.Errorf("unexpected command for %s: %s", commandPod.Name, command)
			}
		}

		result, converged, err := reconciler.reconcileMysqlHealthyPrimaryRuntime(context.Background(), cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(Equal(mysqlReplicationRuntimeRequeueAfter))
		g.Expect(converged).To(BeFalse())
		g.Expect(commands).To(Equal([]string{
			replica2.Name + ":" + mysqlShowSlaveStatusCommand(),
			replica3.Name + ":" + mysqlShowSlaveStatusCommand(),
			replica3.Name + ":" + mysqlConfigureReplicaCommand(cluster.Spec.MasterService),
		}))
	})
}

func TestPhase3CReplicationRuntimeRolePublication(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase3c-roleless", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
	rolelessReplica := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 2)
	memoryClient := newStatefulSetReconcileMemoryClient(statefulSet, primary, rolelessReplica)
	healthy := false
	commands := make([]string, 0, 3)
	reconciler := &MysqlClusterReconciler{
		Client: memoryClient,
		execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
			commands = append(commands, command)
			if command == mysqlShowSlaveStatusCommand() {
				if healthy {
					return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
				}
				return "", nil
			}
			if command == mysqlInitializeReplicaCommand(cluster.Spec.MasterService) {
				return "", nil
			}
			return "", fmt.Errorf("unexpected command: %s", command)
		},
	}

	result, converged, err := reconciler.reconcileMysqlHealthyPrimaryRuntime(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(Equal(mysqlReplicationRuntimeRequeueAfter))
	g.Expect(converged).To(BeFalse())
	expectMysqlPodRoleForTest(t, ctx, reconciler, rolelessReplica, "")
	g.Expect(memoryClient.updateCount).To(Equal(0))

	healthy = true
	result, converged, err = reconciler.reconcileMysqlHealthyPrimaryRuntime(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(BeZero())
	g.Expect(converged).To(BeTrue())
	expectMysqlPodRoleForTest(t, ctx, reconciler, rolelessReplica, "slave")
	g.Expect(memoryClient.updateCount).To(Equal(1))
	g.Expect(commands).To(Equal([]string{
		mysqlShowSlaveStatusCommand(),
		mysqlInitializeReplicaCommand(cluster.Spec.MasterService),
		mysqlShowSlaveStatusCommand(),
	}))
}

func TestPhase3CReplicationRuntimeTopologySafety(t *testing.T) {
	t.Run("exactly one published primary is required before SQL", func(t *testing.T) {
		for _, testCase := range []struct {
			name         string
			clusterName  string
			primaryRoles []string
			expected     string
		}{
			{name: "no primary", clusterName: "phase3c-primary-none", primaryRoles: []string{"slave", "slave"}, expected: "found 0"},
			{name: "multiple primaries", clusterName: "phase3c-primary-multiple", primaryRoles: []string{"master", "master"}, expected: "found 2"},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				g := NewWithT(t)
				cluster := phase1HCluster(testCase.clusterName, true)
				statefulSet := phase1HStatefulSet(t, cluster)
				pod1 := phase1HPod(t, cluster, statefulSet, 1, testCase.primaryRoles[0], true)
				pod2 := phase1HPod(t, cluster, statefulSet, 2, testCase.primaryRoles[1], true)
				execCalls := 0
				reconciler := phase1HReconciler(t, statefulSet, pod1, pod2)
				reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
					execCalls++
					return "", nil
				}

				_, _, err := reconciler.reconcileMysqlHealthyPrimaryRuntime(context.Background(), cluster)
				g.Expect(err).To(MatchError(ContainSubstring(testCase.expected)))
				g.Expect(execCalls).To(Equal(0))
			})
		}
	})

	t.Run("partial role labels fail closed before SQL", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase3c-partial-role", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		delete(replica.Labels, LegacyLabelRole)
		execCalls := 0
		reconciler := phase1HReconciler(t, statefulSet, primary, replica)
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, _, err := reconciler.reconcileMysqlHealthyPrimaryRuntime(context.Background(), cluster)
		g.Expect(err).To(MatchError(ContainSubstring("incomplete MySQL role labels")))
		g.Expect(execCalls).To(Equal(0))
	})

	t.Run("ownership ambiguity fails closed before SQL", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase3c-owner-ambiguity", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		replica.OwnerReferences = nil
		execCalls := 0
		reconciler := phase1HReconciler(t, statefulSet, primary, replica)
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, _, err := reconciler.reconcileMysqlHealthyPrimaryRuntime(context.Background(), cluster)
		g.Expect(err).To(MatchError(ContainSubstring("has no controller owner")))
		g.Expect(execCalls).To(Equal(0))
	})

	t.Run("malformed SHOW SLAVE STATUS fails closed", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase3c-malformed", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		commands := make([]string, 0, 1)
		reconciler := phase1HReconciler(t, statefulSet, primary, replica)
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			commands = append(commands, mysqlShowSlaveStatusCommand())
			return "Slave_IO_Running: Yes\nSlave_SQL_Running: Yes\n", nil
		}

		_, _, err := reconciler.reconcileMysqlHealthyPrimaryRuntime(context.Background(), cluster)
		g.Expect(err).To(MatchError(ContainSubstring("missing required fields")))
		g.Expect(commands).To(HaveLen(1))
	})

	t.Run("Endpoint target does not select replica SQL targets", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase3c-endpoint-independence", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		publishedPrimary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica := phase1HPod(t, cluster, statefulSet, 2, "slave", false)
		misleadingEndpoint := phase1HEndpoints(cluster, replica)
		commandsOn := make([]string, 0, 1)
		reconciler := phase1HReconciler(t, cluster, statefulSet, publishedPrimary, replica, misleadingEndpoint)
		reconciler.execCommandOnPodFn = func(commandPod *corev1.Pod, command string) (string, error) {
			commandsOn = append(commandsOn, commandPod.Name)
			g.Expect(command).To(Equal(mysqlShowSlaveStatusCommand()))
			return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
		}

		result, converged, err := reconciler.reconcileMasterSlave(context.Background(), *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.RequeueAfter).To(BeZero())
		g.Expect(converged).To(BeTrue())
		g.Expect(commandsOn).To(Equal([]string{replica.Name}))
	})
}

func TestPhase3CReplicationRuntimeTransitionCompletion(t *testing.T) {
	transition := &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3}
	pendingCases := []struct {
		name             string
		newReplicaStatus func(masterHost string) string
		correction       func(masterHost string) string
	}{
		{name: "CONFIGURE", newReplicaStatus: func(string) string { return "" }, correction: mysqlInitializeReplicaCommand},
		{
			name: "RECONFIGURE",
			newReplicaStatus: func(string) string {
				return mysqlSlaveStatusOutputForTest("wrong-primary", "replica", "1", "Yes", "Yes", "", "")
			},
			correction: mysqlConfigureReplicaCommand,
		},
		{
			name: "WAIT",
			newReplicaStatus: func(masterHost string) string {
				return mysqlSlaveStatusOutputForTest(masterHost, "replica", "1", "Connecting", "Yes", "", "")
			},
		},
	}

	for _, testCase := range pendingCases {
		t.Run(testCase.name+" keeps the transition active", func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase2B2Cluster("phase3c-transition-"+testCase.name, 3, replicaCountCopy(2), transition)
			statefulSet := phase2B2StatefulSet(t, cluster, 3)
			primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
			replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
			newReplica := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
			commands := make([]string, 0, 3)
			reconciler := phase1HReconciler(
				t,
				cluster,
				statefulSet,
				phase1HCredentialSecret(cluster),
				primary,
				replica2,
				newReplica,
				phase1HEndpoints(cluster, primary),
			)
			reconciler.execCommandOnPodFn = func(commandPod *corev1.Pod, command string) (string, error) {
				commands = append(commands, commandPod.Name+":"+command)
				if command == mysqlShowSlaveStatusCommand() {
					if commandPod.Name == newReplica.Name {
						return testCase.newReplicaStatus(cluster.Spec.MasterService), nil
					}
					return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
				}
				if testCase.correction != nil && command == testCase.correction(cluster.Spec.MasterService) {
					return "", nil
				}
				return "", fmt.Errorf("unexpected command for %s: %s", commandPod.Name, command)
			}

			result, complete, err := reconciler.reconcileStatefulSetRuntime(context.Background(), cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result.RequeueAfter).To(Equal(mysqlReplicationRuntimeRequeueAfter))
			g.Expect(complete).To(BeFalse())
			g.Expect(phase2B2StoredCluster(t, reconciler, cluster).Status.ReplicaTransition).To(Equal(transition))
			if testCase.correction != nil {
				g.Expect(commands).To(ContainElement(newReplica.Name + ":" + testCase.correction(cluster.Spec.MasterService)))
			}
			g.Expect(commands).NotTo(ContainElement(primary.Name + ":" + mysqlPreparePrimaryCommand()))
		})
	}
}

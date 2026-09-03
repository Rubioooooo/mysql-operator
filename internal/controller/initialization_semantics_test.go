package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func mysqlSlaveStatusOutputForTest(
	masterHost string,
	masterUser string,
	autoPosition string,
	ioRunning string,
	sqlRunning string,
	lastIOError string,
	lastSQLError string,
) string {
	return fmt.Sprintf(`*************************** 1. row ***************************
               Master_Host: %s
               Master_User: %s
             Auto_Position: %s
          Slave_IO_Running: %s
         Slave_SQL_Running: %s
              Last_IO_Error: %s
             Last_SQL_Error: %s
`, masterHost, masterUser, autoPosition, ioRunning, sqlRunning, lastIOError, lastSQLError)
}

func expectMysqlPodRoleForTest(
	t *testing.T,
	ctx context.Context,
	reconciler *MysqlClusterReconciler,
	pod *corev1.Pod,
	expectedRole string,
) {
	t.Helper()
	g := NewWithT(t)
	stored := &corev1.Pod{}
	g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
	if expectedRole == "" {
		g.Expect(stored.Labels).NotTo(HaveKey(LabelMysqlRole))
		g.Expect(stored.Labels).NotTo(HaveKey(LegacyLabelRole))
		return
	}
	g.Expect(stored.Labels).To(HaveKeyWithValue(LabelMysqlRole, expectedRole))
	g.Expect(stored.Labels).To(HaveKeyWithValue(LegacyLabelRole, expectedRole))
}

func TestMysqlInitializationSemanticConvergence(t *testing.T) {
	ctx := context.Background()
	cluster := statefulSetResourceTestCluster("initial-semantics", types.UID("initial-semantics-uid"))
	cluster.Spec.MasterService = "initial-semantics-primary"
	statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("initial-semantics-statefulset-uid"))
	newTopology := func(t *testing.T) (*corev1.Pod, *corev1.Pod) {
		t.Helper()
		primary := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
		primary.Labels[LabelMysqlRole] = "master"
		primary.Labels[LegacyLabelRole] = "master"
		return primary, statefulSetPodForLifecycleTest(t, cluster, statefulSet, 2)
	}

	t.Run("Connecting does not restart replication or publish slave", func(t *testing.T) {
		g := NewWithT(t)
		primary, replica := newTopology(t)
		commands := make([]string, 0, 1)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, primary, replica),
			execCommandOnPodFn: func(pod *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				if pod.Name != replica.Name || command != mysqlShowSlaveStatusCommand() {
					return "", fmt.Errorf("unexpected command for %s: %s", pod.Name, command)
				}
				return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Connecting", "Yes", "", ""), nil
			},
		}

		converged, err := reconciler.reconcileMysqlInitializationTopology(ctx, primary.Name, []string{replica.Name}, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(converged).To(BeFalse())
		g.Expect(commands).To(Equal([]string{mysqlShowSlaveStatusCommand()}))
		expectMysqlPodRoleForTest(t, ctx, reconciler, primary, "master")
		expectMysqlPodRoleForTest(t, ctx, reconciler, replica, "")
	})

	t.Run("healthy semantic state publishes slave", func(t *testing.T) {
		g := NewWithT(t)
		primary, replica := newTopology(t)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, primary, replica),
			execCommandOnPodFn: func(pod *corev1.Pod, command string) (string, error) {
				if pod.Name != replica.Name || command != mysqlShowSlaveStatusCommand() {
					return "", fmt.Errorf("unexpected command for %s: %s", pod.Name, command)
				}
				return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
			},
		}

		converged, err := reconciler.reconcileMysqlInitializationTopology(ctx, primary.Name, []string{replica.Name}, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(converged).To(BeTrue())
		expectMysqlPodRoleForTest(t, ctx, reconciler, primary, "master")
		expectMysqlPodRoleForTest(t, ctx, reconciler, replica, "slave")
	})

	brokenStates := []struct {
		name              string
		masterHost        string
		masterUser        string
		autoPosition      string
		ioRunning         string
		sqlRunning        string
		lastIOError       string
		lastSQLError      string
		expectReconfigure bool
	}{
		{name: "wrong Master_Host", masterHost: "wrong-primary", masterUser: "replica", autoPosition: "1", ioRunning: "Yes", sqlRunning: "Yes", expectReconfigure: true},
		{name: "wrong Master_User", masterHost: cluster.Spec.MasterService, masterUser: "wrong-user", autoPosition: "1", ioRunning: "Yes", sqlRunning: "Yes", expectReconfigure: true},
		{name: "Auto_Position zero", masterHost: cluster.Spec.MasterService, masterUser: "replica", autoPosition: "0", ioRunning: "Yes", sqlRunning: "Yes", expectReconfigure: true},
		{name: "IO not running", masterHost: cluster.Spec.MasterService, masterUser: "replica", autoPosition: "1", ioRunning: "No", sqlRunning: "Yes"},
		{name: "IO connecting", masterHost: cluster.Spec.MasterService, masterUser: "replica", autoPosition: "1", ioRunning: "Connecting", sqlRunning: "Yes"},
		{name: "SQL not running", masterHost: cluster.Spec.MasterService, masterUser: "replica", autoPosition: "1", ioRunning: "Yes", sqlRunning: "No"},
		{name: "last IO error", masterHost: cluster.Spec.MasterService, masterUser: "replica", autoPosition: "1", ioRunning: "Yes", sqlRunning: "Yes", lastIOError: "connection failed"},
		{name: "last SQL error", masterHost: cluster.Spec.MasterService, masterUser: "replica", autoPosition: "1", ioRunning: "Yes", sqlRunning: "Yes", lastSQLError: "apply failed"},
	}
	for _, testCase := range brokenStates {
		t.Run(testCase.name+" does not publish slave or converge", func(t *testing.T) {
			g := NewWithT(t)
			primary, replica := newTopology(t)
			reconfigureCalls := 0
			reconciler := &MysqlClusterReconciler{
				Client: newStatefulSetReconcileMemoryClient(statefulSet, primary, replica),
				execCommandOnPodFn: func(pod *corev1.Pod, command string) (string, error) {
					if pod.Name != replica.Name {
						return "", fmt.Errorf("unexpected command for %s: %s", pod.Name, command)
					}
					if command == mysqlShowSlaveStatusCommand() {
						return mysqlSlaveStatusOutputForTest(
							testCase.masterHost,
							testCase.masterUser,
							testCase.autoPosition,
							testCase.ioRunning,
							testCase.sqlRunning,
							testCase.lastIOError,
							testCase.lastSQLError,
						), nil
					}
					if command == mysqlConfigureReplicaCommand(cluster.Spec.MasterService) {
						reconfigureCalls++
						return "", nil
					}
					return "", fmt.Errorf("unexpected command for %s: %s", pod.Name, command)
				},
			}

			converged, err := reconciler.reconcileMysqlInitializationTopology(ctx, primary.Name, []string{replica.Name}, *cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(converged).To(BeFalse())
			if testCase.expectReconfigure {
				g.Expect(reconfigureCalls).To(Equal(1))
			} else {
				g.Expect(reconfigureCalls).To(Equal(0))
			}
			expectMysqlPodRoleForTest(t, ctx, reconciler, primary, "master")
			expectMysqlPodRoleForTest(t, ctx, reconciler, replica, "")
		})
	}

	t.Run("mismatched retry targets stable Primary Service", func(t *testing.T) {
		g := NewWithT(t)
		primary, replica := newTopology(t)
		var retryCommand string
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, primary, replica),
			execCommandOnPodFn: func(_ *corev1.Pod, command string) (string, error) {
				if command == mysqlShowSlaveStatusCommand() {
					return mysqlSlaveStatusOutputForTest("wrong-primary", "replica", "1", "No", "Yes", "", ""), nil
				}
				retryCommand = command
				return "", nil
			},
		}

		converged, err := reconciler.reconcileMysqlInitializationTopology(ctx, primary.Name, []string{replica.Name}, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(converged).To(BeFalse())
		g.Expect(retryCommand).To(Equal(mysqlConfigureReplicaCommand(cluster.Spec.MasterService)))
		g.Expect(strings.Index(retryCommand, "STOP SLAVE")).To(BeNumerically("<", strings.Index(retryCommand, "CHANGE MASTER TO")))
		g.Expect(retryCommand).NotTo(ContainSubstring(mysqlHeadlessServiceName(cluster)))
	})
}

func TestParseMysqlShowSlaveStatus(t *testing.T) {
	g := NewWithT(t)
	status, err := parseMysqlShowSlaveStatus(mysqlSlaveStatusOutputForTest(
		"mysql-primary",
		"replica",
		"1",
		"Yes",
		"Yes",
		"",
		"",
	))
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(status.Configured).To(BeTrue())
	g.Expect(status.MasterHost).To(Equal("mysql-primary"))
	g.Expect(status.MasterUser).To(Equal("replica"))
	g.Expect(status.AutoPosition).To(Equal("1"))
	g.Expect(status.semanticallyHealthy("mysql-primary")).To(BeTrue())
}

func TestStatefulSetInitializationStages(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("initial-stages", false)
	cluster.Spec.Replicas = replicaCountCopy(2)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
	replica := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 2)
	memoryClient := newStatefulSetReconcileMemoryClient(
		cluster,
		statefulSet,
		phase1HCredentialSecret(cluster),
		primary,
		replica,
	)
	reconciler := &MysqlClusterReconciler{
		Client: &lifecycleAnnotationPatchClient{statefulSetReconcileMemoryClient: memoryClient},
		Scheme: newStatefulSetReconcileTestScheme(t),
	}
	stageAComplete := false
	reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		switch {
		case pod.Name == primary.Name && command == mysqlPreparePrimaryCommand():
			return "", nil
		case pod.Name == replica.Name && command == mysqlShowSlaveStatusCommand():
			if stageAComplete {
				return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
			}
			return "", nil
		case pod.Name == replica.Name && command == mysqlInitializeReplicaCommand(cluster.Spec.MasterService):
			return "", nil
		default:
			return "", fmt.Errorf("unexpected command for %s: %s", pod.Name, command)
		}
	}

	result, complete, err := reconciler.reconcileStatefulSetInitialization(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(complete).To(BeFalse())
	g.Expect(result.RequeueAfter).To(Equal(mysqlInitializationConvergenceRequeueAfter))
	expectMysqlPodRoleForTest(t, ctx, reconciler, primary, "master")
	expectMysqlPodRoleForTest(t, ctx, reconciler, replica, "")
	storedCluster := &databasev1.MysqlCluster{}
	g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(cluster), storedCluster)).To(Succeed())
	g.Expect(storedCluster.Annotations).NotTo(HaveKey(mysqlClusterInitializedAnnotation))
	g.Expect(storedCluster.Status.LastConvergedReplicas).To(BeNil())
	g.Expect(storedCluster.Status.ReplicaTransition).To(BeNil())

	stageAComplete = true
	result, complete, err = reconciler.reconcileStatefulSetInitialization(ctx, storedCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(complete).To(BeTrue())
	g.Expect(result.RequeueAfter).To(BeZero())
	expectMysqlPodRoleForTest(t, ctx, reconciler, primary, "master")
	expectMysqlPodRoleForTest(t, ctx, reconciler, replica, "slave")
	completedCluster := &databasev1.MysqlCluster{}
	g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(cluster), completedCluster)).To(Succeed())
	g.Expect(completedCluster.Annotations).To(HaveKeyWithValue(mysqlClusterInitializedAnnotation, "true"))
	g.Expect(completedCluster.Status.LastConvergedReplicas).To(BeNil())
	g.Expect(completedCluster.Status.ReplicaTransition).To(BeNil())
}

func TestInitializationRejectsOversizedMasterServiceBeforeSQL(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("initial-master-host-limit", false)
	cluster.Spec.MasterService = strings.Repeat("m", mysqlReplicationMasterHostMaxBytes+1)
	memoryClient := newStatefulSetReconcileMemoryClient(cluster)
	reconciler := &MysqlClusterReconciler{
		Client: memoryClient,
		Scheme: newStatefulSetReconcileTestScheme(t),
	}
	execCalls := 0
	reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
		execCalls++
		return "", nil
	}

	_, complete, err := reconciler.reconcileStatefulSetInitialization(ctx, cluster)
	g.Expect(err).To(MatchError(ContainSubstring("MASTER_HOST limit 60")))
	g.Expect(complete).To(BeFalse())
	g.Expect(execCalls).To(Equal(0))
	g.Expect(memoryClient.updateCount).To(Equal(0))
	g.Expect(memoryClient.statusPatchCount).To(Equal(0))
	g.Expect(cluster.Annotations).NotTo(HaveKey(mysqlClusterInitializedAnnotation))
}

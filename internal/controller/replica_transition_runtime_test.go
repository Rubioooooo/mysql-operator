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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func phase2B2Cluster(
	name string,
	desired int32,
	lastConverged *int32,
	transition *databasev1.MysqlClusterReplicaTransitionStatus,
) *databasev1.MysqlCluster {
	cluster := phase1HCluster(name, true)
	cluster.Spec.Replicas = replicaCountCopy(desired)
	cluster.Status.LastConvergedReplicas = lastConverged
	cluster.Status.ReplicaTransition = replicaTransitionCopy(transition)
	cluster.Status.Primary = "status-primary-sentinel"
	cluster.Status.Conditions = []metav1.Condition{{
		Type:   "StatusSentinel",
		Status: metav1.ConditionTrue,
		Reason: "Preserved",
	}}
	return cluster
}

func phase2B2StatefulSet(
	t *testing.T,
	cluster *databasev1.MysqlCluster,
	replicas int32,
) *appsv1.StatefulSet {
	t.Helper()
	statefulSet := phase1HStatefulSet(t, cluster)
	statefulSet.Spec.Replicas = replicaCountCopy(replicas)
	return statefulSet
}

func phase2B2StoredCluster(t *testing.T, reconciler *MysqlClusterReconciler, cluster *databasev1.MysqlCluster) *databasev1.MysqlCluster {
	t.Helper()
	stored := &databasev1.MysqlCluster{}
	NewWithT(t).Expect(reconciler.Get(context.Background(), client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
	return stored
}

func phase2B2StoredStatefulSet(t *testing.T, reconciler *MysqlClusterReconciler, cluster *databasev1.MysqlCluster) *appsv1.StatefulSet {
	t.Helper()
	stored := &appsv1.StatefulSet{}
	NewWithT(t).Expect(reconciler.Get(context.Background(), client.ObjectKey{
		Namespace: cluster.Namespace,
		Name:      mysqlStatefulSetName(cluster),
	}, stored)).To(Succeed())
	return stored
}

func phase2B2HealthyDomainExec(t *testing.T, reconciler *MysqlClusterReconciler) *int {
	t.Helper()
	calls := 0
	reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
		calls++
		switch command {
		case mysqlShowSlaveStatusCommand():
			return "Slave_SQL_Running: Yes\nSlave_IO_Running: Yes\n", nil
		case mysqlPreparePrimaryCommand(), mysqlConfigureReplicaCommand(""):
			return "", nil
		}
		if strings.HasPrefix(command, mysqlReplicationPasswordSQLAssignment+"; ") {
			return "", nil
		}
		return "", fmt.Errorf("unexpected command: %s", command)
	}
	return &calls
}

func TestPhase2B2BootstrapAndIntentOrdering(t *testing.T) {
	ctx := context.Background()

	t.Run("legacy stable bootstrap persists the StatefulSet checkpoint and returns", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-bootstrap-stable", 3, nil, nil)
		statefulSet := phase2B2StatefulSet(t, cluster, 3)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster))
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		g.Expect(execCalls).To(Equal(0))
		stored := phase2B2StoredCluster(t, reconciler, cluster)
		g.Expect(stored.Status.LastConvergedReplicas).To(HaveValue(Equal(int32(3))))
		g.Expect(stored.Status.ReplicaTransition).To(BeNil())
		g.Expect(stored.Status.CredentialsSecretUID).To(Equal("phase1h-credential-secret-uid"))
		g.Expect(stored.Status.Primary).To(Equal("status-primary-sentinel"))
		g.Expect(stored.Status.Conditions).To(HaveLen(1))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(3))))
	})

	t.Run("legacy drift bootstrap persists intent before changing the StatefulSet", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-bootstrap-drift", 3, nil, nil)
		statefulSet := phase2B2StatefulSet(t, cluster, 2)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster))
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		g.Expect(execCalls).To(Equal(0))
		stored := phase2B2StoredCluster(t, reconciler, cluster)
		g.Expect(stored.Status.LastConvergedReplicas).To(HaveValue(Equal(int32(2))))
		g.Expect(stored.Status.ReplicaTransition).To(Equal(&databasev1.MysqlClusterReplicaTransitionStatus{
			FromReplicas: 2, TargetReplicas: 3,
		}))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(2))))
	})

	t.Run("missing StatefulSet bootstrap records desired compatibility checkpoint before recreation", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-bootstrap-missing", 3, nil, nil)
		reconciler := phase1HReconciler(t, cluster, phase1HCredentialSecret(cluster))

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		stored := phase2B2StoredCluster(t, reconciler, cluster)
		g.Expect(stored.Status.LastConvergedReplicas).To(HaveValue(Equal(int32(3))))
		g.Expect(stored.Status.ReplicaTransition).To(BeNil())
		missing := &appsv1.StatefulSet{}
		g.Expect(reconciler.Get(ctx, client.ObjectKey{
			Namespace: cluster.Namespace,
			Name:      mysqlStatefulSetName(cluster),
		}, missing)).To(MatchError(ContainSubstring("not found")))

		_, _, err = reconciler.reconcileStatefulSetRuntime(ctx, stored)
		g.Expect(err).To(HaveOccurred()) // Existing runtime recreation reaches the domain path.
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(3))))
	})

	t.Run("new transition status is persisted before replica mutation", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-intent", 3, replicaCountCopy(2), nil)
		statefulSet := phase2B2StatefulSet(t, cluster, 2)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster))

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		g.Expect(phase2B2StoredCluster(t, reconciler, cluster).Status.ReplicaTransition).To(Equal(
			&databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3},
		))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(2))))
	})

	t.Run("failed transition intent persistence prevents replica mutation", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-intent-write-failure", 3, replicaCountCopy(2), nil)
		statefulSet := phase2B2StatefulSet(t, cluster, 2)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster))
		memoryClient := reconciler.Client.(*statefulSetReconcileMemoryClient)
		memoryClient.statusPatchError = errors.New("status API unavailable")

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("status API unavailable")))
		g.Expect(complete).To(BeFalse())
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(2))))
		g.Expect(phase2B2StoredCluster(t, reconciler, cluster).Status.ReplicaTransition).To(BeNil())
	})

	for _, testCase := range []struct {
		name        string
		from        int32
		oldTarget   int32
		newTarget   int32
		setReplicas int32
	}{
		{name: "scale up target expands", from: 3, oldTarget: 4, newTarget: 5, setReplicas: 4},
		{name: "scale up reverses to scale down", from: 3, oldTarget: 5, newTarget: 2, setReplicas: 5},
		{name: "target returns to from", from: 3, oldTarget: 4, newTarget: 3, setReplicas: 4},
	} {
		t.Run(testCase.name+" persists only the new target first", func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase2B2Cluster(
				"phase2b2-target-"+strings.ReplaceAll(testCase.name, " ", "-"),
				testCase.newTarget,
				replicaCountCopy(testCase.from),
				&databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: testCase.from, TargetReplicas: testCase.oldTarget},
			)
			statefulSet := phase2B2StatefulSet(t, cluster, testCase.setReplicas)
			reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster))

			_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(complete).To(BeFalse())
			stored := phase2B2StoredCluster(t, reconciler, cluster)
			g.Expect(stored.Status.LastConvergedReplicas).To(HaveValue(Equal(testCase.from)))
			g.Expect(stored.Status.ReplicaTransition).To(Equal(
				&databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: testCase.from, TargetReplicas: testCase.newTarget},
			))
			g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(testCase.setReplicas)))
		})
	}

	t.Run("invalid durable status fails before any mutation or SQL", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-invalid-status", 3, replicaCountCopy(0), nil)
		statefulSet := phase2B2StatefulSet(t, cluster, 2)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster))
		memoryClient := reconciler.Client.(*statefulSetReconcileMemoryClient)
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("invalid replica transition status")))
		g.Expect(complete).To(BeFalse())
		g.Expect(execCalls).To(Equal(0))
		g.Expect(memoryClient.updateCount).To(Equal(0))
		g.Expect(memoryClient.statusPatchCount).To(Equal(0))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(2))))
	})
}

func TestPhase2B2ScaleUpDeltaGate(t *testing.T) {
	ctx := context.Background()
	transition := &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3}

	for _, testCase := range []struct {
		name      string
		newMember func(*testing.T, *databasev1.MysqlCluster, *appsv1.StatefulSet) *corev1.Pod
	}{
		{name: "new member missing"},
		{
			name: "new member NotReady",
			newMember: func(t *testing.T, cluster *databasev1.MysqlCluster, statefulSet *appsv1.StatefulSet) *corev1.Pod {
				return phase1HPod(t, cluster, statefulSet, 3, "slave", false)
			},
		},
	} {
		t.Run(testCase.name+" blocks SQL without error", func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase2B2Cluster("phase2b2-scaleup-"+strings.ReplaceAll(testCase.name, " ", "-"), 3, replicaCountCopy(2), transition)
			statefulSet := phase2B2StatefulSet(t, cluster, 2)
			objects := []client.Object{cluster, statefulSet, phase1HCredentialSecret(cluster)}
			if testCase.newMember != nil {
				objects = append(objects, testCase.newMember(t, cluster, statefulSet))
			}
			reconciler := phase1HReconciler(t, objects...)
			execCalls := 0
			reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
				execCalls++
				return "", nil
			}

			_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(complete).To(BeFalse())
			g.Expect(execCalls).To(Equal(0))
			g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(3))))
			g.Expect(phase2B2StoredCluster(t, reconciler, cluster).Status.ReplicaTransition).NotTo(BeNil())
		})
	}

	t.Run("Ready new member allows established primary failure to enter HA", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-scaleup-primary-ha", 3, replicaCountCopy(2), transition)
		statefulSet := phase2B2StatefulSet(t, cluster, 3)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		newReplica := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), primary, replica2, newReplica, phase1HEndpoints(cluster, nil))
		reconciler.MasterGTIDSnapshot = "uuid:1-10"
		promoted := ""
		reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
			switch {
			case command == mysqlShowSlaveGTIDCommand():
				return "uuid:1-10", nil
			case strings.HasPrefix(command, "du -sb "):
				if pod.Name == newReplica.Name {
					return "200", nil
				}
				return "100", nil
			case command == mysqlPreparePrimaryCommand():
				promoted = pod.Name
				return "", nil
			case command == mysqlConfigureReplicaCommand(cluster.Spec.MasterService):
				return "", nil
			default:
				return "", fmt.Errorf("unexpected command for %s: %s", pod.Name, command)
			}
		}

		_, _, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(promoted).To(Equal(newReplica.Name))
		g.Expect(phase2B2StoredCluster(t, reconciler, cluster).Status.ReplicaTransition).NotTo(BeNil())
	})

	t.Run("Ready new member does not globally gate an established NotReady replica", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-scaleup-replica-domain", 3, replicaCountCopy(2), transition)
		statefulSet := phase2B2StatefulSet(t, cluster, 3)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", false)
		newReplica := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), primary, replica2, newReplica, phase1HEndpoints(cluster, primary))
		execCalls := 0
		reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
			execCalls++
			if pod.Name == replica2.Name && command == mysqlShowSlaveStatusCommand() {
				return "", errors.New("established replica MySQL is unavailable")
			}
			return "Slave_SQL_Running: Yes\nSlave_IO_Running: Yes\n", nil
		}

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("established replica MySQL is unavailable")))
		g.Expect(complete).To(BeFalse())
		g.Expect(execCalls).To(BeNumerically(">", 0))
		g.Expect(phase2B2StoredCluster(t, reconciler, cluster).Status.ReplicaTransition).NotTo(BeNil())
	})

	t.Run("same-UID spoofed new member fails closed before SQL", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-scaleup-spoof", 3, replicaCountCopy(2), transition)
		statefulSet := phase2B2StatefulSet(t, cluster, 2)
		spoofed := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		spoofed.OwnerReferences = nil
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), spoofed)
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, _, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("has no controller owner")))
		g.Expect(execCalls).To(Equal(0))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(2))))
	})
}

func TestPhase2B2ScaleDownDeltaGate(t *testing.T) {
	ctx := context.Background()
	transition := &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 2}

	t.Run("stable child over-replication cannot remove the current primary", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-stable-drift-primary-safety", 2, replicaCountCopy(2), nil)
		statefulSet := phase2B2StatefulSet(t, cluster, 3)
		replica1 := phase1HPod(t, cluster, statefulSet, 1, "slave", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		primary3 := phase1HPod(t, cluster, statefulSet, 3, "master", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), replica1, replica2, primary3)
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, _, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("would remove current Primary ordinal 3")))
		g.Expect(execCalls).To(Equal(0))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(3))))
		stored := phase2B2StoredCluster(t, reconciler, cluster)
		g.Expect(stored.Status.LastConvergedReplicas).To(HaveValue(Equal(int32(2))))
		g.Expect(stored.Status.ReplicaTransition).To(BeNil())
	})

	t.Run("stable child over-replication is repaired when the primary is retained", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-stable-drift-safe", 2, replicaCountCopy(2), nil)
		statefulSet := phase2B2StatefulSet(t, cluster, 3)
		primary1 := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		replica3 := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		reconciler := phase1HReconciler(
			t,
			cluster,
			statefulSet,
			phase1HCredentialSecret(cluster),
			primary1,
			replica2,
			replica3,
			phase1HEndpoints(cluster, primary1),
		)
		execCalls := phase2B2HealthyDomainExec(t, reconciler)

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeTrue())
		g.Expect(*execCalls).To(BeNumerically(">", 0))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(2))))
		stored := phase2B2StoredCluster(t, reconciler, cluster)
		g.Expect(stored.Status.LastConvergedReplicas).To(HaveValue(Equal(int32(2))))
		g.Expect(stored.Status.ReplicaTransition).To(BeNil())
	})

	t.Run("removed member still observable blocks SQL after replica target mutation", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-scaledown-pending", 2, replicaCountCopy(3), transition)
		statefulSet := phase2B2StatefulSet(t, cluster, 3)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		removed := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), primary, replica2, removed)
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		g.Expect(execCalls).To(Equal(0))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(2))))
		g.Expect(phase2B2StoredCluster(t, reconciler, cluster).Status.ReplicaTransition).NotTo(BeNil())
	})

	t.Run("removed member gone allows domain reconciliation and completion", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-scaledown-ready", 2, replicaCountCopy(3), transition)
		statefulSet := phase2B2StatefulSet(t, cluster, 2)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), primary, replica2, phase1HEndpoints(cluster, primary))
		execCalls := phase2B2HealthyDomainExec(t, reconciler)

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse()) // Completion status write returns immediately.
		g.Expect(*execCalls).To(BeNumerically(">", 0))
		stored := phase2B2StoredCluster(t, reconciler, cluster)
		g.Expect(stored.Status.LastConvergedReplicas).To(HaveValue(Equal(int32(2))))
		g.Expect(stored.Status.ReplicaTransition).To(BeNil())
	})

	t.Run("retained primary NotReady enters HA once removed member is gone", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-scaledown-primary-ha", 2, replicaCountCopy(3), transition)
		statefulSet := phase2B2StatefulSet(t, cluster, 2)
		oldPrimary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), oldPrimary, replica2, phase1HEndpoints(cluster, nil))
		reconciler.MasterGTIDSnapshot = "uuid:1-10"
		promoted := false
		reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
			switch {
			case command == mysqlShowSlaveGTIDCommand():
				return "uuid:1-10", nil
			case strings.HasPrefix(command, "du -sb "):
				return "100", nil
			case command == mysqlPreparePrimaryCommand():
				promoted = true
				return "", nil
			default:
				return "", fmt.Errorf("unexpected command: %s", command)
			}
		}

		_, _, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(promoted).To(BeTrue())
		g.Expect(phase2B2StoredCluster(t, reconciler, cluster).Status.ReplicaTransition).NotTo(BeNil())
	})

	t.Run("scale-down that would remove the primary is rejected before mutation", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-scaledown-primary-safety", 2, replicaCountCopy(3), transition)
		statefulSet := phase2B2StatefulSet(t, cluster, 3)
		replica1 := phase1HPod(t, cluster, statefulSet, 1, "slave", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		primary3 := phase1HPod(t, cluster, statefulSet, 3, "master", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), replica1, replica2, primary3)
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, _, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("would remove current Primary ordinal 3")))
		g.Expect(execCalls).To(Equal(0))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(3))))
	})

	t.Run("return-to-from remains active while an extra member is observable", func(t *testing.T) {
		g := NewWithT(t)
		returnTransition := &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 3, TargetReplicas: 3}
		cluster := phase2B2Cluster("phase2b2-return-to-from", 3, replicaCountCopy(3), returnTransition)
		statefulSet := phase2B2StatefulSet(t, cluster, 4)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		replica3 := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		extra := phase1HPod(t, cluster, statefulSet, 4, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), primary, replica2, replica3, extra)
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		g.Expect(execCalls).To(Equal(0))
		g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(3))))
		g.Expect(phase2B2StoredCluster(t, reconciler, cluster).Status.ReplicaTransition).To(Equal(returnTransition))
	})
}

func TestPhase2B2CompletionDurability(t *testing.T) {
	ctx := context.Background()
	transition := &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3}

	t.Run("domain failure leaves the stable checkpoint and transition unchanged", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-domain-failure", 3, replicaCountCopy(2), transition)
		statefulSet := phase2B2StatefulSet(t, cluster, 3)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		replica3 := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), primary, replica2, replica3, phase1HEndpoints(cluster, primary))
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			return "", errors.New("domain SQL failure")
		}

		_, _, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("domain SQL failure")))
		stored := phase2B2StoredCluster(t, reconciler, cluster)
		g.Expect(stored.Status.LastConvergedReplicas).To(HaveValue(Equal(int32(2))))
		g.Expect(stored.Status.ReplicaTransition).To(Equal(transition))
	})

	t.Run("domain success without full target membership does not clear, later recovery does", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-full-convergence", 3, replicaCountCopy(2), transition)
		statefulSet := phase2B2StatefulSet(t, cluster, 3)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		newReplica := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		first := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), primary, newReplica, phase1HEndpoints(cluster, primary))
		firstCalls := phase2B2HealthyDomainExec(t, first)

		_, complete, err := first.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeTrue())
		g.Expect(*firstCalls).To(BeNumerically(">", 0))
		stored := phase2B2StoredCluster(t, first, cluster)
		g.Expect(stored.Status.LastConvergedReplicas).To(HaveValue(Equal(int32(2))))
		g.Expect(stored.Status.ReplicaTransition).NotTo(BeNil())

		recoveredReplica := phase1HPod(t, stored, statefulSet, 2, "slave", true)
		second := phase1HReconciler(t, stored, statefulSet, phase1HCredentialSecret(stored), primary, recoveredReplica, newReplica, phase1HEndpoints(stored, primary))
		secondCalls := phase2B2HealthyDomainExec(t, second)
		_, complete, err = second.reconcileStatefulSetRuntime(ctx, stored)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		g.Expect(*secondCalls).To(BeNumerically(">", 0))
		completed := phase2B2StoredCluster(t, second, stored)
		g.Expect(completed.Status.LastConvergedReplicas).To(HaveValue(Equal(int32(3))))
		g.Expect(completed.Status.ReplicaTransition).To(BeNil())
	})

	t.Run("persisted transition is sufficient for a new reconciler process", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase2B2Cluster("phase2b2-restart", 3, replicaCountCopy(2), nil)
		statefulSet := phase2B2StatefulSet(t, cluster, 2)
		first := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster))
		_, _, err := first.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		storedCluster := phase2B2StoredCluster(t, first, cluster)
		storedStatefulSet := phase2B2StoredStatefulSet(t, first, cluster)
		g.Expect(storedCluster.Status.ReplicaTransition).To(Equal(transition))
		g.Expect(storedStatefulSet.Spec.Replicas).To(HaveValue(Equal(int32(2))))

		second := phase1HReconciler(t, storedCluster, storedStatefulSet, phase1HCredentialSecret(storedCluster))
		execCalls := 0
		second.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}
		_, complete, err := second.reconcileStatefulSetRuntime(ctx, storedCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		g.Expect(execCalls).To(Equal(0))
		g.Expect(phase2B2StoredStatefulSet(t, second, storedCluster).Spec.Replicas).To(HaveValue(Equal(int32(3))))
		g.Expect(phase2B2StoredCluster(t, second, storedCluster).Status.ReplicaTransition).To(Equal(transition))
	})
}

func TestPhase2B2LegacyBootstrapDoesNotSuppressHA(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase2B2Cluster("phase2b2-bootstrap-ha", 3, nil, nil)
	statefulSet := phase2B2StatefulSet(t, cluster, 3)
	oldPrimary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
	replica3 := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
	reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), oldPrimary, replica2, replica3, phase1HEndpoints(cluster, nil))
	reconciler.MasterGTIDSnapshot = "uuid:1-10"
	execCalls := 0
	reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		execCalls++
		switch {
		case command == mysqlShowSlaveGTIDCommand():
			return "uuid:1-10", nil
		case strings.HasPrefix(command, "du -sb "):
			if pod.Name == replica3.Name {
				return "200", nil
			}
			return "100", nil
		case command == mysqlPreparePrimaryCommand(), command == mysqlConfigureReplicaCommand(cluster.Spec.MasterService):
			return "", nil
		default:
			return "", fmt.Errorf("unexpected command: %s", command)
		}
	}

	_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(complete).To(BeFalse())
	g.Expect(execCalls).To(Equal(0))
	stored := phase2B2StoredCluster(t, reconciler, cluster)

	_, complete, err = reconciler.reconcileStatefulSetRuntime(ctx, stored)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(complete).To(BeTrue())
	g.Expect(execCalls).To(BeNumerically(">", 0))
	storedReplica3 := &corev1.Pod{}
	g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(replica3), storedReplica3)).To(Succeed())
	g.Expect(storedReplica3.Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
}

func TestPhase2B2OrdinalIdentityFailsClosed(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase2B2Cluster(
		"phase2b2-ordinal-identity",
		3,
		replicaCountCopy(2),
		&databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3},
	)
	statefulSet := phase2B2StatefulSet(t, cluster, 2)
	malformed := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
	malformed.Name = mysqlStatefulSetPodName(cluster, 2)
	malformed.UID = types.UID("malformed-ordinal-identity")
	reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), malformed)
	execCalls := 0
	reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
		execCalls++
		return "", nil
	}

	_, _, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
	g.Expect(err).To(MatchError(ContainSubstring("ordinal identity")))
	g.Expect(execCalls).To(Equal(0))
	g.Expect(phase2B2StoredStatefulSet(t, reconciler, cluster).Spec.Replicas).To(HaveValue(Equal(int32(2))))
}

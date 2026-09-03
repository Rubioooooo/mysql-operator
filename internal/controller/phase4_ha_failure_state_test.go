package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func phase4StoredCluster(t *testing.T, reconciler *MysqlClusterReconciler, cluster *databasev1.MysqlCluster) *databasev1.MysqlCluster {
	t.Helper()
	stored := &databasev1.MysqlCluster{}
	NewWithT(t).Expect(reconciler.Get(context.Background(), client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
	return stored
}

func phase4HAStatus(state databasev1.MysqlClusterHAState, primary *corev1.Pod) *databasev1.MysqlClusterHAStatus {
	return &databasev1.MysqlClusterHAStatus{
		State:      state,
		Primary:    primary.Name,
		PrimaryUID: string(primary.UID),
	}
}

func TestPhase4HAFailureClassification(t *testing.T) {
	ctx := context.Background()

	t.Run("healthy Endpoint persists Healthy without using TargetRef as topology authority", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase4-healthy", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		misleadingEndpoint := phase1HEndpoints(cluster, replica)
		reconciler := phase1HReconciler(t, cluster, statefulSet, primary, replica, misleadingEndpoint)
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
		}

		result, converged, err := reconciler.reconcileMasterSlave(ctx, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(converged).To(BeTrue())
		g.Expect(result.RequeueAfter).To(BeZero())
		g.Expect(execCalls).To(Equal(1))
		g.Expect(phase4StoredCluster(t, reconciler, cluster).Status.HA).To(Equal(phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, primary)))
	})

	t.Run("missing tracked primary is immediately confirmed and persisted", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase4-missing", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		missingPrimary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
		cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, missingPrimary)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, replica2, phase1HEndpoints(cluster, nil))
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(execCalls).To(Equal(0))
		storedHA := phase4StoredCluster(t, reconciler, cluster).Status.HA
		g.Expect(storedHA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverRequired))
		g.Expect(storedHA.Primary).To(Equal(missingPrimary.Name))
		g.Expect(storedHA.PrimaryUID).To(Equal(string(missingPrimary.UID)))
		g.Expect(storedHA.FailureCount).To(Equal(int32(1)))
		g.Expect(storedHA.FirstFailureTime).NotTo(BeNil())
	})

	t.Run("same UID NotReady requires two observations and recovery resets suspicion", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase4-suspicion", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
		replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		endpoints := phase1HEndpoints(cluster, nil)
		reconciler := phase1HReconciler(t, cluster, statefulSet, primary, replica, endpoints)
		execCalls := 0
		promotionCalls := 0
		reconciler.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
			execCalls++
			if command == mysqlPreparePrimaryCommand() {
				promotionCalls++
			}
			if command == mysqlShowSlaveStatusCommand() {
				return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
			}
			return "", nil
		}

		_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		first := phase4StoredCluster(t, reconciler, cluster)
		g.Expect(first.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateSuspected))
		g.Expect(first.Status.HA.FailureCount).To(Equal(int32(1)))
		g.Expect(first.Status.HA.FirstFailureTime).NotTo(BeNil())
		g.Expect(execCalls).To(Equal(0))

		_, _, err = reconciler.reconcileMasterSlave(ctx, *first)
		g.Expect(err).NotTo(HaveOccurred())
		second := phase4StoredCluster(t, reconciler, cluster)
		g.Expect(second.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverRequired))
		g.Expect(second.Status.HA.FailureCount).To(Equal(int32(2)))
		g.Expect(second.Status.HA.FirstFailureTime).To(Equal(first.Status.HA.FirstFailureTime))
		g.Expect(execCalls).To(Equal(0), "the confirming reconcile must only persist FailoverRequired")

		recoveredPrimary := primary.DeepCopy()
		for i := range recoveredPrimary.Status.ContainerStatuses {
			if recoveredPrimary.Status.ContainerStatuses[i].Name == mysqlContainerName {
				recoveredPrimary.Status.ContainerStatuses[i].Ready = true
			}
		}
		g.Expect(reconciler.Update(ctx, recoveredPrimary)).To(Succeed())
		g.Expect(reconciler.Update(ctx, phase1HEndpoints(cluster, recoveredPrimary))).To(Succeed())

		_, _, err = reconciler.reconcileMasterSlave(ctx, *second)
		g.Expect(err).NotTo(HaveOccurred())
		recovered := phase4StoredCluster(t, reconciler, cluster)
		g.Expect(recovered.Status.HA).To(Equal(phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, recoveredPrimary)))
		g.Expect(execCalls).To(Equal(1), "recovery may resume Phase 3 semantic observation")
		g.Expect(promotionCalls).To(Equal(0))
	})

	t.Run("missing Endpoint with Ready primary is Degraded and never mutates MySQL", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase4-degraded", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, primary, replica, phase1HEndpoints(cluster, nil))
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(phase4StoredCluster(t, reconciler, cluster).Status.HA).To(Equal(phase4HAStatus(databasev1.MysqlClusterHAStateDegraded, primary)))
		g.Expect(execCalls).To(Equal(0))
	})
}

func TestPhase4HAFailsClosedBeforeSQL(t *testing.T) {
	ctx := context.Background()

	t.Run("zero published primary without tracked identity", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase4-zero-primary", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		replica1 := phase1HPod(t, cluster, statefulSet, 1, "slave", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, replica1, replica2, phase1HEndpoints(cluster, nil))
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { execCalls++; return "", nil }

		_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
		g.Expect(err).To(MatchError(ContainSubstring("zero published primaries")))
		g.Expect(execCalls).To(Equal(0))
	})

	t.Run("multiple published primaries", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase4-multiple-primary", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary1 := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		primary2 := phase1HPod(t, cluster, statefulSet, 2, "master", true)
		reconciler := phase1HReconciler(t, cluster, statefulSet, primary1, primary2, phase1HEndpoints(cluster, primary1))
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { execCalls++; return "", nil }

		_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
		g.Expect(err).To(MatchError(ContainSubstring("at most one published primary")))
		g.Expect(execCalls).To(Equal(0))
	})

	t.Run("foreign ownership", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase4-foreign-owner", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		foreign := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      mysqlStatefulSetPodName(cluster, 1),
			Namespace: cluster.Namespace,
			UID:       types.UID("foreign-primary-uid"),
			Labels:    mysqlRoleLabels(cluster, "master"),
		}}
		foreign.Labels[statefulSetPodIndexLabel] = "1"
		reconciler := phase1HReconciler(t, cluster, statefulSet, foreign, phase1HEndpoints(cluster, foreign))
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { execCalls++; return "", nil }

		_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
		g.Expect(err).To(MatchError(ContainSubstring("has no controller owner")))
		g.Expect(execCalls).To(Equal(0))
	})
}

func TestPhase4HAUIDReplacementInvalidatesStaleDecision(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase4-uid-replacement", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateFailoverRequired, primary)
	cluster.Status.HA.FailureCount = 2
	replacement := primary.DeepCopy()
	replacement.UID = types.UID("replacement-primary-uid")
	replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
	reconciler := phase1HReconciler(t, cluster, statefulSet, replacement, replica, phase1HEndpoints(cluster, nil))
	execCalls := 0
	reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { execCalls++; return "", nil }

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	stored := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateSuspected))
	g.Expect(stored.Status.HA.PrimaryUID).To(Equal(string(replacement.UID)))
	g.Expect(stored.Status.HA.FailureCount).To(Equal(int32(1)))
	g.Expect(execCalls).To(Equal(0))
}

func TestPhase4HAMutationBarrierAndVerifying(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase4-mutation-barrier", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	oldPrimary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
	replica3 := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
	reconciler := phase1HReconciler(t, cluster, statefulSet, phase1HCredentialSecret(cluster), oldPrimary, replica2, replica3, phase1HEndpoints(cluster, nil))
	reconciler.MasterGTIDSnapshot = "uuid:1-10"
	promotionCalls := 0
	execCalls := 0
	reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		execCalls++
		stored := phase4StoredCluster(t, reconciler, cluster)
		g.Expect(stored.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverInProgress), "HA SQL must run only after durable in-progress publication")
		g.Expect(stored.Status.HA.Primary).To(Equal(oldPrimary.Name))
		g.Expect(stored.Status.HA.PrimaryUID).To(Equal(string(oldPrimary.UID)))
		switch {
		case command == mysqlShowSlaveGTIDCommand():
			return "uuid:1-10\n", nil
		case strings.HasPrefix(command, "du -sb "):
			if pod.Name == replica3.Name {
				return "200\n", nil
			}
			return "100\n", nil
		case command == mysqlPreparePrimaryCommand():
			promotionCalls++
			return "", nil
		case command == mysqlConfigureReplicaCommand(cluster.Spec.MasterService):
			return "", nil
		default:
			return "", fmt.Errorf("unexpected command for Pod %s: %s", pod.Name, command)
		}
	}

	_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	suspected := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(suspected.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateSuspected))
	g.Expect(execCalls).To(Equal(0))

	_, _, err = reconciler.reconcileMasterSlave(ctx, *suspected)
	g.Expect(err).NotTo(HaveOccurred())
	required := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(required.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverRequired))
	g.Expect(required.Status.HA.Primary).To(Equal(oldPrimary.Name))
	g.Expect(required.Status.HA.PrimaryUID).To(Equal(string(oldPrimary.UID)))
	g.Expect(execCalls).To(Equal(0), "the reconcile that first persists FailoverRequired must not execute HA SQL")
	g.Expect(promotionCalls).To(Equal(0))

	_, _, err = reconciler.reconcileMasterSlave(ctx, *required)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(execCalls).To(Equal(0), "failover entry must persist the Phase 5-A fence plan before any SQL")
	g.Expect(promotionCalls).To(Equal(0))
	inProgress := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(inProgress.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverInProgress))
	g.Expect(inProgress.Status.HA.Primary).To(Equal(oldPrimary.Name))
	g.Expect(inProgress.Status.HA.PrimaryUID).To(Equal(string(oldPrimary.UID)))
	g.Expect(inProgress.Status.HA.Failover).To(Equal(phase5FencingHA(oldPrimary, databasev1.MysqlClusterFenceStatePending).Failover))
}

func TestPhase4HAVerifyingSemanticWait(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase4-verifying-wait", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
	replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
	cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateVerifying, primary)
	reconciler := phase1HReconciler(t, cluster, statefulSet, primary, replica, phase1HEndpoints(cluster, primary))

	semanticConverged := false
	commands := make([]string, 0, 2)
	reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		commands = append(commands, pod.Name+":"+command)
		g.Expect(command).To(Equal(mysqlShowSlaveStatusCommand()))
		ioRunning := "Connecting"
		if semanticConverged {
			ioRunning = "Yes"
		}
		return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", ioRunning, "Yes", "", ""), nil
	}

	result, converged, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(converged).To(BeFalse())
	g.Expect(result.RequeueAfter).To(Equal(mysqlReplicationRuntimeRequeueAfter))
	g.Expect(commands).To(HaveLen(1), "Phase 3 semantic observation must run while Verifying")
	waiting := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(waiting.Status.HA).To(Equal(phase4HAStatus(databasev1.MysqlClusterHAStateVerifying, primary)))

	semanticConverged = true
	result, converged, err = reconciler.reconcileMasterSlave(ctx, *waiting)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(converged).To(BeFalse(), "Healthy publication returns for a later observation cycle")
	g.Expect(result.RequeueAfter).To(Equal(mysqlHAFailureRequeueAfter))
	g.Expect(commands).To(HaveLen(2))
	g.Expect(phase4StoredCluster(t, reconciler, cluster).Status.HA).To(Equal(phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, primary)))
	for _, command := range commands {
		g.Expect(command).NotTo(ContainSubstring(mysqlPreparePrimaryCommand()))
		g.Expect(command).NotTo(ContainSubstring(mysqlConfigureReplicaCommand(cluster.Spec.MasterService)))
	}
}

func TestPhase4HAFailoverInProgressIdentityAdoption(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase4-in-progress-adoption", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	oldPrimary := phase1HPod(t, cluster, statefulSet, 1, "slave", false)
	newPrimary := phase1HPod(t, cluster, statefulSet, 3, "master", true)
	replica := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
	cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateFailoverInProgress, oldPrimary)
	reconciler := phase1HReconciler(
		t,
		cluster,
		statefulSet,
		oldPrimary,
		replica,
		newPrimary,
		phase1HEndpoints(cluster, newPrimary),
	)

	semanticConverged := false
	commands := make([]string, 0, 4)
	reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
		commands = append(commands, pod.Name+":"+command)
		g.Expect(command).To(Equal(mysqlShowSlaveStatusCommand()))
		ioRunning := "Yes"
		if pod.Name == replica.Name && !semanticConverged {
			ioRunning = "Connecting"
		}
		return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", ioRunning, "Yes", "", ""), nil
	}

	result, converged, err := reconciler.reconcileMasterSlave(ctx, *cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(converged).To(BeFalse())
	g.Expect(result.RequeueAfter).To(Equal(mysqlHAFailureRequeueAfter))
	g.Expect(commands).To(BeEmpty(), "identity adoption must execute no election, promotion, reconfiguration, or semantic SQL")
	adopted := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(adopted.Status.HA).To(Equal(phase4HAStatus(databasev1.MysqlClusterHAStateVerifying, newPrimary)))
	g.Expect(adopted.Status.HA.State).NotTo(Equal(databasev1.MysqlClusterHAStateHealthy))

	result, converged, err = reconciler.reconcileMasterSlave(ctx, *adopted)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(converged).To(BeFalse())
	g.Expect(result.RequeueAfter).To(Equal(mysqlReplicationRuntimeRequeueAfter))
	g.Expect(commands).To(HaveLen(2), "the reconcile after adoption must use Phase 3 semantic observation")
	waiting := phase4StoredCluster(t, reconciler, cluster)
	g.Expect(waiting.Status.HA).To(Equal(phase4HAStatus(databasev1.MysqlClusterHAStateVerifying, newPrimary)))

	semanticConverged = true
	result, converged, err = reconciler.reconcileMasterSlave(ctx, *waiting)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(converged).To(BeFalse())
	g.Expect(result.RequeueAfter).To(Equal(mysqlHAFailureRequeueAfter))
	g.Expect(commands).To(HaveLen(4))
	g.Expect(phase4StoredCluster(t, reconciler, cluster).Status.HA).To(Equal(phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, newPrimary)))
}

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	databasev1 "github.com/egonlin/api/v1"
)

func phase1HCluster(name string, initialized bool) *databasev1.MysqlCluster {
	cluster := statefulSetResourceTestCluster(name, types.UID(name+"-uid"))
	cluster.Spec.MasterService = name + "-primary"
	cluster.Spec.SlaveService = name + "-replica"
	cluster.Status.CredentialsSecretUID = "phase1h-credential-secret-uid"
	if initialized {
		cluster.Annotations = map[string]string{"initialized": "true"}
		cluster.Status.LastConvergedReplicas = replicaCountCopy(desiredReplicas(cluster))
	}
	return cluster
}

func phase1HStatefulSet(t *testing.T, cluster *databasev1.MysqlCluster) *appsv1.StatefulSet {
	t.Helper()
	statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID(cluster.Name+"-statefulset-uid"))
	return statefulSet
}

func phase1HPod(
	t *testing.T,
	cluster *databasev1.MysqlCluster,
	statefulSet *appsv1.StatefulSet,
	ordinal int32,
	role string,
	ready bool,
) *corev1.Pod {
	t.Helper()
	pod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, ordinal)
	pod.UID = types.UID(fmt.Sprintf("%s-pod-%d-uid", cluster.Name, ordinal))
	pod.Labels[LabelMysqlRole] = role
	pod.Labels[LegacyLabelRole] = role
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == mysqlContainerName {
			pod.Status.ContainerStatuses[i].Ready = ready
		}
	}
	return pod
}

func phase1HCredentialSecret(cluster *databasev1.MysqlCluster) *corev1.Secret {
	secret := immutableCredentialSecret(cluster, []byte("phase1h-root"), []byte("phase1h-replication"))
	secret.UID = types.UID(cluster.Status.CredentialsSecretUID)
	return secret
}

func phase1HEndpoints(cluster *databasev1.MysqlCluster, primaryPod *corev1.Pod) *corev1.Endpoints {
	endpoints := &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.MasterService, Namespace: cluster.Namespace}}
	if primaryPod != nil {
		endpoints.Subsets = []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{
				TargetRef: &corev1.ObjectReference{Name: primaryPod.Name, Namespace: primaryPod.Namespace},
			}},
		}}
	}
	return endpoints
}

func phase1HReconciler(t *testing.T, objects ...client.Object) *MysqlClusterReconciler {
	t.Helper()
	return newStatefulSetReconcileTestReconciler(t, newStatefulSetReconcileTestScheme(t), objects...)
}

func TestPhase1HRuntimeReadinessGates(t *testing.T) {
	ctx := context.Background()

	t.Run("initialization still waits for every desired member to be Ready", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase1h-initialization", false)
		statefulSet := phase1HStatefulSet(t, cluster)
		pod1 := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		pod2 := phase1HPod(t, cluster, statefulSet, 2, "slave", false)
		pod3 := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		reconciler := phase1HReconciler(t, statefulSet, phase1HCredentialSecret(cluster), pod1, pod2, pod3)
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, complete, err := reconciler.reconcileStatefulSetInitialization(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(complete).To(BeFalse())
		g.Expect(execCalls).To(Equal(0))
		g.Expect(cluster.Annotations).NotTo(HaveKey("initialized"))
	})

	for _, primaryState := range []string{"not-ready", "missing"} {
		t.Run("initialized runtime enters durable fencing when primary is "+primaryState, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("phase1h-runtime-"+primaryState, true)
			statefulSet := phase1HStatefulSet(t, cluster)
			replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", true)
			replica3 := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
			oldPrimary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
			objects := []client.Object{cluster, statefulSet, phase1HCredentialSecret(cluster), replica2, replica3, phase1HEndpoints(cluster, nil)}
			if primaryState == "not-ready" {
				objects = append(objects, oldPrimary)
			} else {
				cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, oldPrimary)
			}
			reconciler := phase1HReconciler(t, objects...)
			reconciler.MasterGTIDSnapshot = "uuid:1-10"
			promotedPod := ""
			reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
				if oldPrimary != nil && pod.Name == oldPrimary.Name {
					return "", errors.New("old primary SQL must not be required for metadata demotion")
				}
				switch {
				case command == mysqlShowSlaveGTIDCommand():
					return "uuid:1-10\n", nil
				case strings.HasPrefix(command, "du -sb "):
					if pod.Name == replica3.Name {
						return "200\n", nil
					}
					return "100\n", nil
				case command == mysqlPreparePrimaryCommand():
					promotedPod = pod.Name
					return "", nil
				case command == mysqlConfigureReplicaCommand(cluster.Spec.MasterService):
					return "", nil
				default:
					return "", fmt.Errorf("unexpected command for Pod %s: %s", pod.Name, command)
				}
			}

			confirmationReconciles := 2
			if primaryState == "not-ready" {
				confirmationReconciles = 3
			}
			var (
				complete bool
				err      error
			)
			for reconcileIndex := 0; reconcileIndex < confirmationReconciles; reconcileIndex++ {
				current := phase4StoredCluster(t, reconciler, cluster)
				_, complete, err = reconciler.reconcileStatefulSetRuntime(ctx, current)
				g.Expect(err).NotTo(HaveOccurred())
				if reconcileIndex < confirmationReconciles-1 {
					g.Expect(promotedPod).To(BeEmpty())
				}
			}
			g.Expect(complete).To(BeFalse())
			g.Expect(promotedPod).To(BeEmpty())
			storedCluster := phase4StoredCluster(t, reconciler, cluster)
			g.Expect(storedCluster.Status.HA.State).To(Equal(databasev1.MysqlClusterHAStateFailoverInProgress))
			g.Expect(storedCluster.Status.HA.Failover).To(Equal(phase5FencingHA(oldPrimary, databasev1.MysqlClusterFenceStatePending).Failover))
			storedReplica3 := &corev1.Pod{}
			g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(replica3), storedReplica3)).To(Succeed())
			g.Expect(storedReplica3.Labels).To(HaveKeyWithValue(LabelMysqlRole, "slave"))
			if primaryState == "not-ready" {
				storedOldPrimary := &corev1.Pod{}
				g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(oldPrimary), storedOldPrimary)).To(Succeed())
				g.Expect(storedOldPrimary.Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
				g.Expect(storedOldPrimary.Labels).To(HaveKeyWithValue(LegacyLabelRole, "master"))
			}
		})
	}

	t.Run("a transient NotReady replica cannot replace a healthy primary", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase1h-replica-transient", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		replica2 := phase1HPod(t, cluster, statefulSet, 2, "slave", false)
		replica3 := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, primary)
		reconciler := phase1HReconciler(
			t,
			cluster,
			statefulSet,
			phase1HCredentialSecret(cluster),
			primary,
			replica2,
			replica3,
			phase1HEndpoints(cluster, primary),
		)
		promotionCommands := 0
		reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
			if command == mysqlPreparePrimaryCommand() {
				promotionCommands++
			}
			if command == mysqlWriteSafetyObservationCommand() {
				return "1\t1\tON\tON\n", nil
			}
			if command == mysqlShowSlaveStatusCommand() {
				if pod.Name == replica2.Name {
					return "", errors.New("local MySQL socket is not ready")
				}
				return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
			}
			return "", fmt.Errorf("unexpected command for Pod %s: %s", pod.Name, command)
		}

		_, complete, err := reconciler.reconcileStatefulSetRuntime(ctx, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("local MySQL socket is not ready")))
		g.Expect(complete).To(BeFalse())
		g.Expect(promotionCommands).To(Equal(0))
		for _, pod := range []*corev1.Pod{primary, replica2, replica3} {
			stored := &corev1.Pod{}
			g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
			g.Expect(stored.Labels[LabelMysqlRole]).To(Equal(pod.Labels[LabelMysqlRole]))
		}
	})
}

func TestPhase1HOldPrimaryDemotionRequiresSuccessfulElection(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase1h-demotion-election-failure", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	oldPrimary := phase1HPod(t, cluster, statefulSet, 1, "master", false)
	notReadyCandidate := phase1HPod(t, cluster, statefulSet, 2, "slave", false)
	reconciler := phase1HReconciler(t, statefulSet, oldPrimary, notReadyCandidate)
	execCalls := 0
	reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
		execCalls++
		return "", nil
	}

	err := reconciler.handleMasterFailure(ctx, *cluster)
	g.Expect(err).To(MatchError(ContainSubstring("no suitable slave found")))
	g.Expect(execCalls).To(Equal(0))
	storedOldPrimary := &corev1.Pod{}
	g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(oldPrimary), storedOldPrimary)).To(Succeed())
	g.Expect(storedOldPrimary.Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
	g.Expect(storedOldPrimary.Labels).To(HaveKeyWithValue(LegacyLabelRole, "master"))
}

func TestPhase1HElectionCandidateSafety(t *testing.T) {
	ctx := context.Background()

	t.Run("skips a NotReady MySQL replica candidate", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase1h-candidate-ready", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		notReady := phase1HPod(t, cluster, statefulSet, 2, "slave", false)
		ready := phase1HPod(t, cluster, statefulSet, 3, "slave", true)
		reconciler := phase1HReconciler(t, statefulSet, notReady, ready)
		reconciler.execCommandOnPodFn = func(pod *corev1.Pod, command string) (string, error) {
			if pod.Name == notReady.Name {
				return "", errors.New("NotReady candidate must not be queried")
			}
			switch {
			case command == mysqlShowSlaveGTIDCommand():
				return "uuid:1-10", nil
			case strings.HasPrefix(command, "du -sb "):
				return "100", nil
			default:
				return "", fmt.Errorf("unexpected command: %s", command)
			}
		}

		candidate, _, err := reconciler.electNewMaster(ctx, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(candidate).To(Equal(ready.Name))
	})

	t.Run("rejects a label-spoofed non-StatefulSet candidate", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase1h-candidate-owner", true)
		spoofed := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "spoofed-replica",
				Namespace: cluster.Namespace,
				Labels:    mysqlRoleLabels(cluster, "slave"),
			},
			Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{{Name: mysqlContainerName, Ready: true}},
			},
		}
		reconciler := phase1HReconciler(t, spoofed)
		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "", nil
		}

		_, _, err := reconciler.electNewMaster(ctx, *cluster)
		g.Expect(err).To(MatchError(ContainSubstring("invalid election candidate")))
		g.Expect(execCalls).To(Equal(0))
	})
}

func TestPhase1HOwnershipBeforeSQL(t *testing.T) {
	ctx := context.Background()

	t.Run("rejects a same-UID spoofed replica before SHOW SLAVE STATUS", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase1h-owner-replica", true)
		statefulSet := phase1HStatefulSet(t, cluster)
		primary := phase1HPod(t, cluster, statefulSet, 1, "master", true)
		spoofedReplica := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "same-uid-spoofed-replica",
				Namespace: cluster.Namespace,
				Labels:    mysqlRoleLabels(cluster, "slave"),
			},
		}
		reconciler := phase1HReconciler(
			t,
			statefulSet,
			primary,
			spoofedReplica,
			phase1HEndpoints(cluster, primary),
		)

		_, discoveredNames := reconciler.getActualReplicaInfo(ctx, *cluster)
		g.Expect(discoveredNames).To(ContainElement(spoofedReplica.Name))

		execCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			execCalls++
			return "Slave_SQL_Running: Yes\nSlave_IO_Running: Yes\n", nil
		}

		_, _, err := reconciler.checkReplicaStatus(ctx, *cluster)
		g.Expect(err).To(MatchError(ContainSubstring("replica status SQL")))
		g.Expect(err).To(MatchError(ContainSubstring("has no controller owner")))
		g.Expect(execCalls).To(Equal(0))
	})

	t.Run("rejects a foreign primary Endpoint target before preparation SQL", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase1h-owner-primary", true)
		foreignPrimary := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "foreign-endpoint-primary",
				Namespace: cluster.Namespace,
				Labels:    mysqlRoleLabels(cluster, "master"),
			},
		}
		reconciler := phase1HReconciler(t, foreignPrimary, phase1HEndpoints(cluster, foreignPrimary))
		primarySQLExecCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			primarySQLExecCalls++
			return "", nil
		}

		_, _, err := reconciler.reconcileMasterSlave(ctx, *cluster)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError(ContainSubstring("has no controller owner")))
		g.Expect(primarySQLExecCalls).To(Equal(0))
	})

	t.Run("rejects a spoofed primary before GTID SQL and snapshot mutation", func(t *testing.T) {
		g := NewWithT(t)
		cluster := phase1HCluster("phase1h-owner-gtid", true)
		spoofedPrimary := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "spoofed-gtid-primary",
				Namespace: cluster.Namespace,
				Labels:    mysqlRoleLabels(cluster, "master"),
			},
		}
		reconciler := phase1HReconciler(t, spoofedPrimary)
		reconciler.MasterGTIDSnapshot = "known-good"
		gtidExecCalls := 0
		reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			gtidExecCalls++
			return "spoofed:1-100", nil
		}

		err := reconciler.updateMasterGTIDSnapshotFromPod(ctx, spoofedPrimary, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("primary GTID observation SQL")))
		g.Expect(err).To(MatchError(ContainSubstring("has no controller owner")))
		g.Expect(gtidExecCalls).To(Equal(0))
		g.Expect(reconciler.MasterGTIDSnapshot).To(Equal("known-good"))
	})
}

func TestPhase1HGTIDFailurePropagationAndSnapshotPreservation(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("phase1h-gtid-preservation", true)
	statefulSet := phase1HStatefulSet(t, cluster)
	pod := phase1HPod(t, cluster, statefulSet, 1, "master", true)
	reconciler := phase1HReconciler(t, statefulSet, pod)
	reconciler.MasterGTIDSnapshot = "known-good"
	reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
		return "", errors.New("mysql authentication failed")
	}

	g.Expect(mysqlShowMasterGTIDCommand()).To(ContainSubstring("SELECT @@GLOBAL.gtid_executed"))
	g.Expect(mysqlShowSlaveGTIDCommand()).To(ContainSubstring("SELECT @@GLOBAL.gtid_executed"))
	g.Expect(mysqlShowMasterGTIDCommand()).NotTo(ContainSubstring("|"))
	g.Expect(mysqlShowSlaveGTIDCommand()).NotTo(ContainSubstring("|"))
	_, err := reconciler.getMasterGTIDSet(pod)
	g.Expect(err).To(MatchError(ContainSubstring("mysql authentication failed")))
	_, err = reconciler.getSlaveGTIDSet(pod)
	g.Expect(err).To(MatchError(ContainSubstring("mysql authentication failed")))
	g.Expect(reconciler.updateMasterGTIDSnapshotFromPod(ctx, pod, cluster)).To(MatchError(ContainSubstring("mysql authentication failed")))
	g.Expect(reconciler.MasterGTIDSnapshot).To(Equal("known-good"))
}

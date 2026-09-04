package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func expectMysqlObservability(t *testing.T, cluster *databasev1.MysqlCluster, phase databasev1.MysqlClusterPhase, primary string, current, ready int32, available, progressing, degraded bool) {
	t.Helper()
	g := NewWithT(t)
	g.Expect(cluster.Status.Phase).To(Equal(phase))
	g.Expect(cluster.Status.Primary).To(Equal(primary))
	g.Expect(cluster.Status.CurrentReplicas).To(Equal(current))
	g.Expect(cluster.Status.ReadyReplicas).To(Equal(ready))
	g.Expect(cluster.Status.ObservedGeneration).To(Equal(cluster.Generation))
	g.Expect(cluster.Status.Conditions).To(HaveLen(3))
	for name, value := range map[string]bool{mysqlConditionAvailable: available, mysqlConditionProgressing: progressing, mysqlConditionDegraded: degraded} {
		condition := meta.FindStatusCondition(cluster.Status.Conditions, name)
		g.Expect(condition).NotTo(BeNil())
		want := metav1.ConditionFalse
		if value {
			want = metav1.ConditionTrue
		}
		g.Expect(condition.Status).To(Equal(want), name)
		g.Expect(condition.ObservedGeneration).To(Equal(cluster.Generation))
		g.Expect(condition.Reason).To(MatchRegexp(`^[A-Z][a-zA-Z0-9]+$`))
		g.Expect(condition.LastTransitionTime.IsZero()).To(BeFalse())
	}
}

func TestMysqlObservabilitySemanticMatrix(t *testing.T) {
	tests := []struct {
		name                             string
		initialized                      bool
		ha                               databasev1.MysqlClusterHAState
		transition                       bool
		primaryRole                      string
		unready                          int
		phase                            databasev1.MysqlClusterPhase
		available, progressing, degraded bool
	}{
		{name: "initializing", primaryRole: "", unready: -1, phase: databasev1.MysqlClusterPhaseInitializing, progressing: true},
		{name: "stable", initialized: true, ha: databasev1.MysqlClusterHAStateHealthy, primaryRole: "master", unready: -1, phase: databasev1.MysqlClusterPhaseRunning, available: true},
		{name: "replica transition", initialized: true, ha: databasev1.MysqlClusterHAStateHealthy, transition: true, primaryRole: "master", unready: -1, phase: databasev1.MysqlClusterPhaseRunning, available: true, progressing: true},
		{name: "healthy primary degraded replica", initialized: true, ha: databasev1.MysqlClusterHAStateHealthy, primaryRole: "master", unready: 2, phase: databasev1.MysqlClusterPhaseDegraded, available: true, degraded: true},
		{name: "suspected", initialized: true, ha: databasev1.MysqlClusterHAStateSuspected, primaryRole: "master", unready: 0, phase: databasev1.MysqlClusterPhaseDegraded, progressing: true, degraded: true},
		{name: "failover required", initialized: true, ha: databasev1.MysqlClusterHAStateFailoverRequired, primaryRole: "", unready: -1, phase: databasev1.MysqlClusterPhaseDegraded, progressing: true, degraded: true},
		{name: "failover in progress", initialized: true, ha: databasev1.MysqlClusterHAStateFailoverInProgress, primaryRole: "", unready: -1, phase: databasev1.MysqlClusterPhaseDegraded, progressing: true, degraded: true},
		{name: "verifying published primary", initialized: true, ha: databasev1.MysqlClusterHAStateVerifying, primaryRole: "master", unready: -1, phase: databasev1.MysqlClusterPhaseDegraded, available: true, progressing: true, degraded: true},
		{name: "HA degraded", initialized: true, ha: databasev1.MysqlClusterHAStateDegraded, primaryRole: "master", unready: -1, phase: databasev1.MysqlClusterPhaseDegraded, available: true, degraded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("observability", tt.initialized)
			cluster.Generation = 7
			sts := phase1HStatefulSet(t, cluster)
			pods := []*corev1.Pod{phase1HPod(t, cluster, sts, 1, tt.primaryRole, tt.unready != 0), phase1HPod(t, cluster, sts, 2, "slave", true), phase1HPod(t, cluster, sts, 3, "slave", tt.unready != 2)}
			if tt.primaryRole == "" {
				delete(pods[0].Labels, LabelMysqlRole)
				delete(pods[0].Labels, LegacyLabelRole)
			}
			if tt.ha != "" {
				cluster.Status.HA = &databasev1.MysqlClusterHAStatus{State: tt.ha, Primary: pods[0].Name, PrimaryUID: string(pods[0].UID)}
			}
			if tt.transition {
				cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3}
			}
			r := phase1HReconciler(t, cluster, sts, pods[0], pods[1], pods[2], phase1HEndpoints(cluster, pods[0]))
			changed, err := r.reconcileMysqlObservability(context.Background(), cluster, tt.initialized)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(changed).To(BeTrue())
			stored := phase4StoredCluster(t, r, cluster)
			primary := ""
			if tt.available {
				primary = pods[0].Name
			}
			ready := int32(3)
			if tt.unready >= 0 {
				ready--
			}
			expectMysqlObservability(t, stored, tt.phase, primary, 3, ready, tt.available, tt.progressing, tt.degraded)
			changed, err = r.reconcileMysqlObservability(context.Background(), stored, tt.initialized)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(changed).To(BeFalse())
			g.Expect(r.Client.(*statefulSetReconcileMemoryClient).statusPatchCount).To(Equal(1))
		})
	}
}

func TestMysqlObservabilityPublicationAndIdentity(t *testing.T) {
	for _, name := range []string{"candidate only", "fenced old primary still labeled", "published candidate", "replaced candidate", "two primaries", "conflicting roles", "legacy only", "deleting primary", "replaced tracked primary", "foreign instance", "stale owner", "wrong ordinal", "same name foreign owner"} {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("publication", true)
			sts := phase1HStatefulSet(t, cluster)
			old := phase1HPod(t, cluster, sts, 1, "master", true)
			candidate := phase1HPod(t, cluster, sts, 2, "slave", true)
			cluster.Status.HA = phase5FencingHA(old, databasev1.MysqlClusterFenceStateVerified)
			fo := cluster.Status.HA.Failover
			fo.Stage = databasev1.MysqlClusterFailoverStagePromoting
			fo.Candidate, fo.CandidateUID = candidate.Name, string(candidate.UID)
			delete(old.Labels, LabelMysqlRole)
			delete(old.Labels, LegacyLabelRole)
			wantPrimary := ""
			wantCount := int32(2)
			wantError := false
			switch name {
			case "fenced old primary still labeled":
				old.Labels[LabelMysqlRole], old.Labels[LegacyLabelRole] = "master", "master"
			case "published candidate":
				candidate.Labels[LabelMysqlRole], candidate.Labels[LegacyLabelRole] = "master", "master"
				wantPrimary = candidate.Name
			case "replaced candidate":
				candidate.Labels[LabelMysqlRole], candidate.Labels[LegacyLabelRole] = "master", "master"
				candidate.UID = "replacement"
			case "two primaries":
				old.Labels[LabelMysqlRole], old.Labels[LegacyLabelRole] = "master", "master"
				candidate.Labels[LabelMysqlRole], candidate.Labels[LegacyLabelRole] = "master", "master"
			case "conflicting roles":
				candidate.Labels[LabelMysqlRole] = "master"
			case "legacy only":
				delete(candidate.Labels, LabelMysqlRole)
				candidate.Labels[LegacyLabelRole] = "master"
			case "deleting primary":
				candidate.Labels[LabelMysqlRole], candidate.Labels[LegacyLabelRole] = "master", "master"
				now := metav1.Now()
				candidate.DeletionTimestamp = &now
			case "replaced tracked primary":
				cluster.Status.HA.Failover = nil
				old.Labels[LabelMysqlRole], old.Labels[LegacyLabelRole] = "master", "master"
				old.UID = "replacement"
			case "foreign instance":
				candidate.Labels[LabelAppInstance] = "foreign"
				wantCount = 1
			case "stale owner", "same name foreign owner":
				candidate.OwnerReferences[0].UID = types.UID("foreign-sts")
				wantError = true
			case "wrong ordinal":
				candidate.Labels[statefulSetPodIndexLabel] = "4"
				wantError = true
			}
			r := phase1HReconciler(t, cluster, sts, old, candidate, phase1HEndpoints(cluster, candidate))
			_, err := r.reconcileMysqlObservability(context.Background(), cluster, true)
			if wantError {
				g.Expect(err).To(HaveOccurred())
				g.Expect(r.Client.(*statefulSetReconcileMemoryClient).statusPatchCount).To(BeZero())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			stored := phase4StoredCluster(t, r, cluster)
			g.Expect(stored.Status.Primary).To(Equal(wantPrimary))
			g.Expect(meta.IsStatusConditionTrue(stored.Status.Conditions, mysqlConditionAvailable)).To(Equal(wantPrimary != ""))
			g.Expect(stored.Status.CurrentReplicas).To(Equal(wantCount))
		})
	}
}

func TestMysqlObservabilityPreservationAndTransitions(t *testing.T) {
	g := NewWithT(t)
	cluster := phase1HCluster("preserve-projection", true)
	cluster.Generation = 9
	sts := phase1HStatefulSet(t, cluster)
	pod := phase1HPod(t, cluster, sts, 1, "master", true)
	cluster.Status.Master, cluster.Status.Slaves = "compatibility-primary", []string{"compatibility-replica"}
	cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3}
	cluster.Status.HA = phase5FencingHA(pod, databasev1.MysqlClusterFenceStateVerified)
	gtid := "sensitive-gtid-proof"
	cluster.Status.HA.Failover.FailedPrimaryGTIDSet = &gtid
	before := cluster.DeepCopy()
	r := phase1HReconciler(t, cluster, sts, pod)
	_, err := r.reconcileMysqlObservability(context.Background(), cluster, true)
	g.Expect(err).NotTo(HaveOccurred())
	stored := phase4StoredCluster(t, r, cluster)
	g.Expect(stored.Status.CredentialsSecretUID).To(Equal(before.Status.CredentialsSecretUID))
	g.Expect(stored.Status.LastConvergedReplicas).To(Equal(before.Status.LastConvergedReplicas))
	g.Expect(stored.Status.ReplicaTransition).To(Equal(before.Status.ReplicaTransition))
	g.Expect(stored.Status.HA).To(Equal(before.Status.HA))
	g.Expect(stored.Status.Master).To(Equal(before.Status.Master))
	g.Expect(stored.Status.Slaves).To(Equal(before.Status.Slaves))
	for _, condition := range stored.Status.Conditions {
		g.Expect(condition.Message).NotTo(ContainSubstring(gtid))
	}
	// Use explicit times: neither generation, reason nor message changes are
	// condition-status transitions; recovery is a genuine transition.
	members := []mysqlStatefulSetMember{{Ordinal: 1, Pod: pod}}
	t0 := metav1.NewTime(time.Unix(100, 0))
	cluster.Status.Conditions = nil
	projectMysqlObservability(cluster, true, members, phase1HEndpoints(cluster, pod), t0)
	cluster.Generation++
	cluster.Status.HA.State = databasev1.MysqlClusterHAStateVerifying
	cluster.Status.HA.Failover = nil
	projectMysqlObservability(cluster, true, members, phase1HEndpoints(cluster, pod), metav1.NewTime(time.Unix(200, 0)))
	g.Expect(meta.FindStatusCondition(cluster.Status.Conditions, mysqlConditionDegraded).LastTransitionTime).To(Equal(t0))
	g.Expect(meta.FindStatusCondition(cluster.Status.Conditions, mysqlConditionDegraded).ObservedGeneration).To(Equal(cluster.Generation))
	cluster.Spec.Replicas = replicaCountCopy(1)
	cluster.Status.LastConvergedReplicas = replicaCountCopy(1)
	cluster.Status.ReplicaTransition = nil
	cluster.Status.HA.State = databasev1.MysqlClusterHAStateHealthy
	t2 := metav1.NewTime(time.Unix(300, 0))
	projectMysqlObservability(cluster, true, members, phase1HEndpoints(cluster, pod), t2)
	g.Expect(meta.FindStatusCondition(cluster.Status.Conditions, mysqlConditionDegraded).LastTransitionTime).To(Equal(t2))
	g.Expect(meta.IsStatusConditionFalse(cluster.Status.Conditions, mysqlConditionDegraded)).To(BeTrue())
}

func TestMysqlObservabilityReconcileBarriersAndErrors(t *testing.T) {
	ctx := context.Background()
	for _, patchErr := range []error{nil, apierrors.NewConflict(schema.GroupResource{Resource: "mysqlclusters"}, "barrier", errors.New("concurrent update")), errors.New("API unavailable")} {
		t.Run("patch "+formatObservabilityError(patchErr), func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("barrier", true)
			sts := phase1HStatefulSet(t, cluster)
			pod := phase1HPod(t, cluster, sts, 1, "master", false)
			cluster.Status.HA = phase5FencingHA(pod, databasev1.MysqlClusterFenceStatePending)
			r := phase1HReconciler(t, cluster, sts, pod, phase1HCredentialSecret(cluster))
			memory := r.Client.(*statefulSetReconcileMemoryClient)
			memory.statusPatchError = patchErr
			sql := 0
			r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { sql++; return "1\t1\tON\tON\n", nil }
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
			result, err := r.Reconcile(ctx, req)
			g.Expect(sql).To(BeZero())
			g.Expect(memory.updateCount).To(BeZero())
			stored := phase4StoredCluster(t, r, cluster)
			g.Expect(stored.Status.HA).To(Equal(cluster.Status.HA))
			g.Expect(stored.Status.Master).To(BeEmpty())
			g.Expect(stored.Status.Slaves).To(BeEmpty())
			if patchErr != nil {
				g.Expect(errors.Is(err, patchErr)).To(BeTrue())
				g.Expect(stored.Status).To(Equal(cluster.Status))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result).To(Equal(ctrl.Result{Requeue: true}))
			g.Expect(memory.statusPatchCount).To(Equal(1))
			// With the same observation, control enters fencing unchanged and
			// persists exactly its existing fence-verification barrier.
			_, err = r.Reconcile(ctx, req)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(sql).To(Equal(1))
			g.Expect(memory.statusPatchCount).To(Equal(2))
			g.Expect(phase4StoredCluster(t, r, cluster).Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStateVerified))
			// The next Phase 5 barrier unpublishes the old primary. No trailing
			// observability status write is added to that role mutation.
			_, err = r.Reconcile(ctx, req)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(memory.statusPatchCount).To(Equal(2))
			g.Expect(phase5StoredPod(t, r, pod).Labels).NotTo(HaveKey(LabelMysqlRole))
			// A subsequent control error retains its existing error semantics.
			secret := phase1HCredentialSecret(cluster)
			delete(memory.objects, memory.objectKey(secret))
			beforeError := phase4StoredCluster(t, r, cluster).DeepCopy()
			_, err = r.Reconcile(ctx, req)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(ContainSubstring(cluster.Spec.CredentialsSecretName))
			g.Expect(phase4StoredCluster(t, r, cluster).Status).To(Equal(beforeError.Status))
		})
	}
}

func formatObservabilityError(err error) string {
	if err == nil {
		return "success"
	}
	return err.Error()
}

// Existing lifecycle regression tests assert one control iteration at a time.
// Permit exactly one preceding projection-only iteration, never retry an error
// or a control-path requeue. Continued projection churn is a test failure.
func reconcileAfterObservability(ctx context.Context, r *MysqlClusterReconciler, request ctrl.Request) (ctrl.Result, error) {
	result, err := r.Reconcile(ctx, request)
	if err != nil || result != (ctrl.Result{Requeue: true}) {
		return result, err
	}
	result, err = r.Reconcile(ctx, request)
	if err == nil && result == (ctrl.Result{Requeue: true}) {
		return result, errors.New("unchanged observations unexpectedly required a second projection-only reconcile")
	}
	return result, err
}

func TestMysqlObservabilityDoesNotDriveHAControl(t *testing.T) {
	for _, phase := range []databasev1.MysqlClusterPhase{databasev1.MysqlClusterPhaseRunning, databasev1.MysqlClusterPhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("no-control-dependency", true)
			sts := phase1HStatefulSet(t, cluster)
			pod := phase1HPod(t, cluster, sts, 1, "master", false)
			cluster.Status.HA = phase5FencingHA(pod, databasev1.MysqlClusterFenceStatePending)
			cluster.Status.Phase, cluster.Status.Primary = phase, "fabricated-primary"
			cluster.Status.Conditions = []metav1.Condition{{Type: mysqlConditionAvailable, Status: metav1.ConditionTrue}}
			r := phase1HReconciler(t, cluster, sts, pod)
			r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { return "1\t1\tON\tON\n", nil }
			_, _, err := r.reconcileMasterSlave(context.Background(), *cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase4StoredCluster(t, r, cluster).Status.HA.Failover.FenceState).To(Equal(databasev1.MysqlClusterFenceStateVerified))
		})
	}
}

func TestMysqlObservabilityRoutingAvailability(t *testing.T) {
	for _, name := range []string{"ready endpoint", "missing endpoints", "empty endpoints", "not ready address", "wrong target", "stale UID", "unpublished candidate", "published candidate missing endpoint", "published candidate old endpoint", "published candidate routed"} {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("routing-projection", true)
			cluster.Spec.Replicas = replicaCountCopy(2)
			cluster.Status.LastConvergedReplicas = replicaCountCopy(2)
			sts := phase1HStatefulSet(t, cluster)
			primary := phase1HPod(t, cluster, sts, 1, "master", true)
			candidate := phase1HPod(t, cluster, sts, 2, "slave", true)
			endpoints := phase1HEndpoints(cluster, primary)
			wantPrimary := primary.Name
			available := name == "ready endpoint" || name == "published candidate routed"
			takeover := name == "unpublished candidate" || name == "published candidate missing endpoint" || name == "published candidate old endpoint" || name == "published candidate routed"
			if takeover {
				cluster.Status.HA = phase5FencingHA(primary, databasev1.MysqlClusterFenceStateVerified)
				fo := cluster.Status.HA.Failover
				fo.Stage = databasev1.MysqlClusterFailoverStagePromoting
				fo.Candidate, fo.CandidateUID = candidate.Name, string(candidate.UID)
				delete(primary.Labels, LabelMysqlRole)
				delete(primary.Labels, LegacyLabelRole)
				wantPrimary = candidate.Name
				if name == "unpublished candidate" {
					wantPrimary = ""
				} else {
					candidate.Labels[LabelMysqlRole], candidate.Labels[LegacyLabelRole] = "master", "master"
				}
			}
			switch name {
			case "missing endpoints", "published candidate missing endpoint":
				endpoints = nil
			case "empty endpoints":
				endpoints.Subsets = nil
			case "not ready address":
				endpoints.Subsets[0].NotReadyAddresses = endpoints.Subsets[0].Addresses
				endpoints.Subsets[0].Addresses = nil
			case "wrong target":
				endpoints.Subsets[0].Addresses[0].TargetRef.Name = "foreign-pod"
			case "stale UID":
				endpoints.Subsets[0].Addresses[0].TargetRef.UID = "replaced-pod"
			case "unpublished candidate", "published candidate routed":
				endpoints = phase1HEndpoints(cluster, candidate)
			}
			objects := []client.Object{cluster, sts, primary, candidate}
			if endpoints != nil {
				objects = append(objects, endpoints)
			}
			r := phase1HReconciler(t, objects...)
			r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { t.Fatal("projection must not execute SQL"); return "", nil }
			changed, err := r.reconcileMysqlObservability(context.Background(), cluster, true)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(changed).To(BeTrue())
			phase := databasev1.MysqlClusterPhaseRunning
			degraded := !available || takeover
			if degraded {
				phase = databasev1.MysqlClusterPhaseDegraded
			}
			expectMysqlObservability(t, phase4StoredCluster(t, r, cluster), phase, wantPrimary, 2, 2, available, takeover, degraded)
		})
	}
}

type mysqlObservabilityEndpointErrorClient struct {
	client.Client
	err error
}

func (c *mysqlObservabilityEndpointErrorClient) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*corev1.Endpoints); ok {
		return c.err
	}
	return c.Client.Get(ctx, key, object, options...)
}

func TestMysqlObservabilityRoutingReadError(t *testing.T) {
	g := NewWithT(t)
	cluster := phase1HCluster("routing-read-error", true)
	cluster.Generation = 12
	sts := phase1HStatefulSet(t, cluster)
	pod := phase1HPod(t, cluster, sts, 1, "master", true)
	r := phase1HReconciler(t, cluster, sts, pod)
	memory := r.Client.(*statefulSetReconcileMemoryClient)
	readErr := errors.New("endpoint API unavailable")
	r.Client = &mysqlObservabilityEndpointErrorClient{Client: r.Client, err: readErr}
	r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
		t.Fatal("routing read error must stop before control work")
		return "", nil
	}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	g.Expect(errors.Is(err, readErr)).To(BeTrue())
	g.Expect(phase4StoredCluster(t, r, cluster).Status).To(Equal(cluster.Status))
	g.Expect(memory.statusPatchCount).To(BeZero())
	g.Expect(memory.updateCount).To(BeZero())
}

func TestMysqlObservabilityReplicaTransitionStableCore(t *testing.T) {
	for _, tt := range []struct {
		name                          string
		from, target                  int32
		missing, unready, terminating int32
		degraded                      bool
	}{
		{name: "scale up new member unready", from: 2, target: 3, unready: 3},
		{name: "scale up new member missing", from: 2, target: 3, missing: 3},
		{name: "scale up new member terminating", from: 2, target: 3, terminating: 3},
		{name: "scale up core unready", from: 2, target: 3, unready: 2, degraded: true},
		{name: "scale up core missing with delta present", from: 2, target: 3, missing: 2, degraded: true},
		{name: "scale down removed member terminating and unready", from: 4, target: 3, unready: 4, terminating: 4},
		{name: "scale down removed member missing", from: 4, target: 3, missing: 4},
		{name: "scale down core missing with delta present", from: 4, target: 3, missing: 2, degraded: true},
		{name: "scale down core terminating", from: 4, target: 3, terminating: 2, degraded: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("transition-projection", true)
			cluster.Generation = 8
			cluster.Spec.Replicas = replicaCountCopy(tt.target)
			cluster.Status.LastConvergedReplicas = replicaCountCopy(tt.from)
			cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: tt.from, TargetReplicas: tt.target}
			sts := phase1HStatefulSet(t, cluster)
			objects := []client.Object{cluster, sts}
			var current, ready int32
			for ordinal := int32(1); ordinal <= max(tt.from, tt.target); ordinal++ {
				if ordinal == tt.missing {
					continue
				}
				role := "slave"
				if ordinal == 1 {
					role = "master"
				}
				// In scale-up cases the newly added member is also unready,
				// so a healthy delta cannot hide a failed stable-core member.
				isReady := ordinal != tt.unready && !(tt.target > tt.from && ordinal > tt.from)
				pod := phase1HPod(t, cluster, sts, ordinal, role, isReady)
				if ordinal == tt.terminating {
					now := metav1.Now()
					pod.DeletionTimestamp = &now
				}
				objects = append(objects, pod)
				current++
				if isReady {
					ready++
				}
				if ordinal == 1 {
					objects = append(objects, phase1HEndpoints(cluster, pod))
				}
			}
			r := phase1HReconciler(t, objects...)
			_, err := r.reconcileMysqlObservability(context.Background(), cluster, true)
			g.Expect(err).NotTo(HaveOccurred())
			stored := phase4StoredCluster(t, r, cluster)
			phase := databasev1.MysqlClusterPhaseRunning
			if tt.degraded {
				phase = databasev1.MysqlClusterPhaseDegraded
			}
			expectMysqlObservability(t, stored, phase, mysqlStatefulSetPodName(cluster, 1), current, ready, true, true, tt.degraded)
			g.Expect(stored.Status.ReplicaTransition).To(Equal(cluster.Status.ReplicaTransition))
			g.Expect(stored.Status.LastConvergedReplicas).To(Equal(cluster.Status.LastConvergedReplicas))
		})
	}
}

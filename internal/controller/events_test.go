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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
)

const eventSensitiveGTID = "SENSITIVE-GTID-MUST-NOT-APPEAR"

// Check storage at the instant Event is called, then retain FakeRecorder's
// normal output so both persistence order and public event text are tested.
type mysqlEventOrderRecorder struct {
	record.EventRecorder
	check func(runtime.Object)
}

func (r *mysqlEventOrderRecorder) Event(object runtime.Object, eventType, reason, message string) {
	r.check(object)
	r.EventRecorder.Event(object, eventType, reason, message)
}

func expectMysqlEvent(t *testing.T, recorder *record.FakeRecorder, eventType, reason string) string {
	t.Helper()
	g := NewWithT(t)
	var output string
	if reason != "" {
		g.Expect(recorder.Events).To(Receive(&output))
		g.Expect(output).To(HavePrefix(eventType + " " + reason + " "))
		g.Expect(reason).To(MatchRegexp(`^[A-Z][a-zA-Z0-9]+$`))
		for _, sensitive := range []string{eventSensitiveGTID, "SENSITIVE-SERVER-UUID", "SENSITIVE-SECRET", "SENSITIVE-SQL", "FailedPrimaryGTIDSet"} {
			g.Expect(output).NotTo(ContainSubstring(sensitive))
		}
	}
	// Every call must produce zero or one event, never overlapping reasons.
	g.Expect(recorder.Events).NotTo(Receive())
	return output
}

func TestMysqlHAEventMatrix(t *testing.T) {
	healthy := &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateHealthy, Primary: "mysql-1", PrimaryUID: "old-uid"}
	suspected := healthy.DeepCopy()
	suspected.State = databasev1.MysqlClusterHAStateSuspected
	required := healthy.DeepCopy()
	required.State = databasev1.MysqlClusterHAStateFailoverRequired
	pending := &databasev1.MysqlClusterHAStatus{
		State: databasev1.MysqlClusterHAStateFailoverInProgress, Primary: "mysql-1", PrimaryUID: "old-uid",
		Failover: &databasev1.MysqlClusterFailoverStatus{
			Stage: databasev1.MysqlClusterFailoverStageFencing, FenceState: databasev1.MysqlClusterFenceStatePending,
			FailedPrimary: "mysql-1", FailedPrimaryUID: "old-uid", FenceMethod: databasev1.MysqlClusterFenceMethodMySQLSuperReadOnly,
		},
	}
	verified := pending.DeepCopy()
	verified.Failover.FenceState, verified.Failover.FencedPrimaryUID = databasev1.MysqlClusterFenceStateVerified, "old-uid"
	blocked := pending.DeepCopy()
	blocked.State, blocked.Failover.FenceState = databasev1.MysqlClusterHAStateDegraded, databasev1.MysqlClusterFenceStateBlocked
	noCandidate := verified.DeepCopy()
	noCandidate.State = databasev1.MysqlClusterHAStateDegraded
	selected := verified.DeepCopy()
	selected.Failover.Stage = databasev1.MysqlClusterFailoverStageCandidateSelected
	selected.Failover.Candidate, selected.Failover.CandidateUID = "mysql-2", "candidate-uid"
	gtid := eventSensitiveGTID
	selected.Failover.FailedPrimaryGTIDSet = &gtid
	selected.Failover.FailedPrimaryServerUUID = "SENSITIVE-SERVER-UUID"
	promoting := selected.DeepCopy()
	promoting.Failover.Stage = databasev1.MysqlClusterFailoverStagePromoting
	reconfiguring := promoting.DeepCopy()
	reconfiguring.Failover.Stage = databasev1.MysqlClusterFailoverStageReconfiguring
	reconfiguring.Primary, reconfiguring.PrimaryUID = "mysql-2", "candidate-uid"
	unsafeRejoin := reconfiguring.DeepCopy()
	unsafeRejoin.State = databasev1.MysqlClusterHAStateDegraded
	verifying := reconfiguring.DeepCopy()
	verifying.State, verifying.Failover = databasev1.MysqlClusterHAStateVerifying, nil
	aborted := healthy.DeepCopy()
	aborted.State = databasev1.MysqlClusterHAStateVerifying
	badPromotionName := reconfiguring.DeepCopy()
	badPromotionName.Primary = "mysql-3"
	badPromotionUID := reconfiguring.DeepCopy()
	badPromotionUID.PrimaryUID = "replacement-uid"
	unclearedCandidate := selected.DeepCopy()
	unclearedCandidate.Failover.Stage = databasev1.MysqlClusterFailoverStageFencing
	newAttempt := pending.DeepCopy()
	newAttempt.Failover.FailedPrimaryUID = "replacement-uid"
	for _, tt := range []struct {
		name             string
		before, after    *databasev1.MysqlClusterHAStatus
		typeName, reason string
	}{
		{"required", suspected, required, corev1.EventTypeWarning, "FailoverRequired"},
		{"started", required, pending, corev1.EventTypeNormal, "FailoverStarted"},
		{"fenced", pending, verified, corev1.EventTypeNormal, "PrimaryFenced"},
		{"fence lost", verified, pending, corev1.EventTypeWarning, "PrimaryFenceLost"},
		{"blocked", pending, blocked, corev1.EventTypeWarning, "FailoverBlocked"},
		{"no candidate", verified, noCandidate, corev1.EventTypeWarning, "NoSafeCandidate"},
		{"selected", verified, selected, corev1.EventTypeNormal, "CandidateSelected"},
		{"invalidated", selected, verified, corev1.EventTypeWarning, "CandidateInvalidated"},
		{"promotion started", selected, promoting, corev1.EventTypeNormal, "PromotionStarted"},
		{"promoted", promoting, reconfiguring, corev1.EventTypeNormal, "PrimaryPromoted"},
		{"unsafe rejoin", reconfiguring, unsafeRejoin, corev1.EventTypeWarning, "UnsafeReplicaRejoin"},
		{"verifying", reconfiguring, verifying, corev1.EventTypeNormal, "FailoverVerifying"},
		{"aborted", pending, aborted, corev1.EventTypeNormal, "FailoverAborted"},
		{"recovered", verifying, healthy, corev1.EventTypeNormal, "HARecovered"},
		{"first healthy", nil, healthy, "", ""},
		{"suspected is silent", healthy, suspected, "", ""},
		{"cleared HA", healthy, nil, "", ""},
		{"promotion identity mismatch", promoting, badPromotionName, "", ""},
		{"promotion UID mismatch", promoting, badPromotionUID, "", ""},
		{"candidate proof not cleared", selected, unclearedCandidate, "", ""},
		{"same attempt retry", blocked, pending, "", ""},
		{"different attempt", pending, newAttempt, corev1.EventTypeNormal, "FailoverStarted"},
		{"fence loss overrides invalidation and start", selected, pending, corev1.EventTypeWarning, "PrimaryFenceLost"},
		{"block overrides invalidation", selected, blocked, corev1.EventTypeWarning, "FailoverBlocked"},
		{"no candidate overrides fence verified", pending, noCandidate, corev1.EventTypeWarning, "NoSafeCandidate"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("ha-events", true)
			cluster.Status.HA = mysqlCopyHAStatus(tt.before)
			cluster.Spec.CredentialsSecretName = "SENSITIVE-SECRET"
			cluster.Status.CredentialsSecretUID = "SENSITIVE-SECRET-UID"
			cluster.Annotations["test-secret"] = "SENSITIVE-SQL"
			r := phase1HReconciler(t, cluster)
			metricOwner, metricRegistry := newMysqlMetricsForTest(t)
			r.Metrics = metricOwner
			memory := r.Client.(*statefulSetReconcileMemoryClient)
			fake := record.NewFakeRecorder(10)
			r.Recorder = &mysqlEventOrderRecorder{EventRecorder: fake, check: func(object runtime.Object) {
				g.Expect(object).To(BeIdenticalTo(cluster))
				g.Expect(memory.statusPatchCount).To(Equal(1))
				g.Expect(phase4StoredCluster(t, r, cluster).Status.HA).To(Equal(tt.after))
				expectMysqlTransitionMetric(t, metricRegistry, cluster, "ha_transitions_total", tt.reason, 1)
			}}
			changed, err := r.persistMysqlClusterHAStatus(context.Background(), cluster, tt.after)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(changed).To(BeTrue())
			output := expectMysqlEvent(t, fake, tt.typeName, tt.reason)
			wantCount := float64(0)
			if tt.reason != "" {
				wantCount = 1
			}
			expectMysqlTransitionMetric(t, metricRegistry, cluster, "ha_transitions_total", tt.reason, wantCount)
			if tt.reason == "CandidateSelected" {
				g.Expect(output).To(Equal("Normal CandidateSelected Selected mysql-2 as the failover candidate."))
			}
			g.Expect(phase4StoredCluster(t, r, cluster).Status.HA).To(Equal(tt.after))
			changed, err = r.persistMysqlClusterHAStatus(context.Background(), cluster, mysqlCopyHAStatus(tt.after))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(changed).To(BeFalse())
			g.Expect(memory.statusPatchCount).To(Equal(1))
			expectMysqlEvent(t, fake, "", "")
			expectMysqlTransitionMetric(t, metricRegistry, cluster, "ha_transitions_total", tt.reason, wantCount)
		})
	}
	// Counter/proof churn within the same milestone is a successful status
	// patch, but not another high-signal transition.
	for _, before := range []*databasev1.MysqlClusterHAStatus{healthy, pending, blocked, noCandidate, unsafeRejoin, promoting, selected} {
		name := "silent metadata update " + string(before.State)
		if before.Failover != nil {
			name += " " + string(before.Failover.Stage) + " " + string(before.Failover.FenceState)
		}
		t.Run(name, func(t *testing.T) {
			cluster := phase1HCluster("same-milestone", true)
			cluster.Status.HA = before.DeepCopy()
			r := phase1HReconciler(t, cluster)
			metricOwner, metricRegistry := newMysqlMetricsForTest(t)
			r.Metrics = metricOwner
			fake := record.NewFakeRecorder(10)
			r.Recorder = fake
			after := before.DeepCopy()
			after.FailureCount++
			changed, err := r.persistMysqlClusterHAStatus(context.Background(), cluster, after)
			NewWithT(t).Expect(err).NotTo(HaveOccurred())
			NewWithT(t).Expect(changed).To(BeTrue())
			expectMysqlEvent(t, fake, "", "")
			expectMysqlTransitionMetric(t, metricRegistry, cluster, "ha_transitions_total", "", 0)
		})
	}
}

func TestMysqlReplicaTransitionEvents(t *testing.T) {
	transition := func(from, target int32) *databasev1.MysqlClusterReplicaTransitionStatus {
		return &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: from, TargetReplicas: target}
	}
	for _, tt := range []struct {
		name            string
		before          *databasev1.MysqlClusterReplicaTransitionStatus
		after           *databasev1.MysqlClusterReplicaTransitionStatus
		checkpoint      int32
		reason, message string
	}{
		{"start", nil, transition(2, 3), 2, "ReplicaTransitionStarted", "Replica transition started from 2 to 3."},
		{"retarget", transition(2, 3), transition(2, 4), 2, "ReplicaTransitionRetargeted", "Replica transition retargeted from 3 to 4 replicas."},
		{"complete", transition(2, 3), nil, 3, "ReplicaTransitionCompleted", "Replica transition completed at 3 replicas."},
		{"compatibility checkpoint", nil, nil, 2, "", ""},
		{"same transition", transition(2, 3), transition(2, 3), 2, "", ""},
		{"clear without convergence", transition(2, 3), nil, 2, "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("replica-events", true)
			cluster.Status.LastConvergedReplicas = nil
			cluster.Status.ReplicaTransition = replicaTransitionCopy(tt.before)
			r := phase1HReconciler(t, cluster)
			metricOwner, metricRegistry := newMysqlMetricsForTest(t)
			r.Metrics = metricOwner
			fake := record.NewFakeRecorder(10)
			r.Recorder = &mysqlEventOrderRecorder{EventRecorder: fake, check: func(runtime.Object) {
				stored := phase4StoredCluster(t, r, cluster)
				g.Expect(stored.Status.LastConvergedReplicas).To(Equal(replicaCountCopy(tt.checkpoint)))
				g.Expect(stored.Status.ReplicaTransition).To(Equal(tt.after))
				g.Expect(r.Client.(*statefulSetReconcileMemoryClient).statusPatchCount).To(Equal(1))
			}}
			g.Expect(r.persistMysqlClusterReplicaTransitionStatus(context.Background(), cluster, tt.checkpoint, tt.after)).To(Succeed())
			output := expectMysqlEvent(t, fake, corev1.EventTypeNormal, tt.reason)
			wantCount := float64(0)
			if tt.reason != "" {
				wantCount = 1
			}
			expectMysqlTransitionMetric(t, metricRegistry, cluster, "replica_transitions_total", tt.reason, wantCount)
			if tt.reason != "" {
				g.Expect(output).To(Equal("Normal " + tt.reason + " " + tt.message))
			}
			g.Expect(r.persistMysqlClusterReplicaTransitionStatus(context.Background(), cluster, tt.checkpoint, replicaTransitionCopy(tt.after))).To(Succeed())
			expectMysqlEvent(t, fake, "", "")
			expectMysqlTransitionMetric(t, metricRegistry, cluster, "replica_transitions_total", tt.reason, wantCount)
		})
	}
}

func TestMysqlEventFailedPersistenceAndNilRecorder(t *testing.T) {
	for _, domain := range []string{"HA", "replicas"} {
		for _, mode := range []string{"conflict", "API error", "nil recorder"} {
			t.Run(domain+" "+mode, func(t *testing.T) {
				g := NewWithT(t)
				cluster := phase1HCluster("event-failure", true)
				before := cluster.DeepCopy()
				r := phase1HReconciler(t, cluster)
				metricOwner, metricRegistry := newMysqlMetricsForTest(t)
				r.Metrics = metricOwner
				memory := r.Client.(*statefulSetReconcileMemoryClient)
				fake := record.NewFakeRecorder(10)
				if mode != "nil recorder" {
					r.Recorder = fake
					memory.statusPatchError = errors.New("status API unavailable")
					if mode == "conflict" {
						memory.statusPatchError = apierrors.NewConflict(schema.GroupResource{Resource: "mysqlclusters"}, cluster.Name, errors.New("conflict"))
					}
				}
				var err error
				if domain == "HA" {
					_, err = r.persistMysqlClusterHAStatus(context.Background(), cluster, &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateFailoverRequired, Primary: "mysql-1", PrimaryUID: "uid"})
				} else {
					err = r.persistMysqlClusterReplicaTransitionStatus(context.Background(), cluster, 2, &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3})
				}
				if mode == "nil recorder" {
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(memory.statusPatchCount).To(Equal(1))
				} else {
					g.Expect(errors.Is(err, memory.statusPatchError)).To(BeTrue())
					g.Expect(phase4StoredCluster(t, r, cluster).Status).To(Equal(before.Status))
				}
				expectMysqlEvent(t, fake, "", "")
				wantCount := float64(0)
				if mode == "nil recorder" {
					wantCount = 1
				}
				family, reason := "ha_transitions_total", "FailoverRequired"
				if domain == "replicas" {
					family, reason = "replica_transitions_total", "ReplicaTransitionStarted"
				}
				expectMysqlTransitionMetric(t, metricRegistry, cluster, family, reason, wantCount)
			})
		}
	}
}

type mysqlFailingEventSink struct{ attempted chan struct{} }

func (s *mysqlFailingEventSink) Create(event *corev1.Event) (*corev1.Event, error) {
	select {
	case s.attempted <- struct{}{}:
	default:
	}
	return nil, apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, event.Name, errors.New("event delivery rejected"))
}
func (s *mysqlFailingEventSink) Update(event *corev1.Event) (*corev1.Event, error) {
	return s.Create(event)
}
func (s *mysqlFailingEventSink) Patch(event *corev1.Event, _ []byte) (*corev1.Event, error) {
	return s.Create(event)
}

func TestMysqlEventDeliveryFailureDoesNotAffectPersistence(t *testing.T) {
	g := NewWithT(t)
	cluster := phase1HCluster("event-delivery", true)
	r := phase1HReconciler(t, cluster)
	metricOwner, metricRegistry := newMysqlMetricsForTest(t)
	r.Metrics = metricOwner
	broadcaster := record.NewBroadcaster()
	defer broadcaster.Shutdown()
	sink := &mysqlFailingEventSink{attempted: make(chan struct{}, 1)}
	watcher := broadcaster.StartRecordingToSink(sink)
	defer watcher.Stop()
	r.Recorder = broadcaster.NewRecorder(r.Scheme, corev1.EventSource{Component: "mysql-event-test"})
	desired := &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateFailoverRequired, Primary: "mysql-1", PrimaryUID: "uid"}
	changed, err := r.persistMysqlClusterHAStatus(context.Background(), cluster, desired)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(changed).To(BeTrue())
	select {
	case <-sink.attempted:
	case <-time.After(3 * time.Second):
		t.Fatal("event was not delivered to the failing local sink")
	}
	g.Expect(phase4StoredCluster(t, r, cluster).Status.HA).To(Equal(desired))
	g.Expect(r.Client.(*statefulSetReconcileMemoryClient).statusPatchCount).To(Equal(1))
	expectMysqlTransitionMetric(t, metricRegistry, cluster, "ha_transitions_total", "FailoverRequired", 1)
}

func TestMysqlEventProjectionIsSilent(t *testing.T) {
	g := NewWithT(t)
	cluster := phase1HCluster("projection-no-event", true)
	sts := phase1HStatefulSet(t, cluster)
	pod := phase1HPod(t, cluster, sts, 1, "master", true)
	r := phase1HReconciler(t, cluster, sts, pod)
	fake := record.NewFakeRecorder(10)
	r.Recorder = fake
	changed, err := r.reconcileMysqlObservability(context.Background(), cluster, true)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(changed).To(BeTrue())
	endpoints := phase1HEndpoints(cluster, pod)
	g.Expect(r.Create(context.Background(), endpoints)).To(Succeed())
	g.Expect(r.mapMysqlPrimaryEndpointsToMysqlClusters(context.Background(), endpoints)).To(HaveLen(1))
	stored := phase4StoredCluster(t, r, cluster)
	changed, err = r.reconcileMysqlObservability(context.Background(), stored, true)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(changed).To(BeTrue())
	expectMysqlEvent(t, fake, "", "")
}

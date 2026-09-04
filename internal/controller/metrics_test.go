package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const mysqlTestMetricPrefix = "mysql_operator_mysqlcluster_"

func TestHandoffMetrics(t *testing.T) {
	g := NewWithT(t)
	m, registry := newMysqlMetricsForTest(t)
	cluster := phase1HCluster("handoff-metrics", true)
	upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStageReplicasVerified)
	for _, stage := range append(append([]string{""}, mysqlMetricHandoffStages...), "SENSITIVE-FUTURE") {
		cluster.Status.Upgrade.Handoff = nil
		if stage != "" {
			proof := "SENSITIVE-GTID"
			cluster.Status.Upgrade.Handoff = &databasev1.MysqlClusterUpgradeHandoffStatus{Stage: databasev1.MysqlClusterUpgradeHandoffStage(stage), OldPrimary: "SENSITIVE-POD", OldPrimaryUID: "SENSITIVE-UID", Candidate: "SENSITIVE-CANDIDATE", CandidateUID: "SENSITIVE-CANDIDATE-UID", OldPrimaryServerUUID: "SENSITIVE-UUID", OldPrimaryGTIDSet: &proof}
		}
		m.syncStatus(cluster)
		expectMysqlGauge(t, registry, cluster, "upgrade_handoff_active", "", "", mysqlMetricBool(stage != ""))
		for _, allowed := range mysqlMetricHandoffStages {
			expectMysqlGauge(t, registry, cluster, "upgrade_handoff_stage", "stage", allowed, mysqlMetricBool(stage == allowed))
		}
		for _, sample := range gatherMysqlMetrics(t, registry) {
			for _, value := range sample.labels {
				g.Expect(value).NotTo(ContainSubstring("SENSITIVE-"))
			}
		}
	}
	m.deleteCluster(cluster.Namespace, cluster.Name)
	g.Expect(gatherMysqlMetrics(t, registry)).To(BeEmpty())
}

func TestUpgradeReplacementMetrics(t *testing.T) {
	g := NewWithT(t)
	m, registry := newMysqlMetricsForTest(t)
	cluster := phase1HCluster("replacement-metrics", true)
	upgradeTestPlan(cluster, databasev1.MysqlClusterUpgradeStageTemplateReady)
	for _, stage := range []string{"", "DeletePending", "WaitingForReplacement", "Verifying", "SENSITIVE-FUTURE"} {
		cluster.Status.Upgrade.Replacement = nil
		if stage != "" {
			cluster.Status.Upgrade.Replacement = &databasev1.MysqlClusterUpgradeReplacementStatus{Stage: databasev1.MysqlClusterUpgradeReplacementStage(stage), Ordinal: 99, PodName: "SENSITIVE-POD", OldPodUID: "SENSITIVE-OLD", NewPodUID: "SENSITIVE-NEW"}
		}
		m.syncStatus(cluster)
		expectMysqlGauge(t, registry, cluster, "upgrade_replacement_active", "", "", mysqlMetricBool(stage != ""))
		for _, allowed := range []string{"DeletePending", "WaitingForReplacement", "Verifying"} {
			expectMysqlGauge(t, registry, cluster, "upgrade_replacement_stage", "stage", allowed, mysqlMetricBool(stage == allowed))
		}
		for _, sample := range gatherMysqlMetrics(t, registry) {
			for key, value := range sample.labels {
				g.Expect(key).NotTo(BeElementOf("pod", "uid", "ordinal", "image", "server_uuid", "GTID"))
				g.Expect(value).NotTo(ContainSubstring("SENSITIVE-"))
			}
		}
	}
	m.deleteCluster(cluster.Namespace, cluster.Name)
	g.Expect(gatherMysqlMetrics(t, registry)).To(BeEmpty())
}

func TestUpgradeMetrics(t *testing.T) {
	g := NewWithT(t)
	m, registry := newMysqlMetricsForTest(t)
	cluster := phase1HCluster("upgrade-metrics", true)
	for _, stage := range []string{"", "Preparing", "TemplatePending", "TemplateReady", "ReplicasVerified", "PrimaryReady", "SENSITIVE-UNKNOWN"} {
		cluster.Status.Upgrade = nil
		if stage != "" {
			cluster.Status.Upgrade = &databasev1.MysqlClusterUpgradeStatus{FromImage: "SENSITIVE-IMAGE-OLD", TargetImage: "SENSITIVE-IMAGE-NEW", Stage: databasev1.MysqlClusterUpgradeStage(stage)}
		}
		m.syncStatus(cluster)
		expectMysqlGauge(t, registry, cluster, "upgrade_active", "", "", mysqlMetricBool(stage != ""))
		for _, allowed := range []string{"Preparing", "TemplatePending", "TemplateReady", "ReplicasVerified", "PrimaryReady"} {
			expectMysqlGauge(t, registry, cluster, "upgrade_stage", "stage", allowed, mysqlMetricBool(stage == allowed))
		}
		for _, sample := range gatherMysqlMetrics(t, registry) {
			for _, value := range sample.labels {
				g.Expect(value).NotTo(ContainSubstring("SENSITIVE-"))
			}
		}
	}
	m.deleteCluster(cluster.Namespace, cluster.Name)
	g.Expect(gatherMysqlMetrics(t, registry)).To(BeEmpty())
}

type mysqlTestMetricSample struct {
	name, kind string
	labels     map[string]string
	value      float64
}

func newMysqlMetricsForTest(t *testing.T) (*MysqlClusterMetrics, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	m, err := NewMysqlClusterMetrics(registry)
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	return m, registry
}

func gatherMysqlMetrics(t *testing.T, registry *prometheus.Registry) []mysqlTestMetricSample {
	t.Helper()
	families, err := registry.Gather()
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	var samples []mysqlTestMetricSample
	for _, family := range families {
		for _, metric := range family.Metric {
			sample := mysqlTestMetricSample{name: family.GetName(), kind: family.GetType().String(), labels: map[string]string{}}
			for _, label := range metric.Label {
				sample.labels[label.GetName()] = label.GetValue()
			}
			if metric.Gauge != nil {
				sample.value = metric.Gauge.GetValue()
			} else if metric.Counter != nil {
				sample.value = metric.Counter.GetValue()
			} else {
				t.Fatalf("unexpected metric type: %s", sample.kind)
			}
			samples = append(samples, sample)
		}
	}
	return samples
}

func mysqlMetricSnapshot(t *testing.T, registry *prometheus.Registry) map[string]float64 {
	t.Helper()
	snapshot := map[string]float64{}
	for _, sample := range gatherMysqlMetrics(t, registry) {
		labels, err := json.Marshal(sample.labels)
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		snapshot[sample.name+string(labels)] = sample.value
	}
	return snapshot
}

func expectMysqlGauge(t *testing.T, registry *prometheus.Registry, cluster *databasev1.MysqlCluster, suffix, dimension, label string, want float64) {
	t.Helper()
	g := NewWithT(t)
	labels := map[string]string{"namespace": cluster.Namespace, "cluster": cluster.Name}
	if dimension != "" {
		labels[dimension] = label
	}
	for _, sample := range gatherMysqlMetrics(t, registry) {
		if sample.name == mysqlTestMetricPrefix+suffix && sample.labels["namespace"] == cluster.Namespace && sample.labels["cluster"] == cluster.Name && (dimension == "" || sample.labels[dimension] == label) {
			g.Expect(sample.kind).To(Equal("GAUGE"))
			g.Expect(sample.labels).To(Equal(labels))
			g.Expect(sample.value).To(Equal(want), "%s %s", suffix, label)
			return
		}
	}
	t.Fatalf("missing gauge %s %v", suffix, labels)
}

// Assert both the chosen label and the sum across the whole counter family.
// This catches duplicate increments under an overlapping milestone reason.
func expectMysqlTransitionMetric(t *testing.T, registry *prometheus.Registry, cluster *databasev1.MysqlCluster, suffix, reason string, want float64) {
	t.Helper()
	g := NewWithT(t)
	var sum, selected float64
	for _, sample := range gatherMysqlMetrics(t, registry) {
		if sample.name != mysqlTestMetricPrefix+suffix || sample.labels["namespace"] != cluster.Namespace || sample.labels["cluster"] != cluster.Name {
			continue
		}
		g.Expect(sample.kind).To(Equal("COUNTER"))
		g.Expect(sample.labels).To(HaveLen(3))
		sum += sample.value
		if sample.labels["transition"] == reason {
			selected += sample.value
		}
	}
	g.Expect(sum).To(Equal(want))
	g.Expect(selected).To(Equal(want))
}

func TestMysqlMetricsRegistration(t *testing.T) {
	g := NewWithT(t)
	m, registry := newMysqlMetricsForTest(t)
	cluster := phase1HCluster("registered", true)
	m.syncStatus(cluster)
	m.incrementHA(cluster.Namespace, cluster.Name, "FailoverRequired")
	m.incrementReplica(cluster.Namespace, cluster.Name, "ReplicaTransitionStarted")
	families, err := registry.Gather()
	g.Expect(err).NotTo(HaveOccurred())
	var names []string
	for _, family := range families {
		names = append(names, strings.TrimPrefix(family.GetName(), mysqlTestMetricPrefix))
		g.Expect(family.GetName()).To(HavePrefix(mysqlTestMetricPrefix))
	}
	g.Expect(names).To(ConsistOf("desired_replicas", "current_replicas", "ready_replicas", "phase", "condition", "replica_transition_active", "ha_state", "failover_stage", "fence_state", "ha_transitions_total", "replica_transitions_total", "upgrade_active", "upgrade_stage", "upgrade_replacement_active", "upgrade_replacement_stage", "upgrade_handoff_active", "upgrade_handoff_stage"))
	before := mysqlMetricSnapshot(t, registry)
	duplicate, err := NewMysqlClusterMetrics(registry)
	g.Expect(err).To(HaveOccurred())
	g.Expect(duplicate).To(BeNil())
	g.Expect(mysqlMetricSnapshot(t, registry)).To(Equal(before))
	separate, separateRegistry := newMysqlMetricsForTest(t)
	separate.syncStatus(cluster)
	separate.incrementHA(cluster.Namespace, cluster.Name, "HARecovered")
	expectMysqlTransitionMetric(t, separateRegistry, cluster, "ha_transitions_total", "HARecovered", 1)
	expectMysqlTransitionMetric(t, registry, cluster, "ha_transitions_total", "FailoverRequired", 1)
	_, err = NewMysqlClusterMetrics(nil)
	g.Expect(err).To(HaveOccurred())
}

func TestMysqlMetricsStateAndCardinality(t *testing.T) {
	g := NewWithT(t)
	m, registry := newMysqlMetricsForTest(t)
	cluster := phase1HCluster("state-metrics", true)
	cluster.Spec.Replicas = replicaCountCopy(5)
	cluster.Status.CurrentReplicas, cluster.Status.ReadyReplicas = 4, 3
	cluster.Status.Phase = databasev1.MysqlClusterPhaseDegraded
	cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 4, TargetReplicas: 5}
	cluster.Status.Conditions = []metav1.Condition{
		{Type: "Available", Status: metav1.ConditionTrue, Reason: "SENSITIVE-SECRET", Message: "SENSITIVE-GTID"},
		{Type: "Progressing", Status: metav1.ConditionUnknown}, {Type: "Degraded", Status: metav1.ConditionFalse},
		{Type: "SENSITIVE-SECRET", Status: metav1.ConditionTrue},
	}
	gtid := "SENSITIVE-GTID"
	cluster.Status.Primary = "SENSITIVE-PRIMARY"
	cluster.Status.CredentialsSecretUID = "SENSITIVE-SECRET"
	cluster.Status.HA = &databasev1.MysqlClusterHAStatus{
		State: databasev1.MysqlClusterHAStateFailoverInProgress, PrimaryUID: "SENSITIVE-PRIMARY-UID",
		Failover: &databasev1.MysqlClusterFailoverStatus{Stage: databasev1.MysqlClusterFailoverStagePromoting, FenceState: databasev1.MysqlClusterFenceStatePending, CandidateUID: "SENSITIVE-CANDIDATE-UID", FailedPrimaryServerUUID: "SENSITIVE-SERVER-UUID", FailedPrimaryGTIDSet: &gtid},
	}
	m.syncStatus(cluster)
	expectMysqlGauge(t, registry, cluster, "desired_replicas", "", "", 5)
	expectMysqlGauge(t, registry, cluster, "current_replicas", "", "", 4)
	expectMysqlGauge(t, registry, cluster, "ready_replicas", "", "", 3)
	expectMysqlGauge(t, registry, cluster, "replica_transition_active", "", "", 1)
	domains := map[string]struct {
		label   string
		values  []string
		current string
	}{
		"phase":          {"phase", []string{"Pending", "Initializing", "Running", "Degraded", "Failed"}, "Degraded"},
		"condition":      {"condition", []string{"Available", "Progressing", "Degraded"}, "Available"},
		"ha_state":       {"state", []string{"Healthy", "Suspected", "FailoverRequired", "FailoverInProgress", "Verifying", "Degraded"}, "FailoverInProgress"},
		"failover_stage": {"stage", []string{"Fencing", "CandidateSelected", "Promoting", "Reconfiguring"}, "Promoting"},
		"fence_state":    {"state", []string{"Pending", "Verified", "Blocked"}, "Pending"},
	}
	for family, domain := range domains {
		for _, value := range domain.values {
			want := float64(0)
			if value == domain.current {
				want = 1
			}
			expectMysqlGauge(t, registry, cluster, family, domain.label, value, want)
		}
	}
	for _, reason := range mysqlMetricHATransitions {
		m.incrementHA(cluster.Namespace, cluster.Name, reason)
	}
	for _, reason := range mysqlMetricReplicaTransitions {
		m.incrementReplica(cluster.Namespace, cluster.Name, reason)
	}
	for _, reason := range []string{"Suspected", "FailureCount", "SENSITIVE-GTID", "SENSITIVE-SECRET", "arbitrary"} {
		m.incrementHA(cluster.Namespace, cluster.Name, reason)
		m.incrementReplica(cluster.Namespace, cluster.Name, reason)
	}
	for _, sample := range gatherMysqlMetrics(t, registry) {
		for key, value := range sample.labels {
			g.Expect(key).To(BeElementOf("namespace", "cluster", "phase", "condition", "state", "stage", "transition"))
			g.Expect(value).NotTo(ContainSubstring("SENSITIVE-"))
		}
		if sample.kind == "COUNTER" {
			g.Expect(sample.value).To(Equal(float64(1)))
			if strings.HasSuffix(sample.name, "_ha_transitions_total") {
				g.Expect(sample.labels["transition"]).To(BeElementOf(mysqlMetricHATransitions))
			} else {
				g.Expect(sample.labels["transition"]).To(BeElementOf(mysqlMetricReplicaTransitions))
			}
		}
	}
	// Clear every previously active finite series, including unknown enums.
	for _, unknown := range []string{"", "SENSITIVE-UNKNOWN"} {
		cluster.Status.Phase = databasev1.MysqlClusterPhase(unknown)
		cluster.Status.HA = nil
		if unknown != "" {
			cluster.Status.HA = &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAState(unknown), Failover: &databasev1.MysqlClusterFailoverStatus{Stage: databasev1.MysqlClusterFailoverStage(unknown), FenceState: databasev1.MysqlClusterFenceState(unknown)}}
		}
		cluster.Status.Conditions, cluster.Status.ReplicaTransition = nil, nil
		m.syncStatus(cluster)
		for family, domain := range domains {
			for _, value := range domain.values {
				expectMysqlGauge(t, registry, cluster, family, domain.label, value, 0)
			}
		}
		expectMysqlGauge(t, registry, cluster, "replica_transition_active", "", "", 0)
		for _, sample := range gatherMysqlMetrics(t, registry) {
			for _, value := range sample.labels {
				g.Expect(value).NotTo(ContainSubstring("SENSITIVE-"))
			}
		}
	}
	cluster.Status.HA = &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateHealthy}
	m.syncStatus(cluster)
	expectMysqlGauge(t, registry, cluster, "ha_state", "state", "Healthy", 1)
	for _, family := range []string{"failover_stage", "fence_state"} {
		domain := domains[family]
		for _, value := range domain.values {
			expectMysqlGauge(t, registry, cluster, family, domain.label, value, 0)
		}
	}
}

type mysqlMetricPatchCheckClient struct {
	client.Client
	check func()
}
type mysqlMetricPatchCheckWriter struct {
	client.SubResourceWriter
	check func()
}

func (c *mysqlMetricPatchCheckClient) Status() client.SubResourceWriter {
	return &mysqlMetricPatchCheckWriter{SubResourceWriter: c.Client.Status(), check: c.check}
}
func (w *mysqlMetricPatchCheckWriter) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.SubResourcePatchOption) error {
	w.check()
	err := w.SubResourceWriter.Patch(ctx, object, patch, options...)
	w.check() // Even after persistence, gauges wait until Patch returns success.
	return err
}

func TestMysqlMetricsProjectionPublicationAndRestart(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "persist and restart", true: "failed patch retains snapshot"}[fail], func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("projection-metrics", true)
			cluster.Spec.Replicas = replicaCountCopy(1)
			cluster.Status.LastConvergedReplicas = replicaCountCopy(1)
			sts := phase1HStatefulSet(t, cluster)
			pod := phase1HPod(t, cluster, sts, 1, "master", true)
			endpoints := phase1HEndpoints(cluster, pod)
			r := phase1HReconciler(t, cluster, sts, pod, endpoints)
			memory := r.Client.(*statefulSetReconcileMemoryClient)
			m, registry := newMysqlMetricsForTest(t)
			r.Metrics = m
			m.syncStatus(cluster)
			oldSnapshot := mysqlMetricSnapshot(t, registry)
			r.Client = &mysqlMetricPatchCheckClient{Client: r.Client, check: func() { g.Expect(mysqlMetricSnapshot(t, registry)).To(Equal(oldSnapshot)) }}
			r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
				t.Fatal("metric publication must not execute SQL")
				return "", nil
			}
			if fail {
				memory.statusPatchError = apierrors.NewConflict(schema.GroupResource{Resource: "mysqlclusters"}, cluster.Name, errors.New("conflict"))
			}
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
			result, err := r.Reconcile(context.Background(), request)
			if fail {
				g.Expect(errors.Is(err, memory.statusPatchError)).To(BeTrue())
				g.Expect(mysqlMetricSnapshot(t, registry)).To(Equal(oldSnapshot))
				g.Expect(memory.statusPatchCount).To(BeZero())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result).To(Equal(ctrl.Result{Requeue: true}))
			g.Expect(memory.statusPatchCount).To(Equal(1))
			stored := phase4StoredCluster(t, r, cluster)
			expectMysqlGauge(t, registry, stored, "current_replicas", "", "", float64(stored.Status.CurrentReplicas))
			expectMysqlGauge(t, registry, stored, "ready_replicas", "", "", 1)
			expectMysqlGauge(t, registry, stored, "phase", "phase", "Running", 1)
			// Recreating just the process-local owner leaves durable status
			// current. A no-op projection must still rebuild all state gauges.
			r.Metrics, registry = newMysqlMetricsForTest(t)
			g.Expect(gatherMysqlMetrics(t, registry)).To(BeEmpty())
			changed, err := r.reconcileMysqlObservability(context.Background(), stored, true)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(changed).To(BeFalse())
			g.Expect(memory.statusPatchCount).To(Equal(1))
			expectMysqlGauge(t, registry, stored, "current_replicas", "", "", 1)
			expectMysqlGauge(t, registry, stored, "phase", "phase", "Running", 1)
			g.Expect(gatherMysqlMetrics(t, registry)).To(HaveLen(42))
			withoutMetrics := phase1HReconciler(t, cluster, sts, pod, endpoints)
			withoutResult, withoutErr := withoutMetrics.Reconcile(context.Background(), request)
			g.Expect(withoutErr).NotTo(HaveOccurred())
			g.Expect(withoutResult).To(Equal(result))
			g.Expect(withoutMetrics.Client.(*statefulSetReconcileMemoryClient).statusPatchCount).To(Equal(1))
		})
	}
}

type mysqlMetricGetErrorClient struct {
	client.Client
	err error
}

func (c *mysqlMetricGetErrorClient) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return c.err
}

func TestMysqlMetricsDeletionCleanup(t *testing.T) {
	for _, transient := range []bool{false, true} {
		t.Run(map[bool]string{false: "not found", true: "transient error"}[transient], func(t *testing.T) {
			g := NewWithT(t)
			m, registry := newMysqlMetricsForTest(t)
			cluster := phase1HCluster("deleted-metrics", true)
			otherName, otherNamespace := cluster.DeepCopy(), cluster.DeepCopy()
			otherName.Name, otherNamespace.Namespace = "other-cluster", "other-namespace"
			for _, object := range []*databasev1.MysqlCluster{cluster, otherName, otherNamespace} {
				m.syncStatus(object)
				for _, reason := range mysqlMetricHATransitions {
					m.incrementHA(object.Namespace, object.Name, reason)
				}
				for _, reason := range mysqlMetricReplicaTransitions {
					m.incrementReplica(object.Namespace, object.Name, reason)
				}
			}
			r := phase1HReconciler(t)
			r.Metrics = m
			before := mysqlMetricSnapshot(t, registry)
			getErr := errors.New("temporary API error")
			if transient {
				r.Client = &mysqlMetricGetErrorClient{Client: r.Client, err: getErr}
			}
			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			g.Expect(result).To(Equal(ctrl.Result{}))
			if transient {
				g.Expect(errors.Is(err, getErr)).To(BeTrue())
				g.Expect(mysqlMetricSnapshot(t, registry)).To(Equal(before))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			samples := gatherMysqlMetrics(t, registry)
			g.Expect(samples).To(HaveLen(2 * (42 + 14 + 3)))
			for _, sample := range samples {
				g.Expect(sample.labels["namespace"] == cluster.Namespace && sample.labels["cluster"] == cluster.Name).To(BeFalse())
			}
			r.Metrics = nil
			_, err = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			g.Expect(err).NotTo(HaveOccurred())
		})
	}
}

func TestMysqlMetricsAndRecorderAreIndependent(t *testing.T) {
	for _, withMetrics := range []bool{false, true} {
		t.Run(map[bool]string{false: "recorder only", true: "metrics only"}[withMetrics], func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("independent-observers", true)
			r := phase1HReconciler(t, cluster)
			m, registry := newMysqlMetricsForTest(t)
			fake := record.NewFakeRecorder(4)
			if withMetrics {
				r.Metrics = m
			} else {
				r.Recorder = fake
			}
			_, err := r.persistMysqlClusterHAStatus(context.Background(), cluster, &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateFailoverRequired})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(r.persistMysqlClusterReplicaTransitionStatus(context.Background(), cluster, 2, &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3})).To(Succeed())
			if withMetrics {
				expectMysqlTransitionMetric(t, registry, cluster, "ha_transitions_total", "FailoverRequired", 1)
				expectMysqlTransitionMetric(t, registry, cluster, "replica_transitions_total", "ReplicaTransitionStarted", 1)
				g.Expect(fake.Events).NotTo(Receive())
			} else {
				var first, second string
				g.Expect(fake.Events).To(Receive(&first))
				g.Expect(first).To(HavePrefix("Warning FailoverRequired "))
				g.Expect(fake.Events).To(Receive(&second))
				g.Expect(second).To(HavePrefix("Normal ReplicaTransitionStarted "))
				g.Expect(gatherMysqlMetrics(t, registry)).To(BeEmpty())
			}
		})
	}
}

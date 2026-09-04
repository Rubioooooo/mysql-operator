package controller

import (
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
)

// Only namespace and cluster are runtime label values. All remaining label
// domains are fixed here, including the allowlists for Gate 2-B reasons.
var (
	mysqlMetricUpgradeStages  = []string{"Preparing", "TemplatePending", "TemplateReady"}
	mysqlMetricPhases         = []string{"Pending", "Initializing", "Running", "Degraded", "Failed"}
	mysqlMetricConditions     = []string{"Available", "Progressing", "Degraded"}
	mysqlMetricHAStates       = []string{"Healthy", "Suspected", "FailoverRequired", "FailoverInProgress", "Verifying", "Degraded"}
	mysqlMetricFailoverStages = []string{"Fencing", "CandidateSelected", "Promoting", "Reconfiguring"}
	mysqlMetricFenceStates    = []string{"Pending", "Verified", "Blocked"}
	mysqlMetricHATransitions  = []string{
		"FailoverRequired", "FailoverStarted", "PrimaryFenced", "PrimaryFenceLost",
		"FailoverBlocked", "NoSafeCandidate", "CandidateSelected", "CandidateInvalidated",
		"PromotionStarted", "PrimaryPromoted", "UnsafeReplicaRejoin", "FailoverVerifying", "FailoverAborted", "HARecovered",
	}
	mysqlMetricReplicaTransitions = []string{"ReplicaTransitionStarted", "ReplicaTransitionRetargeted", "ReplicaTransitionCompleted"}
)

// MysqlClusterMetrics owns process-local collectors. It has no Kubernetes client
// or persistence and is safe to omit from a reconciler. Prometheus vectors handle
// concurrent updates; no control operation depends on metric values.
type MysqlClusterMetrics struct {
	upgradeActive, upgradeStage                     *prometheus.GaugeVec
	desiredReplicas, currentReplicas, readyReplicas *prometheus.GaugeVec
	phase, condition, replicaTransitionActive       *prometheus.GaugeVec
	haState, failoverStage, fenceState              *prometheus.GaugeVec
	haTransitions, replicaTransitions               *prometheus.CounterVec
}

// NewMysqlClusterMetrics registers only with the supplied registerer. Duplicate
// registration is an explicit setup error, never an implicit global panic.
func NewMysqlClusterMetrics(registerer prometheus.Registerer) (*MysqlClusterMetrics, error) {
	if registerer == nil {
		return nil, fmt.Errorf("MysqlCluster metrics require a registerer")
	}
	gauge := func(name, help string, dimensions ...string) *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "mysql_operator", Subsystem: "mysqlcluster", Name: name, Help: help}, append([]string{"namespace", "cluster"}, dimensions...))
	}
	counter := func(name, help string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "mysql_operator", Subsystem: "mysqlcluster", Name: name, Help: help}, []string{"namespace", "cluster", "transition"})
	}
	m := &MysqlClusterMetrics{
		upgradeActive:           gauge("upgrade_active", "Whether a durable image upgrade is active."),
		upgradeStage:            gauge("upgrade_stage", "Current durable upgrade stage, as a one-hot gauge.", "stage"),
		desiredReplicas:         gauge("desired_replicas", "Desired MySQL member count."),
		currentReplicas:         gauge("current_replicas", "Current MySQL member count from persisted status."),
		readyReplicas:           gauge("ready_replicas", "Ready MySQL member count from persisted status."),
		phase:                   gauge("phase", "Current persisted cluster phase, as a one-hot gauge.", "phase"),
		condition:               gauge("condition", "Whether a persisted cluster condition is True.", "condition"),
		replicaTransitionActive: gauge("replica_transition_active", "Whether a durable replica transition is active."),
		haState:                 gauge("ha_state", "Current durable HA state, as a one-hot gauge.", "state"),
		failoverStage:           gauge("failover_stage", "Current durable failover stage, as a one-hot gauge.", "stage"),
		fenceState:              gauge("fence_state", "Current durable fence state, as a one-hot gauge.", "state"),
		haTransitions:           counter("ha_transitions_total", "High-signal HA transitions successfully persisted in this process."),
		replicaTransitions:      counter("replica_transitions_total", "Replica transitions successfully persisted in this process."),
	}
	collectors := []prometheus.Collector{m.desiredReplicas, m.currentReplicas, m.readyReplicas, m.phase, m.condition, m.replicaTransitionActive, m.haState, m.failoverStage, m.fenceState, m.haTransitions, m.replicaTransitions, m.upgradeActive, m.upgradeStage}
	for i, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			// Undo only this constructor's registrations, leaving any existing
			// owner's collectors intact after a partial setup failure.
			for _, registered := range collectors[:i] {
				registerer.Unregister(registered)
			}
			return nil, fmt.Errorf("register MysqlCluster metrics: %w", err)
		}
	}
	return m, nil
}

// syncStatus is called only at Gate 2-A's successful/no-op publication boundary.
// It never observes Pods or SQL and does not reconstruct status.
func (m *MysqlClusterMetrics) syncStatus(cluster *databasev1.MysqlCluster) {
	if m == nil {
		return
	}
	namespace, name := cluster.Namespace, cluster.Name
	m.upgradeActive.WithLabelValues(namespace, name).Set(mysqlMetricBool(cluster.Status.Upgrade != nil))
	upgradeStage := ""
	if cluster.Status.Upgrade != nil {
		upgradeStage = string(cluster.Status.Upgrade.Stage)
	}
	setMysqlMetricOneHot(m.upgradeStage, namespace, name, mysqlMetricUpgradeStages, upgradeStage)
	m.desiredReplicas.WithLabelValues(namespace, name).Set(float64(desiredReplicas(cluster)))
	m.currentReplicas.WithLabelValues(namespace, name).Set(float64(cluster.Status.CurrentReplicas))
	m.readyReplicas.WithLabelValues(namespace, name).Set(float64(cluster.Status.ReadyReplicas))
	setMysqlMetricOneHot(m.phase, namespace, name, mysqlMetricPhases, string(cluster.Status.Phase))
	for _, condition := range mysqlMetricConditions {
		m.condition.WithLabelValues(namespace, name, condition).Set(mysqlMetricBool(meta.IsStatusConditionTrue(cluster.Status.Conditions, condition)))
	}
	m.replicaTransitionActive.WithLabelValues(namespace, name).Set(mysqlMetricBool(cluster.Status.ReplicaTransition != nil))
	var state, stage, fence string
	if ha := cluster.Status.HA; ha != nil {
		state = string(ha.State)
		if ha.Failover != nil {
			stage, fence = string(ha.Failover.Stage), string(ha.Failover.FenceState)
		}
	}
	setMysqlMetricOneHot(m.haState, namespace, name, mysqlMetricHAStates, state)
	setMysqlMetricOneHot(m.failoverStage, namespace, name, mysqlMetricFailoverStages, stage)
	setMysqlMetricOneHot(m.fenceState, namespace, name, mysqlMetricFenceStates, fence)
}

func mysqlMetricBool(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func setMysqlMetricOneHot(vector *prometheus.GaugeVec, namespace, cluster string, domain []string, current string) {
	for _, value := range domain {
		vector.WithLabelValues(namespace, cluster, value).Set(mysqlMetricBool(value == current))
	}
}

func (m *MysqlClusterMetrics) incrementHA(namespace, cluster, reason string) {
	if m == nil {
		return
	}
	incrementMysqlMetricTransition(m.haTransitions, namespace, cluster, reason, mysqlMetricHATransitions)
}

func (m *MysqlClusterMetrics) incrementReplica(namespace, cluster, reason string) {
	if m == nil {
		return
	}
	incrementMysqlMetricTransition(m.replicaTransitions, namespace, cluster, reason, mysqlMetricReplicaTransitions)
}

func incrementMysqlMetricTransition(vector *prometheus.CounterVec, namespace, cluster, reason string, domain []string) {
	for _, value := range domain {
		if reason == value {
			vector.WithLabelValues(namespace, cluster, value).Inc()
			return
		}
	}
}

// Called only when the MysqlCluster GET returns NotFound. Counters intentionally
// share the cluster's in-memory lifetime; they are not a durable event ledger.
func (m *MysqlClusterMetrics) deleteCluster(namespace, cluster string) {
	if m == nil {
		return
	}
	for _, vector := range []*prometheus.GaugeVec{m.desiredReplicas, m.currentReplicas, m.readyReplicas, m.replicaTransitionActive, m.upgradeActive} {
		vector.DeleteLabelValues(namespace, cluster)
	}
	deleteMysqlMetricDomain(m.phase, namespace, cluster, mysqlMetricPhases)
	deleteMysqlMetricDomain(m.upgradeStage, namespace, cluster, mysqlMetricUpgradeStages)
	deleteMysqlMetricDomain(m.condition, namespace, cluster, mysqlMetricConditions)
	deleteMysqlMetricDomain(m.haState, namespace, cluster, mysqlMetricHAStates)
	deleteMysqlMetricDomain(m.failoverStage, namespace, cluster, mysqlMetricFailoverStages)
	deleteMysqlMetricDomain(m.fenceState, namespace, cluster, mysqlMetricFenceStates)
	deleteMysqlMetricDomain(m.haTransitions, namespace, cluster, mysqlMetricHATransitions)
	deleteMysqlMetricDomain(m.replicaTransitions, namespace, cluster, mysqlMetricReplicaTransitions)
}

func deleteMysqlMetricDomain(vector interface{ DeleteLabelValues(...string) bool }, namespace, cluster string, domain []string) {
	for _, value := range domain {
		vector.DeleteLabelValues(namespace, cluster, value)
	}
}

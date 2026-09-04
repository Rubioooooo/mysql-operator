package controller

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"sync"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	"github.com/go-logr/logr"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type mysqlCapturedLog struct {
	Level   int
	Message string
	Values  map[string]interface{}
	Error   string
}

type mysqlLogCapture struct {
	mu      sync.Mutex
	entries []mysqlCapturedLog
	check   func(mysqlCapturedLog)
}

type mysqlCaptureLogSink struct {
	capture  *mysqlLogCapture
	maxLevel int
	values   []interface{}
}

func (s *mysqlCaptureLogSink) Init(logr.RuntimeInfo)        {}
func (s *mysqlCaptureLogSink) Enabled(level int) bool       { return level <= s.maxLevel }
func (s *mysqlCaptureLogSink) WithName(string) logr.LogSink { copy := *s; return &copy }
func (s *mysqlCaptureLogSink) WithValues(values ...interface{}) logr.LogSink {
	copy := *s
	copy.values = append(append([]interface{}{}, s.values...), values...)
	return &copy
}
func (s *mysqlCaptureLogSink) Info(level int, message string, values ...interface{}) {
	s.captureEntry(level, message, nil, values)
}
func (s *mysqlCaptureLogSink) Error(err error, message string, values ...interface{}) {
	s.captureEntry(0, message, err, values)
}
func (s *mysqlCaptureLogSink) captureEntry(level int, message string, err error, values []interface{}) {
	entry := mysqlCapturedLog{Level: level, Message: message, Values: map[string]interface{}{}}
	if err != nil {
		entry.Error = err.Error()
	}
	all := append(append([]interface{}{}, s.values...), values...)
	for i := 0; i < len(all); i += 2 {
		entry.Values[all[i].(string)] = all[i+1]
	}
	if s.capture.check != nil {
		s.capture.check(entry)
	}
	s.capture.mu.Lock()
	defer s.capture.mu.Unlock()
	s.capture.entries = append(s.capture.entries, entry)
}
func (c *mysqlLogCapture) snapshot() []mysqlCapturedLog {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]mysqlCapturedLog{}, c.entries...)
}
func newMysqlLogContext(level int) (context.Context, *mysqlLogCapture) {
	capture := &mysqlLogCapture{}
	return log.IntoContext(context.Background(), logr.New(&mysqlCaptureLogSink{capture: capture, maxLevel: level})), capture
}

func TestHandoffLogSafety(t *testing.T) {
	g := NewWithT(t)
	ctx, capture := newMysqlLogContext(0)
	h := newHandoffTest(t, 1)
	h.r.Log = logr.Discard()
	h.r.Recorder = nil
	g.Expect(h.r.reconcileMysqlHandoffEntry(ctx, h.stored())).To(Succeed())
	entries := capture.snapshot()
	g.Expect(entries).To(HaveLen(1))
	g.Expect(entries[0].Values).To(Equal(map[string]interface{}{"operation": "upgrade_handoff", "handoff_stage": databasev1.MysqlClusterUpgradeHandoffStageFencing}))
	expectMysqlSafeLogs(t, entries)
}

func TestUpgradeReplacementLoggingSafety(t *testing.T) {
	g := NewWithT(t)
	ctx, capture := newMysqlLogContext(0)
	logMysqlUpgradeReplacement(ctx, &databasev1.MysqlClusterUpgradeReplacementStatus{Ordinal: 2, Stage: databasev1.MysqlClusterUpgradeReplacementStageVerifying, PodName: "SENSITIVE-POD", OldPodUID: "SENSITIVE-OLD", NewPodUID: "SENSITIVE-NEW"})
	entries := capture.snapshot()
	g.Expect(entries).To(HaveLen(1))
	g.Expect(entries[0].Values).To(Equal(map[string]interface{}{"operation": "upgrade_replacement", "replacement_stage": databasev1.MysqlClusterUpgradeReplacementStageVerifying, "ordinal": int32(2)}))
	expectMysqlSafeLogs(t, entries)
	deleteErr := &mysqlUpgradeDeleteError{cause: errors.New("SENSITIVE-POD-UID SENSITIVE-SERVER-UUID")}
	g.Expect(deleteErr.Error()).NotTo(ContainSubstring("SENSITIVE-"))
	g.Expect(errors.Unwrap(deleteErr)).To(Equal(deleteErr.cause))
}

func TestUpgradeStructuredLogs(t *testing.T) {
	g := NewWithT(t)
	ctx, capture := newMysqlLogContext(0)
	r, c, cluster := newUpgradeTest(t)
	cluster.Spec.Image = "mysql:new"
	storeUpgradeTestCluster(t, c, cluster)
	c.statusPatchError = errors.New("SENSITIVE-STDERR SENSITIVE-SQL")
	_, err := r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
	g.Expect(err).To(HaveOccurred())
	g.Expect(capture.snapshot()).To(BeEmpty())
	c.statusPatchError = nil
	_, err = r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	entries := capture.snapshot()
	g.Expect(entries).To(HaveLen(1))
	g.Expect(entries[0].Values).To(Equal(map[string]interface{}{
		"operation": "upgrade_transition", "upgrade_stage": databasev1.MysqlClusterUpgradeStagePreparing,
		"from_image": "mysql:old", "target_image": "mysql:new",
	}))
	expectMysqlSafeLogs(t, entries)
}

func mysqlSensitiveLogCluster() *databasev1.MysqlCluster {
	cluster := phase1HCluster("logging-cluster", true)
	cluster.UID = "SENSITIVE-CLUSTER-UID"
	cluster.Status.CredentialsSecretUID = "SENSITIVE-SECRET-UID"
	cluster.Annotations["root"] = "SENSITIVE-ROOT-PASSWORD"
	cluster.Annotations["replication"] = "SENSITIVE-REPLICATION-PASSWORD"
	cluster.Annotations["output"] = "SENSITIVE-SQL SENSITIVE-STDERR"
	proof := "SENSITIVE-GTID"
	cluster.Status.HA = &databasev1.MysqlClusterHAStatus{
		State: databasev1.MysqlClusterHAStateFailoverInProgress, Primary: "mysql-1", PrimaryUID: "SENSITIVE-PRIMARY-UID",
		Failover: &databasev1.MysqlClusterFailoverStatus{
			Stage: databasev1.MysqlClusterFailoverStageCandidateSelected, FenceState: databasev1.MysqlClusterFenceStateVerified,
			FailedPrimary: "mysql-1", FailedPrimaryUID: "SENSITIVE-FAILED-UID", FencedPrimaryUID: "SENSITIVE-FAILED-UID",
			Candidate: "mysql-2", CandidateUID: "SENSITIVE-CANDIDATE-UID", FailedPrimaryServerUUID: "SENSITIVE-SERVER-UUID", FailedPrimaryGTIDSet: &proof,
		},
	}
	return cluster
}

func expectMysqlSafeLogs(t *testing.T, entries []mysqlCapturedLog) {
	t.Helper()
	g := NewWithT(t)
	rendered, err := json.Marshal(entries)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(string(rendered)).NotTo(ContainSubstring("SENSITIVE-"))
	allowed := []string{"namespace", "cluster", "operation", "reason", "phase", "ha_state", "failover_stage", "fence_state", "primary", "candidate", "failed_primary", "desired_replicas", "current_replicas", "ready_replicas", "from_replicas", "target_replicas", "pod", "pods", "replicas", "failed_replicas", "upgrade_stage", "from_image", "target_image", "replacement_stage", "ordinal", "handoff_stage"}
	for _, entry := range entries {
		for key := range entry.Values {
			g.Expect(key).To(BeElementOf(allowed))
		}
	}
}

func TestMysqlLogHADurableTransition(t *testing.T) {
	for _, mode := range []string{"success", "no-op", "metadata only", "failed patch", "first healthy"} {
		t.Run(mode, func(t *testing.T) {
			g := NewWithT(t)
			ctx, capture := newMysqlLogContext(0)
			cluster := mysqlSensitiveLogCluster()
			after := cluster.Status.HA.DeepCopy()
			before := after.DeepCopy()
			clearMysqlElectionProof(before.Failover)
			switch mode {
			case "no-op":
				before = after.DeepCopy()
			case "metadata only":
				before = after.DeepCopy()
				after.FailureCount++
			case "first healthy":
				before = nil
				after = &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateHealthy}
			}
			cluster.Status.HA = before
			r := phase1HReconciler(t, cluster)
			memory := r.Client.(*statefulSetReconcileMemoryClient)
			if mode == "failed patch" {
				memory.statusPatchError = errors.New("status unavailable")
			}
			capture.check = func(mysqlCapturedLog) {
				g.Expect(memory.statusPatchCount).To(Equal(1))
				g.Expect(phase4StoredCluster(t, r, cluster).Status.HA).To(Equal(after))
			}
			_, err := r.persistMysqlClusterHAStatus(ctx, cluster, after)
			if mode == "failed patch" {
				g.Expect(errors.Is(err, memory.statusPatchError)).To(BeTrue())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
			}
			entries := capture.snapshot()
			if mode == "success" {
				g.Expect(entries).To(HaveLen(1))
				_, reason, _ := mysqlHAStatusTransitionEvent(before, after)
				g.Expect(entries[0].Level).To(Equal(0))
				g.Expect(entries[0].Message).To(Equal("mysqlcluster HA transition persisted"))
				g.Expect(entries[0].Values).To(HaveKeyWithValue("operation", "ha_transition"))
				g.Expect(entries[0].Values).To(HaveKeyWithValue("reason", reason))
				g.Expect(entries[0].Values).To(HaveKeyWithValue("candidate", "mysql-2"))
				g.Expect(entries[0].Values).To(HaveKeyWithValue("ha_state", after.State))
				g.Expect(entries[0].Values).To(HaveKeyWithValue("failover_stage", after.Failover.Stage))
			} else {
				g.Expect(entries).To(BeEmpty())
			}
			expectMysqlSafeLogs(t, entries)
		})
	}
}

func TestMysqlLogReplicaDurableTransition(t *testing.T) {
	for _, mode := range []string{"start", "retarget", "complete", "no-op", "compatibility", "failed patch"} {
		t.Run(mode, func(t *testing.T) {
			g := NewWithT(t)
			ctx, capture := newMysqlLogContext(0)
			cluster := mysqlSensitiveLogCluster()
			cluster.Status.LastConvergedReplicas = replicaCountCopy(2)
			beforeTransition := &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3}
			afterTransition := replicaTransitionCopy(beforeTransition)
			checkpoint := int32(2)
			switch mode {
			case "start", "failed patch":
				beforeTransition = nil
			case "retarget":
				afterTransition.TargetReplicas = 4
			case "complete":
				afterTransition = nil
				checkpoint = 3
			case "compatibility":
				beforeTransition, afterTransition = nil, nil
				cluster.Status.LastConvergedReplicas = nil
			}
			cluster.Status.ReplicaTransition = beforeTransition
			before := cluster.DeepCopy()
			r := phase1HReconciler(t, cluster)
			memory := r.Client.(*statefulSetReconcileMemoryClient)
			if mode == "failed patch" {
				memory.statusPatchError = errors.New("status unavailable")
			}
			capture.check = func(mysqlCapturedLog) {
				g.Expect(memory.statusPatchCount).To(Equal(1))
				stored := phase4StoredCluster(t, r, cluster)
				g.Expect(stored.Status.ReplicaTransition).To(Equal(afterTransition))
				g.Expect(stored.Status.LastConvergedReplicas).To(Equal(replicaCountCopy(checkpoint)))
			}
			err := r.persistMysqlClusterReplicaTransitionStatus(ctx, cluster, checkpoint, afterTransition)
			if mode == "failed patch" {
				g.Expect(errors.Is(err, memory.statusPatchError)).To(BeTrue())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
			}
			entries := capture.snapshot()
			if mode == "start" || mode == "retarget" || mode == "complete" {
				g.Expect(entries).To(HaveLen(1))
				reason, _ := mysqlReplicaTransitionEvent(&before.Status, &cluster.Status)
				g.Expect(entries[0].Level).To(Equal(0))
				g.Expect(entries[0].Message).To(Equal("mysqlcluster replica transition persisted"))
				g.Expect(entries[0].Values).To(HaveKeyWithValue("reason", reason))
				g.Expect(entries[0].Values).To(HaveKeyWithValue("from_replicas", int32(2)))
				target := int32(3)
				if mode == "retarget" {
					target = 4
				}
				g.Expect(entries[0].Values).To(HaveKeyWithValue("target_replicas", target))
			} else {
				g.Expect(entries).To(BeEmpty())
			}
			expectMysqlSafeLogs(t, entries)
		})
	}
}

type mysqlLoggingChildClient struct{ client.Client }

func (c *mysqlLoggingChildClient) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	log.FromContext(ctx).V(1).Info("child context observed", "operation", "replica_observation")
	return c.Client.List(ctx, list, options...)
}

func TestMysqlLogReconcileContextAndProjection(t *testing.T) {
	for _, mode := range []string{"debug", "default level", "failed projection"} {
		t.Run(mode, func(t *testing.T) {
			g := NewWithT(t)
			level := 1
			if mode == "default level" {
				level = 0
			}
			ctx, capture := newMysqlLogContext(level)
			cluster := mysqlSensitiveLogCluster()
			cluster.Status.HA = nil
			cluster.Spec.Replicas, cluster.Status.LastConvergedReplicas = replicaCountCopy(1), replicaCountCopy(1)
			sts := phase1HStatefulSet(t, cluster)
			pod := phase1HPod(t, cluster, sts, 1, "master", true)
			pod.UID = types.UID("SENSITIVE-POD-UID")
			r := phase1HReconciler(t, cluster, sts, pod, phase1HEndpoints(cluster, pod))
			memory := r.Client.(*statefulSetReconcileMemoryClient)
			r.Client = &mysqlLoggingChildClient{Client: r.Client}
			if mode == "failed projection" {
				memory.statusPatchError = errors.New("status unavailable")
			}
			capture.check = func(entry mysqlCapturedLog) {
				if entry.Values["operation"] == "observability" {
					g.Expect(memory.statusPatchCount).To(Equal(1))
					g.Expect(phase4StoredCluster(t, r, cluster).Status.Phase).To(Equal(databasev1.MysqlClusterPhaseRunning))
				}
			}
			result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
			if mode == "failed projection" {
				g.Expect(err).To(HaveOccurred())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(result).To(Equal(ctrl.Result{Requeue: true}))
			}
			entries := capture.snapshot()
			counts := map[string]int{}
			for _, entry := range entries {
				g.Expect(entry.Level).To(Equal(1))
				g.Expect(entry.Values).To(HaveKeyWithValue("namespace", cluster.Namespace))
				g.Expect(entry.Values).To(HaveKeyWithValue("cluster", cluster.Name))
				counts[entry.Values["operation"].(string)]++
			}
			if level == 0 {
				g.Expect(entries).To(BeEmpty())
			} else {
				g.Expect(counts["reconcile"]).To(Equal(1))
				g.Expect(counts["replica_observation"]).To(BeNumerically(">", 0))
				want := 1
				if mode == "failed projection" {
					want = 0
				}
				g.Expect(counts["observability"]).To(Equal(want))
			}
			expectMysqlSafeLogs(t, entries)
			if mode == "debug" {
				noopCtx, noopCapture := newMysqlLogContext(1)
				stored := phase4StoredCluster(t, r, cluster)
				changed, err := r.reconcileMysqlObservability(noopCtx, stored, true)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(changed).To(BeFalse())
				for _, entry := range noopCapture.snapshot() {
					g.Expect(entry.Values["operation"]).NotTo(Equal("observability"))
				}
				g.Expect(memory.statusPatchCount).To(Equal(1))
			}
		})
	}
}

type mysqlLoggingListErrorClient struct{ client.Client }

func (c *mysqlLoggingListErrorClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("SENSITIVE-SQL SENSITIVE-STDERR")
}

func TestMysqlLogPhase5BarrierEntry(t *testing.T) {
	for _, operation := range []string{"fencing", "election", "promotion", "rejoin"} {
		for _, level := range []int{0, 1} {
			t.Run(operation+string(rune('0'+level)), func(t *testing.T) {
				g := NewWithT(t)
				ctx, capture := newMysqlLogContext(level)
				cluster := mysqlSensitiveLogCluster()
				cluster.Status.HA.Failover.Stage = databasev1.MysqlClusterFailoverStageFencing
				r := phase1HReconciler(t, cluster)
				memory := r.Client.(*statefulSetReconcileMemoryClient)
				r.Client = &mysqlLoggingListErrorClient{Client: r.Client}
				r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
					t.Fatal("barrier logging must not execute SQL")
					return "", nil
				}
				functions := map[string]func(context.Context, *databasev1.MysqlCluster) (ctrl.Result, bool, error){"fencing": r.reconcileMysqlFailoverFencing, "election": r.reconcileMysqlGTIDElection, "promotion": r.reconcileMysqlCandidateTakeover, "rejoin": r.reconcileMysqlReconfiguringReplicas}
				result, complete, err := functions[operation](ctx, cluster)
				g.Expect(err).To(HaveOccurred())
				g.Expect(complete).To(BeFalse())
				g.Expect(result).To(Equal(ctrl.Result{}))
				g.Expect(memory.statusPatchCount).To(BeZero())
				g.Expect(memory.updateCount).To(BeZero())
				entries := capture.snapshot()
				if level == 0 {
					g.Expect(entries).To(BeEmpty())
				} else {
					g.Expect(entries).To(HaveLen(1))
					g.Expect(entries[0].Level).To(Equal(1))
					g.Expect(entries[0].Values).To(HaveKeyWithValue("operation", operation))
					g.Expect(entries[0].Values).To(HaveKeyWithValue("candidate", "mysql-2"))
					g.Expect(entries[0].Values).To(HaveKeyWithValue("failed_primary", "mysql-1"))
				}
				expectMysqlSafeLogs(t, entries)
			})
		}
	}
}

func TestMysqlLogSnapshotRedactionAndInventoryLevels(t *testing.T) {
	for _, level := range []int{0, 1} {
		t.Run(string(rune('0'+level)), func(t *testing.T) {
			g := NewWithT(t)
			ctx, capture := newMysqlLogContext(level)
			cluster := mysqlSensitiveLogCluster()
			sts := phase1HStatefulSet(t, cluster)
			pod := phase1HPod(t, cluster, sts, 1, "master", true)
			pod.UID = "SENSITIVE-POD-UID"
			r := phase1HReconciler(t, cluster, sts, pod)
			r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { return "SENSITIVE-GTID", nil }
			g.Expect(r.updateMasterGTIDSnapshotFromPod(ctx, pod, cluster)).To(Succeed())
			g.Expect(r.MasterGTIDSnapshot).To(Equal("SENSITIVE-GTID"))
			logMysqlGTIDSnapshotRefresh(ctx, true)
			r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
				return "SENSITIVE-SQL", &mysqlPodCommandExecutionError{cause: errors.New("SENSITIVE-ROOT-PASSWORD SENSITIVE-REPLICATION-PASSWORD"), stderr: "SENSITIVE-STDERR"}
			}
			g.Expect(r.updateMasterGTIDSnapshotFromPod(ctx, pod, cluster)).NotTo(Succeed())
			logMysqlGTIDSnapshotRefresh(ctx, false)
			count, names := r.getActualReplicaInfo(ctx, *cluster)
			g.Expect(count).To(Equal(int32(1)))
			g.Expect(names).To(Equal([]string{pod.Name}))
			entries := capture.snapshot()
			if level == 0 {
				g.Expect(entries).To(BeEmpty())
			} else {
				g.Expect(entries).To(HaveLen(3))
				g.Expect(entries[0].Message).To(Equal("primary GTID snapshot refreshed"))
				g.Expect(entries[1].Values).To(HaveKeyWithValue("reason", "mysql_observation_failed"))
				g.Expect(entries[2].Message).To(Equal("replica inventory observed"))
				g.Expect(entries[2].Values).To(HaveKeyWithValue("replicas", []string{pod.Name}))
				for _, entry := range entries {
					g.Expect(entry.Level).To(Equal(1))
				}
			}
			expectMysqlSafeLogs(t, entries)
		})
	}
	// No explicit logger is required by runtime helpers.
	logMysqlGTIDSnapshotRefresh(context.Background(), true)
	logMysqlControlBarrier(context.Background(), "fencing", &databasev1.MysqlCluster{})
}

func TestMysqlRuntimeLogSourceAudit(t *testing.T) {
	files, err := os.ReadDir(".")
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".go") || strings.HasSuffix(file.Name(), "_test.go") {
			continue
		}
		source, err := parser.ParseFile(token.NewFileSet(), file.Name(), nil, 0)
		NewWithT(t).Expect(err).NotTo(HaveOccurred())
		ast.Inspect(source, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name == "Printf" || selector.Sel.Name == "Println" {
				if receiver, ok := selector.X.(*ast.Ident); ok && (receiver.Name == "fmt" || receiver.Name == "log") {
					t.Errorf("unstructured runtime log in %s", file.Name())
				}
			}
			if selector.Sel.Name == "Info" || selector.Sel.Name == "Error" {
				for _, arg := range call.Args {
					ast.Inspect(arg, func(value ast.Node) bool {
						if field, ok := value.(*ast.SelectorExpr); ok && field.Sel.Name == "MasterGTIDSnapshot" {
							t.Errorf("snapshot passed to logger in %s", file.Name())
						}
						return true
					})
				}
			}
			return true
		})
	}
}

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func stablePDBTestCluster() *databasev1.MysqlCluster {
	c := statefulSetResourceTestCluster("pdb", "cluster-uid")
	c.Status.LastConvergedReplicas = replicaCountCopy(3)
	c.Status.LastConvergedImage = c.Spec.Image
	c.Status.HA = &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateHealthy}
	return c
}

// Existing control-barrier tests can start with an already converged PDB.
// Missing/drifted PDB ordering is exercised separately through main Reconcile.
func prepareMysqlPDBForControlTest(t *testing.T, r *MysqlClusterReconciler, cluster *databasev1.MysqlCluster) {
	t.Helper()
	_, err := r.reconcileMysqlPodDisruptionBudget(context.Background(), cluster)
	NewWithT(t).Expect(err).NotTo(HaveOccurred())
}

type pdbBarrierTrackingClient struct {
	client.Client
	pdbMutated bool
}

func (c *pdbBarrierTrackingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	err := c.Client.Create(ctx, obj, opts...)
	if _, ok := obj.(*policyv1.PodDisruptionBudget); ok && err == nil {
		c.pdbMutated = true
	}
	return err
}

func (c *pdbBarrierTrackingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	err := c.Client.Update(ctx, obj, opts...)
	if _, ok := obj.(*policyv1.PodDisruptionBudget); ok && err == nil {
		c.pdbMutated = true
	}
	return err
}

func TestMysqlPodDisruptionBudgetMatrix(t *testing.T) {
	cases := []struct {
		name string
		edit func(*databasev1.MysqlCluster)
		want int32
	}{
		{"stable-three", func(c *databasev1.MysqlCluster) {}, 1},
		{"stable-two", func(c *databasev1.MysqlCluster) {
			c.Spec.Replicas = replicaCountCopy(2)
			c.Status.LastConvergedReplicas = replicaCountCopy(2)
		}, 1},
		{"stable-one", func(c *databasev1.MysqlCluster) {
			c.Spec.Replicas = replicaCountCopy(1)
			c.Status.LastConvergedReplicas = replicaCountCopy(1)
		}, 0},
		{"unknown-replicas", func(c *databasev1.MysqlCluster) { c.Status.LastConvergedReplicas = nil }, 0},
		{"replica-intent", func(c *databasev1.MysqlCluster) { c.Status.LastConvergedReplicas = replicaCountCopy(1) }, 0},
		{"replica-transition", func(c *databasev1.MysqlCluster) {
			c.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{}
		}, 0},
		{"unknown-image", func(c *databasev1.MysqlCluster) { c.Status.LastConvergedImage = "" }, 0},
		{"image-intent", func(c *databasev1.MysqlCluster) { c.Spec.Image = "mysql:new" }, 0},
		{"active-upgrade", func(c *databasev1.MysqlCluster) { c.Status.Upgrade = &databasev1.MysqlClusterUpgradeStatus{} }, 0},
		{"unknown-ha", func(c *databasev1.MysqlCluster) { c.Status.HA = nil }, 0},
		{"healthy-with-failover", func(c *databasev1.MysqlCluster) { c.Status.HA.Failover = &databasev1.MysqlClusterFailoverStatus{} }, 0},
	}
	for _, state := range []databasev1.MysqlClusterHAState{"", "Unknown", databasev1.MysqlClusterHAStateSuspected, databasev1.MysqlClusterHAStateFailoverRequired, databasev1.MysqlClusterHAStateFailoverInProgress, databasev1.MysqlClusterHAStateVerifying, databasev1.MysqlClusterHAStateDegraded} {
		state := state
		cases = append(cases, struct {
			name string
			edit func(*databasev1.MysqlCluster)
			want int32
		}{
			"ha-" + string(state), func(c *databasev1.MysqlCluster) { c.Status.HA.State = state }, 0,
		})
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			c := stablePDBTestCluster()
			tt.edit(c)
			g.Expect(mysqlPodDisruptionBudgetMaxUnavailable(c)).To(Equal(tt.want))
			pdb := desiredMysqlPodDisruptionBudget(c)
			g.Expect(pdb.Spec.MinAvailable).To(BeNil())
			g.Expect(pdb.Spec.MaxUnavailable).To(Equal(&intstr.IntOrString{Type: intstr.Int, IntVal: tt.want}))
			g.Expect(pdb.Spec.UnhealthyPodEvictionPolicy).To(HaveValue(Equal(policyv1.IfHealthyBudget)))
		})
	}
}

func TestMysqlPodDisruptionBudgetIdentityAndSelector(t *testing.T) {
	g := NewWithT(t)
	c := stablePDBTestCluster()
	pdb := desiredMysqlPodDisruptionBudget(c)
	g.Expect(pdb.Name).To(Equal("pdb-mysql-pdb"))
	g.Expect(pdb.Namespace).To(Equal(c.Namespace))
	g.Expect(pdb.Labels).To(Equal(mysqlIdentityLabels(c)))
	g.Expect(pdb.Spec.Selector).To(Equal(&metav1.LabelSelector{MatchLabels: mysqlStatefulSetSelectorLabels(c)}))
	g.Expect(pdb.Spec.Selector.MatchLabels).NotTo(HaveKey(LabelMysqlRole))
	g.Expect(pdb.Spec.Selector.MatchLabels).NotTo(HaveKey(LegacyLabelRole))
	selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
	g.Expect(err).NotTo(HaveOccurred())
	for _, role := range []string{"master", "slave", ""} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: mysqlRoleLabels(c, role)}}
		g.Expect(selector.Matches(labels.Set(pod.Labels))).To(BeTrue())
	}
	other := c.DeepCopy()
	other.UID = "other-cluster-uid"
	g.Expect(selector.Matches(labels.Set(mysqlRoleLabels(other, "master")))).To(BeFalse())
	c.Name = strings.Repeat("a", 253)
	g.Expect(mysqlPodDisruptionBudgetName(c)).To(Equal(boundedMysqlChildName(c.Name, "-mysql-pdb")))
	g.Expect(len(mysqlPodDisruptionBudgetName(c))).To(Equal(253))
}

// Record every control mutation, including subresources, so a PDB barrier
// cannot silently fall through into lifecycle/status/Pod/SQL work.
type pdbRecordingClient struct {
	client.Client
	writes       []string
	fail         string
	failure      error
	updateObject *policyv1.PodDisruptionBudget
}

func (c *pdbRecordingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*policyv1.PodDisruptionBudget); ok && c.fail == "get" {
		return c.failure
	}
	return c.Client.Get(ctx, key, obj, opts...)
}
func (c *pdbRecordingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	c.writes = append(c.writes, fmt.Sprintf("create %T", obj))
	if c.fail == "create" {
		return c.failure
	}
	return c.Client.Create(ctx, obj, opts...)
}
func (c *pdbRecordingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	c.writes = append(c.writes, fmt.Sprintf("update %T", obj))
	if pdb, ok := obj.(*policyv1.PodDisruptionBudget); ok {
		c.updateObject = pdb.DeepCopy()
	}
	if c.fail == "update" {
		return c.failure
	}
	return c.Client.Update(ctx, obj, opts...)
}
func (c *pdbRecordingClient) Delete(context.Context, client.Object, ...client.DeleteOption) error {
	c.writes = append(c.writes, "delete")
	return errors.New("unexpected delete")
}
func (c *pdbRecordingClient) DeleteAllOf(context.Context, client.Object, ...client.DeleteAllOfOption) error {
	c.writes = append(c.writes, "delete-all")
	return errors.New("unexpected delete-all")
}
func (c *pdbRecordingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.writes = append(c.writes, fmt.Sprintf("patch %T", obj))
	return c.Client.Patch(ctx, obj, patch, opts...)
}

type pdbRecordingSubresource struct {
	client.SubResourceWriter
	recorder *pdbRecordingClient
}

func (c *pdbRecordingClient) Status() client.SubResourceWriter {
	return &pdbRecordingSubresource{SubResourceWriter: c.Client.Status(), recorder: c}
}
func (c *pdbRecordingClient) SubResource(name string) client.SubResourceClient {
	c.writes = append(c.writes, "subresource "+name)
	return c.Client.SubResource(name)
}
func (w *pdbRecordingSubresource) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	w.recorder.writes = append(w.recorder.writes, "status patch")
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}
func (w *pdbRecordingSubresource) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	w.recorder.writes = append(w.recorder.writes, "status update")
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}
func (w *pdbRecordingSubresource) Create(ctx context.Context, obj, sub client.Object, opts ...client.SubResourceCreateOption) error {
	w.recorder.writes = append(w.recorder.writes, "status create")
	return w.SubResourceWriter.Create(ctx, obj, sub, opts...)
}

func TestMysqlPodDisruptionBudgetLifecycle(t *testing.T) {
	for _, scenario := range []string{"missing", "stable", "empty-selector-expressions", "labels", "selector", "min-available", "budget", "percentage", "unhealthy-policy", "nil-policy", "foreign", "unowned", "get", "create", "update"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			cluster := stablePDBTestCluster()
			scheme := newStatefulSetReconcileTestScheme(t)
			pdb := desiredMysqlPodDisruptionBudget(cluster)
			setControllerReferenceForTest(t, scheme, cluster, pdb)
			pdb.ResourceVersion = "42"
			pdb.Annotations = map[string]string{"external": "preserved"}
			pdb.UID = "pdb-uid"
			pdb.Finalizers = []string{"external.example/finalizer"}
			pdb.Status = policyv1.PodDisruptionBudgetStatus{CurrentHealthy: 3, DisruptionsAllowed: 1}
			switch scenario {
			case "labels":
				pdb.Labels = map[string]string{"foreign": "label"}
			case "selector":
				pdb.Spec.Selector.MatchLabels[LabelMysqlRole] = "master"
			case "empty-selector-expressions":
				pdb.Spec.Selector.MatchExpressions = []metav1.LabelSelectorRequirement{}
			case "min-available":
				pdb.Spec.MinAvailable = &intstr.IntOrString{IntVal: 2}
				pdb.Spec.MaxUnavailable = nil
			case "budget", "update":
				pdb.Spec.MaxUnavailable = &intstr.IntOrString{IntVal: 0}
			case "percentage":
				v := intstr.FromString("1%")
				pdb.Spec.MaxUnavailable = &v
			case "unhealthy-policy":
				v := policyv1.AlwaysAllow
				pdb.Spec.UnhealthyPodEvictionPolicy = &v
			case "nil-policy":
				pdb.Spec.UnhealthyPodEvictionPolicy = nil
			case "foreign":
				pdb.OwnerReferences[0].UID = "foreign"
			case "unowned":
				pdb.OwnerReferences = nil
			}
			memory := newStatefulSetReconcileMemoryClient()
			if scenario != "missing" && scenario != "create" {
				memory = newStatefulSetReconcileMemoryClient(pdb)
			}
			c := &pdbRecordingClient{Client: memory, fail: scenario, failure: errors.New("injected " + scenario)}
			r := &MysqlClusterReconciler{Client: c, Scheme: scheme}
			changed, err := r.reconcileMysqlPodDisruptionBudget(ctx, cluster)
			if scenario == "foreign" || scenario == "unowned" || scenario == "get" || scenario == "create" || scenario == "update" {
				g.Expect(err).To(HaveOccurred())
				g.Expect(changed).To(BeFalse())
				if scenario == "foreign" || scenario == "unowned" || scenario == "get" {
					g.Expect(c.writes).To(BeEmpty())
				} else {
					g.Expect(c.writes).To(HaveLen(1))
				}
				if scenario == "get" || scenario == "create" || scenario == "update" {
					g.Expect(errors.Is(err, c.failure)).To(BeTrue())
				}
				if scenario != "get" && scenario != "create" {
					stored := &policyv1.PodDisruptionBudget{}
					g.Expect(memory.Get(ctx, client.ObjectKeyFromObject(pdb), stored)).To(Succeed())
					g.Expect(stored).To(Equal(pdb))
				}
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(changed).To(Equal(scenario != "stable" && scenario != "empty-selector-expressions"))
			if changed {
				g.Expect(c.writes).To(HaveLen(1))
			} else {
				g.Expect(c.writes).To(BeEmpty())
			}
			stored := &policyv1.PodDisruptionBudget{}
			g.Expect(memory.Get(ctx, client.ObjectKeyFromObject(pdb), stored)).To(Succeed())
			g.Expect(metav1.IsControlledBy(stored, cluster)).To(BeTrue())
			g.Expect(stored.Labels).To(Equal(mysqlIdentityLabels(cluster)))
			g.Expect(stored.Spec.MaxUnavailable).To(Equal(desiredMysqlPodDisruptionBudget(cluster).Spec.MaxUnavailable))
			if c.updateObject != nil {
				want := pdb.DeepCopy()
				want.Labels = desiredMysqlPodDisruptionBudget(cluster).Labels
				want.Spec = desiredMysqlPodDisruptionBudget(cluster).Spec
				g.Expect(c.updateObject).To(Equal(want), "preserve status, resourceVersion and unowned metadata in update payload")
			}
			c.writes = nil
			changed, err = r.reconcileMysqlPodDisruptionBudget(ctx, cluster)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(changed).To(BeFalse())
			g.Expect(c.writes).To(BeEmpty())
		})
	}
}

func TestMysqlPodDisruptionBudgetReconcileBarrier(t *testing.T) {
	for _, scenario := range []string{"missing", "drift", "get", "create", "update", "foreign"} {
		t.Run(scenario, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			cluster := phase1HCluster("pdb-barrier", false)
			cluster.Status.LastConvergedReplicas = nil
			cluster.Status.LastConvergedImage = ""
			memory := newStatefulSetReconcileMemoryClient(cluster)
			c := &pdbRecordingClient{Client: memory, fail: scenario, failure: errors.New("injected")}
			r := &MysqlClusterReconciler{Client: c, Scheme: newStatefulSetReconcileTestScheme(t), SnapGoIsEnabled: true}
			sql := 0
			r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { sql++; return "", errors.New("unexpected SQL") }
			if scenario == "drift" || scenario == "update" || scenario == "foreign" {
				pdb := desiredMysqlPodDisruptionBudget(cluster)
				setControllerReferenceForTest(t, r.Scheme, cluster, pdb)
				pdb.Spec.MaxUnavailable = &intstr.IntOrString{IntVal: 1}
				if scenario == "foreign" {
					pdb.OwnerReferences[0].UID = "foreign"
				}
				g.Expect(memory.Create(ctx, pdb)).To(Succeed())
			}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
			result, err := r.Reconcile(ctx, req)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(result).To(Equal(ctrl.Result{Requeue: true}))
			g.Expect(c.writes).To(Equal([]string{"status patch"}), "observability projection must precede PDB")
			before := phase4StoredCluster(t, r, cluster)
			c.writes = nil
			result, err = r.Reconcile(ctx, req)
			if scenario == "missing" || scenario == "drift" {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(result).To(Equal(ctrl.Result{Requeue: true}))
			} else {
				g.Expect(err).To(HaveOccurred())
			}
			switch scenario {
			case "missing", "create":
				g.Expect(c.writes).To(Equal([]string{"create *v1.PodDisruptionBudget"}))
			case "drift", "update":
				g.Expect(c.writes).To(Equal([]string{"update *v1.PodDisruptionBudget"}))
			default:
				g.Expect(c.writes).To(BeEmpty())
			}
			g.Expect(sql).To(BeZero())
			g.Expect(phase4StoredCluster(t, r, cluster).Status).To(Equal(before.Status))
		})
	}
}

func TestMysqlPodDisruptionBudgetPreIntentTightening(t *testing.T) {
	t.Run("scale-one-to-two", func(t *testing.T) {
		g := NewWithT(t)
		c := stablePDBTestCluster()
		c.Spec.Replicas = replicaCountCopy(1)
		c.Status.LastConvergedReplicas = replicaCountCopy(1)
		r := newStatefulSetReconcileTestReconciler(t, newStatefulSetReconcileTestScheme(t))
		changed, err := r.reconcileMysqlPodDisruptionBudget(context.Background(), c)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(changed).To(BeTrue())
		c.Spec.Replicas = replicaCountCopy(2)
		g.Expect(c.Status.ReplicaTransition).To(BeNil())
		changed, err = r.reconcileMysqlPodDisruptionBudget(context.Background(), c)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(changed).To(BeFalse())
		pdb := &policyv1.PodDisruptionBudget{}
		getObjectForTest(t, r, client.ObjectKey{Namespace: c.Namespace, Name: mysqlPodDisruptionBudgetName(c)}, pdb)
		g.Expect(pdb.Spec.MaxUnavailable).To(HaveValue(Equal(intstr.FromInt32(0))))
	})
	t.Run("image-before-upgrade-status", func(t *testing.T) {
		g := NewWithT(t)
		ctx := context.Background()
		r, memory, cluster := newUpgradeTest(t)
		changed, err := r.reconcileMysqlPodDisruptionBudget(ctx, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(changed).To(BeTrue())
		pdb := &policyv1.PodDisruptionBudget{}
		key := client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlPodDisruptionBudgetName(cluster)}
		getObjectForTest(t, r, key, pdb)
		g.Expect(pdb.Spec.MaxUnavailable).To(HaveValue(Equal(intstr.FromInt32(1))))
		cluster.Spec.Image = "mysql:new"
		storeUpgradeTestCluster(t, memory, cluster)
		recorder := &pdbRecordingClient{Client: r.Client}
		r.Client = recorder
		sql := 0
		r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) { sql++; return "", errors.New("unexpected SQL") }
		req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
		_, err = r.Reconcile(ctx, req)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(recorder.writes).To(Equal([]string{"status patch"}))
		recorder.writes = nil
		result, err := r.Reconcile(ctx, req)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result).To(Equal(ctrl.Result{Requeue: true}))
		g.Expect(recorder.writes).To(Equal([]string{"update *v1.PodDisruptionBudget"}))
		g.Expect(phase4StoredCluster(t, r, cluster).Status.Upgrade).To(BeNil())
		getObjectForTest(t, r, key, pdb)
		g.Expect(pdb.Spec.MaxUnavailable).To(HaveValue(Equal(intstr.FromInt32(0))))
		g.Expect(sql).To(BeZero())
		g.Expect(memory.templateWrites).To(BeZero())
		recorder.writes = nil
		_, err = r.Reconcile(ctx, req)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(recorder.writes).To(Equal([]string{"status patch"}), "stable PDB permits the existing upgrade-intent barrier")
		g.Expect(phase4StoredCluster(t, r, cluster).Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStagePreparing))
		g.Expect(sql).To(BeZero())
		g.Expect(memory.templateWrites).To(BeZero())
	})
}

package controller

import (
	"context"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Envtest verifies the API-server contract, not kube-controller-manager's
// disruptionsAllowed calculation or actual Eviction API enforcement.
var _ = Describe("Phase 7-D PodDisruptionBudget API contract", func() {
	It("round-trips budgets and ownership, repairs drift, stays idempotent and recreates after deletion", func() {
		ctx := context.Background()
		cluster := validMysqlClusterForAdmission("p7d-api")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() { cleanupMysqlClusterForAdmission(ctx, cluster) })
		r := &MysqlClusterReconciler{Client: k8sClient, Scheme: scheme.Scheme}
		key := client.ObjectKey{Namespace: cluster.Namespace, Name: mysqlPodDisruptionBudgetName(cluster)}
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}))).To(Succeed())
		})
		read := func() *policyv1.PodDisruptionBudget {
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, key, pdb)).To(Succeed())
			return pdb
		}
		reconcile := func(wantChanged bool) {
			changed, err := r.reconcileMysqlPodDisruptionBudget(ctx, cluster)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(Equal(wantChanged))
		}
		assertContract := func(pdb *policyv1.PodDisruptionBudget, budget int32) {
			// Typed decoding clears TypeMeta; inspect the wire GVK separately.
			wire := &unstructured.Unstructured{}
			wire.SetGroupVersionKind(policyv1.SchemeGroupVersion.WithKind("PodDisruptionBudget"))
			Expect(k8sClient.Get(ctx, key, wire)).To(Succeed())
			Expect(wire.GetAPIVersion()).To(Equal("policy/v1"))
			Expect(wire.GetKind()).To(Equal("PodDisruptionBudget"))
			expectMysqlClusterController(pdb, cluster)
			Expect(pdb.Labels).To(Equal(mysqlIdentityLabels(cluster)))
			Expect(pdb.Spec.Selector).To(Equal(&metav1.LabelSelector{MatchLabels: mysqlStatefulSetSelectorLabels(cluster)}))
			Expect(pdb.Spec.MinAvailable).To(BeNil())
			Expect(pdb.Spec.MaxUnavailable).To(HaveValue(Equal(intstr.FromInt32(budget))))
			Expect(pdb.Spec.UnhealthyPodEvictionPolicy).To(HaveValue(Equal(policyv1.IfHealthyBudget)))
		}
		reconcile(true)
		assertContract(read(), 0)
		cluster.Status.LastConvergedReplicas = replicaCountCopy(desiredReplicas(cluster))
		cluster.Status.LastConvergedImage = cluster.Spec.Image
		cluster.Status.HA = &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateHealthy}
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
		reconcile(true)
		pdb := read()
		assertContract(pdb, 1)
		for i := 0; i < 3; i++ {
			reconcile(false)
			Expect(read().ResourceVersion).To(Equal(pdb.ResourceVersion))
		}
		pdb.Annotations = map[string]string{"external.example/note": "preserve"}
		pdb.Labels = map[string]string{"drift": "true"}
		pdb.Spec.Selector.MatchLabels[LabelMysqlRole] = "master"
		pdb.Spec.MaxUnavailable = nil
		minimum := intstr.FromInt32(2)
		pdb.Spec.MinAvailable = &minimum
		allow := policyv1.AlwaysAllow
		pdb.Spec.UnhealthyPodEvictionPolicy = &allow
		Expect(k8sClient.Update(ctx, pdb)).To(Succeed())
		before := read()
		reconcile(true)
		pdb = read()
		assertContract(pdb, 1)
		Expect(pdb.Annotations).To(Equal(before.Annotations))
		Expect(pdb.UID).To(Equal(before.UID))
		Expect(pdb.OwnerReferences).To(Equal(before.OwnerReferences))
		Expect(pdb.Status).To(Equal(before.Status))
		reconcile(false)
		Expect(read().ResourceVersion).To(Equal(pdb.ResourceVersion))
		// API omission/default handling must converge to an explicit policy.
		pdb.Spec.UnhealthyPodEvictionPolicy = nil
		Expect(k8sClient.Update(ctx, pdb)).To(Succeed())
		_, err := r.reconcileMysqlPodDisruptionBudget(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		pdb = read()
		assertContract(pdb, 1)
		reconcile(false)
		Expect(read().ResourceVersion).To(Equal(pdb.ResourceVersion))
		uid := pdb.UID
		Expect(k8sClient.Delete(ctx, pdb)).To(Succeed())
		Eventually(func() bool { return apierrors.IsNotFound(k8sClient.Get(ctx, key, &policyv1.PodDisruptionBudget{})) }).Should(BeTrue())
		reconcile(true)
		pdb = read()
		Expect(pdb.UID).NotTo(Equal(uid))
		assertContract(pdb, 1)
		reconcile(false)
		Expect(read().ResourceVersion).To(Equal(pdb.ResourceVersion))
	})

	It("rejects a foreign same-name PDB without mutation or adoption", func() {
		ctx := context.Background()
		cluster := validMysqlClusterForAdmission("p7d-foreign")
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(func() { cleanupMysqlClusterForAdmission(ctx, cluster) })
		foreign := desiredMysqlPodDisruptionBudget(cluster)
		foreign.OwnerReferences = []metav1.OwnerReference{{APIVersion: databasev1.GroupVersion.String(), Kind: "MysqlCluster", Name: "another", UID: "another-uid", Controller: boolPtrForPDBTest(true)}}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, foreign)).To(Succeed()) })
		before := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreign), before)).To(Succeed())
		r := &MysqlClusterReconciler{Client: k8sClient, Scheme: scheme.Scheme}
		changed, err := r.reconcileMysqlPodDisruptionBudget(ctx, cluster)
		Expect(err).To(MatchError(ContainSubstring("not controlled")))
		Expect(changed).To(BeFalse())
		after := &policyv1.PodDisruptionBudget{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(foreign), after)).To(Succeed())
		Expect(after).To(Equal(before))
	})
})

func boolPtrForPDBTest(value bool) *bool { return &value }

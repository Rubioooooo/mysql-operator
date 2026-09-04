package controller

import (
	"context"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

var _ = Describe("Observability status isolated API-server integration", func() {
	ctx := context.Background()

	It("round-trips the existing schema, preserves durable state and avoids no-op writes", func() {
		cluster := configureStatefulSetRuntimeCluster(ctx, "p6-status", "mysql-status", 3, true)
		deferStatefulSetRuntimeResourceCleanup(cluster)
		sts := createStatefulSetRuntimeWorkload(ctx, cluster, 3)
		old := createStatefulSetRuntimePod(ctx, cluster, sts, 1, "master")
		candidate := createStatefulSetRuntimePod(ctx, cluster, sts, 2, "slave")
		createStatefulSetRuntimePod(ctx, cluster, sts, 3, "slave")
		cluster.Status.LastConvergedReplicas = replicaCountCopy(2)
		cluster.Status.ReplicaTransition = &databasev1.MysqlClusterReplicaTransitionStatus{FromReplicas: 2, TargetReplicas: 3}
		cluster.Status.CredentialsSecretUID = "accepted-secret-uid"
		cluster.Status.Master, cluster.Status.Slaves = "preserved", []string{"preserved-replica"}
		cluster.Status.HA = phase5FencingHA(old, databasev1.MysqlClusterFenceStateVerified)
		proof := "server:1-5"
		fo := cluster.Status.HA.Failover
		fo.Stage = databasev1.MysqlClusterFailoverStagePromoting
		fo.Candidate, fo.CandidateUID = candidate.Name, string(candidate.UID)
		fo.FailedPrimaryServerUUID, fo.FailedPrimaryGTIDSet = "server", &proof
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
		before := cluster.DeepCopy()
		r := statefulSetEnvtestReconciler()
		changed, err := r.reconcileMysqlObservability(ctx, cluster, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())
		stored := &databasev1.MysqlCluster{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
		Expect(stored.Status.Primary).To(BeEmpty())
		Expect(stored.Status.Phase).To(Equal(databasev1.MysqlClusterPhaseDegraded))
		Expect(stored.Status.CurrentReplicas).To(Equal(int32(3)))
		Expect(stored.Status.ReadyReplicas).To(Equal(int32(3)))
		Expect(stored.Status.Conditions).To(HaveLen(3))
		Expect(meta.IsStatusConditionFalse(stored.Status.Conditions, mysqlConditionAvailable)).To(BeTrue())
		Expect(stored.Status.ObservedGeneration).To(Equal(stored.Generation))
		Expect(stored.Status.HA).To(Equal(before.Status.HA))
		Expect(stored.Status.ReplicaTransition).To(Equal(before.Status.ReplicaTransition))
		Expect(stored.Status.LastConvergedReplicas).To(Equal(before.Status.LastConvergedReplicas))
		Expect(stored.Status.CredentialsSecretUID).To(Equal(before.Status.CredentialsSecretUID))
		Expect(stored.Status.Master).To(Equal(before.Status.Master))
		Expect(stored.Status.Slaves).To(Equal(before.Status.Slaves))
		version := stored.ResourceVersion
		changed, err = r.reconcileMysqlObservability(ctx, stored, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeFalse())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
		Expect(stored.ResourceVersion).To(Equal(version))
		// Simulate the existing publication boundary using Kubernetes objects;
		// envtest never executes MySQL commands.
		delete(old.Labels, LabelMysqlRole)
		delete(old.Labels, LegacyLabelRole)
		Expect(k8sClient.Update(ctx, old)).To(Succeed())
		candidate.Labels[LabelMysqlRole], candidate.Labels[LegacyLabelRole] = "master", "master"
		Expect(k8sClient.Update(ctx, candidate)).To(Succeed())
		changed, err = r.reconcileMysqlObservability(ctx, stored, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
		Expect(stored.Status.Primary).To(Equal(candidate.Name))
		Expect(meta.IsStatusConditionFalse(stored.Status.Conditions, mysqlConditionAvailable)).To(BeTrue())
		// A delayed endpoint still pointing to the old primary is not proof
		// that the newly published candidate is available through the Service.
		endpoints := phase1HEndpoints(cluster, old)
		endpoints.Subsets[0].Addresses[0].IP = "10.0.0.1"
		endpoints.Subsets[0].Ports = []corev1.EndpointPort{{Port: 3306}}
		Expect(k8sClient.Create(ctx, endpoints)).To(Succeed())
		DeferCleanup(func() { cleanupStatefulSetEnvtestObject(ctx, endpoints) })
		changed, err = r.reconcileMysqlObservability(ctx, stored, true)
		Expect(err).NotTo(HaveOccurred())
		Expect(changed).To(BeFalse())
		oldEndpoints := endpoints.DeepCopy()
		endpoints.Subsets[0].Addresses[0].TargetRef.Name = candidate.Name
		endpoints.Subsets[0].Addresses[0].TargetRef.UID = candidate.UID
		endpoints.Subsets[0].Addresses[0].IP = "10.0.0.2"
		Expect(k8sClient.Update(ctx, endpoints)).To(Succeed())
		// Deliver the API update through the same map handler used by
		// SetupWithManager, then execute its queued request. This tests the
		// handler/queue/Reconcile boundary without starting a manager that
		// would also run unrelated lifecycle and SQL control iterations.
		queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
		defer queue.ShutDown()
		endpointHandler := handler.EnqueueRequestsFromMapFunc(r.mapMysqlPrimaryEndpointsToMysqlClusters)
		endpointHandler.Update(ctx, event.UpdateEvent{ObjectOld: oldEndpoints, ObjectNew: endpoints}, queue)
		Expect(queue.Len()).To(Equal(1))
		item, shutdown := queue.Get()
		Expect(shutdown).To(BeFalse())
		request, ok := item.(ctrl.Request)
		Expect(ok).To(BeTrue())
		Expect(request.NamespacedName).To(Equal(client.ObjectKeyFromObject(cluster)))
		r.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
			Fail("endpoint-triggered projection must not execute SQL")
			return "", nil
		}
		result, err := r.Reconcile(ctx, request)
		queue.Done(item)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(ctrl.Result{Requeue: true}))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
		Expect(stored.Status.Primary).To(Equal(candidate.Name))
		Expect(meta.IsStatusConditionTrue(stored.Status.Conditions, mysqlConditionAvailable)).To(BeTrue())
		Expect(stored.Status.HA).To(Equal(before.Status.HA))
	})

	It("rejects stale generation and HA snapshots without overwriting concurrent durable changes", func() {
		r := statefulSetEnvtestReconciler()
		for _, concurrentChange := range []string{"generation", "ha"} {
			cluster := configureStatefulSetRuntimeCluster(ctx, "p6-conflict-"+concurrentChange, "mysql-conflict", 1, false)
			deferStatefulSetRuntimeResourceCleanup(cluster)
			stale := cluster.DeepCopy()
			if concurrentChange == "generation" {
				cluster.Spec.Replicas = replicaCountCopy(2)
				Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
			} else {
				cluster.Status.HA = &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateDegraded, Primary: "tracked", PrimaryUID: "tracked-uid"}
				Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
			}
			changed, err := r.reconcileMysqlObservability(ctx, stale, false)
			Expect(apierrors.IsConflict(err)).To(BeTrue(), "concurrent %s update must conflict: %v", concurrentChange, err)
			Expect(changed).To(BeFalse())
			current := &databasev1.MysqlCluster{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), current)).To(Succeed())
			Expect(current.Status).To(Equal(cluster.Status))
			changed, err = r.reconcileMysqlObservability(ctx, current, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(changed).To(BeTrue())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
			Expect(cluster.Status.ObservedGeneration).To(Equal(cluster.Generation))
			Expect(cluster.Status.Phase).To(Equal(databasev1.MysqlClusterPhaseInitializing))
			Expect(cluster.Status.Primary).To(BeEmpty())
			Expect(cluster.Status.Master).To(BeEmpty())
			Expect(cluster.Status.Slaves).To(BeEmpty())
			pods := &corev1.PodList{}
			Expect(k8sClient.List(ctx, pods, client.InNamespace(cluster.Namespace))).To(Succeed())
			Expect(pods.Items).To(BeEmpty())
		}
	})
})

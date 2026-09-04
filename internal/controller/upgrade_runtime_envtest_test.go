package controller

import (
	"context"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Phase 7-A isolated API-server barriers", func() {
	It("rejects a stale upgrade patch without changing memory or emitting an Event", func() {
		ctx := context.Background()
		cluster := configureStatefulSetRuntimeCluster(ctx, "p7-conflict", "mysql-upgrade", 3, true)
		cluster.Status.LastConvergedImage = "mysql:old"
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
		stale := cluster.DeepCopy()
		cluster.Spec.Image = "mysql:new"
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		r := statefulSetEnvtestReconciler()
		recorder := record.NewFakeRecorder(10)
		r.Recorder = recorder
		plan := &databasev1.MysqlClusterUpgradeStatus{FromImage: "mysql:old", TargetImage: "mysql:new", Stage: databasev1.MysqlClusterUpgradeStagePreparing}
		err := r.persistMysqlClusterUpgradeStatus(ctx, stale, "mysql:old", plan)
		Expect(apierrors.IsConflict(err)).To(BeTrue())
		Expect(stale.Status.Upgrade).To(BeNil())
		Expect(recorder.Events).To(BeEmpty())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
		Expect(cluster.Status.Upgrade).To(BeNil())
	})

	It("observes the defaulted target template on the next reconcile before persisting TemplateReady", func() {
		ctx := context.Background()
		cluster := configureStatefulSetRuntimeCluster(ctx, "p7-template", "mysql-upgrade", 3, true)
		deferStatefulSetRuntimeResourceCleanup(cluster)
		sts := createStatefulSetRuntimeWorkload(ctx, cluster, 3)
		primary := createStatefulSetRuntimePod(ctx, cluster, sts, 1, "master")
		createStatefulSetRuntimePod(ctx, cluster, sts, 2, "slave")
		createStatefulSetRuntimePod(ctx, cluster, sts, 3, "slave")
		endpoints := phase1HEndpoints(cluster, primary)
		endpoints.Subsets[0].Addresses[0].IP = "10.0.0.1"
		endpoints.Subsets[0].Ports = []corev1.EndpointPort{{Port: 3306}}
		Expect(k8sClient.Create(ctx, endpoints)).To(Succeed())
		DeferCleanup(func() { cleanupStatefulSetEnvtestObject(ctx, endpoints) })
		oldImage := cluster.Spec.Image
		cluster.Spec.Image = "mysql:8.0"
		Expect(k8sClient.Update(ctx, cluster)).To(Succeed())
		cluster.Status.LastConvergedImage = oldImage
		cluster.Status.LastConvergedReplicas = replicaCountCopy(3)
		cluster.Status.HA = phase4HAStatus(databasev1.MysqlClusterHAStateHealthy, primary)
		cluster.Status.Upgrade = &databasev1.MysqlClusterUpgradeStatus{FromImage: oldImage, TargetImage: cluster.Spec.Image, Stage: databasev1.MysqlClusterUpgradeStageTemplatePending}
		Expect(k8sClient.Status().Update(ctx, cluster)).To(Succeed())
		r := statefulSetEnvtestReconciler()
		r.execCommandOnPodFn = func(_ *corev1.Pod, command string) (string, error) {
			Expect(command).To(Equal(mysqlShowSlaveStatusCommand()))
			return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
		}
		version := cluster.ResourceVersion
		handled, err := r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)).To(Succeed())
		Expect(cluster.ResourceVersion).To(Equal(version))
		Expect(cluster.Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStageTemplatePending))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sts), sts)).To(Succeed())
		Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal("mysql:8.0"))
		Expect(sts.Spec.Template.Spec.InitContainers[0].Image).To(Equal("mysql:8.0"))
		Expect(sts.Spec.UpdateStrategy.Type).To(Equal(appsv1.OnDeleteStatefulSetStrategyType))
		version = sts.ResourceVersion
		handled, err = r.reconcileMysqlUpgradePreRuntime(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		Expect(handled).To(BeTrue())
		Expect(cluster.Status.Upgrade.Stage).To(Equal(databasev1.MysqlClusterUpgradeStageTemplateReady))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sts), sts)).To(Succeed())
		Expect(sts.ResourceVersion).To(Equal(version))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(primary), primary)).To(Succeed())
		Expect(primary.Spec.Containers[0].Image).To(Equal(oldImage))
		Expect(primary.DeletionTimestamp.IsZero()).To(BeTrue())
	})
})

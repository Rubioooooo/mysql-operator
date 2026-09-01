package controller

import (
	"context" // 用于处理上下文，提供超时、取消等操作
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1 "github.com/egonlin/api/v1" // 导入自定义的 MySQLCluster API 资源定义
	v1 "k8s.io/api/core/v1"                // 核心 Kubernetes API 对象，例如 Pod 和 Service
)

// getActualReplicaInfo remains a compatibility view used by the current HA
// member logic; StatefulSet reconciliation is authoritative for workload replicas.
func (r *MysqlClusterReconciler) getActualReplicaInfo(ctx context.Context, cluster databasev1.MysqlCluster) (int32, []string) {
	log := log.FromContext(ctx)

	// 创建一个 Pod 列表对象
	podList := &v1.PodList{}

	// 获取 Pod 列表
	if err := r.List(ctx, podList, mysqlClusterPodListOptions(&cluster, "")...); err != nil {
		log.Error(err, "获取 Pod 列表失败")
		return 0, nil
	}

	// 提取 Pod 名称
	var podNames []string
	for _, pod := range podList.Items {
		podNames = append(podNames, pod.Name)
	}

	log.Info("当前副本情况", "副本数", len(podList.Items), "PodNames", podNames, "预期副本数", desiredReplicas(&cluster))

	// 计算实际副本数
	return int32(len(podList.Items)), podNames
}

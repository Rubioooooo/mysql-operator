package controller

import (
	"context" // 用于处理上下文，提供超时、取消等操作
	"fmt"     // 格式化I/O函数，如字符串格式化和打印
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client" // 提供与 Kubernetes API 交互的客户端
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1 "github.com/egonlin/api/v1" // 导入自定义的 MySQLCluster API 资源定义
	v1 "k8s.io/api/core/v1"                // 核心 Kubernetes API 对象，例如 Pod 和 Service
	ctrl "sigs.k8s.io/controller-runtime"
)

// 主从调谐逻辑. The bool reports only whether the replica database
// domain is semantically converged; a successfully handled HA path returns false.
func (r *MysqlClusterReconciler) reconcileMasterSlave(ctx context.Context, cluster databasev1.MysqlCluster) (ctrl.Result, bool, error) {
	observation, err := r.observeMysqlPrimaryFailure(ctx, &cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}

	current := cluster.Status.HA
	switch observation.Classification {
	case mysqlPrimaryHealthy:
		if current != nil {
			switch current.State {
			case databasev1.MysqlClusterHAStateVerifying:
				if !mysqlHAIdentityMatches(current, observation) {
					verifying := &databasev1.MysqlClusterHAStatus{
						State:      databasev1.MysqlClusterHAStateVerifying,
						Primary:    observation.PrimaryName,
						PrimaryUID: observation.PrimaryUID,
					}
					if _, err := r.persistMysqlClusterHAStatus(ctx, &cluster, verifying); err != nil {
						return ctrl.Result{}, false, err
					}
					return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
				}

				result, converged, err := r.reconcileMysqlHealthyPrimaryRuntime(ctx, &cluster)
				if err != nil || !converged {
					return result, false, err
				}
				if _, err := r.persistMysqlClusterHAStatus(ctx, &cluster, mysqlHealthyHAStatus(observation)); err != nil {
					return ctrl.Result{}, false, err
				}
				return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil

			case databasev1.MysqlClusterHAStateFailoverInProgress:
				verifying := &databasev1.MysqlClusterHAStatus{
					State:      databasev1.MysqlClusterHAStateVerifying,
					Primary:    observation.PrimaryName,
					PrimaryUID: observation.PrimaryUID,
				}
				if _, err := r.persistMysqlClusterHAStatus(ctx, &cluster, verifying); err != nil {
					return ctrl.Result{}, false, err
				}
				return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
			}
		}

		desired := mysqlHealthyHAStatus(observation)
		_, err := r.persistMysqlClusterHAStatus(ctx, &cluster, desired)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		return r.reconcileMysqlHealthyPrimaryRuntime(ctx, &cluster)

	case mysqlPrimaryDegraded:
		if _, err := r.persistMysqlClusterHAStatus(ctx, &cluster, mysqlDegradedHAStatus(observation)); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil

	case mysqlPrimarySuspected:
		if mysqlHAIdentityMatches(current, observation) {
			switch current.State {
			case databasev1.MysqlClusterHAStateSuspected:
				if _, err := r.persistMysqlClusterHAStatus(
					ctx,
					&cluster,
					mysqlFailoverRequiredHAStatus(current, observation),
				); err != nil {
					return ctrl.Result{}, false, err
				}
				return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
			case databasev1.MysqlClusterHAStateFailoverRequired,
				databasev1.MysqlClusterHAStateFailoverInProgress:
				return r.executePersistedMysqlFailover(ctx, &cluster, observation)
			}
		}
		if _, err := r.persistMysqlClusterHAStatus(ctx, &cluster, mysqlSuspectedHAStatus(observation)); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil

	case mysqlPrimaryFailureConfirmed:
		if mysqlHAIdentityMatches(current, observation) &&
			(current.State == databasev1.MysqlClusterHAStateFailoverRequired ||
				current.State == databasev1.MysqlClusterHAStateFailoverInProgress) {
			return r.executePersistedMysqlFailover(ctx, &cluster, observation)
		}
		if _, err := r.persistMysqlClusterHAStatus(
			ctx,
			&cluster,
			mysqlFailoverRequiredHAStatus(current, observation),
		); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil

	default:
		return ctrl.Result{}, false, fmt.Errorf("unsupported primary failure classification %q", observation.Classification)
	}
}

func (r *MysqlClusterReconciler) executePersistedMysqlFailover(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	observation mysqlPrimaryFailureObservation,
) (ctrl.Result, bool, error) {
	if !mysqlHAIdentityMatches(cluster.Status.HA, observation) {
		return ctrl.Result{}, false, fmt.Errorf("refusing HA execution for a stale primary identity")
	}

	inProgress := cluster.Status.HA.DeepCopy()
	inProgress.State = databasev1.MysqlClusterHAStateFailoverInProgress
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, inProgress); err != nil {
		return ctrl.Result{}, false, err
	}
	if err := r.revalidateMysqlHAFailureIdentity(ctx, cluster, observation); err != nil {
		return ctrl.Result{}, false, err
	}

	if err := r.handleMasterFailure(ctx, *cluster); err != nil {
		return ctrl.Result{}, false, err
	}

	newPrimary, err := r.observeSinglePublishedPrimary(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	verifying := &databasev1.MysqlClusterHAStatus{
		State:      databasev1.MysqlClusterHAStateVerifying,
		Primary:    newPrimary.Name,
		PrimaryUID: string(newPrimary.UID),
	}
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, verifying); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

// 当主库挂掉时，需要选举新的主库并重新配置主从关系：
func (r *MysqlClusterReconciler) handleMasterFailure(ctx context.Context, cluster databasev1.MysqlCluster) error {
	log := log.FromContext(ctx)
	oldPrimaryPods := &v1.PodList{}
	if err := r.List(ctx, oldPrimaryPods, mysqlClusterPodListOptions(&cluster, "master")...); err != nil {
		return fmt.Errorf("failed to list current primary Pods for MysqlCluster %s before election: %w", cluster.Name, err)
	}

	// 选举新的主库（假设选举逻辑已经实现）
	newMasterName, remainingSlaves, err := r.electNewMaster(ctx, cluster)
	if err != nil {
		return err
	}

	log.Info("选举出新主库", "newMasterName", newMasterName, "remainingSlaves", remainingSlaves)

	// Promote and publish the new primary before changing the old primary role.
	if err := r.setupMysqlPrimary(ctx, newMasterName, cluster); err != nil {
		return err
	}

	// Demotion is a control-plane metadata operation and must not depend on the
	// failed primary being reachable through MySQL.
	for i := range oldPrimaryPods.Items {
		oldPrimary := &oldPrimaryPods.Items[i]
		if oldPrimary.Name == newMasterName {
			continue
		}
		if err := r.labelPod(ctx, oldPrimary.Name, "slave", cluster); err != nil {
			return fmt.Errorf(
				"failed to demote old primary Pod %s/%s after promoting %s: %w",
				oldPrimary.Namespace,
				oldPrimary.Name,
				newMasterName,
				err,
			)
		}
	}

	if err := r.setupMysqlReplicas(ctx, remainingSlaves, cluster); err != nil {
		return err
	}

	return nil
}

// 检查所有从pod的主从状态，返回当前主库名与所以异常的从pod名构成的数组
func (r *MysqlClusterReconciler) checkReplicaStatus(ctx context.Context, cluster databasev1.MysqlCluster) (string, []string, error) {
	/*
		1、调用已有的函数getMasterPodNameFromEndpoints、getReplicaPodsNames来获取所有的从pod名字
		2、判断所有从pod名字的主从状态，判断依据是判断sql线程与io线程同时为yes代表成功否则失败，执行命令是调用已有函数func (r *MysqlClusterReconciler) execCommandOnPod(pod *v1.Pod, command string) (string, error)

		3、最后返回需要当前主库名，所有主从状态异常的从库数组
	*/
	log := log.FromContext(ctx)

	// 获取主库 Pod 名称
	masterPodName, err := r.getMasterPodNameFromEndpoints(ctx, cluster)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get master pod name: %v", err)
	}

	// 获取所有从库 Pod 名称
	replicaPodNames, err := r.getReplicaPodsNames(ctx, cluster, masterPodName)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get replica pods: %v", err)
	}

	// 准备 SQL 查询命令
	sqlQuery := mysqlShowSlaveStatusCommand()

	var failedReplicas []string
	for _, replicaPodName := range replicaPodNames {
		// 获取 Pod 对象
		pod := &v1.Pod{}
		if err := r.Get(ctx, client.ObjectKey{Name: replicaPodName, Namespace: cluster.Namespace}, pod); err != nil {
			return "", nil, fmt.Errorf("failed to get pod %s: %v", replicaPodName, err)
		}
		if err := r.validateMysqlPodBeforeSQL(ctx, pod, &cluster, "replica status SQL"); err != nil {
			return "", nil, err
		}

		// 执行 SQL 查询
		output, err := r.executeCommandOnPod(pod, sqlQuery)
		if err != nil {
			return "", nil, fmt.Errorf("failed to execute command on pod %s: %v", replicaPodName, err)
		}

		// 解析 SQL 查询结果
		sqlThread := strings.Contains(output, "Slave_SQL_Running: Yes")
		ioThread := strings.Contains(output, "Slave_IO_Running: Yes")

		if !(sqlThread && ioThread) {
			failedReplicas = append(failedReplicas, replicaPodName)
		}
	}

	log.Info("主从状态检查完成", "主库", masterPodName, "状态失败的从库", failedReplicas)

	// 返回主库名称和所有主从状态异常的从库名称
	return masterPodName, failedReplicas, nil
}

// 该函数负责确保所有非主库的 Pod 标签都是 slave。
func (r *MysqlClusterReconciler) ensureSlaveRoles(ctx context.Context, cluster databasev1.MysqlCluster) error {
	// 获取 master-service 关联的 Pod 名称
	//endpoints := &v1.Endpoints{}
	//if err := r.Get(ctx, client.ObjectKey{Name: cluster.Spec.MasterService, Namespace: cluster.Namespace}, endpoints); err != nil {
	//	return err
	//}
	//masterPodName := endpoints.Subsets[0].Addresses[0].TargetRef.Name

	masterPodName, err := r.getMasterPodNameFromEndpoints(ctx, cluster)
	if err != nil {
		return err
	}

	// 获取所有与集群相关的 Pods
	podList := &v1.PodList{}
	if err := r.List(ctx, podList, mysqlClusterPodListOptions(&cluster, "")...); err != nil {
		return err
	}

	// 遍历所有 Pods，确保非 master 的 Pod 的 role 都是 slave
	for _, pod := range podList.Items {
		if pod.Name != masterPodName && (pod.Labels[LegacyLabelRole] != "slave" || pod.Labels[LabelMysqlRole] != "slave") {
			if err := r.labelPod(ctx, pod.Name, "slave", cluster); err != nil {
				return err
			}
		}
	}

	return nil
}

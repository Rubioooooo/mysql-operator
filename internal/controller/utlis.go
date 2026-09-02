package controller

import (
	"bufio"
	"context" // 用于处理上下文，提供超时、取消等操作
	"fmt"     // 格式化I/O函数，如字符串格式化和打印
	"os"
	"strings"

	"k8s.io/client-go/kubernetes"
	//"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client" // 提供与 Kubernetes API 交互的客户端
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1 "github.com/egonlin/api/v1" // 导入自定义的 MySQLCluster API 资源定义
	v1 "k8s.io/api/core/v1"                // 核心 Kubernetes API 对象，例如 Pod 和 Service
)

const (
	mysqlRootClientCommand                = `MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot`
	mysqlReplicationPasswordSQLAssignment = `replication_password_sql=$(printf '%s' "$MYSQL_REPLICATION_PASSWORD" | sed "s/\\\\/\\\\\\\\/g; s/'/''/g")`
	mysqlReplicationMasterHostMaxBytes    = 60
)

func mysqlPreparePrimaryCommand() string {
	return mysqlReplicationPasswordSQLAssignment + `; ` + mysqlRootClientCommand +
		` -e "CREATE USER IF NOT EXISTS 'replica'@'%' IDENTIFIED BY '${replication_password_sql}'; GRANT REPLICATION SLAVE ON *.* TO 'replica'@'%';STOP slave;"`
}

func mysqlConfigureReplicaCommand(masterServiceName string) string {
	return mysqlReplicationPasswordSQLAssignment + `; ` + mysqlRootClientCommand + fmt.Sprintf(
		` -e "STOP SLAVE;CHANGE MASTER TO MASTER_HOST='%s', MASTER_USER='replica', MASTER_PASSWORD='${replication_password_sql}', MASTER_AUTO_POSITION=1; START SLAVE;"`,
		masterServiceName,
	)
}

func mysqlInitializeReplicaCommand(initialPrimaryHost string) string {
	return mysqlReplicationPasswordSQLAssignment + `; ` + mysqlRootClientCommand + fmt.Sprintf(
		` -e "CHANGE MASTER TO MASTER_HOST='%s', MASTER_USER='replica', MASTER_PASSWORD='${replication_password_sql}', MASTER_AUTO_POSITION=1; START SLAVE;"`,
		initialPrimaryHost,
	)
}

func mysqlShowSlaveStatusCommand() string {
	return mysqlRootClientCommand + ` -e "SHOW SLAVE STATUS \G"`
}

func mysqlShowMasterGTIDCommand() string {
	return mysqlRootClientCommand + ` -Nse "SELECT @@GLOBAL.gtid_executed;"`
}

func mysqlShowSlaveGTIDCommand() string {
	return mysqlRootClientCommand + ` -Nse "SELECT @@GLOBAL.gtid_executed;"`
}

// 制作主从同步的函数
func (r *MysqlClusterReconciler) setupMasterSlaveReplication(ctx context.Context, masterName string, slaveNames []string, cluster databasev1.MysqlCluster) error {
	log := log.FromContext(ctx)
	log.Info("setupMasterSlaveReplication函数", "masterName", masterName, "slaveNames", slaveNames)
	if err := r.setupMysqlPrimary(ctx, masterName, cluster); err != nil {
		return err
	}
	return r.setupMysqlReplicas(ctx, slaveNames, cluster)
}

type mysqlSlaveReplicationStatus struct {
	MasterHost      string
	MasterUser      string
	AutoPosition    string
	SlaveIORunning  string
	SlaveSQLRunning string
	LastIOError     string
	LastSQLError    string
}

func validateMysqlReplicationMasterHost(cluster *databasev1.MysqlCluster) error {
	if len(cluster.Spec.MasterService) <= mysqlReplicationMasterHostMaxBytes {
		return nil
	}
	return fmt.Errorf(
		"MysqlCluster %s/%s masterService length %d exceeds MySQL replication MASTER_HOST limit %d",
		cluster.Namespace,
		cluster.Name,
		len(cluster.Spec.MasterService),
		mysqlReplicationMasterHostMaxBytes,
	)
}

func parseMysqlShowSlaveStatus(output string) (*mysqlSlaveReplicationStatus, error) {
	if strings.TrimSpace(output) == "" {
		return nil, nil
	}

	fields := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "***") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		fields[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to parse SHOW SLAVE STATUS output: %w", err)
	}

	return &mysqlSlaveReplicationStatus{
		MasterHost:      fields["Master_Host"],
		MasterUser:      fields["Master_User"],
		AutoPosition:    fields["Auto_Position"],
		SlaveIORunning:  fields["Slave_IO_Running"],
		SlaveSQLRunning: fields["Slave_SQL_Running"],
		LastIOError:     fields["Last_IO_Error"],
		LastSQLError:    fields["Last_SQL_Error"],
	}, nil
}

func (status *mysqlSlaveReplicationStatus) configurationMatches(masterHost string) bool {
	return status != nil &&
		status.MasterHost == masterHost &&
		status.MasterUser == "replica" &&
		status.AutoPosition == "1"
}

func (status *mysqlSlaveReplicationStatus) semanticallyHealthy(masterHost string) bool {
	return status.configurationMatches(masterHost) &&
		status.SlaveIORunning == "Yes" &&
		status.SlaveSQLRunning == "Yes" &&
		status.LastIOError == "" &&
		status.LastSQLError == ""
}

func mysqlPodHasPublishedRole(pod *v1.Pod, expectedRole string) (bool, error) {
	canonicalRole := pod.Labels[LabelMysqlRole]
	legacyRole := pod.Labels[LegacyLabelRole]
	if canonicalRole == "" && legacyRole == "" {
		return false, nil
	}
	if canonicalRole == "" || legacyRole == "" || canonicalRole != legacyRole {
		return false, fmt.Errorf(
			"Pod %s/%s has inconsistent MySQL role labels: %s=%q, %s=%q",
			pod.Namespace,
			pod.Name,
			LabelMysqlRole,
			canonicalRole,
			LegacyLabelRole,
			legacyRole,
		)
	}
	return canonicalRole == expectedRole, nil
}

// reconcileMysqlInitializationTopology advances exactly one initialization
// stage. It returns true only after every replica is semantically healthy and
// topology roles are safe to publish.
func (r *MysqlClusterReconciler) reconcileMysqlInitializationTopology(
	ctx context.Context,
	masterName string,
	slaveNames []string,
	cluster databasev1.MysqlCluster,
) (bool, error) {
	masterPod := &v1.Pod{}
	masterPodKey := client.ObjectKey{Namespace: cluster.Namespace, Name: masterName}
	if err := r.Get(ctx, masterPodKey, masterPod); err != nil {
		return false, fmt.Errorf("failed to get initial primary Pod %s: %v", masterName, err)
	}
	if err := r.validateMysqlPodBeforeSQL(ctx, masterPod, &cluster, "initial primary preparation SQL"); err != nil {
		return false, err
	}
	masterPublished, err := mysqlPodHasPublishedRole(masterPod, "master")
	if err != nil {
		return false, err
	}
	if !masterPublished {
		if _, err := r.executeCommandOnPod(masterPod, mysqlPreparePrimaryCommand()); err != nil {
			return false, fmt.Errorf("failed to execute command on initial primary pod %s: %v", masterName, err)
		}
	}

	allReplicasHealthy := true
	replicaConfigurationChanged := false
	for _, slaveName := range slaveNames {
		slavePod := &v1.Pod{}
		slavePodKey := client.ObjectKey{Namespace: cluster.Namespace, Name: slaveName}
		if err := r.Get(ctx, slavePodKey, slavePod); err != nil {
			return false, fmt.Errorf("failed to get initial replica Pod %s: %v", slaveName, err)
		}
		if err := r.validateMysqlPodBeforeSQL(ctx, slavePod, &cluster, "initial replica status SQL"); err != nil {
			return false, err
		}

		slaveStatus, err := r.executeCommandOnPod(slavePod, mysqlShowSlaveStatusCommand())
		if err != nil {
			return false, fmt.Errorf("failed to inspect initial replica pod %s: %v", slaveName, err)
		}
		status, err := parseMysqlShowSlaveStatus(slaveStatus)
		if err != nil {
			return false, fmt.Errorf("failed to inspect initial replica pod %s: %v", slaveName, err)
		}
		if status == nil {
			if _, err := r.executeCommandOnPod(slavePod, mysqlInitializeReplicaCommand(cluster.Spec.MasterService)); err != nil {
				return false, fmt.Errorf("failed to execute command on initial replica pod %s: %v", slaveName, err)
			}
			allReplicasHealthy = false
			replicaConfigurationChanged = true
			continue
		}
		if !status.configurationMatches(cluster.Spec.MasterService) {
			if _, err := r.executeCommandOnPod(slavePod, mysqlConfigureReplicaCommand(cluster.Spec.MasterService)); err != nil {
				return false, fmt.Errorf("failed to reconfigure initial replica pod %s: %v", slaveName, err)
			}
			allReplicasHealthy = false
			replicaConfigurationChanged = true
			continue
		}
		if !status.semanticallyHealthy(cluster.Spec.MasterService) {
			allReplicasHealthy = false
		}
	}

	if !masterPublished {
		if err := r.labelPod(ctx, masterName, "master", cluster); err != nil {
			return false, fmt.Errorf("failed to label initial primary pod %s: %v", masterName, err)
		}
		return false, nil
	}
	if replicaConfigurationChanged || !allReplicasHealthy {
		return false, nil
	}

	for _, slaveName := range slaveNames {
		if err := r.labelPod(ctx, slaveName, "slave", cluster); err != nil {
			return false, fmt.Errorf("failed to label initial replica pod %s: %v", slaveName, err)
		}
	}
	currentMaster := &v1.Pod{}
	if err := r.Get(ctx, masterPodKey, currentMaster); err != nil {
		return false, fmt.Errorf("failed to revalidate initial primary Pod %s: %v", masterName, err)
	}
	if err := r.validateMysqlPodBeforeSQL(ctx, currentMaster, &cluster, "initial primary completion validation"); err != nil {
		return false, err
	}
	masterPublished, err = mysqlPodHasPublishedRole(currentMaster, "master")
	if err != nil {
		return false, err
	}
	if !masterPublished {
		return false, fmt.Errorf("initial primary Pod %s no longer has authoritative master role", masterName)
	}
	return true, nil
}

func (r *MysqlClusterReconciler) validateMysqlPodBeforeSQL(
	ctx context.Context,
	pod *v1.Pod,
	cluster *databasev1.MysqlCluster,
	action string,
) error {
	if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
		return fmt.Errorf(
			"refusing %s on Pod %s/%s before full StatefulSet ownership validation: %w",
			action,
			pod.Namespace,
			pod.Name,
			err,
		)
	}
	return nil
}

func (r *MysqlClusterReconciler) setupMysqlPrimary(ctx context.Context, masterName string, cluster databasev1.MysqlCluster) error {
	// 获取主库 Pod 对象
	masterPod := &v1.Pod{}
	masterPodKey := client.ObjectKey{Namespace: cluster.Namespace, Name: masterName}
	if err := r.Get(ctx, masterPodKey, masterPod); err != nil {
		return fmt.Errorf("failed to get master pod %s: %v", masterName, err)
	}
	if err := r.validateMysqlPodBeforeSQL(ctx, masterPod, &cluster, "primary preparation SQL"); err != nil {
		return err
	}

	// 为主库创建复制用户，并停止slave线程（如果之前自己是从库，那就应该停掉）
	masterCommand := mysqlPreparePrimaryCommand()
	if _, err := r.executeCommandOnPod(masterPod, masterCommand); err != nil {
		return fmt.Errorf("failed to execute command on master pod %s: %v", masterName, err)
	}

	// Publish the primary role only after the database-side transition succeeds.
	if err := r.labelPod(ctx, masterName, "master", cluster); err != nil {
		return fmt.Errorf("failed to label master pod %s: %v", masterName, err)
	}
	return nil
}

func (r *MysqlClusterReconciler) setupMysqlReplicas(ctx context.Context, slaveNames []string, cluster databasev1.MysqlCluster) error {
	// 配置每个从库: 如果从库名数组为空，则
	for _, slaveName := range slaveNames { // 如果没有从库，则循环结束，不会配置从库
		slavePod := &v1.Pod{}
		slavePodKey := client.ObjectKey{Namespace: cluster.Namespace, Name: slaveName}
		if err := r.Get(ctx, slavePodKey, slavePod); err != nil {
			return fmt.Errorf("failed to get slave pod %s: %v", slaveName, err)
		}
		if err := r.validateMysqlPodBeforeSQL(ctx, slavePod, &cluster, "replica configuration SQL"); err != nil {
			return err
		}

		// 配置主从复制: 先停slave，再配置、然后再启slave
		slaveCommand := mysqlConfigureReplicaCommand(cluster.Spec.MasterService)
		if _, err := r.executeCommandOnPod(slavePod, slaveCommand); err != nil {
			return fmt.Errorf("failed to execute command on slave pod %s: %v", slaveName, err)
		}

		// 打标签
		if err := r.labelPod(ctx, slaveName, "slave", cluster); err != nil {
			return fmt.Errorf("failed to label slave pod %s: %v", slaveName, err)
		}
	}

	return nil
}

func (r *MysqlClusterReconciler) executeCommandOnPod(pod *v1.Pod, command string) (string, error) {
	if r.execCommandOnPodFn != nil {
		return r.execCommandOnPodFn(pod, command)
	}
	return r.execCommandOnPod(pod, command)
}

// labelPod 为 Pod 打标签
func (r *MysqlClusterReconciler) labelPod(ctx context.Context, podName, role string, cluster databasev1.MysqlCluster) error {
	pod := &v1.Pod{}
	podKey := client.ObjectKey{Namespace: cluster.Namespace, Name: podName}
	if err := r.Get(ctx, podKey, pod); err != nil {
		return fmt.Errorf("failed to get pod %s: %v", podName, err)
	}
	if err := r.validateMysqlClusterPodForRoleMutation(ctx, pod, &cluster); err != nil {
		return err
	}

	// 更新 Pod 标签
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[LegacyLabelRole] = role
	pod.Labels[LabelMysqlRole] = role

	if err := r.Update(ctx, pod); err != nil {
		return fmt.Errorf("failed to update pod %s: %v", podName, err)
	}

	return nil
}

// kubectl exec 进入pod内执行命令
func (r *MysqlClusterReconciler) execCommandOnPod(pod *v1.Pod, command string) (string, error) {
	// Load kubeconfig from default location
	//kubeconfig := os.Getenv("KUBECONFIG")
	//if kubeconfig == "" {
	//	kubeconfig = "/root/.kube/config" // Fallback to default path
	//}
	//config, err := clientcmd.BuildConfigFromFlags("", KubeConfigPath) // 来自包："k8s.io/client-go/tools/clientcmd"
	config, err := rest.InClusterConfig() // 来自包："k8s.io/client-go/rest"

	if err != nil {
		return "", err
	}

	// Create a new Kubernetes clientset
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", err
	}

	// Create REST client for pod exec
	restClient := kubeClient.CoreV1().RESTClient()
	req := restClient.
		Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		Param("stdin", "false").
		Param("stdout", "true").
		Param("stderr", "true").
		Param("tty", "false").
		Param("container", pod.Spec.Containers[0].Name).
		Param("command", "/bin/sh").
		Param("command", "-c").
		Param("command", command)

	// Create an executor
	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", err
	}

	// Execute the command
	var output strings.Builder
	err = executor.Stream(remotecommand.StreamOptions{
		Stdout: &output,
		Stderr: os.Stderr,
	})
	if err != nil {
		return "", err
	}

	return output.String(), nil
}

// 通过master-service中的endpoint地址获取当前主库pod名
func (r *MysqlClusterReconciler) getMasterPodNameFromEndpoints(ctx context.Context, cluster databasev1.MysqlCluster) (string, error) {
	log := log.FromContext(ctx)

	// 获取 master-service 关联的 Pod 名称
	endpoints := &v1.Endpoints{}
	if err := r.Get(ctx, client.ObjectKey{Name: cluster.Spec.MasterService, Namespace: cluster.Namespace}, endpoints); err != nil {
		return "", fmt.Errorf("failed to get endpoints for service %s: %v", cluster.Spec.MasterService, err)
	}

	if len(endpoints.Subsets) == 0 || len(endpoints.Subsets[0].Addresses) == 0 {
		return "", fmt.Errorf("no endpoints found for service %s", cluster.Spec.MasterService)
	}

	masterPodName := endpoints.Subsets[0].Addresses[0].TargetRef.Name
	log.Info("从master-service的endpoints获取主pod名", "masterPodName", masterPodName)

	// 返回第一个 Pod 名称
	return masterPodName, nil
}

// 获取所有当前从pod的名字
func (r *MysqlClusterReconciler) getReplicaPodsNames(ctx context.Context, cluster databasev1.MysqlCluster, masterPodName string) ([]string, error) {
	// 获取所有实际副本信息
	actualReplicaCount, podNames := r.getActualReplicaInfo(ctx, cluster)

	// 如果没有实际副本，直接返回空数组
	if actualReplicaCount == 0 {
		return nil, nil
	}

	// 获取主库 Pod 名称
	//masterPodName, err := r.getMasterPodNameFromEndpoints(ctx, cluster)
	//if err != nil {
	//	return nil, err
	//}

	// 过滤掉主库 Pod 名称，得到从库 Pod 名称
	var replicaPodNames []string
	for _, podName := range podNames {
		if podName != masterPodName {
			replicaPodNames = append(replicaPodNames, podName)
		}
	}

	return replicaPodNames, nil
}

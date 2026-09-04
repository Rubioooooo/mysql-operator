package controller

import (
	"context" // 用于处理上下文，提供超时、取消等操作
	"fmt"

	"github.com/go-logr/logr" // 用于记录日志
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime" // 提供对象的通用机制，如序列化和版本转换
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/controller-runtime/pkg/client" // 提供与 Kubernetes API 交互的客户端
	"sigs.k8s.io/controller-runtime/pkg/log"

	databasev1 "github.com/egonlin/api/v1" // 导入自定义的 MySQLCluster API 资源定义
	v1 "k8s.io/api/core/v1"                // 核心 Kubernetes API 对象，例如 Pod 和 Service
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

const mysqlClusterInitializedAnnotation = "initialized"

// MysqlClusterReconciler reconciles a MysqlCluster object
type MysqlClusterReconciler struct {
	client.Client                  // 嵌入 client.Client 接口，用于与 Kubernetes API 交互
	Log                logr.Logger // 日志记录器
	Scheme             *runtime.Scheme
	Recorder           record.EventRecorder
	Metrics            *MysqlClusterMetrics
	MasterGTIDSnapshot string // 用于存储主库的 GTID 快照
	SnapGoIsEnabled    bool   // 标识用于记录GTID快照的协程序是否启动，默认值为false，只有启动后才会设置为true
	execCommandOnPodFn func(*v1.Pod, string) (string, error)
}

func mysqlClusterIsInitialized(cluster *databasev1.MysqlCluster) (bool, error) {
	value, exists := cluster.GetAnnotations()[mysqlClusterInitializedAnnotation]
	switch {
	case !exists:
		return false, nil
	case value == "true":
		return true, nil
	default:
		return false, fmt.Errorf(
			"MysqlCluster %s/%s has invalid %s annotation value %q",
			cluster.Namespace,
			cluster.Name,
			mysqlClusterInitializedAnnotation,
			value,
		)
	}
}

/*
在您的代码中，涉及到的 Kubernetes 资源包括：Pod、ConfigMap、Service、Endpoints、namespace对应设置权限如下
*/
// +kubebuilder:rbac:groups=apps.egonlin.com,resources=mysqlclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps.egonlin.com,resources=mysqlclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps.egonlin.com,resources=mysqlclusters/finalizers,verbs=update

// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete

// +kubebuilder:rbac:groups="",resources=pods;services;configmaps,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create;get;list;watch
// +kubebuilder:rbac:groups="",resources=endpoints,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;get;list;watch;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create;update;delete

// 调谐函数
func (r *MysqlClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("调谐函数触发执行", "req", req) // 额外增加1个字段

	var cluster databasev1.MysqlCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			r.Metrics.deleteCluster(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	initialized, lifecycleErr := mysqlClusterIsInitialized(&cluster)
	if lifecycleErr != nil {
		return ctrl.Result{}, lifecycleErr
	}
	projected, projectionErr := r.reconcileMysqlObservability(ctx, &cluster, initialized)
	if projectionErr != nil {
		return ctrl.Result{}, projectionErr
	}
	if projected {
		// This iteration only publishes observations. The next iteration
		// executes the unchanged lifecycle/HA path with fresh durable state.
		return ctrl.Result{Requeue: true}, nil
	}

	var (
		result   ctrl.Result
		complete bool
		err      error
	)
	if !initialized {
		result, complete, err = r.reconcileStatefulSetInitialization(ctx, &cluster)
	} else {
		result, complete, err = r.reconcileStatefulSetRuntime(ctx, &cluster)
	}
	if err != nil {
		return result, err
	}
	if !complete {
		return result, nil
	}

	// 启用协程定期记录当前主库的GTID快照，用于选举依据
	if !r.SnapGoIsEnabled {
		r.startAndUpdateGTIDSnapshot(ctx, cluster)
		r.SnapGoIsEnabled = true
	}

	return ctrl.Result{}, nil
}

// 在cmd/main.go入口main函数中会调用该函数，来对你的控制器进行设置，指定控制器管理的资源，并将控制器注册到控制器管理器中
func (r *MysqlClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&databasev1.MysqlCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&v1.Service{}).
		Owns(&v1.ConfigMap{}).
		Watches(
			&v1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.mapCredentialSecretToMysqlClusters),
		).
		Watches(
			&v1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.mapMysqlStatefulSetPodToMysqlCluster),
		).
		Watches(
			&v1.Endpoints{},
			handler.EnqueueRequestsFromMapFunc(r.mapMysqlPrimaryEndpointsToMysqlClusters),
		).
		Complete(r)
}

// bug说明
// 上述SetupWithManager的核心在与针对MysqlCluster资源所拥有的Pod控制器只会在 MysqlCluster 对象本身发生 创建 / 更新 / 删除 时被触发
// 我检测当pod发生挂掉时，会触发调谐函数的执行，检查主从状态的逻辑是根据endpoint来判断的，如果endpoint控制器摘出失效的ednpoint的慢了（异步执行的），会导致我的调谐逻辑运行失效，所有从库的主从状态都挂掉，
// 然而后续也不会有事件出来了，这就会导致主从状态无法恢复正常，这个该如何解决呢？
// 在SetupWithManager中添加对相关endpoint的watch事件，例如下面的样子，代码没有经过测试，只罗列大致逻辑
// func (r *MysqlClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
//     return ctrl.NewControllerManagedBy(mgr).
//         For(&databasev1.MysqlCluster{}).
//         Owns(&corev1.Pod{}).
//         Watches(&source.Kind{Type: &corev1.Endpoints{}}, &handler.EnqueueRequestForOwner{
//             OwnerType:    &databasev1.MysqlCluster{},
//             IsController: true,
//         }).
//         Complete(r)
// }
// 这样：
// （1）Pod 变动触发 Reconcile
// （2）Endpoint 更新/删除 也会触发 Reconcile
// → 你就不会错过 Endpoint 状态更新。

# Phase 2-B2 — 面向副本变更的 Transition-Aware 收敛机制

## 概述

Phase 2-B2 为 MySQL Operator 引入了面向副本数量变化的 Transition-Aware Runtime Reconciliation。

这一阶段解决的问题并不是简单地修改：

```text
StatefulSet.spec.replicas
```

而是让 Controller 能够持久化并区分：

- 最近一次真正完成收敛的稳定副本数量；
- 当前是否存在正在进行的副本数量变更；
- 本次变更从哪个稳定副本数量开始；
- 当前变更目标是多少；
- Kubernetes 子资源是否已经完成收敛；
- MySQL 拓扑是否已经完成收敛。

最终形成的核心原则是：

> **先持久化生命周期变更意图，再修改 Kubernetes 子资源。**

这样，即使 Controller 在生命周期变更的中间阶段退出或重启，也可以根据 API Server 中已经持久化的状态继续调谐，而不需要根据一个尚未完全收敛的 StatefulSet 反向猜测之前发生了什么。

Phase 2-B2 最终通过：

- Controller regression；
- 完整仓库测试；
- `go vet`；
- Runtime E2E compile guard；
- 真实 Kubernetes；
- 真实 StatefulSet 生命周期；
- 真实 MySQL replication；

进行了验证。

最终实现封板于：

```text
470958189aee4cd68acd320fcf9251298b29c3b2
feat(runtime): add transition-aware replica convergence
```

---

## 1. 为什么副本调谐需要持久化 Transition

一次副本数量变化实际上跨越了两个不同的控制系统。

第一层是 Kubernetes：

```text
StatefulSet
→ Pod
→ PVC
→ Scheduler / Kubelet
```

第二层是 MySQL Operator：

```text
MySQL topology
→ replication
→ role
→ routing
```

如果把这两层简单理解成一次即时操作，就会出现生命周期状态不明确的问题。

例如用户将：

```text
replicas: 2
```

修改为：

```text
replicas: 3
```

如果 Controller 立即把：

```text
StatefulSet.spec.replicas
```

修改为 3，然后在 MySQL 拓扑配置完成之前退出，那么 Controller 下一次启动时可能观察到：

```text
spec.replicas = 3
StatefulSet.spec.replicas = 3
Pods = 2 或 3
```

但这些状态本身并不能回答：

- 这次扩容是从 2 开始的吗？
- 第三个成员是否已经被 Operator 接受为稳定集群成员？
- StatefulSet 是否刚刚发生过扩容？
- MySQL replication 是否已经完成？
- 当前是否仍处于生命周期变更过程中？

因此，Phase 2-B 引入了显式、持久化的副本 Transition 状态模型。

状态结构为：

```go
type MysqlClusterReplicaTransitionStatus struct {
    FromReplicas   int32 `json:"fromReplicas"`
    TargetReplicas int32 `json:"targetReplicas"`
}
```

同时在 `MysqlClusterStatus` 中加入：

```go
LastConvergedReplicas *int32
ReplicaTransition     *MysqlClusterReplicaTransitionStatus
```

其语义分别为：

### `LastConvergedReplicas`

表示最近一次**真正完成生命周期收敛**时的稳定副本数量。

### `ReplicaTransition`

非 `nil` 时表示当前存在正在进行的副本数量 Transition。

### `FromReplicas`

表示本次 Transition 开始时的稳定副本数量。

### `TargetReplicas`

表示当前副本变化目标。

其中：

```text
FromReplicas
```

不会因为用户在 Transition 中途修改目标而丢失。

---

## 2. Status-First Reconciliation

Phase 2-B2 最重要的设计之一，是将：

```text
记录 Transition
```

和：

```text
修改 StatefulSet
```

拆成不同的 Reconcile。

当用户修改副本数量、且当前不存在 Transition 时，Controller 首先执行：

```text
desired replicas changed
        |
        v
persist LastConvergedReplicas
persist ReplicaTransition
        |
        v
return
```

这一轮 Reconcile **不会修改 StatefulSet 的副本数量**。

只有之后的 Reconcile，在 Transition 已经成功持久化的前提下，才允许继续修改 StatefulSet。

完整流程可以表示为：

```text
稳定状态
    |
    | 用户修改 spec.replicas
    v
持久化 ReplicaTransition
    |
    | return
    v
下一次 Reconcile
    |
    v
修改 StatefulSet
    |
    v
Kubernetes 原生收敛
    |
    v
MySQL topology reconciliation
    |
    v
确认完整 Transition 已经收敛
    |
    v
更新 LastConvergedReplicas
清除 ReplicaTransition
```

状态持久化使用：

```go
Status().Patch(ctx, cluster, client.MergeFrom(base))
```

而不是整体覆盖整个 `status`。

这样 Replica Transition 逻辑只修改它实际负责的状态字段，避免覆盖其他 Controller 状态。

---

## 3. Transition Delta Ready 不等于全局集群健康

Phase 2-B2 的一个重要设计约束来自之前已经建立的 HA 行为。

曾经出现过一种设计：

在进入 MySQL domain reconciliation 前要求：

```text
所有目标成员存在
+
成员数量完全一致
+
所有成员 Ready
```

这个设计最终被否决。

原因是：

对于 HA Controller 来说：

```text
Primary NotReady
```

或者：

```text
Primary Missing
```

本身就是应该进入故障处理逻辑的信号。

如果 Controller 规定：

> Primary 必须 Ready 才允许进入 HA reconciliation

那么就会发生：

```text
Primary 发生故障
        ↓
Primary NotReady
        ↓
Lifecycle Gate 阻止进入 HA
        ↓
Failover 无法运行
```

因此 Phase 2-B2 将两个概念明确拆开。

### `mysqlReplicaTransitionDeltaReady`

它只判断：

> 这次副本变化本身是否已经具备进入后续 domain reconciliation 的条件？

对于扩容：

只检查 Transition 新增加的 ordinal：

```text
FromReplicas + 1
...
TargetReplicas
```

这些新成员必须：

- 存在；
- 通过 ownership / identity 校验；
- Running；
- MySQL Ready。

原本已经存在的稳定成员不会因此被重新定义成一个新的全局 Ready Gate。

对于缩容以及回到原目标的 Transition：

Controller 等待：

```text
ordinal > TargetReplicas
```

的成员真正消失。

### `mysqlReplicaTransitionFullyConverged`

这是一个更严格的判断。

它用于判断：

> 当前 Transition 是否已经可以正式提交为稳定状态？

此时才要求目标成员：

- 数量与目标一致；
- ordinal 连续且正确；
- identity 正确；
- 满足对应健康条件。

因此：

```text
Transition Delta Ready
```

与：

```text
完整 Transition 收敛
```

是两个不同概念。

最终需要保持的核心不变量是：

```text
稳定运行期间出现的 readiness 故障
!=
副本生命周期 Transition 的全局前置条件
```

---

## 4. 对旧 Initialized 集群的兼容 Bootstrap

在 Phase 2-B2 之前创建并初始化完成的 MysqlCluster，可能存在：

```text
initialized = true
LastConvergedReplicas = nil
ReplicaTransition = nil
```

这些对象并不知道新的 durable Transition contract。

Phase 2-B2 为这种情况增加了兼容 Bootstrap。

如果已经存在受当前 MysqlCluster 正确控制的 StatefulSet，则使用：

```text
StatefulSet 当前副本数量
```

作为兼容 checkpoint。

如果：

```text
StatefulSet 当前副本数 == desired replicas
```

则持久化：

```text
LastConvergedReplicas = current
ReplicaTransition = nil
```

然后立即返回。

如果：

```text
StatefulSet 当前副本数 != desired replicas
```

则记录：

```text
LastConvergedReplicas = current

ReplicaTransition = {
    FromReplicas: current,
    TargetReplicas: desired,
}
```

然后返回。

这一轮不会：

- 修改 StatefulSet；
- 执行 SQL；
- 进入 MySQL topology reconciliation。

这样既不会伪造一个不存在的历史稳定副本数量，也可以让旧对象安全进入新的 Transition 模型。

---

## 5. Transition 过程中目标发生变化

用户可能在一个副本 Transition 还没有结束时再次修改：

```text
spec.replicas
```

例如：

```text
stable = 2
        ↓
desired = 3
        ↓
Transition = 2 -> 3
        ↓
desired 又改回 2
```

这种情况下 Controller 不会重新开始一个新的 Transition。

它会保留：

```text
LastConvergedReplicas
FromReplicas
```

只更新：

```text
TargetReplicas
```

然后持久化状态并返回。

因此下面这种状态是合法的：

```text
LastConvergedReplicas = 2

ReplicaTransition = {
    FromReplicas: 2,
    TargetReplicas: 2,
}
```

也就是说：

```text
FromReplicas == TargetReplicas
```

并不表示状态非法。

它记录的是：

> 一个真实开始过、随后目标又返回原稳定数量的生命周期过程。

---

## 6. 缩容安全

StatefulSet 缩容通常会优先删除最高 ordinal 的 Pod。

例如：

```text
3 -> 2
```

通常意味着：

```text
ordinal 3
```

会被删除。

因此，Operator 在降低 StatefulSet 副本数量前必须判断：

> 本次缩容会不会删除当前 Primary？

如果会造成不安全的 Primary 删除，则缩容必须失败关闭。

Phase 2-B2 同时修复了另一个容易遗漏的情况。

即：

```text
ReplicaTransition = nil
LastConvergedReplicas == desired
```

但是 StatefulSet 发生 child drift：

```text
StatefulSet.spec.replicas > desired
```

这种情况下，Controller 仍然必须执行 Primary Removal Safety。

不能因为没有 active Transition 就绕过缩容安全检查。

Phase 2-B2 当前并没有在普通副本缩容过程中自动执行 Primary Switchover。

如果目标缩容会违反 Primary 安全：

```text
fail closed
```

而不是自行发起主库切换。

---

## 7. 真实运行时问题：Replication Password 长度

真实 MySQL 环境验证发现了一个此前没有在数据库变更前正确限制的问题。

某次初始化过程中，MySQL 返回：

```text
ERROR 3056 (HY000):
The password provided for the replication user exceeds
the maximum length of 32 characters
```

当时实际观察到：

```text
initialized = absent
master role = 未发布
slave role = 未发布
```

说明初始化没有错误地把尚未完成的拓扑发布成稳定状态。

但这个失败同时暴露出另一个问题：

> 能够在进入数据库 mutation 之前发现的静态约束，不应该等 MySQL 真正执行 SQL 后才失败。

因此 Controller 增加：

```go
mysqlReplicationPasswordMaxBytes = 32
```

并在 credential validation 阶段检查 replication password。

最终行为为：

```text
32 bytes
→ 接受

> 32 bytes
→ 在数据库 mutation 前拒绝
```

这一限制只应用于：

```text
replication-password
```

而不是简单对所有 MySQL 密码使用相同长度限制。

同时错误信息可以包含：

- Secret identity；
- key；
- 长度限制；

但不能打印真实 credential value。

---

## 8. 真实运行时问题：MASTER_HOST 被截断

第二个真实集群故障更加隐蔽。

最初初始化 replica 时使用的是初始 Primary Pod 的 Headless Service DNS：

```text
phase2b2-regression-mysql-1.phase2b2-regression-mysql-headless
```

用于：

```sql
CHANGE MASTER TO MASTER_HOST='...'
```

这一配置命令本身并没有导致 Reconcile 直接报错。

但是之后检查：

```sql
SHOW SLAVE STATUS\G
```

实际得到：

```text
Master_Host:
phase2b2-regression-mysql-1.phase2b2-regression-mysql-headle

Slave_IO_Running:
Connecting

Slave_SQL_Running:
Yes
```

即：

配置进去的 `MASTER_HOST` 已经发生截断。

这个故障证明了两个问题。

第一：

原先使用的 Pod Headless DNS identity 超出了当前 MySQL 5.7 replication host 所能安全保存的边界。

第二：

```text
CHANGE MASTER / START SLAVE 执行成功
```

不能等价于：

```text
replication 已经健康
```

修复之后初始化不再把：

```text
<pod>.<headless-service>
```

作为 replication `MASTER_HOST`。

而是使用稳定的 Primary Service：

```text
cluster.Spec.MasterService
```

Controller 中加入：

```go
mysqlReplicationMasterHostMaxBytes = 60
```

同时 CRD 对：

```text
masterService
```

增加：

```text
maxLength: 60
```

运行时同样会再次执行 fail-closed validation。

这样，MySQL 外部系统约束不仅存在于代码假设中，而是进入：

```text
CRD API validation
+
runtime validation
```

两个边界。

---

## 9. Replication Semantic Health

Phase 2-B2 明确区分：

```text
Replication Configuration
```

和：

```text
Replication Semantic Health
```

Controller 会解析：

```sql
SHOW SLAVE STATUS\G
```

并获取至少以下字段：

```text
Master_Host
Master_User
Auto_Position
Slave_IO_Running
Slave_SQL_Running
Last_IO_Error
Last_SQL_Error
```

配置匹配要求：

```text
Master_Host == cluster.Spec.MasterService
Master_User == replica
Auto_Position == 1
```

在此基础上，真正的 semantic health 还必须满足：

```text
Slave_IO_Running == Yes
Slave_SQL_Running == Yes
Last_IO_Error == ""
Last_SQL_Error == ""
```

因此：

```text
SQL command returned successfully
```

只能证明命令执行完成。

它不能证明：

```text
replication is healthy
```

这一设计正是由真实 `MASTER_HOST` 截断故障推动完成的。

---

## 10. 初始化阶段的 Role Publication Ordering

初始化过程现在采用分阶段发布角色的方式。

整体顺序为：

```text
ownership / credential / replication host validation
        |
        v
准备 Primary 数据库
        |
        v
配置 Replica
        |
        v
发布 master role
        |
        | return
        v
后续 Reconcile
        |
        v
SHOW SLAVE STATUS
        |
        v
验证 replication semantic health
        |
        v
发布 slave role
        |
        v
完成 initialization
```

也就是说：

### Master Role

只有所需的数据库配置步骤成功后才允许发布。

### Slave Role

只有对应 Replica 已经通过 replication semantic health 验证后才允许发布。

### `initialized=true`

不能领先于完整初始化语义。

这种顺序可以防止：

```text
Kubernetes routing metadata
```

提前于：

```text
真实 MySQL topology
```

进入“看起来已经完成”的状态。

---

## 11. 为什么必须进行真实 Kubernetes 验证

Unit Test 和 Envtest 可以很好地验证：

- Controller 分支逻辑；
- Kubernetes API 操作；
- status patch；
- ownership；
- validation；
- Reconcile 行为。

但是 Envtest 不包含真实的：

- StatefulSet Controller；
- Scheduler；
- Kubelet；
- Pod process lifecycle；
- Local PV 调度和绑定；
- MySQL server；
- Service / EndpointSlice 完整收敛链路。

因此 Phase 2-B2 增加了显式的真实 Runtime Gate。

对应测试 Harness 使用：

```text
build tag:
runtimee2e
```

并且只有显式设置：

```text
PHASE2B2_GATE_C_RUNTIME=1
```

才会真正运行。

Harness 使用 direct controller-runtime client，并且每一个 test-process invocation 只直接执行一次：

```text
Reconcile
```

不会启动正常 production Manager。

这是因为正常 Manager 的 watch/cache 范围比这个隔离 Runtime Gate 所需要的权限边界更大。

因此真实生命周期验证被拆成多个明确的 mutation boundary，由外部脚本逐次控制 Reconcile。

---

## 12. 真实扩容验证：2 -> 3

真实扩容从稳定双节点状态开始：

```text
spec.replicas = 2

LastConvergedReplicas = 2

ReplicaTransition = nil

StatefulSet.spec.replicas = 2
```

用户将目标修改为：

```text
3
```

### 第一次 Reconcile：只持久化 Intent

第一次 Reconcile 之后观察到：

```text
spec.replicas = 3

LastConvergedReplicas = 2

ReplicaTransition = {
    FromReplicas: 2,
    TargetReplicas: 3,
}

StatefulSet.spec.replicas = 2

Pod3 = absent
PVC3 = absent
```

这直接证明：

```text
durable intent before child mutation
```

即：

Transition 已经落盘，但 Kubernetes child resource 尚未被修改。

### 后续 Reconcile：修改 StatefulSet

在 durable Transition 已经存在之后，后续 Reconcile 将：

```text
StatefulSet.spec.replicas
```

从：

```text
2
```

修改为：

```text
3
```

随后由 Kubernetes StatefulSet Controller 原生创建第三个成员。

刚刚发生扩容后实际观察到：

```text
Pod3 present
ordinal = 3
role = 未发布
phase = Pending

PVC3 present
phase = Bound
```

Pod 和 PVC 并不是 Operator 手动创建的。

### 扩容完成

当 Kubernetes 原生生命周期完成、且 MySQL topology reconciliation 成功后，新成员实际观察到：

```text
Master_Host = phase2b2-regression-primary
Master_User = replica
Auto_Position = 1
Slave_IO_Running = Yes
Slave_SQL_Running = Yes
```

之后第三个 Pod 被发布为：

```text
slave
```

最终状态：

```text
LastConvergedReplicas = 3
ReplicaTransition = nil

Pod1 = master
Pod2 = slave
Pod3 = slave

Primary Ready Endpoint = 1
Replica Ready Endpoint = 2
```

真实：

```text
2 -> 3
```

扩容验证完成。

---

## 13. 真实缩容验证：3 -> 2

缩容从稳定三成员拓扑开始。

第一次 Reconcile 只持久化：

```text
LastConvergedReplicas = 3

ReplicaTransition = {
    FromReplicas: 3,
    TargetReplicas: 2,
}

StatefulSet.spec.replicas = 3
```

因此和扩容一样：

```text
Transition Intent
```

先于：

```text
StatefulSet mutation
```

持久化。

之后的 Reconcile 通过 Primary Removal Safety，并将：

```text
StatefulSet.spec.replicas
```

从：

```text
3
```

修改为：

```text
2
```

此时 Transition 仍然保持 active。

随后 StatefulSet Controller 原生完成 Pod3 删除。

实际观察到 StatefulSet 从：

```text
desired = 2
actual Pods = 3
Pod3 present
```

收敛为：

```text
StatefulSet = 2/2
Pod3 absent
```

没有执行手工：

```text
kubectl delete pod
```

来模拟缩容。

### PVC / PV 保留

Pod3 被删除后，对应 PVC 仍然：

```text
Bound
```

其 Local PV 也仍然保持：

```text
Bound
```

即实际生命周期符合：

```text
scale-down
    |
    +--> Pod removed
    |
    +--> PVC retained
    |
    +--> PV retained
```

只有 Kubernetes membership 与 MySQL domain reconciliation 都完成以后，Controller 才最终提交：

```text
LastConvergedReplicas = 2
ReplicaTransition = nil
```

真实：

```text
3 -> 2
```

缩容验证完成。

---

## 14. 真实 Target Reversal：2 -> 3 -> 2

Phase 2-B2 还验证了：

> Transition 已经开始，但 Child Mutation 尚未发生时，用户再次修改目标。

起始稳定状态：

```text
LastConvergedReplicas = 2
ReplicaTransition = nil
StatefulSet.spec.replicas = 2
```

目标首先修改为：

```text
3
```

第一次 Reconcile 持久化：

```text
ReplicaTransition = {
    FromReplicas: 2,
    TargetReplicas: 3,
}
```

StatefulSet 仍然：

```text
2
```

随后，在真正修改 StatefulSet 前，用户将：

```text
spec.replicas
```

重新修改为：

```text
2
```

下一次 Reconcile 将 Transition 更新成：

```text
ReplicaTransition = {
    FromReplicas: 2,
    TargetReplicas: 2,
}
```

同时保持：

```text
LastConvergedReplicas = 2
FromReplicas = 2
StatefulSet.spec.replicas = 2
```

全过程没有创建 Pod3。

后续 Reconcile 完成这个 return-to-origin Transition 后：

```text
LastConvergedReplicas = 2
ReplicaTransition = nil
```

这一真实 Runtime 场景证明：

```text
Target 发生变化
```

不会破坏：

```text
Transition 原始稳定起点
```

---

## 15. 验证矩阵

Phase 2-B2 并不是只通过一个扩容案例后就完成封板。

最终验证覆盖：

| 验证项 | 结果 |
| --- | --- |
| Replica Transition 专项测试 | PASS |
| Initialization Semantic 测试 | PASS |
| Phase 1-H HA Regression | PASS |
| Controller Test Suite | PASS |
| Full Repository Test Suite | PASS |
| `go vet ./...` | PASS |
| `runtimee2e` Compile Guard | PASS |
| 真实 Initialization / Bootstrap | PASS |
| 真实 2 -> 3 Scale-Up | PASS |
| 真实 3 -> 2 Scale-Down | PASS |
| 真实 2 -> 3 -> 2 Reversal | PASS |
| Final Stable Real-Cluster State | PASS |
| Protected Runtime Guard | PASS |
| Exact Change Boundary | PASS |
| Worktree Mutation Fingerprint Guard | PASS |

最终 regression 完成后，隔离测试集群保持在稳定双成员状态。

同时没有修改受保护的其他 Runtime workload。

---

## 16. 核心设计取舍

### Kubernetes 负责成员生命周期

Operator 修改 StatefulSet 的 desired state。

Kubernetes 原生负责：

- Pod 创建；
- Pod 删除；
- ordinal identity；
- PVC 生命周期；
- scheduling；
- restart / recreation。

Operator 不重新实现这些 Kubernetes 已经具备的能力。

### Kubernetes 收敛与数据库收敛是两个概念

Pod：

```text
Running
```

或者：

```text
Ready
```

并不足以证明：

```text
MySQL replication 正确
```

MySQL topology 必须独立执行 semantic validation。

### HA 故障信号不能被 Lifecycle Gate 屏蔽

稳定运行阶段不能要求：

```text
所有成员永远 Ready
```

才能进入 domain reconciliation。

否则 Primary NotReady 等真正应该触发 HA 的状态反而会被 lifecycle 层拦截。

### Transition 历史应该持久化，而不是猜测

通过：

```text
LastConvergedReplicas
+
ReplicaTransition
```

Controller 可以直接根据 API Server 持久化状态判断生命周期阶段。

而不是根据一个可能尚未完全收敛的 StatefulSet 去反推：

> 之前到底发生了什么。

### 不安全缩容必须 Fail Closed

副本缩容不会为了“让请求成功”而自动执行未设计好的 Primary Switchover。

如果当前 Primary 会因为缩容被删除：

```text
fail closed
```

优先保护数据库拓扑安全。

---

## 17. 工程经验总结

Phase 2-B2 最终沉淀出了几个可以推广到其他 Kubernetes Operator 的经验。

### 1. Side Effect 之前先持久化 Intent

对于多阶段 Reconcile：

```text
修改 Child Resource
```

不应该成为唯一能够证明“Transition 已经开始”的事实来源。

先持久化 Transition，可以让 Controller restart / retry 后仍然从明确的状态继续。

### 2. API Success 不等于 Semantic Success

Kubernetes API update 成功：

不代表 StatefulSet 已经完成收敛。

MySQL SQL command 成功：

也不代表 replication 已经健康。

因此必须为每一层定义自己的 convergence condition。

### 3. 真实系统能够暴露 Mock 难以发现的约束

Replication password 长度问题和 `MASTER_HOST` 截断问题，都来自真实 MySQL runtime。

Controller 流程在代码层面看起来可以正常执行，但数据库本身仍然可能拒绝或者改变输入。

### 4. Kubernetes 原生 Lifecycle 应继续由 Kubernetes 管理

StatefulSet 已经提供：

- stable identity；
- ordinal；
- Pod lifecycle；
- PVC lifecycle。

Operator 更适合负责：

- database-aware safety；
- topology intent；
- replication；
- routing；
- lifecycle completion。

而不是重新实现 StatefulSet。

### 5. Lifecycle 与 HA 必须一起设计

一个局部看来非常“严格”的规则：

```text
所有成员必须 Ready
```

并不一定更加安全。

如果它使：

```text
Primary failure
```

无法进入：

```text
HA reconciliation
```

那么这个 Gate 实际上会降低系统可恢复性。

因此 Lifecycle Gate 必须结合整个 Controller 状态机一起判断，而不能只看局部函数。

---

## 18. 最终状态

Phase 2-B2 最终封板于：

```text
470958189aee4cd68acd320fcf9251298b29c3b2
```

最终实现的能力包括：

```text
Durable Replica Transition Intent
+
Status-First Child Mutation Ordering
+
Legacy Checkpoint Bootstrap
+
Scale-Up Convergence
+
Scale-Down Safety
+
Target Reversal
+
MySQL Replication Semantic Validation
+
Real Kubernetes Lifecycle Validation
```

Phase 2-B2 最终继续遵循项目的核心职责边界：

```text
Kubernetes：
负责原生资源生命周期

Operator：
负责数据库感知的意图、安全、拓扑与收敛
```

也就是：

> **如果 Kubernetes 已经能够原生正确解决的问题，就不要在 Operator 中重新实现一套。**

这一边界为后续继续建设 Declarative Lifecycle、Replication Controller 和 HA State Machine 提供了更稳定的基础。

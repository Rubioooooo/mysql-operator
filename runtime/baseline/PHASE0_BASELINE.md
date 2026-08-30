# Phase 0 — Original Runtime Baseline

## Baseline Purpose

This baseline records the behavior of the original MySQL Operator before
production-oriented refactoring.

The purpose is not to claim production readiness.

The purpose is to establish which original domain capabilities actually work
in a real Kubernetes + MySQL runtime, and which architectural defects must be
fixed without regressing those capabilities.

---

## Verified Core Capabilities

### BASELINE_CAPABILITY_001 — One-primary multi-replica

PASS.

A three-instance MySQL cluster was created successfully:

- mysql-01
- mysql-02
- mysql-03

The three Pods were scheduled onto three different Kubernetes nodes and used
three independent Local PVs.

Runtime topology converged to one Primary and two Replicas.

### BASELINE_CAPABILITY_002 — GTID replication

PASS.

GTID mode was enabled.

A real transaction written to the Primary was successfully replicated to both
Replicas.

Both Replica SQL and IO threads were healthy.

GTID_SUBSET validation confirmed that the Primary GTID set was contained in
both Replica executed GTID sets.

### BASELINE_CAPABILITY_003 — Replica recovery

PASS.

After Primary election, the former Primary was successfully reconfigured as a
Replica of the newly elected Primary.

Replication returned to:

- Slave_IO_Running: Yes
- Slave_SQL_Running: Yes
- Seconds_Behind_Master: 0

### BASELINE_CAPABILITY_004 — Primary election

PASS.

With the Master Service Endpoint deliberately established as empty before
restarting the Operator, the Operator detected loss of the Primary and elected
mysql-02 as the new Primary.

The Kubernetes master Service subsequently routed to mysql-02.

This validates the election path itself.

It does NOT prove that arbitrary real MySQL process or node failures are
reliably detected by the current implementation.

### BASELINE_CAPABILITY_005 — Failover

PASS.

After mysql-02 became Primary:

1. mysql-01 rejoined as a Replica.
2. mysql-03 followed the new Primary.
3. A new transaction was written to mysql-02.
4. The transaction appeared on mysql-01 and mysql-03.
5. GTID_SUBSET validation passed on both Replicas.
6. Replica IO and SQL threads remained healthy.

---

## Important Baseline Defects

### BASELINE_DEFECT_001 — Invalid controller envtest fixture

The existing controller test constructs an incomplete MysqlCluster and cannot
validate a successful Reconcile.

### BASELINE_DEFECT_002 — Build mutates source formatting

`make build` depends on formatting and can modify tracked source files.

### BASELINE_DEFECT_003 — Unsafe E2E execution

The default test path can execute E2E tests against the active kubeconfig and
mutate a real Kubernetes cluster.

### BASELINE_DEFECT_004 — Pod exec architecture

Database operations are tightly coupled to in-cluster Pod exec and shell
commands, including credentials in command arguments and weak timeout/test
boundaries.

### BASELINE_DEFECT_005 — Deployment packaging isolation

The original deployment configuration uses generic names/namespaces and is not
safe for environment-isolated deployment.

### BASELINE_DEFECT_006 — Nested Kustomize image rewrite

The manager base rewrites `controller:latest` to a historical registry image,
preventing a normal upper-level overlay from matching `controller`.

### BASELINE_DEFECT_007 — MySQL readiness race

The Operator treats Kubernetes container Ready as MySQL readiness.

During initial MySQL entrypoint initialization, the Operator attempted root
authentication before final MySQL initialization completed, causing repeated
1045 errors.

The cluster eventually self-recovered after MySQL initialization completed.

### BASELINE_DEFECT_008 — Invalid PVC OwnerReference API version

PVC OwnerReferences use:

`database.kubebuilder.io/v1`

instead of the actual MysqlCluster API:

`apps.egonlin.com/v1`

PVC lifecycle / garbage-collection semantics therefore cannot be trusted.

### BASELINE_DEFECT_009 — Replica write protection missing

Primary and Replica instances all report:

- read_only=0
- super_read_only=0

Replica role labels therefore do not enforce database write semantics.

### BASELINE_DEFECT_010 — Incomplete multi-UUID GTID snapshot

After failover, the real Primary GTID set contains multiple server UUID
entries.

The Operator MasterGTIDSnapshot captures only part of the complete GTID set.

This can make subsequent election candidate scoring incorrect.

---

## Baseline Conclusion

The original implementation proves that the core MySQL domain workflow is
real and functional:

MysqlCluster
→ three MySQL instances
→ one Primary / multiple Replicas
→ GTID replication
→ Primary election
→ topology reconstruction
→ old Primary rejoin
→ post-failover replication

However, the current implementation is not production-grade HA.

The following areas require redesign:

- Stateful workload lifecycle
- MySQL startup/readiness detection
- failover fencing
- Replica write protection
- GTID parsing and election correctness
- multi-cluster isolation
- credential management
- reconciliation state machine
- status and conditions
- OwnerReference lifecycle
- observability
- safe testing
- deployment packaging

Future phases must preserve the five verified baseline capabilities while
replacing unsafe or non-cloud-native implementations.

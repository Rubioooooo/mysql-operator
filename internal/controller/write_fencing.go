package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mysqlWriteRole string

const (
	mysqlWriteRoleWritable    mysqlWriteRole = "Writable"
	mysqlWriteRoleReadOnly    mysqlWriteRole = "ReadOnly"
	mysqlWriteRoleUnknown     mysqlWriteRole = "Unknown"
	mysqlWriteRoleUnsupported mysqlWriteRole = "Unsupported"
)

type mysqlWriteSafetyObservation struct {
	ReadOnly               bool
	SuperReadOnly          bool
	GTIDMode               string
	EnforceGTIDConsistency string
	WriteRole              mysqlWriteRole
	GTIDReady              bool
}

func mysqlWriteSafetyObservationCommand() string {
	return mysqlRootClientCommand + ` -Nse "SELECT @@GLOBAL.read_only, @@GLOBAL.super_read_only, @@GLOBAL.gtid_mode, @@GLOBAL.enforce_gtid_consistency;"`
}

func mysqlSetSuperReadOnlyCommand() string {
	return mysqlRootClientCommand + ` -e "SET GLOBAL super_read_only = ON;"`
}

func parseMysqlBoolean(name, value string) (bool, error) {
	switch value {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("invalid %s value %q: expected 0 or 1", name, value)
	}
}

func parseMysqlWriteSafetyObservation(output string) (mysqlWriteSafetyObservation, error) {
	line := strings.TrimSuffix(output, "\n")
	line = strings.TrimSuffix(line, "\r")
	if line == "" || strings.ContainsAny(line, "\r\n") {
		return mysqlWriteSafetyObservation{}, fmt.Errorf("malformed MySQL write-safety observation: expected exactly one row")
	}

	fields := strings.Split(line, "\t")
	if len(fields) != 4 {
		return mysqlWriteSafetyObservation{}, fmt.Errorf(
			"malformed MySQL write-safety observation: expected 4 tab-separated fields, got %d",
			len(fields),
		)
	}

	readOnly, err := parseMysqlBoolean("read_only", fields[0])
	if err != nil {
		return mysqlWriteSafetyObservation{}, err
	}
	superReadOnly, err := parseMysqlBoolean("super_read_only", fields[1])
	if err != nil {
		return mysqlWriteSafetyObservation{}, err
	}

	switch fields[2] {
	case "OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE", "ON":
	default:
		return mysqlWriteSafetyObservation{}, fmt.Errorf("invalid gtid_mode value %q", fields[2])
	}
	switch fields[3] {
	case "OFF", "WARN", "ON":
	default:
		return mysqlWriteSafetyObservation{}, fmt.Errorf("invalid enforce_gtid_consistency value %q", fields[3])
	}

	observation := mysqlWriteSafetyObservation{
		ReadOnly:               readOnly,
		SuperReadOnly:          superReadOnly,
		GTIDMode:               fields[2],
		EnforceGTIDConsistency: fields[3],
		GTIDReady:              fields[2] == "ON" && fields[3] == "ON",
	}
	switch {
	case !readOnly && !superReadOnly:
		observation.WriteRole = mysqlWriteRoleWritable
	case readOnly && !superReadOnly:
		observation.WriteRole = mysqlWriteRoleWritable
	case readOnly && superReadOnly:
		observation.WriteRole = mysqlWriteRoleReadOnly
	default:
		observation.WriteRole = mysqlWriteRoleUnknown
	}
	return observation, nil
}

func mysqlSuperReadOnlyUnsupported(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	commandError := &mysqlPodCommandExecutionError{}
	if errors.As(err, &commandError) {
		message += " " + strings.ToLower(commandError.stderr)
	}
	return strings.Contains(message, "super_read_only") &&
		(strings.Contains(message, "unknown system variable") || strings.Contains(message, "unknown variable"))
}

func (r *MysqlClusterReconciler) observeMysqlWriteSafety(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
) (mysqlWriteSafetyObservation, error) {
	if err := r.validateMysqlPodBeforeSQL(ctx, pod, cluster, "write-safety observation SQL"); err != nil {
		return mysqlWriteSafetyObservation{}, err
	}
	output, err := r.executeCommandOnPod(pod, mysqlWriteSafetyObservationCommand())
	if err != nil {
		if mysqlSuperReadOnlyUnsupported(err) {
			return mysqlWriteSafetyObservation{WriteRole: mysqlWriteRoleUnsupported}, nil
		}
		return mysqlWriteSafetyObservation{}, fmt.Errorf(
			"failed to observe MySQL write safety on Pod %s/%s: %w",
			pod.Namespace,
			pod.Name,
			err,
		)
	}
	return parseMysqlWriteSafetyObservation(output)
}

func (r *MysqlClusterReconciler) getValidatedMysqlFailedPrimary(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	failover *databasev1.MysqlClusterFailoverStatus,
) (*corev1.Pod, mysqlPublishedRole, error) {
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: failover.FailedPrimary}
	pod := &corev1.Pod{}
	if err := r.Get(ctx, key, pod); err != nil {
		return nil, mysqlPublishedRoleNone, err
	}
	if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
		return nil, mysqlPublishedRoleNone, err
	}
	ordinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil {
		return nil, mysqlPublishedRoleNone, err
	}
	if expectedName := mysqlStatefulSetPodName(cluster, ordinal); pod.Name != expectedName {
		return nil, mysqlPublishedRoleNone, fmt.Errorf(
			"failed primary Pod %s/%s has non-canonical ordinal identity: expected %s",
			pod.Namespace,
			pod.Name,
			expectedName,
		)
	}
	if string(pod.UID) != failover.FailedPrimaryUID {
		return nil, mysqlPublishedRoleNone, fmt.Errorf(
			"failed primary Pod %s/%s UID changed from %q to %q",
			pod.Namespace,
			pod.Name,
			failover.FailedPrimaryUID,
			pod.UID,
		)
	}
	role, err := observeMysqlPublishedRole(pod)
	if err != nil {
		return nil, mysqlPublishedRoleNone, err
	}
	if role == mysqlPublishedRoleSlave {
		return nil, mysqlPublishedRoleNone, fmt.Errorf(
			"failed primary Pod %s/%s is unexpectedly published as a replica",
			pod.Namespace,
			pod.Name,
		)
	}
	return pod, role, nil
}

func (r *MysqlClusterReconciler) persistMysqlFencingBlocked(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	desired := cluster.Status.HA.DeepCopy()
	desired.State = databasev1.MysqlClusterHAStateDegraded
	clearMysqlElectionProof(desired.Failover)
	desired.Failover.FenceState = databasev1.MysqlClusterFenceStateBlocked
	desired.Failover.FencedPrimaryUID = ""
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, desired); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

func (r *MysqlClusterReconciler) persistMysqlFencingPending(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	desired := cluster.Status.HA.DeepCopy()
	desired.State = databasev1.MysqlClusterHAStateFailoverInProgress
	clearMysqlElectionProof(desired.Failover)
	desired.Failover.FenceState = databasev1.MysqlClusterFenceStatePending
	desired.Failover.FencedPrimaryUID = ""
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, desired); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

func (r *MysqlClusterReconciler) persistMysqlFenceVerified(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	desired := cluster.Status.HA.DeepCopy()
	desired.State = databasev1.MysqlClusterHAStateFailoverInProgress
	clearMysqlElectionProof(desired.Failover)
	desired.Failover.FenceState = databasev1.MysqlClusterFenceStateVerified
	desired.Failover.FencedPrimaryUID = desired.Failover.FailedPrimaryUID
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, desired); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

func (r *MysqlClusterReconciler) mysqlFailedPrimaryRecovered(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	pod *corev1.Pod,
) (bool, error) {
	if mysqlStatefulSetPodHealthy(pod) {
		return true, nil
	}
	endpoints := &corev1.Endpoints{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.MasterService}
	if err := r.Get(ctx, key, endpoints); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return mysqlMasterEndpointAvailable(endpoints), nil
}

func (r *MysqlClusterReconciler) abortMysqlFailoverBeforeFence(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	pod *corev1.Pod,
) (ctrl.Result, bool, error) {
	verifying := &databasev1.MysqlClusterHAStatus{
		State:      databasev1.MysqlClusterHAStateVerifying,
		Primary:    pod.Name,
		PrimaryUID: string(pod.UID),
	}
	if _, err := r.persistMysqlClusterHAStatus(ctx, cluster, verifying); err != nil {
		return ctrl.Result{}, false, err
	}
	return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
}

func (r *MysqlClusterReconciler) clearMysqlPublishedRole(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	podName string,
	expectedUID string,
) error {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: podName}
	if err := r.Get(ctx, key, pod); err != nil {
		return err
	}
	if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
		return err
	}
	ordinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil {
		return err
	}
	if pod.Name != mysqlStatefulSetPodName(cluster, ordinal) {
		return fmt.Errorf("refusing role quarantine for non-canonical Pod %s/%s", pod.Namespace, pod.Name)
	}
	if string(pod.UID) != expectedUID {
		return fmt.Errorf("refusing role quarantine for replacement Pod %s/%s", pod.Namespace, pod.Name)
	}
	role, err := observeMysqlPublishedRole(pod)
	if err != nil {
		return err
	}
	if role == mysqlPublishedRoleNone {
		return nil
	}
	if role != mysqlPublishedRoleMaster {
		return fmt.Errorf("refusing to quarantine Pod %s/%s with role %q", pod.Namespace, pod.Name, role)
	}
	delete(pod.Labels, LabelMysqlRole)
	delete(pod.Labels, LegacyLabelRole)
	if err := r.Update(ctx, pod); err != nil {
		return fmt.Errorf("failed to quarantine former primary Pod %s: %w", key, err)
	}
	return nil
}

func (r *MysqlClusterReconciler) reconcileMysqlFailoverFencing(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (ctrl.Result, bool, error) {
	logMysqlControlBarrier(ctx, "fencing", cluster)
	if cluster.Status.HA == nil || cluster.Status.HA.Failover == nil {
		return ctrl.Result{}, false, fmt.Errorf("active MySQL fencing requires durable failover status")
	}
	failover := cluster.Status.HA.Failover
	if failover.Stage != databasev1.MysqlClusterFailoverStageFencing ||
		failover.FailedPrimary == "" || failover.FailedPrimaryUID == "" ||
		failover.FenceMethod != databasev1.MysqlClusterFenceMethodMySQLSuperReadOnly {
		return ctrl.Result{}, false, fmt.Errorf("invalid durable MySQL fencing status")
	}
	if failover.FenceState == databasev1.MysqlClusterFenceStateVerified &&
		failover.FencedPrimaryUID != failover.FailedPrimaryUID {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}

	pod, role, err := r.getValidatedMysqlFailedPrimary(ctx, cluster, failover)
	if err != nil {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}
	if role == mysqlPublishedRoleNone {
		if _, err := r.listMysqlStatefulSetPods(ctx, cluster); err != nil {
			return ctrl.Result{}, false, err
		}
	}
	writeSafety, err := r.observeMysqlWriteSafety(ctx, pod, cluster)
	if err != nil {
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}

	switch writeSafety.WriteRole {
	case mysqlWriteRoleReadOnly:
		if failover.FenceState != databasev1.MysqlClusterFenceStateVerified ||
			failover.FencedPrimaryUID != failover.FailedPrimaryUID ||
			cluster.Status.HA.State != databasev1.MysqlClusterHAStateFailoverInProgress {
			return r.persistMysqlFenceVerified(ctx, cluster)
		}
		if role == mysqlPublishedRoleMaster {
			if err := r.clearMysqlPublishedRole(ctx, cluster, pod.Name, failover.FailedPrimaryUID); err != nil {
				return r.persistMysqlFencingBlocked(ctx, cluster)
			}
			return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil
		}
		if role != mysqlPublishedRoleNone {
			return r.persistMysqlFencingBlocked(ctx, cluster)
		}
		return r.reconcileMysqlGTIDElection(ctx, cluster)

	case mysqlWriteRoleWritable:
		if failover.FenceState == databasev1.MysqlClusterFenceStateVerified || failover.FencedPrimaryUID != "" {
			return r.persistMysqlFencingPending(ctx, cluster)
		}
		recovered, recoveryErr := r.mysqlFailedPrimaryRecovered(ctx, cluster, pod)
		if recoveryErr != nil {
			return r.persistMysqlFencingBlocked(ctx, cluster)
		}
		if recovered && role == mysqlPublishedRoleMaster {
			return r.abortMysqlFailoverBeforeFence(ctx, cluster, pod)
		}
		if failover.FenceState != databasev1.MysqlClusterFenceStatePending ||
			cluster.Status.HA.State != databasev1.MysqlClusterHAStateFailoverInProgress {
			return r.persistMysqlFencingPending(ctx, cluster)
		}

		freshPod, freshRole, err := r.getValidatedMysqlFailedPrimary(ctx, cluster, failover)
		if err != nil {
			return r.persistMysqlFencingBlocked(ctx, cluster)
		}
		recovered, recoveryErr = r.mysqlFailedPrimaryRecovered(ctx, cluster, freshPod)
		if recoveryErr != nil {
			return r.persistMysqlFencingBlocked(ctx, cluster)
		}
		if recovered && freshRole == mysqlPublishedRoleMaster {
			return r.abortMysqlFailoverBeforeFence(ctx, cluster, freshPod)
		}
		if _, err := r.executeCommandOnPod(freshPod, mysqlSetSuperReadOnlyCommand()); err != nil {
			return r.persistMysqlFencingBlocked(ctx, cluster)
		}
		return ctrl.Result{RequeueAfter: mysqlHAFailureRequeueAfter}, false, nil

	case mysqlWriteRoleUnknown, mysqlWriteRoleUnsupported:
		return r.persistMysqlFencingBlocked(ctx, cluster)

	default:
		return r.persistMysqlFencingBlocked(ctx, cluster)
	}
}

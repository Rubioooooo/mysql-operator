package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mysqlReplicaConvergenceAction string

const (
	mysqlReplicaConvergenceNoop        mysqlReplicaConvergenceAction = "noop"
	mysqlReplicaConvergenceConfigure   mysqlReplicaConvergenceAction = "configure"
	mysqlReplicaConvergenceReconfigure mysqlReplicaConvergenceAction = "reconfigure"
	mysqlReplicaConvergenceWait        mysqlReplicaConvergenceAction = "wait"
	mysqlReplicaConvergenceWriteSafety mysqlReplicaConvergenceAction = "write-safety"
	mysqlReplicaConvergenceProvenance  mysqlReplicaConvergenceAction = "provenance"
)

type mysqlReplicaConvergenceResult struct {
	Action    mysqlReplicaConvergenceAction
	Converged bool
	Mutated   bool
}

func classifyMysqlReplicaConvergence(
	channel mysqlReplicationChannelObservation,
	expectedMasterHost string,
) mysqlReplicaConvergenceAction {
	switch {
	case !channel.Configured:
		return mysqlReplicaConvergenceConfigure
	case !channel.configurationMatches(expectedMasterHost):
		return mysqlReplicaConvergenceReconfigure
	case channel.semanticallyHealthy(expectedMasterHost):
		return mysqlReplicaConvergenceNoop
	default:
		return mysqlReplicaConvergenceWait
	}
}

func mysqlReplicaNeedsStrictBootstrapProof(cluster *databasev1.MysqlCluster, ordinal int32) (bool, error) {
	initialized, err := mysqlClusterIsInitialized(cluster)
	if err != nil {
		return false, err
	}
	return !initialized || mysqlActiveScaleUpOrdinal(cluster, ordinal), nil
}

func (r *MysqlClusterReconciler) getMysqlAuthoritativeHealthyPrimaryForReplicaRepair(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (*corev1.Pod, error) {
	if cluster.Status.HA == nil || cluster.Status.HA.State != databasev1.MysqlClusterHAStateHealthy || cluster.Status.HA.Failover != nil {
		return nil, fmt.Errorf("established replica repair requires durable healthy primary authority")
	}
	observation, err := r.observeMysqlPrimaryFailure(ctx, cluster)
	if err != nil || observation.Classification != mysqlPrimaryHealthy || !mysqlHAIdentityMatches(cluster.Status.HA, observation) {
		return nil, fmt.Errorf("established replica repair primary authority is ambiguous")
	}
	primary, err := r.observeSinglePublishedPrimary(ctx, cluster)
	if err != nil || primary.Name != observation.PrimaryName || string(primary.UID) != observation.PrimaryUID || !mysqlStatefulSetPodHealthy(primary) {
		return nil, fmt.Errorf("established replica repair primary identity is invalid")
	}
	endpoints, err := r.observeMysqlPrimaryRoutingEndpoints(ctx, cluster)
	if err != nil || !mysqlPublishedPrimaryRoutingAvailable(primary, endpoints) {
		return nil, fmt.Errorf("established replica repair primary routing authority is unavailable")
	}
	fresh := &corev1.Pod{}
	key := client.ObjectKeyFromObject(primary)
	if err := r.Get(ctx, key, fresh); err != nil {
		return nil, err
	}
	if fresh.UID != primary.UID || !mysqlStatefulSetPodHealthy(fresh) {
		return nil, fmt.Errorf("established replica repair primary Pod identity changed")
	}
	if err := r.validateStatefulSetManagedMysqlPod(ctx, fresh, cluster); err != nil {
		return nil, err
	}
	ordinal, err := mysqlStatefulSetPodOrdinal(fresh)
	if err != nil || fresh.Name != mysqlStatefulSetPodName(cluster, ordinal) {
		return nil, fmt.Errorf("established replica repair primary has invalid canonical identity")
	}
	role, err := observeMysqlPublishedRole(fresh)
	if err != nil || role != mysqlPublishedRoleMaster {
		return nil, fmt.Errorf("established replica repair primary publication changed")
	}
	return fresh, nil
}

func (r *MysqlClusterReconciler) proveMysqlEstablishedReplicaChannelRepair(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	replica *corev1.Pod,
) error {
	replicaReference, err := r.observeMysqlElectionReference(ctx, replica, cluster)
	if err != nil {
		return err
	}
	if err := r.validateMysqlGTIDBootstrapIdentity(ctx, cluster, replica, replicaReference.ServerUUID); err != nil {
		return err
	}
	replication, err := r.observeMysqlMemberReplication(ctx, replica, cluster)
	if err != nil {
		return err
	}
	if replication.Channel.Configured {
		return fmt.Errorf("established replica channel reappeared during repair proof")
	}

	primary, err := r.getMysqlAuthoritativeHealthyPrimaryForReplicaRepair(ctx, cluster)
	if err != nil {
		return err
	}
	writeSafety, err := r.observeMysqlWriteSafety(ctx, primary, cluster)
	if err != nil || !writeSafety.GTIDReady || writeSafety.ReadOnly || writeSafety.SuperReadOnly || writeSafety.WriteRole != mysqlWriteRoleWritable {
		return fmt.Errorf("established replica repair primary is not a GTID-ready writable authority")
	}
	sourceReady, err := r.observeMysqlSourceCapability(ctx, primary, cluster)
	if err != nil || !sourceReady {
		return fmt.Errorf("established replica repair primary is not source capable")
	}
	primaryReference, err := r.observeMysqlElectionReference(ctx, primary, cluster)
	if err != nil {
		return err
	}
	if err := r.validateMysqlGTIDBootstrapIdentity(ctx, cluster, primary, primaryReference.ServerUUID); err != nil {
		return err
	}
	trusted, err := mysqlTrustedBootstrapGTIDSet(cluster)
	if err != nil {
		return err
	}
	output, err := r.executeCommandOnPod(primary, mysqlMemberAncestryAgainstCurrentPrimaryCommand(replicaReference.GTIDSet, trusted))
	if err != nil {
		return fmt.Errorf("failed to prove established replica ancestry: %w", err)
	}
	ancestry, err := parseMysqlMemberAncestryObservation(output)
	if err != nil {
		return err
	}
	if !ancestry.MemberSubsetOfPrimary {
		return fmt.Errorf("established replica effective GTID is not a subset of the authoritative primary")
	}

	freshCluster := &databasev1.MysqlCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), freshCluster); err != nil {
		return err
	}
	if freshCluster.UID != cluster.UID || freshCluster.ResourceVersion != cluster.ResourceVersion {
		return fmt.Errorf("established replica repair authority changed during ancestry proof")
	}
	freshPrimary, err := r.getMysqlAuthoritativeHealthyPrimaryForReplicaRepair(ctx, cluster)
	if err != nil || freshPrimary.Name != primary.Name || freshPrimary.UID != primary.UID {
		return fmt.Errorf("established replica repair primary changed during ancestry proof")
	}
	return nil
}

func (r *MysqlClusterReconciler) reconcileMysqlReplicaChannel(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
) (mysqlReplicaConvergenceResult, error) {
	if err := validateMysqlReplicationMasterHost(cluster); err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}

	freshPod, err := r.getFreshMysqlReplicaConvergencePod(ctx, pod, cluster, "write-safety observation")
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	writeSafety, err := r.observeMysqlWriteSafety(ctx, freshPod, cluster)
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	if !writeSafety.GTIDReady {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"replica Pod %s/%s is not GTID-ready for replication convergence: gtid_mode=%q, enforce_gtid_consistency=%q",
			freshPod.Namespace,
			freshPod.Name,
			writeSafety.GTIDMode,
			writeSafety.EnforceGTIDConsistency,
		)
	}
	if !writeSafety.ReadOnly || !writeSafety.SuperReadOnly {
		freshPod, err = r.getFreshMysqlReplicaConvergencePod(ctx, pod, cluster, "write-safety mutation")
		if err != nil {
			return mysqlReplicaConvergenceResult{}, err
		}
		if _, err := r.executeCommandOnPod(freshPod, mysqlSetSuperReadOnlyCommand()); err != nil {
			return mysqlReplicaConvergenceResult{}, fmt.Errorf(
				"failed to enable super_read_only on replica Pod %s/%s: %w",
				freshPod.Namespace,
				freshPod.Name,
				err,
			)
		}
		return mysqlReplicaConvergenceResult{Action: mysqlReplicaConvergenceWriteSafety, Mutated: true}, nil
	}

	observation, err := r.observeMysqlMemberReplication(ctx, freshPod, cluster)
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	ordinal, err := mysqlStatefulSetPodOrdinal(freshPod)
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	entry, hasProvenance, err := mysqlGTIDBootstrapEntry(cluster, ordinal)
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	establishedChannelRepair := false
	if !observation.Channel.Configured {
		strictBootstrap, err := mysqlReplicaNeedsStrictBootstrapProof(cluster, ordinal)
		if err != nil {
			return mysqlReplicaConvergenceResult{}, err
		}
		if strictBootstrap {
			ready, barrier, err := r.reconcileMysqlScaleUpGTIDBootstrap(
				ctx,
				cluster,
				mysqlStatefulSetMember{Ordinal: ordinal, Pod: freshPod},
			)
			if err != nil {
				return mysqlReplicaConvergenceResult{}, err
			}
			if barrier {
				return mysqlReplicaConvergenceResult{Action: mysqlReplicaConvergenceProvenance, Mutated: true}, nil
			}
			if !ready {
				return mysqlReplicaConvergenceResult{}, fmt.Errorf("replica ordinal %d lacks verified GTID bootstrap provenance", ordinal)
			}
		} else {
			if err := r.proveMysqlEstablishedReplicaChannelRepair(ctx, cluster, freshPod); err != nil {
				return mysqlReplicaConvergenceResult{}, err
			}
			establishedChannelRepair = true
		}
	} else if hasProvenance {
		reference, err := r.observeMysqlElectionReference(ctx, freshPod, cluster)
		if err != nil {
			return mysqlReplicaConvergenceResult{}, err
		}
		if entry.ServerUUID != reference.ServerUUID {
			return mysqlReplicaConvergenceResult{}, fmt.Errorf("replica ordinal %d MySQL server identity changed", ordinal)
		}
		if err := r.validateMysqlGTIDBootstrapIdentity(ctx, cluster, freshPod, reference.ServerUUID); err != nil {
			return mysqlReplicaConvergenceResult{}, err
		}
	}

	action := classifyMysqlReplicaConvergence(observation.Channel, cluster.Spec.MasterService)
	switch action {
	case mysqlReplicaConvergenceNoop:
		return mysqlReplicaConvergenceResult{Action: action, Converged: true}, nil
	case mysqlReplicaConvergenceWait:
		return mysqlReplicaConvergenceResult{Action: action}, nil
	}

	currentPod, err := r.getFreshMysqlReplicaConvergencePod(ctx, pod, cluster, string(action)+" mutation")
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	currentOrdinal, err := mysqlStatefulSetPodOrdinal(currentPod)
	if err != nil {
		return mysqlReplicaConvergenceResult{}, err
	}
	if currentOrdinal != observation.Ordinal || currentPod.Name != observation.PodName {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"refusing %s for replica Pod %s: observed member identity %s ordinal %d changed to %s ordinal %d",
			action,
			client.ObjectKeyFromObject(pod),
			observation.PodName,
			observation.Ordinal,
			currentPod.Name,
			currentOrdinal,
		)
	}
	if establishedChannelRepair {
		if err := r.proveMysqlEstablishedReplicaChannelRepair(ctx, cluster, currentPod); err != nil {
			return mysqlReplicaConvergenceResult{}, err
		}
	}

	var command string
	switch action {
	case mysqlReplicaConvergenceConfigure:
		command = mysqlInitializeReplicaCommand(cluster.Spec.MasterService)
	case mysqlReplicaConvergenceReconfigure:
		command = mysqlConfigureReplicaCommand(cluster.Spec.MasterService)
	default:
		return mysqlReplicaConvergenceResult{}, fmt.Errorf("unsupported replica convergence action %q", action)
	}
	if _, err := r.executeCommandOnPod(currentPod, command); err != nil {
		return mysqlReplicaConvergenceResult{}, fmt.Errorf(
			"failed to %s replication channel on Pod %s/%s: %w",
			action,
			currentPod.Namespace,
			currentPod.Name,
			err,
		)
	}

	return mysqlReplicaConvergenceResult{Action: action, Mutated: true}, nil
}

func (r *MysqlClusterReconciler) getFreshMysqlReplicaConvergencePod(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
	purpose string,
) (*corev1.Pod, error) {
	observedOrdinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil {
		return nil, err
	}
	if expectedName := mysqlStatefulSetPodName(cluster, observedOrdinal); pod.Name != expectedName {
		return nil, fmt.Errorf(
			"Pod %s/%s ordinal identity does not match %s label %d: expected name %s",
			pod.Namespace,
			pod.Name,
			statefulSetPodIndexLabel,
			observedOrdinal,
			expectedName,
		)
	}

	podKey := client.ObjectKeyFromObject(pod)
	freshPod := &corev1.Pod{}
	if err := r.Get(ctx, podKey, freshPod); err != nil {
		return nil, fmt.Errorf("failed to re-fetch replica Pod %s before %s: %w", podKey, purpose, err)
	}
	if freshPod.UID != pod.UID {
		return nil, fmt.Errorf(
			"refusing %s for replica Pod %s: observed UID %q changed to %q",
			purpose,
			podKey,
			pod.UID,
			freshPod.UID,
		)
	}
	if err := r.validateMysqlPodBeforeSQL(ctx, freshPod, cluster, purpose+" SQL"); err != nil {
		return nil, err
	}
	freshOrdinal, err := mysqlStatefulSetPodOrdinal(freshPod)
	if err != nil {
		return nil, err
	}
	if expectedName := mysqlStatefulSetPodName(cluster, freshOrdinal); freshPod.Name != expectedName {
		return nil, fmt.Errorf(
			"Pod %s/%s ordinal identity does not match %s label %d: expected name %s",
			freshPod.Namespace,
			freshPod.Name,
			statefulSetPodIndexLabel,
			freshOrdinal,
			expectedName,
		)
	}
	if freshOrdinal != observedOrdinal {
		return nil, fmt.Errorf(
			"refusing %s for replica Pod %s: observed ordinal %d changed to %d",
			purpose,
			podKey,
			observedOrdinal,
			freshOrdinal,
		)
	}
	return freshPod, nil
}

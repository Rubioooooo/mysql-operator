package controller

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mysqlGTIDBootstrapObservation struct {
	ServerUUID      string
	GTIDPurged      string
	GTIDExecuted    string
	ExecutedOwnOnly bool
}

type mysqlGTIDBootstrapProof struct {
	PodUID string
	Entry  databasev1.MysqlClusterGTIDBootstrapStatus
}

const mysqlGTIDMaxGNO int64 = 9223372036854775806

func mysqlGTIDBootstrapObservationCommand() string {
	return mysqlRootClientCommand + fmt.Sprintf(
		` -Nse "SELECT @@GLOBAL.server_uuid, REPLACE(TO_BASE64(@@GLOBAL.gtid_purged), CHAR(10), ''), REPLACE(TO_BASE64(@@GLOBAL.gtid_executed), CHAR(10), ''), GTID_SUBSET(@@GLOBAL.gtid_executed, CONCAT(@@GLOBAL.server_uuid, ':1-%d'));"`,
		mysqlGTIDMaxGNO,
	)
}

func parseMysqlGTIDBootstrapObservation(output string) (mysqlGTIDBootstrapObservation, error) {
	fields, err := parseMysqlSingleRow(output, "GTID bootstrap provenance", 4)
	if err != nil {
		return mysqlGTIDBootstrapObservation{}, err
	}
	if !mysqlServerUUIDPattern.MatchString(fields[0]) {
		return mysqlGTIDBootstrapObservation{}, fmt.Errorf("malformed MySQL GTID bootstrap provenance: invalid server_uuid %q", fields[0])
	}
	purged, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return mysqlGTIDBootstrapObservation{}, fmt.Errorf("malformed MySQL gtid_purged encoding: %w", err)
	}
	executed, err := base64.StdEncoding.DecodeString(fields[2])
	if err != nil {
		return mysqlGTIDBootstrapObservation{}, fmt.Errorf("malformed MySQL gtid_executed encoding: %w", err)
	}
	ownOnly, err := parseMysqlBoolean("bootstrap GTID ownership", fields[3])
	if err != nil {
		return mysqlGTIDBootstrapObservation{}, err
	}
	return mysqlGTIDBootstrapObservation{
		ServerUUID:      fields[0],
		GTIDPurged:      string(purged),
		GTIDExecuted:    string(executed),
		ExecutedOwnOnly: ownOnly,
	}, nil
}

func (r *MysqlClusterReconciler) observeMysqlGTIDBootstrap(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
) (mysqlGTIDBootstrapObservation, error) {
	if err := r.validateMysqlPodBeforeSQL(ctx, pod, cluster, "GTID bootstrap provenance SQL"); err != nil {
		return mysqlGTIDBootstrapObservation{}, err
	}
	output, err := r.executeCommandOnPod(pod, mysqlGTIDBootstrapObservationCommand())
	if err != nil {
		return mysqlGTIDBootstrapObservation{}, fmt.Errorf("failed to observe GTID bootstrap provenance on Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return parseMysqlGTIDBootstrapObservation(output)
}

func validateMysqlGTIDBootstrapStatus(entries []databasev1.MysqlClusterGTIDBootstrapStatus) error {
	ordinals := make(map[int32]struct{}, len(entries))
	pvcUIDs := make(map[string]struct{}, len(entries))
	serverUUIDs := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.Ordinal < 1 {
			return fmt.Errorf("GTID bootstrap ordinal %d must be at least 1", entry.Ordinal)
		}
		if _, exists := ordinals[entry.Ordinal]; exists {
			return fmt.Errorf("duplicate GTID bootstrap ordinal %d", entry.Ordinal)
		}
		ordinals[entry.Ordinal] = struct{}{}
		if entry.PVCUID == "" {
			return fmt.Errorf("GTID bootstrap ordinal %d has empty PVC UID", entry.Ordinal)
		}
		if _, exists := pvcUIDs[entry.PVCUID]; exists {
			return fmt.Errorf("duplicate GTID bootstrap PVC UID at ordinal %d", entry.Ordinal)
		}
		pvcUIDs[entry.PVCUID] = struct{}{}
		if !mysqlServerUUIDPattern.MatchString(entry.ServerUUID) {
			return fmt.Errorf("GTID bootstrap ordinal %d has invalid server UUID", entry.Ordinal)
		}
		if _, exists := serverUUIDs[entry.ServerUUID]; exists {
			return fmt.Errorf("duplicate GTID bootstrap server UUID at ordinal %d", entry.Ordinal)
		}
		serverUUIDs[entry.ServerUUID] = struct{}{}
	}
	return nil
}

func mysqlGTIDBootstrapEntry(cluster *databasev1.MysqlCluster, ordinal int32) (*databasev1.MysqlClusterGTIDBootstrapStatus, bool, error) {
	if err := validateMysqlGTIDBootstrapStatus(cluster.Status.GTIDBootstrap); err != nil {
		return nil, false, err
	}
	for i := range cluster.Status.GTIDBootstrap {
		if cluster.Status.GTIDBootstrap[i].Ordinal == ordinal {
			return &cluster.Status.GTIDBootstrap[i], true, nil
		}
	}
	return nil, false, nil
}

func mysqlTrustedBootstrapGTIDSet(cluster *databasev1.MysqlCluster) (string, error) {
	if len(cluster.Status.GTIDBootstrap) == 0 {
		return "", fmt.Errorf("MysqlCluster %s/%s has no durable GTID bootstrap provenance", cluster.Namespace, cluster.Name)
	}
	if err := validateMysqlGTIDBootstrapStatus(cluster.Status.GTIDBootstrap); err != nil {
		return "", err
	}
	entries := append([]databasev1.MysqlClusterGTIDBootstrapStatus(nil), cluster.Status.GTIDBootstrap...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Ordinal < entries[j].Ordinal })
	sets := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.BootstrapGTIDSet != "" {
			sets = append(sets, entry.BootstrapGTIDSet)
		}
	}
	return strings.Join(sets, ","), nil
}

func mysqlPodDataPVCName(pod *corev1.Pod) (string, error) {
	expected := mysqlDataVolume + "-" + pod.Name
	for _, volume := range pod.Spec.Volumes {
		if volume.Name != mysqlDataVolume {
			continue
		}
		if volume.PersistentVolumeClaim == nil || volume.PersistentVolumeClaim.ClaimName != expected {
			return "", fmt.Errorf("Pod %s/%s has invalid canonical MySQL data PVC reference", pod.Namespace, pod.Name)
		}
		return expected, nil
	}
	return "", fmt.Errorf("Pod %s/%s has no MySQL data PVC reference", pod.Namespace, pod.Name)
}

func (r *MysqlClusterReconciler) mysqlPodDataPVCUID(ctx context.Context, pod *corev1.Pod) (string, error) {
	claimName, err := mysqlPodDataPVCName(pod)
	if err != nil {
		return "", err
	}
	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: claimName}, pvc); err != nil {
		return "", fmt.Errorf("failed to get MySQL data PVC %s/%s: %w", pod.Namespace, claimName, err)
	}
	if pvc.UID == "" {
		return "", fmt.Errorf("MySQL data PVC %s/%s has no UID", pvc.Namespace, pvc.Name)
	}
	return string(pvc.UID), nil
}

func (r *MysqlClusterReconciler) validateMysqlGTIDBootstrapIdentity(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	pod *corev1.Pod,
	serverUUID string,
) error {
	ordinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil || pod.Name != mysqlStatefulSetPodName(cluster, ordinal) {
		return fmt.Errorf("GTID bootstrap member has invalid canonical ordinal identity")
	}
	entry, found, err := mysqlGTIDBootstrapEntry(cluster, ordinal)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("missing durable GTID bootstrap provenance for ordinal %d", ordinal)
	}
	pvcUID, err := r.mysqlPodDataPVCUID(ctx, pod)
	if err != nil {
		return err
	}
	if entry.PVCUID != pvcUID {
		return fmt.Errorf("GTID bootstrap PVC identity changed for ordinal %d", ordinal)
	}
	if entry.ServerUUID != serverUUID {
		return fmt.Errorf("GTID bootstrap MySQL server identity changed for ordinal %d", ordinal)
	}
	return nil
}

func (r *MysqlClusterReconciler) validateMysqlGTIDBootstrapMembers(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	members []mysqlStatefulSetMember,
) error {
	if len(cluster.Status.GTIDBootstrap) == 0 {
		return fmt.Errorf("MysqlCluster %s/%s has no durable GTID bootstrap provenance", cluster.Namespace, cluster.Name)
	}
	for _, member := range members {
		reference, err := r.observeMysqlElectionReference(ctx, member.Pod, cluster)
		if err != nil {
			return err
		}
		if err := r.validateMysqlGTIDBootstrapIdentity(ctx, cluster, member.Pod, reference.ServerUUID); err != nil {
			return err
		}
	}
	return nil
}

func (r *MysqlClusterReconciler) observeMysqlGTIDBootstrapProof(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	member mysqlStatefulSetMember,
	requireReplicaReadOnly bool,
	requireUnpublished bool,
) (mysqlGTIDBootstrapProof, bool, error) {
	pod := &corev1.Pod{}
	key := client.ObjectKeyFromObject(member.Pod)
	if err := r.Get(ctx, key, pod); err != nil {
		return mysqlGTIDBootstrapProof{}, false, err
	}
	if pod.UID == "" || pod.UID != member.Pod.UID {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("GTID bootstrap Pod %s identity changed during observation", key)
	}
	if err := r.validateStatefulSetManagedMysqlPod(ctx, pod, cluster); err != nil {
		return mysqlGTIDBootstrapProof{}, false, err
	}
	ordinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil || ordinal != member.Ordinal || pod.Name != mysqlStatefulSetPodName(cluster, ordinal) {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("GTID bootstrap Pod %s has invalid canonical ordinal identity", key)
	}
	if !mysqlStatefulSetPodHealthy(pod) {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("GTID bootstrap Pod %s is not Ready", key)
	}
	role, err := observeMysqlPublishedRole(pod)
	if err != nil {
		return mysqlGTIDBootstrapProof{}, false, err
	}
	if requireUnpublished && role != mysqlPublishedRoleNone {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("GTID bootstrap requires all topology roles to remain unpublished")
	}
	if !requireUnpublished && role == mysqlPublishedRoleMaster {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("scale-up GTID bootstrap member cannot be published as master")
	}
	writeSafety, err := r.observeMysqlWriteSafety(ctx, pod, cluster)
	if err != nil {
		return mysqlGTIDBootstrapProof{}, false, err
	}
	if !writeSafety.GTIDReady {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("GTID bootstrap member ordinal %d is not GTID ready", ordinal)
	}
	if requireReplicaReadOnly && (!writeSafety.ReadOnly || !writeSafety.SuperReadOnly) {
		return mysqlGTIDBootstrapProof{}, true, nil
	}
	sourceReady, err := r.observeMysqlSourceCapability(ctx, pod, cluster)
	if err != nil || !sourceReady {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("GTID bootstrap member ordinal %d is not source capable", ordinal)
	}
	replication, err := r.observeMysqlMemberReplication(ctx, pod, cluster)
	if err != nil {
		return mysqlGTIDBootstrapProof{}, false, err
	}
	if replication.Channel.Configured {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("GTID bootstrap member ordinal %d already has a replication channel", ordinal)
	}
	observation, err := r.observeMysqlGTIDBootstrap(ctx, pod, cluster)
	if err != nil {
		return mysqlGTIDBootstrapProof{}, false, err
	}
	if observation.GTIDPurged != "" {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("GTID bootstrap member ordinal %d has non-empty gtid_purged", ordinal)
	}
	if !observation.ExecutedOwnOnly {
		return mysqlGTIDBootstrapProof{}, false, fmt.Errorf("GTID bootstrap member ordinal %d has non-local gtid_executed", ordinal)
	}
	pvcUID, err := r.mysqlPodDataPVCUID(ctx, pod)
	if err != nil {
		return mysqlGTIDBootstrapProof{}, false, err
	}
	return mysqlGTIDBootstrapProof{
		PodUID: string(pod.UID),
		Entry: databasev1.MysqlClusterGTIDBootstrapStatus{
			Ordinal:          ordinal,
			PVCUID:           pvcUID,
			ServerUUID:       observation.ServerUUID,
			BootstrapGTIDSet: observation.GTIDExecuted,
		},
	}, false, nil
}

func sameMysqlGTIDBootstrapProof(left, right mysqlGTIDBootstrapProof) bool {
	return left.PodUID == right.PodUID && left.Entry == right.Entry
}

func (r *MysqlClusterReconciler) persistMysqlGTIDBootstrap(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	additions []databasev1.MysqlClusterGTIDBootstrapStatus,
) error {
	if len(additions) == 0 {
		return fmt.Errorf("refusing empty GTID bootstrap persistence")
	}
	fresh := &databasev1.MysqlCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(cluster), fresh); err != nil {
		return err
	}
	if fresh.UID != cluster.UID || fresh.ResourceVersion != cluster.ResourceVersion {
		return fmt.Errorf("MysqlCluster changed during GTID bootstrap proof")
	}
	if err := validateMysqlGTIDBootstrapStatus(fresh.Status.GTIDBootstrap); err != nil {
		return err
	}
	updated := fresh.DeepCopy()
	updated.Status.GTIDBootstrap = append(updated.Status.GTIDBootstrap, additions...)
	sort.Slice(updated.Status.GTIDBootstrap, func(i, j int) bool {
		return updated.Status.GTIDBootstrap[i].Ordinal < updated.Status.GTIDBootstrap[j].Ordinal
	})
	if err := validateMysqlGTIDBootstrapStatus(updated.Status.GTIDBootstrap); err != nil {
		return err
	}
	if err := r.Status().Patch(ctx, updated, client.MergeFromWithOptions(fresh, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("failed to persist GTID bootstrap provenance: %w", err)
	}
	*cluster = *updated
	return nil
}

func mysqlActiveScaleUpOrdinal(cluster *databasev1.MysqlCluster, ordinal int32) bool {
	transition := cluster.Status.ReplicaTransition
	return transition != nil && transition.TargetReplicas > transition.FromReplicas &&
		ordinal > transition.FromReplicas && ordinal <= transition.TargetReplicas
}

func (r *MysqlClusterReconciler) reconcileMysqlInitialGTIDBootstrap(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (bool, bool, error) {
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return false, false, err
	}
	if int32(len(members)) != desiredReplicas(cluster) {
		return false, false, nil
	}
	if len(cluster.Status.GTIDBootstrap) != 0 && len(cluster.Status.GTIDBootstrap) != len(members) {
		return false, false, fmt.Errorf("initial GTID bootstrap provenance is incomplete")
	}
	first := make([]mysqlGTIDBootstrapProof, 0, len(members))
	for _, member := range members {
		proof, needsFence, err := r.observeMysqlGTIDBootstrapProof(ctx, cluster, member, member.Ordinal != 1, true)
		if err != nil {
			return false, false, err
		}
		if needsFence {
			fresh, err := r.getFreshMysqlReplicaConvergencePod(ctx, member.Pod, cluster, "initial GTID bootstrap write-safety mutation")
			if err != nil {
				return false, false, err
			}
			if _, err := r.executeCommandOnPod(fresh, mysqlSetSuperReadOnlyCommand()); err != nil {
				return false, false, err
			}
			return false, true, nil
		}
		first = append(first, proof)
	}
	if len(cluster.Status.GTIDBootstrap) != 0 {
		for _, proof := range first {
			entry, found, err := mysqlGTIDBootstrapEntry(cluster, proof.Entry.Ordinal)
			if err != nil || !found || *entry != proof.Entry {
				return false, false, fmt.Errorf("initial GTID bootstrap provenance no longer matches ordinal %d", proof.Entry.Ordinal)
			}
		}
		return true, false, nil
	}
	second := make([]databasev1.MysqlClusterGTIDBootstrapStatus, 0, len(first))
	for i, member := range members {
		proof, needsFence, err := r.observeMysqlGTIDBootstrapProof(ctx, cluster, member, member.Ordinal != 1, true)
		if err != nil || needsFence || !sameMysqlGTIDBootstrapProof(first[i], proof) {
			if err != nil {
				return false, false, err
			}
			return false, false, fmt.Errorf("GTID bootstrap proof changed before persistence")
		}
		second = append(second, proof.Entry)
	}
	if err := r.persistMysqlGTIDBootstrap(ctx, cluster, second); err != nil {
		return false, false, err
	}
	return false, true, nil
}

func (r *MysqlClusterReconciler) reconcileMysqlScaleUpGTIDBootstrap(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	member mysqlStatefulSetMember,
) (bool, bool, error) {
	entry, found, err := mysqlGTIDBootstrapEntry(cluster, member.Ordinal)
	if err != nil {
		return false, false, err
	}
	if found {
		proof, needsFence, err := r.observeMysqlGTIDBootstrapProof(ctx, cluster, member, true, false)
		if err != nil {
			return false, false, err
		}
		if needsFence {
			return false, false, fmt.Errorf("scale-up GTID bootstrap replica lost its write fence")
		}
		if entry.PVCUID != proof.Entry.PVCUID {
			return false, false, fmt.Errorf("scale-up GTID bootstrap PVC identity changed for ordinal %d", member.Ordinal)
		}
		if entry.ServerUUID != proof.Entry.ServerUUID {
			return false, false, fmt.Errorf("scale-up GTID bootstrap MySQL identity changed for ordinal %d", member.Ordinal)
		}
		if entry.BootstrapGTIDSet != proof.Entry.BootstrapGTIDSet {
			return false, false, fmt.Errorf("scale-up GTID bootstrap raw GTID changed for ordinal %d", member.Ordinal)
		}
		return true, false, nil
	}
	if !mysqlActiveScaleUpOrdinal(cluster, member.Ordinal) {
		return false, false, fmt.Errorf("refusing GTID bootstrap capture outside an active scale-up delta")
	}
	first, needsFence, err := r.observeMysqlGTIDBootstrapProof(ctx, cluster, member, true, false)
	if err != nil || needsFence {
		if err != nil {
			return false, false, err
		}
		return false, false, fmt.Errorf("scale-up GTID bootstrap replica lost its write fence")
	}
	second, needsFence, err := r.observeMysqlGTIDBootstrapProof(ctx, cluster, member, true, false)
	if err != nil || needsFence || !sameMysqlGTIDBootstrapProof(first, second) {
		if err != nil {
			return false, false, err
		}
		return false, false, fmt.Errorf("scale-up GTID bootstrap proof changed before persistence")
	}
	if err := r.persistMysqlGTIDBootstrap(ctx, cluster, []databasev1.MysqlClusterGTIDBootstrapStatus{second.Entry}); err != nil {
		return false, false, err
	}
	return false, true, nil
}

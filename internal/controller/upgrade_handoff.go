package controller

import (
	"context"
	"fmt"

	databasev1 "github.com/egonlin/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type mysqlHandoffMember struct {
	member    mysqlStatefulSetMember
	image     string
	role      mysqlPublishedRole
	write     mysqlWriteSafetyObservation
	reference mysqlElectionReference
	channel   mysqlReplicationChannelObservation
	source    bool
}

type mysqlHandoffObservation struct {
	sts       *appsv1.StatefulSet
	members   []mysqlHandoffMember
	endpoints *corev1.Endpoints
}

func (o *mysqlHandoffObservation) named(name string) *mysqlHandoffMember {
	for i := range o.members {
		if o.members[i].member.Pod.Name == name {
			return &o.members[i]
		}
	}
	return nil
}

func mysqlHandoffWritable(m *mysqlHandoffMember) bool {
	return m != nil && m.write.GTIDReady && !m.write.ReadOnly && !m.write.SuperReadOnly && m.write.WriteRole == mysqlWriteRoleWritable
}

func mysqlHandoffReadOnly(m *mysqlHandoffMember) bool {
	return m != nil && m.write.GTIDReady && m.write.ReadOnly && m.write.SuperReadOnly && m.write.WriteRole == mysqlWriteRoleReadOnly
}

func mysqlHandoffEntryAllowed(cluster *databasev1.MysqlCluster) bool {
	return cluster.Spec.Image == cluster.Status.Upgrade.TargetImage && mysqlUpgradeHAHealthy(cluster) && cluster.Status.ReplicaTransition == nil && cluster.Status.LastConvergedReplicas != nil && *cluster.Status.LastConvergedReplicas == desiredReplicas(cluster)
}

// During restoration use the converged workload checkpoint, never a new replica
// request. This function is read-only and cannot resize/recreate a StatefulSet.
func (r *MysqlClusterReconciler) observeMysqlHandoff(ctx context.Context, cluster *databasev1.MysqlCluster, restoring bool) (*mysqlHandoffObservation, error) {
	sts, members, err := r.observeMysqlUpgradeWorkload(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("handoff workload identity is invalid")
	}
	count := desiredReplicas(cluster)
	if restoring {
		if cluster.Status.LastConvergedReplicas == nil {
			return nil, fmt.Errorf("handoff requires converged membership checkpoint")
		}
		count = *cluster.Status.LastConvergedReplicas
	}
	image, err := mysqlWorkloadImage(&sts.Spec.Template.Spec)
	if err != nil || image != cluster.Status.Upgrade.TargetImage || sts.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType || sts.Spec.UpdateStrategy.RollingUpdate != nil || mysqlStatefulSetCurrentReplicas(sts) != count || !mysqlReplicaTransitionFullyConverged(members, count) {
		return nil, fmt.Errorf("handoff requires complete healthy OnDelete target workload")
	}
	o := &mysqlHandoffObservation{sts: sts}
	for _, member := range members {
		pod := member.Pod
		if pod.UID == "" || !pod.DeletionTimestamp.IsZero() {
			return nil, fmt.Errorf("handoff member is terminating or lacks identity")
		}
		image, err := mysqlWorkloadImage(&pod.Spec)
		if err != nil || (image != cluster.Status.Upgrade.FromImage && image != cluster.Status.Upgrade.TargetImage) {
			return nil, fmt.Errorf("unexpected handoff member image")
		}
		role, err := observeMysqlPublishedRole(pod)
		if err != nil {
			return nil, fmt.Errorf("ambiguous handoff member role")
		}
		write, err := r.observeMysqlWriteSafety(ctx, pod, cluster)
		if err != nil {
			return nil, fmt.Errorf("handoff write safety observation failed")
		}
		reference, err := r.observeMysqlElectionReference(ctx, pod, cluster)
		if err != nil {
			return nil, fmt.Errorf("handoff history observation failed")
		}
		replication, err := r.observeMysqlMemberReplication(ctx, pod, cluster)
		if err != nil {
			return nil, fmt.Errorf("handoff replication observation failed")
		}
		source, err := r.observeMysqlSourceCapability(ctx, pod, cluster)
		if err != nil {
			return nil, fmt.Errorf("handoff source capability observation failed")
		}
		o.members = append(o.members, mysqlHandoffMember{member: member, image: image, role: role, write: write, reference: reference, channel: replication.Channel, source: source})
	}
	o.endpoints, err = r.observeMysqlPrimaryRoutingEndpoints(ctx, cluster)
	return o, err
}

func (r *MysqlClusterReconciler) recheckMysqlHandoffIdentities(ctx context.Context, cluster *databasev1.MysqlCluster, o *mysqlHandoffObservation) error {
	sts, members, err := r.observeMysqlUpgradeWorkload(ctx, cluster)
	if err != nil || sts.UID != o.sts.UID || sts.ResourceVersion != o.sts.ResourceVersion || len(members) != len(o.members) {
		return fmt.Errorf("handoff workload changed during proof")
	}
	for i, member := range members {
		before := o.members[i]
		role, roleErr := observeMysqlPublishedRole(member.Pod)
		image, imageErr := mysqlWorkloadImage(&member.Pod.Spec)
		if member.Ordinal != before.member.Ordinal || member.Pod.Name != before.member.Pod.Name || member.Pod.UID != before.member.Pod.UID || !member.Pod.DeletionTimestamp.IsZero() || !mysqlStatefulSetPodHealthy(member.Pod) || roleErr != nil || role != before.role || imageErr != nil || image != before.image {
			return fmt.Errorf("handoff member identity changed during proof")
		}
	}
	endpoints, err := r.observeMysqlPrimaryRoutingEndpoints(ctx, cluster)
	if err != nil || !apiequality.Semantic.DeepEqual(endpoints, o.endpoints) {
		return fmt.Errorf("handoff routing changed during proof")
	}
	return r.recheckMysqlUpgradeSnapshot(ctx, cluster)
}

func mysqlHandoffSingleMaster(o *mysqlHandoffObservation, name, uid string, routed bool) bool {
	count := 0
	for _, m := range o.members {
		if m.role == mysqlPublishedRoleMaster {
			count++
			if m.member.Pod.Name != name || string(m.member.Pod.UID) != uid {
				return false
			}
		}
	}
	m := o.named(name)
	return count == 1 && m != nil && (!routed || mysqlPublishedPrimaryRoutingAvailable(m.member.Pod, o.endpoints))
}

func mysqlHandoffRoutingDrained(o *mysqlHandoffObservation) bool {
	if o.endpoints == nil {
		return true
	}
	for _, subset := range o.endpoints.Subsets {
		if len(subset.Addresses) > 0 || len(subset.NotReadyAddresses) > 0 {
			return false
		}
	}
	return true
}

func (r *MysqlClusterReconciler) mysqlHandoffEqual(ctx context.Context, cluster *databasev1.MysqlCluster, m *mysqlHandoffMember, checkpoint string) bool {
	comparison, err := r.compareMysqlCandidateGTID(ctx, m.member.Pod, cluster, checkpoint)
	return err == nil && comparison.Relation == mysqlGTIDRelationEqual
}

func mysqlHandoffChannelHealthy(m *mysqlHandoffMember, cluster *databasev1.MysqlCluster, source string) bool {
	return m.channel.semanticallyHealthy(cluster.Spec.MasterService) && m.channel.MasterUUID != "" && m.channel.MasterUUID == source
}

func (r *MysqlClusterReconciler) reconcileMysqlHandoffEntry(ctx context.Context, cluster *databasev1.MysqlCluster) error {
	if !mysqlHandoffEntryAllowed(cluster) || cluster.Status.Upgrade.Handoff != nil {
		return nil
	}
	o, err := r.observeMysqlHandoff(ctx, cluster, false)
	if err != nil {
		return err
	}
	primary := o.named(cluster.Status.HA.Primary)
	if primary == nil || !mysqlHandoffSingleMaster(o, primary.member.Pod.Name, cluster.Status.HA.PrimaryUID, true) || !mysqlHandoffWritable(primary) || !primary.source {
		return fmt.Errorf("planned handoff requires exact writable routed primary")
	}
	if primary.image == cluster.Status.Upgrade.TargetImage {
		return r.completeMysqlUpgrade(ctx, cluster)
	}
	for _, m := range o.members {
		if m.member.Pod.Name != primary.member.Pod.Name && (m.image != cluster.Status.Upgrade.TargetImage || m.role != mysqlPublishedRoleSlave || !mysqlHandoffReadOnly(&m) || !mysqlHandoffChannelHealthy(&m, cluster, primary.reference.ServerUUID)) {
			return fmt.Errorf("planned handoff requires verified target replicas")
		}
	}
	var candidate *mysqlHandoffMember
	for i := range o.members {
		m := &o.members[i]
		if m.member.Pod.Name != primary.member.Pod.Name && m.source && r.mysqlHandoffEqual(ctx, cluster, m, primary.reference.GTIDSet) {
			candidate = m
			break
		}
	}
	if candidate == nil {
		return fmt.Errorf("no safe target-image handoff candidate")
	}
	// Repeat the selected proof without changing the selected identities.
	fresh, err := r.observeMysqlHandoff(ctx, cluster, false)
	if err != nil {
		return err
	}
	p, c := fresh.named(primary.member.Pod.Name), fresh.named(candidate.member.Pod.Name)
	if p == nil || c == nil || p.image != cluster.Status.Upgrade.FromImage || c.image != cluster.Status.Upgrade.TargetImage || p.member.Pod.UID != primary.member.Pod.UID || c.member.Pod.UID != candidate.member.Pod.UID || !mysqlHandoffSingleMaster(fresh, p.member.Pod.Name, string(p.member.Pod.UID), true) || !mysqlHandoffWritable(p) || !p.source || !mysqlHandoffReadOnly(c) || !c.source || c.role != mysqlPublishedRoleSlave || !mysqlHandoffChannelHealthy(c, cluster, p.reference.ServerUUID) || !r.mysqlHandoffEqual(ctx, cluster, c, p.reference.GTIDSet) {
		return fmt.Errorf("handoff selection proof changed")
	}
	for _, m := range fresh.members {
		if m.member.Pod.Name != p.member.Pod.Name && (m.image != cluster.Status.Upgrade.TargetImage || m.role != mysqlPublishedRoleSlave || !mysqlHandoffReadOnly(&m) || !mysqlHandoffChannelHealthy(&m, cluster, p.reference.ServerUUID)) {
			return fmt.Errorf("handoff replica proof changed during selection")
		}
	}
	if err := r.recheckMysqlHandoffIdentities(ctx, cluster, fresh); err != nil {
		return err
	}
	upgrade := cluster.Status.Upgrade.DeepCopy()
	upgrade.Handoff = &databasev1.MysqlClusterUpgradeHandoffStatus{Stage: databasev1.MysqlClusterUpgradeHandoffStageFencing, OldPrimary: p.member.Pod.Name, OldPrimaryUID: string(p.member.Pod.UID), Candidate: c.member.Pod.Name, CandidateUID: string(c.member.Pod.UID)}
	return r.persistMysqlClusterUpgradeStatus(ctx, cluster, cluster.Status.LastConvergedImage, upgrade)
}

type mysqlHandoffAction struct {
	kind    string
	name    string
	command string
	role    mysqlPublishedRole
}

// The handoff owns its intentional zero-master interval. Failover, when really
// present, remains a separate authority and is never synthesized by this code.
func (r *MysqlClusterReconciler) reconcileMysqlHandoffPreRuntime(ctx context.Context, cluster *databasev1.MysqlCluster) (bool, error) {
	h := cluster.Status.Upgrade.Handoff
	if cluster.Status.HA != nil && cluster.Status.HA.Failover != nil {
		return false, nil
	}
	if h.Stage == databasev1.MysqlClusterUpgradeHandoffStageFencing {
		pod := &corev1.Pod{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: h.OldPrimary}, pod); err != nil {
			return true, fmt.Errorf("handoff old primary is missing")
		}
		if string(pod.UID) != h.OldPrimaryUID {
			return true, fmt.Errorf("handoff old primary identity changed")
		}
		write, err := r.observeMysqlWriteSafety(ctx, pod, cluster)
		// A crash may have followed a successful SRO command. An unavailable
		// observation is not proof that ordinary topology changes are safe.
		if err != nil || (write.WriteRole != mysqlWriteRoleWritable && write.WriteRole != mysqlWriteRoleReadOnly) {
			return true, fmt.Errorf("planned write-fence state is unknown")
		}
		fenced := write.ReadOnly && write.SuperReadOnly
		if !fenced && !mysqlHandoffEntryAllowed(cluster) {
			return false, nil
		}
	}
	first, err := r.observeMysqlHandoff(ctx, cluster, true)
	if err != nil {
		return true, err
	}
	action, err := r.planMysqlHandoffAction(ctx, cluster, first)
	if err != nil {
		return true, err
	}
	if action.kind == "wait" {
		return true, nil
	}
	fresh, err := r.observeMysqlHandoff(ctx, cluster, true)
	if err != nil {
		return true, err
	}
	confirmed, err := r.planMysqlHandoffAction(ctx, cluster, fresh)
	if err != nil {
		return true, err
	}
	if action != confirmed {
		return true, fmt.Errorf("handoff safety observation changed before barrier")
	}
	for i := range first.members {
		if first.members[i].member.Pod.UID != fresh.members[i].member.Pod.UID {
			return true, fmt.Errorf("handoff identity changed during safety proof")
		}
	}
	if err := r.recheckMysqlHandoffIdentities(ctx, cluster, fresh); err != nil {
		return true, err
	}
	if err := r.recheckMysqlHandoffAuthority(ctx, cluster, fresh, confirmed); err != nil {
		return true, err
	}
	return true, r.executeMysqlHandoffAction(ctx, cluster, fresh, confirmed)
}

// The authority is checked again AFTER candidate/ancestry SQL. A changing
// source must not authorize unpublication or attachment from an earlier read.
func (r *MysqlClusterReconciler) recheckMysqlHandoffAuthority(ctx context.Context, cluster *databasev1.MysqlCluster, o *mysqlHandoffObservation, action mysqlHandoffAction) error {
	h := cluster.Status.Upgrade.Handoff
	old, candidate := o.named(h.OldPrimary), o.named(h.Candidate)
	primary := old
	if h.Stage == databasev1.MysqlClusterUpgradeHandoffStageReconfiguring || candidate.role == mysqlPublishedRoleMaster {
		primary = candidate
	}
	write, err := r.observeMysqlWriteSafety(ctx, primary.member.Pod, cluster)
	if err != nil {
		return fmt.Errorf("handoff authority write safety changed")
	}
	capable, err := r.observeMysqlSourceCapability(ctx, primary.member.Pod, cluster)
	if err != nil || (primary == candidate && !capable) {
		return fmt.Errorf("handoff authority source capability changed")
	}
	reference, err := r.observeMysqlElectionReference(ctx, primary.member.Pod, cluster)
	if err != nil || reference.ServerUUID != primary.reference.ServerUUID {
		return fmt.Errorf("handoff authority server identity changed")
	}
	if primary == candidate {
		if write.ReadOnly || write.SuperReadOnly || !write.GTIDReady || write.WriteRole != mysqlWriteRoleWritable {
			return fmt.Errorf("handoff new authority is not writable")
		}
	}
	candidateWrite, err := r.observeMysqlWriteSafety(ctx, candidate.member.Pod, cluster)
	if err != nil || candidateWrite.ReadOnly != candidate.write.ReadOnly || candidateWrite.SuperReadOnly != candidate.write.SuperReadOnly || !candidateWrite.GTIDReady {
		return fmt.Errorf("candidate write safety changed before handoff barrier")
	}
	candidateSource, err := r.observeMysqlSourceCapability(ctx, candidate.member.Pod, cluster)
	if err != nil || !candidateSource {
		return fmt.Errorf("candidate lost source capability before handoff barrier")
	}
	if h.Stage != databasev1.MysqlClusterUpgradeHandoffStageReconfiguring {
		oldWrite, err := r.observeMysqlWriteSafety(ctx, old.member.Pod, cluster)
		if err != nil {
			return fmt.Errorf("old primary write safety recheck failed")
		}
		if h.Stage != databasev1.MysqlClusterUpgradeHandoffStageFencing || action.kind == "checkpoint" {
			checkpoint := old.reference.GTIDSet
			uuid := old.reference.ServerUUID
			if h.Stage != databasev1.MysqlClusterUpgradeHandoffStageFencing {
				checkpoint = *h.OldPrimaryGTIDSet
				uuid = h.OldPrimaryServerUUID
			}
			oldRef, err := r.observeMysqlElectionReference(ctx, old.member.Pod, cluster)
			if err != nil || oldRef.ServerUUID != uuid || !oldWrite.ReadOnly || !oldWrite.SuperReadOnly || !oldWrite.GTIDReady || !r.mysqlHandoffEqual(ctx, cluster, old, checkpoint) {
				return fmt.Errorf("old primary durable fence changed before barrier")
			}
		} else if oldWrite.ReadOnly || oldWrite.SuperReadOnly || !oldWrite.GTIDReady {
			return fmt.Errorf("old primary changed before planned fence")
		}
	}
	return r.recheckMysqlHandoffIdentities(ctx, cluster, o)
}

func (r *MysqlClusterReconciler) validateMysqlHandoffFence(ctx context.Context, cluster *databasev1.MysqlCluster, old *mysqlHandoffMember) error {
	h := cluster.Status.Upgrade.Handoff
	if !mysqlHandoffReadOnly(old) || old.reference.ServerUUID != h.OldPrimaryServerUUID || !r.mysqlHandoffEqual(ctx, cluster, old, *h.OldPrimaryGTIDSet) {
		return fmt.Errorf("durable old-primary fence or history changed")
	}
	return nil
}

func (r *MysqlClusterReconciler) planMysqlHandoffAction(ctx context.Context, cluster *databasev1.MysqlCluster, o *mysqlHandoffObservation) (mysqlHandoffAction, error) {
	wait := mysqlHandoffAction{kind: "wait"}
	h := cluster.Status.Upgrade.Handoff
	old, candidate := o.named(h.OldPrimary), o.named(h.Candidate)
	if old == nil || candidate == nil || string(old.member.Pod.UID) != h.OldPrimaryUID || string(candidate.member.Pod.UID) != h.CandidateUID || old.image != cluster.Status.Upgrade.FromImage || candidate.image != cluster.Status.Upgrade.TargetImage {
		return wait, fmt.Errorf("durable handoff identities or images changed")
	}
	for _, m := range o.members {
		if m.member.Pod.Name != h.OldPrimary && m.image != cluster.Status.Upgrade.TargetImage {
			return wait, fmt.Errorf("handoff acquired an unexpected old-image member")
		}
	}
	if h.Stage == databasev1.MysqlClusterUpgradeHandoffStageReconfiguring {
		return r.planMysqlHandoffRejoin(ctx, cluster, o)
	}
	for _, m := range o.members {
		if m.member.Pod.Name != h.OldPrimary && m.member.Pod.Name != h.Candidate && (m.image != cluster.Status.Upgrade.TargetImage || m.role != mysqlPublishedRoleSlave || !mysqlHandoffReadOnly(&m)) {
			return wait, fmt.Errorf("unexpected role or writable node during handoff")
		}
	}
	if !candidate.source || !candidate.write.GTIDReady {
		return wait, fmt.Errorf("handoff candidate lost source capability")
	}
	if h.Stage == databasev1.MysqlClusterUpgradeHandoffStageFencing {
		if cluster.Status.HA == nil || cluster.Status.HA.Primary != h.OldPrimary || cluster.Status.HA.PrimaryUID != h.OldPrimaryUID {
			return wait, fmt.Errorf("pre-fence durable HA identity changed")
		}
		if !mysqlHandoffSingleMaster(o, h.OldPrimary, h.OldPrimaryUID, true) || candidate.role != mysqlPublishedRoleSlave || !mysqlHandoffReadOnly(candidate) || !mysqlHandoffChannelHealthy(candidate, cluster, old.reference.ServerUUID) {
			return wait, fmt.Errorf("pre-fence handoff authority is unsafe")
		}
		if mysqlHandoffReadOnly(old) {
			return mysqlHandoffAction{kind: "checkpoint"}, nil
		}
		if !mysqlHandoffEntryAllowed(cluster) {
			return wait, nil
		}
		if !mysqlHandoffWritable(old) || !r.mysqlHandoffEqual(ctx, cluster, candidate, old.reference.GTIDSet) {
			return wait, fmt.Errorf("handoff requires writable primary and equal candidate history")
		}
		return mysqlHandoffAction{kind: "sql", name: h.OldPrimary, command: mysqlSetSuperReadOnlyCommand()}, nil
	}
	if err := r.validateMysqlHandoffFence(ctx, cluster, old); err != nil {
		return wait, err
	}
	if h.Stage == databasev1.MysqlClusterUpgradeHandoffStageFenceVerified || h.Stage == databasev1.MysqlClusterUpgradeHandoffStageCandidateReady {
		if !mysqlHandoffReadOnly(candidate) || candidate.role != mysqlPublishedRoleSlave || !r.mysqlHandoffEqual(ctx, cluster, candidate, *h.OldPrimaryGTIDSet) || !candidate.channel.configurationMatches(cluster.Spec.MasterService) || candidate.channel.MasterUUID != h.OldPrimaryServerUUID {
			return wait, fmt.Errorf("candidate is not equal to durable fenced history")
		}
		if old.role == mysqlPublishedRoleMaster {
			if !mysqlHandoffSingleMaster(o, h.OldPrimary, h.OldPrimaryUID, true) || !mysqlHandoffChannelHealthy(candidate, cluster, h.OldPrimaryServerUUID) {
				return wait, fmt.Errorf("old primary or candidate routing proof changed")
			}
			if h.Stage == databasev1.MysqlClusterUpgradeHandoffStageFenceVerified {
				return mysqlHandoffAction{kind: "candidate-ready"}, nil
			}
			return mysqlHandoffAction{kind: "unpublish", name: h.OldPrimary}, nil
		}
		if h.Stage != databasev1.MysqlClusterUpgradeHandoffStageCandidateReady || old.role != mysqlPublishedRoleNone {
			return wait, fmt.Errorf("old primary unpublished outside its durable barrier")
		}
		if err := r.validateNoPublishedMysqlPrimaryBeforeTakeover(ctx, cluster); err != nil {
			return wait, fmt.Errorf("handoff requires zero published masters")
		}
		if !mysqlHandoffRoutingDrained(o) {
			return wait, nil
		}
		return mysqlHandoffAction{kind: "promoting"}, nil
	}
	if h.Stage != databasev1.MysqlClusterUpgradeHandoffStagePromoting || old.role != mysqlPublishedRoleNone {
		return wait, fmt.Errorf("invalid handoff promotion topology")
	}
	if candidate.role == mysqlPublishedRoleMaster {
		if !mysqlHandoffWritable(candidate) || !mysqlReplicationStopped(candidate.channel) || !mysqlHandoffSingleMaster(o, h.Candidate, h.CandidateUID, false) {
			return wait, fmt.Errorf("published handoff candidate is unsafe")
		}
		if !mysqlPublishedPrimaryRoutingAvailable(candidate.member.Pod, o.endpoints) {
			return wait, nil
		}
		if cluster.Status.HA == nil || cluster.Status.HA.Failover != nil {
			return wait, fmt.Errorf("planned authority conflicts with HA")
		}
		if cluster.Status.HA.State == databasev1.MysqlClusterHAStateHealthy && cluster.Status.HA.Primary == h.Candidate && cluster.Status.HA.PrimaryUID == h.CandidateUID {
			return mysqlHandoffAction{kind: "reconfiguring"}, nil
		}
		if cluster.Status.HA.Primary != h.OldPrimary || cluster.Status.HA.PrimaryUID != h.OldPrimaryUID {
			return wait, fmt.Errorf("unexpected durable HA authority during handoff")
		}
		return mysqlHandoffAction{kind: "ha"}, nil
	}
	if candidate.role != mysqlPublishedRoleSlave && candidate.role != mysqlPublishedRoleNone {
		return wait, fmt.Errorf("ambiguous candidate publication")
	}
	if err := r.validateNoPublishedMysqlPrimaryBeforeTakeover(ctx, cluster); err != nil {
		return wait, fmt.Errorf("promotion requires zero published masters")
	}
	if !mysqlHandoffRoutingDrained(o) {
		return wait, nil
	}
	if !candidate.channel.configurationMatches(cluster.Spec.MasterService) || candidate.channel.MasterUUID != h.OldPrimaryServerUUID {
		return wait, fmt.Errorf("candidate lost old source identity")
	}
	if mysqlHandoffReadOnly(candidate) {
		if !r.mysqlHandoffEqual(ctx, cluster, candidate, *h.OldPrimaryGTIDSet) {
			return wait, fmt.Errorf("candidate is not caught up to final history")
		}
		if !mysqlReplicationStopped(candidate.channel) {
			return mysqlHandoffAction{kind: "sql", name: h.Candidate, command: mysqlStopSlaveCommand()}, nil
		}
		if candidate.role == mysqlPublishedRoleSlave {
			return mysqlHandoffAction{kind: "role", name: h.Candidate, role: mysqlPublishedRoleNone}, nil
		}
		return mysqlHandoffAction{kind: "sql", name: h.Candidate, command: mysqlSetReadOnlyOffCommand()}, nil
	}
	if !mysqlHandoffWritable(candidate) || candidate.role != mysqlPublishedRoleNone || !mysqlReplicationStopped(candidate.channel) {
		return wait, fmt.Errorf("candidate became writable outside promotion barrier")
	}
	return mysqlHandoffAction{kind: "role", name: h.Candidate, role: mysqlPublishedRoleMaster}, nil
}

func (r *MysqlClusterReconciler) planMysqlHandoffRejoin(ctx context.Context, cluster *databasev1.MysqlCluster, o *mysqlHandoffObservation) (mysqlHandoffAction, error) {
	wait := mysqlHandoffAction{kind: "wait"}
	h := cluster.Status.Upgrade.Handoff
	p := o.named(h.Candidate)
	if !mysqlUpgradeHAHealthy(cluster) || cluster.Status.HA.Primary != h.Candidate || cluster.Status.HA.PrimaryUID != h.CandidateUID || !mysqlHandoffSingleMaster(o, h.Candidate, h.CandidateUID, true) || !mysqlHandoffWritable(p) || !p.source || !mysqlReplicationStopped(p.channel) {
		return wait, fmt.Errorf("handoff rejoin lacks authoritative new primary")
	}
	action := mysqlHandoffAction{kind: "primary-ready"}
	// Fence a writable non-primary before considering any source attachment.
	// An unsafe history must remain fenced even when ancestry cannot pass.
	for i := range o.members {
		m := &o.members[i]
		if m.member.Pod.Name == h.Candidate {
			continue
		}
		if m.role != mysqlPublishedRoleNone && m.role != mysqlPublishedRoleSlave {
			return wait, fmt.Errorf("unexpected primary during rejoin")
		}
		if m.member.Pod.Name == h.OldPrimary && m.reference.ServerUUID != h.OldPrimaryServerUUID {
			return wait, fmt.Errorf("former primary server identity changed")
		}
		if m.write.WriteRole == mysqlWriteRoleWritable && action.kind == "primary-ready" {
			action = mysqlHandoffAction{kind: "sql", name: m.member.Pod.Name, command: mysqlSetSuperReadOnlyCommand()}
		}
	}
	if action.kind != "primary-ready" {
		return action, nil
	}
	for i := range o.members {
		m := &o.members[i]
		if m.member.Pod.Name == h.Candidate {
			continue
		}
		if m.role != mysqlPublishedRoleNone && m.role != mysqlPublishedRoleSlave {
			return wait, fmt.Errorf("unexpected primary during rejoin")
		}
		if m.member.Pod.Name == h.OldPrimary && m.reference.ServerUUID != h.OldPrimaryServerUUID {
			return wait, fmt.Errorf("former primary server identity changed")
		}
		if !m.write.GTIDReady {
			return wait, fmt.Errorf("rejoin member is not GTID ready")
		}
		output, err := r.executeCommandOnPod(p.member.Pod, mysqlMemberAncestryAgainstCurrentPrimaryCommand(m.reference.GTIDSet))
		if err != nil {
			return wait, fmt.Errorf("rejoin ancestry observation failed")
		}
		ancestry, err := parseMysqlMemberAncestryObservation(output)
		if err != nil || !ancestry.MemberSubsetOfPrimary {
			return wait, fmt.Errorf("unsafe rejoin ancestry")
		}
		next := mysqlHandoffAction{kind: "wait"}
		correct := m.channel.configurationMatches(cluster.Spec.MasterService) && m.channel.MasterUUID == p.reference.ServerUUID
		switch {
		case !mysqlHandoffReadOnly(m):
			if m.write.WriteRole != mysqlWriteRoleWritable {
				return wait, fmt.Errorf("ambiguous non-primary write safety")
			}
			next = mysqlHandoffAction{kind: "sql", name: m.member.Pod.Name, command: mysqlSetSuperReadOnlyCommand()}
		case m.role == mysqlPublishedRoleSlave && !mysqlHandoffChannelHealthy(m, cluster, p.reference.ServerUUID):
			next = mysqlHandoffAction{kind: "role", name: m.member.Pod.Name, role: mysqlPublishedRoleNone}
		case !m.channel.Configured:
			next = mysqlHandoffAction{kind: "sql", name: m.member.Pod.Name, command: mysqlInitializeReplicaCommand(cluster.Spec.MasterService)}
		case !correct && !mysqlReplicationStopped(m.channel):
			next = mysqlHandoffAction{kind: "sql", name: m.member.Pod.Name, command: mysqlStopSlaveCommand()}
		case !correct:
			next = mysqlHandoffAction{kind: "sql", name: m.member.Pod.Name, command: mysqlConfigureReplicaCommand(cluster.Spec.MasterService)}
		case mysqlReplicationStopped(m.channel):
			next = mysqlHandoffAction{kind: "sql", name: m.member.Pod.Name, command: mysqlStartSlaveCommand()}
		case !mysqlHandoffChannelHealthy(m, cluster, p.reference.ServerUUID):
			return wait, nil
		case m.role == mysqlPublishedRoleNone:
			next = mysqlHandoffAction{kind: "role", name: m.member.Pod.Name, role: mysqlPublishedRoleSlave}
		}
		if (action.kind == "primary-ready" && next.kind != "wait") || (next.command == mysqlSetSuperReadOnlyCommand() && action.command != mysqlSetSuperReadOnlyCommand()) {
			action = next
		}
	}
	return action, nil
}

func (r *MysqlClusterReconciler) executeMysqlHandoffAction(ctx context.Context, cluster *databasev1.MysqlCluster, o *mysqlHandoffObservation, action mysqlHandoffAction) error {
	upgrade := cluster.Status.Upgrade.DeepCopy()
	h := upgrade.Handoff
	switch action.kind {
	case "sql":
		m := o.named(action.name)
		pod, err := r.getFreshMysqlRejoinMember(ctx, cluster, m.member, m.role)
		if err != nil {
			return fmt.Errorf("handoff SQL identity changed")
		}
		if _, err := r.executeCommandOnPod(pod, action.command); err != nil {
			return fmt.Errorf("planned handoff SQL barrier failed")
		}
		return nil
	case "role":
		m := o.named(action.name)
		if action.role == mysqlPublishedRoleMaster {
			if err := r.validateNoPublishedMysqlPrimaryBeforeTakeover(ctx, cluster); err != nil {
				return fmt.Errorf("master appeared before handoff publication")
			}
		}
		return r.mutateMysqlRejoinMemberRole(ctx, cluster, m.member, m.role, action.role)
	case "unpublish":
		return r.clearMysqlPublishedRole(ctx, cluster, h.OldPrimary, h.OldPrimaryUID)
	case "checkpoint":
		ref := o.named(h.OldPrimary).reference
		h.OldPrimaryServerUUID = ref.ServerUUID
		h.OldPrimaryGTIDSet = &ref.GTIDSet
		h.Stage = databasev1.MysqlClusterUpgradeHandoffStageFenceVerified
	case "candidate-ready":
		h.Stage = databasev1.MysqlClusterUpgradeHandoffStageCandidateReady
	case "promoting":
		h.Stage = databasev1.MysqlClusterUpgradeHandoffStagePromoting
	case "ha":
		updated := cluster.DeepCopy()
		updated.Status.HA = &databasev1.MysqlClusterHAStatus{State: databasev1.MysqlClusterHAStateHealthy, Primary: h.Candidate, PrimaryUID: h.CandidateUID}
		if err := r.Status().Patch(ctx, updated, client.MergeFromWithOptions(cluster, client.MergeFromWithOptimisticLock{})); err != nil {
			return fmt.Errorf("planned primary authority persistence failed: %w", err)
		}
		*cluster = *updated
		return nil // no failover event/counter and no Handoff patch
	case "reconfiguring":
		h.Stage = databasev1.MysqlClusterUpgradeHandoffStageReconfiguring
	case "primary-ready":
		h.Stage = databasev1.MysqlClusterUpgradeHandoffStageCompleted
		upgrade.Stage = databasev1.MysqlClusterUpgradeStagePrimaryReady
	default:
		return fmt.Errorf("unknown handoff control action")
	}
	return r.persistMysqlClusterUpgradeStatus(ctx, cluster, cluster.Status.LastConvergedImage, upgrade)
}

// Also used by Gate 7-B at PrimaryReady; no second delete mechanism exists.
func (r *MysqlClusterReconciler) proveMysqlPrimaryReady(ctx context.Context, cluster *databasev1.MysqlCluster, allTarget bool) (*mysqlHandoffObservation, error) {
	if !mysqlHandoffEntryAllowed(cluster) {
		return nil, fmt.Errorf("final upgrade interlock is not stable")
	}
	o, err := r.observeMysqlHandoff(ctx, cluster, false)
	if err != nil {
		return nil, err
	}
	p := o.named(cluster.Status.HA.Primary)
	if p == nil || p.image != cluster.Status.Upgrade.TargetImage || !mysqlHandoffWritable(p) || !p.source || !mysqlHandoffSingleMaster(o, p.member.Pod.Name, cluster.Status.HA.PrimaryUID, true) {
		return nil, fmt.Errorf("final primary authority proof failed")
	}
	h := cluster.Status.Upgrade.Handoff
	if h != nil && (p.member.Pod.Name != h.Candidate || string(p.member.Pod.UID) != h.CandidateUID) {
		return nil, fmt.Errorf("current primary differs from durable handoff candidate")
	}
	for i := range o.members {
		m := &o.members[i]
		if m.image != cluster.Status.Upgrade.TargetImage {
			if allTarget || h == nil || m.member.Pod.Name != h.OldPrimary || string(m.member.Pod.UID) != h.OldPrimaryUID || m.image != cluster.Status.Upgrade.FromImage {
				return nil, fmt.Errorf("unexpected remaining old-image member")
			}
			if m.reference.ServerUUID != h.OldPrimaryServerUUID {
				return nil, fmt.Errorf("former primary server identity changed before replacement")
			}
		}
		if h != nil && m.member.Pod.Name == h.OldPrimary && m.image == cluster.Status.Upgrade.TargetImage && string(m.member.Pod.UID) == h.OldPrimaryUID {
			return nil, fmt.Errorf("former primary has not acquired a new identity")
		}
		if m.member.Pod.Name == p.member.Pod.Name {
			continue
		}
		if m.role != mysqlPublishedRoleSlave || !mysqlHandoffReadOnly(m) || !mysqlHandoffChannelHealthy(m, cluster, p.reference.ServerUUID) {
			return nil, fmt.Errorf("final replica write or source proof failed")
		}
	}
	if h != nil && o.named(h.OldPrimary) == nil {
		return nil, fmt.Errorf("former-primary ordinal is absent")
	}
	if err := r.recheckMysqlHandoffIdentities(ctx, cluster, o); err != nil {
		return nil, err
	}
	return o, nil
}

func (r *MysqlClusterReconciler) completeMysqlUpgrade(ctx context.Context, cluster *databasev1.MysqlCluster) error {
	if err := validateMysqlClusterUpgradeStatus(&cluster.Status); err != nil {
		return err
	}
	u := cluster.Status.Upgrade
	if u == nil || u.Replacement != nil || (u.Stage != databasev1.MysqlClusterUpgradeStagePrimaryReady && !(u.Stage == databasev1.MysqlClusterUpgradeStageReplicasVerified && u.Handoff == nil)) {
		return fmt.Errorf("upgrade completion is outside its durable barrier")
	}
	first, err := r.proveMysqlPrimaryReady(ctx, cluster, true)
	if err != nil {
		return err
	}
	fresh, err := r.proveMysqlPrimaryReady(ctx, cluster, true)
	if err != nil {
		return err
	}
	for i := range first.members {
		if first.members[i].member.Pod.UID != fresh.members[i].member.Pod.UID {
			return fmt.Errorf("final upgrade identity changed during proof")
		}
	}
	return r.persistMysqlClusterUpgradeStatus(ctx, cluster, cluster.Status.Upgrade.TargetImage, nil)
}

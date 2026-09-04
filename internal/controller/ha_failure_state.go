package controller

import (
	"context"
	"fmt"
	"time"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const mysqlHAFailureRequeueAfter = 2 * time.Second

type mysqlPrimaryFailureClassification string

const (
	mysqlPrimaryHealthy          mysqlPrimaryFailureClassification = "healthy"
	mysqlPrimarySuspected        mysqlPrimaryFailureClassification = "suspected"
	mysqlPrimaryFailureConfirmed mysqlPrimaryFailureClassification = "failure-confirmed"
	mysqlPrimaryDegraded         mysqlPrimaryFailureClassification = "degraded"
)

type mysqlPrimaryFailureObservation struct {
	Classification mysqlPrimaryFailureClassification
	PrimaryName    string
	PrimaryUID     string
	PrimaryMissing bool
}

func mysqlMasterEndpointAvailable(endpoints *corev1.Endpoints) bool {
	for _, subset := range endpoints.Subsets {
		if len(subset.Addresses) != 0 {
			return true
		}
	}
	return false
}

func (r *MysqlClusterReconciler) observeMysqlPrimaryFailure(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (mysqlPrimaryFailureObservation, error) {
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return mysqlPrimaryFailureObservation{}, err
	}

	primaries := make([]*corev1.Pod, 0, 1)
	for _, member := range members {
		role, err := observeMysqlPublishedRole(member.Pod)
		if err != nil {
			return mysqlPrimaryFailureObservation{}, err
		}
		if role == mysqlPublishedRoleMaster {
			primaries = append(primaries, member.Pod)
		}
	}

	if len(primaries) > 1 {
		return mysqlPrimaryFailureObservation{}, fmt.Errorf(
			"MysqlCluster %s/%s HA observation requires at most one published primary, found %d",
			cluster.Namespace,
			cluster.Name,
			len(primaries),
		)
	}

	endpointAvailable := false
	endpoints := &corev1.Endpoints{}
	endpointKey := client.ObjectKey{Namespace: cluster.Namespace, Name: cluster.Spec.MasterService}
	if err := r.Get(ctx, endpointKey, endpoints); err != nil {
		if !apierrors.IsNotFound(err) {
			return mysqlPrimaryFailureObservation{}, fmt.Errorf("failed to observe primary Endpoints %s: %w", endpointKey, err)
		}
	} else {
		endpointAvailable = mysqlMasterEndpointAvailable(endpoints)
	}

	if len(primaries) == 0 {
		if endpointAvailable {
			return mysqlPrimaryFailureObservation{}, fmt.Errorf(
				"MysqlCluster %s/%s HA observation found zero published primaries while primary Endpoints %s still has an address",
				cluster.Namespace,
				cluster.Name,
				endpointKey,
			)
		}
		tracked := cluster.Status.HA
		if tracked == nil || tracked.Primary == "" || tracked.PrimaryUID == "" {
			return mysqlPrimaryFailureObservation{}, fmt.Errorf(
				"MysqlCluster %s/%s HA observation found zero published primaries without a durable primary identity while observing primary Endpoints %s",
				cluster.Namespace,
				cluster.Name,
				endpointKey,
			)
		}

		trackedPod := &corev1.Pod{}
		trackedKey := client.ObjectKey{Namespace: cluster.Namespace, Name: tracked.Primary}
		if err := r.Get(ctx, trackedKey, trackedPod); err != nil {
			if apierrors.IsNotFound(err) {
				return mysqlPrimaryFailureObservation{
					Classification: mysqlPrimaryFailureConfirmed,
					PrimaryName:    tracked.Primary,
					PrimaryUID:     tracked.PrimaryUID,
					PrimaryMissing: true,
				}, nil
			}
			return mysqlPrimaryFailureObservation{}, fmt.Errorf("failed to re-observe tracked primary Pod %s: %w", trackedKey, err)
		}
		if err := r.validateStatefulSetManagedMysqlPod(ctx, trackedPod, cluster); err != nil {
			return mysqlPrimaryFailureObservation{}, fmt.Errorf("invalid tracked primary Pod %s: %w", trackedKey, err)
		}
		return mysqlPrimaryFailureObservation{}, fmt.Errorf(
			"MysqlCluster %s/%s HA observation found zero published primaries while tracked Pod %s still exists",
			cluster.Namespace,
			cluster.Name,
			trackedKey,
		)
	}

	primary := primaries[0]
	if primary.UID == "" {
		return mysqlPrimaryFailureObservation{}, fmt.Errorf("published primary Pod %s/%s has no UID", primary.Namespace, primary.Name)
	}

	observation := mysqlPrimaryFailureObservation{
		PrimaryName: primary.Name,
		PrimaryUID:  string(primary.UID),
	}
	switch {
	case endpointAvailable:
		observation.Classification = mysqlPrimaryHealthy
	case mysqlStatefulSetPodHealthy(primary):
		observation.Classification = mysqlPrimaryDegraded
	default:
		observation.Classification = mysqlPrimarySuspected
	}
	return observation, nil
}

func mysqlHAIdentityMatches(status *databasev1.MysqlClusterHAStatus, observation mysqlPrimaryFailureObservation) bool {
	return status != nil &&
		status.Primary == observation.PrimaryName &&
		status.PrimaryUID == observation.PrimaryUID
}

func mysqlHealthyHAStatus(observation mysqlPrimaryFailureObservation) *databasev1.MysqlClusterHAStatus {
	return &databasev1.MysqlClusterHAStatus{
		State:      databasev1.MysqlClusterHAStateHealthy,
		Primary:    observation.PrimaryName,
		PrimaryUID: observation.PrimaryUID,
	}
}

func mysqlDegradedHAStatus(observation mysqlPrimaryFailureObservation) *databasev1.MysqlClusterHAStatus {
	return &databasev1.MysqlClusterHAStatus{
		State:      databasev1.MysqlClusterHAStateDegraded,
		Primary:    observation.PrimaryName,
		PrimaryUID: observation.PrimaryUID,
	}
}

func mysqlSuspectedHAStatus(observation mysqlPrimaryFailureObservation) *databasev1.MysqlClusterHAStatus {
	now := metav1.Now()
	return &databasev1.MysqlClusterHAStatus{
		State:            databasev1.MysqlClusterHAStateSuspected,
		Primary:          observation.PrimaryName,
		PrimaryUID:       observation.PrimaryUID,
		FailureCount:     1,
		FirstFailureTime: &now,
	}
}

func mysqlFailoverRequiredHAStatus(
	current *databasev1.MysqlClusterHAStatus,
	observation mysqlPrimaryFailureObservation,
) *databasev1.MysqlClusterHAStatus {
	failureCount := int32(1)
	firstFailureTime := metav1.Now()
	if mysqlHAIdentityMatches(current, observation) {
		failureCount = current.FailureCount + 1
		if current.FirstFailureTime != nil {
			firstFailureTime = *current.FirstFailureTime.DeepCopy()
		}
	}
	return &databasev1.MysqlClusterHAStatus{
		State:            databasev1.MysqlClusterHAStateFailoverRequired,
		Primary:          observation.PrimaryName,
		PrimaryUID:       observation.PrimaryUID,
		FailureCount:     failureCount,
		FirstFailureTime: &firstFailureTime,
	}
}

func mysqlCopyHAStatus(status *databasev1.MysqlClusterHAStatus) *databasev1.MysqlClusterHAStatus {
	if status == nil {
		return nil
	}
	return status.DeepCopy()
}

func (r *MysqlClusterReconciler) persistMysqlClusterHAStatus(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	status *databasev1.MysqlClusterHAStatus,
) (bool, error) {
	if apiequality.Semantic.DeepEqual(cluster.Status.HA, status) {
		return false, nil
	}
	base := cluster.DeepCopy()
	cluster.Status.HA = mysqlCopyHAStatus(status)
	if err := r.Status().Patch(ctx, cluster, client.MergeFrom(base)); err != nil {
		return false, fmt.Errorf(
			"failed to persist HA status on MysqlCluster %s/%s: %w",
			cluster.Namespace,
			cluster.Name,
			err,
		)
	}
	r.emitMysqlHAStatusTransitionEvent(ctx, cluster, base.Status.HA, cluster.Status.HA)
	return true, nil
}

func (r *MysqlClusterReconciler) revalidateMysqlHAFailureIdentity(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
	observation mysqlPrimaryFailureObservation,
) error {
	key := client.ObjectKey{Namespace: cluster.Namespace, Name: observation.PrimaryName}
	current := &corev1.Pod{}
	err := r.Get(ctx, key, current)
	if observation.PrimaryMissing {
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to revalidate missing primary Pod %s: %w", key, err)
		}
		return fmt.Errorf("primary Pod %s reappeared before HA execution", key)
	}
	if err != nil {
		return fmt.Errorf("failed to freshly get failed primary Pod %s before HA execution: %w", key, err)
	}
	if err := r.validateStatefulSetManagedMysqlPod(ctx, current, cluster); err != nil {
		return fmt.Errorf("failed primary Pod %s failed ownership and identity revalidation: %w", key, err)
	}
	role, err := observeMysqlPublishedRole(current)
	if err != nil {
		return err
	}
	if role != mysqlPublishedRoleMaster {
		return fmt.Errorf("failed primary Pod %s is no longer the published primary", key)
	}
	if string(current.UID) != observation.PrimaryUID {
		return fmt.Errorf(
			"failed primary Pod %s UID changed from observed %q to %q before HA execution",
			key,
			observation.PrimaryUID,
			current.UID,
		)
	}
	if mysqlStatefulSetPodHealthy(current) {
		return fmt.Errorf("failed primary Pod %s recovered before HA execution", key)
	}
	return nil
}

func (r *MysqlClusterReconciler) observeSinglePublishedPrimary(
	ctx context.Context,
	cluster *databasev1.MysqlCluster,
) (*corev1.Pod, error) {
	members, err := r.listMysqlStatefulSetPods(ctx, cluster)
	if err != nil {
		return nil, err
	}
	var primary *corev1.Pod
	count := 0
	for _, member := range members {
		role, err := observeMysqlPublishedRole(member.Pod)
		if err != nil {
			return nil, err
		}
		if role == mysqlPublishedRoleMaster {
			primary = member.Pod.DeepCopy()
			count++
		}
	}
	if count != 1 {
		return nil, fmt.Errorf(
			"MysqlCluster %s/%s post-failover verification requires exactly one published primary, found %d",
			cluster.Namespace,
			cluster.Name,
			count,
		)
	}
	if primary.UID == "" {
		return nil, fmt.Errorf("post-failover primary Pod %s/%s has no UID", primary.Namespace, primary.Name)
	}
	return primary, nil
}

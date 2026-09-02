package controller

import (
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
)

func replicaCountPointer(value int32) *int32 {
	return &value
}

func TestMysqlClusterReplicaTransitionStatusDeepCopy(t *testing.T) {
	g := NewWithT(t)
	cluster := &databasev1.MysqlCluster{
		Status: databasev1.MysqlClusterStatus{
			LastConvergedReplicas: replicaCountPointer(3),
			ReplicaTransition: &databasev1.MysqlClusterReplicaTransitionStatus{
				FromReplicas:   3,
				TargetReplicas: 4,
			},
		},
	}

	copy := cluster.DeepCopy()
	g.Expect(copy.Status.LastConvergedReplicas).NotTo(BeIdenticalTo(cluster.Status.LastConvergedReplicas))
	g.Expect(copy.Status.ReplicaTransition).NotTo(BeIdenticalTo(cluster.Status.ReplicaTransition))
	g.Expect(*copy.Status.LastConvergedReplicas).To(Equal(int32(3)))
	g.Expect(copy.Status.ReplicaTransition.FromReplicas).To(Equal(int32(3)))
	g.Expect(copy.Status.ReplicaTransition.TargetReplicas).To(Equal(int32(4)))

	*copy.Status.LastConvergedReplicas = 5
	copy.Status.ReplicaTransition.FromReplicas = 5
	copy.Status.ReplicaTransition.TargetReplicas = 6
	g.Expect(*cluster.Status.LastConvergedReplicas).To(Equal(int32(3)))
	g.Expect(cluster.Status.ReplicaTransition.FromReplicas).To(Equal(int32(3)))
	g.Expect(cluster.Status.ReplicaTransition.TargetReplicas).To(Equal(int32(4)))
}

func TestMysqlClusterReplicaTransitionStatusValidation(t *testing.T) {
	testCases := []struct {
		name        string
		status      databasev1.MysqlClusterStatus
		errorSubstr string
	}{
		{
			name:   "empty pre-migration status is valid",
			status: databasev1.MysqlClusterStatus{},
		},
		{
			name: "stable checkpoint is valid",
			status: databasev1.MysqlClusterStatus{
				LastConvergedReplicas: replicaCountPointer(3),
			},
		},
		{
			name: "active transition is valid",
			status: databasev1.MysqlClusterStatus{
				LastConvergedReplicas: replicaCountPointer(3),
				ReplicaTransition: &databasev1.MysqlClusterReplicaTransitionStatus{
					FromReplicas:   3,
					TargetReplicas: 4,
				},
			},
		},
		{
			name: "active transition requires last converged checkpoint",
			status: databasev1.MysqlClusterStatus{
				ReplicaTransition: &databasev1.MysqlClusterReplicaTransitionStatus{
					FromReplicas:   3,
					TargetReplicas: 4,
				},
			},
			errorSubstr: "requires lastConvergedReplicas",
		},
		{
			name: "zero last converged replicas is invalid",
			status: databasev1.MysqlClusterStatus{
				LastConvergedReplicas: replicaCountPointer(0),
			},
			errorSubstr: "lastConvergedReplicas must be greater than zero",
		},
		{
			name: "negative last converged replicas is invalid",
			status: databasev1.MysqlClusterStatus{
				LastConvergedReplicas: replicaCountPointer(-1),
			},
			errorSubstr: "lastConvergedReplicas must be greater than zero",
		},
		{
			name: "zero transition source is invalid",
			status: databasev1.MysqlClusterStatus{
				LastConvergedReplicas: replicaCountPointer(3),
				ReplicaTransition: &databasev1.MysqlClusterReplicaTransitionStatus{
					FromReplicas:   0,
					TargetReplicas: 4,
				},
			},
			errorSubstr: "fromReplicas must be greater than zero",
		},
		{
			name: "negative transition source is invalid",
			status: databasev1.MysqlClusterStatus{
				LastConvergedReplicas: replicaCountPointer(3),
				ReplicaTransition: &databasev1.MysqlClusterReplicaTransitionStatus{
					FromReplicas:   -1,
					TargetReplicas: 4,
				},
			},
			errorSubstr: "fromReplicas must be greater than zero",
		},
		{
			name: "zero transition target is invalid",
			status: databasev1.MysqlClusterStatus{
				LastConvergedReplicas: replicaCountPointer(3),
				ReplicaTransition: &databasev1.MysqlClusterReplicaTransitionStatus{
					FromReplicas:   3,
					TargetReplicas: 0,
				},
			},
			errorSubstr: "targetReplicas must be greater than zero",
		},
		{
			name: "negative transition target is invalid",
			status: databasev1.MysqlClusterStatus{
				LastConvergedReplicas: replicaCountPointer(3),
				ReplicaTransition: &databasev1.MysqlClusterReplicaTransitionStatus{
					FromReplicas:   3,
					TargetReplicas: -1,
				},
			},
			errorSubstr: "targetReplicas must be greater than zero",
		},
		{
			name: "transition source must match last converged replicas",
			status: databasev1.MysqlClusterStatus{
				LastConvergedReplicas: replicaCountPointer(3),
				ReplicaTransition: &databasev1.MysqlClusterReplicaTransitionStatus{
					FromReplicas:   2,
					TargetReplicas: 4,
				},
			},
			errorSubstr: "does not match lastConvergedReplicas",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			err := validateMysqlClusterReplicaTransitionStatus(&testCase.status)
			if testCase.errorSubstr == "" {
				g.Expect(err).NotTo(HaveOccurred())
				return
			}
			g.Expect(err).To(MatchError(ContainSubstring(testCase.errorSubstr)))
		})
	}
}

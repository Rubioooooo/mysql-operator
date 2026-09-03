package controller

import (
	"context"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

func TestPhase5EUnknownDurableFailoverStageFailsClosedAtDispatcher(t *testing.T) {
	testCases := []struct {
		name  string
		stage databasev1.MysqlClusterFailoverStage
	}{
		{name: "empty", stage: ""},
		{name: "unknown", stage: databasev1.MysqlClusterFailoverStage("Bogus")},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			fixture := newPhase5DFixture(t, "phase5e-stage-"+testCase.name)
			fixture.cluster.Status.HA.Failover.Stage = testCase.stage
			fixture.reconciler = phase1HReconciler(
				t,
				phase5BObjects(
					fixture.cluster,
					fixture.statefulSet,
					fixture.former,
					[]*corev1.Pod{fixture.candidate, fixture.ordinary},
				)...,
			)
			execCalls := 0
			fixture.reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
				execCalls++
				return "", nil
			}

			_, converged, err := fixture.reconciler.reconcileMasterSlave(context.Background(), *fixture.cluster)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err.Error()).To(ContainSubstring("unsupported durable MySQL failover stage"))
			g.Expect(err.Error()).To(ContainSubstring(`"` + string(testCase.stage) + `"`))
			g.Expect(converged).To(BeFalse())
			g.Expect(execCalls).To(Equal(0))

			stored := phase4StoredCluster(t, fixture.reconciler, fixture.cluster)
			g.Expect(stored.Status.HA).NotTo(BeNil())
			g.Expect(stored.Status.HA.Failover).NotTo(BeNil())
			g.Expect(stored.Status.HA.Failover.Stage).To(Equal(testCase.stage))
			g.Expect(stored.Status.HA.State).NotTo(Equal(databasev1.MysqlClusterHAStateVerifying))
			g.Expect(stored.Status.HA.State).NotTo(Equal(databasev1.MysqlClusterHAStateHealthy))
			g.Expect(phase5DStoredRole(t, fixture, fixture.candidate)).To(Equal(mysqlPublishedRoleMaster))
		})
	}
}

func TestPhase5EMissingDesiredMemberDuringReconfiguringSafelyWaits(t *testing.T) {
	g := NewWithT(t)
	fixture := newPhase5DFixture(t, "phase5e-missing-member")
	fixture.reconciler = phase1HReconciler(
		t,
		phase5BObjects(
			fixture.cluster,
			fixture.statefulSet,
			fixture.former,
			[]*corev1.Pod{fixture.candidate},
		)...,
	)
	fixture.plan.commands = nil
	fixture.plan.mutations = nil
	fixture.reconciler.execCommandOnPodFn = fixture.plan.execute

	_, converged, err := fixture.reconciler.reconcileMasterSlave(context.Background(), *fixture.cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(converged).To(BeFalse())
	g.Expect(fixture.plan.commands).To(BeEmpty())
	g.Expect(fixture.plan.mutations).To(BeEmpty())

	stored := phase4StoredCluster(t, fixture.reconciler, fixture.cluster)
	g.Expect(stored.Status.HA).NotTo(BeNil())
	g.Expect(stored.Status.HA.Failover).NotTo(BeNil())
	g.Expect(stored.Status.HA.Failover.Stage).To(Equal(databasev1.MysqlClusterFailoverStageReconfiguring))
	g.Expect(stored.Status.HA.State).NotTo(Equal(databasev1.MysqlClusterHAStateVerifying))
	g.Expect(stored.Status.HA.State).NotTo(Equal(databasev1.MysqlClusterHAStateHealthy))
	g.Expect(phase5DStoredRole(t, fixture, fixture.candidate)).To(Equal(mysqlPublishedRoleMaster))
	g.Expect(phase5DStoredRole(t, fixture, fixture.former)).To(Equal(mysqlPublishedRoleSlave))
}

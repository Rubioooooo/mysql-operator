package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestParseMysqlReplicationObservation(t *testing.T) {
	t.Run("empty output means no configured channel", func(t *testing.T) {
		g := NewWithT(t)
		observation, err := parseMysqlShowSlaveStatus(" \r\n\t")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(observation.Configured).To(BeFalse())
		g.Expect(observation.configurationMatches("mysql-primary")).To(BeFalse())
		g.Expect(observation.semanticallyHealthy("mysql-primary")).To(BeFalse())
	})

	healthyOutput := mysqlSlaveStatusOutputForTest(
		"mysql-primary",
		"replica",
		"1",
		"Yes",
		"Yes",
		"",
		"",
	)
	t.Run("valid healthy MySQL 5.7 status", func(t *testing.T) {
		g := NewWithT(t)
		observation, err := parseMysqlShowSlaveStatus(healthyOutput)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(observation).To(Equal(mysqlReplicationChannelObservation{
			Configured:   true,
			MasterHost:   "mysql-primary",
			MasterUser:   "replica",
			AutoPosition: "1",
			IORunning:    "Yes",
			SQLRunning:   "Yes",
		}))
		g.Expect(observation.configurationMatches("mysql-primary")).To(BeTrue())
		g.Expect(observation.semanticallyHealthy("mysql-primary")).To(BeTrue())
	})

	unhealthyCases := []struct {
		name                 string
		masterHost           string
		masterUser           string
		autoPosition         string
		ioRunning            string
		sqlRunning           string
		lastIOError          string
		lastSQLError         string
		configurationMatches bool
	}{
		{name: "wrong MasterHost", masterHost: "other-primary", masterUser: "replica", autoPosition: "1", ioRunning: "Yes", sqlRunning: "Yes"},
		{name: "wrong MasterUser", masterHost: "mysql-primary", masterUser: "other-user", autoPosition: "1", ioRunning: "Yes", sqlRunning: "Yes"},
		{name: "AutoPosition is not one", masterHost: "mysql-primary", masterUser: "replica", autoPosition: "0", ioRunning: "Yes", sqlRunning: "Yes"},
		{name: "IO thread is not Yes", masterHost: "mysql-primary", masterUser: "replica", autoPosition: "1", ioRunning: "Connecting", sqlRunning: "Yes", configurationMatches: true},
		{name: "SQL thread is not Yes", masterHost: "mysql-primary", masterUser: "replica", autoPosition: "1", ioRunning: "Yes", sqlRunning: "No", configurationMatches: true},
		{name: "LastIOError is non-empty", masterHost: "mysql-primary", masterUser: "replica", autoPosition: "1", ioRunning: "Yes", sqlRunning: "Yes", lastIOError: "connection refused", configurationMatches: true},
		{name: "LastSQLError is non-empty", masterHost: "mysql-primary", masterUser: "replica", autoPosition: "1", ioRunning: "Yes", sqlRunning: "Yes", lastSQLError: "apply failed", configurationMatches: true},
	}
	for _, testCase := range unhealthyCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			observation, err := parseMysqlShowSlaveStatus(mysqlSlaveStatusOutputForTest(
				testCase.masterHost,
				testCase.masterUser,
				testCase.autoPosition,
				testCase.ioRunning,
				testCase.sqlRunning,
				testCase.lastIOError,
				testCase.lastSQLError,
			))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(observation.Configured).To(BeTrue())
			g.Expect(observation.configurationMatches("mysql-primary")).To(Equal(testCase.configurationMatches))
			g.Expect(observation.semanticallyHealthy("mysql-primary")).To(BeFalse())
		})
	}

	t.Run("missing required field is malformed", func(t *testing.T) {
		g := NewWithT(t)
		output := strings.Replace(healthyOutput, "               Master_User: replica\n", "", 1)
		_, err := parseMysqlShowSlaveStatus(output)
		g.Expect(err).To(MatchError(ContainSubstring("missing required fields: Master_User")))
	})

	t.Run("unknown fields are ignored and colons in values are preserved", func(t *testing.T) {
		g := NewWithT(t)
		output := strings.Replace(
			healthyOutput,
			"              Last_IO_Error: \n",
			"              Last_IO_Error: dial tcp 10.0.0.1:3306: connection refused\n                 Unknown_Field: ignored:value\n",
			1,
		)
		observation, err := parseMysqlShowSlaveStatus(output)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(observation.LastIOError).To(Equal("dial tcp 10.0.0.1:3306: connection refused"))
		g.Expect(observation.semanticallyHealthy("mysql-primary")).To(BeFalse())
	})
}

func TestMysqlPublishedRoleObservation(t *testing.T) {
	testCases := []struct {
		name         string
		labels       map[string]string
		expectedRole mysqlPublishedRole
		expectError  bool
	}{
		{name: "labels absent", labels: nil, expectedRole: mysqlPublishedRoleNone},
		{name: "master", labels: map[string]string{LabelMysqlRole: "master", LegacyLabelRole: "master"}, expectedRole: mysqlPublishedRoleMaster},
		{name: "slave", labels: map[string]string{LabelMysqlRole: "slave", LegacyLabelRole: "slave"}, expectedRole: mysqlPublishedRoleSlave},
		{name: "canonical only", labels: map[string]string{LabelMysqlRole: "slave"}, expectError: true},
		{name: "legacy only", labels: map[string]string{LegacyLabelRole: "slave"}, expectError: true},
		{name: "labels disagree", labels: map[string]string{LabelMysqlRole: "master", LegacyLabelRole: "slave"}, expectError: true},
		{name: "unknown role", labels: map[string]string{LabelMysqlRole: "primary", LegacyLabelRole: "primary"}, expectError: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mysql-1", Namespace: "database", Labels: testCase.labels}}
			role, err := observeMysqlPublishedRole(pod)
			if testCase.expectError {
				g.Expect(err).To(HaveOccurred())
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(role).To(Equal(testCase.expectedRole))
		})
	}
}

func TestMysqlMemberReplicationObservation(t *testing.T) {
	ctx := context.Background()
	cluster := statefulSetResourceTestCluster("observe-member", types.UID("observe-member-cluster-uid"))
	statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("observe-member-statefulset-uid"))
	newReplica := func() *corev1.Pod {
		pod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 2)
		pod.Labels[LabelMysqlRole] = "slave"
		pod.Labels[LegacyLabelRole] = "slave"
		return pod
	}

	t.Run("valid member executes SHOW SLAVE STATUS only", func(t *testing.T) {
		g := NewWithT(t)
		replica := newReplica()
		commands := make([]string, 0, 1)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, replica),
			execCommandOnPodFn: func(pod *corev1.Pod, command string) (string, error) {
				commands = append(commands, command)
				g.Expect(pod.Name).To(Equal(replica.Name))
				return mysqlSlaveStatusOutputForTest(cluster.Spec.MasterService, "replica", "1", "Yes", "Yes", "", ""), nil
			},
		}

		observation, err := reconciler.observeMysqlMemberReplication(ctx, replica, cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(commands).To(Equal([]string{mysqlShowSlaveStatusCommand()}))
		g.Expect(observation.PodName).To(Equal(replica.Name))
		g.Expect(observation.Ordinal).To(Equal(int32(2)))
		g.Expect(observation.PublishedRole).To(Equal(mysqlPublishedRoleSlave))
		g.Expect(observation.Channel.semanticallyHealthy(cluster.Spec.MasterService)).To(BeTrue())
	})

	t.Run("same UID spoofed Pod is rejected before SQL", func(t *testing.T) {
		g := NewWithT(t)
		spoofed := newReplica()
		spoofed.Name = "spoofed-member"
		execCalls := 0
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet),
			execCommandOnPodFn: func(*corev1.Pod, string) (string, error) {
				execCalls++
				return "", nil
			},
		}

		_, err := reconciler.observeMysqlMemberReplication(ctx, spoofed, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("ordinal identity does not match")))
		g.Expect(execCalls).To(Equal(0))
	})

	t.Run("SQL execution error is propagated", func(t *testing.T) {
		g := NewWithT(t)
		replica := newReplica()
		sqlErr := errors.New("exec failed")
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet),
			execCommandOnPodFn: func(*corev1.Pod, string) (string, error) {
				return "", sqlErr
			},
		}

		_, err := reconciler.observeMysqlMemberReplication(ctx, replica, cluster)
		g.Expect(err).To(MatchError(ContainSubstring("failed to execute SHOW SLAVE STATUS")))
		g.Expect(errors.Is(err, sqlErr)).To(BeTrue())
	})
}

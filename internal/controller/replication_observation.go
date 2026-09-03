package controller

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	databasev1 "github.com/egonlin/api/v1"
	corev1 "k8s.io/api/core/v1"
)

type mysqlPublishedRole string

const (
	mysqlPublishedRoleNone   mysqlPublishedRole = ""
	mysqlPublishedRoleMaster mysqlPublishedRole = "master"
	mysqlPublishedRoleSlave  mysqlPublishedRole = "slave"
)

type mysqlReplicationChannelObservation struct {
	Configured   bool
	MasterHost   string
	MasterUser   string
	AutoPosition string
	IORunning    string
	SQLRunning   string
	LastIOError  string
	LastSQLError string
}

type mysqlMemberReplicationObservation struct {
	PodName       string
	Ordinal       int32
	PublishedRole mysqlPublishedRole
	Channel       mysqlReplicationChannelObservation
}

var mysqlRequiredSlaveStatusFields = []string{
	"Master_Host",
	"Master_User",
	"Auto_Position",
	"Slave_IO_Running",
	"Slave_SQL_Running",
	"Last_IO_Error",
	"Last_SQL_Error",
}

func parseMysqlShowSlaveStatus(output string) (mysqlReplicationChannelObservation, error) {
	if strings.TrimSpace(output) == "" {
		return mysqlReplicationChannelObservation{}, nil
	}

	fields := make(map[string]string)
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "***") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		fields[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	if err := scanner.Err(); err != nil {
		return mysqlReplicationChannelObservation{}, fmt.Errorf("failed to parse SHOW SLAVE STATUS output: %w", err)
	}

	missing := make([]string, 0)
	for _, requiredField := range mysqlRequiredSlaveStatusFields {
		if _, found := fields[requiredField]; !found {
			missing = append(missing, requiredField)
		}
	}
	if len(missing) != 0 {
		return mysqlReplicationChannelObservation{}, fmt.Errorf(
			"failed to parse non-empty SHOW SLAVE STATUS output: missing required fields: %s",
			strings.Join(missing, ", "),
		)
	}

	return mysqlReplicationChannelObservation{
		Configured:   true,
		MasterHost:   fields["Master_Host"],
		MasterUser:   fields["Master_User"],
		AutoPosition: fields["Auto_Position"],
		IORunning:    fields["Slave_IO_Running"],
		SQLRunning:   fields["Slave_SQL_Running"],
		LastIOError:  fields["Last_IO_Error"],
		LastSQLError: fields["Last_SQL_Error"],
	}, nil
}

func (observation mysqlReplicationChannelObservation) configurationMatches(expectedMasterHost string) bool {
	return observation.Configured &&
		observation.MasterHost == expectedMasterHost &&
		observation.MasterUser == "replica" &&
		observation.AutoPosition == "1"
}

func (observation mysqlReplicationChannelObservation) semanticallyHealthy(expectedMasterHost string) bool {
	return observation.configurationMatches(expectedMasterHost) &&
		observation.IORunning == "Yes" &&
		observation.SQLRunning == "Yes" &&
		observation.LastIOError == "" &&
		observation.LastSQLError == ""
}

func observeMysqlPublishedRole(pod *corev1.Pod) (mysqlPublishedRole, error) {
	canonicalRole, canonicalFound := pod.Labels[LabelMysqlRole]
	legacyRole, legacyFound := pod.Labels[LegacyLabelRole]

	if !canonicalFound && !legacyFound {
		return mysqlPublishedRoleNone, nil
	}
	if canonicalFound != legacyFound {
		return mysqlPublishedRoleNone, fmt.Errorf(
			"Pod %s/%s has incomplete MySQL role labels: %s present=%t, %s present=%t",
			pod.Namespace,
			pod.Name,
			LabelMysqlRole,
			canonicalFound,
			LegacyLabelRole,
			legacyFound,
		)
	}
	if canonicalRole != legacyRole {
		return mysqlPublishedRoleNone, fmt.Errorf(
			"Pod %s/%s has conflicting MySQL role labels: %s=%q, %s=%q",
			pod.Namespace,
			pod.Name,
			LabelMysqlRole,
			canonicalRole,
			LegacyLabelRole,
			legacyRole,
		)
	}

	switch canonicalRole {
	case string(mysqlPublishedRoleMaster):
		return mysqlPublishedRoleMaster, nil
	case string(mysqlPublishedRoleSlave):
		return mysqlPublishedRoleSlave, nil
	default:
		return mysqlPublishedRoleNone, fmt.Errorf(
			"Pod %s/%s has unknown MySQL published role %q",
			pod.Namespace,
			pod.Name,
			canonicalRole,
		)
	}
}

func (r *MysqlClusterReconciler) observeMysqlMemberReplication(
	ctx context.Context,
	pod *corev1.Pod,
	cluster *databasev1.MysqlCluster,
) (mysqlMemberReplicationObservation, error) {
	if err := r.validateMysqlPodBeforeSQL(ctx, pod, cluster, "replication observation SQL"); err != nil {
		return mysqlMemberReplicationObservation{}, err
	}

	ordinal, err := mysqlStatefulSetPodOrdinal(pod)
	if err != nil {
		return mysqlMemberReplicationObservation{}, err
	}
	expectedPodName := mysqlStatefulSetPodName(cluster, ordinal)
	if pod.Name != expectedPodName {
		return mysqlMemberReplicationObservation{}, fmt.Errorf(
			"Pod %s/%s ordinal identity does not match %s label %d: expected name %s",
			pod.Namespace,
			pod.Name,
			statefulSetPodIndexLabel,
			ordinal,
			expectedPodName,
		)
	}

	publishedRole, err := observeMysqlPublishedRole(pod)
	if err != nil {
		return mysqlMemberReplicationObservation{}, err
	}

	output, err := r.executeCommandOnPod(pod, mysqlShowSlaveStatusCommand())
	if err != nil {
		return mysqlMemberReplicationObservation{}, fmt.Errorf(
			"failed to execute SHOW SLAVE STATUS on Pod %s/%s: %w",
			pod.Namespace,
			pod.Name,
			err,
		)
	}
	channel, err := parseMysqlShowSlaveStatus(output)
	if err != nil {
		return mysqlMemberReplicationObservation{}, fmt.Errorf(
			"failed to observe replication channel on Pod %s/%s: %w",
			pod.Namespace,
			pod.Name,
			err,
		)
	}

	return mysqlMemberReplicationObservation{
		PodName:       pod.Name,
		Ordinal:       ordinal,
		PublishedRole: publishedRole,
		Channel:       channel,
	}, nil
}

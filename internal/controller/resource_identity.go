package controller

import (
	"crypto/sha256"
	"fmt"
	"strings"

	databasev1 "github.com/egonlin/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	LabelAppName     = "app.kubernetes.io/name"
	LabelAppInstance = "app.kubernetes.io/instance"
	LabelManagedBy   = "app.kubernetes.io/managed-by"
	LabelMysqlRole   = "mysqlcluster.apps.egonlin.com/role"

	LegacyLabelApp  = "app"
	LegacyLabelRole = "role"

	mysqlAppName   = "mysql"
	mysqlManagedBy = "mysql-operator"
)

const maxChildResourceNameLength = 253

const maxDNSLabelLength = 63

func boundedMysqlChildName(clusterName, suffix string) string {
	candidate := clusterName + suffix
	if len(candidate) <= maxChildResourceNameLength {
		return candidate
	}

	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(clusterName)))[:8]
	hashPart := "-" + hash

	maxPrefixLength := maxChildResourceNameLength - len(hashPart) - len(suffix)
	if maxPrefixLength < 1 {
		panic("mysql child resource suffix exceeds Kubernetes name limit")
	}

	return clusterName[:maxPrefixLength] + hashPart + suffix
}

func mysqlPodName(cluster *databasev1.MysqlCluster, ordinal int) string {
	return boundedMysqlChildName(
		cluster.Name,
		fmt.Sprintf("-mysql-%02d", ordinal),
	)
}

func mysqlPVCName(cluster *databasev1.MysqlCluster, ordinal int) string {
	return mysqlPodName(cluster, ordinal)
}

func mysqlConfigMapName(cluster *databasev1.MysqlCluster, ordinal int) string {
	return boundedMysqlChildName(
		cluster.Name,
		fmt.Sprintf("-mysql-config-%02d", ordinal),
	)
}

func boundedMysqlDNSLabelName(clusterName, suffix string) string {
	candidate := clusterName + suffix
	if len(candidate) <= maxDNSLabelLength && len(validation.IsDNS1123Label(candidate)) == 0 {
		return candidate
	}

	normalizedName := strings.ReplaceAll(clusterName, ".", "-")
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(clusterName)))[:8]
	hashPart := "-" + hash

	maxPrefixLength := maxDNSLabelLength - len(hashPart) - len(suffix)
	if maxPrefixLength < 1 {
		panic("mysql DNS-label resource suffix exceeds Kubernetes name limit")
	}

	prefix := normalizedName
	if len(prefix) > maxPrefixLength {
		prefix = prefix[:maxPrefixLength]
	}
	prefix = strings.TrimRight(prefix, "-")
	if prefix == "" {
		panic("mysql DNS-label resource name has no valid prefix")
	}

	return prefix + hashPart + suffix
}

func mysqlStatefulSetName(cluster *databasev1.MysqlCluster) string {
	return boundedMysqlDNSLabelName(cluster.Name, "-mysql")
}

func mysqlStatefulSetPodName(cluster *databasev1.MysqlCluster, ordinal int32) string {
	return fmt.Sprintf("%s-%d", mysqlStatefulSetName(cluster), ordinal)
}

func mysqlHeadlessServiceName(cluster *databasev1.MysqlCluster) string {
	return boundedMysqlDNSLabelName(cluster.Name, "-mysql-headless")
}

func mysqlSharedConfigMapName(cluster *databasev1.MysqlCluster) string {
	return boundedMysqlChildName(cluster.Name, "-mysql-config")
}

func mysqlPodDisruptionBudgetName(cluster *databasev1.MysqlCluster) string {
	return boundedMysqlChildName(cluster.Name, "-mysql-pdb")
}

func mysqlIdentityLabels(cluster *databasev1.MysqlCluster) map[string]string {
	return map[string]string{
		LabelAppName:     mysqlAppName,
		LabelAppInstance: string(cluster.UID),
		LabelManagedBy:   mysqlManagedBy,
		LegacyLabelApp:   mysqlAppName,
	}
}

func mysqlStatefulSetSelectorLabels(cluster *databasev1.MysqlCluster) map[string]string {
	return map[string]string{
		LabelAppName:     mysqlAppName,
		LabelAppInstance: string(cluster.UID),
		LabelManagedBy:   mysqlManagedBy,
	}
}

func mysqlRoleLabels(cluster *databasev1.MysqlCluster, role string) map[string]string {
	labels := mysqlIdentityLabels(cluster)
	labels[LabelMysqlRole] = role
	labels[LegacyLabelRole] = role
	return labels
}

func mysqlClusterPodListOptions(cluster *databasev1.MysqlCluster, role string) []client.ListOption {
	matchingLabels := map[string]string{
		LabelAppInstance: string(cluster.UID),
	}
	if role != "" {
		matchingLabels[LabelMysqlRole] = role
	}

	return []client.ListOption{
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(matchingLabels),
	}
}

func validateControlledBy(object client.Object, cluster *databasev1.MysqlCluster, resourceKind string) error {
	if metav1.IsControlledBy(object, cluster) {
		return nil
	}

	return fmt.Errorf(
		"%s %s/%s is not controlled by MysqlCluster %s",
		resourceKind,
		object.GetNamespace(),
		object.GetName(),
		cluster.Name,
	)
}

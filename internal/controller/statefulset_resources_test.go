package controller

import (
	"strings"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

func statefulSetResourceTestCluster(name string, uid types.UID) *databasev1.MysqlCluster {
	replicas := int32(3)
	return &databasev1.MysqlCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "mysql-system",
			UID:       uid,
		},
		Spec: databasev1.MysqlClusterSpec{
			Image:                 "example.com/mysql:5.7",
			Replicas:              &replicas,
			CredentialsSecretName: name + "-credentials",
			Storage: databasev1.StorageConfig{
				StorageClassName: "fast-storage",
				Size:             resource.MustParse("10Gi"),
			},
			Resources: databasev1.ResourceRequirements{
				Requests: databasev1.ResourceRequests{
					CPU:    resource.MustParse("250m"),
					Memory: resource.MustParse("256Mi"),
				},
				Limits: databasev1.ResourceLimits{
					CPU:    resource.MustParse("1"),
					Memory: resource.MustParse("1Gi"),
				},
			},
		},
	}
}

func findContainerByName(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func findEnvVarByName(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

func findVolumeMountByName(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == name {
			return &mounts[i]
		}
	}
	return nil
}

func TestStatefulSetResourceFoundation(t *testing.T) {
	t.Run("builds deterministic DNS-label-safe names", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("demo", types.UID("uid-demo"))

		g.Expect(mysqlStatefulSetName(cluster)).To(Equal("demo-mysql"))
		g.Expect(mysqlHeadlessServiceName(cluster)).To(Equal("demo-mysql-headless"))
		g.Expect(mysqlSharedConfigMapName(cluster)).To(Equal("demo-mysql-config"))
		g.Expect(mysqlSharedConfigMapName(cluster)).NotTo(Equal(mysqlConfigMapName(cluster, 1)))

		longCluster := statefulSetResourceTestCluster(strings.Repeat("a", 63), types.UID("uid-long"))
		longStatefulSetName := mysqlStatefulSetName(longCluster)
		longServiceName := mysqlHeadlessServiceName(longCluster)
		g.Expect(len(longStatefulSetName)).To(BeNumerically("<=", maxDNSLabelLength))
		g.Expect(len(longServiceName)).To(BeNumerically("<=", maxDNSLabelLength))
		g.Expect(validation.IsDNS1123Label(longStatefulSetName)).To(BeEmpty())
		g.Expect(validation.IsDNS1123Label(longServiceName)).To(BeEmpty())

		dottedCluster := statefulSetResourceTestCluster("orders.prod.example", types.UID("uid-dotted"))
		dottedName := mysqlHeadlessServiceName(dottedCluster)
		g.Expect(dottedName).NotTo(ContainSubstring("."))
		g.Expect(validation.IsDNS1123Label(dottedName)).To(BeEmpty())
		g.Expect(mysqlHeadlessServiceName(dottedCluster)).To(Equal(dottedName))

		problematicA := statefulSetResourceTestCluster("team.alpha-prod", types.UID("uid-a"))
		problematicB := statefulSetResourceTestCluster("team-alpha.prod", types.UID("uid-b"))
		g.Expect(mysqlStatefulSetName(problematicA)).NotTo(Equal(mysqlStatefulSetName(problematicB)))
	})

	t.Run("builds the governing headless Service", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("service-foundation", types.UID("uid-service"))
		service := desiredMysqlHeadlessService(cluster)

		g.Expect(service.Name).To(Equal(mysqlHeadlessServiceName(cluster)))
		g.Expect(service.Namespace).To(Equal(cluster.Namespace))
		g.Expect(service.Labels).To(Equal(mysqlIdentityLabels(cluster)))
		g.Expect(service.OwnerReferences).To(BeEmpty())
		g.Expect(service.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		g.Expect(service.Spec.Selector).To(Equal(mysqlStatefulSetSelectorLabels(cluster)))
		g.Expect(service.Spec.Selector).To(HaveLen(3))
		g.Expect(service.Spec.Selector).To(HaveKeyWithValue(LabelAppName, "mysql"))
		g.Expect(service.Spec.Selector).To(HaveKeyWithValue(LabelAppInstance, string(cluster.UID)))
		g.Expect(service.Spec.Selector).To(HaveKeyWithValue(LabelManagedBy, "mysql-operator"))
		g.Expect(service.Spec.Selector).NotTo(HaveKey(LabelMysqlRole))
		g.Expect(service.Spec.Selector).NotTo(HaveKey(LegacyLabelRole))
		g.Expect(service.Spec.PublishNotReadyAddresses).To(BeFalse())
		g.Expect(service.Spec.Ports).To(HaveLen(1))
		g.Expect(service.Spec.Ports[0].Protocol).To(Equal(corev1.ProtocolTCP))
		g.Expect(service.Spec.Ports[0].Port).To(Equal(int32(3306)))

		otherCluster := statefulSetResourceTestCluster("service-foundation-other", types.UID("uid-service-other"))
		g.Expect(desiredMysqlHeadlessService(otherCluster).Spec.Selector).
			NotTo(Equal(service.Spec.Selector))
	})

	t.Run("builds the shared base ConfigMap without a fixed server ID", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("config-foundation", types.UID("uid-config"))
		configMap := desiredMysqlSharedConfigMap(cluster)

		g.Expect(configMap.Name).To(Equal(mysqlSharedConfigMapName(cluster)))
		g.Expect(configMap.Namespace).To(Equal(cluster.Namespace))
		g.Expect(configMap.Labels).To(Equal(mysqlIdentityLabels(cluster)))
		g.Expect(configMap.OwnerReferences).To(BeEmpty())

		config := configMap.Data["my.cnf"]
		g.Expect(configMap.Data).NotTo(HaveKey(mysqlRootPasswordSecretKey))
		g.Expect(configMap.Data).NotTo(HaveKey(mysqlReplicationPasswordSecretKey))
		g.Expect(config).NotTo(ContainSubstring("MYSQL_ROOT_PASSWORD"))
		g.Expect(config).NotTo(ContainSubstring("MYSQL_REPLICATION_PASSWORD"))
		g.Expect(config).To(ContainSubstring("[mysqld]"))
		g.Expect(config).To(ContainSubstring("binlog_format=row"))
		g.Expect(config).To(ContainSubstring("log-bin=mysql-bin"))
		g.Expect(config).To(ContainSubstring("gtid-mode=on"))
		g.Expect(config).To(ContainSubstring("enforce-gtid-consistency=true"))
		g.Expect(config).NotTo(ContainSubstring("server-id="))
	})

	t.Run("builds the StatefulSet workload and storage foundation", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("stateful-foundation", types.UID("uid-stateful"))
		statefulSet := desiredMysqlStatefulSet(cluster)

		g.Expect(statefulSet.Name).To(Equal(mysqlStatefulSetName(cluster)))
		g.Expect(statefulSet.Namespace).To(Equal(cluster.Namespace))
		g.Expect(statefulSet.Labels).To(Equal(mysqlIdentityLabels(cluster)))
		g.Expect(statefulSet.OwnerReferences).To(BeEmpty())
		g.Expect(statefulSet.Spec.Replicas).NotTo(BeNil())
		g.Expect(*statefulSet.Spec.Replicas).To(Equal(desiredReplicas(cluster)))
		g.Expect(statefulSet.Spec.Ordinals).NotTo(BeNil())
		g.Expect(statefulSet.Spec.Ordinals.Start).To(Equal(int32(1)))
		g.Expect(statefulSet.Spec.ServiceName).To(Equal(mysqlHeadlessServiceName(cluster)))
		g.Expect(statefulSet.Spec.PodManagementPolicy).To(Equal(appsv1.OrderedReadyPodManagement))
		g.Expect(statefulSet.Spec.UpdateStrategy.Type).To(Equal(appsv1.OnDeleteStatefulSetStrategyType))

		expectedSelector := mysqlStatefulSetSelectorLabels(cluster)
		g.Expect(statefulSet.Spec.Selector.MatchLabels).To(Equal(expectedSelector))
		g.Expect(statefulSet.Spec.Selector.MatchLabels).To(HaveLen(3))
		g.Expect(statefulSet.Spec.Selector.MatchLabels).To(HaveKeyWithValue(LabelAppName, "mysql"))
		g.Expect(statefulSet.Spec.Selector.MatchLabels).To(HaveKeyWithValue(LabelAppInstance, string(cluster.UID)))
		g.Expect(statefulSet.Spec.Selector.MatchLabels).To(HaveKeyWithValue(LabelManagedBy, "mysql-operator"))
		g.Expect(statefulSet.Spec.Selector.MatchLabels).NotTo(HaveKey(LabelMysqlRole))
		g.Expect(statefulSet.Spec.Selector.MatchLabels).NotTo(HaveKey(LegacyLabelRole))
		for key, value := range statefulSet.Spec.Selector.MatchLabels {
			g.Expect(statefulSet.Spec.Template.Labels).To(HaveKeyWithValue(key, value))
		}
		g.Expect(statefulSet.Spec.Template.Labels).NotTo(HaveKey(LabelMysqlRole))
		g.Expect(statefulSet.Spec.Template.Labels).NotTo(HaveKey(LegacyLabelRole))

		g.Expect(statefulSet.Spec.VolumeClaimTemplates).To(HaveLen(1))
		volumeClaimTemplate := statefulSet.Spec.VolumeClaimTemplates[0]
		g.Expect(volumeClaimTemplate.Name).To(Equal(mysqlDataVolume))
		g.Expect(volumeClaimTemplate.Labels).To(Equal(mysqlIdentityLabels(cluster)))
		g.Expect(volumeClaimTemplate.Spec.AccessModes).To(Equal([]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}))
		g.Expect(volumeClaimTemplate.Spec.StorageClassName).NotTo(BeNil())
		g.Expect(*volumeClaimTemplate.Spec.StorageClassName).To(Equal(cluster.Spec.Storage.StorageClassName))
		requestedStorage := volumeClaimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
		g.Expect(requestedStorage.Cmp(cluster.Spec.Storage.Size)).To(Equal(0))
		g.Expect(statefulSet.Spec.PersistentVolumeClaimRetentionPolicy).To(BeNil())

		mysqlContainer := findContainerByName(statefulSet.Spec.Template.Spec.Containers, mysqlContainerName)
		g.Expect(mysqlContainer).NotTo(BeNil())
		g.Expect(mysqlContainer.Image).To(Equal(cluster.Spec.Image))
		g.Expect(mysqlContainer.Ports).To(ContainElement(corev1.ContainerPort{Name: "mysql", ContainerPort: 3306, Protocol: corev1.ProtocolTCP}))
		rootPassword := findEnvVarByName(mysqlContainer.Env, "MYSQL_ROOT_PASSWORD")
		g.Expect(rootPassword).NotTo(BeNil())
		g.Expect(rootPassword.Value).To(BeEmpty())
		g.Expect(rootPassword.ValueFrom).NotTo(BeNil())
		g.Expect(rootPassword.ValueFrom.SecretKeyRef).NotTo(BeNil())
		g.Expect(rootPassword.ValueFrom.SecretKeyRef.Name).To(Equal(cluster.Spec.CredentialsSecretName))
		g.Expect(rootPassword.ValueFrom.SecretKeyRef.Key).To(Equal(mysqlRootPasswordSecretKey))
		replicationPassword := findEnvVarByName(mysqlContainer.Env, "MYSQL_REPLICATION_PASSWORD")
		g.Expect(replicationPassword).NotTo(BeNil())
		g.Expect(replicationPassword.Value).To(BeEmpty())
		g.Expect(replicationPassword.ValueFrom).NotTo(BeNil())
		g.Expect(replicationPassword.ValueFrom.SecretKeyRef).NotTo(BeNil())
		g.Expect(replicationPassword.ValueFrom.SecretKeyRef.Name).To(Equal(cluster.Spec.CredentialsSecretName))
		g.Expect(replicationPassword.ValueFrom.SecretKeyRef.Key).To(Equal(mysqlReplicationPasswordSecretKey))
		requestedCPU := mysqlContainer.Resources.Requests[corev1.ResourceCPU]
		requestedMemory := mysqlContainer.Resources.Requests[corev1.ResourceMemory]
		limitedCPU := mysqlContainer.Resources.Limits[corev1.ResourceCPU]
		limitedMemory := mysqlContainer.Resources.Limits[corev1.ResourceMemory]
		g.Expect(requestedCPU.Cmp(cluster.Spec.Resources.Requests.CPU)).To(Equal(0))
		g.Expect(requestedMemory.Cmp(cluster.Spec.Resources.Requests.Memory)).To(Equal(0))
		g.Expect(limitedCPU.Cmp(cluster.Spec.Resources.Limits.CPU)).To(Equal(0))
		g.Expect(limitedMemory.Cmp(cluster.Spec.Resources.Limits.Memory)).To(Equal(0))

		configMount := findVolumeMountByName(mysqlContainer.VolumeMounts, mysqlConfigRuntimeVolume)
		g.Expect(configMount).NotTo(BeNil())
		g.Expect(configMount.MountPath).To(Equal(mysqlConfigPath))
		g.Expect(configMount.SubPath).To(Equal("my.cnf"))
		dataMount := findVolumeMountByName(mysqlContainer.VolumeMounts, mysqlDataVolume)
		g.Expect(dataMount).NotTo(BeNil())
		g.Expect(dataMount.MountPath).To(Equal(mysqlDataPath))

		g.Expect(statefulSet.Spec.Template.Spec.InitContainers).To(HaveLen(1))
		initContainer := statefulSet.Spec.Template.Spec.InitContainers[0]
		g.Expect(initContainer.Name).To(Equal(mysqlConfigInitName))
		g.Expect(initContainer.Image).To(Equal(cluster.Spec.Image))
		g.Expect(initContainer.Command[:2]).To(Equal([]string{"/bin/sh", "-ec"}))
		command := strings.Join(initContainer.Command, " ")
		g.Expect(command).To(ContainSubstring("POD_INDEX"))
		g.Expect(command).To(ContainSubstring("POD_INDEX is required"))
		g.Expect(command).To(ContainSubstring("*[!0-9]*"))
		g.Expect(command).To(ContainSubstring("-lt 1"))
		g.Expect(command).To(ContainSubstring("cp /config-base/my.cnf /config-runtime/my.cnf"))
		g.Expect(command).To(ContainSubstring("server-id=%s"))
		g.Expect(command).NotTo(ContainSubstring("hostname"))
		g.Expect(command).NotTo(ContainSubstring("metadata.name"))
		g.Expect(initContainer.Env).To(HaveLen(1))
		g.Expect(initContainer.Env[0].Name).To(Equal("POD_INDEX"))
		g.Expect(initContainer.Env[0].ValueFrom).NotTo(BeNil())
		g.Expect(initContainer.Env[0].ValueFrom.FieldRef).NotTo(BeNil())
		g.Expect(initContainer.Env[0].ValueFrom.FieldRef.FieldPath).
			To(Equal("metadata.labels['apps.kubernetes.io/pod-index']"))
		baseMount := findVolumeMountByName(initContainer.VolumeMounts, mysqlConfigBaseVolume)
		g.Expect(baseMount).NotTo(BeNil())
		g.Expect(baseMount.MountPath).To(Equal(mysqlConfigBasePath))
		g.Expect(baseMount.ReadOnly).To(BeTrue())
		runtimeMount := findVolumeMountByName(initContainer.VolumeMounts, mysqlConfigRuntimeVolume)
		g.Expect(runtimeMount).NotTo(BeNil())
		g.Expect(runtimeMount.MountPath).To(Equal(mysqlConfigRuntimePath))

		g.Expect(statefulSet.Spec.Template.Spec.Volumes).To(HaveLen(2))
		g.Expect(statefulSet.Spec.Template.Spec.Volumes[0].ConfigMap).NotTo(BeNil())
		g.Expect(statefulSet.Spec.Template.Spec.Volumes[0].ConfigMap.Name).To(Equal(mysqlSharedConfigMapName(cluster)))
		g.Expect(statefulSet.Spec.Template.Spec.Volumes[1].EmptyDir).NotTo(BeNil())
	})
}

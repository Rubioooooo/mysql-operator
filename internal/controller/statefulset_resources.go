package controller

import (
	databasev1 "github.com/egonlin/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	mysqlContainerName       = "mysql"
	mysqlConfigInitName      = "mysql-config-init"
	mysqlConfigBaseVolume    = "mysql-config-base"
	mysqlConfigRuntimeVolume = "mysql-config-runtime"
	mysqlDataVolume          = "mysql-data"

	mysqlConfigBasePath    = "/config-base"
	mysqlConfigRuntimePath = "/config-runtime"
	mysqlConfigPath        = "/etc/my.cnf"
	mysqlDataPath          = "/var/lib/mysql"
)

const mysqlBaseConfig = `[mysqld]
binlog_format=row
log-bin=mysql-bin
skip-name-resolve
gtid-mode=on
enforce-gtid-consistency=true
log-slave-updates=1
relay_log_purge=0
`

const mysqlConfigInitScript = `: "${POD_INDEX:?POD_INDEX is required}"
case "${POD_INDEX}" in
  *[!0-9]*) echo "POD_INDEX must be a decimal number" >&2; exit 1 ;;
esac
if [ "${POD_INDEX}" -lt 1 ]; then
  echo "POD_INDEX must be at least 1" >&2
  exit 1
fi
cp /config-base/my.cnf /config-runtime/my.cnf
printf '\nserver-id=%s\n' "${POD_INDEX}" >> /config-runtime/my.cnf
`

func desiredMysqlHeadlessService(cluster *databasev1.MysqlCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mysqlHeadlessServiceName(cluster),
			Namespace: cluster.Namespace,
			Labels:    mysqlIdentityLabels(cluster),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  mysqlStatefulSetSelectorLabels(cluster),
			Ports: []corev1.ServicePort{
				{
					Name:       "mysql",
					Protocol:   corev1.ProtocolTCP,
					Port:       3306,
					TargetPort: intstr.FromInt32(3306),
				},
			},
		},
	}
}

func desiredMysqlSharedConfigMap(cluster *databasev1.MysqlCluster) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mysqlSharedConfigMapName(cluster),
			Namespace: cluster.Namespace,
			Labels:    mysqlIdentityLabels(cluster),
		},
		Data: map[string]string{
			"my.cnf": mysqlBaseConfig,
		},
	}
}

func desiredMysqlStatefulSet(cluster *databasev1.MysqlCluster) *appsv1.StatefulSet {
	replicas := desiredReplicas(cluster)
	storageClassName := cluster.Spec.Storage.StorageClassName

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mysqlStatefulSetName(cluster),
			Namespace: cluster.Namespace,
			Labels:    mysqlIdentityLabels(cluster),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: mysqlHeadlessServiceName(cluster),
			Ordinals: &appsv1.StatefulSetOrdinals{
				Start: 1,
			},
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.OnDeleteStatefulSetStrategyType,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: mysqlStatefulSetSelectorLabels(cluster),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: mysqlIdentityLabels(cluster),
				},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Name:    mysqlConfigInitName,
							Image:   cluster.Spec.Image,
							Command: []string{"/bin/sh", "-ec", mysqlConfigInitScript},
							Env: []corev1.EnvVar{
								{
									Name: "POD_INDEX",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											APIVersion: "v1",
											FieldPath:  "metadata.labels['apps.kubernetes.io/pod-index']",
										},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: mysqlConfigBaseVolume, MountPath: mysqlConfigBasePath, ReadOnly: true},
								{Name: mysqlConfigRuntimeVolume, MountPath: mysqlConfigRuntimePath},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:  mysqlContainerName,
							Image: cluster.Spec.Image,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    cluster.Spec.Resources.Requests.CPU,
									corev1.ResourceMemory: cluster.Spec.Resources.Requests.Memory,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    cluster.Spec.Resources.Limits.CPU,
									corev1.ResourceMemory: cluster.Spec.Resources.Limits.Memory,
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "MYSQL_ROOT_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.CredentialsSecretName},
											Key:                  mysqlRootPasswordSecretKey,
										},
									},
								},
								{
									Name: "MYSQL_REPLICATION_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Spec.CredentialsSecretName},
											Key:                  mysqlReplicationPasswordSecretKey,
										},
									},
								},
							},
							Ports: []corev1.ContainerPort{
								{Name: "mysql", ContainerPort: 3306, Protocol: corev1.ProtocolTCP},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: mysqlConfigRuntimeVolume, MountPath: mysqlConfigPath, SubPath: "my.cnf"},
								{Name: mysqlDataVolume, MountPath: mysqlDataPath},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: mysqlConfigBaseVolume,
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: mysqlSharedConfigMapName(cluster),
									},
								},
							},
						},
						{
							Name: mysqlConfigRuntimeVolume,
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:   mysqlDataVolume,
						Labels: mysqlIdentityLabels(cluster),
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: cluster.Spec.Storage.Size,
							},
						},
						StorageClassName: &storageClassName,
					},
				},
			},
		},
	}
}

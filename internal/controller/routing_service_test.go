package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type rolePublicationOrderClient struct {
	*statefulSetReconcileMemoryClient
	publications []string
}

func (c *rolePublicationOrderClient) Update(
	ctx context.Context,
	object client.Object,
	options ...client.UpdateOption,
) error {
	if pod, ok := object.(*corev1.Pod); ok && pod.Labels[LabelMysqlRole] != "" {
		c.publications = append(c.publications, fmt.Sprintf("%s:%s", pod.Name, pod.Labels[LabelMysqlRole]))
	}
	return c.statefulSetReconcileMemoryClient.Update(ctx, object, options...)
}

func TestMysqlRoutingServiceReconciliation(t *testing.T) {
	ctx := context.Background()

	t.Run("builds canonical-only primary and replica selectors", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-desired", types.UID("routing-desired-uid"))

		for _, testCase := range []struct {
			name string
			role string
		}{
			{name: "primary", role: "master"},
			{name: "replica", role: "slave"},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				service := desiredMysqlRoutingService(cluster, "routing-"+testCase.name, testCase.role)
				g.Expect(service.Spec.Selector).To(Equal(map[string]string{
					LabelAppName:     mysqlAppName,
					LabelAppInstance: string(cluster.UID),
					LabelManagedBy:   mysqlManagedBy,
					LabelMysqlRole:   testCase.role,
				}))
				g.Expect(service.Spec.Selector).NotTo(HaveKey(LegacyLabelApp))
				g.Expect(service.Spec.Selector).NotTo(HaveKey(LegacyLabelRole))
			})
		}
	})

	t.Run("creates owned primary and replica routing Services", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-create", types.UID("routing-create-uid"))
		scheme := newStatefulSetReconcileTestScheme(t)
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme)

		for _, testCase := range []struct {
			name string
			role string
		}{
			{name: "routing-primary", role: "master"},
			{name: "routing-replica", role: "slave"},
		} {
			service, err := reconciler.ensureMysqlRoutingService(
				ctx,
				desiredMysqlRoutingService(cluster, testCase.name, testCase.role),
				cluster,
			)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(metav1.IsControlledBy(service, cluster)).To(BeTrue())
			g.Expect(service.Spec.Selector).To(Equal(mysqlRoutingSelectorLabels(cluster, testCase.role)))
			g.Expect(service.Spec.Ports).To(Equal([]corev1.ServicePort{{
				Name: "mysql", Protocol: corev1.ProtocolTCP, Port: 3306, TargetPort: intstr.FromInt32(3306),
			}}))
			g.Expect(service.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
		}
	})

	t.Run("repairs mutable drift while preserving API-assigned network identity", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-drift", types.UID("routing-drift-uid"))
		scheme := newStatefulSetReconcileTestScheme(t)
		service := desiredMysqlRoutingService(cluster, "routing-drift-primary", "master")
		service.Labels = map[string]string{"drifted": "true"}
		service.Spec.Selector = map[string]string{"wrong": "selector"}
		service.Spec.Ports = []corev1.ServicePort{{Name: "wrong", Port: 1234, NodePort: 31234}}
		service.Spec.Type = corev1.ServiceTypeNodePort
		service.Spec.ClusterIP = "10.96.0.25"
		service.Spec.ClusterIPs = []string{"10.96.0.25"}
		service.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
		policy := corev1.IPFamilyPolicySingleStack
		service.Spec.IPFamilyPolicy = &policy
		setControllerReferenceForTest(t, scheme, cluster, service)
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, service)

		result, err := reconciler.ensureMysqlRoutingService(
			ctx,
			desiredMysqlRoutingService(cluster, service.Name, "master"),
			cluster,
		)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(result.Labels).To(Equal(mysqlRoleLabels(cluster, "master")))
		g.Expect(result.Spec.Selector).To(Equal(mysqlRoutingSelectorLabels(cluster, "master")))
		g.Expect(result.Spec.Ports).To(Equal(desiredMysqlRoutingService(cluster, service.Name, "master").Spec.Ports))
		g.Expect(result.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
		g.Expect(result.Spec.ClusterIP).To(Equal("10.96.0.25"))
		g.Expect(result.Spec.ClusterIPs).To(Equal([]string{"10.96.0.25"}))
		g.Expect(result.Spec.IPFamilies).To(Equal([]corev1.IPFamily{corev1.IPv4Protocol}))
		g.Expect(result.Spec.IPFamilyPolicy).To(Equal(&policy))
	})

	t.Run("rejects foreign ownership without mutation", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-foreign", types.UID("routing-foreign-uid"))
		scheme := newStatefulSetReconcileTestScheme(t)
		foreign := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "routing-foreign-primary", Namespace: cluster.Namespace},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"sentinel": "unchanged"}, Ports: []corev1.ServicePort{{Port: 3306}}},
		}
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, foreign)

		_, err := reconciler.ensureMysqlRoutingService(
			ctx,
			desiredMysqlRoutingService(cluster, foreign.Name, "master"),
			cluster,
		)
		g.Expect(err).To(MatchError(ContainSubstring("is not controlled by MysqlCluster")))
		stored := &corev1.Service{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(foreign), stored)
		g.Expect(stored.Spec.Selector).To(Equal(map[string]string{"sentinel": "unchanged"}))
		g.Expect(stored.OwnerReferences).To(BeEmpty())
	})

	t.Run("rejects owned headless routing drift without replacement", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("routing-headless", types.UID("routing-headless-uid"))
		scheme := newStatefulSetReconcileTestScheme(t)
		headless := desiredMysqlRoutingService(cluster, "routing-headless-primary", "master")
		headless.Spec.ClusterIP = corev1.ClusterIPNone
		setControllerReferenceForTest(t, scheme, cluster, headless)
		reconciler := newStatefulSetReconcileTestReconciler(t, scheme, headless)

		_, err := reconciler.ensureMysqlRoutingService(
			ctx,
			desiredMysqlRoutingService(cluster, headless.Name, "master"),
			cluster,
		)
		g.Expect(err).To(MatchError(ContainSubstring("is headless and cannot be reconciled in place")))
		stored := &corev1.Service{}
		getObjectForTest(t, reconciler, client.ObjectKeyFromObject(headless), stored)
		g.Expect(stored.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
	})
}

func TestPrimaryRolePublicationOrdering(t *testing.T) {
	ctx := context.Background()
	cluster := statefulSetResourceTestCluster("role-order", types.UID("role-order-uid"))
	cluster.Spec.MasterService = "role-order-primary"

	newPrimary := func(t *testing.T) (client.Object, *corev1.Pod) {
		t.Helper()
		statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("role-order-statefulset-uid"))
		pod := statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1)
		return statefulSet, pod
	}

	t.Run("failed database transition does not publish primary role", func(t *testing.T) {
		g := NewWithT(t)
		statefulSet, pod := newPrimary(t)
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, pod),
			execCommandOnPodFn: func(*corev1.Pod, string) (string, error) {
				return "", errors.New("database transition failed")
			},
		}

		err := reconciler.setupMasterSlaveReplication(ctx, pod.Name, nil, *cluster)
		g.Expect(err).To(MatchError(ContainSubstring("database transition failed")))
		stored := &corev1.Pod{}
		g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
		g.Expect(stored.Labels).NotTo(HaveKey(LabelMysqlRole))
		g.Expect(stored.Labels).NotTo(HaveKey(LegacyLabelRole))
	})

	t.Run("successful database transition precedes primary role publication", func(t *testing.T) {
		g := NewWithT(t)
		statefulSet, pod := newPrimary(t)
		var reconciler *MysqlClusterReconciler
		reconciler = &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, pod),
			execCommandOnPodFn: func(commandPod *corev1.Pod, _ string) (string, error) {
				observed := &corev1.Pod{}
				g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(commandPod), observed)).To(Succeed())
				g.Expect(observed.Labels).NotTo(HaveKey(LabelMysqlRole))
				return "", nil
			},
		}

		g.Expect(reconciler.setupMasterSlaveReplication(ctx, pod.Name, nil, *cluster)).To(Succeed())
		stored := &corev1.Pod{}
		g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
		g.Expect(stored.Labels).To(HaveKeyWithValue(LabelMysqlRole, "master"))
		g.Expect(stored.Labels).To(HaveKeyWithValue(LegacyLabelRole, "master"))
	})
}

func TestInitializationTopologyPublicationOrdering(t *testing.T) {
	ctx := context.Background()
	cluster := statefulSetResourceTestCluster("initial-role-order", types.UID("initial-role-order-uid"))
	cluster.Spec.MasterService = "initial-role-order-primary"
	statefulSet := controlledStatefulSetForLifecycleTest(t, cluster, types.UID("initial-role-order-statefulset-uid"))
	newTopology := func(t *testing.T) (*corev1.Pod, *corev1.Pod) {
		t.Helper()
		return statefulSetPodForLifecycleTest(t, cluster, statefulSet, 1),
			statefulSetPodForLifecycleTest(t, cluster, statefulSet, 2)
	}
	initialPrimaryHost := cluster.Spec.MasterService

	assertNoPublishedRoles := func(g *WithT, reconciler *MysqlClusterReconciler, pods ...*corev1.Pod) {
		for _, pod := range pods {
			stored := &corev1.Pod{}
			g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(pod), stored)).To(Succeed())
			g.Expect(stored.Labels).NotTo(HaveKey(LabelMysqlRole))
			g.Expect(stored.Labels).NotTo(HaveKey(LegacyLabelRole))
		}
	}

	t.Run("replica database failure publishes no topology roles", func(t *testing.T) {
		g := NewWithT(t)
		primary, replica := newTopology(t)
		var reconciler *MysqlClusterReconciler
		reconciler = &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, primary, replica),
			execCommandOnPodFn: func(pod *corev1.Pod, command string) (string, error) {
				assertNoPublishedRoles(g, reconciler, primary, replica)
				switch {
				case pod.Name == primary.Name && command == mysqlPreparePrimaryCommand():
					return "", nil
				case pod.Name == replica.Name && command == mysqlShowSlaveStatusCommand():
					return "", nil
				case pod.Name == replica.Name && command == mysqlInitializeReplicaCommand(initialPrimaryHost):
					return "", errors.New("replica database transition failed")
				default:
					return "", fmt.Errorf("unexpected command for %s: %s", pod.Name, command)
				}
			},
		}

		converged, err := reconciler.reconcileMysqlInitializationTopology(ctx, primary.Name, []string{replica.Name}, *cluster)
		g.Expect(err).To(MatchError(ContainSubstring("replica database transition failed")))
		g.Expect(converged).To(BeFalse())
		assertNoPublishedRoles(g, reconciler, primary, replica)
	})

	t.Run("stage A database operations precede master-only publication", func(t *testing.T) {
		g := NewWithT(t)
		primary, replica := newTopology(t)
		memoryClient := newStatefulSetReconcileMemoryClient(statefulSet, primary, replica)
		orderedClient := &rolePublicationOrderClient{statefulSetReconcileMemoryClient: memoryClient}
		commands := make([]string, 0, 3)
		var reconciler *MysqlClusterReconciler
		reconciler = &MysqlClusterReconciler{
			Client: orderedClient,
			execCommandOnPodFn: func(pod *corev1.Pod, command string) (string, error) {
				assertNoPublishedRoles(g, reconciler, primary, replica)
				commands = append(commands, pod.Name+":"+command)
				return "", nil
			},
		}

		converged, err := reconciler.reconcileMysqlInitializationTopology(ctx, primary.Name, []string{replica.Name}, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(converged).To(BeFalse())
		g.Expect(commands).To(Equal([]string{
			primary.Name + ":" + mysqlPreparePrimaryCommand(),
			replica.Name + ":" + mysqlShowSlaveStatusCommand(),
			replica.Name + ":" + mysqlInitializeReplicaCommand(initialPrimaryHost),
		}))
		g.Expect(orderedClient.publications).To(Equal([]string{primary.Name + ":master"}))

		freshCommand := mysqlInitializeReplicaCommand(initialPrimaryHost)
		g.Expect(freshCommand).To(ContainSubstring("CHANGE MASTER TO"))
		g.Expect(freshCommand).To(ContainSubstring("MASTER_AUTO_POSITION=1"))
		g.Expect(freshCommand).To(ContainSubstring("START SLAVE"))
		g.Expect(freshCommand).NotTo(ContainSubstring("STOP SLAVE"))
		g.Expect(freshCommand).To(ContainSubstring(initialPrimaryHost))
		g.Expect(freshCommand).NotTo(ContainSubstring(mysqlHeadlessServiceName(cluster)))
	})

	t.Run("already configured initialization retry uses stop change start with stable Primary Service", func(t *testing.T) {
		g := NewWithT(t)
		primary, replica := newTopology(t)
		var retryCommand string
		reconciler := &MysqlClusterReconciler{
			Client: newStatefulSetReconcileMemoryClient(statefulSet, primary, replica),
			execCommandOnPodFn: func(pod *corev1.Pod, command string) (string, error) {
				switch {
				case pod.Name == primary.Name && command == mysqlPreparePrimaryCommand():
					return "", nil
				case pod.Name == replica.Name && command == mysqlShowSlaveStatusCommand():
					return mysqlSlaveStatusOutputForTest(
						"previous-primary",
						"replica",
						"1",
						"Yes",
						"Yes",
						"",
						"",
					), nil
				case pod.Name == replica.Name:
					retryCommand = command
					return "", nil
				default:
					return "", fmt.Errorf("unexpected command for %s: %s", pod.Name, command)
				}
			},
		}

		converged, err := reconciler.reconcileMysqlInitializationTopology(ctx, primary.Name, []string{replica.Name}, *cluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(converged).To(BeFalse())
		g.Expect(retryCommand).To(Equal(mysqlConfigureReplicaCommand(initialPrimaryHost)))
		g.Expect(strings.Index(retryCommand, "STOP SLAVE")).To(BeNumerically("<", strings.Index(retryCommand, "CHANGE MASTER TO")))
		g.Expect(retryCommand).To(ContainSubstring(initialPrimaryHost))
		g.Expect(retryCommand).NotTo(ContainSubstring(mysqlHeadlessServiceName(cluster)))
	})
}

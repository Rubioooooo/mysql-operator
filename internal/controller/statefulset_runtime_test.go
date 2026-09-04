package controller

import (
	"context"
	"fmt"
	"testing"

	databasev1 "github.com/egonlin/api/v1"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type lifecycleAnnotationPatchClient struct {
	*statefulSetReconcileMemoryClient
}

func (c *lifecycleAnnotationPatchClient) Patch(
	ctx context.Context,
	object client.Object,
	_ client.Patch,
	_ ...client.PatchOption,
) error {
	return c.Update(ctx, object)
}

func TestStatefulSetRuntimeHelpers(t *testing.T) {
	t.Run("builds initial topology exclusively from StatefulSet ordinals", func(t *testing.T) {
		g := NewWithT(t)
		cluster := statefulSetResourceTestCluster("runtime-topology", types.UID("runtime-topology-uid"))

		primary, replicas := mysqlStatefulSetInitialTopology(cluster)

		g.Expect(primary).To(Equal("runtime-topology-mysql-1"))
		g.Expect(replicas).To(Equal([]string{
			"runtime-topology-mysql-2",
			"runtime-topology-mysql-3",
		}))
		g.Expect(primary).NotTo(ContainSubstring("-01"))
	})

	t.Run("uses Kubernetes default replicas when the stored StatefulSet pointer is nil", func(t *testing.T) {
		g := NewWithT(t)
		statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "runtime-replicas"}}
		g.Expect(mysqlStatefulSetCurrentReplicas(statefulSet)).To(Equal(int32(1)))

		replicas := int32(4)
		statefulSet.Spec.Replicas = &replicas
		g.Expect(mysqlStatefulSetCurrentReplicas(statefulSet)).To(Equal(int32(4)))
	})
}

func TestMysqlClusterLifecycleAnnotationContract(t *testing.T) {
	ctx := context.Background()
	annotationValue := func(value string) *string { return &value }

	testCases := []struct {
		name              string
		annotation        *string
		expectRuntimePath bool
		expectInvalid     bool
	}{
		{name: "absent", annotation: nil, expectRuntimePath: false},
		{name: "true", annotation: annotationValue("true"), expectRuntimePath: true},
		{name: "false", annotation: annotationValue("false"), expectInvalid: true},
		{name: "empty", annotation: annotationValue(""), expectInvalid: true},
		{name: "garbage", annotation: annotationValue("garbage"), expectInvalid: true},
		{name: "uppercase-true", annotation: annotationValue("TRUE"), expectInvalid: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			g := NewWithT(t)
			cluster := phase1HCluster("lifecycle-contract-"+testCase.name, false)
			if testCase.annotation != nil {
				cluster.Annotations = map[string]string{
					mysqlClusterInitializedAnnotation: *testCase.annotation,
				}
			}
			if testCase.expectRuntimePath {
				cluster.Status.LastConvergedReplicas = replicaCountCopy(desiredReplicas(cluster))
				// This exercises initialized child recreation, not legacy migration.
				cluster.Status.LastConvergedImage = cluster.Spec.Image
			}

			initialized, stateErr := mysqlClusterIsInitialized(cluster)
			if testCase.expectInvalid {
				g.Expect(stateErr).To(HaveOccurred())
				g.Expect(initialized).To(BeFalse())
			} else {
				g.Expect(stateErr).NotTo(HaveOccurred())
				g.Expect(initialized).To(Equal(testCase.expectRuntimePath))
			}

			reconciler := phase1HReconciler(t, cluster, phase1HCredentialSecret(cluster))
			execCalls := 0
			reconciler.execCommandOnPodFn = func(*corev1.Pod, string) (string, error) {
				execCalls++
				return "", nil
			}

			result, reconcileErr := reconcileAfterObservability(
				ctx, reconciler,
				ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)},
			)
			g.Expect(result).To(Equal(ctrl.Result{}))
			g.Expect(execCalls).To(Equal(0))

			statefulSet := &appsv1.StatefulSet{}
			statefulSetKey := client.ObjectKey{
				Namespace: cluster.Namespace,
				Name:      mysqlStatefulSetName(cluster),
			}
			statefulSetErr := reconciler.Get(ctx, statefulSetKey, statefulSet)

			switch {
			case testCase.expectInvalid:
				g.Expect(reconcileErr).To(MatchError(ContainSubstring(
					fmt.Sprintf("MysqlCluster %s/%s has invalid %s annotation value %q", cluster.Namespace, cluster.Name, mysqlClusterInitializedAnnotation, *testCase.annotation),
				)))
				g.Expect(apierrors.IsNotFound(statefulSetErr)).To(BeTrue())
				for _, object := range []client.Object{
					&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cluster.Spec.MasterService, Namespace: cluster.Namespace}},
					&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: mysqlSharedConfigMapName(cluster), Namespace: cluster.Namespace}},
				} {
					getErr := reconciler.Get(ctx, client.ObjectKeyFromObject(object), object)
					g.Expect(apierrors.IsNotFound(getErr)).To(BeTrue())
				}
			case testCase.expectRuntimePath:
				g.Expect(reconcileErr).To(HaveOccurred())
				g.Expect(reconcileErr.Error()).To(ContainSubstring(cluster.Spec.MasterService))
				g.Expect(statefulSetErr).NotTo(HaveOccurred())
			default:
				g.Expect(reconcileErr).NotTo(HaveOccurred())
				g.Expect(statefulSetErr).NotTo(HaveOccurred())
			}
		})
	}
}

func TestMarkMysqlClusterInitializedWritesCanonicalValue(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	cluster := phase1HCluster("lifecycle-contract-write", false)
	cluster.Annotations = map[string]string{"unrelated": "preserved"}
	memoryClient := newStatefulSetReconcileMemoryClient(cluster)
	reconciler := &MysqlClusterReconciler{
		Client: &lifecycleAnnotationPatchClient{statefulSetReconcileMemoryClient: memoryClient},
		Scheme: newStatefulSetReconcileTestScheme(t),
	}

	g.Expect(reconciler.markMysqlClusterInitialized(ctx, cluster)).To(Succeed())
	stored := &databasev1.MysqlCluster{}
	g.Expect(reconciler.Get(ctx, client.ObjectKeyFromObject(cluster), stored)).To(Succeed())
	g.Expect(stored.Annotations).To(HaveKeyWithValue(mysqlClusterInitializedAnnotation, "true"))
	g.Expect(stored.Annotations).To(HaveKeyWithValue("unrelated", "preserved"))

	updatesBeforeNoOp := memoryClient.updateCount
	g.Expect(reconciler.markMysqlClusterInitialized(ctx, stored)).To(Succeed())
	g.Expect(memoryClient.updateCount).To(Equal(updatesBeforeNoOp))
}

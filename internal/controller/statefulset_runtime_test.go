package controller

import (
	"testing"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

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

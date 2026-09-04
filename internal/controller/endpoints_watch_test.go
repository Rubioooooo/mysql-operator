package controller

import (
	"context"
	"errors"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestMysqlPrimaryEndpointsMapping(t *testing.T) {
	ctx := context.Background()
	clusterA := credentialTestCluster("cluster-a", "db")
	clusterA.Spec.MasterService = "mysql-a-master"
	clusterB := credentialTestCluster("cluster-b", "db")
	clusterB.Spec.MasterService = "mysql-b-master"
	// Neither deprecated status nor the replica Service is matching authority.
	clusterB.Status.Master = clusterA.Spec.MasterService
	clusterB.Spec.SlaveService = clusterA.Spec.MasterService
	clusterOtherNamespace := clusterA.DeepCopy()
	clusterOtherNamespace.Namespace = "other-db"
	r := phase1HReconciler(t, clusterA, clusterB, clusterOtherNamespace)
	for _, tt := range []struct {
		name   string
		object client.Object
		want   []reconcile.Request
	}{
		{name: "matching primary Service", object: &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "mysql-a-master"}}, want: []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(clusterA)}}},
		{name: "namespace isolation", object: &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Namespace: "other-db", Name: "mysql-a-master"}}, want: []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(clusterOtherNamespace)}}},
		{name: "unrelated Endpoints", object: &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "unrelated"}}},
		{name: "namespace without clusters", object: &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Namespace: "empty-db", Name: "mysql-a-master"}}},
		{name: "wrong object kind", object: &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "mysql-a-master"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			before := tt.object.DeepCopyObject()
			requests := r.mapMysqlPrimaryEndpointsToMysqlClusters(ctx, tt.object)
			if len(tt.want) == 0 {
				g.Expect(requests).To(BeEmpty())
			} else {
				g.Expect(requests).To(Equal(tt.want))
			}
			g.Expect(tt.object).To(Equal(before))
		})
	}
	// Multiple explicit references are all scheduled, in deterministic order.
	clusterShared := clusterA.DeepCopy()
	clusterShared.Name = "cluster-shared"
	g := NewWithT(t)
	g.Expect(r.Create(ctx, clusterShared)).To(Succeed())
	endpoints := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "mysql-a-master", OwnerReferences: []metav1.OwnerReference{{Name: "unrelated-owner", UID: "unrelated-uid"}}},
		Subsets:    []corev1.EndpointSubset{{Addresses: []corev1.EndpointAddress{{TargetRef: &corev1.ObjectReference{Name: "foreign-pod", Namespace: "other-db"}}}}},
	}
	g.Expect(r.mapMysqlPrimaryEndpointsToMysqlClusters(ctx, endpoints)).To(Equal([]reconcile.Request{
		{NamespacedName: client.ObjectKeyFromObject(clusterA)},
		{NamespacedName: client.ObjectKeyFromObject(clusterShared)},
	}))
	g.Expect(r.Client.(*statefulSetReconcileMemoryClient).statusPatchCount).To(BeZero())
	g.Expect(r.Client.(*statefulSetReconcileMemoryClient).updateCount).To(BeZero())
}

type mysqlEndpointsMapListErrorClient struct {
	client.Client
}

func (c *mysqlEndpointsMapListErrorClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("list unavailable")
}

func TestMysqlPrimaryEndpointsMappingListFailure(t *testing.T) {
	r := &MysqlClusterReconciler{Client: &mysqlEndpointsMapListErrorClient{}}
	endpoints := &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "mysql-master"}}
	NewWithT(t).Expect(r.mapMysqlPrimaryEndpointsToMysqlClusters(context.Background(), endpoints)).To(BeEmpty())
}

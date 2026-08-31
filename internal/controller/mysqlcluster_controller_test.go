/*
Copyright 2024 egonlin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	appsv1 "github.com/egonlin/api/v1"
)

var _ = Describe("MysqlCluster envtest API boundary", func() {
	const (
		resourceName      = "test-resource"
		resourceNamespace = "default"
	)

	ctx := context.Background()

	namespacedName := types.NamespacedName{
		Name:      resourceName,
		Namespace: resourceNamespace,
	}

	AfterEach(func() {
		resource := &appsv1.MysqlCluster{}
		err := k8sClient.Get(ctx, namespacedName, resource)

		if errors.IsNotFound(err) {
			return
		}

		Expect(err).NotTo(HaveOccurred())

		By("cleaning up the MysqlCluster resource")
		Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
	})

	It("should create, read, and delete a structurally valid MysqlCluster", func() {
		replicas := int32(3)

		resource := &appsv1.MysqlCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: resourceNamespace,
			},
			Spec: appsv1.MysqlClusterSpec{
				Image:         "example.com/mysql:5.7",
				Replicas:      &replicas,
				MasterService: "test-master",
				SlaveService:  "test-replica",
				Storage: appsv1.StorageConfig{
					StorageClassName: "test-storage",
					Size:             mustQuantityForTest("1Gi"),
				},
				Resources: appsv1.ResourceRequirements{
					Requests: appsv1.ResourceRequests{
						CPU:    mustQuantityForTest("100m"),
						Memory: mustQuantityForTest("128Mi"),
					},
					Limits: appsv1.ResourceLimits{
						CPU:    mustQuantityForTest("500m"),
						Memory: mustQuantityForTest("512Mi"),
					},
				},
			},
		}

		By("creating the MysqlCluster in the isolated envtest API server")
		Expect(k8sClient.Create(ctx, resource)).To(Succeed())

		stored := &appsv1.MysqlCluster{}

		By("reading the MysqlCluster back from the isolated envtest API server")
		Expect(k8sClient.Get(ctx, namespacedName, stored)).To(Succeed())

		Expect(stored.Spec.Image).To(Equal("example.com/mysql:5.7"))
		Expect(stored.Spec.Replicas).NotTo(BeNil())
		Expect(*stored.Spec.Replicas).To(Equal(int32(3)))
		Expect(stored.Spec.MasterService).To(Equal("test-master"))
		Expect(stored.Spec.SlaveService).To(Equal("test-replica"))
		Expect(stored.Spec.Storage.StorageClassName).To(Equal("test-storage"))
		Expect(stored.Spec.Storage.Size.Cmp(mustQuantityForTest("1Gi"))).To(Equal(0))

		By("deleting the MysqlCluster")
		Expect(k8sClient.Delete(ctx, stored)).To(Succeed())

		Eventually(func() bool {
			current := &appsv1.MysqlCluster{}
			err := k8sClient.Get(ctx, namespacedName, current)
			return errors.IsNotFound(err)
		}).Should(BeTrue())
	})
})

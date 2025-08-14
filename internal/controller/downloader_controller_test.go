/*
Copyright 2025.

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

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rhdlv1alpha1 "gitlab.cee.redhat.com/rhdl/operator/api/v1alpha1"
)

var _ = Describe("Downloader Controller", func() {
	Context("When reconciling a resource", Ordered, func() {
		const resourceName = "test-resource"
		const secretName = "rhdl-credentials"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		downloader := &rhdlv1alpha1.Downloader{}

		BeforeAll(func() {
			By("creating the Secret with RHDL credentials")
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: typeNamespacedName.Namespace,
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{
					"RHDL_ACCESS_KEY": []byte("test-key"),
					"RHDL_SECRET_KEY": []byte("test-key"),
				},
			}
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		})

		BeforeEach(func() {
			By("creating the custom resource for the Kind Downloader")
			err := k8sClient.Get(ctx, typeNamespacedName, downloader)
			if err != nil && errors.IsNotFound(err) {
				resource := &rhdlv1alpha1.Downloader{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: typeNamespacedName.Namespace,
					},
					Spec: rhdlv1alpha1.DownloaderSpec{
						Topic:       "RHEL-9.2",
						Schedule:    "0 4 * * 1",
						Credentials: secretName,
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &rhdlv1alpha1.Downloader{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Downloader")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		AfterAll(func() {
			By("Cleanup the Secret with RHDL credentials")
			secret := &corev1.Secret{}
			secretNamespacedName := types.NamespacedName{
				Name:      secretName,
				Namespace: typeNamespacedName.Namespace,
			}
			err := k8sClient.Get(ctx, secretNamespacedName, secret)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("reconciling the created resource")
			controllerReconciler := &DownloaderReconciler{
				Client:   k8sClient,
				logger:   logr.Discard(), // Use a no-op logger for tests
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(100),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})

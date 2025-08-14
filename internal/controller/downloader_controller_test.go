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
	"k8s.io/apimachinery/pkg/api/resource"
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
		const defaultSecret = "credentials"
		const pvcName = "storage"

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

			By("creating the Secret named credentials")
			credentialsSecret := secret.DeepCopy()
			credentialsSecret.ObjectMeta.Name = defaultSecret

			// create both secrets after copy before objects are populated with
			// k8s metadata
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())
			Expect(k8sClient.Create(ctx, credentialsSecret)).To(Succeed())

			By("creating the PersistentVolumeClaim for storage")
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: typeNamespacedName.Namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
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

			By("Cleanup the Secret named credentials")
			credentialsSecret := &corev1.Secret{}
			credentialsNamespacedName := types.NamespacedName{
				Name:      defaultSecret,
				Namespace: typeNamespacedName.Namespace,
			}
			err = k8sClient.Get(ctx, credentialsNamespacedName, credentialsSecret)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Delete(ctx, credentialsSecret)).To(Succeed())

			By("Cleanup the PersistentVolumeClaim for storage")
			pvc := &corev1.PersistentVolumeClaim{}
			pvcNamespacedName := types.NamespacedName{
				Name:      pvcName,
				Namespace: typeNamespacedName.Namespace,
			}
			err = k8sClient.Get(ctx, pvcNamespacedName, pvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Delete(ctx, pvc)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("reconciling the created resource")
			controllerReconciler := NewDownloaderReconciler(
				k8sClient,
				logr.Discard(),
				k8sClient.Scheme(),
				record.NewFakeRecorder(100),
			)

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})

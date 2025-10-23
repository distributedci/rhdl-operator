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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rhdlv1alpha1 "github.com/distributedci/rhdl-operator/api/v1alpha1"
)

var _ = Describe("Downloader Controller", func() {
	Context("When reconciling a resource", Ordered, func() {
		const defaultSecret = "credentials"
		const defaultImage = "quay.io/rhdl/cli:latest"
		const defaultSchedule = "@daily"
		const resourceName = "test-resource"
		const secretName = "rhdl-credentials"
		const pvcName = "storage"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		createTestDownloader := func(credentialsName ...string) *rhdlv1alpha1.Downloader {
			spec := rhdlv1alpha1.DownloaderSpec{}
			if len(credentialsName) > 0 {
				spec.Credentials = credentialsName[0]
			}

			return &rhdlv1alpha1.Downloader{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: typeNamespacedName.Namespace,
				},
				Spec: spec,
			}
		}

		createControllerReconciler := func() *DownloaderReconciler {
			return NewDownloaderReconciler(
				k8sClient,
				logr.Discard(),
				k8sClient.Scheme(),
				record.NewFakeRecorder(100),
			)
		}

		// Helper function to perform two-phase reconciling for finalizer handling
		performFullReconcile := func(reconciler *DownloaderReconciler) error {
			// First reconcile call to add finalizer
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			if err != nil {
				return err
			}

			// Second reconcile call to do the actual work
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			return err
		}

		// Helper function to check if the last condition is Ready=True
		lastConditionIsReady := func(reconciler *DownloaderReconciler) bool {
			lastCondition, err := reconciler.LastCondition()
			if err != nil {
				return false
			}
			return lastCondition.Type == "Ready" && lastCondition.Status == metav1.ConditionTrue
		}

		// Helper function to check if the last condition is Ready=False
		lastConditionIsNotReady := func(reconciler *DownloaderReconciler) bool {
			lastCondition, err := reconciler.LastCondition()
			if err != nil {
				return false
			}
			return lastCondition.Type == "Ready" || lastCondition.Status == metav1.ConditionFalse
		}

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

			By("creating the default Secret named credentials")
			credentialsSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      defaultSecret,
					Namespace: typeNamespacedName.Namespace,
				},
				Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{
					"RHDL_ACCESS_KEY": []byte("test-key"),
					"RHDL_SECRET_KEY": []byte("test-key"),
				},
			}

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

		AfterEach(func() {
			// Cleanup logic - with finalizers, we need to manually trigger the deletion reconcile
			// since there's no controller manager running in unit tests
			resource := &rhdlv1alpha1.Downloader{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Downloader")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			// Manually trigger the deletion reconcile since no controller manager is running
			By("Processing finalizer cleanup")
			controllerReconciler := createControllerReconciler()
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify the Downloader is fully deleted
			Eventually(func() bool {
				err := k8sClient.Get(ctx, typeNamespacedName, resource)
				return errors.IsNotFound(err)
			}, "5s", "100ms").Should(BeTrue())
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

		It("should successfully reconcile the resource with default values", func() {
			By("creating the test Downloader resource with default values")
			testDownloader := createTestDownloader()
			Expect(k8sClient.Create(ctx, testDownloader)).To(Succeed())

			By("reconciling the created resource")
			controllerReconciler := createControllerReconciler()
			err := performFullReconcile(controllerReconciler)
			Expect(err).NotTo(HaveOccurred())

			By("checking the status of the resource")
			err = k8sClient.Get(ctx, typeNamespacedName, testDownloader)
			Expect(err).NotTo(HaveOccurred())
			Expect(testDownloader.Status.Conditions).ToNot(BeEmpty())
			// Check that the last condition indicates success
			Expect(lastConditionIsReady(controllerReconciler)).To(BeTrue())

			By("checking the CronJob resource")
			cronJob := &batchv1.CronJob{}
			cronJobNamespacedName := types.NamespacedName{
				Name:      testDownloader.Name,
				Namespace: testDownloader.Namespace,
			}
			err = k8sClient.Get(ctx, cronJobNamespacedName, cronJob)
			Expect(err).NotTo(HaveOccurred())

			By("checking the default values of the CronJob")
			Expect(cronJob.Spec.Schedule).To(Equal(defaultSchedule))
			Expect(cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image).To(Equal(defaultImage))
			Expect(cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name).To(Equal(defaultSecret))
			Expect(cronJob.Spec.JobTemplate.Spec.Template.Spec.Volumes).To(BeEmpty())
		})

		It("should fail when referencing a non-existent secret", func() {
			const nonExistentSecret = "non-existent"

			By("creating the test Downloader resource with non-existent credentials")
			testDownloader := createTestDownloader(nonExistentSecret)
			Expect(k8sClient.Create(ctx, testDownloader)).To(Succeed())

			By("reconciling the created resource")
			controllerReconciler := createControllerReconciler()
			err := performFullReconcile(controllerReconciler)
			Expect(err).To(HaveOccurred())

			By("checking the status of the resource shows CredentialsError")
			err = k8sClient.Get(ctx, typeNamespacedName, testDownloader)
			Expect(err).NotTo(HaveOccurred())
			Expect(testDownloader.Status.Conditions).ToNot(BeEmpty())
			// Check that the last condition indicates failure
			Expect(lastConditionIsNotReady(controllerReconciler)).To(BeTrue())
			// Verify the specific reason is CredentialsError
			lastCondition, _ := controllerReconciler.LastCondition()
			Expect(lastCondition.Reason).To(Equal("CredentialsError"))

			By("checking that no CronJob was created")
			cronJob := &batchv1.CronJob{}
			cronJobNamespacedName := types.NamespacedName{
				Name:      testDownloader.Name,
				Namespace: testDownloader.Namespace,
			}
			err = k8sClient.Get(ctx, cronJobNamespacedName, cronJob)
			Expect(err).To(HaveOccurred())
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})

		It("should not add duplicate conditions on repeated reconciles", func() {
			By("creating the test Downloader resource with default values")
			testDownloader := createTestDownloader()
			Expect(k8sClient.Create(ctx, testDownloader)).To(Succeed())

			By("performing first reconcile")
			controllerReconciler := createControllerReconciler()
			err := performFullReconcile(controllerReconciler)
			Expect(err).NotTo(HaveOccurred())

			By("checking initial status conditions")
			err = k8sClient.Get(ctx, typeNamespacedName, testDownloader)
			Expect(err).NotTo(HaveOccurred())
			initialCondCount := len(testDownloader.Status.Conditions)

			By("performing second reconcile (should not add any new conditions)")
			err = performFullReconcile(controllerReconciler)
			Expect(err).NotTo(HaveOccurred())

			By("verifying no new conditions were added")
			err = k8sClient.Get(ctx, typeNamespacedName, testDownloader)
			Expect(err).NotTo(HaveOccurred())
			finalCondCount := len(testDownloader.Status.Conditions)
			Expect(initialCondCount).To(Equal(finalCondCount))
		})

		It("should successfully reconcile a resource with non-default secret", func() {
			By("creating the test Downloader resource with non-default secret")
			testDownloader := createTestDownloader(secretName) // Uses "rhdl-credentials"
			Expect(k8sClient.Create(ctx, testDownloader)).To(Succeed())

			By("reconciling the created resource")
			controllerReconciler := createControllerReconciler()
			err := performFullReconcile(controllerReconciler)
			Expect(err).NotTo(HaveOccurred())

			By("checking the status of the resource")
			err = k8sClient.Get(ctx, typeNamespacedName, testDownloader)
			Expect(err).NotTo(HaveOccurred())
			Expect(testDownloader.Status.Conditions).ToNot(BeEmpty())
			// Check that the last condition indicates success
			Expect(lastConditionIsReady(controllerReconciler)).To(BeTrue())

			By("checking the CronJob resource")
			cronJob := &batchv1.CronJob{}
			cronJobNamespacedName := types.NamespacedName{
				Name:      testDownloader.Name,
				Namespace: testDownloader.Namespace,
			}
			err = k8sClient.Get(ctx, cronJobNamespacedName, cronJob)
			Expect(err).NotTo(HaveOccurred())

			By("checking that the CronJob uses the non-default secret")
			Expect(cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name).To(Equal(secretName))
		})
	})
})

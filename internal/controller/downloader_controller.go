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
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rhdlv1alpha1 "gitlab.cee.redhat.com/rhdl/operator/api/v1alpha1"
)

const API_URL = "https://api.rhdl.distributed-ci.io"

var log = logf.Log.WithName("rhdl-controller")

// DownloaderCredentials contains the validated credentials for RHDL access
type DownloaderCredentials struct {
	AccessKey string
	SecretKey string
	ApiURL    string
}

// String interface to redact sensitive information
func (dc DownloaderCredentials) String() string {
	return fmt.Sprintf("DownloaderCredentials{AccessKey: %s, SecretKey: [REDACTED], ApiURL: %s}",
		dc.AccessKey, dc.ApiURL)
}

// DownloaderReconciler reconciles a Downloader object
type DownloaderReconciler struct {
	client.Client
	logger     logr.Logger
	downloader *rhdlv1alpha1.Downloader
	Scheme     *runtime.Scheme
	Recorder   record.EventRecorder
}

// +kubebuilder:rbac:groups=rhdl.distributed-ci.io,resources=downloaders,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rhdl.distributed-ci.io,resources=downloaders/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=rhdl.distributed-ci.io,resources=downloaders/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Downloader object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *DownloaderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	r.logger.Info("Reconciling Downloader")
	r.logger = log.WithValues("Request.Namespace", req.Namespace, "Request.Name", req.Name)

	// Get the Downloader object
	downloader := &rhdlv1alpha1.Downloader{}
	if err := r.Get(ctx, req.NamespacedName, downloader); err != nil {
		if errors.IsNotFound(err) {
			r.logger.Info("Downloader not found, skipping reconciliation")
			return ctrl.Result{}, nil
		}
		r.logger.Error(err, "Failed to get Downloader")
		return ctrl.Result{}, err
	}
	r.logger.Info("Downloader object", "spec", downloader.Spec)
	r.downloader = downloader

	creds, err := r.readCredentials(ctx)
	if err != nil {
		r.logger.Error(err, "Failed to read credentials")
		return ctrl.Result{}, err
	}

	r.logger.Info("Credentials read", "creds", creds)

	// Create or update the CronJob
	if err := r.reconcileCronJob(ctx, downloader); err != nil {
		r.logger.Error(err, "Failed to reconcile CronJob")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// readCredentials validates that the referenced secret exists and contains required keys
func (r *DownloaderReconciler) readCredentials(ctx context.Context) (*DownloaderCredentials, error) {
	if r.downloader == nil {
		return nil, fmt.Errorf("downloader object is nil")
	}

	credentialsName := r.downloader.Spec.Credentials

	// Get the secret
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      credentialsName,
		Namespace: r.downloader.Namespace,
	}

	if err := r.Get(ctx, secretKey, secret); err != nil {
		return nil, fmt.Errorf("failed to get credentials secret %s: %w", credentialsName, err)
	}

	// Check for required keys
	requiredKeys := []string{"RHDL_ACCESS_KEY", "RHDL_SECRET_KEY"}
	for _, key := range requiredKeys {
		if _, exists := secret.Data[key]; !exists {
			return nil, fmt.Errorf("credentials secret %s is missing required key: %s", credentialsName, key)
		}
	}

	// Build credentials struct
	credentials := &DownloaderCredentials{
		AccessKey: string(secret.Data["RHDL_ACCESS_KEY"]),
		SecretKey: string(secret.Data["RHDL_SECRET_KEY"]),
		ApiURL:    API_URL,
	}

	// Override ApiURL if provided in the secret
	if apiURL, exists := secret.Data["RHDL_API_URL"]; exists {
		credentials.ApiURL = string(apiURL)
	}

	r.logger.Info("Credentials validation successful")

	return credentials, nil
}

// createCronJobSpec generates a CronJob specification based on the Downloader
func (r *DownloaderReconciler) createCronJobSpec() *batchv1.CronJob {
	// Use envFrom to source all environment variables from the credentials secret
	envFromSources := []corev1.EnvFromSource{
		{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: r.downloader.Spec.Credentials,
				},
			},
		},
	}

	// Build container args
	args := []string{
		"download", r.downloader.Spec.Topic,
		"--tag", r.downloader.Spec.Tag,
	}

	// Add extra args if specified
	if len(r.downloader.Spec.ExtraArgs) > 0 {
		args = append(args, r.downloader.Spec.ExtraArgs...)
	}

	// Prepare volume mounts and volumes
	var volumeMounts []corev1.VolumeMount
	var volumes []corev1.Volume

	// Add PVC mount if specified
	if r.downloader.Spec.PersistentVolumeClaim != "" {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "data-volume",
			MountPath: "/mnt",
		})
		volumes = append(volumes, corev1.Volume{
			Name: "data-volume",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: r.downloader.Spec.PersistentVolumeClaim,
				},
			},
		})
	}

	// Create the CronJob spec
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      strings.ToLower(r.downloader.Name),
			Namespace: r.downloader.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "rhdl-downloader",
				"app.kubernetes.io/instance":   r.downloader.Name,
				"app.kubernetes.io/component":  "downloader",
				"app.kubernetes.io/managed-by": "rhdl-operator",
				"rhdl.distributed-ci.io/topic": r.downloader.Spec.Topic,
				"rhdl.distributed-ci.io/tag":   r.downloader.Spec.Tag,
			},
		},
		Spec: batchv1.CronJobSpec{
			Schedule: r.downloader.Spec.Schedule,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Containers: []corev1.Container{
								{
									Name:            "downloader",
									Image:           r.downloader.Spec.ContainerImage,
									Args:            args,
									EnvFrom:         envFromSources,
									VolumeMounts:    volumeMounts,
									ImagePullPolicy: corev1.PullPolicy(r.downloader.Spec.PullPolicy),
									WorkingDir:      "/mnt",
								},
							},
							Volumes: volumes,
						},
					},
				},
			},
		},
	}

	return cronJob
}

// reconcileCronJob creates or updates the CronJob for the Downloader
func (r *DownloaderReconciler) reconcileCronJob(ctx context.Context, downloader *rhdlv1alpha1.Downloader) error {
	// Generate the desired CronJob spec
	desiredCronJob := r.createCronJobSpec()

	// Set the owner reference so the CronJob is cleaned up when the Downloader is deleted
	if err := controllerutil.SetControllerReference(downloader, desiredCronJob, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	// Check if the CronJob already exists
	existingCronJob := &batchv1.CronJob{}
	cronJobKey := types.NamespacedName{
		Name:      desiredCronJob.Name,
		Namespace: desiredCronJob.Namespace,
	}

	err := r.Get(ctx, cronJobKey, existingCronJob)
	if err != nil {
		if errors.IsNotFound(err) {
			// CronJob doesn't exist, create it
			r.logger.Info("Creating new CronJob", "cronJob", desiredCronJob.Name)
			if err := r.Create(ctx, desiredCronJob); err != nil {
				return fmt.Errorf("failed to create CronJob: %w", err)
			}
			r.Recorder.Event(downloader, corev1.EventTypeNormal, "CronJobCreated",
				fmt.Sprintf("Created CronJob %s", desiredCronJob.Name))
			return nil
		}
		return fmt.Errorf("failed to get CronJob: %w", err)
	}

	// CronJob exists, check if it needs to be updated
	if r.cronJobNeedsUpdate(existingCronJob, desiredCronJob) {
		r.logger.Info("Updating existing CronJob", "cronJob", existingCronJob.Name)

		// Update the existing CronJob with the desired spec
		existingCronJob.Spec = desiredCronJob.Spec

		if err := r.Update(ctx, existingCronJob); err != nil {
			return fmt.Errorf("failed to update CronJob: %w", err)
		}

		r.Recorder.Event(downloader, corev1.EventTypeNormal, "CronJobUpdated",
			fmt.Sprintf("Updated CronJob %s", existingCronJob.Name))
	}

	return nil
}

// cronJobNeedsUpdate compares the existing and desired CronJob to determine if an update is needed
func (r *DownloaderReconciler) cronJobNeedsUpdate(existing, desired *batchv1.CronJob) bool {
	// Check if Topic changed
	if existing.Labels["rhdl.distributed-ci.io/topic"] != desired.Labels["rhdl.distributed-ci.io/topic"] {
		return true
	}

	// Check if Tag changed
	if existing.Labels["rhdl.distributed-ci.io/tag"] != desired.Labels["rhdl.distributed-ci.io/tag"] {
		return true
	}

	// Check if Schedule changed
	if existing.Spec.Schedule != desired.Spec.Schedule {
		return true
	}

	// Check if ContainerImage changed
	existingImage := existing.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image
	desiredImage := desired.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image
	if existingImage != desiredImage {
		return true
	}

	// Check if PullPolicy changed
	existingPullPolicy := existing.Spec.JobTemplate.Spec.Template.Spec.Containers[0].ImagePullPolicy
	desiredPullPolicy := desired.Spec.JobTemplate.Spec.Template.Spec.Containers[0].ImagePullPolicy
	if existingPullPolicy != desiredPullPolicy {
		return true
	}

	// Check if Credentials reference changed
	existingCredentials := existing.Spec.JobTemplate.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name
	desiredCredentials := desired.Spec.JobTemplate.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name
	if existingCredentials != desiredCredentials {
		return true
	}

	// Check if ExtraArgs changed
	var existingArgs = make([]string, len(existing.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args))
	var desiredArgs = make([]string, len(desired.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args))

	// Trim whitespace from existing args and sort
	for i, arg := range existing.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args {
		existingArgs[i] = strings.TrimSpace(arg)
	}
	sort.Strings(existingArgs)

	// Trim whitespace from desired args and sort
	for i, arg := range desired.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args {
		desiredArgs[i] = strings.TrimSpace(arg)
	}
	sort.Strings(desiredArgs)

	if !reflect.DeepEqual(existingArgs, desiredArgs) {
		return true
	}

	// Check if PersistentVolumeClaim changed
	existingPVC := existing.Spec.JobTemplate.Spec.Template.Spec.Volumes
	desiredPVC := desired.Spec.JobTemplate.Spec.Template.Spec.Volumes
	return !reflect.DeepEqual(existingPVC, desiredPVC)
}

// secretToDownloaderRequests maps a Secret to Downloader reconcile requests
func (r *DownloaderReconciler) secretToDownloaderRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return nil
	}

	// List all Downloader resources in the same namespace
	var downloaders rhdlv1alpha1.DownloaderList
	if err := r.List(ctx, &downloaders, client.InNamespace(secret.Namespace)); err != nil {
		r.logger.Error(err, "Failed to list Downloader resources for Secret mapping", "secret", secret.Name, "namespace", secret.Namespace)
		return nil
	}

	var requests []reconcile.Request
	for _, downloader := range downloaders.Items {
		// Check if this downloader references this secret
		if downloader.Spec.Credentials == secret.Name {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      downloader.Name,
					Namespace: downloader.Namespace,
				},
			})
		}
	}

	r.logger.Info("Secret change mapped to Downloader requests", "secret", secret.Name, "namespace", secret.Namespace, "requests", len(requests))

	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *DownloaderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rhdlv1alpha1.Downloader{}).
		Owns(&batchv1.CronJob{}).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.secretToDownloaderRequests),
		).
		Complete(r)
}

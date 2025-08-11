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

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	rhdlv1alpha1 "gitlab.cee.redhat.com/rhdl/operator/api/v1alpha1"
)

var log = logf.Log.WithName("downloader-controller")

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

	return ctrl.Result{}, nil
}

// readCredentials validates that the referenced secret exists and contains required keys
func (r *DownloaderReconciler) readCredentials(ctx context.Context) (*DownloaderCredentials, error) {
	if r.downloader == nil {
		return nil, fmt.Errorf("downloader object is nil")
	}

	credentialsName := r.downloader.Spec.Credentials
	if credentialsName == "" {
		credentialsName = "credentials" // default value
	}

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
		ApiURL:    "https://api.rhdl.distributed-ci.io", // default value
	}

	// Override ApiURL if provided in the secret
	if apiURL, exists := secret.Data["RHDL_API_URL"]; exists {
		credentials.ApiURL = string(apiURL)
	}

	r.logger.Info("Credentials validation successful")

	return credentials, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DownloaderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&rhdlv1alpha1.Downloader{}).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}

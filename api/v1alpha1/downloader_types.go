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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DownloaderSpec defines the desired state of Downloader.
type DownloaderSpec struct {
	// Topic is the name of the topic to download from.
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	Topic string `json:"topic"`
	// PersistentVolumeClaim is the reference to the persistent volume claim to use for the downloader container. If defined, will always mount to /mnt.
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	PersistentVolumeClaim string `json:"persistentVolumeClaim"`
	// Tag is the tag of the topic to download from (default: milestone).
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	// +kubebuilder:default="milestone"
	Tag string `json:"tag,omitempty"`
	// Schedule is how often to download the topic in cron format (default: @daily).
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	// +kubebuilder:default="@daily"
	// +kubebuilder:validation:Pattern=`^(@(annually|yearly|monthly|weekly|daily|hourly))|([0-9*,/-]+\s+[0-9*,/-]+\s+[0-9*,/-]+\s+[0-9*,/-]+\s+[0-9*,/-]+)$`
	Schedule string `json:"schedule,omitempty"`
	// Credentials is the reference to the secret containing the authentication
	// variables e.g. RHDL_ACCESS_KEY, RHDL_SECRET_KEY, RHDL_API_URL (default:
	// credentials).
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	// +kubebuilder:default="credentials"
	Credentials string `json:"credentials,omitempty"`
	// PullPolicy is the policy to use the downloader container (default: Always).
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	// +kubebuilder:default="Always"
	PullPolicy string `json:"pullPolicy,omitempty"`
	// ExtraArgs is a list of extra arguments to pass to the downloader container (default: []).
	// +operator-sdk:csv:customresourcedefinitions:type=spec
	ExtraArgs []string `json:"extraArgs,omitempty"`
}

// DownloaderStatus defines the observed state of Downloader.
type DownloaderStatus struct {
	// Conditions store the status conditions of the Downloader jobs.
	// +operator-sdk:csv:customresourcedefinitions:type=status
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// Downloader is the Schema for the downloaders API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type Downloader struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DownloaderSpec   `json:"spec,omitempty"`
	Status DownloaderStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DownloaderList contains a list of Downloader.
type DownloaderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Downloader `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Downloader{}, &DownloaderList{})
}

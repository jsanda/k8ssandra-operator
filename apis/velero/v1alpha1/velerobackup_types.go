/*
Copyright 2022.

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// VeleroBackupSpec defines the desired state of VeleroBackup
type VeleroBackupSpec struct {
	// +kubebuilder:validation:Required
	// K8ssandraCluster is the cluster to backup. It may span multiple namespaces within
	// the same Kubernetes cluster and/or span multiple Kubernetes clusters.
	K8ssandraCluster corev1.LocalObjectReference `json:"k8ssandraCluster"`

	// Datacenters specifies the CassandraDatacenters to backup. If empty all
	// CassandraDatacenters are backed up.
	// TODO Add a validation check to make sure that all DCs listed are declared in K8ssandraCluster.
	Datacenters []string `json:"datacenters,omitempty"`
}

// VeleroBackupStatus defines the observed state of VeleroBackup
type VeleroBackupStatus struct {
	// StartTime is set when the controller first starts reconciling the VeleroBackup.
	StartTime metav1.Time `json:"startTime,omitempty"`

	// FinishTime is set after all DC backups have completed, regardless of whether they
	// succeeded or failed.
	FinishTime metav1.Time `json:"finishTime,omitempty"`

	//
	Datacenters map[string]DatacenterBackupStatus `json:"datacenters"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// VeleroBackup is the Schema for the cassandrabackups API. A VeleroBackup is a control
// plane object. It should be created in the same cluster and in the same namespace as the
// K8ssandraCluster being backed up. This means that a backup operation can span multiple
// Kubernetes clusters.
type VeleroBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VeleroBackupSpec   `json:"spec,omitempty"`
	Status VeleroBackupStatus `json:"status,omitempty"`
}

type BackupPhase string

const (
	BackupPhaseStarting     BackupPhase = "Starting"
	BackupPhaseInProgress   BackupPhase = "InProgress"
	BackupPhaseCreateFailed BackupPhase = "CreateFailed"
	BackupPhaseCompleted    BackupPhase = "Completed"
	BackupPhaseFailed       BackupPhase = "Failed"
	BackupPhaseDeleted      BackupPhase = "Deleted"
)

type DatacenterBackupStatus struct {
	Phase BackupPhase `json:"phase"`
}

//+kubebuilder:object:root=true

// VeleroBackupList contains a list of VeleroBackup
type VeleroBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VeleroBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VeleroBackup{}, &VeleroBackupList{})
}

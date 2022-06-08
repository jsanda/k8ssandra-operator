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

// VeleroRestoreSpec defines the desired state of VeleroRestore
type VeleroRestoreSpec struct {
	// K8ssandraCluster is the cluster to restore. It may span multiple namespaces within
	// the same Kubernetes cluster and/or span multiple Kubernetes clusters. Currently only
	// in-place restores are supported.
	K8ssandraCluster corev1.LocalObjectReference `json:"k8ssandraCluster"`

	// Datacenters specifies the CassandraDatacenters to restore. If empty all
	// CassandraDatacenters are restored.
	// TODO Add a validation check to make sure that all DCs listed are declared in K8ssandraCluster.
	Datacenters []string `json:"datacenters,omitempty"`

	// Backup is the VeleroBackup from which to restore. It should live in the same
	// namespace as the K8ssandraCluster and the VeleroRestore.
	Backup string `json:"backup"`
}

// VeleroRestoreStatus defines the observed state of VeleroRestore
type VeleroRestoreStatus struct {
	// StartTime is set when the controller first starts reconciling the VeleroRestore
	StartTime metav1.Time `json:"startTime,omitempty"`

	// FinishTime is set after all DC restores have successfully completed and the DCs have
	// be been restarted. In the case of a restore failure, this property will be set
	// after the DC restores are finished without any attempt at restarting the
	// CassandraDatacenters.
	FinishTime metav1.Time `json:"finishTime,omitempty"`

	// Datacenters is a map of status per DC.
	Datacenters map[string]DatacenterRestoreStatus `json:"datacenters,omitempty"`
}

// RestorePhase is the DC-level restore phase.
type RestorePhase string

const (
	// RestorePhaseNotStarted means that no reconciliation actions have been performed yet.
	RestorePhaseNotStarted RestorePhase = "NotStarted"
	// RestorePhaseDatacenterStarting means that the CassandraDatacenter is starting back up.
	RestorePhaseDatacenterStarting RestorePhase = "Starting"
	// RestorePhaseDatacenterStopping means that stopping the CassandraDatacenter is underway.
	RestorePhaseDatacenterStopping RestorePhase = "Stopping"
	// RestorePhaseDatacenterStopped means that the CassandraDatacenter has been stopped.
	RestorePhaseDatacenterStopped RestorePhase = "Stopped"
	// ResstorePhaseVolumesDeleted means that all PVCs for the DC have deleted.
	ResstorePhaseVolumesDeleted RestorePhase = "VolumesDeleted"
	// RestorePhaseInProgress means that the restore operation has started.
	RestorePhaseInProgress RestorePhase = "InProgress"
	// RestorePhaseCompleted means that the restore operation completed successfully.
	RestorePhaseCompleted RestorePhase = "Completed"
	// RestorePhaseFailed means that the restore operation failed.
	RestorePhaseFailed RestorePhase = "Failed"
)

type DatacenterRestoreStatus struct {
	Phase RestorePhase `json:"phase"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// VeleroRestore is the Schema for the velerorestores API. A VeleroRestore is a control
// plane object. It should be created in the same cluster and in the same namespace as
// the K8ssandraCluster being restored. This means that a restore operation can span
// multiple Kubernetes clusters.
type VeleroRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VeleroRestoreSpec   `json:"spec,omitempty"`
	Status VeleroRestoreStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// VeleroRestoreList contains a list of VeleroRestore
type VeleroRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VeleroRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VeleroRestore{}, &VeleroRestoreList{})
}

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

package velero

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/labels"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/velero/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/result"
	"github.com/k8ssandra/k8ssandra-operator/pkg/utils"
	velerovapi "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
)

// VeleroBackupReconciler reconciles a VeleroBackup object
type VeleroBackupReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	ClientCache *clientcache.ClientCache
}

//+kubebuilder:rbac:groups=velero.k8ssandra.io,namespace=k8ssandra-operator,resources=velerobackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=velero.k8ssandra.io,namespace=k8ssandra-operator,resources=velerobackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=velero.k8ssandra.io,namespace=k8ssandra-operator,resources=velerobackups/finalizers,verbs=update
// +kubebuilder:rbac:groups=cassandra.datastax.com,namespace="k8ssandra-operator",resources=cassandradatacenters,verbs=get;list;watch
// +kubebuilder:rbac:groups=velero.io,namespace="k8ssandra-operator",resources=backups,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=core,namespace="k8ssandra-operator",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the VeleroBackup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.11.0/pkg/reconcile
func (r *VeleroBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("VeleroBackup", req.NamespacedName)

	backupKey := req.NamespacedName
	backup := &api.VeleroBackup{}
	if err := r.Get(ctx, backupKey, backup); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !backup.Status.FinishTime.IsZero() {
		return ctrl.Result{}, nil
	}

	backup = backup.DeepCopy()

	defer r.updateStatus(ctx, backup, logger)

	if backup.Status.StartTime.IsZero() {
		backup.Status.StartTime = metav1.Now()
	}

	if !backup.Status.FinishTime.IsZero() {
		return ctrl.Result{}, nil
	}

	if backup.Status.Datacenters == nil {
		backup.Status.Datacenters = make(map[string]api.DatacenterBackupStatus)
	}

	kc := &k8ssandraapi.K8ssandraCluster{}
	kcKey := client.ObjectKey{Namespace: backupKey.Namespace, Name: backup.Spec.K8ssandraCluster.Name}
	if err := r.Get(ctx, kcKey, kc); err != nil {
		return ctrl.Result{}, err
	}
	kc = kc.DeepCopy()

	logger = logger.WithValues("K8ssandraCluster", kcKey)

	backupState, err := r.getBackupState(ctx, backup, kc, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to load backup state: %v", err)
	}

	if recResult := r.checkPersistentVolumes(ctx, backup, kc, backupState, logger); recResult.Completed() {
		return recResult.Output()
	}

	return r.checkBackups(ctx, backup, kc, backupState, logger).Output()
}

func (r *VeleroBackupReconciler) updateStatus(ctx context.Context, kBackup *api.VeleroBackup, logger logr.Logger) {
	if err := r.Status().Update(ctx, kBackup); err != nil {
		logger.Error(err, "Failed to update status")
	}
}

func (r *VeleroBackupReconciler) getBackupState(
	ctx context.Context,
	backup *api.VeleroBackup,
	kc *k8ssandraapi.K8ssandraCluster,
	logger logr.Logger) (map[string]datacenterBackupState, error) {

	logger.Info("Initializing backup state")

	backupState := make(map[string]datacenterBackupState)
	datacenters := make([]string, 0)

	if len(backup.Spec.Datacenters) == 0 {
		for _, dcTemplate := range kc.Spec.Cassandra.Datacenters {
			datacenters = append(datacenters, dcTemplate.Meta.Name)
		}
	} else {
		datacenters = backup.Spec.Datacenters
	}

	for _, dcName := range datacenters {
		i := findDatacenterTemplate(dcName, kc)
		dcTemplate := kc.Spec.Cassandra.Datacenters[i]
		dcKey := client.ObjectKey{Namespace: kc.Namespace, Name: dcTemplate.Meta.Name}
		if dcTemplate.Meta.Namespace != "" {
			dcKey.Namespace = dcTemplate.Meta.Namespace
		}

		remoteClient, err := r.ClientCache.GetRemoteClient(dcTemplate.K8sContext)
		if err != nil {
			return nil, fmt.Errorf("failed to get remote client: K8ssandraCluster (%s), DC (%s), K8sContext (%s): %v",
				utils.GetKey(kc), dcName, dcTemplate.K8sContext, err)
		}

		dc := &cassdcapi.CassandraDatacenter{}
		if err := remoteClient.Get(ctx, dcKey, dc); err != nil {
			return nil, fmt.Errorf("failed to get CassandraDatacenter: K8ssandraCluster (%s), DC (%s), K8sContext (%s): %v",
				utils.GetKey(kc), dcName, dcTemplate.K8sContext, err)
		}

		vBackup := &velerovapi.Backup{}
		vBackupKey := client.ObjectKey{Namespace: dcKey.Namespace, Name: backup.Name}

		if err := remoteClient.Get(ctx, vBackupKey, vBackup); err != nil {
			if errors.IsNotFound(err) {
				vBackup = nil
			} else {
				return nil, fmt.Errorf("failed to get Velero Backup: K8ssandraCluster (%s), DC (%s), K8sContext (%s): %v",
					utils.GetKey(kc), dcName, dcTemplate.K8sContext, err)
			}
		}

		backupState[dcName] = datacenterBackupState{
			idx:          i,
			dcKey:        dcKey,
			dc:           dc,
			remoteClient: remoteClient,
			vBackup:      vBackup,
		}
	}

	return backupState, nil
}

type datacenterBackupState struct {
	idx          int
	dcKey        client.ObjectKey
	dc           *cassdcapi.CassandraDatacenter
	remoteClient client.Client
	vBackup      *velerovapi.Backup
}

// checkPersistentVolumes fetches all PersistentVolumes belonging to Cassandra pods in the
// K8ssandraCluster and ensures that they have the datacenter labels as defined by
// the CassandraDatacenter.GetDatacenterLabels method.
func (r *VeleroBackupReconciler) checkPersistentVolumes(
	ctx context.Context,
	backup *api.VeleroBackup,
	kc *k8ssandraapi.K8ssandraCluster,
	backupState map[string]datacenterBackupState,
	logger logr.Logger) result.ReconcileResult {

	logger.Info("Checking PersistentVolumes")

	for _, dcState := range backupState {
		dcKey := dcState.dcKey
		dc := dcState.dc
		remoteClient := dcState.remoteClient

		pvcList := &corev1.PersistentVolumeClaimList{}
		if err := r.List(ctx, pvcList, client.InNamespace(dcKey.Namespace), client.MatchingLabels(dc.GetDatacenterLabels())); err != nil {
			return result.Error(err)
		}

		for _, pvc := range pvcList.Items {
			pvKey := client.ObjectKey{Namespace: pvc.Namespace, Name: pvc.Spec.VolumeName}
			pv := &corev1.PersistentVolume{}
			if err := remoteClient.Get(ctx, pvKey, pv); err != nil {
				return result.Error(err)
			}
			pv = pv.DeepCopy()

			if labels.HasLabelsWithValues(pv, dc.GetDatacenterLabels()) {
				continue
			}

			logger.Info("Updating labels", "DC", dcKey.Name)

			patch := client.MergeFromWithOptions(pv.DeepCopy())
			for k, v := range dc.GetDatacenterLabels() {
				labels.AddLabel(pv, k, v)
			}
			if err := remoteClient.Patch(ctx, pv, patch); err != nil {
				return result.Error(err)
			}
		}
	}

	return result.Continue()
}

// checkBackups checks each CassandraDatacenter and creates a Velero Backup object if one
// doesn't already exist. This method requeues the reconciliation request until the Backup
// for each CassandraDatacenter has finished. Note that finished can mean either success or
// failure. Note that this method assumes all PersistentVolumes already have datacenter
// labels.
func (r *VeleroBackupReconciler) checkBackups(
	ctx context.Context,
	backup *api.VeleroBackup,
	kc *k8ssandraapi.K8ssandraCluster,
	backupState map[string]datacenterBackupState,
	logger logr.Logger) result.ReconcileResult {

	for _, dcState := range backupState {
		dcKey := dcState.dcKey
		dc := dcState.dc
		remoteClient := dcState.remoteClient
		dcStatus := backup.Status.Datacenters[dcKey.Name]

		if dcState.vBackup == nil {
			dcState.vBackup = &velerovapi.Backup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: dcKey.Namespace,
					Name:      backup.Name,
				},
				Spec: velerovapi.BackupSpec{
					IncludedNamespaces: []string{dcKey.Namespace},
					LabelSelector:      metav1.SetAsLabelSelector(dc.GetDatacenterLabels()),
					IncludedResources: []string{
						"PersistentVolumeClaims",
						"PersistentVolumes",
					},
				},
			}
			labels.SetManagedBy(dcState.vBackup, utils.GetKey(kc))
			vBackupKey := utils.GetKey(dcState.vBackup)

			if err := remoteClient.Create(ctx, dcState.vBackup); err == nil {
				logger.Info("Created Velero backup", "Backup", vBackupKey)
				dcStatus.Phase = api.BackupPhaseInProgress
			} else {
				if errors.IsAlreadyExists(err) {
					dcStatus.Phase = api.BackupPhaseInProgress
				}
				dcStatus.Phase = api.BackupPhaseCreateFailed
				return result.Error(fmt.Errorf("failed to create Velero Backup (%s) for CassandraDatacenter (%s) in K8sssandraCluster (%s)",
					vBackupKey, dcKey, utils.GetKey(kc)))
			}
		}

		if dcStatus.Phase == api.BackupPhaseInProgress {
			if dcState.vBackup.Status.CompletionTimestamp != nil && !dcState.vBackup.Status.CompletionTimestamp.IsZero() {
				if dcState.vBackup.Status.Phase == velerovapi.BackupPhaseCompleted {
					dcStatus.Phase = api.BackupPhaseCompleted
				} else {
					dcStatus.Phase = api.BackupPhaseFailed
				}
			}
		}

		backup.Status.Datacenters[dcKey.Name] = dcStatus
	}

	if backupsFinished(backup) {
		logger.Info("All Velero backups have finished")
		backup.Status.FinishTime = metav1.Now()
		return result.Done()
	}

	return result.RequeueSoon(10 * time.Second)
}

func backupsFinished(backup *api.VeleroBackup) bool {
	for _, dcStatus := range backup.Status.Datacenters {
		if !backupFinished(dcStatus) {
			return false
		}
	}
	return true
}

func backupFinished(dcStatus api.DatacenterBackupStatus) bool {
	switch dcStatus.Phase {
	case api.BackupPhaseCompleted, api.BackupPhaseFailed, api.BackupPhaseDeleted, api.BackupPhaseCreateFailed:
		return true
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *VeleroBackupReconciler) SetupWithManager(mgr ctrl.Manager, clusters []cluster.Cluster) error {
	cb := ctrl.NewControllerManagedBy(mgr).
		For(&api.VeleroBackup{}, builder.WithPredicates(predicate.GenerationChangedPredicate{}))

	// TODO Move func to shared package. It is needed for any multi-cluster controller.
	clusterLabelFilter := func(mapObj client.Object) []reconcile.Request {
		requests := make([]reconcile.Request, 0)

		kcName := labels.GetLabel(mapObj, k8ssandraapi.K8ssandraClusterNameLabel)
		kcNamespace := labels.GetLabel(mapObj, k8ssandraapi.K8ssandraClusterNamespaceLabel)

		if kcName != "" && kcNamespace != "" {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: kcNamespace, Name: kcName}})
		}
		return requests
	}

	for _, c := range clusters {
		cb = cb.Watches(source.NewKindWithCache(&velerovapi.Backup{}, c.GetCache()),
			handler.EnqueueRequestsFromMapFunc(clusterLabelFilter))
	}

	return cb.Complete(r)
}

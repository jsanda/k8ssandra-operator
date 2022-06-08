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
	k8ssandraapi "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/clientcache"
	"github.com/k8ssandra/k8ssandra-operator/pkg/labels"
	"github.com/k8ssandra/k8ssandra-operator/pkg/result"
	"github.com/k8ssandra/k8ssandra-operator/pkg/utils"
	velerovapi "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
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
	api "github.com/k8ssandra/k8ssandra-operator/apis/velero/v1alpha1"
)

// VeleroRestoreReconciler reconciles a VeleroRestore object
type VeleroRestoreReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	ClientCache *clientcache.ClientCache
}

//+kubebuilder:rbac:groups=velero.k8ssandra.io,namespace=k8ssandra-operator,resources=velerorestores,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=velero.k8ssandra.io,namespace=k8ssandra-operator,resources=velerorestores/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=velero.k8ssandra.io,namespace=k8ssandra-operator,resources=velerorestores/finalizers,verbs=update
// +kubebuilder:rbac:groups=velero.io,namespace="k8ssandra-operator",resources=restores,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=cassandra.datastax.com,namespace="k8ssandra-operator",resources=cassandradatacenters,verbs=get;list;watch
// +kubebuilder:rbac:groups=k8ssandra.io,namespace="k8ssandra-operator",resources=k8ssandraclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,namespace="k8ssandra-operator",resources=persistentvolumeclaims,verbs=get;list;watch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the VeleroRestore object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.11.0/pkg/reconcile
func (r *VeleroRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("VeleroRestore", req.NamespacedName)

	restoreKey := req.NamespacedName
	restore := &api.VeleroRestore{}
	if err := r.Get(ctx, restoreKey, restore); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	restore = restore.DeepCopy()

	defer r.applyStatusUpdates(ctx, restore, logger)

	if restore.Status.StartTime.IsZero() {
		restore.Status.StartTime = metav1.Now()
	}

	if !restore.Status.FinishTime.IsZero() {
		return ctrl.Result{}, nil
	}

	kc := &k8ssandraapi.K8ssandraCluster{}
	kcKey := client.ObjectKey{Namespace: restoreKey.Namespace, Name: restore.Spec.K8ssandraCluster.Name}
	if err := r.Get(ctx, kcKey, kc); err != nil {
		return ctrl.Result{}, err
	}
	kc = kc.DeepCopy()

	logger = logger.WithValues("K8ssandraCluster", kcKey)

	restoreState, err := r.getRestoreState(ctx, restore, kc, logger)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to load restore state: %v", err)
	}

	initRestoreStatus(restore)

	if recResult := r.checkDatacentersStopped(ctx, restore, kc, restoreState, logger); recResult.Completed() {
		return recResult.Output()
	}

	if recResult := r.checkPVCsDeleted(ctx, restore, kc, restoreState, logger); recResult.Completed() {
		return recResult.Output()
	}

	if recResult := r.checkRestores(ctx, restore, kc, restoreState, logger); recResult.Completed() {
		return recResult.Output()
	}

	// If we get to this point then the restore operations have finished. We need to check
	// for failures before restarting the DCs. If there are failures, we end reconciliation
	// without attempting to restart DCs.
	if restoreHasFailures(restore) {
		restore.Status.FinishTime = metav1.Now()
		logger.Info("Finished reconciliation with restore failures.")
		return ctrl.Result{}, nil
	}

	if recResult := r.checkDatacentersStarted(ctx, restore, kc, restoreState, logger); recResult.Completed() {
		return recResult.Output()
	}

	restore.Status.FinishTime = metav1.Now()
	logger.Info("Finished reconciliation")
	return ctrl.Result{}, nil
}

func (r *VeleroRestoreReconciler) getRestoreState(
	ctx context.Context,
	restore *api.VeleroRestore,
	kc *k8ssandraapi.K8ssandraCluster,
	logger logr.Logger) (map[string]datacenterRestoreState, error) {

	restoreState := make(map[string]datacenterRestoreState)
	datacenters := make([]string, 0)

	if len(restore.Spec.Datacenters) == 0 {
		for _, dcTemplate := range kc.Spec.Cassandra.Datacenters {
			datacenters = append(datacenters, dcTemplate.Meta.Name)
		}
	} else {
		datacenters = restore.Spec.Datacenters
	}

	logger.Info("Initializing restore state")
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

		vRestore := &velerovapi.Restore{}
		vRestoreKey := client.ObjectKey{Namespace: dcKey.Namespace, Name: restore.Name}

		if err = remoteClient.Get(ctx, vRestoreKey, vRestore); err != nil {
			if errors.IsNotFound(err) {
				vRestore = nil
			} else {
				return nil, fmt.Errorf("failed to get Velero Restore: K8ssandraCluster (%s), DC (%s), K8sContext (%s): %v",
					utils.GetKey(kc), dcName, dcTemplate.K8sContext, err)
			}
		}

		restoreState[dcName] = datacenterRestoreState{
			idx:          i,
			dcKey:        dcKey,
			remoteClient: remoteClient,
			vRestore:     vRestore,
		}
	}

	return restoreState, nil
}

func initRestoreStatus(restore *api.VeleroRestore) {
	if len(restore.Status.Datacenters) == 0 {
		restore.Status.Datacenters = make(map[string]api.DatacenterRestoreStatus)
	}
	for _, dcName := range restore.Spec.Datacenters {
		if _, found := restore.Status.Datacenters[dcName]; !found {
			restore.Status.Datacenters[dcName] = api.DatacenterRestoreStatus{
				Phase: api.RestorePhaseNotStarted,
			}
		}
	}
}

type datacenterRestoreState struct {
	idx          int
	dcKey        client.ObjectKey
	remoteClient client.Client
	vRestore     *velerovapi.Restore
}

func (r *VeleroRestoreReconciler) applyStatusUpdates(ctx context.Context, restore *api.VeleroRestore, logger logr.Logger) {
	if err := r.Status().Update(ctx, restore); err != nil {
		logger.Error(err, "Failed to update status")
	}
}

// checkDatacentersStopped Makes sure that all the CassandraDatacenters to be restored
// are stopped if necessary. The CassandraDatacenter will be stopped only if no Restore
// object already exists. This method will cause reconciliation to requeue until all of the
// DCs DatacenterStopped conditions are true.
func (r *VeleroRestoreReconciler) checkDatacentersStopped(
	ctx context.Context,
	restore *api.VeleroRestore,
	kc *k8ssandraapi.K8ssandraCluster,
	restoreState map[string]datacenterRestoreState,
	logger logr.Logger) result.ReconcileResult {

	logger.Info("Check DCs stopped")

	stopping := false
	updated := false
	patch := client.MergeFromWithOptions(kc.DeepCopy(), client.MergeFromWithOptimisticLock{})

	for dcName, dcState := range restoreState {
		logger.Info("Checking restore state", "DC", dcName)
		dcRestoreStatus := restore.Status.Datacenters[dcName]
		if dcState.vRestore == nil {
			if kc.Spec.Cassandra.Datacenters[dcState.idx].Stopped {
				if kc.Status.Datacenters[dcName].Cassandra.GetConditionStatus(cassdcapi.DatacenterStopped) == corev1.ConditionTrue {
					logger.Info("DC is stopped", "DC", dcName)
					dcRestoreStatus.Phase = api.RestorePhaseDatacenterStopped
				} else {
					logger.Info("DC is stopping", "DC", dcName)
					dcRestoreStatus.Phase = api.RestorePhaseDatacenterStopping
					stopping = true
				}
			} else {
				logger.Info("DC needs to be stopped", "DC", dcName)
				updated = true
				stopping = true
				kc.Spec.Cassandra.Datacenters[dcState.idx].Stopped = true
				dcRestoreStatus.Phase = api.RestorePhaseNotStarted
			}
		}
		restore.Status.Datacenters[dcName] = dcRestoreStatus
	}

	if updated {
		logger.Info("Stopping DCs")
		if err := r.Patch(ctx, kc, patch); err != nil {
			return result.Error(fmt.Errorf("failed to stop DCs for K8ssandraCluster (%s): %v", utils.GetKey(kc), err))
		}
		return result.RequeueSoon(10 * time.Second)
	}

	if stopping {
		logger.Info("Waiting for DCs to stop")
		return result.RequeueSoon(10 * time.Second)
	}

	return result.Continue()
}

// checkDatacentersStarted ensures that all the CassandraDatacenters are started back
// up. Note that this method should be called only after all the Velero restores
// operations have completed. This method will cause reconciliation to requeue until all
// the DCs DatacenterReady conditions are true.
func (r *VeleroRestoreReconciler) checkDatacentersStarted(
	ctx context.Context,
	restore *api.VeleroRestore,
	kc *k8ssandraapi.K8ssandraCluster,
	restoreState map[string]datacenterRestoreState,
	logger logr.Logger) result.ReconcileResult {

	logger.Info("Check DCs started")

	starting := false
	updated := false
	patch := client.MergeFromWithOptions(kc.DeepCopy(), client.MergeFromWithOptimisticLock{})

	for dcName, dcState := range restoreState {
		dcRestoreStatus := restore.Status.Datacenters[dcName]
		if kc.Spec.Cassandra.Datacenters[dcState.idx].Stopped {
			updated = true
			starting = true
			kc.Spec.Cassandra.Datacenters[dcState.idx].Stopped = false
			dcRestoreStatus.Phase = api.RestorePhaseDatacenterStarting
		} else if kc.Status.Datacenters[dcName].Cassandra.GetConditionStatus(cassdcapi.DatacenterResuming) == corev1.ConditionTrue {
			starting = true
			dcRestoreStatus.Phase = api.RestorePhaseDatacenterStarting
		} else if kc.Status.Datacenters[dcName].Cassandra.GetConditionStatus(cassdcapi.DatacenterReady) == corev1.ConditionTrue {
			dcRestoreStatus.Phase = api.RestorePhaseCompleted
		}
	}

	if updated {
		logger.Info("Starting DCs")
		if err := r.Patch(ctx, kc, patch); err != nil {
			return result.Error(fmt.Errorf("failed to restart DCs for K8ssandraCluster (%s): %v", utils.GetKey(kc), err))
		}
		return result.RequeueSoon(10 * time.Second)
	}

	if starting {
		return result.RequeueSoon(10 * time.Second)
	}

	return result.Continue()
}

// checkPVCsDeleted ensures that all PVCs for all CassandraDatacenters are deleted. The
// check for a CassandraDatacenter is skipped if a corresponding Velero Restore object
// exists. This method causes reconciliation to requeue until all PVCs are gone. Note that
// this method should be only after all CassandraDatacenters have been stopped.
func (r *VeleroRestoreReconciler) checkPVCsDeleted(
	ctx context.Context,
	restore *api.VeleroRestore,
	kc *k8ssandraapi.K8ssandraCluster,
	restoreState map[string]datacenterRestoreState,
	logger logr.Logger) result.ReconcileResult {

	logger.Info("Checking PVCs deleted")

	count := 0
	for dcName, dcState := range restoreState {
		if dcState.vRestore != nil {
			logger.Info("Skipping PVC deletion check for DC since Velero restore already exists", "DC", dcName, "Velero Restore", utils.GetKey(dcState.vRestore))
			continue
		}
		dcRestoreStatus := restore.Status.Datacenters[dcName]
		dc := &cassdcapi.CassandraDatacenter{}
		if err := dcState.remoteClient.Get(ctx, dcState.dcKey, dc); err != nil {
			return result.Error(err)
		}

		pvcList := &corev1.PersistentVolumeClaimList{}
		if err := r.List(ctx, pvcList, client.InNamespace(dcState.dcKey.Namespace), client.MatchingLabels(dc.GetDatacenterLabels())); err != nil {
			return result.Error(fmt.Errorf("failed to list PVCs for DC (%s) in K8ssandraCluster (%s)", dcState.dcKey, utils.GetKey(kc)))
		}
		count += len(pvcList.Items)

		for _, pvc := range pvcList.Items {
			pvcKey := utils.GetKey(&pvc)
			logger.Info("Deleting PVC", "PVC", pvcKey)
			if err := dcState.remoteClient.Delete(ctx, &pvc); err != nil && !errors.IsNotFound(err) {
				return result.Error(fmt.Errorf("failed to delete PVC (%s) for DC (%s) in K8ssandraCluster (%s)",
					pvcKey, dcState.dcKey, utils.GetKey(kc)))
			}
		}
		dcRestoreStatus.Phase = api.ResstorePhaseVolumesDeleted
		restore.Status.Datacenters[dcName] = dcRestoreStatus
	}

	if count > 0 {
		return result.RequeueSoon(10 * time.Second)
	}

	return result.Continue()
}

// checkRestores checks each CassandraDatacenter and creates a corresponding Velero
// Restore object if one doesn't already exist. This method requeues the reconciliation
// request until the Restore for each CassandraDatacenter has finished. Note that finished
// can mean either success or failure. Note that this method assumes that all PVCs have
// already been deleted.
func (r *VeleroRestoreReconciler) checkRestores(
	ctx context.Context,
	restore *api.VeleroRestore,
	kc *k8ssandraapi.K8ssandraCluster,
	restoreState map[string]datacenterRestoreState,
	logger logr.Logger) result.ReconcileResult {

	logger.Info("Checking Velero restores")

	for dcName, dcState := range restoreState {
		dcRestoreStatus := restore.Status.Datacenters[dcName]
		if dcState.vRestore == nil {
			dc := &cassdcapi.CassandraDatacenter{}
			if err := dcState.remoteClient.Get(ctx, dcState.dcKey, dc); err != nil {
				return result.Error(fmt.Errorf("failed to get CassandraDatacenter (%s) that is in K8ssandraCluster (%s)",
					dcState.dcKey, utils.GetKey(kc)))
			}
			dcState.vRestore = &velerovapi.Restore{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: dcState.dcKey.Namespace,
					Name:      restore.Name,
				},
				Spec: velerovapi.RestoreSpec{
					BackupName:         restore.Spec.Backup,
					IncludedNamespaces: []string{dcState.dcKey.Namespace},
					LabelSelector:      metav1.SetAsLabelSelector(dc.GetDatacenterLabels()),
					IncludedResources: []string{
						"PersistentVolumeClaims",
						"PersistentVolumes",
					},
				},
			}
			labels.SetManagedBy(restore, utils.GetKey(kc))
			vRestoreKey := utils.GetKey(dcState.vRestore)

			logger.Info("Creating Velero Restore", "Restore", vRestoreKey)
			if err := dcState.remoteClient.Create(ctx, dcState.vRestore); err == nil {
				dcRestoreStatus.Phase = api.RestorePhaseDatacenterStarting
			} else {
				return result.Error(fmt.Errorf("failed to create Velero Restore (%s) for CassandraDatacenter (%s) in K8sssandraCluster (%s)",
					vRestoreKey, dcState.dcKey, utils.GetKey(kc)))
			}
		} else {
			switch dcState.vRestore.Status.Phase {
			case velerovapi.RestorePhaseInProgress:
				dcRestoreStatus.Phase = api.RestorePhaseInProgress
			case velerovapi.RestorePhaseCompleted:
				// TODO Should we wait to update the phase to completed until after the DC has successfully restarted?
				dcRestoreStatus.Phase = api.RestorePhaseCompleted
			case velerovapi.RestorePhaseFailed, velerovapi.RestorePhasePartiallyFailed, velerovapi.RestorePhaseFailedValidation:
				dcRestoreStatus.Phase = api.RestorePhaseFailed
			}
		}
		restore.Status.Datacenters[dcName] = dcRestoreStatus
	}

	if restoresFinished(restore) {
		logger.Info("All Velero restores have finished")
		return result.Continue()
	}

	return result.RequeueSoon(10 * time.Second)
}

func restoresFinished(restore *api.VeleroRestore) bool {
	for _, dcStatus := range restore.Status.Datacenters {
		if !restoreFinished(dcStatus) {
			return false
		}
	}
	return true
}

func restoreFinished(dcStatus api.DatacenterRestoreStatus) bool {
	switch dcStatus.Phase {
	case api.RestorePhaseCompleted, api.RestorePhaseFailed:
		return true
	}
	return false
}

func restoreHasFailures(restore *api.VeleroRestore) bool {
	for _, dcStatus := range restore.Status.Datacenters {
		if dcStatus.Phase == api.RestorePhaseFailed {
			return true
		}
	}
	return false
}

func findDatacenterTemplate(dcName string, kc *k8ssandraapi.K8ssandraCluster) int {
	for i, dcTemplate := range kc.Spec.Cassandra.Datacenters {
		if dcTemplate.Meta.Name == dcName {
			return i
		}
	}
	return -1
}

// SetupWithManager sets up the controller with the Manager.
func (r *VeleroRestoreReconciler) SetupWithManager(mgr ctrl.Manager, clusters []cluster.Cluster) error {
	cb := ctrl.NewControllerManagedBy(mgr).
		For(&api.VeleroRestore{}, builder.WithPredicates(predicate.GenerationChangedPredicate{}))

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

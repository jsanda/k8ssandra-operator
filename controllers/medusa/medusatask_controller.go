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

package medusa

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/go-logr/logr"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	medusav1alpha1 "github.com/k8ssandra/k8ssandra-operator/apis/medusa/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/config"
	"github.com/k8ssandra/k8ssandra-operator/pkg/medusa"
	"github.com/k8ssandra/k8ssandra-operator/pkg/shared"
	"github.com/k8ssandra/k8ssandra-operator/pkg/utils"
)

// MedusaTaskReconciler reconciles a MedusaTask object
type MedusaTaskReconciler struct {
	*config.ReconcilerConfig
	client.Client
	Scheme *runtime.Scheme
	medusa.ClientFactory
}

// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=medusatasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=medusatasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=medusatasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=medusa.k8ssandra.io,namespace="k8ssandra",resources=cassandradatacenters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",namespace="k8ssandra",resources=pods;services,verbs=get;list;watch

func (r *MedusaTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("MedusaTask", req.NamespacedName)

	logger.Info("Starting reconciliation for MedusaTask")

	// Fetch the MedusaTask instance
	instance := &medusav1alpha1.MedusaTask{}
	err := r.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		logger.Error(err, "Failed to get MedusaTask")
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	task := instance.DeepCopy()

	// First check to see if the task is already in progress
	if !task.Status.StartTime.IsZero() {
		// If there is anything in progress, simply requeue the request
		if len(task.Status.InProgress) > 0 {
			logger.Info("Tasks already in progress")
			return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
		}

		logger.Info("task complete")

		// Set the finish time
		// Note that the time here is not accurate, but that is ok. For now we are just
		// using it as a completion marker.
		patch := client.MergeFrom(task.DeepCopy())
		task.Status.FinishTime = metav1.Now()
		if err := r.Status().Patch(ctx, task, patch); err != nil {
			logger.Error(err, "failed to patch status with finish time")
			return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
		}

		// Schedule a sync if the task is a purge
		if task.Spec.Operation == medusav1alpha1.OperationTypePurge {
			r.scheduleSyncForPurge(task)
		}

		return ctrl.Result{Requeue: false}, nil
	}

	// If the task is already finished, there is nothing to do.
	if taskFinished(task) {
		logger.Info("Task is already finished", "MedusaTask", req.NamespacedName, "Operation", task.Spec.Operation)
		return ctrl.Result{Requeue: false}, nil
	}

	// Task hasn't started yet
	cassdcKey := types.NamespacedName{Namespace: task.Namespace, Name: task.Spec.CassandraDatacenter}
	cassdc := &cassdcapi.CassandraDatacenter{}
	err = r.Get(ctx, cassdcKey, cassdc)
	if err != nil {
		logger.Error(err, "failed to get cassandradatacenter", "CassandraDatacenter", cassdcKey)
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	pods, err := r.getCassandraDatacenterPods(ctx, cassdc, logger)
	if err != nil {
		logger.Error(err, "Failed to get datacenter pods")
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	if task.Spec.Operation == medusav1alpha1.OperationTypePurge {
		return r.purgeOperation(ctx, task, pods, logger)
	} else if task.Spec.Operation == medusav1alpha1.OperationTypeSync {
		return r.syncOperation(ctx, task, pods, logger)
	} else if task.Spec.Operation == medusav1alpha1.OperationTypePrepareRestore {
		return r.prepareRestoreOperation(ctx, task, pods, logger)
	} else {
		return ctrl.Result{}, fmt.Errorf("unsupported operation %s", task.Spec.Operation)
	}
}

func (r *MedusaTaskReconciler) getCassandraDatacenterPods(ctx context.Context, cassdc *cassdcapi.CassandraDatacenter, logger logr.Logger) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	labels := client.MatchingLabels{cassdcapi.DatacenterLabel: cassdc.Name}
	if err := r.List(ctx, podList, labels); err != nil {
		logger.Error(err, "failed to get pods for cassandradatacenter", "CassandraDatacenter", cassdc.Name)
		return nil, err
	}

	pods := make([]corev1.Pod, 0)
	pods = append(pods, podList.Items...)

	return pods, nil
}

func (r *MedusaTaskReconciler) purgeOperation(ctx context.Context, task *medusav1alpha1.MedusaTask, pods []corev1.Pod, logger logr.Logger) (reconcile.Result, error) {
	logger.Info("Starting purge operations")

	// Set the start time and add the pods to the in progress list
	patch := client.MergeFrom(task.DeepCopy())
	task.Status.StartTime = metav1.Now()
	for _, pod := range pods {
		task.Status.InProgress = append(task.Status.InProgress, pod.Name)
	}

	if err := r.Status().Patch(ctx, task, patch); err != nil {
		logger.Error(err, "Failed to patch status")
		// We received a stale object, requeue for next processing
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	// Invoke the purge operation on all pods, in the background
	go func() {
		wg := sync.WaitGroup{}

		// Mutex to prevent concurrent updates to the Status object
		backupMutex := sync.Mutex{}
		patch := client.MergeFrom(task.DeepCopy())

		for _, p := range pods {
			pod := p
			wg.Add(1)
			go func() {
				logger.Info("starting purge", "CassandraPod", pod.Name)
				succeeded := false
				var response *medusa.PurgeBackupsResponse
				response, purgeErr := doPurge(ctx, &pod, r.ClientFactory)
				if purgeErr == nil {
					logger.Info("finished purge", "CassandraPod", pod.Name)
					succeeded = true
				} else {
					logger.Error(purgeErr, "purge failed", "CassandraPod", pod.Name)
				}
				backupMutex.Lock()
				defer backupMutex.Unlock()
				defer wg.Done()
				task.Status.InProgress = utils.RemoveValue(task.Status.InProgress, pod.Name)
				if succeeded && response != nil {
					purgeResponse := medusav1alpha1.TaskResult{
						PodName:                   pod.Name,
						NbBackupsPurged:           int(response.NbBackupsPurged),
						NbObjectsPurged:           int(response.NbObjectsPurged),
						TotalPurgedSize:           int(response.TotalPurgedSize),
						TotalObjectsWithinGcGrace: int(response.TotalObjectsWithinGcGrace),
					}
					task.Status.Finished = append(task.Status.Finished, purgeResponse)
				} else {
					task.Status.Failed = append(task.Status.Failed, pod.Name)
				}
			}()
		}
		wg.Wait()
		logger.Info("finished task operations")
		if err := r.Status().Patch(context.Background(), task, patch); err != nil {
			logger.Error(err, "failed to patch status", "MedusaTask", fmt.Sprintf("%s/%s", task.Spec.Operation, task.Namespace))
		}
	}()

	return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
}

func (r *MedusaTaskReconciler) prepareRestoreOperation(ctx context.Context, task *medusav1alpha1.MedusaTask, pods []corev1.Pod, logger logr.Logger) (reconcile.Result, error) {
	logger.Info("Starting prepare restore operations")

	// Set the start time and add the pods to the in progress list
	patch := client.MergeFrom(task.DeepCopy())
	task.Status.StartTime = metav1.Now()
	for _, pod := range pods {
		task.Status.InProgress = append(task.Status.InProgress, pod.Name)
	}

	if err := r.Status().Patch(ctx, task, patch); err != nil {
		logger.Error(err, "Failed to patch status")
		// We received a stale object, requeue for next processing
		return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
	}

	// Invoke the purge operation on all pods, in the background
	go func() {
		wg := sync.WaitGroup{}

		// Mutex to prevent concurrent updates to the Status object
		backupMutex := sync.Mutex{}
		patch := client.MergeFrom(task.DeepCopy())

		for _, p := range pods {
			pod := p
			wg.Add(1)
			go func() {
				logger.Info("starting prepare restore", "CassandraPod", pod.Name)
				succeeded := false
				err := prepareRestore(ctx, task, &pod, r.ClientFactory)
				if err == nil {
					logger.Info("finished prepare restore", "CassandraPod", pod.Name)
					succeeded = true
				} else {
					logger.Error(err, "prepare restore failed", "CassandraPod", pod.Name)
				}
				backupMutex.Lock()
				defer backupMutex.Unlock()
				defer wg.Done()
				task.Status.InProgress = utils.RemoveValue(task.Status.InProgress, pod.Name)
				if succeeded {
					operationResponse := medusav1alpha1.TaskResult{
						PodName: pod.Name,
					}
					task.Status.Finished = append(task.Status.Finished, operationResponse)
				} else {
					task.Status.Failed = append(task.Status.Failed, pod.Name)
				}
			}()
		}
		wg.Wait()
		logger.Info("finished task operations")
		if err := r.Status().Patch(context.Background(), task, patch); err != nil {
			logger.Error(err, "failed to patch status", "MedusaTask", fmt.Sprintf("%s/%s", task.Spec.Operation, task.Namespace))
		}
	}()

	return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
}

func (r *MedusaTaskReconciler) syncOperation(ctx context.Context, task *medusav1alpha1.MedusaTask, pods []corev1.Pod, logger logr.Logger) (reconcile.Result, error) {
	logger.Info("Starting sync operation")
	syncStartTime := metav1.Now()
	for _, pod := range pods {
		logger.Info("Listing Backups", "CassandraPod", pod.Name)
		if remoteBackups, err := getBackups(ctx, &pod, r.ClientFactory); err != nil {
			logger.Error(err, "failed to list backups", "CassandraPod", pod.Name)
		} else {
			for _, backup := range remoteBackups {
				logger.Info("Syncing Backup", "Backup", backup.BackupName)
				// Create backups that should exist but are missing
				backupKey := types.NamespacedName{Namespace: task.Namespace, Name: backup.BackupName}
				backupResource := &medusav1alpha1.MedusaBackup{}
				if err := r.Get(ctx, backupKey, backupResource); err != nil {
					if errors.IsNotFound(err) {
						// Backup doesn't exist, create it
						logger.Info("Creating Cassandra Backup", "Backup", backup.BackupName)
						startTime := metav1.Unix(backup.StartTime, 0)
						finishTime := metav1.Unix(backup.FinishTime, 0)
						backupResource = &medusav1alpha1.MedusaBackup{
							ObjectMeta: metav1.ObjectMeta{
								Name:      backup.BackupName,
								Namespace: task.Namespace,
							},
							Spec: medusav1alpha1.MedusaBackupSpec{
								CassandraDatacenter: task.Spec.CassandraDatacenter,
								Type:                shared.BackupType(backup.BackupType),
							},
						}
						if err := r.Create(ctx, backupResource); err != nil {
							logger.Error(err, "failed to create backup", "MedusaBackup", backup.BackupName)
							return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
						} else {
							logger.Info("Created Medusa Backup", "Backup", backupResource)
							// Read the backup again. Multiple attempts may be necessary due to caches, which is why the Get operation is in a loop.
							backupInstance := &medusav1alpha1.MedusaBackup{}
							found := false
							for stay, timeout := true, time.After(10*time.Second); stay; {
								err := r.Get(ctx, backupKey, backupInstance)
								if err == nil {
									stay = false
									found = true
								} else {
									time.Sleep(time.Second)
									select {
									case <-timeout:
										stay = false
									default:
									}
								}
							}
							if !found {
								logger.Error(err, "failed to read backup", "MedusaBackup", backup.BackupName)
								return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
							}
							backupPatch := client.MergeFrom(backupInstance.DeepCopy())
							backupInstance.Status.StartTime = startTime
							backupInstance.Status.FinishTime = finishTime
							if err := r.Status().Patch(ctx, backupInstance, backupPatch); err != nil {
								logger.Error(err, "failed to patch status with finish time")
								return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
							}

						}
					} else {
						logger.Error(err, "failed to get backup", "Backup", backup.BackupName)
						return ctrl.Result{}, err
					}
				}
			}

			// Delete backups that don't exist remotely but are found locally
			localBackups := &medusav1alpha1.MedusaBackupList{}
			if err = r.List(ctx, localBackups, client.InNamespace(task.Namespace)); err != nil {
				logger.Error(err, "failed to list backups")
				return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
			}
			for _, backup := range localBackups.Items {
				if !backupExistsRemotely(remoteBackups, backup.ObjectMeta.Name) {
					logger.Info("Deleting Cassandra Backup", "Backup", backup.ObjectMeta.Name)
					if err := r.Delete(ctx, &backup); err != nil {
						logger.Error(err, "failed to delete backup", "MedusaBackup", backup.ObjectMeta.Name)
						return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
					} else {
						logger.Info("Deleted Cassandra Backup", "Backup", backup.ObjectMeta.Name)
					}
				}
			}

			// Update task status at the end of the reconcile
			logger.Info("finished task operations", "MedusaTask", fmt.Sprintf("%s/%s", task.Spec.Operation, task.Namespace))
			patch := client.MergeFrom(task.DeepCopy())
			task.Status.StartTime = syncStartTime
			task.Status.FinishTime = metav1.Now()
			taskResult := medusav1alpha1.TaskResult{PodName: pod.Name}
			task.Status.Finished = append(task.Status.Finished, taskResult)
			if err := r.Status().Patch(context.Background(), task, patch); err != nil {
				logger.Error(err, "failed to patch status", "MedusaTask", fmt.Sprintf("%s/%s", task.Spec.Operation, task.Namespace))
				return ctrl.Result{RequeueAfter: r.DefaultDelay}, err
			}
			return ctrl.Result{}, nil
		}
	}

	return ctrl.Result{RequeueAfter: r.DefaultDelay}, nil
}

// If the task operation was a purge, we may need to schedule a sync operation next
func (r *MedusaTaskReconciler) scheduleSyncForPurge(task *medusav1alpha1.MedusaTask) error {
	// Check if sync operation exists for this task, and create it if it doesn't
	sync := &medusav1alpha1.MedusaTask{}
	if err := r.Client.Get(context.Background(), types.NamespacedName{Name: task.GetObjectMeta().GetName() + "-sync", Namespace: task.Namespace}, sync); err != nil {
		if errors.IsNotFound(err) {
			// Create the sync task
			sync = &medusav1alpha1.MedusaTask{
				ObjectMeta: metav1.ObjectMeta{
					Name:      task.GetObjectMeta().GetName() + "-sync",
					Namespace: task.Namespace,
				},
				Spec: medusav1alpha1.MedusaTaskSpec{
					Operation:           medusav1alpha1.OperationTypeSync,
					CassandraDatacenter: task.Spec.CassandraDatacenter,
				},
			}
			if err := r.Client.Create(context.Background(), sync); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func doPurge(ctx context.Context, pod *corev1.Pod, clientFactory medusa.ClientFactory) (*medusa.PurgeBackupsResponse, error) {
	addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, shared.BackupSidecarPort)
	if medusaClient, err := clientFactory.NewClient(addr); err != nil {
		return nil, err
	} else {
		defer medusaClient.Close()
		return medusaClient.PurgeBackups(ctx)
	}
}

func prepareRestore(ctx context.Context, task *medusav1alpha1.MedusaTask, pod *corev1.Pod, clientFactory medusa.ClientFactory) error {
	addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, shared.BackupSidecarPort)
	if medusaClient, err := clientFactory.NewClient(addr); err != nil {
		return err
	} else {
		defer medusaClient.Close()
		_, err = medusaClient.PrepareRestore(ctx, task.Spec.CassandraDatacenter, task.Spec.BackupName, task.Spec.RestoreKey)
		return err
	}
}

func getBackups(ctx context.Context, pod *corev1.Pod, clientFactory medusa.ClientFactory) ([]*medusa.BackupSummary, error) {
	addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, shared.BackupSidecarPort)
	if medusaClient, err := clientFactory.NewClient(addr); err != nil {
		return nil, err
	} else {
		defer medusaClient.Close()
		return medusaClient.GetBackups(ctx)
	}
}

func backupExistsRemotely(backups []*medusa.BackupSummary, backupName string) bool {
	for _, backup := range backups {
		if backup.BackupName == backupName {
			return true
		}
	}
	return false
}

func taskFinished(task *medusav1alpha1.MedusaTask) bool {
	return !task.Status.FinishTime.IsZero()
}

// SetupWithManager sets up the controller with the Manager.
func (r *MedusaTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&medusav1alpha1.MedusaTask{}).
		Complete(r)
}

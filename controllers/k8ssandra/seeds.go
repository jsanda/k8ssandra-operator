package k8ssandra

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	cassdcapi "github.com/k8ssandra/cass-operator/apis/cassandra/v1beta1"
	api "github.com/k8ssandra/k8ssandra-operator/apis/k8ssandra/v1alpha1"
	"github.com/k8ssandra/k8ssandra-operator/pkg/annotations"
	"github.com/k8ssandra/k8ssandra-operator/pkg/result"
	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// findSeeds queries for pods labeled as seeds. It does this for each DC, across all
// clusters.
func (r *K8ssandraClusterReconciler) findSeeds(ctx context.Context, kc *api.K8ssandraCluster, logger logr.Logger) ([]corev1.Pod, error) {
	pods := make([]corev1.Pod, 0)

	for _, dcTemplate := range kc.Spec.Cassandra.Datacenters {
		remoteClient, err := r.ClientCache.GetRemoteClient(dcTemplate.K8sContext)
		if err != nil {
			logger.Error(err, "Failed to get remote client", "K8sContext", dcTemplate.K8sContext)
			return nil, err
		}

		namespace := kc.Namespace
		if dcTemplate.Meta.Namespace != "" {
			namespace = dcTemplate.Meta.Namespace
		}

		list := &corev1.PodList{}
		selector := map[string]string{
			cassdcapi.ClusterLabel:    kc.Name,
			cassdcapi.DatacenterLabel: dcTemplate.Meta.Name,
			cassdcapi.SeedNodeLabel:   "true",
		}

		dcKey := client.ObjectKey{Namespace: namespace, Name: dcTemplate.Meta.Name}

		if err := remoteClient.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels(selector)); err != nil {
			logger.Error(err, "Failed to get seed pods", "K8sContext", dcTemplate.K8sContext, "DC", dcKey)
			return nil, err
		}

		pods = append(pods, list.Items...)
	}

	return pods, nil
}

func (r *K8ssandraClusterReconciler) reconcileSeedsEndpoints(
	ctx context.Context,
	dc *cassdcapi.CassandraDatacenter,
	seeds []corev1.Pod,
	additionalSeeds []string,
	remoteClient client.Client,
	logger logr.Logger) result.ReconcileResult {
	logger.Info("Reconciling seeds")

	// The following if block was basically taken straight out of cass-operator. See
	// https://github.com/k8ssandra/k8ssandra-operator/issues/210 for a detailed
	// explanation of why this is being done.

	desiredEndpointSlice := newEndpointSlice(dc, seeds, additionalSeeds)
	actualEndpointSlice := &discovery.EndpointSlice{}
	endpointSliceKey := client.ObjectKey{Namespace: desiredEndpointSlice.Namespace, Name: desiredEndpointSlice.Name}

	if err := remoteClient.Get(ctx, endpointSliceKey, actualEndpointSlice); err == nil {
		// We can't have an Endpoints object that has no addresses or notReadyAddresses for
		// its EndpointSubset elements. This would be the case if both seeds and
		// additionalSeeds are empty, so we delete the Endpoints.
		if len(seeds) == 0 && len(additionalSeeds) == 0 {
			if err := remoteClient.Delete(ctx, actualEndpointSlice); err != nil {
				return result.Error(fmt.Errorf("failed to delete endpoints for dc (%s): %v", dc.Name, err))
			}
			return result.Continue()
		}

		if !annotations.CompareHashAnnotations(actualEndpointSlice, desiredEndpointSlice) {
			logger.Info("Updating endpoints", "EndpointSlice", endpointSliceKey)
			actualEndpointSlice := actualEndpointSlice.DeepCopy()
			resourceVersion := actualEndpointSlice.GetResourceVersion()
			desiredEndpointSlice.DeepCopyInto(actualEndpointSlice)
			actualEndpointSlice.SetResourceVersion(resourceVersion)
			if err = remoteClient.Update(ctx, actualEndpointSlice); err != nil {
				logger.Error(err, "Failed to update endpoints", "EndpointSlice", endpointSliceKey)
				return result.Error(err)
			}
		}
	} else {
		if errors.IsNotFound(err) {
			// If we have seeds then we want to go ahead and create the Endpoints. But
			// if we don't have seeds, then we don't need to do anything for a couple
			// of reasons. First, no seeds means that cass-operator has not labeled any
			// pods as seeds which would be the case when the CassandraDatacenter is f
			// first created and no pods have reached the ready state. Secondly, you
			// cannot create an Endpoints object that has both empty Addresses and
			// empty NotReadyAddresses.
			if len(seeds) > 0 || len(additionalSeeds) > 0 {
				logger.Info("Creating endpoints", "EndpointsSlice", endpointSliceKey)
				if err = remoteClient.Create(ctx, desiredEndpointSlice); err != nil {
					logger.Error(err, "Failed to create endpoints", "EndpointSlice", endpointSliceKey)
					return result.Error(err)
				}
			}
		} else {
			logger.Error(err, "Failed to get endpoints", "EndpointSlice", endpointSliceKey)
			return result.Error(err)
		}
	}
	return result.Continue()
}

// newEndpointSlice returns an Endpoints object who is named after the additional seeds service
// of dc.
func newEndpointSlice(dc *cassdcapi.CassandraDatacenter, seeds []corev1.Pod, additionalSeeds []string) *discovery.EndpointSlice {
	slice := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   dc.Namespace,
			Name:        dc.GetAdditionalSeedsServiceName(),
			Labels:      dc.GetDatacenterLabels(),
			Annotations: map[string]string{},
		},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints:   make([]discovery.Endpoint, 0, len(seeds)),
	}

	for _, seed := range seeds {
		slice.Endpoints = append(slice.Endpoints, discovery.Endpoint{
			Addresses: []string{seed.Status.PodIP},
		})
	}

	//ep := &corev1.Endpoints{
	//	ObjectMeta: metav1.ObjectMeta{
	//		Namespace:   dc.Namespace,
	//		Name:        dc.GetAdditionalSeedsServiceName(),
	//		Labels:      dc.GetDatacenterLabels(),
	//		Annotations: map[string]string{},
	//	},
	//}
	//
	//addresses := make([]corev1.EndpointAddress, 0, len(seeds))
	//for _, seed := range seeds {
	//	addresses = append(addresses, corev1.EndpointAddress{
	//		IP: seed.Status.PodIP,
	//	})
	//}
	//
	//for _, seed := range additionalSeeds {
	//	addresses = append(addresses, corev1.EndpointAddress{IP: seed})
	//}
	//
	//ep.Subsets = []corev1.EndpointSubset{
	//	{
	//		Addresses: addresses,
	//	},
	//}

	annotations.AddHashAnnotation(slice)

	return slice
}

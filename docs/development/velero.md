# Velero Setup Guide

Velero version: v1.8.1

This guide provides instructions for setting up your local environment to work with GKE. 

Velero currently does not work with kind. See [https://github.com/vmware-tanzu/velero/issues/4962](https://github.com/vmware-tanzu/velero/issues/4962).

* Create GKE cluster
* Create GCS bucket
* Generate and download service account key if you don't already have one
* Install Velero CLI
    * On MacOS install with HomeBrew, `brew install velero`
* Install Velero in GKE cluster

```
velero install --provider gcp \
       --plugins velero/velero-plugin-for-gcp:v1.4.1 \
       --bucket <bucket-name> \
       -n k8ssandra-operator \ 
       --secret-file <service-account-key-file>
```
* Check out velero branch from my fork [here](https://github.com/jsanda/k8ssandra-operator/tree/velero)
* Build k8ssandra-operator image
* Push operator image to remote repo
* Deploy k8ssandra-operator. Be sure to use the image you built!
* Deploy a K8ssandraCluster

```yaml
apiVersion: k8ssandra.io/v1alpha1
kind: K8ssandraCluster
metadata:
  name: test
spec:
  cassandra:
    serverVersion: "4.0.3"
    storageConfig:
      cassandraDataVolumeClaimSpec:
        storageClassName: premium-rwo
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 5Gi
    config:
      jvmOptions:
        heapSize: 1Gi
    datacenters:
      - metadata:
          name: dc1
        #k8sContext: gke-dev
        size: 1
```

* Wait for the K8ssandraCluster to become ready
* Take a backup

```yaml
apiVersion: velero.k8ssandra.io/v1alpha1
kind: VeleroBackup
metadata:
  name: backup-1
spec:
  k8ssandraCluster:
    name: test
  datacenters:
  - dc1
```






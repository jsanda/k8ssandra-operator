#!/usr/bin/env bash

credentials="./build/credentials-velero"
namespace="k8ssandra-operator"
num_clusters=2
kubeconfig="./build/kind-kubeconfig"

cat > $credentials << EOF
[default]
aws_access_key_id = minio
aws_secret_access_key = minio123
EOF

for ((i = 0; i < $num_clusters; ++i)); do
  kubectl --kubeconfig $kubeconfig config use-context kind-k8ssandra-$i
  velero install \
      --kubeconfig $kubeconfig \
      --provider aws \
      --plugins velero/velero-plugin-for-aws:v1.1.0 \
      --bucket velero \
      --namespace $namespace \
      --secret-file $credentials \
      --use-volume-snapshots=false \
      --backup-location-config region=minio,s3ForcePathStyle="true",s3Url=http://minio.$namespace.svc:9000
done
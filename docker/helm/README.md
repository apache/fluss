
# Fluss Helm Chart

This chart deploys a Fluss cluster with a CoordinatorServer and TabletServer as StatefulSets.
It requires a Zookeeper ensemble to be running in the same Kubernetes cluster.


To start Zookeeper use
helm install zk bitnami/zookeeper \
  --set replicaCount=3 \
  --set auth.enabled=false \
  --set persistence.size=5Gi


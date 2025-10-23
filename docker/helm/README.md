
# Fluss Helm Chart

This chart deploys an Apache Fluss cluster on Kubernetes, following Helm best practices.
It requires a Zookeeper ensemble to be running in the same Kubernetes cluster. In future releases, we may add support for an embedded Zookeeper cluster.


## Development environment 

| component                                                                      | version |
| ------------------------------------------------------------------------------ | ------- |
| [Docker](https://docs.docker.com/)                                             | v28.3.2 |
| [Minikube](https://minikube.sigs.k8s.io/docs/)                                 | v1.36.0 |
| [Kubernetes](https://kubernetes.io)                                            | v1.25.3 |
| [Helm](https://helm.sh)                                                        | v3.18.6 |
| [Apache Fluss](https://fluss.apache.org/docs/)                                 | v0.8-SNAPSHOT  |


## Image requirements 

A container image for Fluss is available on DockerHub as `fluss/fluss`. You can use it directly or build your own from this repo. To use your own image you need to build the project with [Maven](https://fluss.apache.org/community/dev/building/) and build it with Docker.

The Maven build will create all required artifacts in the `build-target` directory. You need to copy it into the `docker` directory. The Dockerfile in this directory will copy these artifacts into the image.

In minikube, you can use the local Docker daemon to build the image without pushing it to a registry:

```bash
eval $(minikube -p minikube docker-env)
docker build -t fluss/fluss:0.8-SNAPSHOT .
```

## Overview

It creates:
- 1x CoordinatorServer as a StatefulSet with a headless Service (stable per‑pod DNS)
- 3x TabletServers as a StatefulSet with a headless Service (stable per‑pod DNS)
- ConfigMap for server.yaml (CoordinatorServer and TabletServers) to override default Fluss configuration
- Optional PersistentVolumes for data directories

## Quick start

1) ZooKeeper deployment:

To start Zookeeper use Bitnami’s chart or your own deployment. If you have an existing Zookeeper cluster, you can skip this step. Example with Bitnami’s chart:

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm install zk bitnami/zookeeper \
  --set replicaCount=3 \
  --set auth.enabled=false \
  --set persistence.size=5Gi
```

2) Default (Zookeeper available in-cluster):

```bash
helm install fluss ./fluss-helm
```
With an optional namespace flag `--namespace <your-namespace>` if you want to install it in a specific namespace.

This assumes, that Zookeeper is reachable at `zk-zookeeper.<your-namespace>.svc.cluster.local:2181`. If your Zookeeper address is different, you can override it with:

```bash
helm install fluss ./fluss-helm \
  --set zookeeper.address=<your-zk-address>
```

## Configuration reference

Important Fluss options surfaced by the chart:
- zookeeper.address: CoordinatorServer and TabletServer point to your ZK ensemble.
- data.dir, remote.data.dir: Local persistent path for data; remote path for snapshots (OSS/HDFS). TabletServers default to a PVC mounted at data.dir.
- bind.listeners: Where the server actually binds.
- advertised.listeners: Externally advertised endpoints for clients and intra‑cluster communication. In K8s, advertise stable names.
- internal.listener.name: Which listener is used for internal communication (defaults to INTERNAL).
- tablet-server.id: Required to be unique per TabletServer. The chart auto‑derives this from the StatefulSet pod ordinal at runtime.


### Zookeeper and storage
- zookeeper.address must point to a reachable ensemble.
- data.dir defaults to /tmp/fluss/data; use a PVC if persistence.enabled=true.

## Resource management

Set resources with requests/limits as appropriate for production. There are no defaults to make it also run on environments with little resources such as Minikube.

## Troubleshooting
- Image pull errors:
  - If using a private registry, configure image.pullSecrets and ensure the image repository/tag are correct.
- Pods not ready: ensure ZooKeeper is reachable and ports 9123 are open.
- Connection failures: check advertised.listeners configuration and DNS resolution within the cluster by using kubectl exec to get a shell in a pod and test connectivity (using nc).
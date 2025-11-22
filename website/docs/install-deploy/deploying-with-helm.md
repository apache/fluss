---
title: "Deploying with Helm Charts"
sidebar_position: 5
---

# Deploying with Helm Charts

This page provides instructions for deploying a Fluss cluster on Kubernetes using Helm charts. The chart creates a distributed streaming storage system with CoordinatorServer and TabletServer components.

## Prerequisites

Before installing the Fluss Helm chart, ensure you have:

- Kubernetes
- Helm
- **For Local Development**: Minikube and Docker (see [Local Development with Minikube](#running-fluss-locally-with-minikube))

:::note
A Fluss cluster deployment requires a running ZooKeeper ensemble. To provide flexibility in deployment and enable reuse of existing infrastructure,
the Fluss Helm chart does not include a bundled ZooKeeper cluster. If you don’t already have a ZooKeeper running,
the installation documentation provides instructions for deploying one using Bitnami’s Helm chart.
:::

## Supported Versions

| Component | Minimum Version | Recommended Version |
|-----------|----------------|-------------------|
| [Kubernetes](https://kubernetes.io) | v1.19+ | v1.25+ |
| [Helm](https://helm.sh) | v3.8.0+ | v3.18.6+ |
| [ZooKeeper](https://zookeeper.apache.org) | v3.6+ | v3.8+ |
| [Apache Fluss](https://fluss.apache.org/docs/) (Container Image) | $FLUSS_VERSION$ | $FLUSS_VERSION$ |
| [Minikube](https://minikube.sigs.k8s.io) (Local Development) | v1.25+ | v1.32+ |
| [Docker](https://docs.docker.com/) (Local Development) | v20.10+ | v24.0+ |

## Installation

### Running Fluss locally with Minikube

For local testing and development, you can deploy Fluss on Minikube. This is ideal for development, testing, and learning purposes.

#### Prerequisites

- Docker container runtime
- At least 4GB RAM available for Minikube
- At least 2 CPU cores available

#### Start Minikube

```bash
# Start Minikube with recommended settings for Fluss
minikube start

# Verify cluster is ready
kubectl cluster-info
```

#### Configure Docker Environment (Optional)

To build images directly in Minikube you need to configure the Docker CLI to use Minikube's internal Docker daemon:

```bash
# Configure shell to use Minikube's Docker daemon
eval $(minikube docker-env)
```

To build custom images please refer to [Custom Container Images](#custom-container-images).

### Installing the chart on a cluster

This installation process is generally working for a distributed Kubernetes cluster or a Minikube setup.

### Step 1: Deploy ZooKeeper (Optional if ZooKeeper is existing)

To start Zookeeper use Bitnami’s chart or your own deployment. If you have an existing Zookeeper cluster, you can skip this step. Example with Bitnami’s chart:

```bash
# Add Bitnami repository
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# Deploy ZooKeeper
helm install zk bitnami/zookeeper
```
### Step 2: Deploy Fluss

#### Install from Helm repo

```bash
helm repo add fluss https://downloads.apache.org/incubator/fluss/helm-chart
helm repo update
helm install helm-repo/fluss
```

#### Install from Local Chart

```bash
helm install fluss ./helm
```

#### Install with Custom Values

You can customize the installation by providing your own `values.yaml` file or setting individual parameters via the `--set` flag. Using a custom values file:

```bash
helm install fluss ./helm -f my-values.yaml
```

Or for example to change the ZooKeeper address via the `--set` flag:

```bash
helm install fluss ./helm \
  --set configurationOverrides.zookeeper.address=<my-zk-cluster>:2181
```

### Cleanup

```bash
# Uninstall Fluss
helm uninstall fluss

# Uninstall ZooKeeper
helm uninstall zk

# Delete PVCs
kubectl delete pvc -l app.kubernetes.io/name=fluss

# Stop Minikube
minikube stop

# Delete Minikube cluster
minikube delete
```

## Architecture Overview

The Fluss Helm chart deploys the following Kubernetes resources:

### Core Components
- **CoordinatorServer**: 1x StatefulSet with Headless Service for cluster coordination
- **TabletServer**: 3x StatefulSet with Headless Service for data storage and processing
- **ConfigMap**: Configuration management for `server.yaml` settings
- **Services**: Headless services providing stable pod DNS names

### Optional Components
- **PersistentVolumes**: Data persistence when `persistence.enabled=true`


### Step 3: Verify Installation

```bash
# Check pod status
kubectl get pods -l app.kubernetes.io/name=fluss

# Check services
kubectl get svc -l app.kubernetes.io/name=fluss

# View logs
kubectl logs -l app.kubernetes.io/component=coordinator
kubectl logs -l app.kubernetes.io/component=tablet
```

## Configuration Parameters

The following table lists the configurable parameters of the Fluss chart and their default values.

### Global Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nameOverride` | Override the name of the chart | `""` |
| `fullnameOverride` | Override the full name of the resources | `""` |

### Image Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.registry` | Container image registry | `""` |
| `image.repository` | Container image repository | `fluss` |
| `image.tag` | Container image tag | `$FLUSS_VERSION$` |
| `image.pullPolicy` | Container image pull policy | `IfNotPresent` |
| `image.pullSecrets` | Container image pull secrets | `[]` |

### Application Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `appConfig.internalPort` | Internal communication port | `9123` |
| `appConfig.externalPort` | External client port | `9124` |

### Fluss Configuration Overrides

| Parameter | Description | Default |
|-----------|-------------|---------|
| `configurationOverrides.default.bucket.number` | Default number of buckets for tables | `3` |
| `configurationOverrides.default.replication.factor` | Default replication factor | `3` |
| `configurationOverrides.zookeeper.path.root` | ZooKeeper root path for Fluss | `/fluss` |
| `configurationOverrides.zookeeper.address` | ZooKeeper ensemble address | `zk-zookeeper.{{ .Release.Namespace }}.svc.cluster.local:2181` |
| `configurationOverrides.remote.data.dir` | Remote data directory for snapshots | `/tmp/fluss/remote-data` |
| `configurationOverrides.data.dir` | Local data directory | `/tmp/fluss/data` |
| `configurationOverrides.internal.listener.name` | Internal listener name | `INTERNAL` |

### Persistence Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `persistence.enabled` | Enable persistent volume claims | `false` |
| `persistence.size` | Persistent volume size | `1Gi` |
| `persistence.storageClass` | Storage class name | `nil` (uses default) |

### Resource Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.coordinatorServer.requests.cpu` | CPU requests for coordinator | Not set |
| `resources.coordinatorServer.requests.memory` | Memory requests for coordinator | Not set |
| `resources.coordinatorServer.limits.cpu` | CPU limits for coordinator | Not set |
| `resources.coordinatorServer.limits.memory` | Memory limits for coordinator | Not set |
| `resources.tabletServer.requests.cpu` | CPU requests for tablet servers | Not set |
| `resources.tabletServer.requests.memory` | Memory requests for tablet servers | Not set |
| `resources.tabletServer.limits.cpu` | CPU limits for tablet servers | Not set |
| `resources.tabletServer.limits.memory` | Memory limits for tablet servers | Not set |


## Advanced Configuration

### Custom ZooKeeper Configuration

For external ZooKeeper clusters:

```yaml
configurationOverrides:
  zookeeper.address: "zk1.example.com:2181,zk2.example.com:2181,zk3.example.com:2181"
  zookeeper.path.root: "/my-fluss-cluster"
```

### Network Configuration

The chart automatically configures listeners for internal cluster communication and external client access:

- **Internal Port (9123)**: Used for inter-service communication within the cluster
- **External Port (9124)**: Used for client connections

Custom listener configuration:

```yaml
appConfig:
  internalPort: 9123
  externalPort: 9124

configurationOverrides:
  bind.listeners: "INTERNAL://0.0.0.0:9123,CLIENT://0.0.0.0:9124"
  advertised.listeners: "CLIENT://my-cluster.example.com:9124"
```

### Storage Configuration

Configure different storage backends:

```yaml
configurationOverrides:
  data.dir: "/data/fluss"
  remote.data.dir: "s3://my-bucket/fluss-data"
```

## Upgrading

### Upgrade the Chart

```bash
# Upgrade to a newer chart version
helm upgrade fluss ./helm

# Upgrade with new configuration
helm upgrade fluss ./helm -f values-new.yaml
```

### Rolling Updates

The StatefulSets support rolling updates. When you update the configuration, pods will be restarted one by one to maintain availability.

## Custom Container Images

### Building Custom Images

To build and use custom Fluss images:

1. Build the project with Maven:
```bash
mvn clean package -DskipTests
```

2. Build the Docker image:
```bash
# Copy build artifacts
cp -r build-target/* docker/fluss/build-target

# Build image
cd docker
docker build -t my-registry/fluss:custom-tag .
```

3. Use in Helm values:
```yaml
image:
  registry: my-registry
  repository: fluss
  tag: custom-tag
  pullPolicy: Always
```

## Monitoring and Observability

### Health Checks

The chart includes liveness and readiness probes:

```yaml
livenessProbe:
  tcpSocket:
    port: 9124
  initialDelaySeconds: 10
  periodSeconds: 3
  failureThreshold: 100

readinessProbe:
  tcpSocket:
    port: 9124
  initialDelaySeconds: 10
  periodSeconds: 3
  failureThreshold: 100
```

### Logs

Access logs from different components:

```bash
# Coordinator logs
kubectl logs -l app.kubernetes.io/component=coordinator -f

# Tablet server logs
kubectl logs -l app.kubernetes.io/component=tablet -f

# Specific pod logs
kubectl logs coordinator-server-0 -f
kubectl logs tablet-server-0 -f
```

## Troubleshooting

### Common Issues

#### Pod Startup Issues

**Symptoms**: Pods stuck in `Pending` or `CrashLoopBackOff` state

**Solutions**:
```bash
# Check pod events
kubectl describe pod <pod-name>

# Check resource availability
kubectl describe nodes

# Verify ZooKeeper connectivity
kubectl exec -it <fluss-pod> -- nc -zv <zookeeper-host> 2181
```

#### Image Pull Errors

**Symptoms**: `ImagePullBackOff` or `ErrImagePull`

**Solutions**:
- Verify image repository and tag exist
- Check pull secrets configuration
- Ensure network connectivity to registry


#### Connection Issues

**Symptoms**: Clients cannot connect to Fluss cluster

**Solutions**:
```bash
# Check service endpoints
kubectl get endpoints

# Test network connectivity
kubectl exec -it <client-pod> -- nc -zv <fluss-service> 9124

# Verify DNS resolution
kubectl exec -it <client-pod> -- nslookup <fluss-service>
```

### Debug Commands

```bash
# Get all resources
kubectl get all -l app.kubernetes.io/name=fluss

# Check configuration
kubectl get configmap fluss-conf-file -o yaml


# Get detailed pod information
kubectl get pods -o wide -l app.kubernetes.io/name=fluss
```


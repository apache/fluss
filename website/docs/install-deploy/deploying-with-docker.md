---
sidebar_position: 4
---

# Deploying with Docker

This guide will show you how to run a Fluss cluster with Docker.

## Prerequisites

### Software

Ensure that [Docker](https://docs.docker.com/engine/install/) and the [Docker Compose plugin](https://docs.docker.com/compose/install/linux/) are installed on your machine.
All commands were tested with Docker version 27.4.0 and Docker Compose version v2.30.3.

:::note
We encourage you to use a recent version of Docker and [Compose v2](https://docs.docker.com/compose/releases/migrate/) (however, Compose v1 might work with a few adaptions).
:::

### Hardware

Recommended configuration: 4 cores, 16GB memory.


## Deployment

You have 2 options to deploy Fluss with Docker.

- **Option 1**: Deployment as individual Docker containers (`docker run`)
- **Option 2** (recommended): Deployment with Docker compose (`docker compose`)

### Option 1: Deployment as individual Docker containers (`docker run`)

In the following, we give a brief overview how to quickly create a minimal Fluss cluster setup
using `docker run`. All containers will be run in daemon mode (`-d` option).

#### Create a shared tmpfs volume

The `shared-tmpfs` volume will store all Fluss-related data.

```bash
docker volume create shared-tmpfs
```

#### Create a Network

The `fluss-demo` network is used for network communication among Fluss components.

```bash
docker network create fluss-demo
```

#### Start Zookeeper

Zookeeper is the central metadata store for Fluss. The command below runs a _single_ ZooKeeper instance.

:::info
For production use, ZooKeeper should be set up with replication. For more information,
see [Running Replicated ZooKeeper](https://zookeeper.apache.org/doc/r3.6.0/zookeeperStarted.html#sc_RunningReplicatedZooKeeper).
:::

```bash
docker run \
    --name zookeeper \
    --network=fluss-demo \
    --restart always \
    -p 2181:2181 \
    -d zookeeper:3.9.2
```

Fluss-related components have to connect to ZooKeeper (`zookeeper.address`).

#### Start Fluss CoordinatorServer

Start a Fluss CoordinatorServer instance.

```bash
docker run \
    --name coordinator-server \
    --network=fluss-demo \
    --env FLUSS_PROPERTIES="zookeeper.address: zookeeper:2181
coordinator.host: coordinator-server" \
    -p 9123:9123 \
    -d fluss/fluss:0.5.0 coordinatorServer
```

#### Start Fluss TabletServer

You can start one or more TabletServer instances based on your needs. 

:::info
For production use, you should run multiple TabletServer instances.
:::

To start a single TabletServer instance (sufficient for testing purposes), run the command below.

```bash
# tablet-server-0
docker run \
    --name tablet-server-0 \
    --network=fluss-demo \
    --env FLUSS_PROPERTIES="zookeeper.address: zookeeper:2181
tablet-server.host: tablet-server-0
tablet-server.id: 0
tablet-server.port: 9124
data.dir: /tmp/fluss/data/tablet-server-0
remote.data.dir: /tmp/fluss/remote-data" \
    -p 9124:9124 \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/fluss:0.5.0 tabletServer
```

Additional TabletServer instances (production use) can be started in a similar fashion.

**Example**: To start 2 _additional_ TabletServer instances, execute the commands below (note the adapted arguments for `--name`, `tablet-server.*`, `data.dir` and `-p`).

```bash
# tablet-server-1
docker run \
--name tablet-server-1 \
--network=fluss-demo \
--env FLUSS_PROPERTIES="zookeeper.address: zookeeper:2181
tablet-server.host: tablet-server-1
tablet-server.id: 1
tablet-server.port: 9125
data.dir: /tmp/fluss/data/tablet-server-1
remote.data.dir: /tmp/fluss/remote-data" \
-p 9125:9125 \
--volume shared-tmpfs:/tmp/fluss \
-d fluss/fluss:0.5.0 tabletServer
```

```bash
# tablet-server-2
docker run \
    --name tablet-server-2 \
    --network=fluss-demo \
    --env FLUSS_PROPERTIES="zookeeper.address: zookeeper:2181
tablet-server.host: tablet-server-2
tablet-server.id: 2
tablet-server.port: 9126
data.dir: /tmp/fluss/data/tablet-server-2
remote.data.dir: /tmp/fluss/remote-data" \
    -p 9126:9126 \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/fluss:0.5.0 tabletServer
```

Now, all Fluss-related components are running.

Run the command below to check the Fluss cluster status.

```bash
docker container ls
```

You should see at least 1 _running_

- ZooKeeper
- CoordinatorServer
- TabletServer

instance.

#### Interacting with Fluss

After the Fluss cluster is started, you can use **Fluss Client** (e.g., Flink SQL Client) to interact with Fluss.
The following subsections will show you how to build a Flink cluster and use the **Flink SQL Client**
to interact with Fluss.

##### Start Flink Cluster

We will use the `fluss/quickstart-flink` container image that comes with all Fluss-related dependencies.

1. Start a JobManager instance

```bash
docker run \
    --name jobmanager \
    --network=fluss-demo \
    --env FLINK_PROPERTIES=" jobmanager.rpc.address: jobmanager" \
    -p 8083:8081 \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/quickstart-flink:1.20-0.5 jobmanager
```

2. Start a single TaskManager instance

```bash
docker run \
    --name taskmanager \
    --network=fluss-demo \
    --env FLINK_PROPERTIES=" jobmanager.rpc.address: jobmanager" \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/quickstart-flink:1.20-0.5 taskmanager
```

3. Run the command below to check the Fluss cluster status again.

```bash
docker container ls
```

You should now also see 1 _running_ Flink JobManager and 1 _running_ TaskManager instance.

##### Fluss SQL Client

1. Enter the Flink SQL CLI client.

```shell
docker exec -it jobmanager ./sql-client
```

2. Use the commands below to verify that the cluster works.

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
```

Congratulations! You have successfully deployed Fluss with `docker run`. 
Check out [this section](#whats-next) for potential next steps.



## Option 2: Deployment with Docker compose (`docker compose`)

In the following, we give a brief overview how to quickly create a minimal Fluss cluster setup
using `docker compose`.
All containers will be run in daemon mode (-d option).

### Start Fluss Cluster

:::info
For production use,
- ZooKeeper should be set up with replication. For more information, see [Running Replicated ZooKeeper](https://zookeeper.apache.org/doc/r3.6.0/zookeeperStarted.html#sc_RunningReplicatedZooKeeper).
- you should run multiple TabletServer instances.
:::

You can use the following manifest to start a Fluss cluster (sufficient for testing purposes) with
- 1 CoordinatorServer, 
- 1 TabletServer, and 
- 1 ZooKeeper instance (central metadata store used by Fluss).

```yaml
services:
  coordinator-server:
    image: fluss/fluss:0.5.0
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        coordinator.host: coordinator-server
        remote.data.dir: /tmp/fluss/remote-data
  tablet-server-0:
    image: fluss/fluss:0.5.0
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server-0
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
    volumes:
      - shared-tmpfs:/tmp/fluss
  zookeeper:
    restart: always
    image: zookeeper:3.9.2

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

Save the manifest as `docker-compose.yml` and run
```bash
docker compose up -d
```
in the same directory to create the cluster.

Run the command below to check the Fluss cluster status.

```bash
docker container ls
```

You should see 1 _running_

- ZooKeeper
- CoordinatorServer
- TabletServer

instance.

Additional TabletServer instances (production use) can be started by adding them to the manifest file.

**Example**: To start 2 _additional_ TabletServer instances, modify the `docker-compose.yml` manifest file as follows (note the added services `tablet-server-1` and `tablet-server-2` and their modified configurations).

```yaml
services:
  coordinator-server:
    image: fluss/fluss:0.5.0
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        coordinator.host: coordinator-server
        remote.data.dir: /tmp/fluss/remote-data
  tablet-server-0:
    image: fluss/fluss:0.5.0
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server-0
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data/tablet-server-0
        remote.data.dir: /tmp/fluss/remote-data
    volumes:
      - shared-tmpfs:/tmp/fluss
  #begin additional tablet server instances
  tablet-server-1:
    image: fluss/fluss:0.5.0
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server-1
        tablet-server.id: 1
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data/tablet-server-1
        remote.data.dir: /tmp/fluss/remote-data
    volumes:
      - shared-tmpfs:/tmp/fluss
  tablet-server-2:
    image: fluss/fluss:0.5.0
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server-2
        tablet-server.id: 2
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data/tablet-server-2
        remote.data.dir: /tmp/fluss/remote-data
    volumes:
      - shared-tmpfs:/tmp/fluss
  #end
  zookeeper:
    restart: always
    image: zookeeper:3.9.2

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

Save the modifications and re-run
```bash
docker compose up -d
```
in the same directory to apply the changes.

Run the command below to check the Fluss cluster status again.

```bash
docker container ls
```

You should now see 2 additional _running_ TabletServer instances.

### Interacting with Fluss

After the Fluss cluster is started, you can use **Fluss Client** (e.g., Flink SQL Client) to interact with Fluss.
The following subsections will show you how to build a Flink cluster and use the **Flink SQL Client**
to interact with Fluss.

#### Start Flink Cluster

Adapt the `docker-compose.yml` manifest file and add a Flink cluster.
We will use the `fluss/quickstart-flink` container image that comes with all Fluss-related dependencies.
**Note**: The following manifest uses the setup with **3** TabletServer instances from above.

```yaml
services:
  coordinator-server:
    image: fluss/fluss:0.5.0
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        coordinator.host: coordinator-server
        remote.data.dir: /tmp/fluss/remote-data
  tablet-server-0:
    image: fluss/fluss:0.5.0
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server-0
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data/tablet-server-0
        remote.data.dir: /tmp/fluss/remote-data
    volumes:
      - shared-tmpfs:/tmp/fluss
  #begin additional tablet server instances
  tablet-server-1:
    image: fluss/fluss:0.5.0
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server-1
        tablet-server.id: 1
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data/tablet-server-1
        remote.data.dir: /tmp/fluss/remote-data
    volumes:
      - shared-tmpfs:/tmp/fluss
  tablet-server-2:
    image: fluss/fluss:0.5.0
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server-2
        tablet-server.id: 2
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data/tablet-server-2
        remote.data.dir: /tmp/fluss/remote-data
    volumes:
      - shared-tmpfs:/tmp/fluss
  #end
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  #begin Flink cluster
  jobmanager:
    image: fluss/quickstart-flink:1.20-0.5
    ports:
      - "8083:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - shared-tmpfs:/tmp/fluss
  taskmanager:
    image: fluss/quickstart-flink:1.20-0.5
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - shared-tmpfs:/tmp/fluss
  #end
  
volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```

Save the modifications and re-run
```bash
docker compose up -d
```
in the same directory to apply the changes.

Run the command below to check the Fluss cluster status again.

```bash
docker container ls
```

You should now also see 1 _running_ Flink JobManager and 1 _running_ TaskManager instance.

#### Fluss SQL Client

1. Enter the Flink SQL CLI client.

```shell
docker compose exec jobmanager ./sql-client
```

2. Use the commands below to verify that the cluster works.

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
```

Congratulations! You have successfully deployed Fluss with `docker compose`.
Check out [this section](#whats-next) for potential next steps.


## What's next?

After the catalog is created, you can use Flink and the Flink SQL Client to do more with Fluss, for example, create a table, insert data, query data, etc.
For more details, please refer to [Getting Started with Flink Engine](/docs/engine-flink/getting-started/) (you can skip the first steps and start [here](/docs/engine-flink/getting-started/#creating-a-table)).

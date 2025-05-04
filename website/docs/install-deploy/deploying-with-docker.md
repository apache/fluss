---
title: "Deploying with Docker"
sidebar_position: 4
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Deploying with Docker

This guide will show you how to run a Fluss cluster using Docker. In this guide, we will introduce the prerequisites of
the Docker environment and how to quickly create a Fluss cluster using the `docker run` commands
or `docker compose` file.

## Prerequisites

**Overview**

Prepare the build machine before creating the Docker image.

**Hardware**

Recommended configuration: 4 cores, 16GB memory.

**Software**

Docker and the Docker Compose plugin. All commands were tested with Docker version 27.4.0 and Docker Compose version v2.30.3.

## Deploy with Docker

The following is a brief overview of how to quickly create a complete Fluss testing cluster
using the `docker run` commands.

### Create a shared tmpfs volume

Create a shared tmpfs volume:
```bash
docker volume create shared-tmpfs
```

### Create a Network

Create an isolated bridge network in docker
```bash
docker network create fluss-demo
```

### Start Zookeeper

Start Zookeeper in daemon mode. This is a single node zookeeper setup. Zookeeper is the central metadata store
for Fluss and should be set up with replication for production use. For more information,
see [Running zookeeper cluster](https://zookeeper.apache.org/doc/r3.6.0/zookeeperStarted.html#sc_RunningReplicatedZooKeeper).

```bash
docker run \
    --name zookeeper \
    --network=fluss-demo \
    --restart always \
    -p 2181:2181 \
    -d zookeeper:3.9.2
```

### Start Fluss CoordinatorServer

Start Fluss CoordinatorServer in daemon and connect to Zookeeper.
```bash
docker run \
    --name coordinator-server \
    --network=fluss-demo \
    --env FLUSS_PROPERTIES="zookeeper.address: zookeeper:2181
coordinator.host: coordinator-server" \
    -p 9123:9123 \
    -d fluss/fluss:$FLUSS_VERSION$ coordinatorServer
```

### Start Fluss TabletServer

You can start one or more tablet servers based on your needs. For a production environment,
ensure that you have multiple tablet servers.

#### Start with One TabletServer

If you just want to start a sample test, you can start only one TabletServer in daemon and connect to Zookeeper.
The command is as follows:
```bash
docker run \
    --name tablet-server \
    --network=fluss-demo \
    --env FLUSS_PROPERTIES="zookeeper.address: zookeeper:2181
tablet-server.host: tablet-server
tablet-server.id: 0
tablet-server.port: 9124
data.dir: /tmp/fluss/data
remote.data.dir: /tmp/fluss/remote-data" \
    -p 9124:9124 \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/fluss:$FLUSS_VERSION$ tabletServer
```

#### Start with Multiple TabletServer

In a production environment, you need to start multiple Fluss TabletServer nodes.
Here we start 3 Fluss TabletServer nodes in daemon and connect to Zookeeper. The command is as follows:

1. start tablet-server-0
```bash
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
    -d fluss/fluss:$FLUSS_VERSION$ tabletServer
```

2. start tablet-server-1
```bash
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
    -d fluss/fluss:$FLUSS_VERSION$ tabletServer
```

3. start tablet-server-2
```bash
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
    -d fluss/fluss:$FLUSS_VERSION$ tabletServer
```

Now all the Fluss related components are running.

Run the below command to check the Fluss cluster status:

```bash
docker container ls -a
```

### Interacting with Fluss

After the Fluss cluster is started, you can use **Fluss Client** (e.g., Flink SQL Client) to interact with Fluss.
The following subsections will show you how to use 'Docker' to build a Flink cluster and use **Flink SQL Client**
to interact with Fluss.

#### Start Flink Cluster

1. start jobManager

```bash
docker run \
    --name jobmanager \
    --network=fluss-demo \
    --env FLINK_PROPERTIES=" jobmanager.rpc.address: jobmanager" \
    -p 8083:8081 \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/quickstart-flink:1.20-$FLUSS_VERSION_SHORT$ jobmanager
```

2. start taskManager

```bash
docker run \
    --name taskmanager \
    --network=fluss-demo \
    --env FLINK_PROPERTIES=" jobmanager.rpc.address: jobmanager" \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/quickstart-flink:1.20-$FLUSS_VERSION_SHORT$ taskmanager
```

#### Enter into SQL-Client
First, use the following command to enter pod:
```shell
docker exec -it jobmanager /bin/bash
```

Then, use the following command to enter the Flink SQL CLI Container:
```shell
./sql-client
```

#### Create Fluss Catalog

Use the following SQL to create a Fluss catalog:
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
```

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

#### Do more with Fluss

After the catalog is created, you can use Flink SQL Client to do more with Fluss, for example, create a table, insert data, query data, etc.
More details please refer to [Flink Getting started](engine-flink/getting-started.md)

## Deploy with Docker Compose

The following is a brief overview of how to quickly create a complete Fluss testing cluster
using the `docker compose up -d` commands in a detached mode.

### Create docker-compose.yml file

#### Compose file to start Fluss cluster with one TabletServer
You can use the following `docker-compose.yml` file to start a Fluss cluster with one `CoordinatorServer` and one `TabletServer`.

```yaml
services:
  coordinator-server:
    image: fluss/fluss:$FLUSS_VERSION$
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        coordinator.host: coordinator-server
        remote.data.dir: /tmp/fluss/remote-data
  tablet-server:
    image: fluss/fluss:$FLUSS_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        tablet-server.host: tablet-server
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

#### Compose file to start Fluss cluster with multi TabletServer

You can use the following `docker-compose.yml` file to start a Fluss cluster with one `CoordinatorServer` and three `TabletServers`.

```yaml
services:
  coordinator-server:
    image: fluss/fluss:$FLUSS_VERSION$
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
    image: fluss/fluss:$FLUSS_VERSION$
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
  tablet-server-1:
    image: fluss/fluss:$FLUSS_VERSION$
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
    image: fluss/fluss:$FLUSS_VERSION$
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

### Launch the components

Save the `docker-compose.yaml` script and execute the `docker compose up -d` command in the same directory
to create the cluster.

Run the below command to check the container status:

```bash
docker container ls -a
```

### Interacting with Fluss

If you want to interact with this Fluss cluster, you can change the `docker-compose.yml` file to add a Flink cluster.
The changed `docker-compose.yml` file is as follows:

```yaml
services:
  coordinator-server:
    image: fluss/fluss:$FLUSS_VERSION$
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
    image: fluss/fluss:$FLUSS_VERSION$
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
  tablet-server-1:
    image: fluss/fluss:$FLUSS_VERSION$
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
    image: fluss/fluss:$FLUSS_VERSION$
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
  zookeeper:
    restart: always
    image: zookeeper:3.9.2
  jobmanager:
    image: fluss/quickstart-flink:1.20-$FLUSS_VERSION_SHORT$
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
    image: fluss/quickstart-flink:1.20-$FLUSS_VERSION_SHORT$
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
    volumes:
      - shared-tmpfs:/tmp/fluss

volumes:
  shared-tmpfs:
    driver: local
    driver_opts:
      type: "tmpfs"
      device: "tmpfs"
```
Save the `docker-compose.yaml` script and execute the `docker-compose up -d` command in the same directory to create the cluster.

#### Enter into SQL-Client
First, use the following command to enter the Flink SQL CLI Container:
```shell
docker compose exec jobmanager ./sql-client
```

#### Create Fluss Catalog
Use the following SQL to create a Fluss catalog:
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);
```

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

#### Do more with Fluss

After the catalog is created, you can use Flink SQL Client to do more with Fluss, for example, create a table, insert data, query data, etc.
More details please refer to [Flink Getting started](engine-flink/getting-started.md)
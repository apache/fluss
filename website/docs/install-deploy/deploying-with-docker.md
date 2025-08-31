---
title: "Deploying with Docker"
sidebar_position: 4
---

# Deploying with Docker

This guide will show you how to run a Fluss cluster using Docker. 
We will introduce the [prerequisites of the Docker environment](#prerequisites), and how to quickly create a Fluss cluster using [`docker run` commands](#deploy-with-docker)
or a [`docker compose` file](#deploy-with-docker-compose).

## Prerequisites

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
bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123
advertised.listeners: CLIENT://localhost:9123
internal.listener.name: INTERNAL
" \
    -p 9123:9123 \
    -d fluss/fluss:$FLUSS_DOCKER_VERSION$ coordinatorServer
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
bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123
advertised.listeners: CLIENT://localhost:9124
internal.listener.name: INTERNAL
tablet-server.id: 0
kv.snapshot.interval: 0s
data.dir: /tmp/fluss/data
remote.data.dir: /tmp/fluss/remote-data" \
    -p 9124:9123 \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/fluss:$FLUSS_DOCKER_VERSION$ tabletServer
```

#### Start with Multiple TabletServer

In a production environment, you need to start multiple Fluss TabletServer nodes.
Here we start 3 Fluss TabletServer nodes in daemon and connect to Zookeeper. The command is as follows:

1. Start tablet-server-0
```bash
docker run \
    --name tablet-server-0 \
    --network=fluss-demo \
    --env FLUSS_PROPERTIES="zookeeper.address: zookeeper:2181
bind.listeners: INTERNAL://tablet-server-0:0, CLIENT://tablet-server-0:9123
advertised.listeners: CLIENT://localhost:9124
internal.listener.name: INTERNAL
tablet-server.id: 0
kv.snapshot.interval: 0s
data.dir: /tmp/fluss/data/tablet-server-0
remote.data.dir: /tmp/fluss/remote-data" \
    -p 9124:9123 \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/fluss:$FLUSS_DOCKER_VERSION$ tabletServer
```

2. Start tablet-server-1
```bash
docker run \
    --name tablet-server-1 \
    --network=fluss-demo \
    --env FLUSS_PROPERTIES="zookeeper.address: zookeeper:2181
bind.listeners: INTERNAL://tablet-server-1:0, CLIENT://tablet-server-1:9123
advertised.listeners: CLIENT://localhost:9125
internal.listener.name: INTERNAL
tablet-server.id: 1
kv.snapshot.interval: 0s
data.dir: /tmp/fluss/data/tablet-server-1
remote.data.dir: /tmp/fluss/remote-data" \
    -p 9125:9123 \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/fluss:$FLUSS_DOCKER_VERSION$ tabletServer
```

3. Start tablet-server-2
```bash
docker run \
    --name tablet-server-2 \
    --network=fluss-demo \
    --env FLUSS_PROPERTIES="zookeeper.address: zookeeper:2181
bind.listeners: INTERNAL://tablet-server-2:0, CLIENT://tablet-server-2:9123
advertised.listeners: CLIENT://localhost:9126
internal.listener.name: INTERNAL
tablet-server.id: 2
kv.snapshot.interval: 0s
data.dir: /tmp/fluss/data/tablet-server-2
remote.data.dir: /tmp/fluss/remote-data" \
    -p 9126:9123 \
    --volume shared-tmpfs:/tmp/fluss \
    -d fluss/fluss:$FLUSS_DOCKER_VERSION$ tabletServer
```

Now all the Fluss related components are running.

Run the below command to check the Fluss cluster status:

```bash
docker container ls -a
```

## Deploy with Docker Compose

The following is a brief overview of how to quickly create a complete Fluss testing cluster
using the `docker compose up -d` commands in a detached mode.

### Create docker-compose.yml file

#### Compose file to start Fluss cluster with one TabletServer
You can use the following `docker-compose.yml` file to start a Fluss cluster with one `CoordinatorServer` and one `TabletServer`.

```yaml
services:
  coordinator-server:
    image: fluss/fluss:$FLUSS_DOCKER_VERSION$
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123
        advertised.listeners: CLIENT://localhost:9123
        internal.listener.name: INTERNAL
        remote.data.dir: /tmp/fluss/remote-data
    ports:
      - "9123:9123"
  tablet-server:
    image: fluss/fluss:$FLUSS_DOCKER_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://tablet-server:0, CLIENT://tablet-server:9123
        advertised.listeners: CLIENT://localhost:9124
        internal.listener.name: INTERNAL
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
    ports:
        - "9124:9123"
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
    image: fluss/fluss:$FLUSS_DOCKER_VERSION$
    command: coordinatorServer
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://coordinator-server:0, CLIENT://coordinator-server:9123
        advertised.listeners: CLIENT://localhost:9123
        internal.listener.name: INTERNAL
        remote.data.dir: /tmp/fluss/remote-data
    ports:
      - "9123:9123"
  tablet-server-0:
    image: fluss/fluss:$FLUSS_DOCKER_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://tablet-server-0:0, CLIENT://tablet-server-0:9123
        advertised.listeners: CLIENT://localhost:9124
        internal.listener.name: INTERNAL
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data/tablet-server-0
        remote.data.dir: /tmp/fluss/remote-data
    ports:
      - "9124:9123"
    volumes:
      - shared-tmpfs:/tmp/fluss
  tablet-server-1:
    image: fluss/fluss:$FLUSS_DOCKER_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://tablet-server-1:0, CLIENT://tablet-server-1:9123
        advertised.listeners: CLIENT://localhost:9125
        internal.listener.name: INTERNAL
        tablet-server.id: 1
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data/tablet-server-1
        remote.data.dir: /tmp/fluss/remote-data
    ports:
      - "9125:9123"
    volumes:
      - shared-tmpfs:/tmp/fluss
  tablet-server-2:
    image: fluss/fluss:$FLUSS_DOCKER_VERSION$
    command: tabletServer
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: INTERNAL://tablet-server-2:0, CLIENT://tablet-server-2:9123
        advertised.listeners: CLIENT://localhost:9126
        internal.listener.name: INTERNAL
        tablet-server.id: 2
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data/tablet-server-2
        remote.data.dir: /tmp/fluss/remote-data
    ports:
      - "9126:9123"
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

Save the `docker-compose.yml` script and execute the `docker compose up -d` command in the same directory
to create the cluster.

Run the below command to check the container status:

```bash
docker container ls -a
```

## Interacting with Fluss

After the Fluss cluster is started, you can use **Fluss Client** (e.g., Flink SQL Client) to interact with Fluss.
The following subsections will show you how to use 'Docker' to build a Flink cluster and use **Flink SQL Client**
to interact with Fluss.

#### Start Flink Cluster
You can start a Flink standalone cluster refer to [Flink Environment Preparation](engine-flink/getting-started.md#preparation-when-using-flink-sql-client)

**Note**: Make sure the [Fluss connector jar](/downloads/) already has copied to the `lib` directory of your Flink home.
```shell
bin/start-cluster.sh 
```


### Enter into SQL-Client
Use the following command to enter the Flink SQL CLI Container:
```shell
bin/sql-client.sh
```

### Create Fluss Catalog

Use the following SQL to create a Fluss catalog:
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
```

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

### Do more with Fluss

After the catalog is created, you can use Flink SQL Client to do more with Fluss, for example, create a table, insert data, query data, etc.
More details please refer to [Flink Getting started](engine-flink/getting-started.md)

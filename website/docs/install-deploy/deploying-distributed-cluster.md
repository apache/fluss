---
title: "Deploying Distributed Cluster"
sidebar_position: 3
---

# Deploying Distributed Cluster

This page provides instructions on how to deploy a *distributed cluster* for Fluss on bare machines.


## Requirements

### Hardware Requirements

Fluss runs on all *UNIX-like environments*, e.g. **Linux**, **Mac OS X**.
To build a distributed cluster, you need to have at least two nodes.
This doc provides a simple example of how to deploy a distributed cluster on four nodes.

### Software Requirements

Before you start to set up the system, make sure you have installed **Java 11** or higher **on each node** in your cluster.
Java 8 is not supported as the released binary is compiled with Java 11.
While Fluss can run on Java 11, **Java 17 or higher is strongly recommended** for better performance.

Additionally, you need a running **ZooKeeper** cluster with version 3.6.0 or higher. 
We do not recommend to use ZooKeeper versions below 3.6.0.
For further information how to deploy a distributed ZooKeeper cluster, see [Running Replicated ZooKeeper](https://zookeeper.apache.org/doc/r3.6.0/zookeeperStarted.html#sc_RunningReplicatedZooKeeper).

If your cluster does not fulfill these software requirements, you will need to install/upgrade them.

### `JAVA_HOME` Configuration

Fluss requires the `JAVA_HOME` environment variable to be set on all nodes and point to the directory of your Java installation.

## Fluss Setup

This part will describe how to set up a Fluss cluster consisting of one CoordinatorServer and multiple TabletServers
across four machines. Suppose you have four nodes in a `192.168.10/24` subnet with the following IP address assignment:
- Node0: `192.168.10.100`
- Node1: `192.168.10.101`
- Node2: `192.168.10.102`
- Node3: `192.168.10.103`

Node0 will deploy a CoordinatorServer instance. Node1, Node2 and Node3 will deploy one TabletServer instance, respectively.

### Preparation

1. Make sure ZooKeeper has been deployed. We assume that ZooKeeper listens on `192.168.10.199:2181`.

2. Download Fluss


Go to the [downloads page](/downloads) and download the latest Fluss release. After downloading the latest release, copy the archive to all the nodes and extract it:

```shell
tar -xzf fluss-$FLUSS_VERSION$-bin.tgz
cd fluss-$FLUSS_VERSION$/
```

### Configuring Fluss

After having extracted the archived files, you need to configure Fluss for a distributed deployment.
We will use the _default config file_ (`conf/server.yaml`) to configure Fluss.
Adapt the `server.yaml` on each node as follows.

**Node0**

```yaml title="server.yaml"
# coordinator server
bind.listeners: FLUSS://192.168.10.100:9123

zookeeper.address: 192.168.10.199:2181
zookeeper.path.root: /fluss

# When running in distributed mode, be sure to point to a remote path—
# e.g. oss://bucket/path for OSS or hdfs://namenode:port/path for HDFS.
# Otherwise, queries will fail with a “No such file or directory” error.
remote.data.dir: hdfs://namenode:port/tmp/fluss-remote-data
```

**Node1**

```yaml title="server.yaml"
# tablet server
bind.listeners: FLUSS://192.168.10.101:9123 # alternatively, setting the port to 0 assigns a random port
tablet-server.id: 1

zookeeper.address: 192.168.10.199:2181
zookeeper.path.root: /fluss

# When running in distributed mode, be sure to point to a remote path—
# e.g. oss://bucket/path for OSS or hdfs://namenode:port/path for HDFS.
# Otherwise, queries will fail with a “No such file or directory” error.
remote.data.dir: hdfs://namenode:port/tmp/fluss-remote-data
```

**Node2**

```yaml title="server.yaml"
# tablet server
bind.listeners: FLUSS://192.168.10.102:9123 # alternatively, setting the port to 0 assigns a random port
tablet-server.id: 2

zookeeper.address: 192.168.10.199:2181
zookeeper.path.root: /fluss

# When running in distributed mode, be sure to point to a remote path—
# e.g. oss://bucket/path for OSS or hdfs://namenode:port/path for HDFS.
# Otherwise, queries will fail with a “No such file or directory” error.
remote.data.dir: hdfs://namenode:port/tmp/fluss-remote-data
```

**Node3**
```yaml title="server.yaml"
# tablet server
bind.listeners: FLUSS://192.168.10.103:9123 # alternatively, setting the port to 0 assigns a random port
tablet-server.id: 3

zookeeper.address: 192.168.10.199:2181
zookeeper.path.root: /fluss

# When running in distributed mode, be sure to point to a remote path—
# e.g. oss://bucket/path for OSS or hdfs://namenode:port/path for HDFS.
# Otherwise, queries will fail with a “No such file or directory” error.
remote.data.dir: hdfs://namenode:port/tmp/fluss-remote-data
```

:::note
- `tablet-server.id` is the unique id of the TabletServer. If you have multiple TabletServers, you should set a different id for each TabletServer.
- In this example, we only set the mandatory properties. For additional properties, you can refer to [Configuration](maintenance/configuration.md) for more details.
  :::

### Starting Fluss

To deploy a distributed Fluss cluster, you should first start a CoordinatorServer instance on **Node0**. 
Then, start a TabletServer instance on **Node1**, **Node2**, and **Node3**, respectively.

**CoordinatorServer**

On **Node0**, start a CoordinatorServer as follows.
```shell
./bin/coordinator-server.sh start
```

**TabletServer**

On **Node1**, **Node2** and **Node3**, start a TabletServer as follows.
```shell
./bin/tablet-server.sh start
```

After that, you have successfully deployed a distributed Fluss cluster.

## Fluss CoordinatorServer High Availability (HA) Setup

By default, the distributed cluster example above deploys a single CoordinatorServer on Node0,
which is a single point of failure for admin operations (e.g., creating databases/tables).
Fluss supports built-in high availability (HA) for the CoordinatorServer based on ZooKeeper
leader election, requiring no additional configuration options.

### How it works

When multiple CoordinatorServer instances point to the same ZooKeeper cluster, they participate
in leader election. Exactly one is elected as the **leader** and serves the full coordinator
services; the others run as **standby**. If the leader fails, a standby is promoted and
increments the coordinator epoch to fence off the old leader.

:::note
During failover, admin operations (e.g., creating/dropping tables) are temporarily unavailable.
Data reads and writes are not affected if no tablet server fails. The time to elect a new
leader is bounded by `zookeeper.client.session-timeout` (default 60 seconds).
:::

### Configuring CoordinatorServer HA

To enable HA, start at least two CoordinatorServer instances on different nodes, all configured
with the same `zookeeper.address` and `zookeeper.path.root`. No extra configuration options are
needed beyond what a distributed deployment already requires.

Extending the four-node example above, add a second CoordinatorServer on a new node:

**Node4**

```yaml title="server.yaml"
# coordinator server
bind.listeners: FLUSS://192.168.10.104:9123

zookeeper.address: 192.168.10.199:2181
zookeeper.path.root: /fluss

# When running in distributed mode, be sure to point to a remote path—
# e.g. oss://bucket/path for OSS or hdfs://namenode:port/path for HDFS.
# Otherwise, queries will fail with a “No such file or directory” error.
remote.data.dir: hdfs://namenode:port/tmp/fluss-remote-data
```

### Starting CoordinatorServer HA

To deploy a Fluss cluster with CoordinatorServer HA, you should first start a CoordinatorServer instance on **Node0**.
Then, start the second CoordinatorServer instance on **Node4**.

**CoordinatorServer**

On **Node0** and **Node4**, start a CoordinatorServer as follows.

```shell
./bin/coordinator-server.sh start
```

After that, you have successfully deployed a distributed Fluss cluster with CoordinatorServer HA.


## Interacting with Fluss

After the Fluss cluster is started, you can use **Fluss Client** (e.g., Flink SQL Client) to interact with Fluss.
The following subsections will show you how to use Flink SQL Client to interact with Fluss.

### Flink SQL Client

Using Flink SQL Client to interact with Fluss.

#### Preparation

You can start a Flink standalone cluster refer to [Flink Environment Preparation](engine-flink/getting-started.md#preparation-when-using-flink-sql-client)

**Note**: Make sure the [Fluss Flink connector jar](../engine-flink/getting-started.md#dependencies) has already been copied to the `lib` directory of your Flink home.

#### Add catalog

In Flink SQL client, a catalog is created and named by executing the following query:

- Single CoordinatorServer:

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = '192.168.10.100:9123'
);
```

- CoordinatorServer HA:

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = '192.168.10.100:9123,192.168.10.104:9123'
);
```


#### Do more with Fluss

After the catalog is created, you can use Flink SQL Client to do more with Fluss, for example, create a table, insert data, query data, etc.
More details please refer to [Flink Getting Started](engine-flink/getting-started.md).

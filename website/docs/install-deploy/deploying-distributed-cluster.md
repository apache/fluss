---
title: "Deploying Distributed Cluster"
sidebar_position: 3
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

# Deploying Distributed Cluster

This page provides instructions on how to deploy a *distributed cluster* for Fluss on bare machines.


## Requirements

### Hardware Requirements

Fluss runs on all *UNIX-like environments*, e.g. **Linux**, **Mac OS X**.
To build a distributed cluster, you need to have at least two nodes.
This doc provides a simple example of how to deploy a distributed cluster on four nodes.

### Software Requirements

Before you start to set up the system, make sure you have installed **Java 17** or higher **on each node** in your cluster. 
Java 8 and Java 11 are not recommended.

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

remote.data.dir: /tmp/fluss-remote-data
```

**Node1**

```yaml title="server.yaml"
# tablet server
bind.listeners: FLUSS://192.168.10.101:9123 # alternatively, setting the port to 0 assigns a random port
tablet-server.id: 1

zookeeper.address: 192.168.10.199:2181
zookeeper.path.root: /fluss

remote.data.dir: /tmp/fluss-remote-data
```

**Node2**

```yaml title="server.yaml"
# tablet server
bind.listeners: FLUSS://192.168.10.102:9123 # alternatively, setting the port to 0 assigns a random port
tablet-server.id: 2

zookeeper.address: 192.168.10.199:2181
zookeeper.path.root: /fluss

remote.data.dir: /tmp/fluss-remote-data
```

**Node3**
```yaml title="server.yaml"
# tablet server
bind.listeners: FLUSS://192.168.10.103:9123 # alternatively, setting the port to 0 assigns a random port
tablet-server.id: 3

zookeeper.address: 192.168.10.199:2181
zookeeper.path.root: /fluss

remote.data.dir: /tmp/fluss-remote-data
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

## Interacting with Fluss

After the Fluss cluster is started, you can use **Fluss Client** (e.g., Flink SQL Client) to interact with Fluss.
The following subsections will show you how to use Flink SQL Client to interact with Fluss.

### Flink SQL Client

Using Flink SQL Client to interact with Fluss.

#### Preparation

You can start a Flink standalone cluster refer to [Flink Environment Preparation](engine-flink/getting-started.md#preparation-when-using-flink-sql-client)

**Note**: Make sure the [Fluss connector jar](/downloads/) already has copied to the `lib` directory of your Flink home.

#### Add catalog

In Flink SQL client, a catalog is created and named by executing the following query:
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = '192.168.10.100:9123'
);
```

#### Do more with Fluss

After the catalog is created, you can use Flink SQL Client to do more with Fluss, for example, create a table, insert data, query data, etc.
More details please refer to [Flink Getting Started](engine-flink/getting-started.md).

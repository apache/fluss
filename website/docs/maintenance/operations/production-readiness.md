---
title: Production Readiness Checklist
sidebar_position: 0
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied. See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Production Readiness Checklist

This checklist outlines the deployment and configuration recommendations for running Fluss in production.

## ZooKeeper

Fluss uses ZooKeeper as its central metadata store and for cluster coordination and configuration management.
For production, we recommend ZooKeeper 3.8 or later. Use the latest patch release of a supported release line from the
[official ZooKeeper downloads page](https://zookeeper.apache.org/releases/). Follow the upstream
[deployment](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_deployment) and
[configuration](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_configuration) documentation for your
chosen version, then apply the Fluss recommendations below.

### Deployment

- **Cluster size and placement:** A single node is suitable for development and testing. For production, use 3, 5,
  or 7 voting nodes to tolerate 1, 2, or 3 node failures, respectively. Distribute nodes across physical hosts and
  failure domains so a single failure cannot take down a majority of the ensemble.
- **Resources:** Use dedicated Linux hosts or virtual machines. Treat 1 vCPU and 4 GB of memory per node as a starting
  point, and adjust resources for the metadata volume and expected workload.
- **Storage:** Use persistent data volumes separate from the operating-system disk. Place transaction logs on a
  dedicated disk, separate from snapshots, so snapshot writes do not delay transaction-log writes.

### Configuration

The following settings are recommended for deploying Fluss.

#### ZooKeeper configuration (zoo.cfg)

The following example shows the recommended ZooKeeper settings for a three-node ensemble. Adjust the server list
for your deployment. Replace `<snapshot-volume>` and `<transaction-log-volume>` with your persistent
volume mount paths, and ensure the volumes are mounted before starting ZooKeeper.
Follow the [official deployment guide](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_deployment)
for installation and each server's `myid` file.

```text title="conf/zoo.cfg"
tickTime=2000
initLimit=20
syncLimit=10

# Replace the placeholders with your persistent volume mount paths.
dataDir=<snapshot-volume>/zookeeper/data
dataLogDir=<transaction-log-volume>/zookeeper/log
clientPort=2181

4lw.commands.whitelist=srvr,stat,ruok,mntr,conf,isro

autopurge.snapRetainCount=3
autopurge.purgeInterval=1

server.1=zk1.example.com:2888:3888
server.2=zk2.example.com:2888:3888
server.3=zk3.example.com:2888:3888
```

| Setting | Description |
| --- | --- |
| `tickTime` | Base time unit for heartbeats and timeouts, in milliseconds; `2000 ms = 2 s` in this example. |
| `initLimit` | Time allowed for a follower to connect and initially synchronize with the leader, in ticks; `20 × tickTime = 40 s` in this example. |
| `syncLimit` | Time allowed for ongoing follower synchronization, in ticks; `10 × tickTime = 20 s` in this example. Followers exceeding the limit are dropped. |
| `dataDir` | Stores snapshots, plus transaction logs if `dataLogDir` is unset. Use a directory on the mounted snapshot volume. |
| `dataLogDir` | Stores transaction logs. Use a directory on a dedicated transaction-log volume. |
| `clientPort` | Port accepting client connections. |
| `4lw.commands.whitelist` | Enabled four-letter commands. `*` enables all commands, including `wchc` and `wchp`, which can be expensive with many watches. |
| `autopurge.snapRetainCount` | Number of snapshots retained with the logs needed for recovery; minimum 3. Adjust the count to your retention needs. |
| `autopurge.purgeInterval` | Time between automatic cleanup runs, in hours; `1` runs cleanup once per hour. Adjust the interval to your cleanup needs. Purging removes old disk snapshots and transaction logs; it does not trim data inside znodes. |
| `server.N` | Members identified by server ID, with the quorum port followed by the leader-election port (`2888` and `3888` in this example). Use the same membership list on every server. |

#### JVM configuration (java.env)

The official `zkServer.sh` script loads `conf/java.env`; add the JVM option there as shown below. Other deployment
methods must supply it through their own server JVM-argument configuration.

```bash title="conf/java.env"
SERVER_JVMFLAGS="$SERVER_JVMFLAGS -Djute.maxbuffer=104857600"
```

| Setting | Description |
| --- | --- |
| `jute.maxbuffer` | Maximum data size per znode, in bytes; `104857600 bytes = 100 MiB` in this example. |

:::important Increase the ZooKeeper buffer limit (jute.maxbuffer)

Fluss metadata can exceed ZooKeeper's default limit of about 1 MiB per znode, so the znode size limit needs to be
increased. Fluss has already raised the default client limit to 100 MiB through
[zookeeper.client.max-buffer-size](../configuration.md#zookeeper). The ZooKeeper servers should therefore be
configured with `jute.maxbuffer=104857600` (100 MiB) to match the client limit.

:::

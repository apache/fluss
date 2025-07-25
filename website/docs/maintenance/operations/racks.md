---
title: Racks
sidebar_position: 2
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Balancing Replicas Across Racks

The rack awareness feature is designed to distribute replicas of the same bucket across multiple racks. This extends the 
data protection guarantees provided by Fluss beyond server failures to include rack failures, thereby significantly 
reducing the risk of data loss in the event that all TabletServers on a single rack fail simultaneously.

To specify that a TabletServer belongs to a particular rack, you can set the `tablet-server.rack` configuration option:

```yaml title="conf/server.yaml"
tablet-server.rack: RACK1
```

:::note
1. If rack awareness is enabled, the `tablet-server.rack` setting must be configured for each TabletServer. Failure to do so will prevent Fluss from starting and will result in an exception being thrown.
:::

When a table is created, the rack constraint is honored, ensuring that replicas are spread across as many racks as possible. 
Specifically, a bucket will span the minimum of the number of available racks and the `table.replication.factor` (i.e., `min(#racks, table.replication.factor)`).
This approach maximizes the distribution of replicas across racks.

The algorithm used to assign replicas to TabletServers ensures that the number of leader replicas per TabletServer 
remains consistent, regardless of how TabletServers are distributed across racks. This helps maintain balanced throughput 
across the system.

However, if racks are assigned different numbers of TabletServers, the distribution of replicas will not be even. Racks 
with fewer TabletServers will receive more replicas, leading to higher storage usage and increased resource allocation 
for replication. Therefore, it is highly recommended to configure an equal number of TabletServers per rack to ensure 
optimal resource utilization and balanced workload distribution.
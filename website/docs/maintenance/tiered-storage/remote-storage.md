---
title: "Remote Storage"
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

# Remote Storage

Remote storage usually means a cost-efficient and fault-tolerant storage comparing to local disk, such as S3, HDFS, OSS.
See more detail about how to configure remote storage in documentation of [filesystems](maintenance/filesystems/overview.md).

For log table, Fluss will use remote storage to store the tiered log segments of data. For primary key table, Fluss will use remote storage to store the snapshot as well as the tiered log segments for change log.

## Remote Log

As a streaming storage, Fluss data is mostly consumed in a streaming fashion using tail reads. To achieve low
latency for tail reads, Fluss will store recent data in local disk. But for older data, to reduce local disk cost,
Fluss will move data from local to remote storage, such as S3, HDFS or OSS asynchronously.

### Cluster configurations about remote log

By default, Fluss will copy local log segments to remote storage in every 1 minute. The interval is controlled by configuration `remote.log.task-interval-duration`.
If you don't want to copy log segments to remote storage, you can set `remote.log.task-interval-duration` to 0.

Below is the list for all configurations to control the log segments tiered behavior in cluster level:

| Configuration                       | type       | Default | Description                                                                                                                                                                                                                 |
|-------------------------------------|------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| remote.log.task-interval-duration   | Duration   | 1min    | Interval at which remote log manager runs the scheduled tasks like copy segments, clean up remote log segments, delete local log segments etc. If the value is set to 0s, it means that the remote log storage is disabled. |
| remote.log.index-file-cache-size    | MemorySize | 1gb     | The total size of the space allocated to store index files fetched from remote storage in the local storage.                                                                                                                |
| remote.log-manager.thread-pool-size | Integer    | 4       | Size of the thread pool used in scheduling tasks to copy segments, fetch remote log indexes and clean up remote log segments.                                                                                               |
| remote.log.data-transfer-thread-num | Integer    | 4       | The number of threads the server uses to transfer (download and upload) remote log file can be  data file, index file and remote log metadata file.                                                                         |


### Table configurations about remote log

When local log segments are copied to remote storage, the local log segments will be deleted to reduce local disk cost.
But sometimes, we want to keep the several latest log segments retain in local, although they have been coped to remote storage for better read performance.
You can control how many log segments to retain in local by setting the configuration `table.log.tiered.local-segments`(default is 2) per table.

## Remote snapshot of primary key table

In Fluss, one primary key table is distributed to multiple buckets. For each bucket of primary key table, Fluss will only always keep one replica in local disk without any follower replicas.

So, for fault tolerance of local disk fail forever, Fluss will do snapshots to the replicas of primary key table periodically and upload the snapshots to remote storage.
The snapshot will keep a log offset representing the next unread change log while doing the snapshot. Then, when the machine holding the replica fails, Fluss can recover the replica in other live machines by downloading the snapshot from remote storage and apply the change log
since last snapshot.

What's more, with the snapshot and the consistent log offset, Fluss client can seamlessly switch from full reading phase(reading snapshot) to the incremental
phase (subscribe change log from the consistent log offset) without any data duplication or loss.

### Cluster configurations about remote snapshot

Below is the list for all configurations to control the snapshot behavior in cluster level:

| Configuration                    | type     | Default | Description                                                                                                                                                                                         |
|----------------------------------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| kv.snapshot.interval             | Duration | 10min   | The interval to perform periodic snapshot for kv data.                                                                                                                                              |
| kv.snapshot.scheduler-thread-num | Integer  | 1       | The number of threads that the server uses to schedule snapshot kv data for all the replicas in the server.                                                                                         |
| kv.snapshot.transfer-thread-num  | Integer  | 4       | The number of threads the server uses to transfer (download and upload) kv snapshot files.                                                                                                          |
| kv.snapshot.num-retained         | Integer  | 1       | The maximum number of completed snapshots to retain. It's recommended to set it to a larger value to avoid the case that server delete the snapshot while the client is still reading the snapshot. |

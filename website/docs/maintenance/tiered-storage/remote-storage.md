---
title: "Remote Storage"
sidebar_position: 2
---

# Remote Storage

Remote storage usually means a cost-efficient and fault-tolerant storage compared to local disk, such as S3, HDFS, OSS.
See more details about configuring remote storage in the [filesystems documentation](maintenance/filesystems/overview.md).

For log tables, Fluss uses remote storage to store tiered log segments. For primary key tables, Fluss uses remote storage to store both snapshots and tiered log segments for the change log.

## Remote Log

As a streaming storage, Fluss data is mostly consumed in a streaming fashion using tail reads. To achieve low
latency for tail reads, Fluss will store recent data in local disk. But for older data, to reduce local disk cost,
Fluss will move data from local to remote storage, such as S3, HDFS or OSS asynchronously.

### Cluster configurations about remote log

By default, Fluss will copy local log segments to remote storage every minute. The interval is controlled by the `remote.log.task-interval-duration` configuration.
If you don't want to copy log segments to remote storage, you can set `remote.log.task-interval-duration` to 0.

Below is the list of all configurations to control the log segments tiered behavior at the cluster level:

| Configuration                       | Type       | Default | Description                                                                                                                                                                                                                 |
|-------------------------------------|------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| remote.log.task-interval-duration   | Duration   | 1min    | Interval at which remote log manager runs the scheduled tasks like copy segments, clean up remote log segments, delete local log segments etc. If the value is set to 0, it means that the remote log storage is disabled. |
| remote.log.index-file-cache-size    | MemorySize | 1gb     | The total size of the space allocated to store index files fetched from remote storage in the local storage.                                                                                                                |
| remote.log-manager.thread-pool-size | Integer    | 4       | Size of the thread pool used in scheduling tasks to copy segments, fetch remote log indexes and clean up remote log segments.                                                                                               |
| remote.log.data-transfer-thread-num | Integer    | 4       | The number of threads the server uses to transfer (download and upload) remote log files, which can include data files, index files, and remote log metadata files.                                                          |


### Table configurations about remote log

When local log segments are copied to remote storage, the local log segments will be deleted to reduce local disk cost.
However, you may want to keep several of the latest log segments in local storage, even though they have been copied to remote storage, for better read performance.
You can control how many log segments to retain locally by setting the configuration `table.log.tiered.local-segments` (default is 2) per table.

## Remote snapshot of primary key table

In Fluss, a primary key table is distributed across multiple buckets. For each bucket of a primary key table, Fluss maintains only one replica on local disk without any follower replicas.

For fault tolerance against permanent local disk failures, Fluss periodically creates snapshots of primary key table replicas and uploads them to remote storage. Each snapshot records a log offset representing the next unread change log entry at the time of snapshot creation. When a machine hosting a replica fails, Fluss can recover the replica on other available machines by downloading the snapshot from remote storage and applying the change log entries since the last snapshot.

Additionally, with the snapshot and consistent log offset, Fluss clients can seamlessly transition from the full reading phase (reading snapshot) to the incremental phase (subscribing to change log from the consistent log offset) without data duplication or loss.

### Cluster configurations about remote snapshot

Below is the list of all configurations to control the snapshot behavior at the cluster level:

| Configuration                    | Type     | Default | Description                                                                                                                                                                                         |
|----------------------------------|----------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| kv.snapshot.interval             | Duration | 10min   | The interval to perform periodic snapshot for kv data.                                                                                                                                              |
| kv.snapshot.scheduler-thread-num | Integer  | 1       | The number of threads that the server uses to schedule snapshot kv data for all the replicas in the server.                                                                                         |
| kv.snapshot.transfer-thread-num  | Integer  | 4       | The number of threads the server uses to transfer (download and upload) kv snapshot files.                                                                                                          |
| kv.snapshot.num-retained         | Integer  | 1       | The maximum number of completed snapshots to retain. It's recommended to set it to a larger value to avoid the case that server deletes the snapshot while the client is still reading the snapshot. |

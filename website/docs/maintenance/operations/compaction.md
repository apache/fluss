---
title: Manual Compaction
sidebar_position: 3
---

# Manual Compaction

Manual compaction maintains the RocksDB SST files of primary-key tables. It operates on a partition, a table, a database, or the cluster. Submission fixes the table IDs, partition IDs, and buckets in the operation; later tables and partitions are not added. Log tables are excluded. Empty scopes and unsupported primary-key targets are rejected without partial acceptance.

## Compaction types

| Type | Purpose |
| --- | --- |
| `OPTIMIZE` | Compact the LSM tree to reduce read amplification, using the existing compaction filter policy for the bottommost level. |
| `RECLAIM` | Force bottommost-level processing of obsolete versions, deletion markers, and expired data. The amount of space reclaimed is not guaranteed. |
| `MIGRATE` | Rewrite existing SST files to the supported index layout bound from cluster configuration at submission. Files already matching the target skip rewriting. |

Every type creates and commits a new KV snapshot, including a `MIGRATE` that skips rewriting. A bucket counts as successful only after snapshot commitment is confirmed, local execution resources have exited, and the Coordinator has published its completion record. SST replacement alone does not imply success or immediate disk-space reclamation. Snapshot retention and shared SST cleanup follow the existing snapshot store policy.

The supported targets are ordinary primary-key tables using LEVEL compaction and the existing KV snapshot recovery path. Historical partitions and unsupported compaction strategies are rejected. TTL and compaction filters retain their existing semantics; compaction does not produce new changelog DELETE records.

## Submit an operation

Use the Java Admin API:

```java
import org.apache.fluss.cluster.compaction.CompactionType;
import org.apache.fluss.metadata.TablePath;

// Select one scope for the operation.
String operationId = admin.compact(
        TablePath.of("my_database", "my_table"), CompactionType.OPTIMIZE).get();

// Other supported overloads:
// admin.compact(CompactionType.RECLAIM);
// admin.compact("my_database", CompactionType.RECLAIM);
// admin.compact(TablePath.of("my_database", "my_table"), "partition-name",
//         CompactionType.MIGRATE);
```

With a Fluss catalog selected in Flink SQL, the corresponding procedures are:

```sql
CALL sys.compact('OPTIMIZE');
CALL sys.compact('my_database', 'RECLAIM');
CALL sys.compact('my_database', 'my_table', 'OPTIMIZE');
CALL sys.compact('my_database', 'my_table', 'partition-name', 'MIGRATE');
```

Each call returns an `operation_id`. Submit one operation at a time. For partition scope, pass the partition name returned by the catalog/Admin partition listing, including its composite-key encoding when applicable. A missing partition or partition scope on a non-partitioned table is rejected. Nulls and wildcards do not select scopes.

Only one operation may be active in a cluster. An identical active request, including its scope, type, configuration baseline, and resolved target partitions, returns the existing operation ID and retains its original bucket plan. Another request receives `COMPACTION_IN_PROGRESS` with the active operation ID. A completed operation is never reused by a later submission.

Each TabletServer runs at most one bucket at a time. The plan is ordered by table ID, partition ID, and bucket ID; each server selects its lowest ordered pending bucket for which it is currently leader. A server advances to its next bucket on a subsequent control cycle after the previous success is durably published. Servers progress independently, so there is no cluster-wide completion order. TabletServers do not maintain a waiting queue. An occupied server slot is retried on later control cycles until it becomes available or the operation deadline expires. An unavailable leader, incompatible configuration, disk protection, write backpressure, or explicit execution failure fails the operation.

## Migrate the SST index layout

`MIGRATE` supports the layout selected by `kv.rocksdb.block.partitioned-index-filters`. Change the cluster configuration through the normal configuration workflow and reopen the affected databases according to the configuration's existing activation requirements before submitting the operation. Changing a setting does not itself rewrite existing SST files.

The operation binds the cluster configuration at submission and checks that each opened database uses that baseline. It does not accept per-request SST configuration overrides. Configuration drift, an unopened target layout, or unrecognized SST properties fails the operation. All live SST properties are checked after rewriting. A matching database skips native rewriting but still commits a fresh snapshot, even if the covered log offset has not advanced.

## Query progress

```java
import org.apache.fluss.cluster.compaction.CompactionProgress;
import org.apache.fluss.cluster.compaction.CompactionStatus;

CompactionProgress progress = admin.getCompaction(operationId).get();
// Completion requires progress.info().status() == CompactionStatus.SUCCEEDED.
// Bucket completion percentage: 100.0 * progress.succeeded() / progress.total().
admin.listCompactions();
admin.listCompactions("my_database");
admin.listCompactions(TablePath.of("my_database", "my_table"));
admin.listCompactions(TablePath.of("my_database", "my_table"), "partition-name");
```

```sql
CALL sys.get_compaction('operation-id');
CALL sys.list_compactions();
CALL sys.list_compactions('my_database');
CALL sys.list_compactions('my_database', 'my_table');
CALL sys.list_compactions('my_database', 'my_table', 'partition-name');
```

States are `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, and `CANCELLED`. Progress includes `total`, `pending`, `running`, `succeeded`, `failed`, and `cancelled`; the five state counts sum to the fixed, positive total. It also includes the first error, last progress time, observation time, persistence time, and at most one current execution per server with its bucket, leader epoch, phase, phase start time, snapshot ID, and reason. SQL exposes the current executions as JSON. Pending buckets have no current execution; running buckets include work awaiting admission, observation, or success publication. Heartbeats do not advance the last progress time.

The completion percentage measures buckets, not bytes or estimated remaining time. Queries return summaries and current executions, not all completed bucket details. Lists are newest first and include operations whose scope contains or is contained by the queried scope. The fixed submission plan determines which buckets are actually processed. Completed operations are queryable for seven days; expired or unknown IDs return an explicit error, even if physical metadata deletion is delayed.

## Cancellation and failure

```java
admin.cancelCompaction(operationId).get();
```

```sql
CALL sys.cancel_compaction('operation-id');
```

Cancellation stops scheduling and durably publishes `CANCELLED` before requesting local cancellation. Repeated cancellation, including cancellation of a terminal operation, is a no-op. Already published successes remain successful; other buckets count as cancelled. A failed operation retains published successes and counts every remaining bucket as failed. Terminal status and counters are immutable.

Cancellation is best effort for native compaction. A terminal operation does not prove that every native call has returned. A TabletServer slot stays occupied until its worker, snapshot users, and cleanup have actually exited; another operation waits through subsequent control cycles for that occupied slot, subject to its own deadline. Cancellation does not roll back rewritten SST files or invalidate a legitimately committed snapshot. Foreground reads and writes can continue during native compaction, but RocksDB backpressure and the existing checkpoint capture critical section can still delay them.

Each operation has one absolute 24-hour deadline that survives Coordinator recovery. Scheduling is at least once: the immutable bucket plan and published bucket/snapshot successes are durable, while dispatch history is not persisted. Recovery retains published successes and returns every remaining bucket to pending. A bucket whose completion was not published may run again, including after leader movement, but contributes at most one logical success. Transient network errors and occupied slots are retried on later control cycles; leader changes reschedule pending work against the current leader. Coordinator and leader epochs fence stale execution and callbacks. The original deadline limits scheduling and execution eligibility; it does not guarantee that a blocking native or filesystem call has physically exited at that instant.

A lost snapshot commit response is checked through the existing snapshot commitment path using the known snapshot ID. If commitment cannot be confirmed, the operation fails without claiming that the snapshot was never committed and without deleting a potentially committed snapshot. After a client timeout, query or list operations before submitting again. A new submission after a terminal result creates a separate operation.

If remote plan or progress metadata cannot be read while ZooKeeper remains writable, the durable plan summary and success counters allow a failed terminal result and release of the active operation. If ZooKeeper itself is unavailable, the API can report an error without a durably published terminal state; admission resumes only after recovery converges the outstanding operation. Inspect the first error and current execution reason before retrying maintenance.

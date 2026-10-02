---
title: "Bucketing"
sidebar_position: 1
---

# Bucketing

A bucketing strategy is a data distribution technique that divides table data into small pieces 
and distributes the data to multiple hosts and services.

When creating a Fluss table, you can specify the number of buckets by setting `'bucket.num' = '<num>'` property for the table, see more details in [DDL](engine-flink/ddl.md). For partitioned tables, `bucket.num` can be altered via `ALTER TABLE SET ('bucket.num' = '<num>')` — the new value applies to newly created partitions while existing partitions retain their original bucket count. See [Rescaling Bucket Count for Future Partitions](#rescaling-bucket-count-for-future-partitions) for the full semantics, examples, and constraints.
Currently, Fluss supports 3 bucketing strategies: **Hash Bucketing**, **Sticky Bucketing** and **Round-Robin Bucketing**.
Primary-Key Tables only allow to use **Hash Bucketing**. Log Tables use **Sticky Bucketing** by default but can use other two bucketing strategies.

## Hash Bucketing
**Hash Bucketing** is common in OLAP scenarios.
The advantage is that it can be very evenly distributed to multiple nodes, making full use of the capabilities of distributed computing, and has excellent
scalability (rescale buckets or clusters) to cope with massive data.

**Usage**: setting `'bucket.key' = 'col1, col2'` property for the table to specify the bucket key for hash bucketing.
Primary-Key Tables use primary key (excluding partition key) as the bucket key by default.

## Sticky Bucketing

**Sticky Bucketing** enables larger batches and reduces latency when writing records into Log Tables. After sending a batch, the sticky bucket changes. Over time, the records are spread out evenly among all the buckets.
Sticky Bucketing is the default bucketing strategy for Log Tables. This is quite important because Log Tables uses Apache Arrow as the underling data format which is efficient for large batches.

**Usage**: setting `'client.writer.bucket.no-key-assigner'='sticky'` property for the table to enable this strategy. PrimaryKey Tables do not support this strategy.

## Round-Robin Bucketing

**Round-Robin Bucketing** is a simple strategy that randomly selects a bucket for each record before writing it in. This strategy is suitable for scenarios where the data distribution is relatively uniform and the data is not skewed.

**Usage**: setting `'client.writer.bucket.no-key-assigner'='round_robin'` property for the table to enable this strategy. PrimaryKey Tables do not support this strategy.

## Rescaling Bucket Count for Future Partitions

Since Fluss 1.0, the bucket count of a partitioned table can be changed at runtime:

```sql title="Flink SQL"
ALTER TABLE my_part_table SET ('bucket.num' = '8');
```

### Semantics

For a partitioned table, `bucket.num` is the **default bucket count for partitions to be created**: every partition takes the current table-level `bucket.num` as its own bucket count at the moment it is created, and keeps that count for its entire lifetime. Changing `bucket.num` therefore:

- applies only to partitions created after the change — each of them is created with the new count;
- never touches existing partitions — they keep their original bucket count, **without any data redistribution or lake-file rewrite**;
- does not affect partitions that were already pre-created for future dates (including partitions pre-created by auto-partitioning): a pre-created partition is an existing partition and keeps the count it was created with.

Both increasing and decreasing the count are supported. Because a partition's bucket count is immutable, this statement only changes what future partitions will look like — the layout of existing data can never be changed by it.

This applies uniformly to all three partition creation strategies (see [Partitioning](partitioning.md)):

- **explicitly created partitions** (`ALTER TABLE ... ADD PARTITION`) take the table-level `bucket.num` that is current when the `ADD PARTITION` statement runs;
- **auto-created partitions** take the value current when auto-partitioning pre-creates them (e.g. on its periodic check for the next day's partition) — not the value current at the partition's data time;
- **dynamically created partitions** take the value current when the first record of that partition triggers the creation on the write path.

In all three cases what matters is the creation moment, not the partition's data time: a partition is stamped once and keeps its count forever.

### Example

The following walkthrough creates a partitioned log table, rescales it from 2 to 4 buckets, and shows that old and new partitions keep their own counts.

```sql title="Flink SQL"
-- A partitioned table whose partitions are created with 2 buckets by default
CREATE TABLE order_events (
  dt STRING,
  order_id BIGINT,
  amount INT
) PARTITIONED BY (dt) WITH (
  'bucket.num' = '2'
);

-- This partition is created with 2 buckets and keeps them forever
ALTER TABLE order_events ADD PARTITION (dt = '2026-01-01');
INSERT INTO order_events VALUES ('2026-01-01', 1001, 50), ('2026-01-01', 1002, 75);

-- Rescale: partitions created from now on will have 4 buckets.
-- The partition 2026-01-01 is NOT changed and still has 2 buckets.
ALTER TABLE order_events SET ('bucket.num' = '4');

-- This partition is created with 4 buckets
ALTER TABLE order_events ADD PARTITION (dt = '2026-02-01');
INSERT INTO order_events VALUES ('2026-02-01', 2001, 30);

-- Decreasing works the same way: future partitions get 3 buckets from now on
ALTER TABLE order_events SET ('bucket.num' = '3');

-- Writes to existing partitions keep going to each partition's own buckets,
-- no matter what the current default is: this row lands in the 2 buckets
-- of 2026-01-01 ...
INSERT INTO order_events VALUES ('2026-01-01', 1003, 60);
-- ... and this row lands in the 4 buckets of 2026-02-01. Neither matches
-- the current default of 3
INSERT INTO order_events VALUES ('2026-02-01', 2002, 40);

-- Reading across old and new partitions works without any special handling:
-- each partition is read through its own bucket range
SELECT * FROM order_events;
```

The `SELECT` returns all five rows from the mixed-bucket-count table — three from the 2-bucket partition `2026-01-01` and two from the 4-bucket partition `2026-02-01`. Note the two `INSERT`s above: both partitions receive writes although neither matches the current table-level default of 3, and each write is routed by the target partition's own bucket count. No special handling is needed for mixed bucket counts, on either the write or the read path.

### Inspecting bucket counts

`SHOW CREATE TABLE` returns the current table-level `bucket.num`, i.e. the default for future partitions:

```sql title="Flink SQL"
SHOW CREATE TABLE order_events;
```

To inspect the actual bucket count of each partition, use the [`sys.list_partition_infos`](engine-flink/procedures.md#list_partition_infos) procedure:

```sql title="Flink SQL"
CALL sys.list_partition_infos('my_db', 'order_events');
```

It returns one row per partition with the partition id, the partition name, and the partition's actual bucket count. For the table above after the rescale, the result looks like:

```sql
+I[1001, 2026-01-01, 2]
+I[1002, 2026-02-01, 4]
```

The same information is also available through the `Admin#listPartitionInfos` Java API for clients not using Flink SQL.

### What it is not

- **It is not a redistribution of existing partitions.** Existing partitions keep their layout; redistributing the data of an existing partition is not supported.
- **It is not a change of Flink job parallelism.** The bucket count and the parallelism of Flink jobs reading or writing the table are independent of each other.

### Constraints

- Only **partitioned tables** are supported. Altering `bucket.num` on non-partitioned tables is rejected; rescaling a non-partitioned table requires redistributing its data and is not supported yet.
- Rejected on tables using the aggregation merge engine (`'table.merge-engine' = 'aggregation'`): such tables restore from checkpoints via undo recovery, which relies on the bucket shuffle keeping one bucket per writer subtask, and a rescaled table breaks that guarantee for running Flink jobs.
- Rejected on tables with the historical partition enabled (`'table.datalake.historical-partition.enabled' = 'true'`).
- Among lake-enabled tables, only **Paimon** is supported; the new count is also propagated to the Paimon table as part of the same `ALTER TABLE` statement. See [Paimon](../../streaming-lakehouse/datalake-formats/paimon.md) for the lake-side details.
- The new value must be within `[1, max.bucket.num]` (4096 by default). Setting the value equal to the current one is a no-op.
- Changing the bucket count requires the Fluss server and Flink connector to be version 1.0 or later. The bucket-count change uses a newer `ALTER TABLE` request version: an older connector does not support altering `bucket.num` at all and rejects the statement client-side, and a newer connector talking to an older server fails with an unsupported-version error before the request is sent.
- In rare cases, a write job that is still running across the change fails its first writes to a partition created right after the change; the failed records are not sent and succeed on retry.

### Interaction with running jobs

Changing `bucket.num` does not stop or block writers. Fluss routes every record by the **actual** bucket count of the target partition, so running jobs keep writing correctly to both old and new partitions, and reads see each partition through its own bucket range.

One caveat: a Flink sink job with bucket shuffle captures the table-level bucket count at job start and uses it to distribute records across writer subtasks. A job that keeps running across the change distributes records of the new partitions according to the **old** count: the data still lands in the correct buckets, but records of one bucket may be spread across several writer subtasks, degrading batching efficiency and effective write parallelism until the job is restarted. After changing `bucket.num`, restart affected Flink sink jobs (e.g., from a savepoint) so that the shuffle picks up the new value.

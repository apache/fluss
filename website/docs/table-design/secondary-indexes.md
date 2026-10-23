---
title: Secondary Indexes
sidebar_position: 6
---

# Secondary Indexes

A global secondary index lets you look up current rows in a primary key table using equality predicates on non-primary-key columns. An index spans all buckets and partitions of its main table and can return multiple rows for one index value.

Fluss maintains each index in a separate, system-managed primary key table. A lookup first reads candidate primary keys from the index, then fetches the current main-table rows and checks that their indexed values still match. Updates, deletes, and partial updates maintain the index automatically.

## Creating an Indexed Table

Declare indexes when creating the main table. For example, the following Flink SQL creates an index on `user_id`:

```sql
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket.num' = '8',
    'table.changelog.image' = 'FULL',
    'secondary-index.idx_user.columns' = 'user_id',
    'secondary-index.idx_user.visibility' = 'sync',
    'secondary-index.idx_user.bucket.num' = '16'
);
```

Use these options in the Fluss catalog:

| Option | Default | Description |
|---|---|---|
| `secondary-index.<name>.columns` | Required | Ordered index columns, supplied as a comma-separated list or a JSON string array. |
| `secondary-index.<name>.visibility` | `sync` | Whether write acknowledgement waits for this index: `sync` or `async`. |
| `secondary-index.<name>.bucket.num` | Main-table bucket count | Positive number of buckets for this index. The replication factor is inherited from the main table. |

To create a composite index, set the columns option to, for example, `'user_id,amount'` or `'["user_id","amount"]'`. Equality lookup must provide every column in the index. A composite index does not support lookup by only its leading columns.

Index names must be unique within the table and contain only letters, digits, and underscores. They cannot start with an underscore or contain consecutive underscores. Index columns must exist, be distinct, and support Fluss key encoding. A row with a null value in any indexed column has no entry in that index.

Indexed tables require `'table.changelog.image' = 'FULL'`, which is the default. Index maintenance uses the previous row image to remove obsolete entries.

## Java API

Declare an index using the schema builder:

```java
Schema schema = Schema.newBuilder()
        .column("order_id", DataTypes.BIGINT())
        .column("user_id", DataTypes.BIGINT())
        .column("amount", DataTypes.DECIMAL(10, 2))
        .primaryKey("order_id")
        .index(
                "idx_user",
                IndexType.SECONDARY,
                Arrays.asList("user_id"),
                IndexVisibility.SYNC,
                16)
        .build();

TablePath path = TablePath.of("my_db", "orders");
TableDescriptor descriptor = TableDescriptor.builder()
        .schema(schema)
        .distributedBy(8, "order_id")
        .build();
admin.createTable(path, descriptor, false).get();
```

Obtain the named index lookuper from the main table. Supply key fields in the index's declared column order:

```java
try (Table table = connection.getTable(path)) {
    Lookuper lookuper = table.getSecondaryIndexLookuper("idx_user");
    LookupResult result = lookuper.lookup(GenericRow.of(42L)).get();
    for (InternalRow row : result.getRowList()) {
        // Process each matching main-table row.
    }
}
```

The result contains current main-table rows rather than internal index records. See the [Java API](../apis/java/index.md#lookup) for connection setup and primary-key lookups.

## Flink Lookup Joins

Flink selects an index when the equality join conditions contain all of its columns. If the conditions contain the complete primary key, Flink uses the primary-key lookup. Otherwise, it chooses an eligible secondary index with the most columns, with a stable column-position ordering to break ties. Additional join conditions filter the returned rows.

Both synchronous and asynchronous Flink lookup execution are supported. The `lookup.async` option controls Flink execution and is independent of the index's write visibility. See [Flink Lookup Joins](../engine-flink/lookups.md#secondary-index-lookup) for an example.

## Visibility

| Mode | Write acknowledgement | Lookup behavior |
|---|---|---|
| `sync` | Waits for the main-table write to commit and for the corresponding changes to reach every synchronous index. | A subsequent lookup through that index can observe the acknowledged write. |
| `async` | Waits for the main-table write to commit. Index propagation continues independently. | Recent writes may be temporarily absent from index lookup results. |

Both modes fetch and validate the current main-table row, so an obsolete index entry does not return a row whose indexed value no longer matches. This validation cannot recover candidates that have not reached an asynchronous index yet. A lookup does not provide a transactionally consistent snapshot across multiple main-table buckets.

A synchronous write timeout does not roll back a committed main-table write. The index may become visible after the request times out; handle retries using the normal writer guarantees.

## Table and Partition Lifecycle

Each index appears in the same database as its main table under the name `__idx__<main-table-name>__<index-name>`. Its read permissions follow the main table. Use the main-table secondary-index API for queries; direct writes and changelog subscriptions to an internal index table are rejected.

Dropping a main table also deletes its index tables. An index table cannot be dropped independently while its main table exists. If index-table cleanup fails after the main table is deleted, the remaining internal table can be removed separately.

For partitioned tables, lookup spans all main-table partitions without requiring the caller to supply a partition key unless it is itself an index column. Deleted partitions are filtered from results, and compaction eventually removes their index entries. Recreating a partition with the same name creates a new partition identity.

## Operations and Limits

Index maintenance consumes committed main-table WAL. Snapshot metadata preserves index replay progress, and WAL retention covers the data required to resume replication after recovery. A lagging asynchronous index can therefore retain main-table WAL even though it does not delay write acknowledgement.

[Index replication configuration](../maintenance/configuration.md#index-replication) controls reader and sender threads, window sizes, retained batch buffers, and retry backoff. TabletServer aggregate metrics include `indexReplicationPendingBytes`, `indexReplicationMaxNoProgressTimeMs`, `indexReplicationFailedSourceBucketCount`, and `indexReplicationRetriesPerSecond`. Use them with replication logs to investigate backlog or a stopped source bucket.

Before creating indexed tables, upgrade all participating TabletServers to support the private `PutIndex` protocol, then the Coordinator and clients/connectors. Do not schedule an indexed main-table bucket or index bucket on a server without index support. `PutIndex` v0 uses API key 1075; preview binaries using API key 1068 are not wire-compatible.

The following limitations apply:

- Indexes can only be declared when creating a primary key table. Online index creation, backfill, and removal are not supported.
- Index queries require equality on all index columns. Range predicates and partial composite-index keys are not supported.
- Index queries have no pagination or candidate-count limit. Low-selectivity values may cause many primary-key lookups.
- Schema evolution must preserve the names, types, and ordinal positions of primary-key and indexed columns. Appending columns is supported; internal index tables cannot evolve independently.
- Index lookup integration is provided by the Java API and Flink connector. Spark and lakehouse index lookup are not supported.

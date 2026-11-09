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
| `secondary-index.<name>.bucket.num` | Main-table bucket count at creation | Positive number of buckets for this index. The replication factor is inherited from the main table. |

Each Index Table retains its persisted bucket count independently of subsequent changes to the main table's `bucket.num`.

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

Flink first uses a primary-key lookup when the equality join conditions contain the complete primary key, then a supported prefix lookup when all required bucket and partition keys are present. Otherwise, it selects a secondary index whose columns are all covered by the conditions. Among eligible indexes, it prefers the one with the most columns, with a stable column-position ordering to break ties. Additional join conditions filter the returned rows.

Flink's FULL lookup cache supports primary-key and prefix lookups. Use `lookup.cache = NONE` or `PARTIAL` for secondary-index lookups.

Both synchronous and asynchronous Flink lookup execution are supported. The `lookup.async` option controls Flink execution and is independent of the index's write visibility. See [Flink Lookup Joins](../engine-flink/lookups.md#secondary-index-lookup) for an example.

Secondary indexes also support reverse lookups in eligible [foreign-key delta joins](../engine-flink/delta-joins.mdx#foreign-key-joins-with-secondary-indexes). An index does not replace Flink's requirements on join keys and upsert keys.

## Visibility

| Mode | Write acknowledgement | Lookup behavior |
|---|---|---|
| `sync` | Waits for the main-table write to commit and for the corresponding changes to reach every synchronous index. | A subsequent lookup through that index can observe the acknowledged write. |
| `async` | Waits for the main-table write to commit. Index propagation continues independently. | Recent writes may be temporarily absent from index lookup results. |

Both modes fetch and validate the current main-table row, so an obsolete index entry does not return a row whose indexed value no longer matches. This validation cannot recover candidates that have not reached an asynchronous index yet. A lookup does not provide a transactionally consistent snapshot across multiple main-table buckets.

A synchronous write timeout does not roll back a committed main-table write. The index may become visible after the request times out; handle retries using the normal writer guarantees.

With a synchronous index, even `acks=1` waits for the main-table high watermark and synchronous index progress, because index replication consumes only committed WAL.

For tables with synchronous indexes, client log scans expose records only below both the main-table high watermark and the synchronous index replication progress. A stalled synchronous index therefore delays client log consumption as well as write acknowledgement. Remote log files are exposed only once all records in the file satisfy this visibility bound. Server-side log replication and index WAL consumption continue independently of this client read bound.

Full reads select a retained snapshot using committed SYNC index progress, which may lag the live progress. If none qualifies, reconstruction starts at log offset 0 and fails if the required history is unavailable. Retaining more snapshots and a longer log TTL can preserve a usable starting point.

## Table and Partition Lifecycle

Each index appears in the same database as its main table under the name `__idx__<main-table-name>__<index-name>`. Its read permissions follow the main table. Use the main-table secondary-index API for queries; direct writes and changelog subscriptions to an internal index table are rejected.

Dropping a main table also deletes its index tables, but deletion is not atomic across tables. If one index table fails to be deleted, cleanup continues for the others and the error reports the failed index-table paths.

An index table cannot be dropped independently while its main table exists. After the main table has been deleted, remove each remaining index table directly using the reported path, for example:

```sql
DROP TABLE IF EXISTS __idx__orders__idx_user;
```

Retrying `DROP TABLE IF EXISTS orders` does not resume index cleanup once the main table is absent.

For partitioned tables, lookup spans all main-table partitions without requiring the caller to supply a partition key unless it is itself an index column. Partition deletion propagates asynchronously, so lookups may briefly return rows from a dropped partition after `dropPartition()` returns, as with ordinary primary-key lookups. Once the deletion metadata reaches the servers, deleted partitions are filtered from index results; compaction later removes their entries. Recreating a partition with the same name creates a new partition identity.

## Operations and Limits

Index maintenance consumes committed main-table WAL. Snapshot metadata preserves index replay progress, and WAL retention covers the data required to resume replication after recovery. This applies to asynchronous indexes too, although they do not delay write acknowledgement. A prolonged replication failure can retain WAL beyond its configured TTL and exhaust disk space. Monitor replication progress and storage capacity, and repair the failing target; releasing replication buffers does not release required WAL.

An unavailable index bucket prevents source buckets with pending mutations for that target from advancing SYNC visibility. As more source buckets encounter the target, this can stall SYNC write acknowledgements and client log consumption across the whole table. Affected operations retain their existing wait and timeout behavior. During sustained failures, index windows yield their staged payload budget and retry after backoff so other source buckets can compete for capacity.

[Index replication configuration](../maintenance/configuration.md#index-replication) controls reader and sender threads, window sizes, retained batch buffers, and retry backoff. TabletServer aggregate metrics include `indexReplicationPendingBytes`, `indexReplicationMaxNoProgressTimeMs`, `indexReplicationFailedSourceBucketCount`, and `indexReplicationRetriesPerSecond`. Use them with replication logs to investigate backlog and source failures awaiting repair.

Do not create indexed tables during a rolling upgrade. First upgrade all TabletServers, then all Coordinators and the clients/connectors to index-compatible versions.

Creating an indexed table requires every online TabletServer to advertise `PutIndex` v0. Creation is rejected while support is missing or unknown. This creation-time check does not prevent later incompatible server registrations or downgrades.

Once indexed tables exist, do not downgrade any TabletServer or Coordinator to a version without secondary-index support, or allow such servers to join or rejoin the cluster. Doing so can make indexes unavailable and may permanently corrupt index data. Upgrading again does not guarantee recovery.

The following limitations apply:

- Indexes can only be declared when creating a primary key table. Online index creation, backfill, and removal are not supported.
- Tables with `table.datalake.historical-partition.enabled` set to `true` do not support secondary indexes yet. Enabling historical partition access on an indexed table is also rejected.
- Tables with `table.kv.ttl` do not support secondary indexes yet. Row TTL cleanup does not emit changelog deletes to remove index entries.
- Index queries require equality on all index columns. Range predicates and partial composite-index keys are not supported.
- Index queries have no pagination. `client.lookup.secondary-index.max-candidates` must be positive and defaults to 1024. It bounds first-hop candidates before deduplication and main-table validation, including entries awaiting cleanup from dropped partitions. Exceeding it fails the lookup without partial results and can also fail other lookups batched in the same index-bucket request. Restarting a Flink job does not resolve a repeatable limit violation: use a more selective lookup or raise the limit after assessing memory and request costs. Ordinary primary-key and prefix lookups are unaffected.
- Schema evolution must preserve the names, types, and ordinal positions of primary-key and indexed columns. Appending columns is supported; internal index tables cannot evolve independently.
- Index lookup integration is provided by the Java API and Flink connector. Spark and lakehouse index lookup are not supported.

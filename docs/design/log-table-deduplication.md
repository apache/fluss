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

# Log Tables with Deduplication Keys

## Semantics

A log table may declare a deduplication key. It retains the first accepted row
for each key and filters subsequent rows with that key, including rows with
different payloads. First-row selection follows the bucket leader's write order;
it does not compare event timestamps or coordinate across buckets.

The public table has no primary key and exposes append and log scans. Internally,
it uses the primary-key table's first-row write semantics and persistent key
state. The storage difference is that KV contains only a presence marker, while
the complete accepted row is stored in the log.

For a non-partitioned table, keys live until the table is dropped. For a
partitioned table, keys belong to a partition instance. Dropping and recreating
a partition creates a new key scope. There is no cross-partition uniqueness.
Log retention does not expire keys. The number of distinct keys determines the
size of the persistent key set.

## Table Definition

```sql
CREATE TABLE events (
    tenant_id STRING NOT NULL,
    event_id STRING NOT NULL,
    payload STRING
) WITH (
    'bucket.num' = '16',
    'deduplication.key' = 'tenant_id,event_id'
);
```

```sql
CREATE TABLE partitioned_events (
    dt STRING NOT NULL,
    tenant_id STRING NOT NULL,
    event_id STRING NOT NULL,
    payload STRING
) PARTITIONED BY (dt) WITH (
    'bucket.num' = '16',
    'deduplication.key' = 'dt,tenant_id,event_id'
);
```

The option follows `bucket.key` column-list syntax. The catalog resolves physical
column names and stores a structured `Schema.DeduplicationKey`; the option is
not a client-side switch. SQL requires explicit `NOT NULL` declarations because
a connector option is not a Flink nullability constraint.

The Java schema builder provides `deduplicationKey(String...)` and
`deduplicationKey(List<String>)`. As with `primaryKey`, the builder makes the key
columns non-nullable. The key is immutable and survives schema copies and JSON
round trips. A schema cannot contain both a primary key and a deduplication key.
Existing schemas without a deduplication key retain their serialization.

`Schema.getDeduplicationKey()` exposes the optional definition, and
`getDeduplicationKeyColumnNames()` exposes its ordered columns. `TableInfo`
provides `hasDeduplicationKey()` and `getDeduplicationKeys()`. The table has no
primary key and must not be advertised to Flink as a primary-key table.

There is no separate enabled flag, keep strategy, expiration interval, or scope
configuration. First-write retention and the scope described above are fixed.

## Distribution

Partition columns must be included in the logical deduplication key. At least
one deduplication column must not be a partition column. By default, the bucket
key is the deduplication key with partition columns removed. An explicit bucket
key must be a nonempty subset of those non-partition columns.

These rules guarantee that equal keys in a partition route to the same bucket.
Partition columns need not be stored repeatedly in the physical key because
the partition instance provides their namespace. Key types, equality, encoding,
and hashing must use the same contracts as primary-key routing. A hash or Bloom
filter cannot replace exact key comparison.

## Append API

The public entry point is `table.newAppend().createWriter()`. Both `AppendWriter`
and `TypedAppendWriter` return `CompletableFuture<AppendResult>` using the existing
append result contract. Successful completion means that the request succeeded;
it does not distinguish inserted rows from filtered rows. An all-duplicate
request also succeeds.

Append accepts complete INSERT rows. Upsert, partial updates, overwrite mode,
delete, lookup, full-row KV scans, multi-table writes, lake tiering, historical
writes, and auto-increment columns are unsupported. The Java writer and server
validate non-nullable columns before reserving a key.

Writer configuration, sequence validation, retries, acknowledgments, replication,
and failure handling follow the existing primary-key write protocol. Appending
an existing key is filtered by persistent key state.

## Flink and Reads

The Flink catalog option is `deduplication.key`. The catalog persists it as
structured schema metadata and does not expose a Flink primary-key constraint.
The sink accepts insert-only input. Sources consume accepted INSERT records
through log scans, with projection and bounded scans supported. Exact row-count
metadata is unavailable; count queries scan retained log records.

## Write and Storage Path

The append writer encodes the deduplication key using the primary-key encoder
and sends full rows through PUT_KV. A request identifies deduplicating append so
the server can reject attempts to use ordinary upsert on this table. PRODUCE_LOG
cannot bypass persistent key state.

The server uses `KvTablet.putAsLeader` and `KvWriteProcessor.putAsLeader`, including
the existing schema snapshot, WAL construction, sequence checks, rollback,
prewrite boundaries, replication wait, and completion path. The table uses
first-row semantics with deletes disabled. Partial updates and overwrite mode
are rejected.

For this table, the upsert step performs an exact presence lookup in the prewrite
buffer and RocksDB. An existing marker filters the row without decoding an old
value. A missing marker uses the ordinary INSERT path, appending the complete
row to the WAL and staging a one-byte presence marker at its log offset. A second
same-key input sees the staged marker before it is flushed to RocksDB.

An all-filtered batch uses the existing primary-key empty batch behavior to
advance the writer sequence and log offset. Offset differences do not count
accepted rows. Batches use the existing log format and writer snapshots.

## Recovery and Retention

Presence markers participate in normal KV flushes and incremental snapshots.
Recovery loads the completed KV snapshot and replays accepted INSERT records
from its log offset, extracting keys and restoring presence markers. The bucket
opens for writes after the existing KV recovery completes. Writer state follows
the primary-key table's existing log recovery and retry behavior.

Log cleanup must retain records not yet covered by a completed key-state
snapshot. Local cleanup uses the KV retention offset; remote cleanup also uses
that offset as its safe deletion boundary. After snapshot coverage, log TTL
may remove rows while their keys remain in KV. KV TTL is unsupported.

## Schema Evolution and Compatibility

The deduplication key is specified at creation and cannot be enabled, removed,
reordered, or replaced on an existing table. Key column types cannot change and
key columns cannot be dropped. Non-key evolution follows existing log-table
rules. Bucket keys are a nonempty subset of the non-partition deduplication
columns, including equality. Existing partition bucket layouts remain unchanged
when bucket counts change for future partitions.

Creation requires CREATE_TABLE version 1 and checks online TabletServers for
PUT_KV version 4 support. Writes require PUT_KV version 4 to carry the table-kind
flag. All servers and clients accessing these tables must support the schema
metadata and write operation. Mixed-version operation, old-server joins, and
downgrades with these tables are unsupported. Ordinary tables retain their
existing API, storage format, and behavior.

The storage saving relative to a first-row primary-key table comes from retaining
keys and presence markers instead of complete values in KV and its snapshots.
Exact key lookup, RocksDB maintenance, and full-row logging remain necessary.

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Bounded UnionRead

`fluss-lake` captures source boundaries, plans tasks and executes bounded Append or lake-only reads. Supply a `LakeSource` for lake access. The Paimon backend and PK reconciliation are separate integrations.

```rust,no_run
# use std::sync::Arc;
# use fluss::client::FlussConnection;
# use fluss::metadata::TablePath;
# use fluss::predicate::col;
# use fluss_lake::{FlussLakeTable, Result};
# async fn capture(connection: Arc<FlussConnection>, path: &TablePath) -> Result<()> {
let table = FlussLakeTable::new(connection, path);
let context = table.capture_read_context().await?;
let scoped = table.capture_read_context_with_partition_scope(&col("region").ge("US")).await?;
# Ok(())
# }
```

`new()` performs no I/O. Each capture queries current metadata and freezes a new boundary set; reuse the returned context to share boundaries across scans. It contains the table identity, captured schema, partition scope, optional lake mapping/snapshot and concrete half-open log ranges `[start, stop)`. Per-partition bucket counts are preserved. Credentials, projection, row filters and physical file tasks stay outside it. It provides neither a global transactional snapshot nor a retention lease.

## Partition scope

The explicit scope is a core `Predicate` over partition columns. Conditions are evaluated against partition values before offset queries. Safe STRING equalities also narrow the Admin listing; ranges and disjunctions are evaluated locally. The predicate must be exactly encodable by the existing Fluss predicate protocol. Unsupported expressions fail before capture rather than being rounded or silently dropped. The planner extracts safe string equalities from the query filter before capturing boundaries; the full predicate remains for exact evaluation.

Lake planning must apply the same scope, including to lake-only expired partitions. An empty log-range list does not imply an empty lake selection. Scans sharing a scoped context must stay within it; use table-wide capture when sharing across different selections.

Capture rechecks partition IDs after offset queries and excludes confirmed removed or replaced partitions. Lookup failures are suppressed only when the original ID is confirmed absent. A failed recheck does not prove deletion. Capture also rechecks the table identity. This fixes the observed bucket set without claiming atomicity across metadata calls.

## Coordinator and worker boundaries

```rust,no_run
# use fluss::metadata::TableBucket;
# use fluss_lake::{FlussLakeReadContext, FlussLakeTable, Result};
# async fn transfer(context: &FlussLakeReadContext, bucket: &TableBucket, worker: &FlussLakeTable) -> Result<()> {
let selected = context.for_bucket(bucket)?;
let message = selected.to_protobuf()?;
// The engine transports the shared message to its worker.
let restored = worker.import_read_context(&message).await?;
# Ok(())
# }
```

`for_bucket()` retains the captured header and one range. Workers do not need unrelated buckets to validate that range. `is_bucket_scoped()` distinguishes this selection from complete captured partitions: a planner consuming it must restrict both lake and log inputs to that bucket. Backends without a compatible bucket layout must reject this mode. It cannot seed a full-table scan, and lake-only expired partitions require separate tasks. Task allocation and completeness across workers remain the coordinator's responsibility.

`import_read_context()` checks the source table ID and mapping, then resolves the exact schema ID in the message. A later column addition does not invalidate the boundary. When a schema lookup is needed, the table identity is rechecked afterward to detect name reuse. No offsets or snapshot are refreshed. Retain the result per query or worker; this is initialization I/O, not per-record work. Readers must map data by stable field ID into the captured schema. This does not claim support for every schema evolution.

For cached metadata, `FlussLakeReadContext::from_protobuf(message, live_table, captured_schema)` performs the same import without I/O. The caller must resolve that schema from the same table identity. `from_boundary()` and `from_boundary_with_partition_scope()` import structured complete captures; `from_bucket_boundary()` imports a single bucket. Their TableInfo must describe the captured schema/layout. Import validates consistency, not the provenance of independently captured values.

## Shared protocol

`PbUnionReadContext`, `PbUnionReadLake` and `PbUnionReadLogRange` are defined in the canonical `FlussApi.proto` and generated for Java and Rust. Rust exposes them through `fluss::protocol`. The message includes the schema ID, not a serialized Rust schema, and reuses `PbPredicate` with stable field IDs for the partition scope. Java coordinators can use existing snapshot/offset APIs or connector planning and populate this contract. No new Java capture helper or C ABI is included here.

The protocol starts at version 1. Optional additive fields are compatible only when ignoring them preserves the read semantics. Unknown optional fields are accepted; unknown versions, coverage kinds and baseline kinds are rejected. Incompatible semantics require a new version. Rolling upgrades must keep producers within the version and semantic kinds supported by all consumers. Shared historical logs and KV snapshots need explicit future semantics; they cannot be introduced as optional hints that old consumers silently ignore.

`coverage = 0` means complete captured partitions; `coverage = 1` means one bucket; `coverage = 2` means a lake-only snapshot with no captured log ranges. Log ranges do not enumerate every partition in the lake. The engine owns message transport, scheduling and runtime credentials.

## PK correctness and retained data

| Baseline | Read requirement |
| --- | --- |
| `Lake` | Pinned lake snapshot plus its matching log seam. A seam of zero is valid. |
| `None`, Append table | Start at the captured earliest retained offset. |
| `None`, PK table | Complete log history from offset zero. Truncated history fails with `DataUnavailable`, even if the retained range is empty. |

Without a baseline, retained PK changes cannot recover keys whose last update has expired. KV snapshot fallback remains future work. Boundary capture preserves merge-engine metadata; the subsequent default executor targets deduplicate semantics, while other merge engines require explicit executor support.

Call `validate_available()` for required ranges after safe pruning. It detects a lake seam already below the captured retention boundary; execution must also detect later retention or truncation. Missing required data fails the attempt rather than advancing offsets or substituting a newer snapshot. Engines discard partial results before retrying; a fresh context may observe different data and is not guaranteed to recover missing history.

## Follow-ups

Streaming discovery, progress and checkpointing require a future API; this context describes bounded reads. Query-filter accepted/remaining negotiation belongs to the adapter layer and is distinct from the exact partition scope.

Tables with `table.datalake.historical-partition.enabled=true` are rejected during boundary capture and import: writes to expired partitions can remain in the shared `__historical__` log before tiering and are missing from ordinary partition ranges and the lake snapshot. Shared historical-log support is tracked in [#5001](https://github.com/apache/fluss/issues/5001). Expired partitions already present in a lake snapshot remain supported when this option is disabled.

## Default planning and lake sources

`table.new_scan().with_lake_source(source).plan().await?` captures bounded inputs and plans tasks. `plan_with_context(&context)` reuses previously captured boundaries. The optional backend and execution capabilities are described below.

Append lake tasks and bucket log tails are independent. PK tasks group a complete lake baseline and log tail by partition and bucket. The source must reconcile lake versions and deletion vectors before returning a PK baseline. Partition and bucket pruning is conservative; execution must apply the exact filter after reconciliation.

Engines own scheduling and transport. `split.descriptor()` exports immutable source inputs, and `scan.import_splits(&boundary, &descriptors).await?` restores assigned tasks without replanning. The adapter must preserve scan settings and provide credentials at execution time. Expired lake-only partitions remain discoverable through the pinned snapshot.

## Append execution

Create a reader with `scan.new_reader()`. `read_split(split)` synchronously returns a lazy stream; polling starts I/O. `read_splits(splits)` reads tasks sequentially. Engines control parallelism by scheduling separate streams. Dropping a stream cancels pending work; after an error, discard partial results before retrying the query.

The reader applies the exact filter before removing hidden columns and sizing output batches. Append tasks retain their fixed stop offsets. Lake-only reads use the source's baseline semantics. PK lake/log reconciliation is introduced separately and is explicitly rejected in this layer.

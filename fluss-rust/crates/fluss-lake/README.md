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

`fluss-lake` plans and executes bounded Append or lake-only reads. Enable `paimon` for the default lake source or supply a custom `LakeSource`.

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

The protocol starts at version 1. Optional additive fields are compatible only when ignoring them preserves the read semantics. Unknown optional fields are accepted; unknown versions, coverage kinds and baseline kinds are rejected. Incompatible semantics require a new version. Rolling upgrades must keep producers within the version and semantic kinds supported by all consumers. KV snapshots use baseline kind 2 and require `kv_snapshot_id`; older consumers reject that kind. Shared historical logs likewise need an explicit semantic kind rather than an optional hint that older consumers silently ignore.

`coverage = 0` means complete captured partitions; `coverage = 1` means one bucket; `coverage = 2` means a lake-only snapshot with no captured log ranges. Log ranges do not enumerate every partition in the lake. The engine owns message transport, scheduling and runtime credentials.

## PK correctness and retained data

| Baseline | Read requirement |
| --- | --- |
| `Lake` | Pinned lake snapshot plus its matching log seam. A seam of zero is valid. |
| `KvSnapshot { snapshot_id }` | Specified persisted bucket snapshot plus its matching log seam. |
| `None`, Append table | Start at the captured earliest retained offset. |
| `None`, PK table | Complete log history from offset zero. Truncated history fails with `DataUnavailable`, even if the retained range is empty. |

Without a baseline, retained PK changes cannot recover keys whose last update has expired. Capture prefers a readable lake baseline, then a persisted KV snapshot, then complete logs. For live PK buckets without a lake seam, union planning ignores lake tasks and preserves the captured KV or complete-log fallback; lake-only scans still use the selected lake snapshot. Planning preserves the KV snapshot ID and seam in a bucket task, including when the log tail is empty; opening the snapshot and reconciling rows belong to the subsequent PK executor. Boundary capture preserves merge-engine metadata; the subsequent default executor targets deduplicate semantics, while other merge engines require explicit executor support.

Call `validate_available()` for required ranges after safe pruning. It detects a baseline seam already below the captured retention boundary; execution must also detect later retention or truncation. Missing required data fails the attempt rather than advancing offsets or substituting a newer snapshot. Engines discard partial results before retrying; a fresh context may observe different data and is not guaranteed to recover missing history.

KV capture queries metadata only and does not require the `kv-snapshot` feature or RocksDB. The context holds no lease. A worker must open the specified snapshot and verify its log seam; if it has expired, the attempt fails. Engines that need to protect queued tasks must hold a core snapshot lease until download completes. Snapshot leases do not protect the log tail.

## Follow-ups

Streaming discovery, progress and checkpointing require a future API; this context describes bounded reads. Query-filter accepted/remaining negotiation belongs to the adapter layer and is distinct from the exact partition scope.

Tables with `table.datalake.historical-partition.enabled=true` are rejected during boundary capture and import: writes to expired partitions can remain in the shared `__historical__` log before tiering and are missing from ordinary partition ranges and the lake snapshot. Shared historical-log support is tracked in [#5001](https://github.com/apache/fluss/issues/5001). Expired partitions already present in a lake snapshot remain supported when this option is disabled.

## Default planning and lake sources

`table.new_scan().with_lake_source(source).plan().await?` captures bounded inputs and plans tasks. `plan_with_context(&context)` reuses previously captured boundaries. The optional backend and execution capabilities are described below.

Append lake tasks and bucket log tails are independent. PK tasks group a complete lake or KV baseline and log tail by partition and bucket. The source must reconcile lake versions and deletion vectors before returning a PK baseline. Partition and bucket pruning is conservative; execution must apply the exact filter after reconciliation.

`LakePlannerContext` carries the exact partition scope and optional selected bucket to the backend. Sources should prune before expensive planning when supported; returned tasks are also checked against the captured scope. Partition identities use the core Fluss partition encoding. Lake filters may include mutable PK columns, provided the backend preserves its current-view merge/deletion semantics; the complete Fluss tail is folded before final exact filtering.

Engines own scheduling and transport. `split.descriptor()` exports immutable source inputs, and `scan.import_splits(&boundary, &descriptors).await?` restores assigned tasks without replanning. The worker must rebuild the planning filter. Import and execution compare its exact bound predicate with the digest on the task. Replan descriptors created before the digest was recorded. The adapter still supplies the projection, read mode and credentials. Expired lake-only partitions remain discoverable through the pinned snapshot.

## Paimon backend

Enable the `paimon` feature to plan and read pinned Paimon snapshots. The backend owns Paimon version merging, deletion vectors and portable task payloads; a PK baseline returns at most one row per full key. Parquet reads are covered by integration tests; other Paimon file formats have not been validated for UnionRead. Paimon 0.5 and Fluss share Arrow 59. The `fluss-lake` crate requires Rust 1.95; the core client MSRV is unchanged.

The data snapshot stays pinned while output uses the captured Fluss schema. Newly appended nullable columns missing from the snapshot are filled with nulls. This follows Fluss tiering's append-only schema evolution; cross-schema renames and drops are rejected. Paimon and Fluss column IDs belong to separate namespaces.

Fluss evaluates binary range comparisons and timestamp predicates that Paimon cannot represent exactly. Binary equality can still be pushed down. A shared source reuses its catalog and keeps the last few pinned snapshots open; it rejects tables whose properties resolve to a different Paimon catalog. Concurrent tasks for one snapshot share a single open, different snapshots open in parallel, and active streams keep their table after it is evicted.

`PaimonLakeSource::with_baseline_reader` permits replacing baseline reading while retaining Paimon planning. A replacement must preserve the pinned snapshot, file/deletion-vector association and requested PK current-view semantics. UnionRead does not support tables with `query-auth.enabled=true`; the Fluss log tail cannot enforce Paimon row filters or column masks. Primary-key UnionRead also rejects Paimon tables that keep something other than the last write of a key: `sequence.field`, or `ignore-delete=true` while the Fluss table applies deletes.

Task payloads preserve Paimon's native file paths, including `data-file.external-paths`. They must come from a trusted planner; workers use runtime storage credentials to access those locations. Snapshot, partition and bucket identities are checked when tasks are opened.

Partition scope is passed to Paimon before planning. Paimon 0.5 does not expose a public bucket filter, so selected-bucket planning still scans the selected partitions' manifests and filters the resulting tasks by bucket.

Fluss S3/OSS data requests use rustls and return the stored object bytes without HTTP decompression, independently of reqwest features enabled by Paimon. Paimon 0.5 still enables native TLS, so the combined build retains OpenSSL on Linux. OpenDAL's credential providers use a separate internal HTTP client; their TLS selection also requires an upstream fix.

## Append execution

Create one reader per execution attempt with `scan.new_reader()`. The reader and its clones share the initial table identity lookup and lake source across splits. Create a fresh reader on retry. `read_split(split)` synchronously returns a lazy stream; polling starts I/O. `read_splits(splits)` reads tasks sequentially. Engines control parallelism by scheduling separate streams. Dropping a stream cancels pending work; after an error, discard partial results before retrying the query.

The reader applies the exact filter before removing hidden columns and sizing output batches. Append tasks retain their fixed stop offsets. Lake-only reads use the source's baseline semantics. PK lake/log reconciliation is introduced separately and is explicitly rejected in this layer.

Tasks with a persisted KV snapshot baseline are rejected before reading until the default executor integrates that baseline. An empty log tail does not make a KV task empty.

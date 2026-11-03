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

# UnionRead source preparation

`fluss-lake` captures reusable source boundaries for Fluss-only and lake-plus-log reads without opening a lake catalog or reading lake files. This layer provides boundary capture and structured access; the default lake planner and reader are delivered separately.

```rust,no_run
# use std::sync::Arc;
# use fluss::client::FlussConnection;
# use fluss::metadata::TablePath;
# use fluss_lake::{FlussLakeTable};
# async fn capture_read_context(connection: Arc<FlussConnection>, path: &TablePath) -> fluss_lake::Result<()> {
let table = FlussLakeTable::open(connection, path).await?;
let context = table.capture_read_context().await?;
// Reuse this object across scans; engines own cross-process task transport.
# Ok(())
# }
```

The context freezes table/schema identity and half-open log ranges `[start, stop)`. Lake-enabled tables additionally carry the physical lake mapping, format and optional readable snapshot. Non-lake tables skip the lake snapshot lookup. Projection and row predicates belong to the subsequent planning layer. `capture_read_context()` captures table-wide boundaries; `capture_read_context_with_partition_scope()` captures a restricted partition scope before querying offsets. Actual per-partition bucket counts are preserved. The context contains no credentials or physical lake-file tasks, is not a global transactional snapshot and does not acquire a retention lease.

PK tables are accepted by this boundary API. Capturing boundaries does not validate an executor's merge-engine support or supply a KV snapshot: without a lake baseline, retained logs alone may omit existing keys. The subsequent default UnionRead executor targets deduplicate semantics; other merge engines and KV snapshot fallback remain separate work.

## Partition scope and schema

```rust,no_run
# use std::collections::HashMap;
# use fluss::metadata::PartitionSpec;
# use fluss_lake::{FlussLakeTable, Result};
# async fn capture_read_context(table: &FlussLakeTable) -> Result<()> {
let filter = PartitionSpec::new(HashMap::from([("region", "US")]));
let context = table.capture_read_context_with_partition_scope(&filter).await?;
assert_eq!(context.partition_filter().get("region").map(String::as_str), Some("US"));
# Ok(())
# }
```

The spec is an exact conjunction of partition-key/value pairs, reusing the core Admin API. It may contain a subset of the partition keys; an empty spec selects the whole table. Values must use Fluss partition metadata encoding. Automatic predicate-to-scope extraction belongs to the subsequent scan/planning PR. This boundary-only entry is not a general SQL expression API: engines must derive a safe spec or keep the default full scope. Only matching live partitions have offsets queried. A single ID recheck after capture excludes partitions that disappeared or were recreated under the same name. Lookup errors are suppressed only when the original partition ID is confirmed absent; failed rechecks do not prove deletion. The table identity is checked again after capture.

The `partition_filter` map records the captured scope. Lake planning must apply it too, including to lake-only expired partitions. An empty log-range list does not imply an empty lake selection. A scoped context may be shared only by scans within its scope; use a table-wide context when sharing across different partition selections. This layer cannot enforce scope on an engine-native reader: the adapter must honor it, and the subsequent default planner must apply it before generating tasks.

`validate_table()` checks the source table ID and logical path, plus the lake mapping and format when captured. Enabling tiering later does not change an already frozen Fluss-only read. It does not replace the captured schema or reject later column additions. Consumers must bind projection and predicates to `table_schema()` and map batch columns by stable column ID. The core log scanner's fixed-schema mode supports this mapping, but its target is the schema used to create the scanner: constructing a scanner from fresh metadata alone does not pin it to the context. Lake adapters likewise need to map the fixed snapshot's schema to the context's output schema. Unsupported conversions must fail rather than reinterpret columns by live positions. The context stores the typed Fluss schema captured during preparation.

## Boundaries from a Java coordinator

Java coordinators use `org.apache.fluss.client.metadata.UnionReadBoundaryPlanner` in `fluss-client`. It captures structured boundaries using the supplied Admin client, without opening lake files or depending on Flink.

```java
UnionReadContext context = UnionReadBoundaryPlanner.captureReadContext(admin, tablePath);
UnionReadContext scoped = UnionReadBoundaryPlanner.captureReadContext(admin, tablePath, scope);
for (UnionReadContext.LogRange range : scoped.getLogRanges()) {
    // Build engine-owned tasks using the bucket identity and [start, stop).
}
```

The result exposes the frozen schema, lake mapping/snapshot, partition scope and per-bucket ranges. It contains no arbitrary table properties or credentials. Capture applies the scope before querying offsets, uses each partition's bucket count, and rechecks partition IDs and table identity. Only confirmed removed partitions are skipped. The caller owns Admin. Capture blocks the planning thread and queries partitions sequentially; do not invoke it from Admin RPC callbacks. Interrupted waits abort capture without cancelling issued RPCs. Request timeouts follow client configuration.

## Engine-owned transport

Fluss defines structured read boundaries, not a universal cross-language context format. In-process callers share the context directly. A distributed connector maps these fields into its own tasks and owns serialization, version compatibility, distribution and retry. Its worker must preserve source IDs, captured schema, lake mapping/snapshot, partition scope, per-partition bucket counts, baseline presence and concrete offsets. No worker should recapture metadata to reconstruct a missing field. Runtime credentials remain local.

A Rust worker can import engine-decoded fields without another metadata lookup:

```rust,no_run
# use fluss::metadata::{TableInfo, PartitionSpec};
# use fluss_lake::{FlussLakeLogRange, FlussLakeReadContext};
# fn import(captured_table: &TableInfo, snapshot_id: Option<i64>, ranges: Vec<FlussLakeLogRange>, scope: &PartitionSpec) -> fluss_lake::Result<()> {
let context = FlussLakeReadContext::from_boundary_with_partition_filter(
    captured_table, snapshot_id, ranges, scope,
)?;
# Ok(())
# }
```

`FlussLakeLogRange::new()` accepts the physical bucket identity, business partition, bucket count, optional lake seam, stop and earliest offset. `from_boundary()` imports a table-wide capture; `from_boundary_with_partition_filter()` imports a scoped one. These constructors validate table/partition identities, concrete offsets and complete bucket coverage. They do not prove that independently supplied values were captured consistently: the coordinator must keep the readable snapshot and matching seams together, then capture stops.

Each range associates its physical bucket with `FlussLakeLogPartition::Ordinary(identity)`, including an unpartitioned table. Consumers must handle the partition kind or reject execution. Bucket pruning additionally requires the routing rules for that kind. No C ABI or Java-to-Rust metadata conversion adapter is provided in this PR.

Shared historical logs (`__historical__`) are not supported yet. They can contain changes for several business partitions and require a distinct log partition kind, snapshot-aligned offsets and record-level partition filtering. Reading lake-only expired partitions does not cover these pending changes. This remains follow-up work; no historical kind is emitted or accepted by this version.

Each Rust log range exposes its baseline:

| `baseline` | Meaning |
| --- | --- |
| `FlussLakeBaseline::Lake` | Read the context's pinned lake snapshot, then apply logs from `start_offset`, the snapshot's seam for this bucket. A zero seam is valid. |
| `FlussLakeBaseline::None` | No baseline; `start_offset` equals the captured earliest retained offset. |

The baseline is determined by the presence of the bucket's seam, not by whether its offset is positive or lake file planning returns any files. The Rust constructor takes `snapshot_offset: Option<i64>` to preserve this distinction. A PK bucket without a baseline can only reconstruct state represented in the retained log. It cannot recover keys absent from that history. This matches the earliest-log fallback, not a guarantee of a complete historical PK view. `validate_available()` checks whether the frozen range was already missing at capture; execution must also detect later retention.

KV snapshot baselines and SST readers remain future work. They need a snapshot ID and matching log seam captured before the stop offset, plus snapshot lifecycle management. Rust's non-exhaustive baseline and partition enums leave room for additional kinds; adapters must reject unsupported semantics rather than interpreting them as no baseline.

Execution must include selected lake-only partitions from the pinned snapshot, even when they no longer appear among the live log ranges. For required log ranges, check `validate_available()` after safe pruning and still detect retention or truncation while reading. A captured earliest offset does not protect data from later expiry. Runtime credentials and engine-specific scheduling remain separate.

## Execution and future modes

Preparation fixes the captured bucket set; partitions created later are not added by workers. The final identity recheck excludes removed or replaced partitions without refreshing the frozen schema or offsets. Capture is not atomic across buckets or partition metadata.

This context describes a bounded read, not streaming progress or a checkpoint. Streaming UnionRead can reuse source identities and baseline seams through an explicit future API; it must define changelog output, discovery and recovery separately. Runtime connections and credentials remain local. Engines own scheduling, retry and result publication. A missing frozen input must fail rather than switch snapshots or advance offsets; earlier batches are only a partial result of a failed attempt.

Row-filter negotiation belongs to the subsequent adapter layer. Ordinary client filters remain strict; adapters may negotiate accepted/remaining conditions. A condition accepted only for pruning must remain for exact evaluation. The boundary API neither serializes query expressions nor silently drops unsupported filters.

Shared historical-log support is tracked in [#5001](https://github.com/apache/fluss/issues/5001); it is separate from reading expired partitions already present in the lake snapshot.

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

`fluss-lake` captures reusable source boundaries for Fluss-only and lake-plus-log reads without opening a lake catalog or reading lake files. This layer provides preparation and context transport; the default lake planner and reader are delivered separately.

```rust,no_run
# use std::sync::Arc;
# use fluss::client::FlussConnection;
# use fluss::metadata::TablePath;
# use fluss_lake::{FlussLakeTable, FlussLakeReadContext};
# async fn capture_read_context(connection: Arc<FlussConnection>, path: &TablePath) -> fluss_lake::Result<()> {
let table = FlussLakeTable::open(connection, path).await?;
let context = table.capture_read_context().await?;
let transported = FlussLakeReadContext::from_json(&context.to_json()?)?;
# Ok(())
# }
```

The context freezes table/schema identity and half-open log ranges `[start, stop)`. Lake-enabled tables additionally carry the physical lake mapping, format and optional readable snapshot. Non-lake tables skip the lake snapshot lookup. Projection and row predicates belong to the subsequent planning layer. `capture_read_context()` captures table-wide boundaries; `capture_read_context_with_partition_scope()` captures a restricted partition scope before querying offsets. Actual per-partition bucket counts are preserved. The context contains no credentials or physical lake-file tasks, is not a global transactional snapshot and does not acquire a retention lease.

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

The required `partition_filter` map is transported with the context. Lake planning must apply it too, including to lake-only expired partitions. An empty log-range list does not imply an empty lake selection. A scoped context may be shared only by scans within its scope; use a table-wide context when sharing across different partition selections. This layer cannot enforce scope on an engine-native reader: the adapter must honor it, and the subsequent default planner must apply it before generating tasks.

`validate_table()` checks the source table ID and logical path, plus the lake mapping and format when captured. Enabling tiering later does not change an already frozen Fluss-only read. It does not replace the captured schema or reject later column additions. Consumers must bind projection and predicates to `table_schema()` and map batch columns by stable column ID. The core log scanner's fixed-schema mode supports this mapping, but its target is the schema used to create the scanner: constructing a scanner from fresh metadata alone does not pin it to the context. Lake adapters likewise need to map the fixed snapshot's schema to the context's output schema. Unsupported conversions must fail rather than reinterpret columns by live positions. The context stores the original Fluss schema JSON, including optional attributes not exposed by the current Rust schema type.

## Boundaries from a Java coordinator

Java coordinators use `org.apache.fluss.client.metadata.UnionReadBoundaryPlanner` in `fluss-client`. It captures and encodes the boundary using the supplied Admin client, without opening lake files or depending on Flink.

```java
byte[] bytes = UnionReadBoundaryPlanner.captureReadContext(admin, tablePath).get();
// Restrict the input shared by subsequent scans:
byte[] scoped = UnionReadBoundaryPlanner.captureReadContext(admin, tablePath, scope).get();
```

The planner freezes the schema and readable snapshot before capturing concrete earliest/stop offsets. It applies the partition scope before offset requests, uses each partition's bucket count, and rechecks partition IDs and table identity. Only confirmed removed partitions are skipped; other failures propagate. The encoder is package-private. The caller owns the Admin lifecycle and distributes the returned bytes. Captures use a sequential partition pipeline to bound metadata fan-out; cancellation does not guarantee cancellation of composed Admin RPCs.

The Rust worker imports these bytes without a connection or a second boundary lookup:

```rust,no_run
# use fluss_lake::FlussLakeReadContext;
# fn import(bytes: &[u8]) -> fluss_lake::Result<()> {
let context = FlussLakeReadContext::from_json(bytes)?;
# Ok(())
# }
```

Rust coordinators with already resolved metadata can instead use `FlussLakeLogRange::new()` and `FlussLakeReadContext::from_boundary()`, or `from_boundary_with_partition_filter()` for a restricted scope. Both import paths validate table/partition identities, concrete offsets and complete bucket coverage; neither refreshes the supplied snapshot or stops. Callers must keep the readable snapshot and its seam offsets together. The JSON contract is version 1 and shared by these Java and Rust APIs; it remains an evolving format, with incompatible versions rejected explicitly.

Each log range has a physical `table_bucket` and a required `partition` association. Currently `FlussLakeLogPartition::Ordinary(identity)` identifies a regular Fluss partition, including an unpartitioned table. Its JSON form is `{"kind":"ordinary","identity":{"key_values":[["region","US"]]}}`, or `{"kind":"ordinary","identity":"unpartitioned"}`. Native readers must handle every partition kind or reject the read before emitting results. A physical bucket count alone does not establish the routing rules needed for bucket pruning.

Shared historical logs (`__historical__`) are not supported yet. They can contain changes for several business partitions and require a distinct log partition kind, snapshot-aligned offsets and record-level partition filtering. Reading lake-only expired partitions does not cover these pending changes. This remains follow-up work; no historical kind is emitted or accepted by this version.

Each log range carries a required `baseline` object:

| `baseline` | Meaning |
| --- | --- |
| `{"kind":"lake"}` | Read the context's pinned lake snapshot, then apply logs from `start_offset`, the snapshot's seam for this bucket. A zero seam is valid. |
| `{"kind":"none"}` | No baseline; `start_offset` equals the captured earliest retained offset. |

The baseline is determined by the presence of the bucket's seam, not by whether its offset is positive or lake file planning returns any files. The Rust constructor takes `snapshot_offset: Option<i64>` to preserve this distinction. A PK bucket without a baseline can only reconstruct state represented in the retained log. It cannot recover keys absent from that history. This matches the earliest-log fallback, not a guarantee of a complete historical PK view. `validate_available()` checks whether the frozen range was already missing at capture; execution must also detect later retention.

KV snapshot baselines and SST readers are future work. They will need a snapshot ID and its matching log seam captured before the stop offset, plus snapshot lifecycle management. This version rejects unknown baseline kinds. It does not silently downgrade them to `none`. Version 1 is the initial public transport contract under review; the earlier flat lake fields and log-range `partition_identity` field are development-only and must be recaptured. Version 1 is not yet a released compatibility baseline. Earlier PoC version numbers were development-only; those contexts must be recaptured rather than relabeled.

Execution must include selected lake-only partitions from the pinned snapshot, even when they no longer appear among the live log ranges. For required log ranges, check `validate_available()` after safe pruning and still detect retention or truncation while reading. A captured earliest offset does not protect data from later expiry. Runtime credentials and engine-specific scheduling remain separate.

## Transport compatibility

- `context_version` is a semantic compatibility version, starting at 1 for this initial public format. It is independent of private execution-split and lake payload versions.
- Readers ignore unknown optional fields within the context, ranges, table paths, bucket identities and baseline objects. Adding a field is compatible only if an old reader can ignore it without changing results. New readers must supply a semantics-preserving default for any new optional field omitted by old producers.
- Fields required by v1, including `partition_filter`, `lake`, log-range `partition` and `baseline`, remain required. `lake` is null for a non-lake table; otherwise it contains `table_path`, `format` and an optional `snapshot_id`. A configured lake without a readable snapshot remains distinct from a non-lake table. Unknown enum kinds, incompatible versions, unsupported schema format versions and malformed known values fail import. Partition identities use explicit wire names `unpartitioned` and `key_values`.
- Changes to field meanings, new required fields or required read semantics need an incompatible version change. A new baseline or log partition kind must fail on readers that do not support it; it cannot be encoded as an ignorable hint. Roll out readers capable of the new contract before producers emit it, or keep producers emitting the old version while old workers remain. There is no version negotiation or automatic downgrade in this API.
- Contexts are accepted only from trusted coordinators. Producers must not include credentials, including in optional fields or the schema envelope. Unknown context-level fields are not retained on reserialization; the original schema JSON is retained.

The lake crate requires Rust 1.91 independently of the core workspace MSRV. Run `cargo test -p fluss-lake --lib` for unit tests. `cargo test -p fluss-lake --features integration_tests --test test_prepare` exercises frozen offsets, schema additions and scoped partition capture. It uses the existing Fluss Docker test-cluster environment and runs in the `rust-integration` job of `client-integration.yml`; no tiering job or lake reader is needed.

## Execution and future modes

Preparation fixes the captured bucket set; partitions created later are not added by workers. The final identity recheck excludes removed or replaced partitions without refreshing the frozen schema or offsets. Capture is not atomic across buckets or partition metadata.

This context describes a bounded read, not streaming progress or a checkpoint. Streaming UnionRead can reuse source identities and baseline seams through an explicit future API; it must define changelog output, discovery and recovery separately. Runtime connections and credentials remain local. Engines own scheduling, retry and result publication. A missing frozen input must fail rather than switch snapshots or advance offsets; earlier batches are only a partial result of a failed attempt.

Row-filter negotiation belongs to the subsequent adapter layer. Ordinary client filters remain strict; adapters may negotiate accepted/remaining conditions. A condition accepted only for pruning must remain for exact evaluation. The boundary API neither serializes query expressions nor silently drops unsupported filters.

Shared historical-log support is tracked in [#5001](https://github.com/apache/fluss/issues/5001); it is separate from reading expired partitions already present in the lake snapshot.

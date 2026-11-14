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

# UnionRead API discussion draft

This document proposes the public interfaces and their usage for review alongside [boundary PR #4887](https://github.com/apache/fluss/pull/4887). It covers the full read flow, beyond that PR's boundary-capture scope. It contains no executable implementation or tests. Signature blocks omit imports, method bodies and private fields; they are interface notation, not compilable Rust or Java source. Usage examples assume the corresponding implementation PRs.

Implementation reference: [full PoC](https://github.com/naivedogger/fluss/tree/fip48-rust-union-read). Related: [umbrella #4713](https://github.com/apache/fluss/issues/4713), [FIP-48](https://cwiki.apache.org/confluence/spaces/FLUSS/pages/444334625/FIP-48+Introduce+a+Union+Read+Kernel+for+fluss-rust).

## Read flow and ownership

```text
FlussLakeTable::new() (no I/O)
  ├─ capture_read_context() → ReadContext
  │                            ├─ shared by multiple scans
  │                            └─ engine-native planning and execution
  └─ new_scan() → FlussLakeScan
                   ├─ plan() → capture boundaries → ReadPlan → ReadSplits
                   ├─ plan_with_context(context) ──┘
                   └─ new_reader() → read_split(s) → Arrow batches
```

| Object | Owns |
| --- | --- |
| Table | Reusable connection, table path and runtime lake properties. No captured schema or boundaries. |
| ReadContext | Frozen source identity, schema, partition scope, optional lake snapshot and concrete log boundaries. No credentials or file tasks. |
| Scan | Projection, predicate, read mode, output batch size and runtime LakeSource. |
| Plan / Split | Output schema, scheduling descriptors and frozen execution inputs. Default split payloads are opaque to engines. |
| Reader | Scan configuration and execution resources. It does not retain or rebuild a plan. |
| LakeSource | Format-specific lake planning and baseline reading, including version reconciliation and deletion vectors. |
| Engine | Task placement, concurrency, cancellation, retries and result publication. Native engines may own physical planning and PK reconciliation. |

## Table and scan interfaces

```rust
impl FlussLakeTable {
    pub fn new(connection: Arc<FlussConnection>, table_path: &TablePath) -> Self;
    pub fn with_catalog_properties(self, properties: HashMap<String, String>) -> Self;
    pub fn new_scan(&self) -> FlussLakeScan;
    pub async fn capture_read_context(&self) -> Result<FlussLakeReadContext>;
    pub async fn capture_read_context_with_partition_scope(
        &self, scope: &PartitionSpec,
    ) -> Result<FlussLakeReadContext>;
}

impl FlussLakeScan {
    pub fn with_projection(self, fields: Vec<usize>) -> Self;
    pub fn with_projection_by_names(self, names: Vec<String>) -> Self;
    pub fn with_filter(self, filter: Predicate) -> Self;
    pub fn with_batch_size(self, batch_size: usize) -> Self;
    pub fn with_lake_only(self, lake_only: bool) -> Self;
    pub fn with_lake_source(self, source: Arc<dyn LakeSource>) -> Self;
    pub async fn plan(&self) -> Result<FlussLakeReadPlan>;
    pub async fn plan_with_context(&self, context: &FlussLakeReadContext) -> Result<FlussLakeReadPlan>;
    pub fn new_reader(&self) -> FlussLakeReader;
}
```

`new()` constructs a table handle synchronously without I/O or existence checks. `with_catalog_properties()` replaces runtime lake overrides for subsequently created scans; it does not capture metadata. Each `plan()` call binds scan settings to current metadata, captures fresh boundaries and plans tasks. Missing tables and invalid lake configuration fail at planning or explicit capture. Existing plans retain their captured inputs; reusing a Table or Scan does not reuse a previous context. Safe partition predicates can reduce offset queries before capture. The initial planner scope is implied STRING partition equalities, including AND conjuncts; other conditions keep a broader capture scope. Explicit capture remains table-wide by default so multiple scans can share one boundary set. `plan_with_context()` preserves the supplied context and never widens its scope or refreshes its boundaries.

A PartitionSpec is an exact conjunction of metadata-encoded partition values, possibly covering only some partition keys. Empty means table-wide. Its scope constrains both lake and log inputs, including lake-only expired partitions. Explicit scoped capture belongs to #4887; predicate-to-scope extraction belongs to the planner PR.

Projection indexes refer to the frozen Fluss schema; duplicate or out-of-range indexes fail, while an empty projection preserves row counts in zero-column batches. Repeated filters combine with AND. Ordinary client predicates must support exact evaluation or fail binding; they are not silently dropped. Predicate acceptance/remaining negotiation belongs to engine adapters. A predicate accepted only for pruning still needs exact evaluation.

Runtime properties and credentials stay local. `new_reader()` captures immutable scan settings without I/O. Fluss-only tables need no lake backend; lake reads use a compatible LakeSource, with the built-in Paimon backend delivered separately. `with_lake_only(true)` requires lake configuration and returns no input before the first readable lake snapshot.

## Boundary interfaces and semantics

Boundary capture and structured import are reviewed in [#4887](https://github.com/apache/fluss/pull/4887). The main consumer-facing accessors are:

```rust
impl FlussLakeReadContext {
    pub fn table_path(&self) -> &TablePath;
    pub fn table_id(&self) -> i64;
    pub fn schema_id(&self) -> i32;
    pub fn table_schema(&self) -> &Schema;
    pub fn partition_keys(&self) -> &[String];
    pub fn partition_filter(&self) -> &BTreeMap<String, String>;
    pub fn bucket_keys(&self) -> &[String];
    pub fn num_buckets(&self) -> i32;
    pub fn lake_table_path(&self) -> Option<&TablePath>;
    pub fn lake_format(&self) -> Option<&str>;
    pub fn lake_snapshot_id(&self) -> Option<i64>;
    pub fn merge_engine(&self) -> Option<&str>;
    pub fn log_ranges(&self) -> &[FlussLakeLogRange];
    pub fn is_lake_only(&self) -> bool;
    pub fn from_lake_snapshot(
        table: &TableInfo, snapshot_id: Option<i64>, scope: &PartitionSpec,
    ) -> Result<Self>;
    pub fn validate_table(&self, table: &TableInfo) -> Result<()>;
    pub fn from_boundary(
        table: &TableInfo, snapshot_id: Option<i64>, ranges: Vec<FlussLakeLogRange>,
    ) -> Result<Self>;
    pub fn from_boundary_with_partition_filter(
        table: &TableInfo, snapshot_id: Option<i64>, ranges: Vec<FlussLakeLogRange>,
        scope: &PartitionSpec,
    ) -> Result<Self>;
}

#[non_exhaustive]
pub enum FlussLakeBaseline { None, Lake }

#[non_exhaustive]
pub enum FlussLakeLogPartition { Ordinary(FlussLakePartitionIdentity) }

pub enum FlussLakePartitionIdentity {
    Unpartitioned,
    KeyValues(Vec<(String, String)>),
}

impl FlussLakeLogRange {
    pub fn new(
        table_bucket: TableBucket, partition: FlussLakeLogPartition, bucket_count: i32,
        snapshot_offset: Option<i64>, stop_offset: i64, earliest_offset: i64,
    ) -> Self;
    pub fn table_bucket(&self) -> &TableBucket;
    pub fn partition(&self) -> &FlussLakeLogPartition;
    pub fn bucket_count(&self) -> i32;
    pub fn baseline(&self) -> FlussLakeBaseline;
    pub fn start_offset(&self) -> i64;
    pub fn stop_offset(&self) -> i64;
    pub fn earliest_offset(&self) -> i64;
    pub fn is_empty(&self) -> bool;
    pub fn validate_available(&self) -> Result<()>;
}
```

The subsequent planner adds lake-only capture and `from_lake_snapshot()`: these contexts omit log boundaries and report `is_lake_only() == true`. They cannot be reused for union scans. An empty log-range list in an ordinary context instead means no live partitions were captured. Lake-only planning skips live-partition discovery and offset RPCs, while preserving schema, snapshot, scope and source-identity checks. These additions are beyond the boundary-only PR's current implementation.

A context fixes source identity and schema, not a global transactional time point. Each log range is half-open `[start, stop)` and uses concrete offsets captured from the server. Per-partition bucket counts take precedence over the table default. A range is a source boundary, not a mandatory thread, split or engine scheduling unit.

A lake baseline starts at its snapshot seam, including a valid seam of zero. Without a baseline, reading starts at the captured earliest retained offset. PK reads then reconstruct only keys represented in retained history, not the full KV state. KV snapshot fallback remains future work.

The default PK UnionRead executor supports Fluss default deduplicate semantics and a deduplicate lake baseline, rejecting other merge engines with `UnsupportedMergeEngine`. A valid lake baseline and its available log tail provide the current view without KV snapshot fallback. Aggregation and partial-update reconciliation remain outside this executor's scope. Boundary capture preserves merge-engine metadata without claiming execution support; native adapters must validate their own capabilities. Lake-only reads delegate merge semantics to the backend.

Capture rechecks partition IDs after offset lookups and skips only confirmed removed or replaced IDs. Other failures propagate. Lake planning must independently cover the selected snapshot: `log_ranges()` is not a complete lake partition inventory. Table identity and lake mapping are checked without replacing the captured schema; readers map data to that schema by stable column ID. Later column additions do not invalidate it.

Contexts are structured local values with no public serde or cross-language wire contract. Engines own task serialization and version compatibility. Workers may use `from_boundary()` or its scoped variant to validate engine-decoded fields without refreshing boundaries. The supplied TableInfo must describe the captured schema and layout, not freshly queried metadata. Import validates consistency and coverage, not the provenance of independently captured values. An adapter must preserve all required semantics and reject unsupported kinds; credentials remain separate. Default tasks below support local execution; their cross-process representation is left to a future adapter.

## Plan, split and reader interfaces

```rust
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

impl FlussLakeReadPlan {
    pub fn read_context(&self) -> &FlussLakeReadContext;
    pub fn schema(&self) -> SchemaRef;
    pub fn split_count(&self) -> usize;
    pub fn splits(&self) -> &[FlussLakeReadSplit];
    pub fn statistics(&self) -> FlussLakePlanStatistics;
}

pub struct FlussLakePlanStatistics {
    pub split_count: usize,
    pub estimated_rows: Option<usize>,
    pub estimated_size: Option<usize>,
}

pub struct FlussLakeReadSplit { /* private local task state; Clone */ }

impl FlussLakeReadSplit {
    pub fn split_id(&self) -> &str;
    pub fn bucket_id(&self) -> i32;
    pub fn partition(&self) -> &FlussLakePartitionIdentity;
    pub fn estimated_rows(&self) -> Option<usize>;
    pub fn estimated_size(&self) -> Option<usize>;
}

impl FlussLakeReader {
    pub fn read_split(&self, split: &FlussLakeReadSplit) -> Result<RecordBatchStream>;
    pub fn read_splits(&self, splits: &[FlussLakeReadSplit]) -> Result<RecordBatchStream>;
}
```

Plans expose the final output schema. Statistics are input estimates, not exact PK result counts; None means unknown. Default Append planning separates lake tasks from bucket log tails. Default PK planning groups the baseline and tail by bucket. Further subdivision is planner/executor work, not a boundary API requirement.

Splits are cloneable local tasks, not a public wire format. Tasks from one plan share frozen source metadata and can outlive the plan. Readers do not require a particular plan object. Callers must use the scan configuration that produced the tasks, including projection, filter, mode and a compatible lake backend. Public getters expose task identities and estimates without allowing task mutation. A future distributed adapter may define a task transport/recovery contract; this API does not predefine one.

`read_splits()` consumes tasks sequentially in the supplied order. Engines and Rust callers control concurrency by scheduling `read_split()` streams themselves. Row order within each task is unspecified. Stream construction is synchronous and validates scan configuration and the task table without I/O. Polling the stream performs asynchronous initialization and reading; initialization failures are stream errors. Dropping a stream cancels its work. For `read_splits()`, the first error terminates the read without starting remaining tasks. When scheduling separate streams, the caller owns cancellation of sibling tasks. Missing required inputs must fail rather than refresh snapshots, advance offsets or silently skip data. The caller discards partial results before retrying with a new context. Capture does not provide retention leases.

`Result<T>` uses `FlussLakeError`. Consumers distinguish invalid contexts or unsupported lake payload versions, unsupported merge semantics, source/schema incompatibility, unavailable frozen data, and connection/planning/execution failures. Diagnostic strings are not a protocol. `DataUnavailable` invalidates the attempt; retrying with a new context may observe different data and is not guaranteed to recover.

## LakeSource extension

```rust
pub enum LakeReadSemantics { Append, PrimaryKey }

pub trait LakeSource: Send + Sync {
    fn format(&self) -> &str;
    fn plan<'a>(&'a self, context: LakePlannerContext<'a>)
        -> BoxFuture<'a, Result<Vec<LakeSplit>>>;
    fn read<'a>(&'a self, context: LakeReaderContext<'a>)
        -> BoxFuture<'a, Result<RecordBatchStream>>;
}

pub struct LakePlannerContext<'a> {
    pub table_info: &'a TableInfo,
    pub snapshot_id: i64,
    pub semantics: LakeReadSemantics,
    pub reconcile_primary_key: bool,
    pub filter: &'a BoundPredicate,
}

pub struct LakeReaderContext<'a> {
    pub table_info: &'a TableInfo,
    pub snapshot_id: i64,
    pub semantics: LakeReadSemantics,
    pub reconcile_primary_key: bool,
    pub splits: &'a [LakeSplit],
    pub projection: &'a [usize],
    pub schema: SchemaRef,
    pub filter: &'a BoundPredicate,
}

pub struct LakeSplit {
    pub format: String,
    pub snapshot_id: i64,
    pub partition: FlussLakePartitionIdentity,
    pub bucket_id: i32,
    pub bucket_count: i32,
    pub estimated_rows: Option<usize>,
    pub estimated_size: Option<usize>,
    pub payload_version: u32,
    pub payload: Vec<u8>,
}
```

The source owns both lake planning and baseline reading. Snapshot IDs are fixed; formats cannot substitute a newer snapshot. `table_info` carries the frozen schema/layout and locally supplied runtime properties. Reader projection indexes refer to the full Fluss schema, include hidden filter/key columns, and define the returned column order and Arrow schema.

For PK reconciliation, the source must return at most one live row per full key across the supplied task group, applying lake versions and deletion vectors first. `reconcile_primary_key` distinguishes that requirement from lake-only reads using other merge semantics. The default executor then applies the Fluss tail and the exact result predicate. Only predicates safe before reconciliation reach the source; ignoring these optimization hints is permitted.

LakeSplit payloads are format-owned, versioned and credential-free. They must retain snapshot-specific deletion-file associations and position information needed for DV application. PK tasks require compatible bucket ownership; Append tasks may use an independent lake layout, including `(-1, -1)` for bucket-unaware tasks. Sources reject unsupported payloads and release resources when reads are dropped. Concurrent requests must not overwrite shared request-specific state.

This is a Rust extension API, not a stable binary ABI. It does not expose a reader-only plugin for opaque default Paimon payloads. Native engines can bypass it and use the boundary contract directly.

## Usage examples

Examples show call relationships, not runnable programs. `connection`, `path`, imports and runtime setup are assumed. `lake_source` is a compatible `Arc<dyn LakeSource>` supplied by the application.

### Default read with a lake backend

```rust
let table = FlussLakeTable::new(connection, &path);
let scan = table.new_scan()
    .with_lake_source(lake_source)
    .with_filter(col("region").eq("US"))
    .with_projection_by_names(vec!["id".into(), "name".into()]);
let plan = scan.plan().await?;
let mut batches = scan.new_reader().read_splits(plan.splits())?;
while let Some(batch) = batches.try_next().await? {
    // Consume within this query attempt.
}
```

The same flow handles Fluss-only tables without `with_lake_source`. Custom lake readers implement LakeSource and use this entry while retaining default log reading and PK reconciliation.

### Shared boundaries and partition scope

```rust
let context = table.capture_read_context().await?;
let plan_a = scan_a.plan_with_context(&context).await?;
let plan_b = scan_b.plan_with_context(&context).await?;

let scope = PartitionSpec::new(HashMap::from([("region", "US")]));
let scoped = table.capture_read_context_with_partition_scope(&scope).await?;
let scoped_plan = scan_a.plan_with_context(&scoped).await?;
```

Scans sharing a scoped context must stay within that scope. Capturing a table-wide context is the default choice for multiple references with different partition selections.

### Local engine execution

```rust
let plan = scan.plan().await?;
let tasks = plan.splits().to_vec();
let reader = scan.new_reader();
drop(plan);
// An engine can assign individual tasks to its in-process execution partitions.
let mut batches = reader.read_split(&tasks[0])?;
```

Tasks retain their planned boundaries and schema without replanning or a retained plan handle. A DataFusion adapter can schedule them in-process. Cross-process task encoding and recovery belong to a concrete distributed adapter, not this initial default reader contract.

### Java boundaries and engine-native execution

Java coordinators can use existing Admin APIs: `getReadableLakeSnapshot()` returns the snapshot and its matching lake/log seams; offset queries capture concrete log bounds. The Flink connector's `LakeSplitGenerator.generateHybridLakeFlussSplits()` already composes lake and log tasks and can be reused where appropriate. Its output is not a Rust context or a portable Rust lake payload. The adapter must preserve the selected snapshot and schema, source identities, per-partition bucket counts and scope, and resolve offset sentinels rather than recapturing a newer snapshot on a worker.

No new Java capture API is included in #4887. A client-only helper can follow if an engine needs a simpler entry without Flink connector dependencies.

```text
Java coordinator: existing snapshot/offset APIs or connector planning
    ↓ engine field mapping and task encoding
Engine: native file tasks / scan ranges / morsels
    ↓ engine distribution and decoding
Workers: native lake reader + bounded Fluss log reader + PK reconciliation
```

Rust workers can construct a validated context from engine-decoded fields via `from_boundary_with_partition_filter()`. An adapter still needs to map metadata/types across languages. Neither a standard context codec nor this adapter's C ABI is supplied here. A shared in-process Rust context needs no encoding.

StarRocks-like engines may choose their own task granularity and native Parquet readers. They must enforce the pinned snapshot, partition scope, schema, log bounds and lake/DV semantics before reconciling PK changes. A raw Parquet reader alone is insufficient for PK baselines. No C ABI, completed SR MPP integration, Python or Elixir UnionRead bindings are proposed by this document.

## Follow-ups and review scope

Boundary capture and structured import belong to #4887. Default planning, Append/PK execution, lake adapters and engine integrations remain separate implementation work. This reference does not require those implementations to merge together.

KV snapshot fallback, shared historical-log reads ([#5001](https://github.com/apache/fluss/issues/5001)), streaming discovery/checkpointing and shared accepted/remaining predicate negotiation remain follow-ups. Selected lake-only expired partitions are distinct from historical writes still present only in shared logs. New semantic kinds require explicit compatibility handling, not silently ignored hints.

Allowing engine-native reconciliation changes the responsibility split in the published FIP and still needs community agreement. Review should settle that ownership boundary, the capture/scan separation, local task configuration obligations and native engine boundary preservation and LakeSource semantics before implementation details.

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
FlussLakeTable
  ├─ capture_read_context() → ReadContext
  │                            ├─ shared by multiple scans
  │                            └─ engine-native planning and execution
  └─ new_scan() → FlussLakeScan
                   ├─ plan() → ReadPlan → ReadSplits
                   ├─ plan_with_context(context) ──┘
                   └─ new_reader() → read_split(s) → Arrow batches
```

| Object | Owns |
| --- | --- |
| ReadContext | Frozen source identity, schema, partition scope, optional lake snapshot and concrete log boundaries. No credentials or file tasks. |
| Scan | Projection, predicate, read mode, output batch size and runtime LakeSource. |
| Plan / Split | Output schema, scheduling descriptors and frozen execution inputs. Default split payloads are opaque to engines. |
| Reader | Scan configuration and execution resources. It does not retain or rebuild a plan. |
| LakeSource | Format-specific lake planning and baseline reading, including version reconciliation and deletion vectors. |
| Engine | Task placement, concurrency, cancellation, retries and result publication. Native engines may own physical planning and PK reconciliation. |

## Table and scan interfaces

```rust
impl FlussLakeTable {
    pub async fn open(connection: Arc<FlussConnection>, table_path: &TablePath) -> Result<Self>;
    pub async fn open_with_properties(
        connection: Arc<FlussConnection>,
        table_path: &TablePath,
        catalog_property_overrides: HashMap<String, String>,
    ) -> Result<Self>;
    pub fn try_from_table_info(connection: Arc<FlussConnection>, table_info: &TableInfo) -> Result<Self>;
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

`plan()` captures boundaries and plans tasks. Safe partition predicates can reduce offset queries before capture. The initial planner scope is implied STRING partition equalities, including AND conjuncts; other conditions keep a broader capture scope. Explicit capture remains table-wide by default so multiple scans can share one boundary set. `plan_with_context()` preserves the supplied context and never widens its scope or refreshes its boundaries.

A PartitionSpec is an exact conjunction of metadata-encoded partition values, possibly covering only some partition keys. Empty means table-wide. Its scope constrains both lake and log inputs, including lake-only expired partitions. Explicit scoped capture belongs to #4887; predicate-to-scope extraction belongs to the planner PR.

Projection indexes refer to the frozen Fluss schema; duplicate or out-of-range indexes fail, while an empty projection preserves row counts in zero-column batches. Repeated filters combine with AND. Ordinary client predicates must support exact evaluation or fail binding; they are not silently dropped. Predicate acceptance/remaining negotiation belongs to engine adapters. A predicate accepted only for pruning still needs exact evaluation.

Runtime properties and credentials stay local. `new_reader()` captures immutable scan settings without I/O. Fluss-only tables need no lake backend; lake reads use a compatible LakeSource, with the built-in Paimon backend delivered separately. `with_lake_only(true)` requires lake configuration and returns no input before the first readable lake snapshot.

## Boundary interfaces and semantics

Boundary implementation and the v1 wire description are reviewed in [#4887](https://github.com/apache/fluss/pull/4887). The main consumer-facing accessors are:

```rust
impl FlussLakeReadContext {
    pub fn context_version(&self) -> u32;
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
    pub fn validate_table(&self, table: &TableInfo) -> Result<()>;
    pub fn to_json(&self) -> Result<Vec<u8>>;
    pub fn from_json(bytes: &[u8]) -> Result<Self>;
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

A context fixes source identity and schema, not a global transactional time point. Each log range is half-open `[start, stop)` and uses concrete offsets captured from the server. Per-partition bucket counts take precedence over the table default. A range is a source boundary, not a mandatory thread, split or engine scheduling unit.

A lake baseline starts at its snapshot seam, including a valid seam of zero. Without a baseline, reading starts at the captured earliest retained offset. PK reads then reconstruct only keys represented in retained history, not the full KV state. KV snapshot fallback remains future work.

Capture rechecks partition IDs after offset lookups and skips only confirmed removed or replaced IDs. Other failures propagate. Lake planning must independently cover the selected snapshot: `log_ranges()` is not a complete lake partition inventory. Table identity and lake mapping are checked without replacing the captured schema; readers map data to that schema by stable column ID. Later column additions do not invalidate it.

Context v1 is a credential-free Java/Rust transport contract. Unknown optional fields may be ignored only when that preserves semantics. Missing required fields, unsupported versions and unknown baseline/log-partition kinds fail. Incompatible changes require a version bump and reader-first rollout; no automatic negotiation or downgrade is proposed. Runtime authentication remains separate from trusting the coordinator's descriptors.

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

pub struct FlussLakeReadSplit {
    pub split_id: String,
    pub bucket_id: i32,
    pub partition: FlussLakePartitionIdentity,
    pub estimated_rows: Option<usize>,
    pub estimated_size: Option<usize>,
    pub descriptor_version: u32,
    // Private execution descriptor, included in serde transport.
}

impl FlussLakeReader {
    pub async fn read_split(&self, split: &FlussLakeReadSplit) -> Result<RecordBatchStream>;
    pub async fn read_splits(&self, splits: &[FlussLakeReadSplit]) -> Result<RecordBatchStream>;
    pub async fn read_splits_with_concurrency(
        &self, splits: &[FlussLakeReadSplit], concurrency: usize,
    ) -> Result<RecordBatchStream>;
}
```

Plans expose the final output schema. Statistics are input estimates, not exact PK result counts; None means unknown. Default Append planning separates lake tasks from bucket log tails. Default PK planning groups the baseline and tail by bucket. Further subdivision is planner/executor work, not a boundary API requirement.

Splits support serde transport. Engines preserve their contents and provide matching projection, filter, mode, batch size and lake backend at execution. Splits do not transport predicates or prove that worker settings match the coordinator. No local plan identity is required. Supported versions, source identities and payloads are validated before use.

`read_splits()` uses bounded concurrency, initially eight active tasks; the explicit limit must be positive. Engines can instead schedule `read_split()` themselves. Output order is unspecified. Opening is lazy, dropping a stream cancels its work, and the first error terminates the read and drops sibling streams. Missing required inputs must fail rather than refresh snapshots, advance offsets or silently skip data. The caller discards partial results before retrying with a new context. Capture does not provide retention leases.

`Result<T>` uses `FlussLakeError`. Consumers distinguish invalid context or split versions, unsupported merge semantics, source/schema incompatibility, unavailable frozen data, and connection/planning/execution failures. Diagnostic strings are not a protocol. `DataUnavailable` invalidates the attempt; retrying with a new context may observe different data and is not guaranteed to recover.

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
let table = FlussLakeTable::open(connection, &path).await?;
let scan = table.new_scan()
    .with_lake_source(lake_source)
    .with_filter(col("region").eq("US"))
    .with_projection_by_names(vec!["id".into(), "name".into()]);
let plan = scan.plan().await?;
let mut batches = scan.new_reader().read_splits(plan.splits()).await?;
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

### Distributed default execution

```rust
// Coordinator: assign a subset in an actual engine.
let task_bytes = serde_json::to_vec(plan.splits())?;

// Worker: matching scan settings and runtime resources supplied by the engine.
let assigned: Vec<FlussLakeReadSplit> = serde_json::from_slice(&task_bytes)?;
let mut batches = worker_scan.new_reader().read_splits(&assigned).await?;
```

Workers do not recapture boundaries or need the coordinator's plan object. DataFusion can wrap these interfaces in its physical execution plan; adapter scheduling and predicate negotiation remain separate work.

### Java capture and engine-native execution

The Java entry is implemented in #4887. Its public signatures are:

```java
public static CompletableFuture<byte[]> captureReadContext(Admin admin, TablePath tablePath);
public static CompletableFuture<byte[]> captureReadContext(
        Admin admin, TablePath tablePath, PartitionSpec scope);
```

```java
byte[] bytes = UnionReadBoundaryPlanner.captureReadContext(admin, tablePath).get();
byte[] scoped = UnionReadBoundaryPlanner.captureReadContext(admin, tablePath, scope).get();
```

One call performs metadata capture, identity rechecks and context encoding without opening lake files. The caller owns Admin. Cancellation of the returned future does not guarantee cancellation of already issued RPCs. Rust imports the same bytes with `FlussLakeReadContext::from_json(&bytes)`.

```text
Java coordinator: captureReadContext
    ↓ distribute context and engine-owned query configuration
Engine: native file tasks / scan ranges / morsels
    ↓
Workers: native lake reader + bounded Fluss log reader + PK reconciliation
```

StarRocks-like engines may choose their own task granularity and native Parquet readers. They must enforce the pinned snapshot, partition scope, schema, log bounds and lake/DV semantics before reconciling PK changes. A raw Parquet reader alone is insufficient for PK baselines. No C ABI, completed SR MPP integration, Python or Elixir UnionRead bindings are proposed by this document.

## Follow-ups and review scope

Boundary capture and transport belong to #4887. Default planning, Append/PK execution, lake adapters and engine integrations remain separate implementation work. This reference does not require those implementations to merge together.

KV snapshot fallback, shared historical-log reads ([#5001](https://github.com/apache/fluss/issues/5001)), streaming discovery/checkpointing and shared accepted/remaining predicate negotiation remain follow-ups. Selected lake-only expired partitions are distinct from historical writes still present only in shared logs. New semantic kinds require explicit compatibility handling, not silently ignored hints.

Allowing engine-native reconciliation changes the responsibility split in the published FIP and still needs community agreement. Review should settle that ownership boundary, the capture/scan separation, worker configuration obligations and LakeSource semantics before implementation details.

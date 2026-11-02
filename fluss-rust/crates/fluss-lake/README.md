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

# Bounded UnionRead API draft

This draft provides real Table / Scan / Plan / Split / Reader APIs, boundary transport, and the LakeSource extension. The supporting planner/executor are included so these APIs compile and run; they remain separate implementation review units. The Paimon backend and its dependencies are deferred to the backend PR. See [the API review guide](../../docs/union-read-api.md) for entry points and ownership contracts.

Ordinary queries use `scan.with_filter(...).plan()`. It extracts safe STRING partition equalities, including AND conjuncts, before offset queries; other conditions retain a wider scope and still undergo later pruning and exact filtering. `table.capture_read_context()` captures the full table for sharing, while `capture_read_context_with_partition_scope()` restricts the input. A query-specific context records its scope and cannot be reused as a table-wide boundary. `plan_with_context()` preserves supplied boundaries rather than recapturing them. There is no public `scan.capture_read_context()`.

### Complete default reader

Provide a `LakeSource` when reading a lake baseline; Fluss-only reads need no lake backend. The caller schedules append
lake/log tasks or PK bucket splits; the default reader opens lake files, reads the
bounded log tail, reconciles PK changes, and applies exact supported predicates.

```rust,no_run
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use fluss::predicate::col;
use fluss_lake::{FlussLakeTable, Result};
use futures::TryStreamExt;
use std::sync::Arc;

async fn query(connection: Arc<FlussConnection>, path: &TablePath) -> Result<()> {
    let table = FlussLakeTable::open(connection, path).await?;
    let scan = table.new_scan()
        .with_filter(col("id").eq(42_i32))
        .with_projection_by_names(vec!["name".to_string()])
        .with_batch_size(4096);
    let plan = scan.plan().await?;
    let reader = scan.new_reader();
    let mut stream = reader.read_splits(plan.splits()).await?;
    while let Some(batch) = stream.try_next().await? {
        // Feed batches into a query attempt that can be discarded on error.
        assert_eq!(batch.schema(), plan.schema());
    }
    Ok(())
}
```

The plan freezes source boundaries and logical tasks, while the reader captures
scan settings. Callers must use the same projection, filter, mode, batch size
and compatible lake backend for planning and execution. The reader does not
retain a plan or check task membership. It rejects duplicate task
IDs within one `read_splits` call. `read_splits(plan.splits())` reads an unordered
stream with at most eight active logical tasks. Use
`read_splits_with_concurrency(..., n)` to set a positive limit, or schedule
`read_split` in the host engine. Dropping the stream cancels active reads; the
first error is terminal and drops sibling streams. Each `read_splits` call lazily shares
one Fluss table metadata load across its tasks. Separate calls, including
`read_split`, start fresh metadata sessions; frozen schemas and boundaries are
unchanged, and core routing refresh remains active.

`with_projection(vec![])` returns zero-column batches with correct row counts.
The reader retains hidden key/filter columns until reconciliation and filtering
are complete. This supports count scans but is not a metadata-only count.

### Execute transported splits on a worker

The coordinator distributes the original serialized splits. The engine supplies
the same projection, filter, read mode and batch-size configuration to workers,
along with a compatible backend and catalog mapping. Credentials stay local.

```rust,no_run
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use fluss::predicate::col;
use fluss_lake::{FlussLakeReadSplit, FlussLakeTable, Result};
use futures::TryStreamExt;
use std::sync::Arc;

async fn worker(
    connection: Arc<FlussConnection>,
    path: &TablePath,
    assigned_splits: Vec<FlussLakeReadSplit>,
) -> Result<()> {
    let table = FlussLakeTable::open(connection, path).await?;
    // Restore the coordinator's scan configuration from the engine's own plan.
    let reader = table.new_scan()
        .with_filter(col("id").eq(42_i32))
        .with_projection_by_names(vec!["name".to_string()])
        .with_batch_size(4096)
        .new_reader();
    let mut stream = reader.read_splits(&assigned_splits).await?;
    while let Some(batch) = stream.try_next().await? {
        // Consume within a query attempt that can be discarded on error.
        let _ = batch;
    }
    Ok(())
}
```

`scan.new_reader()` performs no planning or I/O and captures immutable settings.
It reads the snapshot and log bounds already in the split, without calling
`capture_read_context()`, `plan()` or `plan_with_context()`. The worker needs neither the
coordinator's plan object nor its complete task list.

Configuration consistency and task delivery are the engine's responsibility.
Splits contain no projection/filter or configuration fingerprint; a mismatch
may produce incomplete results or a wrong output schema. Workers still validate
descriptor versions, table/schema identity, partition/key layout and backend
payloads. Those checks do not authenticate tasks or compare remote scan settings:
accept tasks only from a trusted coordinator. These obligations apply to local
callers as well as distributed workers.

### Reuse one boundary across table references

Preparation captures the full table before projection and predicate pruning.
This lets a query, such as a DataFusion self-join, share source state without
sharing its physical plan or filter.

```rust,no_run
use fluss::predicate::col;
use fluss_lake::{FlussLakeReadContext, FlussLakeTable, Result};

async fn plan_references(table: &FlussLakeTable) -> Result<()> {
    let context = table.capture_read_context().await?;
    let received = FlussLakeReadContext::from_json(&context.to_json()?)?;

    let left = table.new_scan().with_filter(col("id").eq(1_i32));
    let right = table.new_scan().with_projection(vec![0]);
    let left_plan = left.plan_with_context(&received).await?;
    let right_plan = right.plan_with_context(&received).await?;
    assert_eq!(
        left_plan.read_context().to_json()?,
        right_plan.read_context().to_json()?
    );
    // Execute left_plan with left.new_reader(), right_plan with right.new_reader().
    Ok(())
}
```

### Coordinator-defined partition scope

The explicit scope is an advanced input restriction, not a second row-filter API. Ordinary queries express conditions through `with_filter()`.

```rust,no_run
use fluss::metadata::PartitionSpec;
use fluss::predicate::col;
use fluss_lake::{FlussLakeTable, Result};
use std::collections::HashMap;

async fn plan_in_scope(table: &FlussLakeTable) -> Result<()> {
    let scope = PartitionSpec::new(HashMap::from([
        ("region".to_string(), "US".to_string()),
    ]));
    let context = table.capture_read_context_with_partition_scope(&scope).await?;
    let scan = table.new_scan().with_filter(col("id").eq(42_i32));
    let plan = scan.plan_with_context(&context).await?;
    assert_eq!(plan.read_context().partition_filter(), context.partition_filter());
    Ok(())
}
```

### Custom lake source

A source owns planning and reading for one lake format. Its opaque payload must retain snapshot-specific deletion information; for PK tables it must produce a logical baseline, not raw file versions. Default execution continues to read Fluss logs, reconcile PK changes and apply the final filter.

```rust,no_run
use fluss_lake::{FlussLakeTable, LakeSource, Result};
use futures::TryStreamExt;
use std::sync::Arc;

async fn query_with_source(table: &FlussLakeTable, source: Arc<dyn LakeSource>) -> Result<()> {
    let scan = table.new_scan().with_lake_source(source);
    let plan = scan.plan().await?;
    let mut batches = scan.new_reader().read_splits(plan.splits()).await?;
    while let Some(batch) = batches.try_next().await? {
        let _ = batch;
    }
    Ok(())
}
```

### Java and engine-native execution

```java
byte[] context = UnionReadBoundaryPlanner.captureReadContext(admin, tablePath).get();
byte[] scoped = UnionReadBoundaryPlanner.captureReadContext(admin, tablePath, scope).get();
```

The Java planner captures the schema, readable snapshot and offsets, applies scope before offset lookup and rechecks table/partition identities. It performs no lake file I/O and does not close the caller's Admin. The encoder is internal. Both Java and Rust return the same v1 JSON format; engines distribute it without recapturing boundaries. A native engine may plan its own scan ranges/morsels and use its own readers and reconciliation. It must honor all input boundaries and the selected partition scope, including lake-only expired partitions. It must not interpret the default split payload as a public file-list protocol.

### Semantics and deferred capabilities

Reads are bounded, unordered and fixed to per-bucket ranges, not a global transactional snapshot or retention lease. Missing required inputs fail execution. Engines own retry and result publication; they must not mix partial results with a newly captured boundary. Streaming discovery and checkpointing are separate future work.

A bucket with a lake seam reads the pinned baseline and `[seam, stop)`. Without a baseline, it starts at the concrete earliest retained offset captured during boundary capture. PK reads in this case reconstruct only keys represented in retained logs. Rust has snapshot metadata/lease APIs but no KV snapshot reader fallback here. Java's Fluss-only PK path can use KV state; this draft does not claim that parity.

The context carries frozen schema and stable identities. Readers map by column ID; later added columns do not replace the captured schema. Context v1 accepts unknown optional metadata, and rejects unknown versions or required semantics. Log partition kinds currently support only Ordinary. Shared historical logs require their own seam alignment and business-partition filtering; lake-only expired partitions do not cover historical writes. See the historical-read follow-up in the API guide.

Append lake tasks and log tails are independent; PK work is grouped by bucket. Further split subdivision is deferred. The ordinary filter is exact and strict; engine adapters can later negotiate accepted/remaining predicates. A pruning-only acceptance still requires residual evaluation.

### Validation

```bash
cargo test --locked -p fluss-lake --lib
cargo test --locked -p fluss-lake --doc
cargo clippy --locked -p fluss-lake --all-features --all-targets -- -D warnings
cargo test --locked -p fluss-lake --features integration_tests --test test_prepare --test test_union_read
```

The last command requires the existing Fluss Docker test image. Java `UnionReadBoundaryPlannerTest`, `UnionReadContextEncoderTest` and `UnionReadBoundaryPlannerITCase` use the normal client test entry. This draft does not include a new CI job or the Paimon tiering fixture. The existing boundary PR wires its boundary-capture IT into `rust-integration`; the execution PRs will add their corresponding CI coverage.

Shared historical-log support is tracked in [#5001](https://github.com/apache/fluss/issues/5001); it is separate from reading expired partitions already present in the lake snapshot.

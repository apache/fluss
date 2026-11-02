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

This PR adds only a design document. The APIs below describe the current local PoC and proposed contracts, not APIs available on main. The code examples illustrate call relationships; this document is not an executable example suite.

Related: [FIP-48](https://cwiki.apache.org/confluence/spaces/FLUSS/pages/444334625/FIP-48+Introduce+a+Union+Read+Kernel+for+fluss-rust), [umbrella issue #4713](https://github.com/apache/fluss/issues/4713), and [source-boundary PR #4887](https://github.com/apache/fluss/pull/4887).

## Entry points

Ordinary Rust callers configure a scan and ask it to plan a bounded read. Coordinators that need shared boundaries prepare a ReadContext explicitly. Both paths use the same scan-created reader.

```text
FlussLakeTable
  ├─ new_scan() → FlussLakeScan
  │                ├─ plan() → ReadPlan → ReadSplits
  │                ├─ plan_with_context(context) ──┘
  │                └─ new_reader() → bounded Arrow batches
  └─ prepare() / prepare_with_partition_scope(spec) → ReadContext
                     ├─ default planner
                     └─ engine-native planning and execution
```

| Entry | Contract |
| --- | --- |
| `scan.with_filter(predicate).plan()` | Capture boundaries, plan tasks and retain the predicate for exact result filtering. Unsupported predicates fail binding. |
| `table.prepare()` | Capture table-wide boundaries without opening a lake catalog or reading lake files. |
| `table.prepare_with_partition_scope(spec)` | Capture boundaries restricted to an exact, possibly partial partition spec. The scope applies to lake and log inputs. |
| `scan.plan_with_context(context)` | Use the supplied schema and boundaries without recapturing or narrowing the context. Apply scan filters during planning and execution. |
| `scan.new_reader()` | Create a reader without planning or I/O. Read transported splits using the coordinator's scan configuration. |
| `scan.with_lake_source(source)` | Replace lake planning and baseline reading; retain default Fluss log reading and PK reconciliation. |

The ordinary planning path currently extracts implied STRING partition equalities, including AND conjuncts, before querying offsets. Other predicates retain a wider capture scope and participate in later pruning and exact filtering. The generated context records its scope. Callers sharing boundaries across different partition selections should use `table.prepare()` rather than reuse a query-specific context as a table-wide snapshot.

An explicit PartitionSpec uses Fluss metadata values and constrains the input, rather than supplying a query-filter hint. An empty spec selects the whole table. There is no public `scan.prepare()` method.

## Rust usage

These snippets show the full PoC API, not the API subset delivered by the boundary-only branch. They assume an existing connection and table path. Use `fluss::predicate::col`, `fluss::metadata::PartitionSpec`, `std::collections::HashMap` and `futures::TryStreamExt` as needed. Paimon reads require the `paimon` feature and runtime catalog configuration.

```rust
let table = FlussLakeTable::open(connection, &path).await?;
let scan = table.new_scan()
    .with_filter(col("region").eq("US"))
    .with_projection_by_names(vec!["id".to_string(), "name".to_string()])
    .with_batch_size(4096);
let plan = scan.plan().await?;
let mut batches = scan.new_reader().read_splits(plan.splits()).await?;
while let Some(batch) = batches.try_next().await? {
    // Consume within the current query attempt.
}
```

For two scans sharing table-wide boundaries:

```rust
let context = table.prepare().await?;
let plan_a = scan_a.plan_with_context(&context).await?;
let plan_b = scan_b.plan_with_context(&context).await?;
```

For scans restricted to a common partition scope:

```rust
let scope = PartitionSpec::new(HashMap::from([
    ("region".to_string(), "US".to_string()),
]));
let context = table.prepare_with_partition_scope(&scope).await?;
let plan = scan.plan_with_context(&context).await?;
```

A scoped context also includes eligible lake-only partitions that have expired from Fluss. An empty list of live log ranges does not imply an empty lake selection. The context accessors and wire field remain named `partition_filter`; the scope entry-point rename does not change the wire format.

## Distributed default execution

The coordinator serializes assigned splits without rewriting their contents. Workers reconstruct the same projection, filter, mode and batch size from the engine's physical plan, and provide a compatible lake source and runtime mapping. Credentials remain local.

```rust
// Coordinator: assign a subset in a real engine.
let task_bytes = serde_json::to_vec(plan.splits())?;

// Worker: worker_scan has the coordinator's configuration.
let assigned: Vec<FlussLakeReadSplit> = serde_json::from_slice(&task_bytes)?;
let mut batches = worker_scan.new_reader().read_splits(&assigned).await?;
```

The worker does not replan or need the coordinator's plan object. Splits carry frozen source/schema information, but do not serialize predicates or prove that remote scan settings match. The engine owns that consistency, task assignment, cancellation, retries and result publication. Readers validate supported task versions, identities and payloads; these checks do not authenticate tasks.

## Custom lake readers and native engines

A Rust integration supplies `Arc<dyn LakeSource>` through `with_lake_source`. The source implements `format`, `plan(LakePlannerContext)` and `read(LakeReaderContext)`. Planning uses the pinned snapshot; reading returns the requested baseline as Arrow batches. For PK tables, the source must apply lake-specific version reconciliation and deletion vectors so that each full key has at most one live baseline row within its task group. Default Fluss reconciliation then applies the log tail and final predicate.

The extension replaces both lake planning and reading. There is no stable reader-only ABI for the default Paimon payload. A native Parquet reader alone does not supply PK baseline semantics.

Java coordinators can encode existing Admin results without introducing another Java boundary object:

```java
byte[] bytes = UnionReadContextEncoder.encode(
        tableInfo, readableSnapshot, livePartitions, earliestOffsets, stoppingOffsets);
```

The overload accepting PartitionSpec encodes a restricted scope. Coordinators must apply the same scope before offset lookup, keep the snapshot and its seam offsets together, and recheck table and partition identities. The encoder validates and serializes supplied inputs; it performs no RPCs. Rust imports those bytes with `FlussLakeReadContext::from_json(&bytes)`.

An engine such as StarRocks can consume the boundary contract, plan its own lake tasks and Fluss ranges, and use native readers and PK reconciliation. It must honor the selected snapshot, scope, log bounds, schema and deletion semantics. Context ranges are source boundaries, not mandatory morsels, threads or BE assignments. Default Append tasks currently separate lake work from bucket tails; PK tasks group work by bucket. Further task splitting belongs in the planning/execution PRs and does not require changing the boundary API.

| Consumer | Current implementation status |
| --- | --- |
| Rust client | Full PoC provides default reads, transported splits and LakeSource extension. The boundary branch provides preparation and transport only. |
| DataFusion | Can use default splits/readers through an adapter. Predicate negotiation and runtime scheduling belong to that adapter. This draft does not deliver the adapter. |
| Java | Thin context encoder implemented; RPC orchestration remains with the coordinator. |
| StarRocks / C++ | Native integration contract only here; no UnionRead C ABI or completed SR MPP integration. |
| Python / Elixir | No UnionRead bindings in this change; SDK examples must follow actual binding APIs when implemented. |

## Result and compatibility contracts

Reads are bounded. A ReadContext fixes table identity, schema, partition scope, an optional lake snapshot and per-bucket `[start, stop)` ranges. These ranges are not a cross-bucket transactional snapshot or a retention lease. Workers use concrete offsets rather than resolving earliest again.

Tables without lake configuration use the same entry. Where a bucket has a lake seam, the pinned baseline plus its log tail form the input. Without a baseline, reading starts at the captured earliest retained offset. Append returns records in the selected input; PK applies updates/deletes and filters the reconciled result. PK reads without a baseline reconstruct only keys represented in retained history, not a complete historical current view. KV snapshot fallback is deferred.

Ordinary filters are strict and exact. Adapters may negotiate accepted/remaining predicates; a condition accepted only for pruning must remain for exact evaluation. A shared negotiation helper is separate adapter work. Output order is unspecified. If required inputs expire, execution fails; callers must not combine partial results with a new query attempt using refreshed boundaries.

Context v1 is an unpublished Java/Rust transport draft. Unknown optional metadata may be ignored; unknown versions or required semantics must fail. Log ranges currently use the extensible `Ordinary` partition association. Shared historical logs and streaming discovery/checkpointing need their own semantics and compatibility review before implementation. Neither an extensible enum nor lake-only expired-partition coverage provides those features today.

## Review and landing scope

Review the complete usage and ownership contracts first. The initial API change should contain independently usable contracts and compiled examples for the APIs it actually provides. Concrete Scan/Plan/Reader methods require their implementations; do not introduce `todo!()` or empty results to make an API-only branch compile. Boundary capture, default planning, the Paimon source and Append/PK execution can land in their implementation PRs after the contracts settle.

The native execution path changes the responsibility split described in the published FIP. That clarification still needs community agreement; this draft does not present it as already accepted.

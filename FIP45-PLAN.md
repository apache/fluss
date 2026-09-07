# FIP-45 revision and implementation plan

Response to the dev@fluss review thread on "Log Enrichment via Append Columns"
(thread `ngl30v1zwvl4w44cmwkoydnysl7r1ypq`, May 25 to Jun 24 2026).
Reviewers: Zhe Wang, Giannis Polyzos, Anton Borisov, Lorenzo Affetti.

Sources: FIP-45 wiki page, the nine thread messages, the `option02-lateMaterialized`
POC branch (last commit `db9df8c6`, Jun 1), and upstream `fluss/` for the seams
the redesign relies on.

---

## 0. Decision summary

| # | Reviewer point | Decision |
|---|---|---|
| G1 | Merge-on-read drops zero-copy (`FileLogProjection`); re-encode on every enrichment projection and `SELECT *` | **Server never re-encodes.** Base and group bytes are served as file slices; the client stitches rows. `EnrichmentMerger` is deleted. |
| G2 / A4 | Re-encode loses `commitTimestamp`, `baseLogOffset`, `lastOffsetDelta` | Confirmed on the branch: merged batches also downgrade V2 to V1, and drop `leaderEpoch`, `writerId`, `batchSequence`, statistics, and the append-only attribute. Fixed by construction: the base batch header is untouched on the wire. Time-travel and `__timestamp` come from the base batch. |
| G3 | `EnrichmentSegment` uses source offset as an `OffsetIndex` slot ordinal (2^31 overflow, O(batch²)) | Confirmed on the branch, and worse: the segment never rolls, so the 10 MiB index caps a bucket at about 1.3M enriched offsets, and each merged cell re-opens a batch iterator. **A column group is a shadow log**: a real `LogSegment` chain whose batch `baseLogOffset` is the source offset. Standard relative-offset sparse index, normal rolling, no custom lookup. |
| G4 / A5 (offset 0) | EWM convention muddled in docs | EWM is defined as the group log's end offset (exclusive, starts at 0), identical to LEO/HW. Javadoc and error strings fixed. |
| A2 | CEW seeded from the new leader's local EWM over-claims and can regress | Confirmed, plus a second failure: CEW is held in memory only, so a full-cluster restart resets every CEW to 0 and column-group readers see nothing until every follower reports again. CEW **is** the group log's high watermark. Same machinery as base HW: min over ISR, propagated to followers in fetch responses, checkpointed off the hot path, seeded from checkpoint on promotion. `testNewLeaderSeedsCewFromLocalEwm` replaced. |
| G5 / A3 / A7 | All-groups tier gate pins disk; late enrichment cannot be written once base leaves local disk | **Base tiering is not gated on enrichment.** Base tiers at HW exactly as today. Group segments tier independently as companion files tracked per range in `RemoteLogManifest`. `appendColumns` only requires `source_offset < base HW`, not local presence of base. The lake materialises a range only when every included group covers it: complete but lagging, never partial. No timeout/null escape valve in v1. |
| A5 | `listOffsets(LATEST)` returns CEW for every caller | Reverted to HW. `ListOffsets` gains an optional `column_group` field; the lake tiering split generator asks per group. |
| A1 / A6 | Client stitch vs server splice: decide on wire format and transparency | **Client stitch.** Wire format changes are additive (`FetchLogResponse` carries per-group record blobs). Java-client readers (Flink, Spark, lake tiering) get stitched rows from one implementation. Non-Java-client readers (Kafka protocol) see base columns only in v1. |
| L2 | Proxy sink table is awkward | **Dropped.** Enrichment is an `INSERT INTO <base table>` with an `enrichment.group` option and a column list of `(<metadata cols>, <group cols>)`, using Flink target columns exactly as the PK partial-update sink does. |
| Z1 | Literal-only SELECT silently emits zero rows | Source projection with only metadata columns pushes down one carrier base column instead of falling back to full projection. Self-gating INSERTs are documented and surfaced by a read-lag metric. |
| L3 | Single global cursor per (bucket, group): who enforces single writer? | Connector owns routing (mandatory bucket-keyed shuffle in enrichment mode, one in-flight batch per bucket). Server owns contiguity and replay tolerance (whole-batch duplicates acked, straddling batches rejected with the expected offset). |
| Z2 | Column group name format | Landed (uncommitted on branch): same identifier rules as table names, max 64 chars. |
| L1 / L4 / A8 | Motivation should lead with pipeline coupling; compare with PK partial update; scope to log tables | FIP text changes in WP0. |

---

## 1. Design changes

### D1. A column group is a shadow log

Replace the POC's `EnrichmentSegment` (custom per-row index) with `ColumnGroupLog`, one per `(bucket, group)`, built from the existing `LogSegment` / `OffsetIndex` / `LogLoader` classes.

- Files keep the FIP naming: `{base}.col.{group}.log` and `.index` next to the base segment files.
- Each batch is a standard V2 log record batch containing only the group's columns. The server stamps `baseLogOffset = first source offset` with `DefaultLogRecordBatch.setBaseLogOffset` on the client-produced bytes. The CRC covers `[schemaId, end)` only (`LogRecordBatchFormat.java:100-104`), so this is the same in-place header stamp `LogTablet.assignOffsetAndTimestamp` (`LogTablet.java:802-813`) already does for base appends. No re-encode.
- Because rows are contiguous from the first source offset, the RPC no longer needs a `source_offsets` array. `PbProduceLogColumnsReqForBucket` becomes `{partition_id?, bucket_id, first_source_offset, records}`.
- **EWM_g = group LEO** (exclusive, starts at 0). **CEW_g = group HW.** These are the only two numbers; there is no separate EWM structure.
- Write validation in `appendColumnsAsLeader`:
  - `first_source_offset == groupLEO` and `last_source_offset < base HW` → append.
  - whole batch `< groupLEO` → ack as duplicate (idempotent replay, mirrors `LogTablet.putAsLeader` duplicate handling at `LogTablet.java:733-743`).
  - batch straddles `groupLEO` → `INVALID_COLUMN_GROUP_OFFSET` with `expected_source_offset` in the error so the client re-slices.
  - `last_source_offset >= base HW` → `INVALID_COLUMN_GROUP_OFFSET` (enrichment cannot run ahead of replicated base).
  - `COLUMN_GROUP_SOURCE_OFFSET_TRUNCATED` is only raised when `first_source_offset < base logStartOffset` (base gone from remote and local), never for "base left local disk".
- **Retention advances the group.** Whenever base retention moves `logStartOffset` past a group's LEO, the group's log start, LEO and HW all advance to `logStartOffset`: offsets that no longer exist are trivially complete. This fixes the branch's dead end where a base that was ever retention-truncated (`localLogStart > 0`, EWM 0) can never accept its first enrichment write, and it lets a group added later by `ALTER TABLE` start from live data instead of vanished history.
- Group names are validated on the RPC path too (`appendColumnsAsLeader`, `SchemaUpdate`), not only in `Schema.Builder` and DDL.
- Truncation: when the base log truncates to `t` (follower reconciling with a new leader), every group log truncates to `min(groupLEO, t)`.
- Recovery: `LogLoader` gets a second pass that loads `.col.{group}.*` segments; group LEO comes from the last segment; group HW from the checkpoint file (D3).
- Retention: a group segment is deletable when (a) its range is below the group's remote end offset (if remote storage is on), or (b) its range is below the base `logStartOffset` (base is gone, group data is unreachable). Group segments never keep base segments alive and vice versa.

### D2. Read path: server zero-copy, client stitch

Server side (`Replica` / `LogTablet.read` / `ServerRpcMessageUtils.makeFetchLogResponse`):

1. Derive the touched groups `G` from the projected fields already carried in `FetchLogRequest`. Empty `G` → today's path, byte for byte, up to HW.
2. Clamp the fetch ceiling to `min(HW, min_{g∈G} CEW_g)`.
3. Read base records with the existing code: an unprojected `FileLogRecords.slice` for `SELECT *`, or `FileLogProjection` for the base subset of the projection (`LogSegment.java:540-584`). The returned bytes are file slices in both cases.
4. Let `[first, last]` be the offset range of the base batches returned. For each `g ∈ G`, read the group log from the batch containing `first` until the batch containing `last`, applying `FileLogProjection` for the requested subset of the group's columns. Group batches are small (three columns of fifty), so this read is not byte-bounded.
5. Response: `PbFetchLogRespForBucket` gains `repeated PbColumnGroupRecords { string group; int64 high_watermark; bytes records; }`. Records use the same file-region send as base (`makeFetchLogResponse` already handles `FileLogRecords`, `BytesViewLogRecords` and `MultiBytesView` with multiple channels).

Client side (`LogFetcher` → `DefaultCompletedFetch` / `RemoteCompletedFetch` → `CompletedFetch`):

- `LogRecordReadContext` gains one sub-context per touched group (group row type, group projection, per-schemaId `VectorSchemaRoot`).
- `CompletedFetch.fetchRecords` becomes a merge-join by `logOffset()`: iterate base records; advance each group cursor to the same offset (skip group rows below the base start, ignore group rows past the base end). Batch boundaries need not align between base and groups.
- `toScanRecord` (`CompletedFetch.java:110-124`) builds the projected `GenericRow` from `(baseColumnarRow, groupColumnarRow_1..n)` through an index map. This is the one place the columnar-to-row copy already happens, so the stitch costs one extra field-getter dispatch per enrichment column and nothing per base column.
- CRC: base batches are validated as today (skipped when projection was pushed down, `CompletedFetch.java:283-287`); group batches likewise.
- Batch metadata (`commitTimestamp`, `baseLogOffset`, `lastOffsetDelta`, `writerId`) is read from the base batch. Offset-based and timestamp-based seeks are unchanged.
- Deleted: `EnrichmentMerger`, `EnrichmentSegment`, the `GroupDecoder` cache, `FlinkSourceSplitReader.forceFullProjectionForColumnGroups`, and the merge branch in `Replica.readRecords` (the clamp stays). The clamp and the touched-group computation both use the latest schema; on the branch the clamp uses the table-creation schema while the merger uses the latest, so a group added by `ALTER TABLE` is merged but not gated.
- A fetch with no projection on a column-group table means `SELECT *`: every group is touched, gated, and shipped. The server derives this itself, so the client no longer has to force an identity projection.

Why not the server splice Anton sketched: it needs a CRC recompute per batch (statistics and record count live inside the CRC range), it defeats `FileLogProjection`'s plan cache for every group combination, and it does not extend to remote reads where the client already downloads segment files itself. The client stitch is one implementation shared by every Java-client consumer.

### D3. CEW durability equals HW durability

- Leader: `Replica.maybeIncrementLeaderHW` (`Replica.java:1084`) is generalised to `maybeIncrementLeaderHW(log)` and called for the base log and for each group log. Follower group LEOs are learned from the follower's fetch request (D3 wire change below), the same way base LEO is.
- Follower: the branch's `follower_ewm_requests` / `enrichment_payload_per_group` / `committed_ewms` fetch extensions are kept, renamed to `PbColumnGroupFetch { group; fetch_offset }` and `PbColumnGroupRecords { group; high_watermark; records }`, and lose the `source_offsets` array. The same response shape serves consumers (D2) and followers. The response's per-group `high_watermark` updates the follower's group HW to `min(leaderCEW, localGroupLEO)`, as `ReplicaFetcherThread.java:585` does for base.
- Checkpoint: `ReplicaManager.checkpointHighWatermarks` (`ReplicaManager.java:1565`) also writes `column-group-high-watermark-checkpoint` (an `OffsetCheckpointFile` variant keyed by `(tableId, partitionId?, bucket, group)`). `Replica.createLog` seeds each group HW from it, default 0.
- Promotion: `onBecomeNewLeader` seeds nothing from local LEO. The new leader's CEW is its last known group HW (in memory or checkpoint), then advances as followers report. This can only under-claim, and regression across failover is bounded exactly as it is for HW. The CEW is monotonic within a leader epoch.
- ISR: enrichment lag does not affect ISR membership (failure isolation is a goal). A follower in sync on base but behind on a group holds that group's CEW back and only readers of that group lag. Exposed as `columnGroupReplicaLag{group}`.
- Tests: `testNewLeaderSeedsCewFromLocalEwm` is deleted and replaced by `testNewLeaderSeedsCewFromCheckpoint`, `testCewNeverExceedsMinIsrGroupLeo`, `testCewDoesNotRegressPastReadRowsOnCleanFailover`, `testGroupHwPropagatesToFollowers`.

### D4. Tiering decoupled from enrichment

Adopts Anton's Jun 23 proposal.

Base:
- `LogTieringTask` for base segments is unchanged: rolled segments below HW upload, local base segments are deleted per `tieredLogLocalSegments`. Disk usage for base is bounded exactly as for a plain log table.

Groups:
- In the same `LogTieringTask.runOnce`, after base: for each group, rolled group segments whose end offset `≤ CEW_g` upload as `{start}.col.{group}.log` / `.index` into the same remote bucket directory (own segment uuid, via `LogSegmentFiles` extended with the companion bundle).
- `RemoteLogManifest` gains `columnGroupSegments: Map<group, List<RemoteColumnGroupSegment{start, end, uuid, size}>>`. JSON serde is versioned; old manifests deserialize with an empty map. Commit path (`tryToCommitRemoteLogManifest`, coordinator upsert, `NotifyRemoteLogOffsets`) is reused; the notify payload adds per-group remote end offsets.
- Group segments are deleted locally once below the group's remote end offset (D1 retention). Remote group segments expire together with the base remote range they cover (`RemoteLogTablet.expiredRemoteLogSegments` gates on lake end offset already).

Writing enrichment for a range whose base has left local disk:
- Nothing special. The enrichment job reads base through the normal scanner, which serves remote segments transparently, and `appendColumns` only checks `first == groupLEO` and `last < base HW`. The group log is local and small; it is tiered later by the loop above. This removes the wedge Anton identified: freeing base disk no longer destroys the ability to enrich the freed range.

Remote reads:
- `PbRemoteLogSegment` gains `repeated PbRemoteColumnGroupSegment` for the groups the projection touches. `RemoteLogDownloader` downloads base plus companions; `RemoteCompletedFetch` stitches through the same D2 client code.
- Mixed sourcing is allowed and explicit: when base is remote but the group range is still local (not yet tiered), the response carries the remote base descriptor and the group bytes inline. When a group companion for the range exists neither locally nor remotely, the fetch is clamped by CEW_g anyway, so this cannot produce a partial row.

Lake:
- `TieringSplitGenerator` end offset per bucket for a column-group table is `min(HW, min over included groups of CEW_g)`, obtained through the group-aware `ListOffsets` (D5). `computeTierSafeEndOffset` moves out of the server.
- Lake writers stay unchanged; rows arrive stitched from the client. The Paimon/Iceberg/Lance ITCases from Phase F keep their assertions.
- New table option `table.datalake.excluded-column-groups` (Phase F §6.5) lets an operator keep a group out of the lake and out of the lake progress gate.
- Answer to Anton's question: there is no escape-valve window. A lake reader sees rows only once every included group covers them. A timeout-to-null mode is listed as a possible opt-in follow-up FIP, not part of v1.

### D5. `listOffsets`

- `LATEST` for a client returns HW again. Both halves of the branch change go: `ReplicaManager.computeTierSafeEndOffset` and the `tier_safe_end_offset` response field on the server, and the `Math.min` that `FlussAdmin.listOffsets` applies to every caller on the client.
- `ListOffsetsRequest` gains optional `column_group`. With it, `LATEST` returns CEW_g and `LEADER_END_OFFSET_SNAPSHOT` returns group LEO (used by the enrichment writer on open to skip already-filled offsets after a restart).

### D6. Flink: direct write, no proxy table

Base table DDL (unchanged option, new metadata columns):

```sql
CREATE TABLE device_logs (
  dt STRING, device_id STRING, ip STRING,
  geo_region STRING,
  _partition STRING METADATA FROM 'partition',
  _bucket    INT    METADATA FROM 'bucket',
  _offset    BIGINT METADATA FROM 'offset'
) WITH ('column-groups.enriched_geo' = 'geo_region');
```

Enrichment job:

```sql
INSERT INTO device_logs /*+ OPTIONS('enrichment.group' = 'enriched_geo') */
  (_partition, _bucket, _offset, geo_region)
SELECT _partition, _bucket, _offset, geo_lookup(ip) FROM device_logs;
```

- `FlinkTableSource` implements `SupportsReadingMetadata` for `partition`, `bucket`, `offset` (the POC's `MetadataAppender` is reused; upstream has no metadata support today).
- `FlinkTableSink` implements `SupportsWritingMetadata`. When `enrichment.group` is set, `FlinkTableFactory` builds the sink in enrichment mode: `Context.getTargetColumns()` must be exactly the three metadata columns plus the group's columns, otherwise `ValidationException` at plan time. This is the same target-column mechanism the PK partial-update sink uses, which is the comparison Lorenzo asked for.
- Routing (L3): enrichment mode forces a pre-write shuffle keyed on `(_partition, _bucket)` values in `FlinkSink.addPreWriteTopology`, so each `(partition, bucket, group)` is owned by one subtask. The `EnrichmentSinkWriter` keeps a per-bucket FIFO, allows one in-flight batch per bucket, and on `open()` reads the group LEO and drops rows below it (checkpoint replay). Async lookups must keep ordered output (Flink's default). The server contiguity check is the safety net, not the mechanism.
- Z1: `FlinkTableSource.applyProjection` with no physical columns pushes down the first default-group column as a carrier instead of falling back to full projection, so a literal-only enrichment SELECT reads up to HW. A job whose SELECT projects the group it writes gates itself; this is documented and visible through `columnGroupReadLag`.
- `enrichment.target`, `EnrichmentTableSink`, and the proxy table move to Rejected alternatives.

Verification item: Flink accepts `INSERT INTO t /*+ OPTIONS(...) */ (cols) SELECT` on 1.18+; confirm on the 2.2 module too.

### D7. FIP text

- Motivation leads with pipeline coupling (a slow enrichment job stalls every downstream consumer of table B; CEW gating confines the stall to readers of that group), then storage and I/O.
- New section "Why not primary-key partial update": PK tables do read-modify-write on a key and emit a full row at a new offset; log tables have no key, no update, and offset-addressed replay, so the column group is the log-table equivalent. Scope statement per A8.
- EWM/CEW definitions rewritten as group LEO / group HW.
- Column group name rules (Z2) in Public interfaces.
- Tiering section replaced with D4; read path with D2; Flink section with D6; RPC section with the simplified proto.

---

## 2. Work packages

Order is the dependency order. Each package is one reviewable PR series on the fork with its own tests.

| WP | Scope | Key files | Tests | Size |
|---|---|---|---|---|
| **WP0** FIP revision and thread reply | D7 plus the decision table above posted to the thread | `FIP-LOG-ENRICHMENT.md`, wiki | n/a | S |
| **WP1** Shadow log storage | `ColumnGroupLog` on `LogSegment`; `LogLoader` second pass; append validation and replay rules; truncation; retention; proto `first_source_offset`; error messages | `fluss-server/.../log/ColumnGroupLog.java` (new), `LogTablet.java`, `LogLoader.java`, `FlussApi.proto`, `Errors.java` | `ColumnGroupLogTest` (index, roll, recover, truncate), `LogTabletColumnGroupTest` (contiguity, duplicate ack, straddle error, `< base HW`), delete `EnrichmentSegmentTest` | L |
| **WP2** Replication and CEW | Group HW; follower fetch extension; propagation; checkpoint file; promotion seeding | `Replica.java`, `ReplicaManager.java`, `ReplicaFetcherThread.java`, `OffsetCheckpointFile.java`, `FlussApi.proto` | `ReplicaColumnGroupTest` (min over ISR, propagation), failover tests from D3, `ReplicaManagerTest` checkpoint round-trip | L |
| **WP3** Read path and `listOffsets` | Server per-group zero-copy payload with projection; client stitch; `LATEST` = HW; group-aware `ListOffsets` | `LogTablet.read`, `Replica.java`, `ServerRpcMessageUtils.java`, `FetchParams.java`, `LogRecordReadContext.java`, `CompletedFetch.java`, `DefaultCompletedFetch.java`, `LogFetcher.java`, `Replica.getLatestOffset` | Base-only fetch byte-identical to plain table; `SELECT *` returns unprojected file slice; commitTimestamp and offset seek preserved (G2); misaligned batch merge-join; multi-group projection; `ListOffsetsITCase`; delete `EnrichmentMergerTest` | L |
| **WP4** Remote tiering | Companion upload; manifest v2; group retention; remote read with companions; mixed sourcing | `LogTieringTask.java`, `LogSegmentFiles.java`, `RemoteLogManifest*.java`, `RemoteLogTablet.java`, `DefaultRemoteLogStorage.java`, `RemoteLogDownloader.java`, `RemoteCompletedFetch.java` | Base tiers while group lags (disk bound); enrich a range after base left local disk (the wedge test); manifest serde compatibility; remote read stitched; expiry | L |
| **WP5** Lake tiering | Generator end offset via group `ListOffsets`; excluded groups option; remove server `computeTierSafeEndOffset` | `TieringSplitGenerator.java`, `TieringSplitReader.java`, `ConfigOptions.java` | Existing Paimon/Iceberg/Lance column-group ITCases; excluded-group ITCase; lake never leads CEW | M |
| **WP6** Flink SQL | Metadata read/write; enrichment-mode sink with target columns; bucket-keyed shuffle; replay skip; Z1 carrier projection; drop proxy table | `FlinkTableSource.java`, `FlinkTableSink.java`, `FlinkTableFactory.java`, `FlinkSink.java`, `EnrichmentSinkWriter.java`, `FlinkConnectorOptions.java`, `FlinkConversions.java` | DDL round-trip; plan-time validation (wrong column list, SELECT metadata only); literal-only SELECT produces rows; end-to-end enrichment with restart and replay; two groups by two jobs | L |
| **WP7** Client batching | `EnrichmentAccumulator` in-order per bucket, one in flight, resync on `expected_source_offset`; coalesce buckets into one `ProduceLogColumns` RPC per leader (the branch sends one RPC per bucket batch); retry with metadata refresh on `NotLeaderOrFollower` (the branch fails the futures); per-key locking instead of the global `perKeyAppendLock`; acks plumbing kept | `EnrichmentAccumulator.java`, `EnrichmentWriteBatch.java`, `EnrichmentRouter.java`, `EnrichmentSender.java`, `WriterClient.java` | Straddle resync; ordering under retries; leader change mid-stream; acks=all waits for CEW | M |
| **WP8** Hardening | Metrics (`columnGroupLeo`, `columnGroupHw`, `columnGroupReplicaLag`, `columnGroupReadLag`, `columnGroupRemoteEndOffset`); docs; JMH fetch benchmark | `fluss-jmh`, `website/docs` | Benchmark: base-only fetch throughput equals plain table; stitched fetch CPU per row | M |

Parallelism: WP6 and WP7 can start once WP3's wire format is fixed. WP4 and WP5 are sequential after WP3.

Suggested branch: start a fresh `fip45-v2` branch from upstream main and cherry-pick from `option02-lateMaterialized` (90 files, about 16k lines added) only what survives the redesign:

- keep: `Schema` column groups and name validation, JSON serde, `FlussPaths` naming, error codes, `SchemaUpdate` group handling (Phase H), partitioned-table invariants (Phase M), `MetadataAppender` and `SupportsReadingMetadata`, `FlinkConversions.parseColumnGroups`, the follower fetch proto extensions, the acks plumbing, the accumulator skeleton, and the Paimon/Iceberg/Lance column-group ITCases;
- drop: `EnrichmentSegment`, `EnrichmentMerger`, `readEnrichmentForFollower`'s per-row payload, `computeTierSafeEndOffset` and its client-side `Math.min`, `forceFullProjectionForColumnGroups`, `EnrichmentTableSink`, `enrichment.target`, and `testNewLeaderSeedsCewFromLocalEwm`.

Of the 67 test methods on the branch, the 13 in `ColumnGroupEWMITCase`, the 12 Flink DDL and metadata tests, the 6 `SchemaUpdateTest` tests, and the 5 lake ITCases carry over with small edits; the `LogTabletTest`, `ReplicaTest`, and `PhaseEFetchLogSerdeTest` additions are rewritten against the shadow log.

---

## 3. What to post on the thread

1. Thank the four reviewers; list the decisions from section 0 in the same order as their points.
2. State the two structural changes: column group as a shadow log with client-side stitching (answers Giannis and Anton on zero-copy and batch metadata), and base tiering independent of enrichment with companion remote segments (answers Anton's disk analysis and the lake question).
3. Answer Lorenzo's three points with the new DDL, the routing contract, and the PK comparison paragraph.
4. Ask for input on two remaining choices:
   - Non-Java-client readers (Kafka protocol) see base columns only in v1. Acceptable?
   - Should `table.datalake.excluded-column-groups` be per table (proposed) or should a group declare its own lake eligibility?
5. Link the updated wiki page and the `fip45-v2` branch once WP1 to WP3 are green.

---

## 4. Risks and open items

- **Fetch size accounting.** Group bytes are added on top of `maxBytes` for the base read. Bounded by the group's column count, but the response-size metric and client buffer sizing should include them.
- **Flink hint plus column list syntax.** Confirm `INSERT INTO t /*+ OPTIONS(...) */ (cols) SELECT` parses on every supported Flink version; fallback is a `SET`-scoped option.
- **Nullability of group columns.** A group column may be `NOT NULL`: the contract requires a value for every offset, and rows are never visible before the value lands. Nulls in a group column mean the job wrote null. State this in the FIP.
- **Schema evolution on a group** stays a non-goal for v1; the shadow-log design keeps a schemaId per group batch, so adding a column to a group later is the same problem as adding one to the base.
- **Two jobs writing one group.** Deterministic duplicate writes are acked; a nondeterministic second writer loses (its offsets are already filled). Documented as "single logical writer per group".
- **Kafka protocol reads** return base columns only until a follow-up adds stitching in `fluss-kafka`.

---

## 5. Proof of concept status (2026-09-03)

Branch `fip45-v2`, based on upstream main `da69aee23`. See `FIP45-POC.md` on the branch for the file map and the exact contract.

Implemented end to end and covered by `ColumnGroupLogTest` (server) and `ColumnGroupITCase` (client):

- D1 shadow log: `ColumnGroupLog` on standard `LogSegment`s under `{tabletDir}/col-{group}/`, base offsets stamped in place, LEO as enrichment watermark, HW as committed enrichment watermark, replay tolerance, straddle rejection with the expected offset, base-HW bound, retention advance, truncation with the base log, recovery on reopen.
- D2 read path: server-side gating at `min(HW, CEW_g)` rounded to the batch boundary, base bytes served as file slices (unprojected or `FileLogProjection`), group bytes shipped as additional zero-copy payload, client merge-join in `CompletedFetch`. Base batch timestamps survive stitching. Base-only projections on a column-group table take the unchanged path.
- D5: `listOffsets(LATEST)` returns HW; the request carries an optional `column_group`.
- Client API: `AppendWriter.appendColumns(group, bucket, firstSourceOffset, rows)`; `append(row)` writes only the default-group columns to the base log.

Deferred, with hooks in place: follower replication and CEW checkpointing (WP2: proto fields and leader-side cursor map exist, follower thread untouched), remote tiering and remote reads (WP4), lake tiering (WP5), Flink (WP6), client batching and acks (WP7).

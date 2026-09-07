# FIP-45 proof of concept: column groups as shadow logs

Branch `fip45-v2`, based on `apache/fluss` main at `da69aee23` (2026-09-03).

This branch implements the revised FIP-45 design that answers the dev@ review of the first
proposal (see `FIP45-PLAN.md`). It covers work packages
WP1 (storage) and WP3 (read path and `listOffsets`) of that plan end to end, plus the minimum of
WP7 (client write API) needed to drive them, and it ships an integration test proving the
contract.

## What changed, in one paragraph

A column group is a **shadow log**: a chain of ordinary `LogSegment`s stored under
`{tabletDir}/col-{group}/`, whose batches hold only the group's columns and whose batch base
offsets are the base-log offsets they fill. The group's log end offset is the enrichment
watermark and its high watermark is the committed enrichment watermark, so the base log's index,
recovery, truncation and zero-copy read code apply unchanged. The base log of a column-group
table physically stores only the default-group columns. On a fetch, the server derives the touched
groups from the projection, clamps the read at `min(HW, CEW_g)`, serves the base batches as file
slices exactly as today (unprojected or through `FileLogProjection`), and ships each touched
group's batches for the same offset range as additional zero-copy payload. The client
merge-joins base and group rows by offset when it materialises `ScanRecord`s, so batch metadata
(`commitTimestamp`, `baseLogOffset`, `writerId`, statistics) is never rewritten.

## Files

| Area | Files |
|---|---|
| Schema | `Schema` (column groups, base/group row types, name validation), `ColumnJsonSerde`, `TableDescriptor`, `TablePath`, `ColumnGroupSchemaGetter` |
| Protocol | `FlussApi.proto`: `ProduceLogColumns*`, `PbColumnGroupRecords` on fetch responses, `PbColumnGroupFetch` on fetch requests, `column_group` on `ListOffsetsRequest`; `ApiKeys.PRODUCE_LOG_COLUMNS`; `Errors` 74 to 77; `TabletServerGateway.produceLogColumns` |
| Server storage | `ColumnGroupLog`, `ColumnGroupAppendInfo`, `LogTablet` (load, append validation, ranged read, bounded read, truncation), `LocalLog.convertToBatchEndOffsetMetadata`, `FlussPaths.columnGroupLogDir` |
| Server read/write | `Replica` (gate, group payload, group high watermark, `appendColumnsAsLeader`, group `listOffsets`), `ReplicaManager.appendColumnsToLog`, `ColumnGroupFetchPlan`, `FetchParams`, `LogReadInfo`, `ServerRpcMessageUtils`, `TabletService.produceLogColumns`, `FileLogProjection.lastProjectedOffset` |
| Client write | `AppendWriter.appendColumns`, `AppendWriterImpl` (base-only physical row), `ColumnGroupWriter`, `WriterClient`, `RecordAccumulator` (base row type) |
| Client read | `ColumnGroupReadPlan`, `ColumnGroupStitcher`, `CompletedFetch`, `DefaultCompletedFetch`, `LogFetcher`, `LogRecordReadContext` (physical row type factory) |
| Tests | `ColumnGroupLogTest` (server), `ColumnGroupITCase` (client), plus the schema tests from the first POC |

## Contract implemented

- `appendColumns(group, bucket, firstSourceOffset, rows)`: `firstSourceOffset` must equal the
  group's log end offset; a batch entirely below it is acknowledged as a duplicate; a batch that
  straddles it or leaves a gap fails with `InvalidColumnGroupOffsetException` carrying the expected
  offset; the last row must be below the base high watermark.
- Offsets that base retention removed are trivially complete: the group log advances to the base
  log start offset before validation.
- Reads whose projection touches no group read to HW, byte for byte as before. Reads touching
  groups are clamped at the smallest committed enrichment watermark among them. Because batches are
  never split, the base read may include the batch containing the watermark; group rows are shipped
  only up to the watermark, and the client stops exactly there and fetches again from it.
- A projection with only group columns carries the first base column so the fetch advances.
- `listOffsets(LATEST)` returns the base high watermark for every caller. With `column_group` set
  it returns the group's high watermark; `LEADER_END_OFFSET_SNAPSHOT` returns the group's log end
  offset.

## Running the tests

```bash
./mvnw -o install -DskipTests -pl fluss-common,fluss-rpc,fluss-server,fluss-client
./mvnw -o test -pl fluss-server -Dtest=ColumnGroupLogTest
./mvnw -o verify -pl fluss-client -Dtest=ColumnGroupITCase -Dit.test=ColumnGroupITCase \
    -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false
```

## Not in this proof of concept

- **Follower replication of column groups (WP2).** The fetch protocol carries the follower cursor
  field and the leader tracks reported follower end offsets, but `ReplicaFetcherThread` does not
  send cursors or append shipped group records yet, and the group high watermark is not
  checkpointed. A follower that has not reported is not counted in the group high watermark, so
  with replication factor greater than one the committed enrichment watermark currently equals the
  leader's enrichment watermark. The integration tests use replication factor 1.
- **Remote tiering of group segments and remote reads with companions (WP4)**, lake tiering
  changes (WP5) and the Flink connector (WP6).
- **Client batching, one-in-flight ordering, leader-change retries and resync on
  `expected_source_offset` (WP7).** Every `appendColumns` call is one request.
- **`acks`** on `ProduceLogColumns` is accepted but the response is sent after the local append.
- Projection of a subset of a group's columns on the server (all group columns are shipped; the
  client selects), Arrow-batch polling on projections touching groups, group segment retention,
  and metrics.

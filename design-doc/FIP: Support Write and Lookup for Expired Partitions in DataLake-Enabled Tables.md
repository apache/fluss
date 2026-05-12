# FIP: Support Write and Lookup for Expired Partitions in DataLake-Enabled Tables

## Motivation

For auto-partitioned partitioned tables (Primary Key tables and Log tables) with lake tiering enabled, Fluss will expire old partitions from metadata.
After expiration, users face two gaps:

1. **Lookup gap (Primary Key tables)**: point lookup on expired partitions returns `null` in current behavior, because once Fluss determines the partition does not exist, the row is treated as not found, even though data may still exist in lake storage.
2. **Write gap**: late-arriving updates/inserts for expired partitions are rejected.

This is confusing for users because batch/lake reads can still observe historical data while Fluss online paths cannot.

This FIP proposes a unified solution for both read and write:

- read historical partition data through server-side lake lookup fallback (Primary Key tables),
- write historical partition data through one dedicated special Fluss partition (`__historical__`) for both Primary Key and Log tables,
- allow downstream consumers to consume late records of expired original partitions from this partition.

Scope and eligibility:

- This FIP only supports tables with Paimon-enabled lake storage.
- For Primary Key tables, this is a hard technical requirement: historical writes need old-value resolution to generate correct changelog (`UPDATE_BEFORE` + `UPDATE_AFTER`), and the old value for expired partitions only exists in lake storage. Without a lake backend, old-value lookup is impossible and the write path cannot produce correct results.
- For Log tables, historical writes are technically feasible without lake storage (append-only, no old-value resolution needed). However, we still restrict this FIP to lake-enabled tables to avoid a fragmented feature matrix — supporting historical writes for lake-enabled Log tables but not for non-lake Log tables would create inconsistent user expectations across the same table type.

Note: This FIP primarily focuses on Paimon as the lake storage backend, because Paimon provides efficient point lookup capabilities via its `LocalTableQuery` API. Other lake formats such as Iceberg and Lance do not currently offer comparable point query performance, so support for them can be considered in future FIPs, which will required a full cache on Iceberg.

## Public Interfaces

### RPC Extensions

Extend lookup RPC for historical partition lake lookup:

```protobuf
message PbLookupReqForBucket {
  // existing fields...
  optional string partition_name = N;  // original partition name for historical lookup
}
```

- `partition_name` carries the original partition name when the lookup targets `__historical__` partition.
- Server uses this field to determine which lake partition to query.
- Field is optional; absent for normal lookups.

This reuses the existing `LookupRequest` instead of introducing a separate RPC. The dispatch strategy (synchronous local lookup vs. async lake lookup) is determined server-side based on whether the target is `__historical__` partition — the same approach used for the write path.

Extend put-kv RPC for deterministic historical delete routing:

```protobuf
message PbPutKvReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  required bytes records = 3;
  optional string partition_name = 4;
}
```

- `partition_name` carries the original partition before redirecting to `__historical__`.
- This allows server-side deterministic routing for key-only delete (`row == null`) in `__historical__` path.
- Field is optional for backward compatibility and only needed when partition cannot be derived from row payload.

### New SPI Interface

Expose table-level lake point lookup through `LakeStorage`:

```java
public interface LakeStorage {
    // existing methods...

    /**
     * Create a {@link LakeTableLookuper} for the given table to perform point lookups
     * against lake storage for expired partitions.
     */
    default LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
        throw new UnsupportedOperationException(
                "Point lookup is not supported for this lake storage.");
    }
}

/**
 * An interface for performing point lookups against lake storage for expired partitions.
 *
 * <p>Each instance is bound to a specific table and caches per-table resources
 * (e.g., catalog connections, table metadata) for efficient repeated lookups.
 *
 * <p>The key bytes passed to {@link #lookup} are already encoded in the lake storage's
 * native format (e.g., Paimon BinaryRow format) by the client-side encoder, so
 * implementations can use them directly without re-encoding.
 */
public interface LakeTableLookuper {
    /**
     * Lookup a single key from lake storage for an expired partition.
     *
     * <p>The key bytes are already encoded in the lake storage's native key format
     * by the client-side encoder (e.g., Paimon BinaryRow format). Implementations
     * can wrap them directly as the lake storage's key type without decode/re-encode.
     *
     * <p>The returned value bytes should be encoded with a schema ID prefix
     * so the client can correctly decode them.
     *
     * @param key the encoded key bytes to lookup, in the lake storage's native key format
     * @param context the lookup context containing partition and bucket information
     * @return the encoded value bytes, or null if the key is not found
     * @throws Exception if the lookup fails
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    class LookupContext {
        /** Original partition spec resolved from Fluss. */
        ResolvedPartitionSpec partitionSpec;
        /** Bucket id used by lake lookup for this table partition. */
        int bucketId;
        /** Schema id used to encode value bytes returned to Fluss-compatible format. */
        int schemaId;
    }
}
```

## Proposed Changes

### A. Historical Partition Write Path (Log + Primary Key Table)

#### A.1 Historical Partition

For each eligible table, maintain one special partition (name `__historical__`) as the write target for all expired partitions. In this FIP we call it **Historical Partition**.

Properties:
- always writable,
- not auto-expired,
- normal replication/WAL behavior,
- bucket count same as other partitions of the table.

When a write targets an expired original partition, client redirects it to the historical partition; original partition identity remains in row partition columns. This applies to both Primary Key writes and Log appends.

**Expired partition predicate**:

A partition is considered "expired" (and eligible for `__historical__` redirect) only when **all** of the following are true:

1. The table is an auto-partitioned table with lake tiering enabled (eligible table).
2. The partition name matches the table's auto-partition spec naming pattern (e.g., valid date format for date-partitioned tables).
3. The partition name falls before the TTL expiration boundary computed from the auto-partition spec and current time (i.e., it is older than the retention window).
4. The partition does not exist in current metadata.

If condition 4 is true but any of conditions 1–3 are false, the client must **not** redirect to `__historical__` and should preserve the original `PartitionNotExistException`. This prevents invalid partition names, future partitions, or non-eligible tables from being incorrectly routed to the historical path.

The predicate is evaluated client-side using the table's auto-partition spec and TTL configuration (already available in client metadata cache).

**Creation**:
- `__historical__` is lazily created by the client when it first encounters an expired partition (as defined by the predicate above) — regardless of whether the operation is a write or a lookup. The client checks whether `__historical__` exists in cached metadata, and if not, triggers creation via the partition creation RPC.
- Although `__historical__` creation reuses the existing partition creation RPC, metadata manager, and replica assignment infrastructure, it is treated as a **system partition creation** and is **not controlled by the user-facing `dynamicPartitionEnabled` setting**. This ensures that `dynamicPartitionEnabled = false` tables can still use historical write/lookup — the `__historical__` partition is a system-internal mechanism, not a user-created business partition. Server-side creation validation allows `__historical__` unconditionally for eligible tables (auto-partitioned, Paimon lake-enabled) regardless of the dynamic partition flag.
- If creation fails (e.g., transient coordinator error), the operation fails and the client retries on the next attempt, which will trigger creation again.
- If multiple clients race to create `__historical__` concurrently, only one succeeds; the others receive `PartitionAlreadyExistException` and proceed normally.

**Name reservation**:
- The partition name `__historical__` is reserved by the system. User attempts to create a partition with this name (via DDL or dynamic partition creation) are rejected with an error. This is enforced in the partition creation validation path.

**AutoPartitionManager exclusion**:
- `AutoPartitionManager` recognizes `__historical__` as a system partition and unconditionally skips it during TTL expiration checks. This is implemented by checking the partition name before applying TTL logic.

**Partition discovery and consumer visibility**:
- `__historical__` is included in `listPartitions` results like any normal partition. Consumers discover it through the standard partition discovery mechanism and can subscribe to its buckets.
- Metadata APIs (e.g., `getPartition`) treat it as a normal partition. No special filtering is applied — the partition is fully visible to clients.

#### A.2 Log-table write path (client -> server)

Client-side handling:

1. Producer resolves target partition from record partition columns.
2. If target partition exists, follow normal log write path.
3. If target partition does not exist, client evaluates the expired partition predicate (see A.1). If the partition is determined to be expired, client rewrites destination partition to `__historical__` partition while keeping row data unchanged. If the predicate is not satisfied, the original `PartitionNotExistException` is propagated to the caller.
4. Client computes the destination bucket within `__historical__` partition:
    - **Sticky strategy (no bucket key):** reuse normal sticky behavior; keep writing to current sticky bucket and rotate to next bucket when sticky window is switched.
    - **Bucket-key strategy:** compute bucket directly from `bucketKeyBytes` (reuse already encoded bytes).
5. Record is sent as a normal log append request to the selected `__historical__` bucket leader.

Concrete bucket-key routing code:

```java
byte[] bucketKeyBytes = record.getBucketKey(); // already encoded by existing bucket-key encoder
int bucketId = bucketAssigner.assignBucket(bucketKeyBytes, cluster);
```

Rationale: for Log tables, the main reason is consistency with Primary Key bucket strategy in `__historical__` (and existing bucket-key semantics). Primary Key tables need to bucket by bucket key; the detailed reason is described in the Primary Key section below. Trade-off: hotspots may increase for same bucket keys across different original partitions.

Server-side handling:

1. `__historical__` partition leader appends the record to `__historical__` log with normal replication and ACK semantics.
2. Row payload still contains original partition columns, so no extra envelope field is required.
3. Downstream consumers subscribe to `__historical__` buckets and consume these records through standard log consumption path.
4. Consumer can recover original partition identity from row partition columns.

This keeps log-table historical writes fully compatible with existing producer/consumer protocol and preserves partition semantics in data payload.

Offset Continuity and Semantic Guarantee:

`__historical__` is a **best-effort late-data append path**, not a strict changelog continuation of original partitions. There is **no offset continuity** between the original partition and `__historical__`:

- When a partition expires, both its metadata and log data are deleted; original offsets are gone.
- `__historical__` starts its own offset space from 0; there is no mapping between original and historical offsets.
- The original partition is derived from the partition columns in row data.

**Potential changelog gap**: TTL expiration is time-driven and does not wait for all consumers to finish consuming the original partition. If a consumer has not fully consumed an original partition's changelog when it expires, and new late writes are subsequently redirected to `__historical__`, there may be a gap between the tail of the original partition's changelog and the first record for that partition in `__historical__`. This is a **pre-existing limitation of TTL expiration** — before this FIP, late data was simply rejected, so no data would arrive at all. `__historical__` strictly improves the situation by capturing late data that would otherwise be lost.

**User expectation**: consumers subscribing to `__historical__` should treat it as a supplementary stream of late-arriving records, not as a seamless continuation of any original partition's changelog. For use cases requiring strict changelog completeness across partition expiration, users should ensure TTL is configured long enough for all consumers to finish, or consume from lake storage instead.


#### A.3 Primary Key-table write path (client -> server)

#### A.3.1 Historical Partition Write Flow

Client-side handling:

1. Upsert writer / delete writer resolves target partition from row partition columns.
2. If target partition exists, follow normal Primary Key write path.
3. If target partition does not exist, client evaluates the expired partition predicate (see A.1). If the partition is determined to be expired, client rewrites destination partition to `__historical__` and keeps row payload unchanged. If the predicate is not satisfied, the original `PartitionNotExistException` is propagated to the caller.
4. Client computes bucket:
    - compute destination `__historical__` bucket directly from `bucketKeyBytes`.
5. Record is sent to the selected `__historical__` bucket leader.
6. For redirected PK writes, client carries `partition_name` in put-kv bucket request metadata. This is required for deterministic routing of key-only deletes (`row == null`).

Concrete bucket computation:

```java
byte[] bucketKeyBytes = record.getBucketKey(); // already encoded by existing bucket-key encoder
int bucketId = bucketAssigner.assignBucket(bucketKeyBytes, cluster);
```

Rationale: this preserves bucket-key alignment between online writes and union-read planning for Primary Key tables. Using `originalPartitionName + bucketKey` composite routing would improve hotspot distribution, but would force wider fan-out when union reading historical data from specific Paimon buckets.

Server-side handling:

1. `__historical__` partition leader receives write and enters the historical PK write pipeline.
2. For Primary Key tables, PK encoding excludes partition columns. Different original partitions can produce identical key bytes:
    - `dt=2020,id=1` -> `encode(id=1)`
    - `dt=2019,id=1` -> `encode(id=1)`
3. To avoid collision and wrong old-value resolution, `__historical__` uses **composite key** encoding that prepends the original partition name to the original key:
   ```
   composite key = partitionName length (4 bytes, big-endian) + partitionName bytes (UTF-8) + original key bytes
   ```
   This ensures keys from different original partitions never collide in the same key space.
4. Each `__historical__` bucket maintains a **single, non-persistent RocksDB instance** (separate from normal partition's RocksDB) that stores all original partitions' data using composite keys. This RocksDB:
    - uses a single default CF,
    - does **not** require snapshot or checkpoint — lake is the source of truth, and recovery is handled by WAL replay from the last tiered offset (see A.3.4),
    - is stored in a dedicated data directory (e.g., `__historical_/`) alongside the bucket's normal RocksDB.
5. Extract original partition:
    - upsert (`row != null`): from row partition columns,
    - delete (`row == null`): from `partition_name` field in the RPC request (`PbPutKvReqForBucket.partition_name`), since there is no row payload to extract partition columns from.
6. Encode composite key from original partition name + original key bytes.
7. Process upsert/delete using composite key against the historical RocksDB:
    - upsert → write composite key + value to prewrite buffer,
    - delete → delete composite key from prewrite buffer,
    - flush → prewrite buffer flushes to historical RocksDB.
8. For old-value resolution, when local old value is absent, this path falls back to point lookup from underlying lake storage. The lake lookup is executed asynchronously; see Section C for threading model.

Flow sketch:

```text
Incoming PK record on __historical__
        |
        v
Extract original partition:
  - upsert (row != null): from row partition columns
  - delete (row == null): from `partition_name` in RPC request
        |
        v
Encode composite key: partitionName + originalKey
        |
        v
Process upsert/delete with composite key
        |
        +--> prewrite buffer (composite key)
        +--> historical RocksDB (composite key, single CF)
        |
        v
Old-value lookup: prewrite buffer -> historical RocksDB -> lake
```

Old-value resolution chain:

1. prewrite buffer (composite key lookup),
2. historical RocksDB (composite key lookup),
3. lake fallback (original partition + original key).

Execution note:

- The entire historical write processing (including step 3 lake fallback) is offloaded from the RPC thread to a shared IO executor, so unpredictable remote lake I/O latency does not block RPC threads or real-time write paths. See Section C for details.

#### A.3.2 Design rationale: single non-persistent RocksDB with composite key

Four options were considered for Primary Key state management in `__historical__`:

1. **Per-partition CF in the bucket's existing RocksDB**: one CF per original partition within the bucket's shared RocksDB.
2. **Pure in-memory state** (HashMap per original partition, no RocksDB).
3. **Per-partition non-persistent RocksDB instance**: one RocksDB per original partition.
4. **Single non-persistent RocksDB with composite key** (this FIP's choice).

This FIP chooses option 4 because:

- **No CF proliferation risk**: option 1 requires one CF per active historical partition within a single RocksDB. Each CF allocates its own memtable (default 64MB × 2 = 128MB), so even a few dozen partitions can exhaust memory. RocksDB startup time also degrades with CF count.
- **Controlled memory usage**: option 2 has no natural spill-to-disk mechanism — if tiering is slow or historical writes burst, memory grows unboundedly. RocksDB handles memory → SST flush automatically.
- **O(1) fixed resource overhead**: option 3 (per-partition RocksDB) requires one RocksDB instance per active partition, each with its own memtable (minimum 4–8 MB × 2), MANIFEST, and background flush/compaction activity. With N active historical partitions, the total fixed overhead is O(N). A single RocksDB instance has constant fixed overhead regardless of how many partitions have historical data — only data size grows, which is expected.
- **No persistence complexity**: since lake storage is the source of truth for historical data, the historical RocksDB does not need snapshot or checkpoint. On restart, it is discarded and rebuilt by replaying WAL from the last tiered offset (see A.3.4). This eliminates all snapshot management overhead.
- **Minimal codec impact**: composite key encoding is scoped entirely to the `__historical__` bucket's own RocksDB instance. Normal partition's key codec, snapshot, and recovery paths are completely unaffected. The encoding/decoding is trivial (a 4-byte length prefix + partition name bytes prepended to the original key).
- **Simple management**: one RocksDB instance per `__historical__` bucket — one open, one close, one directory. No need to manage a dynamic set of RocksDB instances or CF handles.

#### A.3.3 Cleanup after tiering sync

Historical writes are expected to be bursty — a pulse of late-arriving data followed by an idle period. The entire burst is typically tiered to lake quickly. Based on this characteristic, cleanup uses a simple whole-RocksDB drop strategy instead of per-partition incremental cleanup:

**Cleanup condition**: when the `__historical__` partition's `tieredOffset >= logEndOffset` for a given bucket, all data in that bucket's historical RocksDB has been durably tiered to lake. Both `tieredOffset` and `logEndOffset` use exclusive next-offset semantics (i.e., the offset of the next record to be written/tiered), so `>=` means all existing records have been tiered.

**Cleanup procedure**:

1. Submit cleanup as a task to the per-bucket serial executor (see C.2), so it is serialized with historical writes.
2. Inside the serial executor, re-check `tieredOffset >= logEndOffset`. If a new write arrived after the initial check, the condition will no longer hold and cleanup is skipped.
3. If the condition holds, close and delete the entire historical RocksDB directory, then create a fresh empty instance (or defer creation lazily until the next historical write arrives).

**Coordination with concurrent lookups**:

Cleanup closes and deletes the historical RocksDB, but historical lookups (which do not go through the per-bucket serial executor, see C.2) may be concurrently reading from it. To prevent lookups from accessing a closed/deleted RocksDB instance, a **reference-counted RocksDB handle** is used: lookups acquire a reference before reading local historical state, and cleanup waits until all outstanding references are released before closing the instance. If cleanup completes first, subsequent lookups see no local instance and fall through directly to lake fallback.

**Why whole-RocksDB drop instead of per-partition `DeleteRange`**:
- No per-partition offset tracking needed — only one check against the partition-level tiered offset.
- No `DeleteRange` tombstone overhead, no compaction dependency.
- No complex locking semantics — the per-bucket serial executor already provides the necessary serialization.
- Matches the bursty write pattern: after a pulse is fully tiered, the RocksDB is effectively empty anyway.

**Future consideration: per-partition incremental cleanup**:

If historical writes turn out to be sustained rather than bursty (e.g., multiple original partitions receiving continuous late writes at different rates), whole-RocksDB drop may not trigger for a long time because `tieredOffset < logEndOffset` remains true as new writes keep arriving. In that case, a per-partition incremental cleanup can be considered:

1. Maintain `partitionEndOffset[partition]` that tracks the latest log offset written for each original partition in `__historical__`.
2. When `tieredOffset >= partitionEndOffset[partition]`, that partition's data is fully tiered.
3. Submit cleanup to the per-bucket serial executor, re-check the condition inside, then issue `DeleteRange` on the partition's composite key prefix.
4. Since composite keys do not contain log offsets, `DeleteRange` can only remove the entire partition prefix — partial cleanup (only records up to a specific offset) is not possible. Therefore the condition must be that **all** writes for that partition have been tiered.

This adds tracking and locking complexity, so it is deferred unless the bursty-write assumption proves insufficient in practice.

#### A.3.4 `__historical__` partition recovery flow

Since the historical RocksDB is non-persistent (no snapshot/checkpoint), recovery is handled entirely by WAL replay from the last tiered offset:

1. **Discard old state**: delete the historical RocksDB data directory if it exists, then create a fresh empty RocksDB instance.
2. **Determine replay start offset**: obtain the `__historical__` partition's tiered offset from tiering service metadata. Since `__historical__` is itself a tiered partition, its tiered offset is tracked as a whole — no per-original-partition offset computation needed.
3. **Replay WAL**: replay `__historical__` log from the tiered offset to **log end**:
    - for each record, extract original partition from row partition columns,
    - encode composite key (partitionName + originalKey),
    - apply upsert/delete to the historical RocksDB.
4. After replay reaches **high watermark**, records beyond high watermark are applied to the prewrite buffer instead of directly to RocksDB (consistent with normal partition recovery semantics).

This approach is significantly simpler than normal partition recovery because:
- No snapshot to restore — always starts from a clean RocksDB.
- Replay data volume is bounded by `log end - last tiered offset`, which is small as long as tiering runs regularly.
- Only one RocksDB instance to manage — no dynamic instance or CF creation during replay.

#### A.3.5 Changelog gap and known limitation

As described in Section A.2 ("Offset Continuity and Semantic Guarantee"), `__historical__` is a best-effort late-data append path. For Primary Key tables, this has an additional implication on changelog correctness:

**Scenario**: a consumer subscribes to both original partitions and `__historical__`. An original partition is TTL-expired before the consumer finishes consuming its changelog. New writes for that partition are redirected to `__historical__`.

**Impact**:
- The consumer may miss the tail of the original partition's changelog (the records between its last consumed offset and partition expiration).
- The first record for that partition in `__historical__` may produce a changelog entry (e.g., `INSERT` or `UPDATE_AFTER`) that is logically inconsistent with the consumer's last seen state for that key.

**Why this is acceptable**:
- This is a pre-existing limitation of TTL-based partition expiration, not introduced by this FIP. Before this FIP, late data was rejected entirely — `__historical__` strictly improves the situation.
- The gap only occurs when TTL expires a partition before all consumers finish. Users who need strict changelog completeness should configure TTL long enough or consume from lake storage.
- For most late-arriving data use cases (e.g., corrections, backfill), best-effort semantics are sufficient.


### B. Historical Partition Point Lookup Path (Primary Key)

#### B.1 Client-side fallback

When `PrimaryKeyLookuper` cannot resolve partition (partition expired, as determined by the expired partition predicate in A.1):

- compute `bucketId` by following the same `__historical__` write bucketing strategy (bucket-key based),
- route lookup request to the leader of `__historical__` partition + `bucketId`,
- send a standard `LookupRequest` with the additional `partition_name` field set to the original partition name.
- `LookupSender` must batch historical lookup requests by `(__historical__ bucket, original partition name)` — the same batching key as historical PK writes (see C.1). Since `partition_name` is a bucket-request-level field, a single `PbLookupReqForBucket` can only carry keys for one original partition. Mixing keys from different original partitions in the same bucket request would cause the server to look up all keys against the wrong lake partition.

#### B.2 Server-Side: Dispatch by Partition Type

`ReplicaManager.lookups()` checks whether the target is `__historical__` partition:

- **Normal partition**: execute lookup synchronously against local RocksDB (existing path, unchanged).
- **`__historical__` partition**: submit the lookup to `ioExecutor` for async processing (see Section C for threading model). The lookup follows the same state chain as the write path's old-value resolution to ensure consistency:
    1. **Prewrite buffer**: check the in-memory prewrite buffer using composite key (partitionName + originalKey). If found, return the value directly.
    2. **Historical RocksDB**: check the historical RocksDB using composite key. If found, return the value directly.
    3. **Lake fallback**: if not found locally, fall back to lake lookup:
        - Validates that lake storage is configured
        - Obtains (or caches) a per-table `LakeTableLookuper`
        - Uses `partition_name` from the request to identify the original partition in lake storage

This ensures that data written to `__historical__` but not yet tiered to lake is still visible to point lookups. Without this local-first lookup, a successful historical write followed by an immediate lookup could return stale data or null.

#### B.3 Paimon Implementation: `PaimonLakeTableLookuper`

The Paimon-specific implementation uses Paimon's `LocalTableQuery` for efficient point lookups:

**Lazy initialization:**
- Resources (Catalog, FileStoreTable, LocalTableQuery, etc.) are lazily initialized on first lookup

**Per-(partition, bucket) file refresh with snapshot-based invalidation:**
- Maintains a `Map<Tuple2<String, Integer>, Long> refreshedBuckets` that maps each (partitionName, bucketId) pair to the Paimon snapshot ID used for the last file refresh.
- On each lookup, compares the current latest Paimon snapshot ID against the cached snapshot ID for that (partition, bucket):
    - If no entry exists or the cached snapshot ID is stale (i.e., a newer snapshot is available), scans Paimon for the data files, calls `tableQuery.refreshFiles()`, and updates the cached snapshot ID.
    - If the cached snapshot ID matches the latest, skips the scan.
- This ensures that newly tiered data from `__historical__` (which produces new Paimon snapshots) is visible to subsequent lookups, while avoiding redundant file scans when no new snapshot has been committed.

**Lookup flow:**
1. The key bytes are already encoded in Paimon's BinaryRow format by the client-side `PaimonKeyEncoder`, so directly wrap them as a Paimon `BinaryRow` via `keyRow.pointTo(MemorySegment.wrap(key), 0, key.length)` — no decode/re-encode needed
2. Call `tableQuery.lookup(partition, bucketId, keyRow)`
3. If found, wrap Paimon result as Fluss `InternalRow` via `PaimonRowAsFlussRow` adapter
4. Encode value using `CompactedRowEncoder` + `ValueEncoder.encodeValue(binaryRow)`

### C. Performance Isolation: Thread Isolation between Real-time and Historical Paths

Historical partition operations (both writes and lookups) involve lake I/O with unpredictable latency. Without isolation, these slow operations would block the RPC threads that also serve real-time partition reads and writes.

Current Fluss write path is fully synchronous on the RPC thread: `TabletService.putKv()` → `ReplicaManager.putRecordsToKv()` → `putToLocalKv()` → `KvTablet.putAsLeader()`. If a historical PK write needs lake old-value lookup on the RPC thread, it blocks not only other writes but all RPC processing (reads, heartbeats, etc.) on that thread.

#### C.1 Client-side: Batching Constraints

Client sender/accumulator layer batches records with the following constraints:

1. **Real-time vs. historical separation**: real-time partitions and `__historical__` partition are accumulated into **separate batches** and sent as **independent requests**. This ensures server receives requests that are purely real-time or purely historical, enabling clean dispatch at the request level.

2. **Historical PK batching by original partition**: for Primary Key tables, historical put-kv requests must be further batched by `(historical bucket, original partition name)`. This is because `PbPutKvReqForBucket.partition_name` is a request-level field — a single request can only carry one original partition name. For upserts (`row != null`), the server can extract the original partition from row partition columns, but for key-only deletes (`row == null`) the server relies entirely on request-level `partition_name`. Mixing key-only deletes for different original partitions in the same request would make them indistinguishable.

   The batching key for historical PK writes is therefore: `(__historical__ bucket, original partition name)`, compared to normal PK writes which batch by `(partition, bucket)` only.

#### C.2 Server-side: Offload Historical Operations to IO Executor

`ReplicaManager` uses a shared `ioExecutor` (bounded thread pool) to offload all historical partition operations from RPC threads:

- **Write path**: `putRecordsToKv()` checks whether the target is `__historical__` partition. If so, the entire write processing is submitted to `ioExecutor` asynchronously, and the RPC thread is released immediately. Real-time writes continue to execute synchronously on the RPC thread as before.
- **Lookup path**: `lookups()` checks whether the target is `__historical__` partition. If so, the lookup is submitted to `ioExecutor` for async lake lookup. Normal lookups continue to execute synchronously against local RocksDB as before.

Both paths share the same `ioExecutor` because they are both lake I/O bound. A shared pool also provides a unified bound on total lake I/O concurrency per server.

```text
Real-time write:    RPC Thread → synchronous putToLocalKv() → RocksDB
Real-time lookup:   RPC Thread → synchronous local RocksDB query

Historical write:   RPC Thread → ioExecutor → putToLocalKv() → lake old-value lookup → historical RocksDB
Historical lookup:  RPC Thread → ioExecutor → Paimon LocalTableQuery
```

Real-time paths are not affected by any new executor or concurrency control — they remain fully synchronous on the RPC thread as before.

**Per-bucket write ordering guarantee**:

Historical writes offloaded to `ioExecutor` must maintain strict ordering within the same `__historical__` bucket. Without ordering, concurrent lake old-value lookups with different latencies could cause later-submitted writes to complete before earlier ones, corrupting PK state and changelog order.

This is enforced by **per-bucket serial execution**: writes targeting the same `__historical__` bucket are submitted to the `ioExecutor` but processed sequentially. This can be implemented via a per-bucket queue (e.g., keyed by table-bucket, where each key's tasks execute in FIFO order within the thread pool). Writes to different buckets can still execute concurrently across `ioExecutor` threads.

Lookups do not require ordering and can execute concurrently regardless of bucket.

#### C.3 Server-side: Flow Control

The `ioExecutor` uses a bounded queue as a natural flow control mechanism. Historical writes and lake lookups share the same queue, which provides a unified bound on total lake I/O concurrency per server.

When the queue is full, the server rejects the request with a `HISTORICAL_PARTITION_THROTTLED` error code. Client receives this error and performs backoff retry using the existing client retry mechanism — no historical-partition-specific retry logic is needed.

Queue size rationale: the upper bound of useful queue capacity is constrained by request timeout — requests that wait too long in the queue will time out before execution, wasting resources. The default queue capacity is configurable; the final value needs to be determined by benchmarking actual lake I/O latency. The formula for tuning: `queue_size = thread_count × (max_acceptable_wait / avg_lake_io_latency)`.

## Compatibility, Deprecation, and Migration Plan

### Previous behavior (before this FIP)

Writing to an expired partition on auto-partitioned tables was already broken:

- If `dynamicPartitionEnabled = true`: client dynamically creates the expired partition, but `AutoPartitionManager` immediately drops it on the next TTL check cycle, causing a create/drop loop.
- If `dynamicPartitionEnabled = false`: client throws `PartitionNotExistException` directly.
- Point lookup on expired partitions returns `null`, even though data exists in lake storage.

In both cases, writing to or reading from expired partitions did not produce correct results.

### New behavior (this FIP)

- Writes to expired partitions are redirected to the `__historical__` partition and succeed.
- Point lookups on expired partitions are routed to `__historical__` partition via `LookupRequest` with `partition_name`, where the server performs a local-first lookup (prewrite buffer → historical RocksDB → lake fallback) and returns the correct value.

### Compatibility

- **Old client -> New server**
    - Request compatibility: old client does not send `partition_name` in put-kv or lookup requests; new server accepts it because the field is optional.
    - Normal writes, deletes, and lookups are unchanged.
    - Old client does not have the redirect-to-`__historical__` logic, so it falls back to the previous behavior (dynamic create/drop loop or `PartitionNotExistException`).

- **New client -> Old server**
    - Old server does not have `__historical__` partition support (no historical RocksDB, no lake old-value lookup, no lake lookup dispatch).
    - No special version check is needed. The new client attempts to redirect to `__historical__`, but since old server does not create or correctly handle `__historical__`, the write/lookup fails — this is equivalent to the previous behavior where writing to expired partitions was already unsupported.
    - Normal writes, deletes, and lookups on non-expired partitions are unaffected.

## Test Plan

- **Integration tests**
    - write to expired partition redirected to `__historical__` partition.
    - update on historical key generates `UPDATE_BEFORE + UPDATE_AFTER` by retrieving old value from lake.
    - log-table write to expired partition is redirected to `__historical__` partition and can be consumed downstream from `__historical__` buckets.
    - look up from an expired partition can still get the value correctly
    - restart/recovery preserves behavior.

## Rejected Alternatives

N/A

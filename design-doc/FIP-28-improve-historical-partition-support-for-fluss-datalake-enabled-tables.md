# FIP-28: Support Write and Lookup for Expired Partitions in Paimon Lake Tables

## Motivation

For auto-partitioned partitioned tables (Primary Key tables and Log tables) with lake tiering enabled, Fluss will expire old partitions from metadata. After expiration, users face two gaps:

1.  **Lookup gap (Primary Key tables)**: point lookup on expired partitions returns `null` in current behavior, because once Fluss determines the partition does not exist, the row is treated as not found, even though data may still exist in lake storage.
2.  **Write gap**: late-arriving updates/inserts for expired partitions are rejected.

This is confusing for users because batch/lake reads can still observe historical data while Fluss online paths cannot.

This FIP proposes a unified solution for both read and write:

- read historical partition data through server-side lake lookup fallback (Primary Key tables),
- write historical partition data through one dedicated special Fluss partition (`__historical__`) for both Primary Key and Log tables,
- allow downstream consumers to consume late records of expired original partitions from this partition.

Scope and eligibility:

- This FIP only supports tables with Paimon-enabled lake storage.
- For Primary Key tables, this is a hard technical requirement: historical writes need old-value resolution to generate correct changelog (`UPDATE_BEFORE` + `UPDATE_AFTER`), and the old value for expired partitions only exists in lake storage. Without a lake backend, old-value lookup is impossible and the write path cannot produce correct results.
- For Log tables, historical writes are technically feasible without lake storage (append-only, no old-value resolution needed). However, we still restrict this FIP to lake-enabled tables to avoid a fragmented feature matrix — supporting historical writes for lake-enabled Log tables but not for non-lake Log tables would create inconsistent user expectations across the same table type.

Note: This FIP primarily focuses on Paimon as the lake storage backend, because Paimon provides efficient point lookup capabilities via its `LocalTableQuery` API. Other lake formats such as Iceberg and Lance do not currently offer comparable point query performance, so support for them can be considered in future FIPs, which would require a full cache on Iceberg.

## Public Interfaces

### RPC Extensions

Extend lookup RPC for historical partition lake lookup:

``` protobuf
message PbLookupReqForBucket {
  // existing fields...
  optional string partition_name = N;  // original partition name for historical lookup
}
```

- `partition_name` carries the original partition name when the lookup targets `__historical__` partition.
- Server uses this field to determine which lake partition to query.
- Field is optional; absent for normal lookups.

Extend put-kv RPC for deterministic historical delete routing:

``` protobuf
message PbPutKvReqForBucket {
  optional int64 partition_id = 1;
  required int32 bucket_id = 2;
  required bytes records = 3;
  optional string partition_name = 4;
}
```

- `partition_name` carries the original partition name before redirecting to `__historical__`.
- For historical partition puts, client always sets this field. Server uses it to encode composite keys and resolve lake fallback partition. For key-only deletes (`row == null`), this is the only source of original partition identity since there is no row payload.
- Field is optional for backward compatibility; absent for normal (non-historical) partition puts.

### New SPI Interface

Expose table-level lake point lookup through `LakeStorage`:

``` java
public interface LakeStorage {
    // existing methods...

    /**
     * Creates a {@link LakeTableLookuper} for point lookup against the specified table
     * in lake storage.
     */
    default LakeTableLookuper createLakeTableLookuper(TablePath tablePath) {
        throw new UnsupportedOperationException(
                "Point lookup is not supported for this lake storage.");
    }
}

/**
 * Interface for looking up a single key from lake storage. This is used for:
 * <ul>
 *   <li>PK table historical write: old-value fallback from lake to produce UPDATE_BEFORE for changelog
 *   <li>Expired partition point lookup: local-first lookup (RocksDB → lake fallback)
 * </ul>
 *
 * <p>Each instance is bound to a specific table and caches per-table resources
 * (e.g., catalog connections, table metadata) for efficient repeated lookups.
 *
 * <p>The key bytes passed to {@link #lookup} are already encoded in the lake storage's
 * native format (e.g., Paimon BinaryRow format) by the client-side encoder, so
 * implementations can use them directly without re-encoding.
 */
public interface LakeTableLookuper extends AutoCloseable {
    /**
     * Lookup a single key from lake storage.
     *
     * <p>Key bytes are already encoded in the lake storage's native key format.
     * Returned value bytes should be encoded with schema ID prefix.
     *
     * @param key encoded key bytes in lake storage's native format
     * @param context lookup context containing partition, bucket, and schema information
     * @return encoded value bytes with schema ID prefix, or null if not found
     * @throws Exception if lookup fails
     */
    @Nullable
    byte[] lookup(byte[] key, LookupContext context) throws Exception;

    /** Context for a single lake lookup operation. */
    class LookupContext {
        @Nullable private final ResolvedPartitionSpec partitionSpec;
        private final int bucketId;
        private final int schemaId;

        public LookupContext(
                @Nullable ResolvedPartitionSpec partitionSpec, int bucketId, int schemaId) {
            this.partitionSpec = partitionSpec;
            this.bucketId = bucketId;
            this.schemaId = schemaId;
        }

        /** Returns the partition spec, or null for non-partitioned tables. */
        @Nullable
        public ResolvedPartitionSpec getPartitionSpec() { return partitionSpec; }

        /** Returns the bucket ID. */
        public int getBucketId() { return bucketId; }

        /** Returns the schema ID used for value encoding. */
        public int getSchemaId() { return schemaId; }
    }
}
```

## Proposed Changes

### A. Historical Partition Write Path (Log + Primary Key Table)

#### A.1 Historical Partition

For each eligible table, maintain special **Historical Partition(s)** as the write target for expired partitions. The historical partition name is derived by replacing the auto-partition key value with `__historical__`:

- **Single partition key** `[dt]`: one historical partition per table — `__historical__`.
- **Multi partition key** `[region, dt]` (dt is auto key): one historical partition **per static partition prefix** — e.g., `us-east$__historical__`, `eu-west$__historical__`. Each static prefix has its own independent historical partition with its own buckets, WAL, and tiering lifecycle.

In this FIP we use the term "historical partition" to refer to any such `__historical__`-suffixed partition.

Properties:

- always writable,
- not auto-expired,
- normal replication/WAL behavior,
- bucket count same as other partitions of the table.

When a write targets an expired original partition, client redirects it to the historical partition; original partition identity remains in row partition columns. This applies to both Primary Key writes and Log appends.

**Expired partition predicate**:

A partition is considered "expired" (and eligible for `__historical__` redirect) only when **all** of the following are true:

1.  The table is an auto-partitioned table with data lake enabled (eligible table).
2.  The partition name matches the table's auto-partition spec naming pattern (e.g., valid date format for date-partitioned tables).
3.  The partition name falls before the TTL expiration boundary computed from the auto-partition spec and current time (i.e., it is older than the retention window).
4.  The partition does not exist in current metadata.

If condition 4 is true but any of conditions 1–3 are false, the client must **not** redirect to `__historical__` and should preserve the original `PartitionNotExistException`. This prevents invalid partition names, future partitions, or non-eligible tables from being incorrectly routed to the historical path.

The predicate is evaluated client-side using the table's auto-partition spec and TTL configuration (already available in client metadata cache). The current time is obtained via `Instant.now()` (UTC epoch) and converted to the table's configured time zone (`autoPartitionStrategy.timeZone()`), so the expiration boundary is consistent across clients regardless of machine-local time zone settings — only wall-clock skew between machines can cause transient disagreement, which is benign: the worst case is one client redirects to `__historical__` slightly earlier or later than another, and both paths produce correct results.

**Creation**:

- `__historical__` is lazily created by the client when it first encounters an expired partition (as defined by the predicate above) — regardless of whether the operation is a write or a lookup. The client checks whether `__historical__` exists in cached metadata, and if not, triggers creation via the partition creation RPC.
- Although `__historical__` creation reuses the existing partition creation RPC, metadata manager, and replica assignment infrastructure, it is treated as a **system partition creation** and is **not controlled by the user-facing `dynamicPartitionEnabled` setting**. This ensures that `dynamicPartitionEnabled = false` tables can still use historical write/lookup — the `__historical__` partition is a system-internal mechanism, not a user-created business partition. Server-side creation validation allows `__historical__` unconditionally for eligible tables (auto-partitioned, Paimon lake-enabled) regardless of the dynamic partition flag.
- If creation fails (e.g., transient coordinator error), the operation fails and the client retries on the next attempt, which will trigger creation again.
- If multiple clients race to create `__historical__` concurrently, only one succeeds; the others receive `PartitionAlreadyExistException` and proceed normally.

**Name reservation**:

- The partition value `__historical__` is conceptually reserved by the system. However, this is currently **not enforced** server-side — the `__historical__` partition is created through the standard partition creation RPC, and the server cannot distinguish system-initiated creation from user-initiated creation. If a user manually creates a partition with this name, it would conflict with the system's historical partition. In practice, the risk is low because `__historical__` is not a valid date/time value and would not be produced by auto-partition strategies.

**AutoPartitionManager exclusion**:

- `AutoPartitionManager` recognizes `__historical__` as a system partition and unconditionally skips it during TTL expiration checks. This is implemented by checking the partition name before applying TTL logic.

**Partition discovery and consumer visibility**:

- `__historical__` is included in `listPartitions` results like any normal partition. Consumers discover it through the standard partition discovery mechanism and can subscribe to its buckets.
- Metadata APIs (e.g., `getPartition`) treat it as a normal partition. No special filtering is applied — the partition is fully visible to clients.

#### A.2 Log-table write path (client -> server)

Client-side handling:

1.  Producer resolves target partition from record partition columns.
2.  If target partition exists, follow normal log write path.
3.  If target partition does not exist, client evaluates the expired partition predicate (see A.1). If the partition is determined to be expired, client rewrites destination partition to `__historical__` partition while keeping row data unchanged. If the predicate is not satisfied, the original `PartitionNotExistException` is propagated to the caller.
4.  Client computes the destination bucket within `__historical__` partition:
    - **Sticky strategy (no bucket key):** reuse normal sticky behavior; keep writing to current sticky bucket and rotate to next bucket when sticky window is switched.
    - **Bucket-key strategy:** compute bucket directly from `bucketKeyBytes` (reuse already encoded bytes).
5.  Record is sent as a normal log append request to the selected `__historical__` bucket leader.

Concrete bucket-key routing code:

``` java
byte[] bucketKeyBytes = record.getBucketKey(); // already encoded by existing bucket-key encoder
int bucketId = bucketAssigner.assignBucket(bucketKeyBytes, cluster);
```

Rationale: for Log tables, the main reason is consistency with Primary Key bucket strategy in `__historical__` (and existing bucket-key semantics). Primary Key tables need to bucket by bucket key; the detailed reason is described in the Primary Key section below. Trade-off: hotspots may increase for same bucket keys across different original partitions.

Server-side handling:

1.  `__historical__` partition leader appends the record to `__historical__` log with normal replication and ACK semantics.
2.  Row payload still contains original partition columns, so no extra envelope field is required.
3.  Downstream consumers subscribe to `__historical__` buckets and consume these records through standard log consumption path.
4.  Consumer can recover original partition identity from row partition columns.

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

1.  Upsert writer / delete writer resolves target partition from row partition columns.
2.  If target partition exists, follow normal Primary Key write path.
3.  If target partition does not exist, client evaluates the expired partition predicate (see A.1). If the partition is determined to be expired, client rewrites destination partition to `__historical__` and keeps row payload unchanged. If the predicate is not satisfied, the original `PartitionNotExistException` is propagated to the caller.
4.  Client computes bucket:
    - compute destination `__historical__` bucket directly from `bucketKeyBytes`.
5.  Record is sent to the selected `__historical__` bucket leader.
6.  For redirected PK writes, client carries `partition_name` in put-kv bucket request metadata. This is required for deterministic routing of key-only deletes (`row == null`).

Concrete bucket computation:

``` java
byte[] bucketKeyBytes = record.getBucketKey(); // already encoded by existing bucket-key encoder
int bucketId = bucketAssigner.assignBucket(bucketKeyBytes, cluster);
```

Rationale: this preserves bucket-key alignment between online writes and union-read planning for Primary Key tables. Using `originalPartitionName + bucketKey` composite routing would improve hotspot distribution, but would force wider fan-out when union reading historical data from specific Paimon buckets.

Server-side handling:

1.  `__historical__` partition leader receives write and enters the historical PK write pipeline.

2.  For Primary Key tables, PK encoding excludes partition columns. Different original partitions can produce identical key bytes:
    - `dt=2020,id=1` -> `encode(id=1)`
    - `dt=2019,id=1` -> `encode(id=1)`

3.  To avoid collision and wrong old-value resolution, `__historical__` uses **composite key** encoding that prepends the original partition name to the original key:

        composite key = partitionName length (4 bytes, big-endian) + partitionName bytes (UTF-8) + original key bytes

    This ensures keys from different original partitions never collide in the same key space.

4.  Each `__historical__` bucket maintains a **single, non-persistent RocksDB instance** (separate from normal partition's RocksDB) that stores all original partitions' data using composite keys. This RocksDB:
    - uses a single default CF,
    - does **not** require snapshot or checkpoint — lake is the source of truth, and recovery is handled by WAL replay from the last tiered offset (see A.3.4),
    - is stored in a dedicated data directory (e.g., `__historical_/`) alongside the bucket's normal RocksDB.

5.  Extract original partition:
    - upsert (`row != null`): from row partition columns,
    - delete (`row == null`): from `partition_name` field in the RPC request (`PbPutKvReqForBucket.partition_name`), since there is no row payload to extract partition columns from.

6.  Encode composite key from original partition name + original key bytes.

7.  Process upsert/delete using composite key against the historical RocksDB:
    - upsert → write composite key + value to prewrite buffer,
    - delete → delete composite key from prewrite buffer,
    - flush → prewrite buffer flushes to historical RocksDB.

8.  For old-value resolution, when local old value is absent, this path falls back to point lookup from underlying lake storage. The lake lookup is executed asynchronously; see Section C for threading model.

Flow sketch:

``` text
Incoming PK record on __historical__
        |
        v
Extract original partition:
  - upsert (row != null): from `partition_name` in RPC request
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

1.  prewrite buffer (composite key lookup),
2.  historical RocksDB (composite key lookup) — if a **tombstone** is found (see below), return `null` immediately without falling through to lake,
3.  lake fallback (original partition + original key).

**Tombstone mechanism for DELETE operations**:

In normal (non-historical) partitions, a RocksDB `delete(key)` physically removes the key. After deletion, a `get(key)` returns `null`, which is correct because there is no lake fallback. In `__historical__` partitions, however, `null` from RocksDB triggers the lake fallback (step 3 above), which would return the stale pre-delete value from lake storage — effectively undoing the delete.

To prevent this, DELETE operations on `__historical__` partitions write a **tombstone** (an empty byte array `new byte[0]`) instead of physically deleting the key:

- A normal encoded value always has at least 2 bytes (schema ID prefix), so an empty byte array is unambiguous as a tombstone marker.
- `isTombstone(value)` returns `true` when `value != null && value.length == 0`.
- The old-value resolution chain (step 2) checks for tombstones: if the RocksDB result is a tombstone, it returns `null` without falling through to lake fallback.

Tombstones are applied consistently across all paths that write to historical RocksDB:

- **Write path** (`KvPreWriteBuffer.flush()`): when flushing a DELETE entry for a historical partition, writes `TOMBSTONE_VALUE` instead of calling `delete()`.
- **Recovery path** (`KvRecoverHelper`): when replaying WAL DELETE records during recovery, writes `TOMBSTONE_VALUE` to the historical RocksDB.

Execution note:

- The entire historical write processing (including step 3 lake fallback) is offloaded from the RPC thread to a shared IO executor, so unpredictable remote lake I/O latency does not block RPC threads or real-time write paths. See Section C for details.

#### A.3.2 Design rationale: single non-persistent RocksDB with composite key

Four options were considered for Primary Key state management in `__historical__`:

1.  **Per-partition CF in the bucket's existing RocksDB**: one CF per original partition within the bucket's shared RocksDB.
2.  **Pure in-memory state** (HashMap per original partition, no RocksDB).
3.  **Per-partition non-persistent RocksDB instance**: one RocksDB per original partition.
4.  **Single non-persistent RocksDB with composite key** (this FIP's choice).

This FIP chooses option 4 because:

- **No CF proliferation risk**: option 1 requires one CF per active historical partition within a single RocksDB. Each CF allocates its own memtable (default 64MB × 2 = 128MB), so even a few dozen partitions can exhaust memory. RocksDB startup time also degrades with CF count.
- **Controlled memory usage**: option 2 has no natural spill-to-disk mechanism — if tiering is slow or historical writes burst, memory grows unboundedly. RocksDB handles memory → SST flush automatically.
- **O(1) fixed resource overhead**: option 3 (per-partition RocksDB) requires one RocksDB instance per active partition, each with its own memtable (minimum 4–8 MB × 2), MANIFEST, and background flush/compaction activity. With N active historical partitions, the total fixed overhead is O(N). A single RocksDB instance has constant fixed overhead regardless of how many partitions have historical data — only data size grows, which is expected.
- **No persistence complexity**: since lake storage is the source of truth for historical data, the historical RocksDB does not need snapshot or checkpoint. On restart, it is discarded and rebuilt by replaying WAL from the last tiered offset (see A.3.4). This eliminates all snapshot management overhead.
- **Minimal codec impact**: composite key encoding is scoped entirely to the `__historical__` bucket's own RocksDB instance. Normal partition's key codec, snapshot, and recovery paths are completely unaffected. The encoding/decoding is trivial (a 4-byte length prefix + partition name bytes prepended to the original key).
- **Simple management**: one RocksDB instance per `__historical__` bucket — one open, one close, one directory. No need to manage a dynamic set of RocksDB instances or CF handles.

**`HistoricalKvManager`**: All per-bucket historical RocksDB instances are managed by a centralized `HistoricalKvManager` that provides:

- **Lazy creation**: RocksDB is only opened on the first `put()` call for a given bucket, not when the bucket is assigned.
- **Idle timeout cleanup**: A scheduled task periodically scans all open instances and evicts those that have been idle longer than a configurable timeout (`kv.historical.idle-timeout`, default 30 minutes). This bounds memory usage when historical writes are bursty — after a burst finishes and data is tiered, the idle RocksDB instances are automatically cleaned up.
- **Read-write lock concurrency**: Each per-bucket handle uses a `ReentrantReadWriteLock`. Read operations (`put`, `get`, `delete`) acquire the shared read lock for concurrent access. Cleanup and drop operations acquire the exclusive write lock via `tryLock()` to avoid blocking active operations.

#### A.3.3 Cleanup after tiering sync

Historical writes are expected to be bursty — a pulse of late-arriving data followed by an idle period. The entire burst is typically tiered to lake quickly. Based on this characteristic, cleanup uses a simple whole-RocksDB drop strategy instead of per-partition incremental cleanup:

**Cleanup condition**: when the `__historical__` partition's `tieredOffset >= logEndOffset` for a given bucket, all data in that bucket's historical RocksDB has been durably tiered to lake. Both `tieredOffset` and `logEndOffset` use exclusive next-offset semantics (i.e., the offset of the next record to be written/tiered), so `>=` means all existing records have been tiered.

**Cleanup procedure**:

1.  Submit cleanup as a task to the per-bucket serial executor (see C.2), so it is serialized with historical writes.
2.  Inside the serial executor, re-check `tieredOffset >= logEndOffset`. If a new write arrived after the initial check, the condition will no longer hold and cleanup is skipped.
3.  If the condition holds, close and delete the entire historical RocksDB directory, then create a fresh empty instance (or defer creation lazily until the next historical write arrives).

**Coordination with concurrent lookups**:

Cleanup closes and deletes the historical RocksDB, but historical lookups (which do not go through the per-bucket serial executor, see C.2) may be concurrently reading from it. To prevent lookups from accessing a closed/deleted RocksDB instance, a **read-write lock** (`ReentrantReadWriteLock`) is used per bucket handle: lookups acquire the shared read lock before reading local historical state (allowing concurrent reads), and cleanup acquires the exclusive write lock via `tryLock()` before closing the instance (if the lock cannot be acquired immediately, cleanup skips that instance and retries later). If cleanup completes first, subsequent lookups see no local instance and fall through directly to lake fallback.

**Why whole-RocksDB drop instead of per-partition `DeleteRange`**:

- No per-partition offset tracking needed — only one check against the partition-level tiered offset.
- No `DeleteRange` tombstone overhead, no compaction dependency.
- No complex locking semantics — the per-bucket serial executor already provides the necessary serialization.
- Matches the bursty write pattern: after a pulse is fully tiered, the RocksDB is effectively empty anyway.

**Future consideration: per-partition incremental cleanup**:

If historical writes turn out to be sustained rather than bursty (e.g., multiple original partitions receiving continuous late writes at different rates), whole-RocksDB drop may not trigger for a long time because `tieredOffset < logEndOffset` remains true as new writes keep arriving. In that case, a per-partition incremental cleanup can be considered:

1.  Maintain `partitionEndOffset[partition]` that tracks the latest log offset written for each original partition in `__historical__`.
2.  When `tieredOffset >= partitionEndOffset[partition]`, that partition's data is fully tiered.
3.  Submit cleanup to the per-bucket serial executor, re-check the condition inside, then issue `DeleteRange` on the partition's composite key prefix.
4.  Since composite keys do not contain log offsets, `DeleteRange` can only remove the entire partition prefix — partial cleanup (only records up to a specific offset) is not possible. Therefore the condition must be that **all** writes for that partition have been tiered.

This adds tracking and locking complexity, so it is deferred unless the bursty-write assumption proves insufficient in practice.

#### A.3.4 `__historical__` partition recovery flow

Since the historical RocksDB is non-persistent (no snapshot/checkpoint), recovery is handled entirely by WAL replay from the last tiered offset:

1.  **Discard old state**: delete the historical RocksDB data directory if it exists, then create a fresh empty RocksDB instance.
2.  **Determine replay start offset**: obtain the `__historical__` partition's tiered offset from tiering service metadata. Since `__historical__` is itself a tiered partition, its tiered offset is tracked as a whole — no per-original-partition offset computation needed.
3.  **Replay WAL**: replay `__historical__` log from the tiered offset to **log end**:
    - for each record, extract original partition from row partition columns,
    - encode composite key (partitionName + originalKey),
    - apply upsert/delete to the historical RocksDB.
4.  After replay reaches **high watermark**, records beyond high watermark are applied to the prewrite buffer instead of directly to RocksDB (consistent with normal partition recovery semantics).

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

**Expired partition cache**: `PrimaryKeyLookuper` maintains an in-memory cache (`Map<String, Long>`) that maps expired partition names to their resolved `__historical__` partition ID. Once a partition is identified as expired and its historical partition ID is resolved (which involves a ZK metadata lookup), the mapping is cached. Subsequent lookups for the same expired partition skip the ZK lookup entirely, avoiding repeated metadata requests that could flood ZooKeeper under high lookup concurrency.

#### B.2 Server-Side: Dispatch by Partition Type

`ReplicaManager.lookups()` checks whether the target is `__historical__` partition:

- **Normal partition**: execute lookup synchronously against local RocksDB (existing path, unchanged).
- **`__historical__` partition**: submit the lookup to `ioExecutor` for async processing (see Section C for threading model). The lookup follows the same state chain as the write path's old-value resolution to ensure consistency:
  1.  **Prewrite buffer**: check the in-memory prewrite buffer using composite key (partitionName + originalKey). If found, return the value directly.
  2.  **Historical RocksDB**: check the historical RocksDB using composite key. If found, return the value directly.
  3.  **Lake fallback**: if not found locally, fall back to lake lookup:
      - Validates that lake storage is configured
      - Obtains (or caches) a per-table `LakeTableLookuper`
      - Uses `partition_name` from the request to identify the original partition in lake storage

This ensures that data written to `__historical__` but not yet tiered to lake is still visible to point lookups. Without this local-first lookup, a successful historical write followed by an immediate lookup could return stale data or null.

#### B.3 Paimon Implementation: `PaimonLakeTableLookuper`

The Paimon-specific implementation uses Paimon's `LocalTableQuery` for efficient point lookups:

**Thread safety:**

- The `lookup()` method is `synchronized` to ensure thread-safe access to shared mutable state (Paimon `LocalTableQuery`, file refresh tracking, scan result cache). Multiple historical lookup requests from the `ioExecutor` may target the same `PaimonLakeTableLookuper` instance concurrently.

**Lazy initialization:**

- Resources (Catalog, FileStoreTable, LocalTableQuery, etc.) are lazily initialized on first `lookup()` call.
- On schema evolution (schema ID change), all resources are closed and re-created to reflect the new schema.

**Per-(partition, bucket) file refresh with snapshot-based invalidation:**

- Maintains a `Map<PaimonPartitionBucket, Long> refreshedSnapshots` that maps each (partition BinaryRow, bucketId) pair to the Paimon snapshot ID used for the last file refresh.
- On each lookup, compares the current latest Paimon snapshot ID against the cached snapshot ID for that (partition, bucket):
  - If no entry exists or the cached snapshot ID is stale (i.e., a newer snapshot is available), scans Paimon for the data files, calls `tableQuery.refreshFiles()`, and updates the cached snapshot ID.
  - If the cached snapshot ID matches the latest, skips the scan.
- This ensures that newly tiered data from `__historical__` (which produces new Paimon snapshots) is visible to subsequent lookups, while avoiding redundant file scans when no new snapshot has been committed.
- **Scan result caching**: The full table scan result is cached by snapshot ID (`lastScannedSnapshotId`). When multiple (partition, bucket) pairs need refresh within the same snapshot, only one scan is performed.
- **File deduplication**: Within each (partition, bucket), data files are deduplicated by `fileName` before passing to `tableQuery.refreshFiles()`. Multiple `DataSplit`s for the same partition/bucket can share data files, and passing duplicates causes Paimon's internal `Levels` constructor to fail because its `TreeSet` silently deduplicates level-0 files.

**Lookup flow:**

1.  The key bytes are already encoded in Paimon's BinaryRow format by the client-side `PaimonKeyEncoder`, so directly wrap them as a Paimon `BinaryRow` via `keyRow.pointTo(MemorySegment.wrap(key), 0, key.length)` — no decode/re-encode needed
2.  Call `tableQuery.lookup(partition, bucketId, keyRow)`
3.  If found, wrap Paimon result as Fluss `InternalRow` via `PaimonRowAsFlussRow` adapter
4.  Encode value using `CompactedRowEncoder` + `ValueEncoder.encodeValue(binaryRow)`

### C. Performance Isolation between Real-time and Historical Paths

Historical partition operations involve lake I/O with unpredictable latency. Without isolation, these slow operations would block resources shared with real-time paths. Current Fluss write path is fully synchronous on the RPC thread: `TabletService.putKv()` → `ReplicaManager.putRecordsToKv()` → `putToLocalKv()` → `KvTablet.putAsLeader()`. If a historical PK write needs lake old-value lookup on the RPC thread, it blocks not only other writes but all RPC processing on that thread.

The isolation strategy has two layers:

- **Server-side** (C.2, C.3): Historical operations are offloaded from RPC threads to a dedicated `ioExecutor`. Since real-time paths never use `ioExecutor`, there is no resource contention — full thread and queue capacity is available for historical operations.
- **Client-side** (C.4): For the **lookup path**, `LookupSender` splits inflight permits into realtime and historical semaphores with a configurable ratio (`client.lookup.historical-inflight-ratio`, default 0.1), preventing slow lake lookups from exhausting permits shared with real-time lookups. For the **write path**, no client-side ratio cap is applied — flow control relies entirely on the server-side semaphore (C.3) combined with client-side exponential backoff on throttle errors.

| Layer | Resource | Historical Cap | Mechanism |
|----|----|----|----|
| Server | `ioExecutor` thread pool | No cap needed — dedicated to historical (C.2) | Real-time paths never use ioExecutor |
| Server | Historical request semaphore | `maxQueued × server.historical-request-queue-ratio` (C.3) | `tryAcquire()` fails → `HISTORICAL_PARTITION_THROTTLED` |
| Client | Write throughput | Bounded by server semaphore (C.4.1) | Server throttle + client exponential backoff (100ms, 2×, max 5s) |
| Client | Lookup inflight permits | `maxLookupInflight × client.lookup.historical-inflight-ratio` (C.4.2) | Separate historical semaphore |

#### C.1 Client-side: Batching Constraints

Client sender/accumulator layer batches records with the following constraints:

1.  **Real-time vs. historical separation**: real-time partitions and `__historical__` partition are accumulated into **separate batches** and sent as **independent requests**. This ensures server receives requests that are purely real-time or purely historical, enabling clean dispatch at the request level.

2.  **Historical PK batching by original partition**: for Primary Key tables, historical put-kv requests must be further batched by `(historical bucket, original partition name)`. This is because `PbPutKvReqForBucket.partition_name` is a request-level field — a single request can only carry one original partition name. For upserts (`row != null`), the server can extract the original partition from row partition columns, but for key-only deletes (`row == null`) the server relies entirely on request-level `partition_name`. Mixing key-only deletes for different original partitions in the same request would make them indistinguishable.

    The batching key for historical PK writes is therefore: `(__historical__ bucket, original partition name)`, compared to normal PK writes which batch by `(partition, bucket)` only.

#### C.2 Server-side: Offload Historical Operations to IO Executor

`ReplicaManager` uses a shared `ioExecutor` (bounded thread pool) to offload all historical partition operations from RPC threads:

- **Write path**: `putRecordsToKv()` checks whether the target is `__historical__` partition. If so, the entire write processing is submitted to `ioExecutor` asynchronously, and the RPC thread is released immediately. Real-time writes continue to execute synchronously on the RPC thread as before.
- **Lookup path**: `lookups()` checks whether the target is `__historical__` partition. If so, the lookup is submitted to `ioExecutor` for async lake lookup. Normal lookups continue to execute synchronously against local RocksDB as before.

Both paths share the same `ioExecutor` because they are both lake I/O bound. A shared pool also provides a unified bound on total lake I/O concurrency per server.

``` text
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

Historical operations are offloaded to `ioExecutor` asynchronously, but the Netty server request queue is shared between real-time and historical requests. If historical requests accumulate without bound (e.g., due to slow lake I/O), they can fill up the shared request queue and cause real-time requests to be rejected. To prevent this, the server uses a bounded `Semaphore` (`HistoricalPartitionHandler.requestPermits`) to limit the number of concurrent in-flight historical requests. Each request acquires one permit via `tryAcquire()` (non-blocking) regardless of how many buckets it contains, and releases the permit when all its sub-tasks complete. The capacity is computed as `maxQueuedRequests * server.historical-request-queue-ratio` (default ratio 0.1), reserving the majority of queue capacity for real-time requests.

When all permits are taken (`tryAcquire()` returns `false`), the server rejects the entire request with a `HISTORICAL_PARTITION_THROTTLED` error code. Client receives this error and performs **exponential backoff retry** (initial 100ms, multiplier 2×, max 5s, jitter 0.2). For write paths with idempotence enabled, the client also marks the bucket as throttled to prevent subsequent batches from being sent with out-of-order sequence numbers.

**Defensive copy for async write submission**: Before submitting a historical write to the `ioExecutor`, the server copies the `KvRecordBatch` to heap memory (`copyToHeap()`). This prevents `IndexOutOfBoundsException` from the Netty ByteBuf being released by the RPC layer while the async task is still processing.

Historical requests accepted by the semaphore are dispatched to the `ioExecutor` for processing. The semaphore and the `ioExecutor` are independent concerns: the semaphore bounds how many historical requests are admitted, while the `ioExecutor` determines how many are processed concurrently.

#### C.4 Client-side: Historical Resource Cap

The client caps historical operations at ~1/10 of total shared resources. This bounds the impact of historical writes/lookups on real-time paths without requiring complex per-resource analysis.

##### C.4.1 Write path: server-side throttle + client-side backoff

The write path relies on the server-side semaphore (C.3) as the primary flow control mechanism, combined with client-side exponential backoff on throttle errors:

1.  Server rejects historical writes when the semaphore is full, returning `HISTORICAL_PARTITION_THROTTLED`.
2.  Client `Sender` handles the throttle error with exponential backoff (`ExponentialBackoff(100, 2, 5000, 0.2)`):
    - Sets `retryAfterMs` on the batch based on the backoff delay.
    - If idempotence is enabled, marks the bucket as throttled via `idempotenceManager.markBucketThrottled()` to prevent subsequent batches from being drained for that bucket (which would cause `OutOfOrderSequenceException`).
    - Re-enqueues the batch for retry after the backoff period.
3.  Real-time batches are never affected — the throttle error and backoff only apply to historical batches targeting `__historical__` partition.

Effects:

- Historical write throughput is bounded by the server semaphore capacity (~1/10 of total request queue). Client automatically adapts its sending rate via backoff.
- **Idempotence safety**: bucket throttle marking prevents sequence number gaps when a batch is delayed by backoff.
- **Flush**: `flush()` waits for all in-flight batches including historical ones to complete. Given server-side lake I/O latency ≈ 10–100ms, this adds at most hundreds of milliseconds — well within checkpoint timeout.

##### C.4.2 Lookup path: historical permit cap

`LookupSender` splits the single inflight semaphore into two independent semaphores. The historical ratio is configured via `client.lookup.historical-inflight-ratio` (default 0.1):

``` text
historicalInflight = max(1, (int)(maxLookupInflight * historicalInflightRatio))  // e.g., 13
realtimeLookupSemaphore   = new Semaphore(maxLookupInflight - historicalInflight) // e.g., 115
historicalLookupSemaphore = new Semaphore(historicalInflight)                     // e.g., 13
```

`LookupSender` acquires the corresponding semaphore based on lookup type:

- Real-time lookup → `realtimeLookupSemaphore.acquire()` — only competes with other real-time lookups.
- Historical lookup → `historicalLookupSemaphore.acquire()` — only competes with other historical lookups.

Historical lookups are grouped by `partitionName` first, then by `(leader, type)` within each partition group. This ensures each RPC request carries the correct `partition_name` for composite key encoding on the server — different expired partitions (e.g., "2000", "2001") may redirect to the same `__historical__` bucket but must be sent in separate requests.

When `LookupSender` receives a `HISTORICAL_PARTITION_THROTTLED` error from the server, it applies the same exponential backoff strategy as the write path (`ExponentialBackoff(100, 2, 5000, 0.2)`), setting `retryAfterMs` on the lookup query to delay the retry.

Effects:

- Even if all historical permits are occupied by slow lake fallback requests, real-time lookups still have ~115 permits available.
- When historical permits are exhausted, the sender thread skips historical batches and continues processing real-time batches — historical lookups are retried in subsequent iterations.

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

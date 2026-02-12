/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.paimon.utils.PaimonDvTableUtils.findLatestSnapshotExactlyHoldingL0Files;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A retriever to retrieve the readable snapshot and offsets for Paimon deletion vector enabled
 * table.
 */
public class DvTableReadableSnapshotRetriever implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(DvTableReadableSnapshotRetriever.class);

    private final TablePath tablePath;
    private final long tableId;
    private final FileStoreTable fileStoreTable;
    private final Admin flussAdmin;
    private final Connection flussConnection;
    private final SnapshotManager snapshotManager;

    public DvTableReadableSnapshotRetriever(
            TablePath tablePath,
            long tableId,
            FileStoreTable paimonFileStoreTable,
            Configuration flussConfig) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.fileStoreTable = paimonFileStoreTable;
        this.flussConnection = ConnectionFactory.createConnection(flussConfig);
        this.flussAdmin = flussConnection.getAdmin();
        this.snapshotManager = fileStoreTable.snapshotManager();
    }

    /**
     * Get readable offsets for DV tables based on the latest compacted snapshot.
     *
     * <p>For Paimon DV tables, when an appended snapshot is committed, we need to check the latest
     * compacted snapshot to determine readable offsets for each bucket. This method implements
     * incremental advancement of readable_snapshot per bucket:
     *
     * <ul>
     *   <li>For buckets without L0 files: use offsets from the latest tiered snapshot. These
     *       buckets can advance their readable offsets since all their data is in base files (L1+).
     *   <li>For buckets with L0 files: traverse backwards through compacted snapshots to find the
     *       latest one that flushed this bucket's L0 files. Then find the latest snapshot that
     *       exactly holds those flushed L0 files, and use the previous APPEND snapshot's offset for
     *       that bucket.
     * </ul>
     *
     * <p>Algorithm:
     *
     * <ol>
     *   <li>Find the latest compacted snapshot before the given tiered snapshot
     *   <li>Look up Fluss (ZK lake node) to see if this compacted snapshot is already registered.
     *       If it exists, skip recomputing tiered and readable offsets and return null (no update
     *       needed). This avoids redundant work when many APPEND snapshots follow a single COMPACT.
     *   <li>Otherwise, check which buckets have no L0 files and which have L0 files in the
     *       compacted snapshot
     *   <li>For buckets without L0 files: use offsets from the latest tiered snapshot (all data is
     *       in base files, safe to advance)
     *   <li>For buckets with L0 files:
     *       <ol>
     *         <li>Traverse backwards through compacted snapshots starting from the latest one
     *         <li>For each compacted snapshot, check which buckets had their L0 files flushed
     *         <li>For each flushed bucket, find the latest snapshot that exactly holds those L0
     *             files using {@link PaimonDvTableUtils#findLatestSnapshotExactlyHoldingL0Files}
     *         <li>Find the previous APPEND snapshot before that snapshot
     *         <li>Use that APPEND snapshot's offset for the bucket
     *       </ol>
     *   <li>Return readable offsets for all buckets, allowing incremental advancement
     * </ol>
     *
     * <p>Note: This allows readable_snapshot to advance incrementally per bucket. Each bucket's
     * readable offset is set to the maximum offset that is actually readable in the compacted
     * snapshot, ensuring no data duplication or loss. The readable_snapshot is set to the latest
     * compacted snapshot ID, and each bucket continues reading from its respective readable offset.
     *
     * <p>Example: If bucket0's L0 files were flushed in snapshot5 (which compacted snapshot1's L0
     * files), and snapshot4 is the latest snapshot that exactly holds those L0 files, then
     * bucket0's readable offset will be set to snapshot4's previous APPEND snapshot's offset.
     *
     * @param tieredSnapshotId the tiered snapshot ID (the appended snapshot that was just
     *     committed)
     * @return a tuple containing the readable snapshot ID (the latest compacted snapshot) and a map
     *     of TableBucket to readable offset for all buckets, or null if:
     *     <ul>
     *       <li>The latest compacted snapshot is already registered in Fluss (ZK); no update needed
     *       <li>No compacted snapshot exists before the tiered snapshot
     *       <li>Cannot find the latest snapshot holding flushed L0 files for some buckets
     *       <li>Cannot find the previous APPEND snapshot for some buckets
     *       <li>Cannot find offsets in Fluss for some buckets
     *     </ul>
     *     The map contains offsets for ALL buckets, allowing incremental advancement.
     * @throws IOException if an error occurs reading snapshots or offsets from Fluss
     */
    @Nullable
    public ReadableSnapshotResult getReadableSnapshotAndOffsets(long tieredSnapshotId)
            throws IOException {
        // Find the latest compacted snapshot
        Snapshot latestCompactedSnapshot =
                findPreviousSnapshot(tieredSnapshotId, Snapshot.CommitKind.COMPACT);
        if (latestCompactedSnapshot == null) {
            // No compacted snapshot found, may happen when no compaction happens or snapshot
            // expiration, we can't update readable offsets, return null directly
            LOG.info(
                    "Can't find latest compacted snapshot before snapshot {}, skip get readable snapshot.",
                    tieredSnapshotId);
            return null;
        }

        LakeSnapshot lastCompactedLakeSnapshot = null;

        try {
            // Attempt to retrieve the snapshot from Fluss.
            // This is a blocking call to unwrap the future.
            lastCompactedLakeSnapshot =
                    flussAdmin.getLakeSnapshot(tablePath, latestCompactedSnapshot.id()).get();
        } catch (Exception e) {
            Throwable cause = ExceptionUtils.stripExecutionException(e);

            // If the error is anything other than the snapshot simply not existing,
            // we log a warning but do not interrupt the flow.
            if (!(cause instanceof LakeTableSnapshotNotExistException)) {
                LOG.warn(
                        "Failed to retrieve lake snapshot {} from Fluss. "
                                + "Will attempt to advance readable snapshot as a fallback.",
                        latestCompactedSnapshot.id(),
                        cause);
            }
            // If LakeTableSnapshotNotExistException occurs, we silently fall through
            // as it is an expected case when the snapshot hasn't been recorded yet.
        }

        // If we successfully retrieved a snapshot, we must validate its integrity.
        if (lastCompactedLakeSnapshot != null) {
            // Consistency Check: The ID in Fluss must strictly match the expected compacted ID.
            // Should never happen
            // If they differ, it indicates a critical state mismatch in the metadata.
            checkState(
                    lastCompactedLakeSnapshot.getSnapshotId() == latestCompactedSnapshot.id(),
                    "Snapshot ID mismatch detected! Expected: %s, Actual in Fluss: %s",
                    latestCompactedSnapshot.id(),
                    lastCompactedLakeSnapshot.getSnapshotId());

            // If the snapshot already exists and is valid, no further action (advancing) is
            // required.
            return null;
        }

        Map<TableBucket, Long> readableOffsets = new HashMap<>();

        FlussTableBucketMapper flussTableBucketMapper = new FlussTableBucketMapper();

        // get all the bucket without l0 files and with l0 files
        Tuple2<Set<PaimonPartitionBucket>, Set<PaimonPartitionBucket>> bucketsWithoutL0AndWithL0 =
                getBucketsWithoutL0AndWithL0(latestCompactedSnapshot);
        Set<PaimonPartitionBucket> bucketsWithoutL0 = bucketsWithoutL0AndWithL0.f0;
        Set<PaimonPartitionBucket> bucketsWithL0 = bucketsWithoutL0AndWithL0.f1;

        // Track the earliest previousAppendSnapshot ID that was accessed
        // This represents the oldest snapshot that might still be needed
        long earliestSnapshotIdToKeep = LakeCommitResult.KEEP_ALL_PREVIOUS;

        if (!bucketsWithoutL0.isEmpty()) {
            // Get latest tiered offsets
            LakeSnapshot latestTieredSnapshot;
            try {
                latestTieredSnapshot = flussAdmin.getLatestLakeSnapshot(tablePath).get();
            } catch (Exception e) {
                LOG.warn(
                        "Failed to get latest lake snapshot from Fluss server for compacted snapshot {}; "
                                + "skipping readable snapshot update.",
                        latestCompactedSnapshot.id(),
                        e);
                return null;
            }
            checkTableConsistent(latestTieredSnapshot);
            // for all buckets without l0, we can use the latest tiered offsets
            for (PaimonPartitionBucket bucket : bucketsWithoutL0) {
                TableBucket tableBucket = flussTableBucketMapper.toTableBucket(bucket);
                if (tableBucket == null) {
                    // can't map such paimon bucket to fluss, just ignore
                    continue;
                }
                readableOffsets.put(
                        tableBucket, latestTieredSnapshot.getTableBucketsOffset().get(tableBucket));
            }
        }

        Snapshot compactedSnapshotPreviousAppendSnapshot =
                findPreviousSnapshot(latestCompactedSnapshot.id(), Snapshot.CommitKind.APPEND);
        if (compactedSnapshotPreviousAppendSnapshot == null) {
            LOG.warn(
                    "Failed to find a previous APPEND snapshot before compacted snapshot {} for table {}. "
                            + "This prevents retrieving baseline offsets from Fluss.",
                    latestCompactedSnapshot.id(),
                    tablePath);
            return null;
        }

        // We keep snapshots because for a compacted snapshot, if a bucket has L0, we find the
        // snapshot that exactly holds those L0, then use that snapshot's previous APPEND's tiered
        // offset as the readable offset (that offset is safe to read). When the current compacted
        // snapshot has no L0 in any bucket, we do not traverse; for any later compact we would
        // traverse to (going backwards in time), if some bucket has L0, the snapshot that exactly
        // holds that L0 must be after the current compacted snapshot on the timeline. So that
        // snapshot's previous APPEND cannot be earlier than the current compacted snapshot's
        // previous APPEND. Therefore the minimum snapshot we need to keep is the current compact's
        // previous APPEND; set earliestSnapshotIdToKeep to it so it is not deleted. Earlier
        // snapshots may be safely deleted.
        if (bucketsWithL0.isEmpty()) {
            earliestSnapshotIdToKeep = compactedSnapshotPreviousAppendSnapshot.id();
        }

        // for all buckets with l0, we need to find the latest compacted snapshot which flushed
        // the buckets, the per-bucket offset should be updated to the corresponding compacted
        // snapshot offsets
        Set<PaimonPartitionBucket> allBucketsToAdvance = new HashSet<>(bucketsWithL0);

        // Cache LakeSnapshot by snapshot ID to avoid repeated getLakeSnapshot RPCs when many
        // buckets share the same snapshot.
        Map<Long, LakeSnapshot> lakeSnapshotBySnapshotId = new HashMap<>();

        long earliestSnapshotId = checkNotNull(snapshotManager.earliestSnapshotId());
        // From latestCompacted forward traverse compacted snapshots
        for (long currentSnapshotId = latestCompactedSnapshot.id();
                currentSnapshotId >= earliestSnapshotId;
                currentSnapshotId--) {
            // no any buckets to advance, break directly
            if (allBucketsToAdvance.isEmpty()) {
                break;
            }
            Snapshot currentSnapshot = snapshotManager.tryGetSnapshot(currentSnapshotId);
            if (currentSnapshot == null
                    || currentSnapshot.commitKind() != Snapshot.CommitKind.COMPACT) {
                continue;
            }
            // Get buckets flushed by current compacted snapshot
            Set<PaimonPartitionBucket> flushedBuckets = getBucketsWithFlushedL0(currentSnapshot);
            // For each flushed bucket, if offset not set yet, set it
            for (PaimonPartitionBucket partitionBucket : flushedBuckets) {
                TableBucket tb = flussTableBucketMapper.toTableBucket(partitionBucket);
                if (tb == null) {
                    // can't map such paimon bucket to fluss,just ignore
                    // don't need to advance offset for the bucket
                    allBucketsToAdvance.remove(partitionBucket);
                    continue;
                }
                if (!readableOffsets.containsKey(tb)) {
                    Snapshot sourceSnapshot =
                            findLatestSnapshotExactlyHoldingL0Files(
                                    fileStoreTable, currentSnapshot);
                    // it happens if there is a compacted snapshot flush l0 files for a bucket,
                    // but the snapshot from which the compacted snapshot compact is expired
                    // it should happen rarely, we can't determine the readable offsets for this
                    // bucket, currently, we just return null to stop readable offset advance
                    // if it happen, compaction should work unexpected, warn it and reminds to
                    // increase snapshot retention
                    if (sourceSnapshot == null) {
                        LOG.warn(
                                "Cannot find snapshot holding L0 files flushed by compacted snapshot {} for bucket {}, "
                                        + "the snapshot may have been expired. Consider increasing snapshot retention.",
                                currentSnapshot.id(),
                                tb);
                        return null;
                    }

                    // we already find that for this bucket, which snapshot do the latest flush,
                    // the offset for the previous one append snapshot should be the readable
                    // offset
                    Snapshot previousAppendSnapshot =
                            sourceSnapshot.commitKind() == Snapshot.CommitKind.APPEND
                                    ? sourceSnapshot
                                    : findPreviousSnapshot(
                                            sourceSnapshot.id(), Snapshot.CommitKind.APPEND);

                    // Can't find previous APPEND snapshot, likely due to snapshot expiration.
                    // This happens when the snapshot holding flushed L0 files is a COMPACT
                    // snapshot,
                    // and all APPEND snapshots before it have been expired.
                    //
                    // TODO: Optimization - Store compacted snapshot offsets in Fluss
                    // Currently, we rely on Paimon to find the previous APPEND snapshot to get its
                    // offset. If Fluss stores offsets for all snapshots (including COMPACT
                    // snapshots),
                    // we could:
                    // 1. Use the sourceSnapshot's offset directly if it's stored in Fluss
                    // 2. Find any previous snapshot (COMPACT or APPEND) and use its offset
                    // 3. This would make the system more resilient to snapshot expiration
                    if (previousAppendSnapshot == null) {
                        LOG.warn(
                                "Cannot find previous APPEND snapshot before snapshot {} for bucket {}. "
                                        + "This may be due to snapshot expiration. Consider increasing paimon snapshot retention.",
                                sourceSnapshot.id(),
                                tb);
                        return null;
                    }

                    // Track the minimum previousAppendSnapshot ID
                    // This snapshot will be accessed via getLakeSnapshot, so we need to keep it
                    if (earliestSnapshotIdToKeep <= 0
                            || previousAppendSnapshot.id() < earliestSnapshotIdToKeep) {
                        earliestSnapshotIdToKeep = previousAppendSnapshot.id();
                    }

                    long snapshotId = previousAppendSnapshot.id();
                    LakeSnapshot lakeSnapshot =
                            getOrFetchLakeSnapshot(snapshotId, lakeSnapshotBySnapshotId);
                    if (lakeSnapshot == null) {
                        return null;
                    }
                    Long offset = lakeSnapshot.getTableBucketsOffset().get(tb);
                    if (offset != null) {
                        readableOffsets.put(tb, offset);
                        allBucketsToAdvance.remove(partitionBucket);
                    } else {
                        LOG.error(
                                "Could not find offset for bucket {} in snapshot {}, skip advancing readable snapshot.",
                                tb,
                                snapshotId);
                        return null;
                    }
                }
            }
        }

        // This happens when there are writes to a bucket, but no compaction has happened for that
        // bucket from the earliest snapshot to the latest compacted snapshot.
        // This should happen rarely in practice, as compaction typically processes all buckets over
        // time.
        //
        // TODO: Optimization - Handle buckets without flushed L0 files
        // We can optimize this case in two ways:
        // 1. If a previous readable snapshot exists between earliest and latest snapshot:
        //    - Reuse the readable snapshot's offset for this bucket (safe since no L0 was flushed)
        // 2. If the earliest snapshot is the first snapshot committed by Fluss:
        //    - Set the readable offset to 0 for this bucket (no data was readable before)
        // These optimizations would allow readable_snapshot to advance even when some buckets
        // haven't been compacted yet, improving overall system progress.
        if (!allBucketsToAdvance.isEmpty()) {
            LOG.warn(
                    "Could not find flushed snapshots for buckets with L0: {}. "
                            + "These buckets have L0 files but no found compaction snapshot has flushed them yet."
                            + " Consider increasing paimon snapshot retention.",
                    allBucketsToAdvance);
            return null;
        }

        // we use the previous append snapshot tiered offset of the compacted snapshot as the
        // compacted snapshot tiered offsets
        LakeSnapshot tieredLakeSnapshot =
                getOrFetchLakeSnapshot(
                        compactedSnapshotPreviousAppendSnapshot.id(), lakeSnapshotBySnapshotId);
        if (tieredLakeSnapshot == null) {
            return null;
        }
        Map<TableBucket, Long> tieredOffsets = tieredLakeSnapshot.getTableBucketsOffset();

        // Return the latest compacted snapshot ID as the unified readable snapshot
        // All buckets can read from this snapshot's base files, then continue from their
        // respective readable offsets
        // Also return the minimum previousAppendSnapshot ID that was accessed
        // Snapshots before this ID can potentially be safely deleted from Fluss
        return new ReadableSnapshotResult(
                latestCompactedSnapshot.id(),
                tieredOffsets,
                readableOffsets,
                earliestSnapshotIdToKeep);
    }

    /**
     * Checks that the given lake snapshot belongs to the current table (same table id). Throws when
     * the table may have been dropped and re-created with a different id; the tiering committer
     * operator will handle the exception.
     *
     * @param lakeSnapshot the snapshot from Fluss
     * @throws IllegalStateException if the snapshot's table id does not match the current table id
     */
    private void checkTableConsistent(LakeSnapshot lakeSnapshot) {
        if (lakeSnapshot.getTableBucketsOffset().isEmpty()) {
            return;
        }
        long snapshotTableId =
                lakeSnapshot.getTableBucketsOffset().keySet().iterator().next().getTableId();
        if (snapshotTableId != tableId) {
            throw new IllegalStateException(
                    String.format(
                            "Table id mismatch: Fluss snapshot is for table %s but current tiering table is %s. "
                                    + "Table may have been re-created.",
                            snapshotTableId, tableId));
        }
    }

    /**
     * Gets a lake snapshot by id, using the cache to avoid repeated RPCs. On cache miss, fetches
     * from Fluss, checks table consistency, and puts the result in the cache.
     *
     * @param snapshotId the snapshot id
     * @param cache cache keyed by snapshot id (mutated on miss)
     * @return the snapshot, or null if fetch fails (logs on failure)
     */
    @Nullable
    private LakeSnapshot getOrFetchLakeSnapshot(long snapshotId, Map<Long, LakeSnapshot> cache) {
        LakeSnapshot snapshot = cache.get(snapshotId);
        if (snapshot != null) {
            return snapshot;
        }
        try {
            snapshot = flussAdmin.getLakeSnapshot(tablePath, snapshotId).get();
        } catch (Exception e) {
            LOG.error(
                    "Failed to retrieve lake snapshot {} from Fluss server for table {}; skipping readable snapshot update.",
                    tablePath,
                    snapshotId,
                    e);
            return null;
        }
        checkTableConsistent(snapshot);
        cache.put(snapshotId, snapshot);
        return snapshot;
    }

    /**
     * Get buckets (with partition info) that have no L0 files in the given snapshot.
     *
     * <p>For Paimon DV tables, we check the snapshot's data files to determine which buckets have
     * no L0 delta files. A bucket has no L0 files if all its data is in base files.
     *
     * <p>For partitioned tables, we include partition information in the returned TableBucket
     * objects.
     *
     * @param snapshot the snapshot to check
     * @return set of TableBucket that have no L0 files
     */
    private Tuple2<Set<PaimonPartitionBucket>, Set<PaimonPartitionBucket>>
            getBucketsWithoutL0AndWithL0(Snapshot snapshot) {
        Set<PaimonPartitionBucket> bucketsWithoutL0 = new HashSet<>();
        Set<PaimonPartitionBucket> bucketsWithL0 = new HashSet<>();

        // Scan the snapshot to get all splits including level0
        Map<String, String> scanOptions = new HashMap<>();
        scanOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshot.id()));
        // hacky: set batch scan mode to compact to make sure we can get l0 level files
        scanOptions.put(
                CoreOptions.BATCH_SCAN_MODE.key(), CoreOptions.BatchScanMode.COMPACT.getValue());

        Map<BinaryRow, Map<Integer, List<ManifestEntry>>> manifestsByBucket =
                FileStoreScan.Plan.groupByPartFiles(
                        fileStoreTable.copy(scanOptions).store().newScan().plan().files());

        for (Map.Entry<BinaryRow, Map<Integer, List<ManifestEntry>>> manifestsByBucketEntry :
                manifestsByBucket.entrySet()) {
            BinaryRow partition = manifestsByBucketEntry.getKey();
            Map<Integer, List<ManifestEntry>> buckets = manifestsByBucketEntry.getValue();
            for (Map.Entry<Integer, List<ManifestEntry>> bucketEntry : buckets.entrySet()) {
                // no l0 file
                if (bucketEntry.getValue().stream()
                        .allMatch(
                                manifestEntry ->
                                        manifestEntry.kind() != FileKind.DELETE
                                                && manifestEntry.file().level() > 0)) {
                    bucketsWithoutL0.add(
                            new PaimonPartitionBucket(partition, bucketEntry.getKey()));
                } else {
                    bucketsWithL0.add(new PaimonPartitionBucket(partition, bucketEntry.getKey()));
                }
            }
        }
        return Tuple2.of(bucketsWithoutL0, bucketsWithL0);
    }

    /**
     * Get buckets (with partition info) whose L0 files were flushed (deleted) in a compacted
     * snapshot's delta.
     *
     * @param compactedSnapshot the compacted snapshot to check
     * @return set of PaimonPartitionBucket whose L0 files were flushed
     */
    private Set<PaimonPartitionBucket> getBucketsWithFlushedL0(Snapshot compactedSnapshot) {
        checkState(compactedSnapshot.commitKind() == Snapshot.CommitKind.COMPACT);
        Set<PaimonPartitionBucket> flushedBuckets = new HashSet<>();

        // Scan the compacted snapshot's delta to find deleted L0 files
        List<ManifestEntry> manifestEntries =
                fileStoreTable
                        .store()
                        .newScan()
                        .withSnapshot(compactedSnapshot.id())
                        .withKind(ScanMode.DELTA)
                        .plan()
                        .files(FileKind.DELETE);

        for (ManifestEntry manifestEntry : manifestEntries) {
            if (manifestEntry.level() == 0) {
                flushedBuckets.add(
                        new PaimonPartitionBucket(
                                manifestEntry.partition(), manifestEntry.bucket()));
            }
        }

        return flushedBuckets;
    }

    @Nullable
    private Snapshot findPreviousSnapshot(long beforeSnapshotId, Snapshot.CommitKind commitKind)
            throws IOException {
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        long earliestSnapshotId = checkNotNull(snapshotManager.earliestSnapshotId());
        for (long currentSnapshotId = beforeSnapshotId - 1;
                currentSnapshotId >= earliestSnapshotId;
                currentSnapshotId--) {
            Snapshot snapshot = snapshotManager.tryGetSnapshot(currentSnapshotId);
            if (snapshot != null && snapshot.commitKind() == commitKind) {
                return snapshot;
            }
        }
        return null;
    }

    /**
     * Get partition name to partition id mapping for the table.
     *
     * @return map from partition name to partition id
     */
    private Map<String, Long> getPartitionNameToIdMapping() throws IOException {
        try {
            List<PartitionInfo> partitionInfos = flussAdmin.listPartitionInfos(tablePath).get();
            return partitionInfos.stream()
                    .collect(
                            Collectors.toMap(
                                    PartitionInfo::getPartitionName,
                                    PartitionInfo::getPartitionId));
        } catch (Exception e) {
            throw new IOException("Fail to list partitions", e);
        }
    }

    /**
     * Convert Paimon BinaryRow partition to Fluss partition name, whose format is:
     * value1$value2$...$valueN.
     *
     * @param partition the BinaryRow partition from Paimon
     * @return partition name string
     */
    private String getPartitionNameFromBinaryRow(BinaryRow partition) {
        List<String> partitionValues = new ArrayList<>();
        for (int i = 0; i < partition.getFieldCount(); i++) {
            // todo: consider other partition type
            BinaryString binaryString = partition.getString(i);
            partitionValues.add(binaryString.toString());
        }
        return String.join(ResolvedPartitionSpec.PARTITION_SPEC_SEPARATOR, partitionValues);
    }

    @Override
    public void close() throws Exception {
        if (flussAdmin != null) {
            flussAdmin.close();
        }
        if (flussConnection != null) {
            flussConnection.close();
        }
    }

    /**
     * Result of {@link #getReadableSnapshotAndOffsets}, containing readable snapshot information
     * and the minimum snapshot ID that can be safely deleted.
     */
    public static class ReadableSnapshotResult {
        private final long readableSnapshotId;
        private final Map<TableBucket, Long> tieredOffsets;
        private final Map<TableBucket, Long> readableOffsets;
        private final long earliestSnapshotIdToKeep;

        public ReadableSnapshotResult(
                long readableSnapshotId,
                Map<TableBucket, Long> tieredOffsets,
                Map<TableBucket, Long> readableOffsets,
                long earliestSnapshotIdToKeep) {
            this.readableSnapshotId = readableSnapshotId;
            this.tieredOffsets = tieredOffsets;
            this.readableOffsets = readableOffsets;
            this.earliestSnapshotIdToKeep = earliestSnapshotIdToKeep;
        }

        public Map<TableBucket, Long> getTieredOffsets() {
            return tieredOffsets;
        }

        public long getReadableSnapshotId() {
            return readableSnapshotId;
        }

        public Map<TableBucket, Long> getReadableOffsets() {
            return readableOffsets;
        }

        /**
         * Returns the earliest snapshot ID that should keep in Fluss.
         *
         * <p>This is the earliest ID among all snapshot that were accessed via {@code
         * getLakeSnapshot} during the retrieve readable offset. Snapshots before this ID can
         * potentially be safely deleted.
         */
        public long getEarliestSnapshotIdToKeep() {
            return earliestSnapshotIdToKeep;
        }
    }

    private final class FlussTableBucketMapper {
        private final Map<String, Long> partitionNameToIdMapping;

        private FlussTableBucketMapper() throws IOException {
            if (!fileStoreTable.partitionKeys().isEmpty()) {
                partitionNameToIdMapping = getPartitionNameToIdMapping();
            } else {
                partitionNameToIdMapping = null;
            }
        }

        @Nullable
        private TableBucket toTableBucket(PaimonPartitionBucket partitionBucket) {
            if (partitionBucket.getPartition().getFieldCount() == 0) {
                // Non-partitioned table: BinaryRow.EMPTY_ROW has 0 fields
                return new TableBucket(tableId, partitionBucket.getBucket());
            } else {
                // Partitioned table: convert partition name to partition id
                String partitionName =
                        getPartitionNameFromBinaryRow(partitionBucket.getPartition());
                Long partitionId = partitionNameToIdMapping.get(partitionName);
                if (partitionId == null) {
                    LOG.warn(
                            "Partition name '{}' not found in Fluss for table {}. "
                                    + "Available partitions: {}",
                            partitionName,
                            tablePath,
                            partitionNameToIdMapping.keySet());
                    return null;
                }
                return new TableBucket(tableId, partitionId, partitionBucket.getBucket());
            }
        }
    }
}

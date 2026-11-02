/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.dv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.StaleSnapshotException;
import org.apache.fluss.server.entity.DvPositionReportData;
import org.apache.fluss.server.utils.RoaringBitmapUtils;
import org.apache.fluss.utils.CloseableIterator;

import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Core state machine managing Deletion Vector writes during the KvTablet write path, as well as the
 * Prepare and Readable Switch phases for DV orchestration.
 *
 * <p>When a -U (update-before) or -D (delete) record is produced, this manager:
 *
 * <ol>
 *   <li>Marks the old record's changelog offset in LogDv (so tiering/union-read can skip superseded
 *       records)
 *   <li>Looks up RowPosIndex to see if the old record's lake position is known
 *   <li>If known: marks LakeDv, writes resolved PendingDeletes, deletes RowPosIndex entry
 *   <li>If unknown: writes pending PendingDeletes for future resolution
 * </ol>
 *
 * <p>During DV orchestration, the coordinator sends Prepare and Readable Switch requests:
 *
 * <ul>
 *   <li><b>Prepare</b>: Downloads SST files from remote storage, writes FileDict entries, and
 *       caches pending state for the subsequent Readable Switch.
 *   <li><b>Readable Switch</b>: Ingests SST into RowPosIndex, batch-resolves PendingDeletes, cleans
 *       up old files, and updates the readable snapshot state.
 * </ul>
 */
@Internal
public class DvManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DvManager.class);

    private final DvRocksDB dvRocksDB;
    private final DvRWLock dvRWLock;

    // Snapshot ID for which Prepare has completed (updated as last step of handlePrepare)
    private long preparedSnapshotId = -1;

    // Current DV-readable snapshot ID (updated as last step of handleReadableSwitch)
    private long readableSnapshotId = -1;

    // Log start offset of the current readable snapshot (updated during handleReadableSwitch)
    private long snapshotStartLogOffset = -1;

    // SST file paths downloaded during Prepare, consumed by Readable Switch
    private final Map<Integer, List<String>> pendingSstPaths = new HashMap<>();

    // Old file IDs resolved during Prepare, consumed by Readable Switch for cleanup
    private final Map<Integer, List<Integer>> pendingOldFileIds = new HashMap<>();

    // Per-bucket readable offsets cached during Prepare, consumed by Readable Switch
    private final Map<Integer, Long> pendingReadableOffsets = new HashMap<>();

    public DvManager(DvRocksDB dvRocksDB, DvRWLock dvRWLock) {
        this.dvRocksDB = dvRocksDB;
        this.dvRWLock = dvRWLock;
    }

    /**
     * Processes collected -U/-D entries after changelog append succeeds. This is the main entry
     * point called from KvTablet's putAsLeader after the WAL batch is synced.
     *
     * @param entries the DV entries to process, each representing a superseded +I/+U record
     */
    public void handleChangelogSynced(List<DvEntry> entries) throws IOException {
        dvRWLock.writeLock();
        try {
            for (DvEntry entry : entries) {
                long oldRowId = entry.getOldRowId();

                // 1. Mark LogDv: the old +I/+U offset is now superseded
                dvRocksDB.logDv().markDeleted(oldRowId);

                // 2. Point-get RowPosIndex to check if old record's lake position is known
                FilePos filePos = dvRocksDB.rowPosIndex().get(oldRowId);

                if (filePos != null) {
                    // Hit: old record's lake position is known
                    // Mark LakeDv (per-file deletion bitmap)
                    dvRocksDB.lakeDv().markDeleted(filePos.fileId(), filePos.rowPosition());
                    // Delete RowPosIndex entry (consumed)
                    dvRocksDB.rowPosIndex().delete(oldRowId);
                    // Write PendingDeletes with resolved position
                    dvRocksDB.pendingDeletes().put(oldRowId, filePos);
                } else {
                    // Miss: position unknown (data not yet tiered or compacted)
                    dvRocksDB.pendingDeletes().putPending(oldRowId);
                }
            }
        } finally {
            dvRWLock.writeUnlock();
        }
    }

    /**
     * Handles DV Prepare phase for this tablet's bucket.
     *
     * <p>No DvRWLock needed: FileDict writes are RocksDB thread-safe and no other operation
     * concurrently modifies FileDict; in-memory pending maps are only accessed sequentially by
     * Prepare/Switch (coordinator orchestration).
     *
     * @param bucketId this tablet's bucket ID
     * @param bucketOffset per-bucket DV data (readableOffset, newFileDictEntries, oldFiles)
     * @param downloader SST downloader configured with remote dir
     * @param snapshotId the index snapshot ID the RowPos SST was uploaded under (may differ from
     *     the readable snapshot; used only to locate the SST files)
     * @param localTempDir local directory for downloaded SST files
     */
    public void handlePrepare(
            int bucketId,
            @Nullable Long partitionId,
            DvPositionReportData.DvBucketOffset bucketOffset,
            RowPosSstDownloader downloader,
            long snapshotId,
            String localTempDir)
            throws IOException {

        // Idempotency: preparedSnapshotId is updated as the last step,
        // so if it already matches, the entire Prepare has completed before.
        if (this.preparedSnapshotId == snapshotId) {
            LOG.info("DV Prepare already completed for snapshot {}, skipping", snapshotId);
            return;
        }

        // Download SST from remote storage
        List<String> sstPaths =
                downloader.downloadBucketSst(snapshotId, partitionId, bucketId, localTempDir);

        // Write newFileDictEntries to FileDict CF
        Map<Integer, String> newEntries = bucketOffset.getNewFileDictEntries();
        for (Map.Entry<Integer, String> entry : newEntries.entrySet()) {
            dvRocksDB.fileDict().put(entry.getKey(), entry.getValue());
        }

        // Resolve oldFiles: convert file_path to file_id
        List<Integer> oldFileIds = new ArrayList<>();
        for (String oldFilePath : bucketOffset.getOldFiles()) {
            int fileId = dvRocksDB.fileDict().getFileId(oldFilePath);
            if (fileId >= 0) {
                oldFileIds.add(fileId);
            }
        }

        // Store SST paths and old file IDs for Readable Switch
        if (!sstPaths.isEmpty()) {
            pendingSstPaths.put(bucketId, sstPaths);
        }
        if (!oldFileIds.isEmpty()) {
            pendingOldFileIds.put(bucketId, oldFileIds);
        }

        // Cache readable offset for Readable Switch
        pendingReadableOffsets.put(bucketId, bucketOffset.getReadableOffset());

        // Mark Prepare as completed (must be last step for idempotency)
        this.preparedSnapshotId = snapshotId;
    }

    /**
     * Handles DV Readable Switch for this tablet's bucket.
     *
     * <p>All steps under DvRWLock write lock:
     *
     * <ol>
     *   <li>Ingest SST into RowPosIndex
     *   <li>Batch resolve PendingDeletes
     *   <li>Cleanup oldFiles from LakeDv and PendingDeletes
     *   <li>Cleanup expired LogDv
     *   <li>Update readableSnapshotId and snapshotStartLogOffset
     * </ol>
     *
     * @param bucketId this tablet's bucket ID
     * @param readableSnapshotId the new readable snapshot ID
     * @param readableOffset the per-bucket readable offset (= snapshotStartLogOffset)
     */
    public void handleReadableSwitch(int bucketId, long readableSnapshotId, long readableOffset)
            throws IOException {

        // Idempotency: readableSnapshotId is updated as the last step,
        // so if it already matches, the entire Switch has completed before.
        if (this.readableSnapshotId == readableSnapshotId) {
            LOG.info(
                    "DV ReadableSwitch already completed for snapshot {}, skipping",
                    readableSnapshotId);
            return;
        }

        dvRWLock.writeLock();
        try {
            // Step 1: Ingest SST into RowPosIndex
            List<String> sstPaths = pendingSstPaths.remove(bucketId);
            if (sstPaths != null && !sstPaths.isEmpty()) {
                dvRocksDB.rowPosIndex().ingestExternalFile(sstPaths);
            }

            // Step 2: Cleanup oldFiles (compaction) first: their masks are re-queued as pending
            List<Integer> oldFileIds = pendingOldFileIds.remove(bucketId);
            List<Long> requeued =
                    oldFileIds != null ? cleanupOldFiles(oldFileIds) : Collections.emptyList();

            // Step 3: Batch resolve PendingDeletes (re-queued masks land on compacted positions)
            batchResolvePendingDeletes(readableOffset);
            // a re-queued row absent from the compacted files was dropped by the rewrite
            for (long rowId : requeued) {
                PendingDeletes.PendingDeleteEntry e = dvRocksDB.pendingDeletes().get(rowId);
                if (e != null && e.isPending()) {
                    dvRocksDB.pendingDeletes().delete(rowId);
                }
            }

            // Step 4: Cleanup expired LogDv
            dvRocksDB.logDv().cleanup(readableOffset);

            // Step 5: Update state
            this.readableSnapshotId = readableSnapshotId;
            this.snapshotStartLogOffset = readableOffset;

            // Consume the pending readable offset
            pendingReadableOffsets.remove(bucketId);
        } finally {
            dvRWLock.writeUnlock();
        }
    }

    /**
     * Batch resolves PendingDeletes against RowPosIndex.
     *
     * <p>For each pending entry in PendingDeletes:
     *
     * <ul>
     *   <li>RowPosIndex hit: mark LakeDv, delete RowPosIndex, update PendingDeletes with filePos
     *   <li>RowPosIndex miss + rowId &lt; readableOffset: orphan, delete PendingDeletes
     *   <li>RowPosIndex miss + rowId &gt;= readableOffset: keep for next round
     * </ul>
     */
    private void batchResolvePendingDeletes(long readableOffset) throws IOException {
        // Phase 1: Iterate and collect changes (RocksDB iterator is snapshot-isolated)
        List<ResolveAction> actions = new ArrayList<>();
        try (CloseableIterator<PendingDeletes.PendingDeleteEntry> iter =
                dvRocksDB.pendingDeletes().iterator()) {
            while (iter.hasNext()) {
                PendingDeletes.PendingDeleteEntry entry = iter.next();
                // Only process entries that are still pending (position unknown)
                if (!entry.isPending()) {
                    continue;
                }

                long rowId = entry.getRowId();
                FilePos hit = dvRocksDB.rowPosIndex().get(rowId);
                if (hit != null) {
                    actions.add(new ResolveAction(rowId, hit, true));
                } else if (rowId < readableOffset) {
                    actions.add(new ResolveAction(rowId, null, false));
                }
                // else: rowId >= readableOffset, keep for next round
            }
        }

        // Phase 2: Apply all changes
        for (ResolveAction action : actions) {
            if (action.resolved) {
                dvRocksDB
                        .lakeDv()
                        .markDeleted(action.filePos.fileId(), action.filePos.rowPosition());
                dvRocksDB.rowPosIndex().delete(action.rowId);
                dvRocksDB.pendingDeletes().put(action.rowId, action.filePos);
            } else {
                // Orphan: row was tiered but never in a data file
                dvRocksDB.pendingDeletes().delete(action.rowId);
            }
        }
    }

    /**
     * Cleans up LakeDv and PendingDeletes entries for old files removed by compaction.
     *
     * @param oldFileIds file IDs of the old files to clean up
     */
    private List<Long> cleanupOldFiles(List<Integer> oldFileIds) throws IOException {
        Set<Integer> oldFileIdSet = new HashSet<>(oldFileIds);

        // Remove LakeDv entries for old files
        for (int fileId : oldFileIds) {
            dvRocksDB.lakeDv().delete(fileId);
        }

        // Prune RowPosIndex entries pointing to old files. Without this, a stale
        // rowId->(oldFile,pos)
        // mapping survives removal and a later supersede/resolve could mark a position that a
        // surviving/live row now occupies -> over-masking (visible count drops below true count).
        dvRocksDB.rowPosIndex().pruneFiles(oldFileIdSet);

        // Remove PendingDeletes entries pointing to old files
        List<Long> toDelete = new ArrayList<>();
        try (CloseableIterator<PendingDeletes.PendingDeleteEntry> iter =
                dvRocksDB.pendingDeletes().iterator()) {
            while (iter.hasNext()) {
                PendingDeletes.PendingDeleteEntry entry = iter.next();
                if (!entry.isPending()) {
                    FilePos fp = entry.getFilePos();
                    if (oldFileIdSet.contains(fp.fileId())) {
                        toDelete.add(entry.getRowId());
                    }
                }
            }
        }
        // the row may live on in a compacted file: re-queue its mask instead of dropping it
        for (long rowId : toDelete) {
            dvRocksDB.pendingDeletes().putPending(rowId);
        }

        // Remove FileDict entries for old files
        for (int fileId : oldFileIds) {
            dvRocksDB.fileDict().delete(fileId);
        }
        return toDelete;
    }

    /**
     * Returns the pending readable offset cached during Prepare for the given bucket. Returns -1 if
     * no pending offset exists.
     */
    public long getPendingReadableOffset(int bucketId) {
        Long offset = pendingReadableOffsets.get(bucketId);
        return offset != null ? offset : -1;
    }

    /**
     * Gets a DV snapshot for union read. Must be called under DvRWLock read lock. Converts file_id
     * to file_path via FileDict before returning.
     *
     * @param requestedSnapshotId the snapshot ID requested by the client
     * @param logEndOffset the current log end offset from the LogTablet
     * @return a {@link DvSnapshot} containing LakeDv, LogDv, and offset information
     * @throws StaleSnapshotException if the requested snapshot ID does not match the current
     *     readable snapshot
     */
    public DvSnapshot getDvForUnionRead(long requestedSnapshotId, long logEndOffset)
            throws IOException, StaleSnapshotException {
        // Verify snapshot consistency
        if (readableSnapshotId != requestedSnapshotId) {
            throw new StaleSnapshotException(readableSnapshotId, requestedSnapshotId);
        }

        // Snapshot LakeDv: read all entries, convert file_id -> file_path via FileDict
        Map<Integer, Roaring64Bitmap> rawLakeDv = dvRocksDB.lakeDv().getAll();
        Map<String, byte[]> resolvedLakeDv = new HashMap<>();
        for (Map.Entry<Integer, Roaring64Bitmap> entry : rawLakeDv.entrySet()) {
            String filePath = dvRocksDB.fileDict().getFilePath(entry.getKey());
            if (filePath != null) {
                resolvedLakeDv.put(
                        filePath, RoaringBitmapUtils.serializeRoaringBitmap64(entry.getValue()));
            }
        }

        // Snapshot LogDv: range [snapshotStartLogOffset, logEndOffset)
        Roaring64Bitmap logDvBitmap =
                dvRocksDB.logDv().snapshot(snapshotStartLogOffset, logEndOffset);
        byte[] logDvBytes = null;
        if (!logDvBitmap.isEmpty()) {
            logDvBytes = RoaringBitmapUtils.serializeRoaringBitmap64(logDvBitmap);
        }

        return new DvSnapshot(resolvedLakeDv, logDvBytes, logEndOffset, snapshotStartLogOffset);
    }

    /**
     * Returns a serialized LogDv bitmap for the given offset range [fromOffset, toOffset). Used by
     * tiering to filter superseded records.
     *
     * @param fromOffset the start offset (inclusive)
     * @param toOffset the end offset (exclusive)
     * @return the serialized LogDv bitmap bytes, or null if no deleted offsets in range
     */
    @javax.annotation.Nullable
    public byte[] getLogDvSnapshot(long fromOffset, long toOffset) throws IOException {
        Roaring64Bitmap bitmap = dvRocksDB.logDv().snapshot(fromOffset, toOffset);
        if (bitmap.isEmpty()) {
            return null;
        }
        return RoaringBitmapUtils.serializeRoaringBitmap64(bitmap);
    }

    /** Returns the current DV-readable snapshot ID. */
    public long getReadableSnapshotId() {
        return readableSnapshotId;
    }

    /** Returns the log start offset of the current readable snapshot. */
    public long getSnapshotStartLogOffset() {
        return snapshotStartLogOffset;
    }

    /** Returns the DV read-write lock. */
    public DvRWLock getDvRWLock() {
        return dvRWLock;
    }

    @Override
    public void close() {
        // DvRocksDB lifecycle is managed externally by KvTablet
    }

    /** Internal action for batch resolve operations. */
    private static class ResolveAction {
        final long rowId;
        final FilePos filePos;
        final boolean resolved;

        ResolveAction(long rowId, FilePos filePos, boolean resolved) {
            this.rowId = rowId;
            this.filePos = filePos;
            this.resolved = resolved;
        }
    }
}

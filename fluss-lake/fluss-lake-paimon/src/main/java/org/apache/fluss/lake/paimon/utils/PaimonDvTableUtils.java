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

import org.apache.fluss.utils.types.Tuple2;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Utils for Paimon delete-vector enabled table. */
public class PaimonDvTableUtils {

    /**
     * Find the latest snapshot that still holds the L0 files flushed by the given compacted
     * snapshot.
     *
     * <p>The method works by:
     *
     * <ol>
     *   <li>Getting the delta of the compacted snapshot to find deleted L0 files
     *   <li>Grouping deleted L0 files by bucket
     *   <li>Searching backwards through previous snapshots to find the latest one whose L0 files
     *       exactly match the deleted L0 files for each bucket
     * </ol>
     *
     * <p>This snapshot is the most recent snapshot that still contains all the L0 files that were
     * flushed (deleted) by the compacted snapshot.
     *
     * @param fileStoreTable the FileStoreTable instance
     * @param compactedSnapshot the compacted snapshot whose flushed L0 files to search for
     * @return the latest snapshot that still holds these L0 files, or null if not found
     * @throws IOException if an error occurs
     */
    @Nullable
    public static Snapshot findLatestSnapshotExactlyHoldingL0Files(
            FileStoreTable fileStoreTable, Snapshot compactedSnapshot) throws IOException {
        SnapshotManager snapshotManager = fileStoreTable.snapshotManager();
        checkState(compactedSnapshot.commitKind() == Snapshot.CommitKind.COMPACT);
        // Get deleted L0 files from the compacted snapshot's delta
        Map<Tuple2<BinaryRow, Integer>, Set<String>> deletedL0FilesByBucket =
                getDeletedL0FilesByBucket(fileStoreTable, compactedSnapshot);

        if (deletedL0FilesByBucket.isEmpty()) {
            // No L0 files were deleted, can't find a snapshot holding these L0 files,
            // return null directly
            return null;
        }

        // Search backwards from the compacted snapshot to find the latest snapshot
        // that still holds these L0 files
        long earliestSnapshot = checkNotNull(snapshotManager.earliestSnapshotId());
        for (long snapshot = compactedSnapshot.id() - 1; snapshot >= earliestSnapshot; snapshot--) {
            Snapshot candidateSnapshot = snapshotManager.tryGetSnapshot(snapshot);
            if (candidateSnapshot == null) {
                // no such snapshot in paimon, skip
                continue;
            }
            if (matchesDeletedL0Files(fileStoreTable, candidateSnapshot, deletedL0FilesByBucket)) {
                return candidateSnapshot;
            }
        }
        return null;
    }

    /**
     * Get deleted L0 files grouped by bucket from a compacted snapshot's delta.
     *
     * @param compactedSnapshot the compacted snapshot
     * @return a map from bucket ID to set of deleted L0 file names
     */
    private static Map<Tuple2<BinaryRow, Integer>, Set<String>> getDeletedL0FilesByBucket(
            FileStoreTable fileStoreTable, Snapshot compactedSnapshot) {
        Map<Tuple2<BinaryRow, Integer>, Set<String>> deletedL0FilesByBucket = new HashMap<>();
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
                deletedL0FilesByBucket
                        .computeIfAbsent(
                                Tuple2.of(manifestEntry.partition(), manifestEntry.bucket()),
                                k -> new HashSet<>())
                        .add(manifestEntry.fileName());
            }
        }
        return deletedL0FilesByBucket;
    }

    /**
     * Check if a candidate snapshot's L0 files exactly match the deleted L0 files for the relevant
     * buckets.
     *
     * @param candidateSnapshot the candidate snapshot to check
     * @param deletedL0FilesByBucket the deleted L0 files grouped by bucket
     * @return true if the candidate snapshot's L0 files match the deleted L0 files per-bucket
     */
    private static boolean matchesDeletedL0Files(
            FileStoreTable fileStoreTable,
            Snapshot candidateSnapshot,
            Map<Tuple2<BinaryRow, Integer>, Set<String>> deletedL0FilesByBucket) {
        // Get L0 files from the candidate snapshot, grouped by bucket
        Map<Tuple2<BinaryRow, Integer>, Set<String>> candidateL0FilesByBucket =
                getL0FilesByBucket(fileStoreTable, candidateSnapshot);

        for (Map.Entry<Tuple2<BinaryRow, Integer>, Set<String>> deleteL0Entry :
                deletedL0FilesByBucket.entrySet()) {
            Set<String> deleteL0Files = candidateL0FilesByBucket.get(deleteL0Entry.getKey());
            if (deleteL0Files == null || !deleteL0Files.equals(deleteL0Entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get L0 files from a snapshot, grouped by bucket.
     *
     * <p>This method uses the scan API to get all L0 files that exist in the snapshot.
     *
     * @param snapshot the snapshot to get L0 files from
     * @return a map from bucket ID to set of L0 file names that exist in the snapshot
     */
    private static Map<Tuple2<BinaryRow, Integer>, Set<String>> getL0FilesByBucket(
            FileStoreTable fileStoreTable, Snapshot snapshot) {
        Map<Tuple2<BinaryRow, Integer>, Set<String>> l0FilesByBucket = new HashMap<>();

        // Use scan API to get all splits including L0 files
        Map<String, String> scanOptions = new HashMap<>();
        scanOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshot.id()));
        // Set batch scan mode to compact to make sure we can get L0 level files
        scanOptions.put(
                CoreOptions.BATCH_SCAN_MODE.key(), CoreOptions.BatchScanMode.COMPACT.getValue());
        List<Split> splits = fileStoreTable.copy(scanOptions).newScan().plan().splits();

        // Check each split to get L0 files
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            for (DataFileMeta dataFileMeta : dataSplit.dataFiles()) {
                if (dataFileMeta.level() == 0) {
                    l0FilesByBucket
                            .computeIfAbsent(
                                    Tuple2.of(dataSplit.partition(), dataSplit.bucket()),
                                    k -> new HashSet<>())
                            .add(dataFileMeta.fileName());
                }
            }
        }

        return l0FilesByBucket;
    }
}

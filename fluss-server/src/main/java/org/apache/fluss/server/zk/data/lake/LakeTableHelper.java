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

package org.apache.fluss.server.zk.data.lake;

import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.json.TableBucketOffsets;
import org.apache.fluss.utils.types.Tuple2;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.metrics.registry.MetricRegistry.LOG;

/** The helper to handle {@link LakeTable}. */
public class LakeTableHelper {

    private final ZooKeeperClient zkClient;
    private final String remoteDataDir;

    public LakeTableHelper(ZooKeeperClient zkClient, String remoteDataDir) {
        this.zkClient = zkClient;
        this.remoteDataDir = remoteDataDir;
    }

    /**
     * Upserts a lake table snapshot for the given table, stored in v1 format. Note: this method is
     * just for back compatibility.
     *
     * @param tableId the table ID
     * @param lakeTableSnapshot the new snapshot to upsert
     * @throws Exception if the operation fails
     */
    public void registerLakeTableSnapshotV1(long tableId, LakeTableSnapshot lakeTableSnapshot)
            throws Exception {
        Optional<LakeTable> optPreviousLakeTable = zkClient.getLakeTable(tableId);
        // Merge with previous snapshot if exists
        if (optPreviousLakeTable.isPresent()) {
            TableBucketOffsets tableBucketOffsets =
                    mergeTableBucketOffsets(
                            optPreviousLakeTable.get(),
                            new TableBucketOffsets(
                                    tableId, lakeTableSnapshot.getBucketLogEndOffset()));
            lakeTableSnapshot = new LakeTableSnapshot(tableId, tableBucketOffsets.getOffsets());
        }
        zkClient.upsertLakeTable(
                tableId, new LakeTable(lakeTableSnapshot), optPreviousLakeTable.isPresent());
    }

    public void registerLakeTableSnapshotV2(
            long tableId, LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata) throws Exception {
        registerLakeTableSnapshotV2(tableId, lakeSnapshotMetadata, null);
    }

    /**
     * Register a lake table snapshot and clean up old snapshots based on the table type.
     *
     * @param tableId the table ID
     * @param lakeSnapshotMetadata the new snapshot metadata to register
     * @param earliestSnapshotIDToKeep the earliest snapshot ID to keep. If null, only the latest
     *     snapshot will be kept. If -1, all snapshots are kept (infinite retention); no previous
     *     snapshots are discarded.
     * @throws Exception if the operation fails
     */
    public void registerLakeTableSnapshotV2(
            long tableId,
            LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata,
            @Nullable Long earliestSnapshotIDToKeep)
            throws Exception {
        Optional<LakeTable> optPreviousTable = zkClient.getLakeTable(tableId);
        List<LakeTable.LakeSnapshotMetadata> previousMetadatas =
                optPreviousTable
                        .map(LakeTable::getLakeSnapshotMetadatas)
                        .orElse(Collections.emptyList());

        // Determine which snapshots to keep and which to discard (but don't discard yet)

        Tuple2<List<LakeTable.LakeSnapshotMetadata>, List<LakeTable.LakeSnapshotMetadata>> result =
                determineSnapshotsToKeepAndDiscard(
                        previousMetadatas, lakeSnapshotMetadata, earliestSnapshotIDToKeep);

        List<LakeTable.LakeSnapshotMetadata> keptSnapshots = result.f0;
        List<LakeTable.LakeSnapshotMetadata> snapshotsToDiscard = result.f1;

        LakeTable lakeTable = new LakeTable(keptSnapshots);
        try {
            // First, upsert to ZK. Only after success, we discard old snapshots.
            zkClient.upsertLakeTable(tableId, lakeTable, optPreviousTable.isPresent());
        } catch (Exception e) {
            LOG.warn("Failed to upsert lake table snapshot to zk.", e);
            throw e;
        }

        // After successful upsert, discard snapshots
        for (LakeTable.LakeSnapshotMetadata metadata : snapshotsToDiscard) {
            metadata.discard();
        }
    }

    /**
     * Determines which snapshots should be retained or discarded based on the timeline according to
     * {@code earliestSnapshotIDToKeep}.
     */
    private Tuple2<List<LakeTable.LakeSnapshotMetadata>, List<LakeTable.LakeSnapshotMetadata>>
            determineSnapshotsToKeepAndDiscard(
                    List<LakeTable.LakeSnapshotMetadata> previousMetadatas,
                    LakeTable.LakeSnapshotMetadata newSnapshotMetadata,
                    @Nullable Long earliestSnapshotIDToKeep) {
        // Scenario 1: No retention boundary or no history -> Keep only latest
        if (earliestSnapshotIDToKeep == null || previousMetadatas.isEmpty()) {
            return new Tuple2<>(
                    Collections.singletonList(newSnapshotMetadata),
                    new ArrayList<>(previousMetadatas));
        }

        // Scenario 2: Find the split point based on position (
        // not compare snapshot id directly for non-monotonic IDs, like iceberg)
        int splitIndex = -1;
        for (int i = 0; i < previousMetadatas.size(); i++) {
            if (previousMetadatas.get(i).getSnapshotId() == earliestSnapshotIDToKeep) {
                splitIndex = i;
                break;
            }
        }

        // If ID not found, play safe: keep everything
        if (splitIndex == -1) {
            List<LakeTable.LakeSnapshotMetadata> kept = new ArrayList<>(previousMetadatas);
            kept.add(newSnapshotMetadata);
            return new Tuple2<>(kept, Collections.emptyList());
        }

        List<LakeTable.LakeSnapshotMetadata> toDiscard =
                new ArrayList<>(previousMetadatas.subList(0, splitIndex));
        List<LakeTable.LakeSnapshotMetadata> toKeep =
                new ArrayList<>(previousMetadatas.subList(splitIndex, previousMetadatas.size()));
        toKeep.add(newSnapshotMetadata);
        return new Tuple2<>(toKeep, toDiscard);
    }

    public TableBucketOffsets mergeTableBucketOffsets(
            LakeTable previousLakeTable, TableBucketOffsets newTableBucketOffsets)
            throws Exception {
        // Merge current  with previous one since the current request
        // may not carry all buckets for the table. It typically only carries buckets
        // that were written after the previous commit.

        // merge log end offsets, current will override the previous
        Map<TableBucket, Long> bucketLogEndOffset =
                new HashMap<>(
                        previousLakeTable.getOrReadLatestTableSnapshot().getBucketLogEndOffset());
        bucketLogEndOffset.putAll(newTableBucketOffsets.getOffsets());
        return new TableBucketOffsets(newTableBucketOffsets.getTableId(), bucketLogEndOffset);
    }

    public FsPath storeLakeTableOffsetsFile(
            TablePath tablePath, TableBucketOffsets tableBucketOffsets) throws Exception {
        // get the remote file path to store the lake table snapshot offset information
        long tableId = tableBucketOffsets.getTableId();
        FsPath remoteLakeTableSnapshotOffsetPath =
                FlussPaths.remoteLakeTableSnapshotOffsetPath(remoteDataDir, tablePath, tableId);
        // check whether the parent directory exists, if not, create the directory
        FileSystem fileSystem = remoteLakeTableSnapshotOffsetPath.getFileSystem();
        if (!fileSystem.exists(remoteLakeTableSnapshotOffsetPath.getParent())) {
            fileSystem.mkdirs(remoteLakeTableSnapshotOffsetPath.getParent());
        }
        // serialize table offsets to json bytes, and write to file
        byte[] jsonBytes = tableBucketOffsets.toJsonBytes();

        try (FSDataOutputStream outputStream =
                fileSystem.create(
                        remoteLakeTableSnapshotOffsetPath, FileSystem.WriteMode.OVERWRITE)) {
            outputStream.write(jsonBytes);
        }
        return remoteLakeTableSnapshotOffsetPath;
    }
}

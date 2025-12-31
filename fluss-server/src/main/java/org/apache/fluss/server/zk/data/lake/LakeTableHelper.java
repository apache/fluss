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
        Optional<LakeTable> optPreviousLakeTable = zkClient.getLakeTable(tableId);
        List<LakeTable.LakeSnapshotMetadata> previousLakeSnapshotMetadatas = null;
        if (optPreviousLakeTable.isPresent()) {
            previousLakeSnapshotMetadatas = optPreviousLakeTable.get().getLakeSnapshotMetadatas();
        }
        LakeTable lakeTable = new LakeTable(lakeSnapshotMetadata);
        try {
            zkClient.upsertLakeTable(tableId, lakeTable, optPreviousLakeTable.isPresent());
        } catch (Exception e) {
            LOG.warn("Failed to upsert lake table snapshot to zk.", e);
            throw e;
        }

        // currently, we keep only one lake snapshot metadata in zk,
        // todo: in solve paimon dv union read issue #2121, we'll keep multiple lake snapshot
        // metadata
        // discard previous lake snapshot metadata
        if (previousLakeSnapshotMetadatas != null) {
            previousLakeSnapshotMetadatas.forEach(LakeTable.LakeSnapshotMetadata::discard);
        }
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

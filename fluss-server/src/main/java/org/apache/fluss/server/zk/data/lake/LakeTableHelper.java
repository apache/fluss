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

import java.io.IOException;
import java.util.HashMap;
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
     * Upserts a lake table snapshot for the given table.
     *
     * <p>This method merges the new snapshot with the existing one (if any) and stores it (data in
     * remote file, the remote file path in ZK).
     *
     * @param tableId the table ID
     * @param tablePath the table path
     * @param lakeTableSnapshot the new snapshot to upsert
     * @throws Exception if the operation fails
     */
    public void upsertLakeTable(
            long tableId, TablePath tablePath, LakeTableSnapshot lakeTableSnapshot)
            throws Exception {
        Optional<LakeTable> optPreviousLakeTable = zkClient.getLakeTable(tableId);
        // Merge with previous snapshot if exists
        if (optPreviousLakeTable.isPresent()) {
            lakeTableSnapshot =
                    mergeLakeTable(
                            optPreviousLakeTable.get().toLakeTableSnapshot(), lakeTableSnapshot);
        }

        FsPath lakeTableSnapshotFsPath =
                storeLakeTableSnapshot(tableId, tablePath, lakeTableSnapshot);
        LakeTable lakeTable = new LakeTable(lakeTableSnapshotFsPath);
        try {
            zkClient.upsertLakeTable(tableId, lakeTable, optPreviousLakeTable.isPresent());
        } catch (Exception e) {
            LOG.warn("Failed to upsert lake table snapshot to zk.", e);
            // delete the new lake table snapshot file
            deleteFile(lakeTableSnapshotFsPath);
            throw e;
        }

        if (optPreviousLakeTable.isPresent()) {
            FsPath previousLakeSnapshotFsPath =
                    optPreviousLakeTable.get().getLakeTableLatestSnapshotFileHandle();
            if (previousLakeSnapshotFsPath != null) {
                deleteFile(previousLakeSnapshotFsPath);
            }
        }
    }

    private LakeTableSnapshot mergeLakeTable(
            LakeTableSnapshot previousLakeTableSnapshot, LakeTableSnapshot newLakeTableSnapshot) {
        // Merge current snapshot with previous one since the current snapshot request
        // may not carry all buckets for the table. It typically only carries buckets
        // that were written after the previous commit.

        // merge log startup offset, current will override the previous
        Map<TableBucket, Long> bucketLogStartOffset =
                new HashMap<>(previousLakeTableSnapshot.getBucketLogStartOffset());
        bucketLogStartOffset.putAll(newLakeTableSnapshot.getBucketLogStartOffset());

        // merge log end offsets, current will override the previous
        Map<TableBucket, Long> bucketLogEndOffset =
                new HashMap<>(previousLakeTableSnapshot.getBucketLogEndOffset());
        bucketLogEndOffset.putAll(newLakeTableSnapshot.getBucketLogEndOffset());

        // merge max timestamp, current will override the previous
        Map<TableBucket, Long> bucketMaxTimestamp =
                new HashMap<>(previousLakeTableSnapshot.getBucketMaxTimestamp());
        bucketMaxTimestamp.putAll(newLakeTableSnapshot.getBucketMaxTimestamp());

        Map<Long, String> partitionNameById =
                new HashMap<>(previousLakeTableSnapshot.getPartitionNameIdByPartitionId());
        partitionNameById.putAll(newLakeTableSnapshot.getPartitionNameIdByPartitionId());

        return new LakeTableSnapshot(
                newLakeTableSnapshot.getSnapshotId(),
                newLakeTableSnapshot.getTableId(),
                bucketLogStartOffset,
                bucketLogEndOffset,
                bucketMaxTimestamp,
                partitionNameById);
    }

    private FsPath storeLakeTableSnapshot(
            long tableId, TablePath tablePath, LakeTableSnapshot lakeTableSnapshot)
            throws Exception {
        // get the remote file path to store the lake table snapshot information
        FsPath remoteLakeTableSnapshotPath =
                FlussPaths.remoteLakeTableSnapshotPath(
                        remoteDataDir, tablePath, tableId, lakeTableSnapshot.getSnapshotId());
        // check whether the parent directory exists, if not, create the directory
        FileSystem fileSystem = remoteLakeTableSnapshotPath.getFileSystem();
        if (!fileSystem.exists(remoteLakeTableSnapshotPath.getParent())) {
            fileSystem.mkdirs(remoteLakeTableSnapshotPath.getParent());
        }
        // serialize table snapshot to json bytes, and write to file
        byte[] jsonBytes = LakeTableSnapshotJsonSerde.toJson(lakeTableSnapshot);
        try (FSDataOutputStream outputStream =
                fileSystem.create(remoteLakeTableSnapshotPath, FileSystem.WriteMode.OVERWRITE)) {
            outputStream.write(jsonBytes);
        }
        return remoteLakeTableSnapshotPath;
    }

    private void deleteFile(FsPath filePath) {
        try {
            FileSystem fileSystem = filePath.getFileSystem();
            if (fileSystem.exists(filePath)) {
                fileSystem.delete(filePath, false);
            }
        } catch (IOException e) {
            LOG.warn("Error deleting filePath at {}", filePath, e);
        }
    }
}

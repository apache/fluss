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
                            optPreviousLakeTable.get().getLatestTableSnapshot(), lakeTableSnapshot);
        }

        // store the lake table snapshot into a file
        FsPath lakeTableSnapshotFsPath =
                storeLakeTableSnapshot(tableId, tablePath, lakeTableSnapshot);

        LakeTable.LakeSnapshotMetadata lakeSnapshotMetadata =
                new LakeTable.LakeSnapshotMetadata(
                        lakeTableSnapshot.getSnapshotId(),
                        // use the lake table snapshot file as the tiered offsets file since
                        // the table snapshot file will contain the tiered log end offsets
                        lakeTableSnapshotFsPath,
                        // currently, readableOffsetsFilePath is always same with
                        // tieredOffsetsFilePath, but in the future we'll commit a readable offsets
                        // separately to mark what the readable offsets are for a snapshot since
                        // in paimon dv table, tiered log end offsets is not same with readable
                        // offsets
                        lakeTableSnapshotFsPath);

        // currently, we keep only one lake snapshot metadata in zk,
        // todo: in solve paimon dv union read issue #2121, we'll keep multiple lake snapshot
        // metadata
        LakeTable lakeTable = new LakeTable(lakeSnapshotMetadata);
        try {
            zkClient.upsertLakeTable(tableId, lakeTable, optPreviousLakeTable.isPresent());
        } catch (Exception e) {
            LOG.warn("Failed to upsert lake table snapshot to zk.", e);
            // discard the new lake snapshot metadata
            lakeSnapshotMetadata.discard();
            throw e;
        }

        if (optPreviousLakeTable.isPresent()) {
            // discard previous latest lake snapshot
            LakeTable.LakeSnapshotMetadata previousLakeSnapshotMetadata =
                    optPreviousLakeTable.get().getLatestLakeSnapshotMetadata();
            if (previousLakeSnapshotMetadata != null) {
                previousLakeSnapshotMetadata.discard();
            }
        }
    }

    private LakeTableSnapshot mergeLakeTable(
            LakeTableSnapshot previousLakeTableSnapshot, LakeTableSnapshot newLakeTableSnapshot) {
        // Merge current snapshot with previous one since the current snapshot request
        // may not carry all buckets for the table. It typically only carries buckets
        // that were written after the previous commit.

        // merge log end offsets, current will override the previous
        Map<TableBucket, Long> bucketLogEndOffset =
                new HashMap<>(previousLakeTableSnapshot.getBucketLogEndOffset());
        bucketLogEndOffset.putAll(newLakeTableSnapshot.getBucketLogEndOffset());

        return new LakeTableSnapshot(newLakeTableSnapshot.getSnapshotId(), bucketLogEndOffset);
    }

    private FsPath storeLakeTableSnapshot(
            long tableId, TablePath tablePath, LakeTableSnapshot lakeTableSnapshot)
            throws Exception {
        // get the remote file path to store the lake table snapshot information
        FsPath remoteLakeTableSnapshotManifestPath =
                FlussPaths.remoteLakeTableSnapshotManifestPath(remoteDataDir, tablePath, tableId);
        // check whether the parent directory exists, if not, create the directory
        FileSystem fileSystem = remoteLakeTableSnapshotManifestPath.getFileSystem();
        if (!fileSystem.exists(remoteLakeTableSnapshotManifestPath.getParent())) {
            fileSystem.mkdirs(remoteLakeTableSnapshotManifestPath.getParent());
        }
        // serialize table snapshot to json bytes, and write to file
        byte[] jsonBytes = LakeTableSnapshotJsonSerde.toJson(lakeTableSnapshot);
        try (FSDataOutputStream outputStream =
                fileSystem.create(
                        remoteLakeTableSnapshotManifestPath, FileSystem.WriteMode.OVERWRITE)) {
            outputStream.write(jsonBytes);
        }
        return remoteLakeTableSnapshotManifestPath;
    }
}

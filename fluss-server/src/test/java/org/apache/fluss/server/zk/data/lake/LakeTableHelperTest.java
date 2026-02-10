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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.utils.json.TableBucketOffsets;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** The UT for {@link LakeTableHelper}. */
class LakeTableHelperTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() {
        zookeeperClient.close();
    }

    @Test
    void testRegisterLakeTableSnapshotCompatibility(@TempDir Path tempDir) throws Exception {
        // Create a ZooKeeperClient with REMOTE_DATA_DIR configuration
        Configuration conf = new Configuration();
        conf.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        LakeTableHelper lakeTableHelper = new LakeTableHelper(zookeeperClient, tempDir.toString());
        try (ZooKeeperClient zooKeeperClient =
                ZooKeeperUtils.startZookeeperClient(conf, NOPErrorHandler.INSTANCE)) {
            // first create a table
            long tableId = 1;
            TablePath tablePath = TablePath.of("test_db", "test_table");
            TableRegistration tableReg = createTableReg(tableId);
            zookeeperClient.registerTable(tablePath, tableReg);

            // Create a legacy version 1 LakeTableSnapshot (full data in ZK)
            long snapshotId = 1L;

            Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();
            bucketLogEndOffset.put(new TableBucket(tableId, 0), 100L);
            bucketLogEndOffset.put(new TableBucket(tableId, 1), 200L);

            LakeTableSnapshot lakeTableSnapshot =
                    new LakeTableSnapshot(snapshotId, bucketLogEndOffset);
            // Write version 1 format data(simulating old system behavior)
            lakeTableHelper.registerLakeTableSnapshotV1(tableId, lakeTableSnapshot);

            // Verify version 1 data can be read
            Optional<LakeTable> optionalLakeTable = zooKeeperClient.getLakeTable(tableId);
            assertThat(optionalLakeTable).isPresent();
            LakeTable lakeTable = optionalLakeTable.get();
            assertThat(lakeTable.getOrReadLatestTableSnapshot()).isEqualTo(lakeTableSnapshot);

            // Test: Call upsertLakeTableSnapshot with new snapshot data
            // This should read the old version 1 data, merge it, and write as version 2
            Map<TableBucket, Long> newBucketLogEndOffset = new HashMap<>();
            newBucketLogEndOffset.put(new TableBucket(tableId, 0), 1500L); // Updated offset
            newBucketLogEndOffset.put(new TableBucket(tableId, 1), 2000L); // new offset

            long snapshot2Id = 2L;
            FsPath tieredOffsetsPath =
                    lakeTableHelper.storeLakeTableOffsetsFile(
                            tablePath, new TableBucketOffsets(tableId, newBucketLogEndOffset));
            lakeTableHelper.registerLakeTableSnapshotV2(
                    tableId,
                    new LakeTable.LakeSnapshotMetadata(
                            snapshot2Id, tieredOffsetsPath, tieredOffsetsPath));

            // Verify: New version 2 data can be read
            Optional<LakeTable> optLakeTableAfter = zooKeeperClient.getLakeTable(tableId);
            assertThat(optLakeTableAfter).isPresent();
            LakeTable lakeTableAfter = optLakeTableAfter.get();
            assertThat(lakeTableAfter.getLatestLakeSnapshotMetadata())
                    .isNotNull(); // Version 2 has file path

            // Verify: The lake snapshot file exists
            FsPath snapshot2FileHandle =
                    lakeTableAfter.getLatestLakeSnapshotMetadata().getReadableOffsetsFilePath();
            FileSystem fileSystem = snapshot2FileHandle.getFileSystem();
            assertThat(fileSystem.exists(snapshot2FileHandle)).isTrue();

            Optional<LakeTableSnapshot> optMergedSnapshot =
                    zooKeeperClient.getLakeTableSnapshot(tableId, null);
            assertThat(optMergedSnapshot).isPresent();
            LakeTableSnapshot mergedSnapshot = optMergedSnapshot.get();

            // verify the snapshot should merge previous snapshot
            assertThat(mergedSnapshot.getSnapshotId()).isEqualTo(snapshot2Id);
            Map<TableBucket, Long> expectedBucketLogEndOffset = new HashMap<>(bucketLogEndOffset);
            expectedBucketLogEndOffset.putAll(newBucketLogEndOffset);
            assertThat(mergedSnapshot.getBucketLogEndOffset())
                    .isEqualTo(expectedBucketLogEndOffset);

            // add a new snapshot 3 again, verify snapshot
            long snapshot3Id = 3L;
            tieredOffsetsPath =
                    lakeTableHelper.storeLakeTableOffsetsFile(
                            tablePath, new TableBucketOffsets(tableId, newBucketLogEndOffset));
            lakeTableHelper.registerLakeTableSnapshotV2(
                    tableId,
                    new LakeTable.LakeSnapshotMetadata(
                            snapshot3Id, tieredOffsetsPath, tieredOffsetsPath));
            // verify snapshot 3 is discarded
            assertThat(fileSystem.exists(snapshot2FileHandle)).isFalse();
        }
    }

    @Test
    void testRegisterLakeTableSnapshotWithRetention(@TempDir Path tempDir) throws Exception {
        LakeTableHelper lakeTableHelper = new LakeTableHelper(zookeeperClient, tempDir.toString());
        long tableId = 1L;
        TablePath tablePath = TablePath.of("test_db", "retention_test");

        // --- Setup: Register table and initialize filesystem ---
        zookeeperClient.registerTable(tablePath, createTableReg(tableId));
        FileSystem fs = LocalFileSystem.getSharedInstance();

        // 1. Create Snapshot 1 (ID=1)
        FsPath path1 = storeOffsetFile(lakeTableHelper, tablePath, tableId, 100L);
        LakeTable.LakeSnapshotMetadata meta1 = new LakeTable.LakeSnapshotMetadata(1L, path1, path1);
        lakeTableHelper.registerLakeTableSnapshotV2(
                tableId, meta1, LakeCommitResult.KEEP_ALL_PREVIOUS);

        // 2. Create Snapshot 2 (ID=2)
        FsPath path2 = storeOffsetFile(lakeTableHelper, tablePath, tableId, 200L);
        LakeTable.LakeSnapshotMetadata meta2 = new LakeTable.LakeSnapshotMetadata(2L, path2, path2);
        lakeTableHelper.registerLakeTableSnapshotV2(
                tableId, meta2, LakeCommitResult.KEEP_ALL_PREVIOUS);

        List<LakeTable.LakeSnapshotMetadata> metadatasAfterStep2 =
                zookeeperClient.getLakeTable(tableId).get().getLakeSnapshotMetadatas();
        assertThat(metadatasAfterStep2).hasSize(2);
        assertThat(metadatasAfterStep2)
                .extracting(LakeTable.LakeSnapshotMetadata::getSnapshotId)
                .containsExactly(1L, 2L);

        // --- Scenario A: earliestSnapshotIDToKeep = KEEP_LATEST (Aggressive Cleanup) ---
        // Expected behavior: Only the latest snapshot is retained; all previous ones are discarded.
        FsPath path3 = storeOffsetFile(lakeTableHelper, tablePath, tableId, 300L);
        LakeTable.LakeSnapshotMetadata meta3 = new LakeTable.LakeSnapshotMetadata(3L, path3, path3);

        lakeTableHelper.registerLakeTableSnapshotV2(tableId, meta3, LakeCommitResult.KEEP_LATEST);

        // Verify physical files: 1 and 2 should be deleted, 3 must exist.
        assertThat(fs.exists(path1)).isFalse();
        assertThat(fs.exists(path2)).isFalse();
        assertThat(fs.exists(path3)).isTrue();
        // Verify metadata: Only 1 entry should remain in ZK.
        assertThat(zookeeperClient.getLakeTable(tableId).get().getLakeSnapshotMetadatas())
                .hasSize(1);

        // --- Scenario B: earliestSnapshotIDToKeep = KEEP_ALL_PREVIOUS (Infinite Retention) ---
        // Expected behavior: No previous snapshots are discarded regardless of history size.
        FsPath path4 = storeOffsetFile(lakeTableHelper, tablePath, tableId, 400L);
        LakeTable.LakeSnapshotMetadata meta4 = new LakeTable.LakeSnapshotMetadata(4L, path4, path4);

        lakeTableHelper.registerLakeTableSnapshotV2(
                tableId, meta4, LakeCommitResult.KEEP_ALL_PREVIOUS);

        // Verify both snapshots 3 and 4 are preserved.
        assertThat(fs.exists(path3)).isTrue();
        assertThat(fs.exists(path4)).isTrue();
        assertThat(zookeeperClient.getLakeTable(tableId).get().getLakeSnapshotMetadatas())
                .hasSize(2);

        // --- Scenario C: earliestSnapshotIDToKeep = Specific ID (Positional Slicing) ---
        // Setup: Current history [3, 4], adding [5, 6].
        FsPath path5 = storeOffsetFile(lakeTableHelper, tablePath, tableId, 500L);
        LakeTable.LakeSnapshotMetadata meta5 = new LakeTable.LakeSnapshotMetadata(5L, path5, path5);
        lakeTableHelper.registerLakeTableSnapshotV2(
                tableId, meta5, LakeCommitResult.KEEP_ALL_PREVIOUS);

        FsPath path6 = storeOffsetFile(lakeTableHelper, tablePath, tableId, 600L);
        LakeTable.LakeSnapshotMetadata meta6 = new LakeTable.LakeSnapshotMetadata(6L, path6, path6);

        // Action: Set retention boundary to Snapshot 4.
        // Expected behavior: Snapshot 3 (before the boundary) is discarded; 4, 5, and 6 are kept.
        lakeTableHelper.registerLakeTableSnapshotV2(tableId, meta6, 4L);

        // Verify physical cleanup.
        assertThat(fs.exists(path3)).isFalse();
        assertThat(fs.exists(path4)).isTrue();
        assertThat(fs.exists(path5)).isTrue();
        assertThat(fs.exists(path6)).isTrue();

        // Verify metadata sequence in Zookeeper.
        assertThat(zookeeperClient.getLakeTable(tableId).get().getLakeSnapshotMetadatas())
                .extracting(LakeTable.LakeSnapshotMetadata::getSnapshotId)
                .containsExactly(4L, 5L, 6L);
    }

    /** Helper to store offset files and return the FsPath. */
    private FsPath storeOffsetFile(
            LakeTableHelper helper, TablePath path, long tableId, long offset) throws Exception {
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(tableId, 0), offset);
        return helper.storeLakeTableOffsetsFile(path, new TableBucketOffsets(tableId, offsets));
    }

    /** Helper to create a basic TableRegistration. */
    private TableRegistration createTableReg(long tableId) {
        return new TableRegistration(
                tableId,
                "test",
                Collections.emptyList(),
                new TableDescriptor.TableDistribution(1, Collections.singletonList("a")),
                Collections.emptyMap(),
                Collections.emptyMap(),
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }
}

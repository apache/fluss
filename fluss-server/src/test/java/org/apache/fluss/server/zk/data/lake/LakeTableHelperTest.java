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
            TableRegistration tableReg =
                    new TableRegistration(
                            tableId,
                            "test table",
                            Collections.emptyList(),
                            new TableDescriptor.TableDistribution(
                                    1, Collections.singletonList("a")),
                            Collections.emptyMap(),
                            Collections.emptyMap(),
                            System.currentTimeMillis(),
                            System.currentTimeMillis());
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
                    zooKeeperClient.getLakeTableSnapshot(tableId);
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
}

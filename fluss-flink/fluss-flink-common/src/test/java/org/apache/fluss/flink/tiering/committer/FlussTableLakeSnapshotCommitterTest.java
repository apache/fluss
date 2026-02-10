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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshotLegacyJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlussTableLakeSnapshotCommitter}. */
class FlussTableLakeSnapshotCommitterTest extends FlinkTestBase {

    private FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;

    @BeforeEach
    public void beforeEach() {
        flussTableLakeSnapshotCommitter =
                new FlussTableLakeSnapshotCommitter(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        flussTableLakeSnapshotCommitter.open();
    }

    @AfterEach
    void afterEach() throws Exception {
        if (flussTableLakeSnapshotCommitter != null) {
            flussTableLakeSnapshotCommitter.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommit(boolean isPartitioned) throws Exception {
        TablePath tablePath =
                TablePath.of("fluss", "test_commit" + (isPartitioned ? "_partitioned" : ""));
        Tuple2<Long, Collection<Long>> tableIdAndPartitions = createTable(tablePath, isPartitioned);
        long tableId = tableIdAndPartitions.f0;
        Collection<Long> partitions = tableIdAndPartitions.f1;

        Map<TableBucket, Long> expectedOffsets = mockLogEndOffsets(tableId, partitions);

        long snapshotId1 = 1;

        String lakeSnapshotFilePath1 =
                flussTableLakeSnapshotCommitter.prepareLakeSnapshot(
                        tableId, tablePath, expectedOffsets);

        // commit offsets
        flussTableLakeSnapshotCommitter.commit(
                tableId,
                snapshotId1,
                lakeSnapshotFilePath1,
                // don't care readable snapshot and offsets,
                null,
                // don't care end offsets, maxTieredTimestamps
                Collections.emptyMap(),
                Collections.emptyMap(),
                // test null which only keep one snapshot
                null);
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(snapshotId1);
        // get and check the offsets
        Map<TableBucket, Long> bucketLogOffsets = lakeSnapshot.getTableBucketsOffset();
        assertThat(bucketLogOffsets).isEqualTo(expectedOffsets);

        // verify no any readable lake snapshot
        assertThatThrownBy(() -> admin.getReadableLakeSnapshot(tablePath).get())
                .rootCause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class)
                .hasMessageContaining(
                        "Lake table readable snapshot doesn't exist for table: fluss.test_commit");

        // commit with readable snapshot
        long snapshotId2 = 2;
        String lakeSnapshotFilePath2 =
                flussTableLakeSnapshotCommitter.prepareLakeSnapshot(
                        tableId, tablePath, expectedOffsets);
        flussTableLakeSnapshotCommitter.commit(
                tableId,
                snapshotId2,
                lakeSnapshotFilePath2,
                // make readable file path is same to lakeSnapshotFilePath
                lakeSnapshotFilePath2,
                // don't care end offsets, maxTieredTimestamps
                Collections.emptyMap(),
                Collections.emptyMap(),
                LakeCommitResult.KEEP_ALL_PREVIOUS);

        // test get readable snapshot
        LakeSnapshot readLakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(readLakeSnapshot.getSnapshotId()).isEqualTo(snapshotId2);
        assertThat(readLakeSnapshot.getTableBucketsOffset()).isEqualTo(bucketLogOffsets);

        // verify we can still get snapshot id 1
        LakeSnapshot lakeSnapshot1 = admin.getLakeSnapshot(tablePath, snapshotId1).get();
        assertThat(lakeSnapshot1.getSnapshotId()).isEqualTo(snapshotId1);
        assertThat(lakeSnapshot1.getTableBucketsOffset()).isEqualTo(bucketLogOffsets);

        // commit with readable snapshot again, but update earliest to keep
        long snapshotId3 = 3L;
        String lakeSnapshotFilePath3 =
                flussTableLakeSnapshotCommitter.prepareLakeSnapshot(
                        tableId, tablePath, expectedOffsets);
        flussTableLakeSnapshotCommitter.commit(
                tableId,
                snapshotId3,
                lakeSnapshotFilePath3,
                // make readable file path null
                null,
                // don't care end offsets, maxTieredTimestamps
                Collections.emptyMap(),
                Collections.emptyMap(),
                snapshotId2);

        // now, verify we can't get snapshot 1
        assertThatThrownBy(() -> admin.getLakeSnapshot(tablePath, snapshotId1).get())
                .rootCause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class)
                .hasMessageContaining(
                        "Lake table snapshot doesn't exist for table: fluss.test_commit")
                .hasMessageContaining(", snapshot id: 1");

        // verify latest snapshot
        LakeSnapshot latestLakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(latestLakeSnapshot.getSnapshotId()).isEqualTo(snapshotId3);

        // verify readable snapshot
        readLakeSnapshot = admin.getReadableLakeSnapshot(tablePath).get();
        assertThat(readLakeSnapshot.getSnapshotId()).isEqualTo(snapshotId2);
        assertThat(readLakeSnapshot.getTableBucketsOffset()).isEqualTo(bucketLogOffsets);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCompatibilityWithOldCommitter(boolean isPartitioned) throws Exception {
        // test commit lake snapshot with old behavior
        TablePath tablePath =
                TablePath.of(
                        "fluss",
                        "test_legacy_version_commit" + (isPartitioned ? "_partitioned" : ""));
        Tuple2<Long, Collection<Long>> tableIdAndPartitions = createTable(tablePath, isPartitioned);
        long tableId = tableIdAndPartitions.f0;
        Collection<Long> partitions = tableIdAndPartitions.f1;

        Map<TableBucket, Long> logEndOffsets = mockLogEndOffsets(tableId, partitions);
        long snapshotId = 3;

        // mock old behavior to commit
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                new CommitLakeTableSnapshotRequest();
        commitLakeTableSnapshotRequest =
                flussTableLakeSnapshotCommitter.addLogEndOffsets(
                        commitLakeTableSnapshotRequest,
                        tableId,
                        snapshotId,
                        logEndOffsets,
                        Collections.emptyMap());
        flussTableLakeSnapshotCommitter
                .getCoordinatorGateway()
                .commitLakeTableSnapshot(commitLakeTableSnapshotRequest)
                .get();

        // make sure it can be deserialized with v1
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        byte[] jsonBytes = zkClient.getOrEmpty(ZkData.LakeTableZNode.path(tableId)).get();

        LakeTableSnapshot lakeTableSnapshot =
                JsonSerdeUtils.readValue(jsonBytes, LakeTableSnapshotLegacyJsonSerde.INSTANCE);
        assertThat(lakeTableSnapshot.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(lakeTableSnapshot.getBucketLogEndOffset()).isEqualTo(logEndOffsets);
    }

    private Map<TableBucket, Long> mockLogEndOffsets(long tableId, Collection<Long> partitionsIds) {
        Map<TableBucket, Long> logEndOffsets = new HashMap<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            long bucketOffset = bucket * bucket;
            for (Long partitionId : partitionsIds) {
                if (partitionId == null) {
                    logEndOffsets.put(new TableBucket(tableId, bucket), bucketOffset);
                } else {
                    logEndOffsets.put(new TableBucket(tableId, partitionId, bucket), bucketOffset);
                }
            }
        }
        return logEndOffsets;
    }

    private Tuple2<Long, Collection<Long>> createTable(TablePath tablePath, boolean isPartitioned)
            throws Exception {
        long tableId =
                createTable(
                        tablePath,
                        isPartitioned
                                ? DATA1_PARTITIONED_TABLE_DESCRIPTOR
                                : DATA1_TABLE_DESCRIPTOR);
        Collection<Long> partitions;
        if (!isPartitioned) {
            partitions = Collections.singletonList(null);
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        } else {
            partitions = FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath).values();
        }
        return new Tuple2<>(tableId, partitions);
    }
}

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
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshotJsonSerde;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.flink.tiering.committer.FlussTableLakeSnapshotCommitter.toCommitLakeTableSnapshotRequest;
import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlussTableLakeSnapshotCommitter}. */
class FlussTableLakeSnapshotCommitterTest extends FlinkTestBase {

    private FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;

    @BeforeEach
    void beforeEach() {
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

        Map<TableBucket, Long> logEndOffsets = mockLogEndOffsets(tableId, partitions);

        long snapshotId = 3;
        // commit offsets
        flussTableLakeSnapshotCommitter.commit(tableId, snapshotId, logEndOffsets);
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(3);

        // get and check the offsets
        Map<TableBucket, Long> bucketLogOffsets = lakeSnapshot.getTableBucketsOffset();
        assertThat(bucketLogOffsets).isEqualTo(logEndOffsets);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCompatibilityWithoutSerializationVersion(boolean isPartitioned) throws Exception {
        TablePath tablePath =
                TablePath.of(
                        "fluss",
                        "test_legacy_version_commit" + (isPartitioned ? "_partitioned" : ""));
        Tuple2<Long, Collection<Long>> tableIdAndPartitions = createTable(tablePath, isPartitioned);
        long tableId = tableIdAndPartitions.f0;
        Collection<Long> partitions = tableIdAndPartitions.f1;

        Map<TableBucket, Long> logEndOffsets = mockLogEndOffsets(tableId, partitions);

        long snapshotId = 3;
        // commit offsets
        FlussTableLakeSnapshot flussTableLakeSnapshot =
                new FlussTableLakeSnapshot(tableId, snapshotId);
        for (Map.Entry<TableBucket, Long> entry : logEndOffsets.entrySet()) {
            flussTableLakeSnapshot.addBucketOffset(entry.getKey(), entry.getValue());
        }

        // not set commit lake snapshot version to mock old version behavior
        flussTableLakeSnapshotCommitter
                .getCoordinatorGateway()
                .commitLakeTableSnapshot(toCommitLakeTableSnapshotRequest(flussTableLakeSnapshot))
                .get();

        // test deserialize with old version deserializer
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        // read the the json node from lake table node
        JsonNode jsonNode =
                new ObjectMapper()
                        .readTree(zkClient.getOrEmpty(ZkData.LakeTableZNode.path(tableId)).get());
        LakeTableSnapshot lakeTableSnapshot =
                LakeTableSnapshotJsonSerde.INSTANCE.deserializeVersion1(jsonNode);

        // verify the deserialized lakeTableSnapshot
        assertThat(lakeTableSnapshot.getSnapshotId()).isEqualTo(3);
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

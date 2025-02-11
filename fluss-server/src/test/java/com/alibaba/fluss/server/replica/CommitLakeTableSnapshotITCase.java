/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import com.alibaba.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import com.alibaba.fluss.server.log.LogTablet;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.testutils.RpcMessageTestUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LakeTableSnapshot;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** IT case for commit lakehouse data. */
class CommitLakeTableSnapshotITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    private static final int BUCKET_NUM = 3;

    private static ZooKeeperClient zkClient;

    @BeforeAll
    static void beforeAll() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testCommitDataLakeData() throws Exception {
        long tableId = createLogTable();

        for (int bucket = 0; bucket < BUCKET_NUM; bucket++) {
            TableBucket tb = new TableBucket(tableId, bucket);
            // get the leader server
            int leaderServer = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
            TabletServerGateway leaderGateWay =
                    FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderServer);

            for (int i = 0; i < 10; i++) {
                leaderGateWay
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1,
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get();
            }
        }

        // now, let's commit the lake table snapshot
        CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
        long snapshotId = 1;
        long dataLakeLogStartOffset = 0;
        long dataLakeLogEndOffset = 50;
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                genCommitLakeTableSnapshotRequest(
                        tableId,
                        BUCKET_NUM,
                        snapshotId,
                        dataLakeLogStartOffset,
                        dataLakeLogEndOffset);
        coordinatorGateway.commitLakeTableSnapshot(commitLakeTableSnapshotRequest).get();

        Map<TableBucket, Long> bucketsLogStartOffset = new HashMap<>();
        Map<TableBucket, Long> bucketsLogEndOffset = new HashMap<>();
        for (int bucket = 0; bucket < BUCKET_NUM; bucket++) {
            TableBucket tb = new TableBucket(tableId, bucket);
            bucketsLogStartOffset.put(tb, dataLakeLogStartOffset);
            bucketsLogEndOffset.put(tb, dataLakeLogEndOffset);
            Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
            retry(
                    Duration.ofMinutes(2),
                    () -> {
                        LogTablet logTablet = replica.getLogTablet();
                        assertThat(logTablet.getLakeLogStartOffset())
                                .isEqualTo(dataLakeLogStartOffset);
                        assertThat(logTablet.getLakeLogEndOffset()).isEqualTo(dataLakeLogEndOffset);
                    });
        }

        LakeTableSnapshot expectedDataLakeTieredInfo =
                new LakeTableSnapshot(
                        snapshotId, tableId, bucketsLogStartOffset, bucketsLogEndOffset);
        checkLakeTableDataInZk(tableId, expectedDataLakeTieredInfo);
    }

    private void checkLakeTableDataInZk(long tableId, LakeTableSnapshot expected) throws Exception {
        LakeTableSnapshot lakeTableSnapshot = zkClient.getLakeTableSnapshot(tableId).get();
        assertThat(lakeTableSnapshot).isEqualTo(expected);
    }

    private static CommitLakeTableSnapshotRequest genCommitLakeTableSnapshotRequest(
            long tableId, int buckets, long snapshotId, long logStartOffset, long logEndOffset) {
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotInfo reqForTable = commitLakeTableSnapshotRequest.addTablesReq();
        reqForTable.setTableId(tableId);
        reqForTable.setSnapshotId(snapshotId);
        for (int bucket = 0; bucket < buckets; bucket++) {
            TableBucket tb = new TableBucket(tableId, bucket);
            PbLakeTableOffsetForBucket lakeTableOffsetForBucket = reqForTable.addBucketsReq();
            if (tb.getPartitionId() != null) {
                lakeTableOffsetForBucket.setPartitionId(tb.getPartitionId());
            }
            lakeTableOffsetForBucket.setBucketId(tb.getBucket());
            lakeTableOffsetForBucket.setLogStartOffset(logStartOffset);
            lakeTableOffsetForBucket.setLogEndOffset(logEndOffset);
        }
        return commitLakeTableSnapshotRequest;
    }

    private long createLogTable() throws Exception {
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(BUCKET_NUM, "a")
                        .property(ConfigOptions.TABLE_DATALAKE_ENABLED.key(), "true")
                        .build();
        return RpcMessageTestUtils.createTable(
                FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, tableDescriptor);
    }
}

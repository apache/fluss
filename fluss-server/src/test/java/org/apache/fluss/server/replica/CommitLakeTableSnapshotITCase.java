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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.LakeTieringTableState;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotResponse;
import org.apache.fluss.rpc.messages.GetLakeSnapshotRequest;
import org.apache.fluss.rpc.messages.GetLakeSnapshotResponse;
import org.apache.fluss.rpc.messages.PbBucketOffset;
import org.apache.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotMetadata;
import org.apache.fluss.rpc.messages.PbTableOffsets;
import org.apache.fluss.rpc.messages.PrepareLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PrepareLakeTableSnapshotResponse;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/** IT case for commit lakehouse data. */
class CommitLakeTableSnapshotITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setClusterConf(initConfig())
                    .setNumOfTabletServers(3)
                    .build();

    private static final int BUCKET_NUM = 3;

    private static ZooKeeperClient zkClient;

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        // set default datalake format for the cluster and enable datalake tables
        conf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        return conf;
    }

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
            FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

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
        long dataLakeLogEndOffset = 50;
        long dataLakeMaxTimestamp = System.currentTimeMillis();
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                genCommitLakeTableSnapshotRequest(
                        tableId,
                        BUCKET_NUM,
                        snapshotId,
                        dataLakeLogEndOffset,
                        dataLakeMaxTimestamp);
        coordinatorGateway.commitLakeTableSnapshot(commitLakeTableSnapshotRequest).get();

        Map<TableBucket, Long> bucketsLogEndOffset = new HashMap<>();
        for (int bucket = 0; bucket < BUCKET_NUM; bucket++) {
            TableBucket tb = new TableBucket(tableId, bucket);
            bucketsLogEndOffset.put(tb, dataLakeLogEndOffset);
            Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
            retry(
                    Duration.ofMinutes(2),
                    () -> {
                        LogTablet logTablet = replica.getLogTablet();
                        assertThat(logTablet.getLakeLogEndOffset()).isEqualTo(dataLakeLogEndOffset);
                        assertThat(logTablet.getLakeMaxTimestamp()).isEqualTo(dataLakeMaxTimestamp);
                    });
        }

        LakeTableSnapshot expectedDataLakeTieredInfo =
                new LakeTableSnapshot(snapshotId, bucketsLogEndOffset);
        checkLakeTableDataInZk(tableId, expectedDataLakeTieredInfo);
    }

    private void checkLakeTableDataInZk(long tableId, LakeTableSnapshot expected) throws Exception {
        LakeTableSnapshot lakeTableSnapshot = zkClient.getLakeTableSnapshot(tableId, null).get();
        assertThat(lakeTableSnapshot).isEqualTo(expected);
    }

    /**
     * Real-cluster RPC coverage: PREPARE embeds the state in the offsets file, COMMIT registers it
     * (fencing a stale epoch), and GetLakeSnapshot reads it back. A state-only round reuses the
     * previous lake snapshot id, and latest/by-id/readable resolve to the latest entry for that id.
     */
    @Test
    void testCommitTieringStateAndReadBackViaRpc() throws Exception {
        long tableId = createLogTable();
        CoordinatorGateway coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();

        Map<TableBucket, Long> offsets = new HashMap<>();
        for (int bucket = 0; bucket < BUCKET_NUM; bucket++) {
            offsets.put(new TableBucket(tableId, bucket), (bucket + 1) * 10L);
        }

        // Round 1 (normal round): PREPARE carries the state into the offsets file; COMMIT registers
        // snapshot 1 with the current tiering epoch (0 for a freshly created lake table).
        long snapshotId = 1L;
        LakeTieringTableState state1 =
                new LakeTieringTableState(true, Collections.singletonMap(1L, 1000L));
        String path1 = prepareOffsetsFile(coordinatorGateway, tableId, offsets, state1);
        assertThat(commit(coordinatorGateway, tableId, snapshotId, path1, 0L).hasErrorCode())
                .isFalse();

        // GetLakeSnapshot latest / by-id / readable all report state1.
        assertThat(readState(coordinatorGateway, latestRequest())).isEqualTo(state1);
        assertThat(readState(coordinatorGateway, byIdRequest(snapshotId))).isEqualTo(state1);
        assertThat(readState(coordinatorGateway, readableRequest())).isEqualTo(state1);

        // Fencing: a commit carrying a stale/wrong epoch is rejected per-table.
        assertThat(commit(coordinatorGateway, tableId, snapshotId, path1, 999L).hasErrorCode())
                .isTrue();

        // Round 2 (state-only round): no new lake commit, reuse snapshot id 1, only the state
        // advances. It appends another entry with the same id; reads resolve to the latest.
        LakeTieringTableState state2 =
                new LakeTieringTableState(true, Collections.singletonMap(1L, 2000L));
        String path2 = prepareOffsetsFile(coordinatorGateway, tableId, offsets, state2);
        assertThat(commit(coordinatorGateway, tableId, snapshotId, path2, 0L).hasErrorCode())
                .isFalse();

        GetLakeSnapshotResponse latest = coordinatorGateway.getLakeSnapshot(latestRequest()).get();
        // the lake snapshot id is unchanged (state-only round reused it)
        assertThat(latest.getSnapshotId()).isEqualTo(snapshotId);
        // latest and by-id both resolve deterministically to the newest state for that id
        assertThat(LakeTieringTableState.fromJsonBytes(latest.getTieringStateJson()))
                .isEqualTo(state2);
        assertThat(readState(coordinatorGateway, byIdRequest(snapshotId))).isEqualTo(state2);
    }

    private static String prepareOffsetsFile(
            CoordinatorGateway gateway,
            long tableId,
            Map<TableBucket, Long> offsets,
            LakeTieringTableState state)
            throws Exception {
        PrepareLakeTableSnapshotRequest request = new PrepareLakeTableSnapshotRequest();
        PbTableOffsets pbTableOffsets = request.addBucketOffset();
        pbTableOffsets.setTableId(tableId);
        pbTableOffsets
                .setTablePath()
                .setDatabaseName(DATA1_TABLE_PATH.getDatabaseName())
                .setTableName(DATA1_TABLE_PATH.getTableName());
        pbTableOffsets.setTieringStateJson(state.toJsonBytes());
        for (Map.Entry<TableBucket, Long> entry : offsets.entrySet()) {
            PbBucketOffset pbBucketOffset = pbTableOffsets.addBucketOffset();
            pbBucketOffset.setBucketId(entry.getKey().getBucket());
            pbBucketOffset.setLogEndOffset(entry.getValue());
        }
        PrepareLakeTableSnapshotResponse response = gateway.prepareLakeTableSnapshot(request).get();
        return response.getPrepareLakeTableRespsList().get(0).getLakeTableOffsetsPath();
    }

    private static org.apache.fluss.rpc.messages.PbCommitLakeTableSnapshotRespForTable commit(
            CoordinatorGateway gateway,
            long tableId,
            long snapshotId,
            String offsetsPath,
            long tieringEpoch)
            throws Exception {
        CommitLakeTableSnapshotRequest request = new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotMetadata metadata = request.addLakeTableSnapshotMetadata();
        metadata.setTableId(tableId);
        metadata.setSnapshotId(snapshotId);
        metadata.setTieredBucketOffsetsFilePath(offsetsPath);
        // make it readable so getReadableLakeSnapshot returns it
        metadata.setReadableBucketOffsetsFilePath(offsetsPath);
        // keep all previous entries so a reused snapshot id yields multiple entries
        metadata.setEarliestSnapshotIdToKeep(-1L);
        metadata.setTieringEpoch(tieringEpoch);
        CommitLakeTableSnapshotResponse response = gateway.commitLakeTableSnapshot(request).get();
        return response.getTableRespsList().get(0);
    }

    private static LakeTieringTableState readState(
            CoordinatorGateway gateway, GetLakeSnapshotRequest request) throws Exception {
        GetLakeSnapshotResponse response = gateway.getLakeSnapshot(request).get();
        assertThat(response.hasTieringStateJson()).isTrue();
        return LakeTieringTableState.fromJsonBytes(response.getTieringStateJson());
    }

    private static GetLakeSnapshotRequest latestRequest() {
        return newGetLakeSnapshotRequest();
    }

    private static GetLakeSnapshotRequest byIdRequest(long snapshotId) {
        GetLakeSnapshotRequest request = newGetLakeSnapshotRequest();
        request.setSnapshotId(snapshotId);
        return request;
    }

    private static GetLakeSnapshotRequest readableRequest() {
        GetLakeSnapshotRequest request = newGetLakeSnapshotRequest();
        request.setReadable(true);
        return request;
    }

    private static GetLakeSnapshotRequest newGetLakeSnapshotRequest() {
        GetLakeSnapshotRequest request = new GetLakeSnapshotRequest();
        request.setTablePath()
                .setDatabaseName(DATA1_TABLE_PATH.getDatabaseName())
                .setTableName(DATA1_TABLE_PATH.getTableName());
        return request;
    }

    private static CommitLakeTableSnapshotRequest genCommitLakeTableSnapshotRequest(
            long tableId, int buckets, long snapshotId, long logEndOffset, long maxTimestamp) {
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
            lakeTableOffsetForBucket.setLogEndOffset(logEndOffset);
            lakeTableOffsetForBucket.setMaxTimestamp(maxTimestamp);
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

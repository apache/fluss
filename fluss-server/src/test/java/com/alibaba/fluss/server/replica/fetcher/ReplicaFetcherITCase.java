/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.replica.fetcher;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.PbPutKvRespForBucket;
import com.alibaba.fluss.rpc.messages.PutKvResponse;
import com.alibaba.fluss.server.entity.FetchData;
import com.alibaba.fluss.server.log.FetchParams;
import com.alibaba.fluss.server.replica.Replica;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.testutils.RpcMessageTestUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA_PK;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static com.alibaba.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static com.alibaba.fluss.record.TestData.EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK;
import static com.alibaba.fluss.server.testutils.KvTestUtils.assertLookupResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertFetchLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertFetchLogResponseWithRowKind;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newFetchLogRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newLookupRequest;
import static com.alibaba.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEqualsWithRowKind;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static com.alibaba.fluss.testutils.DataTestUtils.genKvRecords;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.DataTestUtils.getKeyValuePairs;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for replica fetcher. */
public class ReplicaFetcherITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
    }

    @Test
    void testProduceLogNeedAck() throws Exception {
        // set bucket count to 1 to easy for debug.
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1, "a").build();

        // wait until all the gateway has same metadata because the follower fetcher manager need
        // to get the leader address from server metadata while make follower.
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();

        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, tableDescriptor);
        int bucketId = 0;
        TableBucket tb = new TableBucket(tableId, bucketId);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // send one batch, which need ack.
        assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1,
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                bucketId,
                0L);

        // check leader log data.
        assertFetchLogResponse(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, bucketId, 0L)).get(),
                tableId,
                bucketId,
                10L,
                DATA1);

        // check follower log data.
        LeaderAndIsr leaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        for (int followId :
                leaderAndIsr.isr().stream()
                        .filter(id -> id != leader)
                        .collect(Collectors.toList())) {

            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(followId).getReplicaManager();
            // wait util follower highWaterMark equals leader.
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            replicaManager
                                                    .getReplicaOrException(tb)
                                                    .getLogTablet()
                                                    .getHighWatermark())
                                    .isEqualTo(10L));

            CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                    new CompletableFuture<>();
            // mock client fetch from follower.
            replicaManager.fetchLogRecords(
                    new FetchParams(-1, false, Integer.MAX_VALUE, -1, -1),
                    Collections.singletonMap(tb, new FetchData(tableId, 0L, 1024 * 1024)),
                    future::complete);
            Map<TableBucket, FetchLogResultForBucket> result = future.get();
            assertThat(result.size()).isEqualTo(1);
            FetchLogResultForBucket resultForBucket = result.get(tb);
            assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
            LogRecords records = resultForBucket.records();
            assertThat(records).isNotNull();
            assertLogRecordsEquals(DATA1_ROW_TYPE, records, DATA1);
        }
    }

    @Test
    void testPutKvNeedAck() throws Exception {
        // wait until all the gateway has same metadata because the follower fetcher manager need
        // to get the leader address from server metadata while make follower.
        FLUSS_CLUSTER_EXTENSION.waitUtilAllGatewayHasSameMetadata();

        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, createPkTableDescriptor());
        int bucketId = 0;
        TableBucket tb = new TableBucket(tableId, bucketId);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // send one batch, which need ack.
        assertPutKvResponse(
                leaderGateWay
                        .putKv(
                                newPutKvRequest(
                                        tableId,
                                        bucketId,
                                        -1,
                                        genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                        .get(),
                bucketId);

        // check leader log data.
        assertFetchLogResponseWithRowKind(
                leaderGateWay.fetchLog(newFetchLogRequest(-1, tableId, bucketId, 0L)).get(),
                tableId,
                bucketId,
                8L,
                EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK);

        // check follower log data.
        LeaderAndIsr leaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        for (int followId :
                leaderAndIsr.isr().stream()
                        .filter(id -> id != leader)
                        .collect(Collectors.toList())) {
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(followId).getReplicaManager();

            // wait util follower highWaterMark equals leader. So we can fetch log from follower
            // before highWaterMark.
            retry(
                    Duration.ofMinutes(1),
                    () ->
                            assertThat(
                                            replicaManager
                                                    .getReplicaOrException(tb)
                                                    .getLogTablet()
                                                    .getHighWatermark())
                                    .isEqualTo(8L));

            CompletableFuture<Map<TableBucket, FetchLogResultForBucket>> future =
                    new CompletableFuture<>();
            // mock client fetch from follower.
            replicaManager.fetchLogRecords(
                    new FetchParams(-1, false, Integer.MAX_VALUE, -1, -1),
                    Collections.singletonMap(tb, new FetchData(tableId, 0L, 1024 * 1024)),
                    future::complete);
            Map<TableBucket, FetchLogResultForBucket> result = future.get();
            assertThat(result.size()).isEqualTo(1);
            FetchLogResultForBucket resultForBucket = result.get(tb);
            assertThat(resultForBucket.getTableBucket()).isEqualTo(tb);
            LogRecords records = resultForBucket.records();
            assertThat(records).isNotNull();
            assertLogRecordsEqualsWithRowKind(
                    DATA1_ROW_TYPE, records, EXPECTED_LOG_RESULTS_FOR_DATA_1_WITH_PK);
        }
    }

    @Test
    void testFlushForPutKvNeedAck() throws Exception {
        // create a table and wait all replica ready
        long tableId =
                createTable(
                        FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH_PK, createPkTableDescriptor());
        int bucketId = 0;
        TableBucket tb = new TableBucket(tableId, bucketId);

        FLUSS_CLUSTER_EXTENSION.waitUtilAllReplicaReady(tb);

        // let's kill a non leader server
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);

        int followerToStop =
                FLUSS_CLUSTER_EXTENSION.getTabletServerNodes().stream()
                        .filter(node -> node.id() != leader)
                        .findFirst()
                        .get()
                        .id();

        int leaderEpoch = 0;
        // stop the follower replica for the bucket
        FLUSS_CLUSTER_EXTENSION.stopReplica(followerToStop, tb, leaderEpoch);

        // put kv record batch to the leader,
        // but as one server is killed, the put won't be ack
        // , so kv won't be flushed although the log has been written

        // put kv record batch
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        KvRecordBatch kvRecords =
                genKvRecordBatch(
                        Tuple2.of("k1", new Object[] {1, "a"}),
                        Tuple2.of("k1", new Object[] {2, "b"}),
                        Tuple2.of("k2", new Object[] {3, "b1"}));

        CompletableFuture<PutKvResponse> putResponse =
                leaderGateWay.putKv(newPutKvRequest(tableId, bucketId, -1, kvRecords));

        // wait util the log has been written
        Replica replica = FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(replica.getLocalLogEndOffset()).isEqualTo(4L));

        List<Tuple2<byte[], byte[]>> expectedKeyValues =
                getKeyValuePairs(
                        genKvRecords(
                                Tuple2.of("k1", new Object[] {2, "b"}),
                                Tuple2.of("k2", new Object[] {3, "b1"})));

        // but we can't lookup the kv since it hasn't been flushed
        for (Tuple2<byte[], byte[]> keyValue : expectedKeyValues) {
            assertLookupResponse(
                    leaderGateWay.lookup(newLookupRequest(tableId, bucketId, keyValue.f0)).get(),
                    null);
        }

        // start the follower replica by notify leaderAndIsr,
        // then the kv should be flushed finally
        LeaderAndIsr currentLeaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        currentLeaderAndIsr.leader(),
                        currentLeaderAndIsr.leaderEpoch() + 1,
                        currentLeaderAndIsr.isr(),
                        currentLeaderAndIsr.coordinatorEpoch(),
                        currentLeaderAndIsr.bucketEpoch());
        FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                followerToStop, DATA1_TABLE_PATH, tb, newLeaderAndIsr, Arrays.asList(0, 1, 2));

        // wait util the put future is done
        putResponse.get();

        // then we can check all the value
        for (Tuple2<byte[], byte[]> keyValue : expectedKeyValues) {
            assertLookupResponse(
                    leaderGateWay.lookup(newLookupRequest(tableId, bucketId, keyValue.f0)).get(),
                    keyValue.f1);
        }
    }

    private TableDescriptor createPkTableDescriptor() {
        return TableDescriptor.builder().schema(DATA1_SCHEMA_PK).distributedBy(1, "a").build();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }

    private static void assertPutKvResponse(PutKvResponse putKvResponse, int bucketId) {
        assertThat(putKvResponse.getBucketsRespsCount()).isEqualTo(1);
        PbPutKvRespForBucket putKvRespForBucket = putKvResponse.getBucketsRespsList().get(0);
        assertThat(putKvRespForBucket.getBucketId()).isEqualTo(bucketId);
    }
}

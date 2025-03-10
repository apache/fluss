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
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.PbProduceLogRespForBucket;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.server.testutils.FlussClusterExtension;
import com.alibaba.fluss.server.testutils.RpcMessageTestUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_SCHEMA;
import static com.alibaba.fluss.record.TestData.DATA1_TABLE_PATH;
import static com.alibaba.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
import static com.alibaba.fluss.testutils.common.CommonTestUtils.waitValue;
import static com.alibaba.fluss.utils.function.ThrowingConsumer.unchecked;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for adjust isr while ISR change. */
public class AdjustIsrITCase {
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
    void testIsrShrinkAndExpend() throws Exception {
        long tableId = createLogTable();
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr currentLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
                        Duration.ofSeconds(20),
                        "Leader and isr not found");
        List<Integer> isr = currentLeaderAndIsr.isr();
        assertThat(isr).containsExactlyInAnyOrder(0, 1, 2);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        Integer stopFollower = isr.stream().filter(i -> i != leader).findFirst().get();
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(stopFollower);
        isr.remove(stopFollower);

        // send one batch data to check the stop follower will become out of sync replica.
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        RpcMessageTestUtils.assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        1, // need not ack in this test.
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // Wait the stop follower to be removed from ISR because the follower tablet server will not
        // fetch log from leader anymore.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .isPresent()
                                .hasValueSatisfying(
                                        leaderAndIsr ->
                                                assertThat(leaderAndIsr.isr())
                                                        .containsExactlyInAnyOrderElementsOf(isr)));

        // check leader highWatermark increase even if the stop follower is out of sync.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .getTabletServerById(leader)
                                                .getReplicaManager()
                                                .getReplicaOrException(tb)
                                                .getLogTablet()
                                                .getHighWatermark())
                                .isEqualTo(10L));

        // make this tablet server re-start.
        FLUSS_CLUSTER_EXTENSION.startTabletServer(stopFollower);
        isr.add(stopFollower);

        // retry until the stop follower add back to ISR.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .isPresent()
                                .hasValueSatisfying(
                                        leaderAndIsr ->
                                                assertThat(leaderAndIsr.isr())
                                                        .containsExactlyInAnyOrderElementsOf(isr)));
    }

    @Test
    void testIsrSetSizeLessThanMinInSynReplicasNumber() throws Exception {
        long tableId = createLogTable();
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr currentLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
                        Duration.ofSeconds(20),
                        "Leader and isr not found");
        List<Integer> isr = currentLeaderAndIsr.isr();
        assertThat(isr).containsExactlyInAnyOrder(0, 1, 2);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        List<Integer> followerSet =
                isr.stream().filter(i -> i != leader).collect(Collectors.toList());
        followerSet.forEach(unchecked(FLUSS_CLUSTER_EXTENSION::stopTabletServer));
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        RpcMessageTestUtils.assertProduceLogResponse(
                leaderGateWay
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        1, // need not ack in this test.
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // Wait unit the leader isr set only contains the leader.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .getTabletServerById(leader)
                                                .getReplicaManager()
                                                .getReplicaOrException(tb)
                                                .getIsr())
                                .containsExactlyInAnyOrder(leader));
        // check leader highWatermark not increase because the isr set < min_isr
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getTabletServerById(leader)
                                .getReplicaManager()
                                .getReplicaOrException(tb)
                                .getLogTablet()
                                .getHighWatermark())
                .isEqualTo(0L);

        ProduceLogResponse produceLogResponse =
                leaderGateWay
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1, // need ack
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get();
        assertThat(produceLogResponse.getBucketsRespsCount()).isEqualTo(1);
        PbProduceLogRespForBucket respForBucket = produceLogResponse.getBucketsRespsList().get(0);
        assertThat(respForBucket.getBucketId()).isEqualTo(0);
        assertThat(respForBucket.hasErrorCode()).isTrue();
        assertThat(respForBucket.getErrorCode())
                .isEqualTo(Errors.NOT_ENOUGH_REPLICAS_EXCEPTION.code());
        assertThat(respForBucket.getErrorMessage())
                .contains(
                        String.format(
                                "The size of the current ISR [%s] is insufficient to satisfy the "
                                        + "required acks -1 for table bucket TableBucket{tableId=%s, bucket=0}.",
                                leader, tableId));
        // check again leader highWatermark not increase because the isr set < min_isr
        assertThat(
                        FLUSS_CLUSTER_EXTENSION
                                .getTabletServerById(leader)
                                .getReplicaManager()
                                .getReplicaOrException(tb)
                                .getLogTablet()
                                .getHighWatermark())
                .isEqualTo(0L);
    }

    private long createLogTable() throws Exception {
        // Set bucket to 1 to easy for debug.
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1, "a").build();
        return RpcMessageTestUtils.createTable(
                FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, tableDescriptor);
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);

        // set log replica max lag time to 3 seconds to reduce the test wait time.
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(3));

        // set log replica min in sync replicas number to 2, if the isr set size less than 2,
        // the produce log request will be failed, and the leader HW will not increase.
        conf.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return conf;
    }
}

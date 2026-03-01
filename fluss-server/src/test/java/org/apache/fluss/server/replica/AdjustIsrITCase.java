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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.apache.fluss.utils.function.ThrowingConsumer.unchecked;
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

    /**
     * The test is used to verify that isr will be changed due to follower lags behind leader and
     * catch up with leader.
     */
    @Test
    void testIsrShrinkAndExpand() throws Exception {
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

        FLUSS_CLUSTER_EXTENSION.waitAndGetFollowerReplica(tb, stopFollower);
        // stop follower replica for the bucket
        FLUSS_CLUSTER_EXTENSION.stopReplica(stopFollower, tb, currentLeaderAndIsr.leaderEpoch());

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
                                        1,
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

        currentLeaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        currentLeaderAndIsr.leader(),
                        currentLeaderAndIsr.leaderEpoch() + 1,
                        isr,
                        currentLeaderAndIsr.coordinatorEpoch(),
                        currentLeaderAndIsr.bucketEpoch());
        isr.add(stopFollower);
        FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                stopFollower, DATA1_TABLE_PATH, tb, newLeaderAndIsr, isr);
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

    /**
     * Test time consistency during ISR shrink.
     *
     * <p>Verifies that timestamps remain monotonic when a follower is removed from ISR due to lag.
     * This test ensures:
     *
     * <ul>
     *   <li>Records written before ISR shrink have valid timestamps
     *   <li>Records written after ISR shrink continue with monotonic timestamps
     *   <li>No timestamp regression occurs during the ISR transition
     * </ul>
     */
    @Test
    void testTimeConsistencyDuringIsrShrink() throws Exception {
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

        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // Write records before ISR shrink and capture timestamp
        long timestampBeforeShrink = System.currentTimeMillis();
        RpcMessageTestUtils.assertProduceLogResponse(
                leaderGateway
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1,
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        // Stop follower to trigger ISR shrink
        FLUSS_CLUSTER_EXTENSION.stopReplica(stopFollower, tb, currentLeaderAndIsr.leaderEpoch());
        isr.remove(stopFollower);

        // Wait for ISR to shrink
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .isPresent()
                                .hasValueSatisfying(
                                        leaderAndIsr ->
                                                assertThat(leaderAndIsr.isr())
                                                        .containsExactlyInAnyOrderElementsOf(isr)));

        // Write records after ISR shrink - timestamps should still be monotonic
        long timestampAfterShrink = System.currentTimeMillis();
        assertThat(timestampAfterShrink).isGreaterThanOrEqualTo(timestampBeforeShrink);

        RpcMessageTestUtils.assertProduceLogResponse(
                leaderGateway
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1,
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                10,
                10L);

        // Verify log offsets are sequential (no gaps)
        long highWatermark =
                FLUSS_CLUSTER_EXTENSION
                        .getTabletServerById(leader)
                        .getReplicaManager()
                        .getReplicaOrException(tb)
                        .getLogTablet()
                        .getHighWatermark();
        assertThat(highWatermark).isEqualTo(20L);
    }

    /**
     * Test time consistency during ISR expansion.
     *
     * <p>Verifies that timestamps remain monotonic when a follower rejoins the ISR after catching
     * up. This test ensures:
     *
     * <ul>
     *   <li>Follower catches up with correct timestamp ordering
     *   <li>Records written during expansion maintain timestamp monotonicity
     *   <li>No timestamp anomalies during ISR membership change
     * </ul>
     */
    @Test
    void testTimeConsistencyDuringIsrExpansion() throws Exception {
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

        // Stop and remove follower from ISR
        FLUSS_CLUSTER_EXTENSION.stopReplica(stopFollower, tb, currentLeaderAndIsr.leaderEpoch());
        isr.remove(stopFollower);

        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        RpcMessageTestUtils.assertProduceLogResponse(
                leaderGateway
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1,
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                0,
                0L);

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .isPresent()
                                .hasValueSatisfying(
                                        leaderAndIsr ->
                                                assertThat(leaderAndIsr.isr())
                                                        .containsExactlyInAnyOrderElementsOf(isr)));

        // Capture timestamp before expansion
        long timestampBeforeExpansion = System.currentTimeMillis();

        // Restart follower to rejoin ISR
        currentLeaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
        LeaderAndIsr newLeaderAndIsr =
                new LeaderAndIsr(
                        currentLeaderAndIsr.leader(),
                        currentLeaderAndIsr.leaderEpoch() + 1,
                        isr,
                        currentLeaderAndIsr.coordinatorEpoch(),
                        currentLeaderAndIsr.bucketEpoch());
        isr.add(stopFollower);
        FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                stopFollower, DATA1_TABLE_PATH, tb, newLeaderAndIsr, isr);

        // Wait for follower to rejoin ISR
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .isPresent()
                                .hasValueSatisfying(
                                        leaderAndIsr ->
                                                assertThat(leaderAndIsr.isr())
                                                        .containsExactlyInAnyOrderElementsOf(isr)));

        // Write records after expansion - verify timestamp consistency
        long timestampAfterExpansion = System.currentTimeMillis();
        assertThat(timestampAfterExpansion).isGreaterThanOrEqualTo(timestampBeforeExpansion);

        RpcMessageTestUtils.assertProduceLogResponse(
                leaderGateway
                        .produceLog(
                                RpcMessageTestUtils.newProduceLogRequest(
                                        tableId,
                                        tb.getBucket(),
                                        -1,
                                        genMemoryLogRecordsByObject(DATA1)))
                        .get(),
                10,
                10L);

        // Verify all replicas have consistent high watermark
        retry(
                Duration.ofMinutes(1),
                () -> {
                    for (Integer replicaId : isr) {
                        long hwm =
                                FLUSS_CLUSTER_EXTENSION
                                        .getTabletServerById(replicaId)
                                        .getReplicaManager()
                                        .getReplicaOrException(tb)
                                        .getLogTablet()
                                        .getHighWatermark();
                        assertThat(hwm).isEqualTo(20L);
                    }
                });
    }

    /**
     * Test timestamp consistency across rapid ISR changes.
     *
     * <p>Verifies system behavior under rapid ISR membership fluctuations. This stress test
     * ensures:
     *
     * <ul>
     *   <li>Timestamps remain monotonic despite rapid ISR changes
     *   <li>No data loss or corruption during turbulent periods
     *   <li>System maintains consistency under high churn
     * </ul>
     */
    @Test
    void testTimeConsistencyAcrossRapidIsrChanges() throws Exception {
        long tableId = createLogTable();
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr currentLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
                        Duration.ofSeconds(20),
                        "Leader and isr not found");
        List<Integer> isr = currentLeaderAndIsr.isr();
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        List<Integer> followers =
                isr.stream().filter(i -> i != leader).collect(Collectors.toList());

        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        long lastTimestamp = System.currentTimeMillis();
        long lastOffset = 0L;

        // Simulate 3 cycles of ISR shrink/expand
        for (int cycle = 0; cycle < 3; cycle++) {
            Integer followerToStop = followers.get(cycle % followers.size());

            // Write before change
            long currentTimestamp = System.currentTimeMillis();
            assertThat(currentTimestamp).isGreaterThanOrEqualTo(lastTimestamp);
            lastTimestamp = currentTimestamp;

            RpcMessageTestUtils.assertProduceLogResponse(
                    leaderGateway
                            .produceLog(
                                    RpcMessageTestUtils.newProduceLogRequest(
                                            tableId,
                                            tb.getBucket(),
                                            -1,
                                            genMemoryLogRecordsByObject(DATA1)))
                            .get(),
                    (int) lastOffset,
                    lastOffset);
            lastOffset += 10;

            // Stop follower
            currentLeaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
            FLUSS_CLUSTER_EXTENSION.stopReplica(
                    followerToStop, tb, currentLeaderAndIsr.leaderEpoch());
            Thread.sleep(100);

            // Write during shrink
            currentTimestamp = System.currentTimeMillis();
            assertThat(currentTimestamp).isGreaterThanOrEqualTo(lastTimestamp);
            lastTimestamp = currentTimestamp;

            // Restart follower
            currentLeaderAndIsr = zkClient.getLeaderAndIsr(tb).get();
            List<Integer> currentIsr = currentLeaderAndIsr.isr();
            LeaderAndIsr newLeaderAndIsr =
                    new LeaderAndIsr(
                            currentLeaderAndIsr.leader(),
                            currentLeaderAndIsr.leaderEpoch() + 1,
                            currentIsr,
                            currentLeaderAndIsr.coordinatorEpoch(),
                            currentLeaderAndIsr.bucketEpoch());
            currentIsr.add(followerToStop);
            FLUSS_CLUSTER_EXTENSION.notifyLeaderAndIsr(
                    followerToStop, DATA1_TABLE_PATH, tb, newLeaderAndIsr, currentIsr);
            Thread.sleep(100);
        }

        // Final verification - all timestamps should be monotonic
        long finalTimestamp = System.currentTimeMillis();
        assertThat(finalTimestamp).isGreaterThanOrEqualTo(lastTimestamp);

        // Verify final high watermark is consistent
        retry(
                Duration.ofMinutes(1),
                () -> {
                    long hwm =
                            FLUSS_CLUSTER_EXTENSION
                                    .getTabletServerById(leader)
                                    .getReplicaManager()
                                    .getReplicaOrException(tb)
                                    .getLogTablet()
                                    .getHighWatermark();
                    assertThat(hwm).isGreaterThanOrEqualTo(20L);
                });
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

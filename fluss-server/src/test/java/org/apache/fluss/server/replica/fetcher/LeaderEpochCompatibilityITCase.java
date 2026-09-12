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

package org.apache.fluss.server.replica.fetcher;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.rpc.messages.AlterClusterConfigsRequest;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.PbFetchLogReqForBucket;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newFetchLogRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Exercises production replication modes through real RPC and persistent node restarts. */
class LeaderEpochCompatibilityITCase {
    private FlussClusterExtension cluster;
    private TableBucket bucket;
    private final List<Object[]> expected = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testMixedModesReplicateAndMigrate(boolean leaderEnabled, boolean followerEnabled)
            throws Exception {
        start(leaderEnabled);
        int leader = cluster.waitAndGetLeader(bucket);
        for (int id = 0; id < 3; id++) {
            if (id != leader && followerEnabled != leaderEnabled) {
                restart(id, followerEnabled);
            }
        }
        append();
        assertReplicas();
        for (int id = 0; id < 3; id++) {
            int epoch = replica(id).getLogTablet().lastFetchedEpoch(expected.size());
            if (leaderEnabled && (id == leader || followerEnabled)) {
                assertThat(epoch).isGreaterThanOrEqualTo(0);
            } else {
                assertThat(epoch).isEqualTo(-1);
            }
        }
        assertEpochResponse(leader, followerEnabled, leaderEnabled && followerEnabled);
        migrateAndAppend();
        assertThat(replica(leader).getLogTablet().isLeaderEpochEnabled()).isEqualTo(leaderEnabled);
        for (int id = 0; id < 3; id++) {
            if (id != leader) {
                assertThat(replica(id).getLogTablet().isLeaderEpochEnabled())
                        .isEqualTo(followerEnabled);
            }
        }
    }

    @Test
    void testRollingEnableDisableAndReenable() throws Exception {
        start(false);
        append();
        assertReplicas();
        for (int id = 0; id < 3; id++) {
            restart(id, true);
            append();
            assertReplicas();
        }
        for (int id = 0; id < 3; id++) {
            assertThat(replica(id).getLogTablet().lastFetchedEpoch(10)).isEqualTo(-1);
        }
        int follower = (cluster.waitAndGetLeader(bucket) + 1) % 3;
        restart(follower, false);
        append();
        assertReplicas();
        long untrackedEnd = expected.size();
        restart(follower, true);
        assertThat(replica(follower).getLogTablet().lastFetchedEpoch(untrackedEnd)).isEqualTo(-1);
        append();
        assertReplicas();
        migrateAndAppend();
        for (int id = 0; id < 3; id++) {
            assertThat(replica(id).getLogTablet().lastFetchedEpoch(expected.size()))
                    .isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void testAlterModesWithoutRestartingNodes() throws Exception {
        start(true);
        append();
        assertReplicas();
        List<TabletServer> runningNodes = new ArrayList<>();
        for (int id = 0; id < 3; id++) {
            runningNodes.add(cluster.getTabletServerById(id));
        }
        alterMode(false);
        for (int id = 0; id < 3; id++) {
            assertThat(replica(id).getLogTablet().lastFetchedEpoch(10)).isEqualTo(-1);
        }
        append();
        assertReplicas();
        alterMode(true);
        append();
        assertReplicas();
        for (int id = 0; id < 3; id++) {
            assertThat(cluster.getTabletServerById(id)).isSameAs(runningNodes.get(id));
            // Enabling within the same leader epoch cannot invent a new epoch boundary.
            assertThat(replica(id).getLogTablet().lastFetchedEpoch(expected.size())).isEqualTo(-1);
        }
        migrateAndAppend();
        for (int id = 0; id < 3; id++) {
            assertThat(replica(id).getLogTablet().lastFetchedEpoch(expected.size()))
                    .isGreaterThanOrEqualTo(0);
        }
    }

    private void alterMode(boolean enabled) throws Exception {
        AlterClusterConfigsRequest request = new AlterClusterConfigsRequest();
        request.addAlterConfig()
                .setConfigKey(ConfigOptions.LOG_REPLICATION_LEADER_EPOCH_ENABLED.key())
                .setConfigValue(Boolean.toString(enabled))
                .setOpType(AlterConfigOpType.SET.value());
        cluster.newCoordinatorClient().alterClusterConfigs(request).get(30, TimeUnit.SECONDS);
        retry(
                Duration.ofSeconds(30),
                () -> {
                    for (int id = 0; id < 3; id++) {
                        assertThat(replica(id).getLogTablet().isLeaderEpochEnabled())
                                .isEqualTo(enabled);
                    }
                });
    }

    private void start(boolean enabled) throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(2));
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ZERO);
        FlussClusterExtension.Builder builder =
                FlussClusterExtension.builder().setNumOfTabletServers(3).setClusterConf(conf);
        for (int id = 0; id < 3; id++) {
            builder.setTabletServerConf(id, mode(enabled));
        }
        cluster = builder.build();
        cluster.start();
        long tableId =
                createTable(
                        cluster,
                        DATA1_TABLE_PATH,
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA)
                                .distributedBy(1, "a")
                                .build());
        bucket = new TableBucket(tableId, 0);
        cluster.waitUntilAllReplicaReady(bucket);
    }

    private static Configuration mode(boolean enabled) {
        Configuration config = new Configuration();
        config.set(ConfigOptions.LOG_REPLICATION_LEADER_EPOCH_ENABLED, enabled);
        return config;
    }

    private void restart(int id, boolean enabled) throws Exception {
        cluster.restartTabletServer(id, mode(enabled));
        cluster.waitUntilReplicaExpandToIsr(bucket, id);
        assertReplicas();
    }

    private void append() throws Exception {
        cluster.waitAndGetLeaderReplica(bucket);
        int leader = cluster.waitAndGetLeader(bucket);
        List<Object[]> batch = new ArrayList<>();
        for (int i = 0; i < DATA1.size(); i++) {
            batch.add(new Object[] {expected.size() + i, "row-" + (expected.size() + i)});
        }
        assertProduceLogResponse(
                cluster.newTabletServerClientForNode(leader)
                        .produceLog(
                                newProduceLogRequest(
                                        bucket.getTableId(),
                                        0,
                                        -1,
                                        genMemoryLogRecordsByObject(batch)))
                        .get(30, TimeUnit.SECONDS),
                0,
                (long) expected.size());
        expected.addAll(batch);
    }

    private void assertEpochResponse(int leader, boolean requestEpoch, boolean responseEpoch)
            throws Exception {
        int follower = (leader + 1) % 3;
        FetchLogRequest request =
                newFetchLogRequest(follower, bucket.getTableId(), 0, expected.size());
        if (requestEpoch) {
            PbFetchLogReqForBucket bucketRequest =
                    request.getTablesReqsList().get(0).getBucketsReqsList().get(0);
            bucketRequest
                    .setCurrentLeaderEpoch(replica(leader).getLeaderEpoch())
                    .setLastFetchedEpoch(
                            replica(follower).getLogTablet().lastFetchedEpoch(expected.size()));
        }
        PbFetchLogRespForBucket response =
                cluster.newTabletServerClientForNode(leader)
                        .fetchLog(request)
                        .get(30, TimeUnit.SECONDS)
                        .getTablesRespsList()
                        .get(0)
                        .getBucketsRespsList()
                        .get(0);
        assertThat(response.hasErrorCode()).isFalse();
        assertThat(response.hasCurrentLeaderEpoch()).isEqualTo(responseEpoch);
        assertThat(response.hasDivergingEpoch()).isFalse();
    }

    private void migrateAndAppend() throws Exception {
        int oldLeader = cluster.waitAndGetLeader(bucket);
        cluster.stopTabletServer(oldLeader);
        retry(
                Duration.ofSeconds(30),
                () -> assertThat(cluster.waitAndGetLeader(bucket)).isNotEqualTo(oldLeader));
        append();
        cluster.startTabletServer(oldLeader);
        cluster.waitUntilReplicaExpandToIsr(bucket, oldLeader);
        assertReplicas();
    }

    private Replica replica(int id) {
        return cluster.getTabletServerById(id).getReplicaManager().getReplicaOrException(bucket);
    }

    private void assertReplicas() throws Exception {
        retry(
                Duration.ofSeconds(30),
                () -> {
                    assertThat(cluster.waitAndGetLeaderReplica(bucket).getIsr()).hasSize(3);
                    for (int id = 0; id < 3; id++) {
                        Replica replica = replica(id);
                        assertThat(replica.getLocalLogEndOffset()).isEqualTo(expected.size());
                        assertThat(replica.getLogHighWatermark()).isEqualTo(expected.size());
                        if (!expected.isEmpty()) {
                            assertLogRecordsEquals(
                                    DATA1_ROW_TYPE,
                                    replica.getLogTablet()
                                            .read(
                                                    0,
                                                    Integer.MAX_VALUE,
                                                    FetchIsolation.LOG_END,
                                                    true)
                                            .getRecords(),
                                    expected);
                        }
                    }
                });
    }
}

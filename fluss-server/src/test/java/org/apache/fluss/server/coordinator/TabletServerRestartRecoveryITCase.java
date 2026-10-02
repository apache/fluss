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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FencedTabletServerEpochException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetClusterHealthRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.server.coordinator.event.AccessContextEvent;
import org.apache.fluss.server.coordinator.event.DeadTabletServerEvent;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TabletServerRegistration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for rebuilding control requests after a TabletServer restarts. */
public class TabletServerRestartRecoveryITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(4)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;
    private CoordinatorGateway coordinatorGateway;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
    }

    @AfterEach
    void restoreTabletServers() throws Exception {
        for (int serverId = 0; serverId < 4; serverId++) {
            if (FLUSS_CLUSTER_EXTENSION.getTabletServerById(serverId) == null) {
                FLUSS_CLUSTER_EXTENSION.startTabletServer(serverId);
            }
        }
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(4);
    }

    @Test
    void testRebuildControlStateAfterTabletServerRestart() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        TablePath tablePath = TablePath.of("test_db_restart_recovery", "test_table");
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tableBucket);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tableBucket);

        List<Integer> isr = waitAndGetIsr(tableBucket);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        int restartedServerId =
                isr.stream()
                        .filter(serverId -> serverId != leader)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("No follower replica was assigned"));
        long oldTabletServerEpoch = getTabletServerEpoch(restartedServerId);

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(restartedServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tableBucket).get().isr())
                                .doesNotContain(restartedServerId));

        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        leaderGateway.produceLog(newProduceLogRequest(tableId, 0, 1, records)).get();

        FLUSS_CLUSTER_EXTENSION.startTabletServer(restartedServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(4);

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(getTabletServerEpoch(restartedServerId))
                                .isNotEqualTo(oldTabletServerEpoch));
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tableBucket);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        retry(Duration.ofMinutes(1), this::assertClusterHealthGreen);

        assertOldEpochIsFenced(restartedServerId, oldTabletServerEpoch);

        long currentTabletServerEpoch = getTabletServerEpoch(restartedServerId);
        FLUSS_CLUSTER_EXTENSION
                .getCoordinatorServer()
                .getCoordinatorEventProcessor()
                .getCoordinatorEventManager()
                .put(new DeadTabletServerEvent(restartedServerId, oldTabletServerEpoch));
        long liveTabletServerEpoch =
                fromCoordinatorContext(
                        context ->
                                context.getLiveTabletServers()
                                        .get(restartedServerId)
                                        .tabletServerEpoch());
        assertThat(liveTabletServerEpoch).isEqualTo(currentTabletServerEpoch);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(4);
    }

    private void assertOldEpochIsFenced(int tabletServerId, long oldTabletServerEpoch) {
        TabletServerGateway gateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(tabletServerId);

        assertThatThrownBy(
                        () ->
                                gateway.updateMetadata(
                                                new UpdateMetadataRequest()
                                                        .setCoordinatorEpoch(0)
                                                        .setTabletServerEpoch(oldTabletServerEpoch))
                                        .get())
                .hasCauseInstanceOf(FencedTabletServerEpochException.class);
        assertThatThrownBy(
                        () ->
                                gateway.notifyLeaderAndIsr(
                                                new NotifyLeaderAndIsrRequest()
                                                        .setCoordinatorEpoch(0)
                                                        .setTabletServerEpoch(oldTabletServerEpoch))
                                        .get())
                .hasCauseInstanceOf(FencedTabletServerEpochException.class);
        assertThatThrownBy(
                        () ->
                                gateway.stopReplica(
                                                new StopReplicaRequest()
                                                        .setCoordinatorEpoch(0)
                                                        .setTabletServerEpoch(oldTabletServerEpoch))
                                        .get())
                .hasCauseInstanceOf(FencedTabletServerEpochException.class);
    }

    private <T> T fromCoordinatorContext(Function<CoordinatorContext, T> accessor)
            throws Exception {
        AccessContextEvent<T> event = new AccessContextEvent<>(accessor);
        FLUSS_CLUSTER_EXTENSION
                .getCoordinatorServer()
                .getCoordinatorEventProcessor()
                .getCoordinatorEventManager()
                .put(event);
        return event.getResultFuture().get(30, TimeUnit.SECONDS);
    }

    private long getTabletServerEpoch(int tabletServerId) throws Exception {
        TabletServerRegistration registration =
                waitValue(
                        () -> zkClient.getTabletServer(tabletServerId),
                        Duration.ofMinutes(1),
                        "Tablet server registration is not ready");
        return registration.getRegisterTimestamp();
    }

    private void assertClusterHealthGreen() throws Exception {
        assertThat(coordinatorGateway.getClusterHealth(new GetClusterHealthRequest()).get())
                .satisfies(
                        health -> {
                            assertThat(health.getStatus()).isZero();
                            assertThat(health.getActiveLeaderReplicas())
                                    .isEqualTo(health.getNumLeaderReplicas());
                        });
    }

    private List<Integer> waitAndGetIsr(TableBucket tableBucket) {
        LeaderAndIsr leaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tableBucket),
                        Duration.ofMinutes(1),
                        "leaderAndIsr is not ready");
        return leaderAndIsr.isr();
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }
}

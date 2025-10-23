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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The ITCase for tabletServer shutdown (controlled shutdown). */
public class TabletServerShutdownITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIOExceptionShouldStopTabletServer(boolean isLogTable) throws Exception {
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        Schema schema =
                isLogTable
                        ? Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.STRING())
                                .build()
                        : Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.STRING())
                                .primaryKey("a")
                                .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();

        TablePath tablePath =
                TablePath.of(
                        "test_failover", "test_ioexception_table_" + (isLogTable ? "log" : "pk"));
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // delete the active segment, which will cause IOException when append log/changelog
        LogSegment logSegment =
                FLUSS_CLUSTER_EXTENSION
                        .waitAndGetLeaderReplica(tb)
                        .getLogTablet()
                        .activeLogSegment();
        logSegment.deleteIfExists();

        // should get RetriableException since the leader server is shutdown
        // and new Leader will be on new server
        assertThatThrownBy(() -> writeData(leaderGateWay, tableId, isLogTable))
                .cause()
                .isInstanceOf(RetriableException.class);

        // should only has 2 tablet servers
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(2);

        // restart the shutdown server
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leader, true);
    }

    @Test
    void testControlledShutdownConfiguration() throws Exception {
        // Test that the controlled shutdown configuration options are properly loaded
        Configuration conf = new Configuration();

        // Verify default values are loaded correctly
        assertThat(conf.getInt(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES))
                .isEqualTo(3);
        assertThat(
                        conf.get(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_RETRY_INTERVAL)
                                .toMillis())
                .isEqualTo(1000L);

        // Test custom configuration values
        Configuration customConf = new Configuration();
        customConf.set(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES, 5);
        customConf.set(
                ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_RETRY_INTERVAL,
                Duration.ofMillis(2000));

        assertThat(customConf.getInt(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES))
                .isEqualTo(5);
        assertThat(
                        customConf
                                .get(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_RETRY_INTERVAL)
                                .toMillis())
                .isEqualTo(2000L);
    }

    @Test
    void testControlledShutdown() throws Exception {
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();
        TablePath tablePath = TablePath.of("test_shutdown", "test_controlled_shutdown");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr leaderAndIsr = FLUSS_CLUSTER_EXTENSION.waitLeaderAndIsrReady(tb);
        int leader = leaderAndIsr.leader();

        // test kill the tabletServers with leader on.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(leader);
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();

        // the leader should be removed from isr, and new leader should be elected.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .map(LeaderAndIsr::leader)
                                .isNotEqualTo(leader));

        // restart the shutdown server
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leader, true);
    }

    @Test
    void testControlledShutdownRetriesFailover() throws Exception {
        // This case is to test the scenario that the controlled shutdown request is retried and
        // failed by cannot elect any new leader. In this case the controlled shutdown will finally
        // go uncontrolled shutdown.
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 2)
                        .build();
        TablePath tablePath = TablePath.of("test_failover", "test_controlled_shutdown_failed");
        long tableId = createTable(FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr leaderAndIsr = FLUSS_CLUSTER_EXTENSION.waitLeaderAndIsrReady(tb);
        List<Integer> isr = new ArrayList<>(leaderAndIsr.isr());
        int leader = leaderAndIsr.leader();
        isr.remove(Integer.valueOf(leader));
        int follower = isr.get(0);

        // Let's kil follower. Will go controlled shutdown.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(follower);
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();

        // the follower should be removed from isr
        LeaderAndIsr expectedLeaderAndIsr1 =
                leaderAndIsr.newLeaderAndIsr(Collections.singletonList(leader));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb).get())
                                .isEqualTo(expectedLeaderAndIsr1));

        // kill the leader. As we only have 1 replica, no leader can be elected as we send the
        // controlled shutdown request to the leader. So the controlled shutdown will finally go
        // uncontrolled shutdown.
        FLUSS_CLUSTER_EXTENSION.stopTabletServer(leader);

        // should be no leader
        LeaderAndIsr expectedLeaderAndIsr2 =
                expectedLeaderAndIsr1.newLeaderAndIsr(
                        LeaderAndIsr.NO_LEADER, Collections.singletonList(leader));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb).get())
                                .isEqualTo(expectedLeaderAndIsr2));

        // start the follower
        // should still be no leader since the follower is out of isr, should be elected as leader
        FLUSS_CLUSTER_EXTENSION.startTabletServer(follower);

        // start the leader server, the leader should be the previous leader server
        FLUSS_CLUSTER_EXTENSION.startTabletServer(leader);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zkClient.getLeaderAndIsr(tb).get().leader()).isEqualTo(leader));
    }

    private void writeData(
            TabletServerGateway tabletServerGateway, long tableId, boolean isLogTable)
            throws Exception {
        if (isLogTable) {
            tabletServerGateway
                    .produceLog(
                            newProduceLogRequest(tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                    .get();
        } else {
            tabletServerGateway
                    .putKv(
                            newPutKvRequest(
                                    tableId, 0, 1, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                    .get();
        }
    }
}

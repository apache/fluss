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
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.GetClusterHealthRequest;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.ReplicaDeletionIneligible;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for resuming replica deletion after a TabletServer restarts. */
public class ReplicaDeletionRecoveryITCase {
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
    void testResumeTableDeletionAfterTabletServerRestart() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        TablePath tablePath = TablePath.of("test_db_replica_deletion", "test_orphan_table");
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        List<Integer> isr = waitAndGetIsr(tb);

        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        leaderGateway.produceLog(newProduceLogRequest(tableId, 0, 1, records)).get();

        int offlineServerId = isr.get(0);
        assertThat(zkClient.getTableAssignment(tableId).get().getBucketAssignment(0).getReplicas())
                .contains(offlineServerId);
        ReplicaManager replicaManager =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(offlineServerId).getReplicaManager();
        Replica replica = replicaManager.getReplicaOrException(tb);
        Path offlineTsTableDir = replica.getTabletParentDir();
        File offlineTsLogDir = replica.getLogTablet().getLogDir();
        assertThat(offlineTsTableDir).exists();
        assertThat(offlineTsLogDir).exists();
        assertThat(offlineTsLogDir.listFiles()).isNotEmpty();

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();
        waitUntilReplicaDeletionIneligible(tb, offlineServerId);

        FLUSS_CLUSTER_EXTENSION.startTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(4);

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(offlineTsLogDir).doesNotExist();
                    assertThat(offlineTsTableDir).doesNotExist();
                    assertThat(zkClient.getTableAssignment(tableId)).isEmpty();
                    assertClusterHealthGreen();
                });
    }

    @Test
    void testResumePartitionDeletionAfterTabletServerRestart() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        TablePath tablePath = TablePath.of("test_db_replica_deletion", "test_orphan_partition");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);

        String partitionName = "p1";
        long partitionId =
                RpcMessageTestUtils.createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("b", partitionName)),
                        false);
        TableBucket tb = new TableBucket(tableId, partitionId, 0);
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        List<Integer> isr = waitAndGetIsr(tb);

        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        leaderGateway.produceLog(newProduceLogRequest(tableId, 0, 1, records)).get();

        int offlineServerId = isr.get(0);
        assertThat(
                        zkClient.getPartitionAssignment(partitionId)
                                .get()
                                .getBucketAssignment(0)
                                .getReplicas())
                .contains(offlineServerId);
        ReplicaManager replicaManager =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(offlineServerId).getReplicaManager();
        Replica replica = replicaManager.getReplicaOrException(tb);
        Path offlineTsPartitionDir = replica.getTabletParentDir();
        File offlineTsLogDir = replica.getLogTablet().getLogDir();
        assertThat(offlineTsPartitionDir).exists();
        assertThat(offlineTsLogDir).exists();
        assertThat(offlineTsLogDir.listFiles()).isNotEmpty();

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        coordinatorGateway
                .dropPartition(
                        RpcMessageTestUtils.newDropPartitionRequest(
                                tablePath,
                                new PartitionSpec(Collections.singletonMap("b", partitionName)),
                                false))
                .get();
        waitUntilReplicaDeletionIneligible(tb, offlineServerId);

        FLUSS_CLUSTER_EXTENSION.startTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(4);

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(offlineTsLogDir).doesNotExist();
                    assertThat(offlineTsPartitionDir).doesNotExist();
                    assertThat(zkClient.getPartitionAssignment(partitionId)).isEmpty();
                    assertClusterHealthGreen();
                });
    }

    private void waitUntilReplicaDeletionIneligible(TableBucket tb, int offlineServerId) {
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(
                                        FLUSS_CLUSTER_EXTENSION
                                                .getCoordinatorServer()
                                                .getCoordinatorEventProcessor()
                                                .getCoordinatorContext()
                                                .getReplicaState(
                                                        new TableBucketReplica(
                                                                tb, offlineServerId)))
                                .isEqualTo(ReplicaDeletionIneligible));
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

    private List<Integer> waitAndGetIsr(TableBucket tb) {
        LeaderAndIsr leaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
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

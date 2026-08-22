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
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for stop replica. */
public class StopReplicaITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;
    private CoordinatorGateway coordinatorGateway;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        coordinatorGateway = FLUSS_CLUSTER_EXTENSION.newCoordinatorClient();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testStopReplica(boolean isPkTable) throws Exception {
        TablePath tablePath = isPkTable ? DATA1_TABLE_PATH_PK : DATA1_TABLE_PATH;
        TableDescriptor tableDescriptor =
                isPkTable ? DATA1_TABLE_DESCRIPTOR_PK : DATA1_TABLE_DESCRIPTOR;

        // wait until all the gateway has same metadata because the follower fetcher manager need
        // to get the leader address from server metadata while make follower.
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        List<Integer> isr = waitAndGetIsr(tb);
        List<Path> tableDirs = assertReplicaExistAndGetTableOrPartitionDirs(tb, isr, isPkTable);
        // drop table.
        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        retryUtilReplicaNotExist(tb, isr, tableDirs);
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        // create this table again.
        tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb1 = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb1);

        isr = waitAndGetIsr(tb1);
        tableDirs = assertReplicaExistAndGetTableOrPartitionDirs(tb1, isr, isPkTable);
        // drop table.
        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        // create this table even if the drop table operation may not be completed.
        tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb2 = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb2);

        List<Integer> isr2 = waitAndGetIsr(tb2);
        List<Path> tableDirs2 = assertReplicaExistAndGetTableOrPartitionDirs(tb2, isr2, isPkTable);
        // drop table.
        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        retryUtilReplicaNotExist(tb, isr, tableDirs);
        retryUtilReplicaNotExist(tb, isr2, tableDirs2);
    }

    @Test
    void testDropTableCleansOrphanDirsOnTabletServerRestart() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        TablePath tablePath = TablePath.of("test_db_stop_replica", "test_orphan_table");
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        List<Integer> isr = waitAndGetIsr(tb);

        // Write data so that actual segment files exist on every replica.
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        leaderGateway.produceLog(newProduceLogRequest(tableId, 0, 1, records)).get();

        int offlineServerId = isr.get(0);
        ReplicaManager replicaManager =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(offlineServerId).getReplicaManager();
        Replica replica = replicaManager.getReplicaOrException(tb);
        Path offlineTsTableDir = replica.getTabletParentDir();
        File offlineTsLogDir = replica.getLogTablet().getLogDir();
        assertThat(offlineTsTableDir).exists();
        assertThat(offlineTsLogDir).exists();
        // Verify actual data files (.log, .index) exist under the log directory.
        assertThat(offlineTsLogDir.listFiles()).isNotEmpty();

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(2);

        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();

        FLUSS_CLUSTER_EXTENSION.startTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(offlineTsLogDir).doesNotExist();
                    assertThat(offlineTsTableDir).doesNotExist();
                });
    }

    /**
     * when a tablet server is already offline at the time {@code dropTable} is invoked, deletion
     * must NOT throw and must NOT get stuck. Replicas on the dead tablet server should be parked in
     * {@code ReplicaDeletionIneligible} and the table marked ineligible; once the tablet server
     * reconnects, deletion completes.
     */
    @Test
    void testDropTableNotStuckWhenTabletServerDeadAtKickoff() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        TablePath tablePath = TablePath.of("test_db_stop_replica", "test_drop_dead_kickoff");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        List<Integer> isr = waitAndGetIsr(tb);
        List<Path> tableDirs = assertReplicaExistAndGetTableOrPartitionDirs(tb, isr, false);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        int offlineServerId =
                isr.stream().filter(id -> id != leader).findFirst().orElse(isr.get(0));

        ReplicaManager replicaManager =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(offlineServerId).getReplicaManager();
        Replica replica = replicaManager.getReplicaOrException(tb);
        Path offlineTsTableDir = replica.getTabletParentDir();
        File offlineTsLogDir = replica.getLogTablet().getLogDir();

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(2);

        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        FLUSS_CLUSTER_EXTENSION.startTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(offlineTsLogDir).doesNotExist();
                    assertThat(offlineTsTableDir).doesNotExist();
                });
    }

    /**
     * Verifies that the {@code tableCount} metric (driven by {@code
     * coordinatorContext.allTables().size()}) correctly decreases after a table is dropped, even
     * when a tablet server is temporarily offline and the deletion is initially paused.
     */
    @Test
    void testTableCountMetricDecreasesAfterDropWithOfflineTabletServer() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        CoordinatorContext coordinatorContext =
                FLUSS_CLUSTER_EXTENSION
                        .getCoordinatorServer()
                        .getCoordinatorEventProcessor()
                        .getCoordinatorContext();

        int initialTableCount = coordinatorContext.allTables().size();

        TablePath tablePath = TablePath.of("test_db_stop_replica", "test_table_count_metric");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();
        long tableId =
                RpcMessageTestUtils.createTable(
                        FLUSS_CLUSTER_EXTENSION, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        assertThat(coordinatorContext.allTables().size()).isEqualTo(initialTableCount + 1);

        List<Integer> isr = waitAndGetIsr(tb);

        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        int offlineServerId =
                isr.stream().filter(id -> id != leader).findFirst().orElse(isr.get(0));

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(2);

        coordinatorGateway
                .dropTable(
                        RpcMessageTestUtils.newDropTableRequest(
                                tablePath.getDatabaseName(), tablePath.getTableName(), false))
                .get();
        assertThat(zkClient.tableExist(tablePath)).isFalse();

        FLUSS_CLUSTER_EXTENSION.startTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(coordinatorContext.allTables().size())
                                .as(
                                        "tableCount should return to initial value after "
                                                + "deletion completes")
                                .isEqualTo(initialTableCount));
    }

    @Test
    void testDropPartitionCleansOrphanDirsOnTabletServerRestart() throws Exception {
        FLUSS_CLUSTER_EXTENSION.waitUntilAllGatewayHasSameMetadata();

        TablePath tablePath = TablePath.of("test_db_stop_replica", "test_orphan_partition");

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

        // Create one partition explicitly (auto-partition is disabled for this descriptor).
        String partitionName = "p1";
        long partitionId =
                RpcMessageTestUtils.createPartition(
                        FLUSS_CLUSTER_EXTENSION,
                        tablePath,
                        new PartitionSpec(Collections.singletonMap("b", partitionName)),
                        false);
        TableBucket tb = new TableBucket(tableId, partitionId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);

        List<Integer> isr = waitAndGetIsr(tb);

        // Write data so that actual segment files exist on every replica.
        int leader = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);
        MemoryLogRecords records = genMemoryLogRecordsByObject(DATA1);
        leaderGateway.produceLog(newProduceLogRequest(tableId, 0, 1, records)).get();

        int offlineServerId = isr.get(0);
        ReplicaManager replicaManager =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(offlineServerId).getReplicaManager();
        Replica replica = replicaManager.getReplicaOrException(tb);
        Path offlineTsPartitionDir = replica.getTabletParentDir();
        File offlineTsLogDir = replica.getLogTablet().getLogDir();
        assertThat(offlineTsPartitionDir).exists();
        assertThat(offlineTsLogDir).exists();
        assertThat(offlineTsLogDir.listFiles()).isNotEmpty();

        FLUSS_CLUSTER_EXTENSION.stopTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(2);

        coordinatorGateway
                .dropPartition(
                        RpcMessageTestUtils.newDropPartitionRequest(
                                tablePath,
                                new PartitionSpec(Collections.singletonMap("b", partitionName)),
                                false))
                .get();

        FLUSS_CLUSTER_EXTENSION.startTabletServer(offlineServerId);
        FLUSS_CLUSTER_EXTENSION.assertHasTabletServerNumber(3);

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(offlineTsLogDir).doesNotExist();
                    assertThat(offlineTsPartitionDir).doesNotExist();
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

    private List<Path> assertReplicaExistAndGetTableOrPartitionDirs(
            TableBucket tableBucket, List<Integer> isr, boolean isKvTable) {
        List<Path> tableDirs = new ArrayList<>();
        for (int replicaId : isr) {
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(replicaId).getReplicaManager();
            assertThat(replicaManager.getReplica(tableBucket))
                    .isInstanceOf(ReplicaManager.OnlineReplica.class);
            Replica replica = replicaManager.getReplicaOrException(tableBucket);
            Path dir = replica.getTabletParentDir();
            assertThat(dir).exists();
            tableDirs.add(dir);

            assertThat(replica.getLogTablet().getLogDir()).exists();
            if (isKvTable) {
                // wait the replica become leader, so that we can get the kv tablet
                Replica kvReplica =
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(replica.getTableBucket());
                assertThat(kvReplica.getKvTablet()).isNotNull();
                assertThat(kvReplica.getKvTablet().getKvTabletDir()).exists();
            }
        }

        return tableDirs;
    }

    private void retryUtilReplicaNotExist(
            TableBucket tableBucket, List<Integer> isr, List<Path> tableDirs) {
        retry(
                Duration.ofMinutes(1),
                () -> {
                    // the local table dir should be removed
                    tableDirs.forEach(tableDir -> assertThat(tableDir).doesNotExist());

                    // Replicas in TabletServers should be shutdown
                    isr.forEach(
                            replicaId -> {
                                ReplicaManager replicaManager =
                                        FLUSS_CLUSTER_EXTENSION
                                                .getTabletServerById(replicaId)
                                                .getReplicaManager();
                                assertThat(replicaManager.getReplica(tableBucket))
                                        .isInstanceOf(ReplicaManager.NoneReplica.class);
                            });

                    // at last, when all Replicas are shutdown, the zk data should be removed.
                    assertThat(zkClient.getTableAssignment(tableBucket.getTableId())).isEmpty();
                    assertThat(zkClient.getLeaderAndIsr(tableBucket)).isEmpty();
                });
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        return conf;
    }
}

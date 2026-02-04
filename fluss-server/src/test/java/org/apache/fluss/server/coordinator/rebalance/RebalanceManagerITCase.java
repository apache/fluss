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

package org.apache.fluss.server.coordinator.rebalance;

import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AddServerTagRequest;
import org.apache.fluss.server.coordinator.CoordinatorContext;
import org.apache.fluss.server.coordinator.rebalance.model.ClusterModel;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.log.remote.RemoteLogManifest;
import org.apache.fluss.server.log.remote.RemoteLogTablet;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketAssignment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.coordinator.statemachine.ReplicaState.OnlineReplica;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** IT test for {@link RebalanceManager}. */
public class RebalanceManagerITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(6)
                    .setClusterConf(initConfig())
                    .build();

    private ZooKeeperClient zkClient;
    private RebalanceManager rebalanceManager;

    @BeforeEach
    void beforeEach() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        rebalanceManager = FLUSS_CLUSTER_EXTENSION.getRebalanceManager();
    }

    @Test
    void testBuildClusterModel() throws Exception {
        // one none-partitioned table.
        long tableId1 =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        // one partitioned table.
        TablePath partitionTablePath = TablePath.of("test_db_1", "test_partition_table_1");
        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build();
        long tableId2 =
                createTable(FLUSS_CLUSTER_EXTENSION, partitionTablePath, partitionTableDescriptor);
        String partitionName1 = "b1";
        createPartition(
                FLUSS_CLUSTER_EXTENSION,
                partitionTablePath,
                new PartitionSpec(Collections.singletonMap("b", partitionName1)),
                false);

        ClusterModel clusterModel = rebalanceManager.buildClusterModel();
        assertThat(clusterModel.servers().size()).isEqualTo(6);
        assertThat(clusterModel.aliveServers().size()).isEqualTo(6);
        assertThat(clusterModel.offlineServers().size()).isEqualTo(0);
        assertThat(clusterModel.tables().size()).isEqualTo(2);
        assertThat(clusterModel.tables()).contains(tableId1, tableId2);

        // offline one table.
        AddServerTagRequest request =
                new AddServerTagRequest().setServerTag(ServerTag.PERMANENT_OFFLINE.value);
        request.addServerId(0);
        FLUSS_CLUSTER_EXTENSION.newCoordinatorClient().addServerTag(request).get();

        clusterModel = rebalanceManager.buildClusterModel();
        assertThat(clusterModel.servers().size()).isEqualTo(6);
        assertThat(clusterModel.aliveServers().size()).isEqualTo(5);
        assertThat(clusterModel.offlineServers().size()).isEqualTo(1);
        assertThat(clusterModel.tables().size()).isEqualTo(2);
        assertThat(clusterModel.tables()).contains(tableId1, tableId2);
    }

    @Test
    void testRebalanceWithRemoteLog() throws Exception {
        TableBucket tb = setupTableBucket();
        long tableId = tb.getTableId();

        int leaderId = FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb);
        TabletServerGateway leaderGateway =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leaderId);

        // produce test records
        produceRecordsAndWaitRemoteLogCopy(leaderGateway, tb, 0L);
        // test metadata updated: verify manifest in metadata
        TabletServer tabletServer = FLUSS_CLUSTER_EXTENSION.getTabletServerById(leaderId);
        RemoteLogManager remoteLogManager = tabletServer.getReplicaManager().getRemoteLogManager();
        RemoteLogTablet remoteLogTablet = remoteLogManager.remoteLogTablet(tb);

        RemoteLogManifest manifest = remoteLogTablet.currentManifest();
        assertThat(manifest.getPhysicalTablePath().getTablePath()).isEqualTo(DATA1_TABLE_PATH);
        assertThat(manifest.getTableBucket()).isEqualTo(tb);
        int remoteLogSize = manifest.getRemoteLogSegmentList().size();
        assertThat(remoteLogSize).isGreaterThan(0);

        // try to trigger rebalance. for example, if current assignment is [0, 1, 2], try to trigger
        // rebalance to [3, 4, 5]
        RebalancePlanForBucket planForBucket = buildRebalancePlanForBucket(tb);

        CoordinatorContext coordinatorContext =
                FLUSS_CLUSTER_EXTENSION
                        .getCoordinatorServer()
                        .getCoordinatorEventProcessor()
                        .getCoordinatorContext();
        planForBucket
                .getOriginReplicas()
                .forEach(
                        replicaId ->
                                assertThat(
                                                coordinatorContext.getReplicaState(
                                                        new TableBucketReplica(tb, replicaId)))
                                        .isEqualTo(OnlineReplica));
        planForBucket
                .getNewReplicas()
                .forEach(
                        replicaId ->
                                assertThat(
                                                coordinatorContext.getReplicaState(
                                                        new TableBucketReplica(tb, replicaId)))
                                        .isNull());

        rebalanceManager.registerRebalance(
                "test-rebalance-dsds",
                Collections.singletonMap(tb, planForBucket),
                RebalanceStatus.NOT_STARTED);

        retry(
                Duration.ofMinutes(2),
                () -> {
                    // assignment changed.
                    Set<Integer> newReplicaSet = new HashSet<>(planForBucket.getNewReplicas());
                    BucketAssignment bucketAssignment =
                            zkClient.getTableAssignment(tableId)
                                    .get()
                                    .getBucketAssignment(tb.getBucket());
                    List<Integer> replicas = bucketAssignment.getReplicas();
                    assertThat(newReplicaSet.size()).isEqualTo(replicas.size());
                    assertThat(newReplicaSet.containsAll(replicas)).isTrue();

                    // leader changed.
                    int newLeader = planForBucket.getNewLeader();
                    assertThat(FLUSS_CLUSTER_EXTENSION.waitAndGetLeader(tb)).isEqualTo(newLeader);

                    // origin replicas all set to offline.
                    List<Integer> originReplicas = planForBucket.getOriginReplicas();
                    for (int originReplica : originReplicas) {
                        TabletServer ts =
                                FLUSS_CLUSTER_EXTENSION.getTabletServerById(originReplica);
                        ReplicaManager rm = ts.getReplicaManager();
                        assertThat(rm.getReplica(tb))
                                .isInstanceOf(ReplicaManager.NoneReplica.class);

                        // replica state changes.
                        planForBucket
                                .getOriginReplicas()
                                .forEach(
                                        replicaId ->
                                                assertThat(
                                                                coordinatorContext.getReplicaState(
                                                                        new TableBucketReplica(
                                                                                tb, replicaId)))
                                                        .isNull());
                        planForBucket
                                .getNewReplicas()
                                .forEach(
                                        replicaId ->
                                                assertThat(
                                                                coordinatorContext.getReplicaState(
                                                                        new TableBucketReplica(
                                                                                tb, replicaId)))
                                                        .isEqualTo(OnlineReplica));
                    }
                });
        // remote log not be deleted.
        int newLeader = planForBucket.getNewLeader();
        TabletServer leaderTs = FLUSS_CLUSTER_EXTENSION.getTabletServerById(newLeader);
        RemoteLogManager leaderRm = leaderTs.getReplicaManager().getRemoteLogManager();
        RemoteLogTablet leaderRlt = leaderRm.remoteLogTablet(tb);

        RemoteLogManifest newManifest = leaderRlt.currentManifest();
        assertThat(newManifest.getPhysicalTablePath().getTablePath()).isEqualTo(DATA1_TABLE_PATH);
        assertThat(newManifest.getTableBucket()).isEqualTo(tb);
        assertThat(newManifest.getRemoteLogSegmentList().size()).isEqualTo(remoteLogSize);
    }

    private TableBucket setupTableBucket() throws Exception {
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        return tb;
    }

    private RebalancePlanForBucket buildRebalancePlanForBucket(TableBucket tb) throws Exception {
        long tableId = tb.getTableId();
        BucketAssignment bucketAssignment =
                zkClient.getTableAssignment(tableId).get().getBucketAssignment(tb.getBucket());
        List<Integer> replicas = bucketAssignment.getReplicas();
        Set<Integer> replicasSet = new HashSet<>(bucketAssignment.getReplicas());
        int originLeader = zkClient.getLeaderAndIsr(tb).get().leader();
        List<Integer> newReplicas = new ArrayList<>();
        int newLeader = originLeader;
        for (int i = 0; i < 6; i++) {
            if (!replicasSet.contains(i)) {
                newReplicas.add(i);
                if (i != originLeader && newLeader == originLeader) {
                    newLeader = i;
                }
            }
        }

        return new RebalancePlanForBucket(tb, originLeader, newLeader, replicas, newReplicas);
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 1);
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);

        // set a shorter interval for testing purpose
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1kb"));

        // set a shorter max log time to allow replica shrink from isr. Don't be too low, otherwise
        // normal follower synchronization will also be affected
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(5));
        return conf;
    }

    private void produceRecordsAndWaitRemoteLogCopy(
            TabletServerGateway leaderGateway, TableBucket tb, long baseOffset) throws Exception {
        for (int i = 0; i < 10; i++) {
            assertProduceLogResponse(
                    leaderGateway
                            .produceLog(
                                    newProduceLogRequest(
                                            tb.getTableId(),
                                            0,
                                            1,
                                            genMemoryLogRecordsByObject(DATA1)))
                            .get(),
                    0,
                    baseOffset + i * 10L);
        }
        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(
                new TableBucket(tb.getTableId(), 0));
    }
}

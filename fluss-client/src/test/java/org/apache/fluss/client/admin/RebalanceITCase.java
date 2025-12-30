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

package org.apache.fluss.client.admin;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.cluster.rebalance.GoalType;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.RebalancePlan;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for rebalance. */
public class RebalanceITCase {
    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(4)
                    .setClusterConf(initConfig())
                    .build();

    private Connection conn;
    private Admin admin;

    @BeforeEach
    protected void setup() throws Exception {
        conn = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = conn.getAdmin();
    }

    @AfterEach
    protected void teardown() throws Exception {
        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().deleteRebalancePlan();

        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @Test
    void testRebalanceForLogTable() throws Exception {
        String dbName = "db-balance";
        admin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();

        TableDescriptor logDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .property(
                                ConfigOptions.TABLE_GENERATE_UNBALANCE_TABLE_ASSIGNMENT.key(),
                                "true")
                        .build();
        // create some none partitioned log table.
        for (int i = 0; i < 6; i++) {
            long tableId =
                    createTable(
                            new TablePath(dbName, "test-rebalance_table-" + i),
                            logDescriptor,
                            false);
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        }

        // create some partitioned table with partition.
        TableDescriptor partitionedDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .property(
                                ConfigOptions.TABLE_GENERATE_UNBALANCE_TABLE_ASSIGNMENT.key(),
                                "true")
                        .partitionedBy("b")
                        .build();
        for (int i = 0; i < 3; i++) {
            TablePath tablePath = new TablePath(dbName, "test-rebalance_partitioned_table-" + i);
            long tableId = createTable(tablePath, partitionedDescriptor, false);
            for (int j = 0; j < 2; j++) {
                PartitionSpec partitionSpec =
                        new PartitionSpec(Collections.singletonMap("b", String.valueOf(j)));
                admin.createPartition(tablePath, partitionSpec, false).get();
                long partitionId =
                        admin.listPartitionInfos(tablePath, partitionSpec)
                                .get()
                                .get(0)
                                .getPartitionId();
                FLUSS_CLUSTER_EXTENSION.waitUntilTablePartitionReady(tableId, partitionId);
            }
        }

        // verify before rebalance. As we use unbalance assignment, all replicas will be location on
        // servers [0,1,2], all leader will be location on server 0.
        for (int i = 0; i < 3; i++) {
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(i).getReplicaManager();
            assertThat(replicaManager.onlineReplicas().count()).isEqualTo(36);
            if (i == 0) {
                assertThat(replicaManager.leaderCount()).isEqualTo(36);
            } else {
                assertThat(replicaManager.leaderCount()).isEqualTo(0);
            }
        }
        ReplicaManager replicaManager3 =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(3).getReplicaManager();
        assertThat(replicaManager3.onlineReplicas().count()).isEqualTo(0);
        assertThat(replicaManager3.leaderCount()).isEqualTo(0);

        // trigger rebalance with goal set[ReplicaDistributionGoal, LeaderReplicaDistributionGoal]
        admin.rebalance(
                        Arrays.asList(
                                GoalType.REPLICA_DISTRIBUTION_GOAL,
                                GoalType.LEADER_DISTRIBUTION_GOAL),
                        false)
                .get();

        retry(
                Duration.ofMinutes(2),
                () -> {
                    // TODO use admin#listRebalanceProcess to verify rebalance is finished.
                    Optional<RebalancePlan> rebalancePlan =
                            FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getRebalancePlan();
                    assertThat(rebalancePlan).isPresent();
                    assertThat(rebalancePlan.get().getRebalanceStatus())
                            .isEqualTo(RebalanceStatus.COMPLETED);
                    for (int i = 0; i < 4; i++) {
                        ReplicaManager replicaManager =
                                FLUSS_CLUSTER_EXTENSION.getTabletServerById(i).getReplicaManager();
                        // average will be 27
                        assertThat(replicaManager.onlineReplicas().count()).isBetween(24L, 30L);
                        long leaderCount = replicaManager.leaderCount();
                        // average will be 9
                        assertThat(leaderCount).isBetween(7L, 11L);
                    }
                });

        // add server tag PERMANENT_OFFLINE for server 3, trigger all leader and replica removed
        // from server 3.
        admin.addServerTag(Collections.singletonList(3), ServerTag.PERMANENT_OFFLINE).get();
        admin.rebalance(
                        Arrays.asList(
                                GoalType.REPLICA_DISTRIBUTION_GOAL,
                                GoalType.LEADER_DISTRIBUTION_GOAL),
                        false)
                .get();
        retry(
                Duration.ofMinutes(2),
                () -> {
                    // TODO use admin#listRebalanceProcess to verify rebalance is finished.
                    Optional<RebalancePlan> rebalancePlan =
                            FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().getRebalancePlan();
                    assertThat(rebalancePlan).isPresent();
                    assertThat(rebalancePlan.get().getRebalanceStatus())
                            .isEqualTo(RebalanceStatus.COMPLETED);
                    assertThat(replicaManager3.onlineReplicas().count()).isEqualTo(0);
                    assertThat(replicaManager3.leaderCount()).isEqualTo(0);
                    for (int i = 0; i < 3; i++) {
                        ReplicaManager replicaManager =
                                FLUSS_CLUSTER_EXTENSION.getTabletServerById(i).getReplicaManager();
                        // average will be 36
                        assertThat(replicaManager.onlineReplicas().count()).isBetween(34L, 38L);
                        long leaderCount = replicaManager.leaderCount();
                        // average will be 12
                        assertThat(leaderCount).isBetween(10L, 14L);
                    }
                });
    }

    @Test
    void testListRebalanceProcess() throws Exception {
        RebalanceProgress rebalanceProgress = admin.listRebalanceProgress().get();
        assertThat(rebalanceProgress.progress()).isEqualTo(-1d);
        assertThat(rebalanceProgress.status()).isEqualTo(RebalanceStatus.NO_TASK);
        assertThat(rebalanceProgress.progressForBucketMap()).isEmpty();

        String dbName = "db-rebalance-list";
        admin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();

        TableDescriptor logDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .property(
                                ConfigOptions.TABLE_GENERATE_UNBALANCE_TABLE_ASSIGNMENT.key(),
                                "true")
                        .build();
        // create some none partitioned log table.
        for (int i = 0; i < 6; i++) {
            long tableId =
                    createTable(
                            new TablePath(dbName, "test-rebalance_table-" + i),
                            logDescriptor,
                            false);
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        }

        // trigger rebalance with goal set[ReplicaDistributionGoal, LeaderReplicaDistributionGoal]
        org.apache.fluss.client.admin.RebalancePlan rebalancePlan =
                admin.rebalance(
                                Arrays.asList(
                                        GoalType.REPLICA_DISTRIBUTION_GOAL,
                                        GoalType.LEADER_DISTRIBUTION_GOAL),
                                false)
                        .get();
        retry(
                Duration.ofMinutes(2),
                () -> {
                    RebalanceProgress progress = admin.listRebalanceProgress().get();
                    assertThat(progress.progress()).isEqualTo(1d);
                    assertThat(progress.status()).isEqualTo(RebalanceStatus.COMPLETED);
                    Map<TableBucket, RebalanceResultForBucket> processForBuckets =
                            progress.progressForBucketMap();
                    Map<TableBucket, RebalancePlanForBucket> planForBuckets =
                            rebalancePlan.getPlanForBucketMap();
                    assertThat(planForBuckets.size()).isEqualTo(processForBuckets.size());
                    for (TableBucket tableBucket : planForBuckets.keySet()) {
                        RebalanceResultForBucket processForBucket =
                                processForBuckets.get(tableBucket);
                        assertThat(processForBucket.status()).isEqualTo(RebalanceStatus.COMPLETED);
                        assertThat(processForBucket.plan())
                                .isEqualTo(planForBuckets.get(tableBucket));
                    }
                });

        // cancel rebalance.
        admin.cancelRebalance().get();

        RebalanceProgress progress = admin.listRebalanceProgress().get();
        assertThat(progress.progress()).isEqualTo(1d);
        assertThat(progress.status()).isEqualTo(RebalanceStatus.CANCELED);
    }

    private static Configuration initConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        configuration.set(ConfigOptions.DEFAULT_BUCKET_NUMBER, 3);
        return configuration;
    }

    private long createTable(
            TablePath tablePath, TableDescriptor tableDescriptor, boolean ignoreIfExists)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, ignoreIfExists).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }
}

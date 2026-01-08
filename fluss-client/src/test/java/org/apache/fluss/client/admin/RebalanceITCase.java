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
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NoRebalanceInProgressException;
import org.apache.fluss.exception.RebalanceFailureException;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        admin.cancelRebalance(null).get();
        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().deleteRebalanceTask();

        if (admin != null) {
            admin.close();
            admin = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    // TODO add test for primary key table, trace by https://github.com/apache/fluss/issues/2315

    @Test
    void testRebalanceForLogTable() throws Exception {
        String dbName = "db-balance";
        admin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();

        // add server tag PERMANENT_OFFLINE for server 3, this will avoid to generate bucket
        // assignment on server 3 when create table.
        admin.addServerTag(Collections.singletonList(3), ServerTag.PERMANENT_OFFLINE).get();

        // create some none partitioned log table.
        for (int i = 0; i < 2; i++) {
            long tableId =
                    createTable(
                            new TablePath(dbName, "test-rebalance_table-" + i),
                            DATA1_TABLE_DESCRIPTOR);
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        }

        // create one partitioned table with two partitions.
        TableDescriptor partitionedDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .partitionedBy("b")
                        .build();
        TablePath tablePath = new TablePath(dbName, "test-rebalance_partitioned_table1");
        long tableId = createTable(tablePath, partitionedDescriptor);
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

        // verify before rebalance. As we use unbalance assignment, all replicas will not be
        // location on servers 3.
        for (int i = 0; i < 3; i++) {
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(i).getReplicaManager();
            assertThat(replicaManager.onlineReplicas().count()).isGreaterThan(0);
            assertThat(replicaManager.leaderCount()).isGreaterThan(0);
        }
        ReplicaManager replicaManager3 =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(3).getReplicaManager();
        assertThat(replicaManager3.onlineReplicas().count()).isEqualTo(0);
        assertThat(replicaManager3.leaderCount()).isEqualTo(0);

        // remove tag after crated table.
        admin.removeServerTag(Collections.singletonList(3), ServerTag.PERMANENT_OFFLINE).get();

        // trigger rebalance with goal set[ReplicaDistributionGoal, LeaderReplicaDistributionGoal]
        String rebalanceId =
                admin.rebalance(
                                Arrays.asList(
                                        GoalType.REPLICA_DISTRIBUTION,
                                        GoalType.LEADER_DISTRIBUTION))
                        .get();

        retry(
                Duration.ofMinutes(2),
                () -> {
                    Optional<RebalanceProgress> progressOpt =
                            admin.listRebalanceProgress(null).get();
                    assertThat(progressOpt).isPresent();
                    assertThat(rebalanceId).isEqualTo(progressOpt.get().rebalanceId());
                    assertThat(progressOpt.get().status()).isEqualTo(RebalanceStatus.COMPLETED);
                });
        for (int i = 0; i < 4; i++) {
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(i).getReplicaManager();
            // TODO Change to an reasonable assertion way.
            // average will be 9
            assertThat(replicaManager.onlineReplicas().count()).isBetween(6L, 12L);
            long leaderCount = replicaManager.leaderCount();
            // average will be 3
            assertThat(leaderCount).isBetween(1L, 6L);
        }

        // add server tag PERMANENT_OFFLINE for server 0, trigger all leader and replica removed
        // from server 3.
        admin.addServerTag(Collections.singletonList(0), ServerTag.PERMANENT_OFFLINE).get();
        String rebalanceId2 =
                admin.rebalance(
                                Arrays.asList(
                                        GoalType.REPLICA_DISTRIBUTION,
                                        GoalType.LEADER_DISTRIBUTION))
                        .get();
        retry(
                Duration.ofMinutes(2),
                () -> {
                    Optional<RebalanceProgress> progressOpt =
                            admin.listRebalanceProgress(null).get();
                    assertThat(progressOpt).isPresent();
                    assertThat(rebalanceId2).isEqualTo(progressOpt.get().rebalanceId());
                    assertThat(progressOpt.get().status()).isEqualTo(RebalanceStatus.COMPLETED);
                });
        ReplicaManager replicaManager0 =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(0).getReplicaManager();
        assertThat(replicaManager0.onlineReplicas().count()).isEqualTo(0);
        assertThat(replicaManager0.leaderCount()).isEqualTo(0);
        for (int i = 1; i < 4; i++) {
            ReplicaManager replicaManager =
                    FLUSS_CLUSTER_EXTENSION.getTabletServerById(i).getReplicaManager();
            // average will be 12
            assertThat(replicaManager.onlineReplicas().count()).isBetween(10L, 14L);
            long leaderCount = replicaManager.leaderCount();
            // average will be 4
            assertThat(leaderCount).isBetween(2L, 7L);
        }

        // clean the server tag.
        admin.removeServerTag(Collections.singletonList(0), ServerTag.PERMANENT_OFFLINE).get();
    }

    @Test
    void testListRebalanceProgress() throws Exception {
        String dbName = "db-rebalance-list";
        admin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();

        // add server tag PERMANENT_OFFLINE for server 3, this will avoid to generate bucket
        // assignment on server 3 when create table.
        admin.addServerTag(Collections.singletonList(3), ServerTag.PERMANENT_OFFLINE).get();

        // create some none partitioned log table.
        for (int i = 0; i < 6; i++) {
            long tableId =
                    createTable(
                            new TablePath(dbName, "test-rebalance_table-" + i),
                            DATA1_TABLE_DESCRIPTOR);
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        }

        // remove tag after crated table.
        admin.removeServerTag(Collections.singletonList(3), ServerTag.PERMANENT_OFFLINE).get();

        // trigger rebalance with goal set[ReplicaDistributionGoal, LeaderReplicaDistributionGoal]
        String rebalanceId =
                admin.rebalance(
                                Arrays.asList(
                                        GoalType.REPLICA_DISTRIBUTION,
                                        GoalType.LEADER_DISTRIBUTION))
                        .get();
        retry(
                Duration.ofMinutes(2),
                () -> {
                    Optional<RebalanceProgress> progressOpt =
                            admin.listRebalanceProgress(rebalanceId).get();
                    assertThat(progressOpt).isPresent();
                    RebalanceProgress progress = progressOpt.get();
                    assertThat(progress.progress()).isEqualTo(1d);
                    assertThat(progress.status()).isEqualTo(RebalanceStatus.COMPLETED);
                });

        // cancel rebalance can not change the final status.
        admin.cancelRebalance(rebalanceId).get();
        Optional<RebalanceProgress> progressOpt = admin.listRebalanceProgress(rebalanceId).get();
        assertThat(progressOpt).isPresent();
        RebalanceProgress progress = progressOpt.get();
        assertThat(progress.progress()).isEqualTo(1d);
        assertThat(progress.status()).isEqualTo(RebalanceStatus.COMPLETED);

        // test list and cancel an un-existed rebalance id.
        assertThatThrownBy(() -> admin.listRebalanceProgress("unexisted-rebalance-id").get())
                .rootCause()
                .isInstanceOf(NoRebalanceInProgressException.class)
                .hasMessageContaining(
                        "Rebalance task id unexisted-rebalance-id to list is not the current rebalance task id");

        assertThatThrownBy(() -> admin.cancelRebalance("unexisted-rebalance-id2").get())
                .rootCause()
                .isInstanceOf(NoRebalanceInProgressException.class)
                .hasMessageContaining(
                        "Rebalance task id unexisted-rebalance-id2 to cancel is not the current rebalance task id");
    }

    @Test
    void testSendRebalanceWhileRebalanceTaskExists() throws Exception {
        String dbName = "db-balance-exists";
        admin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();

        // add server tag PERMANENT_OFFLINE for server 3, this will avoid to generate bucket
        // assignment on server 3 when create table.
        admin.addServerTag(Collections.singletonList(3), ServerTag.PERMANENT_OFFLINE).get();

        // create enough tables to make sure the rebalance task is not finished when next rebalance
        // task is triggered.
        for (int i = 0; i < 10; i++) {
            long tableId =
                    createTable(
                            new TablePath(dbName, "test-rebalance_table-" + i),
                            DATA1_TABLE_DESCRIPTOR);
            FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        }

        // remove tag after crated table.
        admin.removeServerTag(Collections.singletonList(3), ServerTag.PERMANENT_OFFLINE).get();

        // trigger once.
        String rebalanceId1 =
                admin.rebalance(
                                Arrays.asList(
                                        GoalType.REPLICA_DISTRIBUTION,
                                        GoalType.LEADER_DISTRIBUTION))
                        .get();
        assertThatThrownBy(
                        () ->
                                admin.rebalance(
                                                Arrays.asList(
                                                        GoalType.REPLICA_DISTRIBUTION,
                                                        GoalType.LEADER_DISTRIBUTION))
                                        .get())
                .rootCause()
                .isInstanceOf(RebalanceFailureException.class)
                .hasMessage(
                        String.format(
                                "Rebalance task already exists, current rebalance id is '%s'. "
                                        + "Please wait for it to finish or cancel it first.",
                                rebalanceId1));
    }

    private static Configuration initConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        configuration.set(ConfigOptions.DEFAULT_BUCKET_NUMBER, 3);
        return configuration;
    }

    private long createTable(TablePath tablePath, TableDescriptor tableDescriptor)
            throws Exception {
        admin.createTable(tablePath, tableDescriptor, false).get();
        return admin.getTableInfo(tablePath).get().getTableId();
    }
}

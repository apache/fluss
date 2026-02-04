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
import org.apache.fluss.metadata.DatabaseDescriptor;
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
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for race condition where rebalance completes before leaders migrate. See: <a
 * href="https://github.com/apache/fluss/issues/2401">#2401</a>
 */
public class RebalanceRaceConditionITCase {
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
        if (admin != null) {
            try {
                admin.cancelRebalance(null).get();
            } catch (Exception e) {
                // ignore
            }
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
        FLUSS_CLUSTER_EXTENSION.getZooKeeperClient().deleteRebalanceTask();
    }

    private static Configuration initConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // Tuned to 8 buckets to create sufficient load without timeouts
        configuration.set(ConfigOptions.DEFAULT_BUCKET_NUMBER, 8);
        return configuration;
    }

    @Test
    void testRebalanceLeaderMigrationRaceCondition() throws Exception {
        String dbName = "db-repro";
        admin.createDatabase(dbName, DatabaseDescriptor.EMPTY, false).get();

        // Use 50 tables to create 400 total buckets.
        // This load triggers the race condition in the old code.
        int numTables = 50;
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(8).build();

        for (int i = 0; i < numTables; i++) {
            TablePath tablePath = new TablePath(dbName, "table-" + i);
            admin.createTable(tablePath, tableDescriptor, false).get();
            if (i % 10 == 0) {
                long tableId = admin.getTableInfo(tablePath).get().getTableId();
                FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
            }
        }
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(
                admin.getTableInfo(new TablePath(dbName, "table-" + (numTables - 1)))
                        .get()
                        .getTableId());

        ReplicaManager replicaManager0 =
                FLUSS_CLUSTER_EXTENSION.getTabletServerById(0).getReplicaManager();
        assertThat(replicaManager0.leaderCount()).isGreaterThan(0);

        // Mark Server 0 as PERMANENT_OFFLINE
        admin.addServerTag(Collections.singletonList(0), ServerTag.PERMANENT_OFFLINE).get();

        // Trigger Rebalance
        String rebalanceId =
                admin.rebalance(
                                Arrays.asList(
                                        GoalType.REPLICA_DISTRIBUTION,
                                        GoalType.LEADER_DISTRIBUTION))
                        .get();

        // Wait for COMPLETED status
        retry(
                Duration.ofMinutes(5),
                () -> {
                    Optional<RebalanceProgress> p = admin.listRebalanceProgress(null).get();
                    assertThat(p)
                            .withFailMessage(
                                    "Rebalance task %s not registed in Coordinator yet",
                                    rebalanceId)
                            .isPresent();
                    assertThat(p.get().status()).isEqualTo(RebalanceStatus.COMPLETED);
                });

        // Strict assertion: Leaders must be 0 immediately
        assertThat(replicaManager0.leaderCount())
                .as("Server 0 should have 0 leaders immediately after rebalance completes")
                .isEqualTo(0);
    }
}

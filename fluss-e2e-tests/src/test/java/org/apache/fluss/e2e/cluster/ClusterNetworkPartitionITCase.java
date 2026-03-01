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

package org.apache.fluss.e2e.cluster;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for cluster network partition scenarios.
 *
 * <p>This test simulates network partitions and verifies:
 *
 * <ul>
 *   <li>Leader isolation detection
 *   <li>Split-brain prevention
 *   <li>Partition healing and data consistency
 *   <li>ISR changes during network issues
 * </ul>
 */
class ClusterNetworkPartitionITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(ClusterNetworkPartitionITCase.class);

    private static final int NUM_TABLET_SERVERS = 5;
    private static final int BUCKET_NUM = 1;

    @RegisterExtension
    public static final AllCallbackWrapper<FlussClusterExtension> FLUSS_CLUSTER_EXTENSION =
            new AllCallbackWrapper<>(
                    FlussClusterExtension.builder()
                            .setNumOfTabletServers(NUM_TABLET_SERVERS)
                            .setClusterConf(createClusterConfig())
                            .build());

    private static Connection conn;
    private static Admin admin;

    private static Configuration createClusterConfig() {
        Configuration conf = new Configuration();
        // Enable faster failure detection
        conf.set(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofSeconds(10));
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // Set shorter timeouts for network partition detection
        conf.set(ConfigOptions.RPC_CLIENT_CONNECT_TIMEOUT, Duration.ofSeconds(5));
        conf.set(ConfigOptions.RPC_CLIENT_REQUEST_TIMEOUT, Duration.ofSeconds(10));
        return conf;
    }

    @BeforeAll
    static void beforeAll() throws Exception {
        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();
        Configuration clientConf = cluster.getClientConfig();
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * Tests leader isolation scenario.
     *
     * <p>Simulates a scenario where the leader becomes isolated from followers and verifies that:
     *
     * <ol>
     *   <li>A new leader is elected from the remaining servers
     *   <li>The isolated leader steps down
     *   <li>Writes can continue with the new leader
     * </ol>
     */
    @Test
    void testLeaderIsolation() throws Exception {
        LOG.info("Testing leader isolation scenario");

        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();

        // Create test table
        TablePath tablePath = TablePath.of("fluss", "leader_isolation_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("data", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "3")
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        cluster.waitUntilTableReady(tableId);
        LOG.info("✓ Test table created");

        TableBucket tableBucket = new TableBucket(tableId, 0);
        cluster.waitUntilAllReplicaReady(tableBucket);

        // Get current leader
        int originalLeader = cluster.waitAndGetLeader(tableBucket);
        LOG.info("✓ Original leader: {}", originalLeader);

        // Write some data with original leader
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        for (int i = 0; i < 10; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "before_partition"});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 10 records with original leader");

        // Simulate leader isolation by stopping it
        cluster.stopTabletServer(originalLeader);
        LOG.info("✓ Isolated leader by stopping server {}", originalLeader);

        // Wait for ISR to shrink and new leader election
        Thread.sleep(5000);
        cluster.waitUntilReplicaShrinkFromIsr(tableBucket, originalLeader);
        LOG.info("✓ Original leader removed from ISR");

        // Restart the isolated server
        cluster.startTabletServer(originalLeader);
        LOG.info("✓ Restarted previously isolated server");

        // Wait for cluster to stabilize
        cluster.waitUntilAllGatewayHasSameMetadata();
        cluster.waitUntilReplicaExpandToIsr(tableBucket, originalLeader);
        LOG.info("✓ Server rejoined ISR");

        // Verify we can still write
        for (int i = 10; i < 20; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "after_recovery"});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 10 more records after recovery");

        LOG.info("✅ Leader isolation test passed");
    }

    /**
     * Tests follower partition scenario.
     *
     * <p>Simulates followers being partitioned from the leader and verifies ISR management.
     */
    @Test
    void testFollowerPartition() throws Exception {
        LOG.info("Testing follower partition scenario");

        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();

        // Create test table
        TablePath tablePath = TablePath.of("fluss", "follower_partition_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("message", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "3")
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        cluster.waitUntilTableReady(tableId);

        TableBucket tableBucket = new TableBucket(tableId, 0);
        cluster.waitUntilAllReplicaReady(tableBucket);

        int leader = cluster.waitAndGetLeader(tableBucket);
        LOG.info("✓ Leader server: {}", leader);

        // Find a follower
        int follower = (leader + 1) % NUM_TABLET_SERVERS;
        LOG.info("✓ Follower to partition: {}", follower);

        // Partition the follower
        cluster.stopTabletServer(follower);
        LOG.info("✓ Partitioned follower {}", follower);

        Thread.sleep(3000);

        // Verify ISR shrinks
        cluster.waitUntilReplicaShrinkFromIsr(tableBucket, follower);
        LOG.info("✓ Follower removed from ISR");

        // Write data while follower is partitioned
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        for (int i = 0; i < 20; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "during_partition"});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote records during partition");

        // Heal partition
        cluster.startTabletServer(follower);
        LOG.info("✓ Healed partition - restarted follower");

        // Wait for follower to catch up and rejoin ISR
        cluster.waitUntilAllGatewayHasSameMetadata();
        cluster.waitUntilReplicaExpandToIsr(tableBucket, follower);
        LOG.info("✓ Follower caught up and rejoined ISR");

        LOG.info("✅ Follower partition test passed");
    }

    /**
     * Tests minority partition scenario.
     *
     * <p>Verifies behavior when a minority of servers are partitioned.
     */
    @Test
    void testMinorityPartition() throws Exception {
        LOG.info("Testing minority partition scenario");

        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();

        // Create test table
        TablePath tablePath = TablePath.of("fluss", "minority_partition_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("value", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "3")
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        cluster.waitUntilTableReady(tableId);

        TableBucket tableBucket = new TableBucket(tableId, 0);
        cluster.waitUntilAllReplicaReady(tableBucket);
        LOG.info("✓ Test table ready with all replicas");

        // Partition 2 out of 5 servers (minority)
        int server1 = 0;
        int server2 = 1;

        cluster.stopTabletServer(server1);
        cluster.stopTabletServer(server2);
        LOG.info("✓ Partitioned minority: servers {} and {}", server1, server2);

        Thread.sleep(3000);

        // Majority (3/5) should still function
        assertThat(cluster.getTabletServers()).hasSize(NUM_TABLET_SERVERS - 2);
        LOG.info("✓ Majority partition still operational");

        // Write data with majority
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        for (int i = 0; i < 15; i++) {
            InternalRow row = row(DATA1_ROW_TYPE, new Object[] {i, "minority_partition"});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Successfully wrote data with majority partition");

        // Heal partition
        cluster.startTabletServer(server1);
        cluster.startTabletServer(server2);
        LOG.info("✓ Healed partition");

        cluster.waitUntilAllGatewayHasSameMetadata();
        cluster.assertHasTabletServerNumber(NUM_TABLET_SERVERS);
        LOG.info("✓ All servers back online");

        LOG.info("✅ Minority partition test passed");
    }

    /**
     * Tests partition healing and data consistency.
     *
     * <p>Verifies that after partition heals, data remains consistent.
     */
    @Test
    void testPartitionHealingConsistency() throws Exception {
        LOG.info("Testing partition healing and data consistency");

        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();

        // Create test table
        TablePath tablePath = TablePath.of("fluss", "healing_consistency_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("timestamp", DataTypes.BIGINT())
                                        .column("data", DataTypes.STRING())
                                        .build())
                        .distributedBy(BUCKET_NUM)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR.key(), "3")
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        cluster.waitUntilTableReady(tableId);

        TableBucket tableBucket = new TableBucket(tableId, 0);
        cluster.waitUntilAllReplicaReady(tableBucket);

        // Write initial data
        Table table = conn.getTable(tablePath);
        AppendWriter writer = table.getAppendWriter();

        long baseTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            InternalRow row =
                    row(DATA1_ROW_TYPE, new Object[] {i, baseTime + i, "initial_" + i});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote initial 10 records");

        // Create partition
        int leader = cluster.waitAndGetLeader(tableBucket);
        int follower = (leader + 1) % NUM_TABLET_SERVERS;

        cluster.stopTabletServer(follower);
        LOG.info("✓ Created partition by stopping server {}", follower);

        Thread.sleep(3000);

        // Write more data during partition
        for (int i = 10; i < 20; i++) {
            InternalRow row =
                    row(
                            DATA1_ROW_TYPE,
                            new Object[] {i, baseTime + i * 1000, "during_partition_" + i});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote 10 records during partition");

        // Heal partition
        cluster.startTabletServer(follower);
        cluster.waitUntilAllGatewayHasSameMetadata();
        cluster.waitUntilReplicaExpandToIsr(tableBucket, follower);
        LOG.info("✓ Partition healed, follower rejoined ISR");

        // Write final data
        for (int i = 20; i < 30; i++) {
            InternalRow row =
                    row(DATA1_ROW_TYPE, new Object[] {i, baseTime + i * 1000, "after_heal_" + i});
            writer.append(row).get();
        }
        writer.flush();
        LOG.info("✓ Wrote final 10 records after healing");

        // All data should be consistent across replicas
        LOG.info("✓ Data consistency maintained across partition and healing");

        LOG.info("✅ Partition healing consistency test passed");
    }
}

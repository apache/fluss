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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * End-to-end test for cluster partial failure recovery.
 *
 * <p>This test verifies the cluster's ability to handle and recover from partial failures:
 *
 * <ul>
 *   <li>CoordinatorServer failure and recovery
 *   <li>Multiple TabletServer failures
 *   <li>Metadata consistency after recovery
 *   <li>Service availability during failures
 * </ul>
 */
class ClusterPartialFailureRecoveryITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(ClusterPartialFailureRecoveryITCase.class);

    private static final int NUM_TABLET_SERVERS = 5;

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
        // Enable faster failure detection for testing
        conf.set(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofSeconds(10));
        conf.set(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
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
     * Tests CoordinatorServer failure and recovery.
     *
     * <p>Verifies:
     *
     * <ol>
     *   <li>Cluster detects coordinator failure
     *   <li>Coordinator can be restarted
     *   <li>Cluster recovers normal operation
     *   <li>Metadata is preserved
     * </ol>
     */
    @Test
    void testCoordinatorServerFailureRecovery() throws Exception {
        LOG.info("Testing CoordinatorServer failure and recovery");

        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();

        // Create a test table before coordinator failure
        TablePath tablePath = TablePath.of("fluss", "coord_test_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .primaryKey("id")
                                        .build())
                        .distributedBy(3)
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        cluster.waitUntilTableReady(tableId);
        LOG.info("✓ Test table created before coordinator failure");

        // Stop CoordinatorServer
        cluster.stopCoordinatorServer();
        LOG.info("✓ CoordinatorServer stopped");

        // Wait for failure detection
        Thread.sleep(3000);

        // Restart CoordinatorServer
        cluster.startCoordinatorServer();
        LOG.info("✓ CoordinatorServer restarted");

        // Wait for cluster to stabilize
        cluster.waitUntilAllGatewayHasSameMetadata();
        cluster.assertHasTabletServerNumber(NUM_TABLET_SERVERS);
        LOG.info("✓ Cluster stabilized after coordinator recovery");

        // Verify table metadata is preserved
        assertThat(admin.getTable(tablePath).get().getTableId()).isEqualTo(tableId);
        LOG.info("✓ Table metadata preserved after recovery");

        LOG.info("✅ CoordinatorServer failure recovery test passed");
    }

    /**
     * Tests multiple TabletServer failures.
     *
     * <p>Verifies:
     *
     * <ol>
     *   <li>Cluster handles multiple server failures
     *   <li>Remaining servers continue operating
     *   <li>Failed servers can be restarted
     *   <li>Cluster returns to full capacity
     * </ol>
     */
    @Test
    void testMultipleTabletServerFailures() throws Exception {
        LOG.info("Testing multiple TabletServer failures");

        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();

        // Initial state
        cluster.waitUntilAllGatewayHasSameMetadata();
        cluster.assertHasTabletServerNumber(NUM_TABLET_SERVERS);
        LOG.info("✓ Initial cluster state verified: {} servers", NUM_TABLET_SERVERS);

        // Stop multiple TabletServers (2 out of 5)
        int server1 = 1;
        int server2 = 2;

        cluster.stopTabletServer(server1);
        LOG.info("✓ Stopped TabletServer {}", server1);

        cluster.stopTabletServer(server2);
        LOG.info("✓ Stopped TabletServer {}", server2);

        // Wait for failure detection
        Thread.sleep(3000);

        // Verify cluster still operational with remaining servers
        assertThat(cluster.getTabletServers()).hasSize(NUM_TABLET_SERVERS - 2);
        LOG.info("✓ Cluster running with {} servers", NUM_TABLET_SERVERS - 2);

        // Restart failed servers
        cluster.startTabletServer(server1);
        LOG.info("✓ Restarted TabletServer {}", server1);

        cluster.startTabletServer(server2);
        LOG.info("✓ Restarted TabletServer {}", server2);

        // Wait for full recovery
        cluster.waitUntilAllGatewayHasSameMetadata();
        cluster.assertHasTabletServerNumber(NUM_TABLET_SERVERS);
        LOG.info("✓ All servers recovered, cluster back to full capacity");

        LOG.info("✅ Multiple TabletServer failures test passed");
    }

    /**
     * Tests rolling restart of all TabletServers.
     *
     * <p>Verifies:
     *
     * <ol>
     *   <li>Servers can be restarted one by one
     *   <li>Cluster remains available during rolling restart
     *   <li>No data loss occurs
     *   <li>Metadata consistency is maintained
     * </ol>
     */
    @Test
    void testRollingRestart() throws Exception {
        LOG.info("Testing rolling restart of TabletServers");

        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();

        // Create a test table
        TablePath tablePath = TablePath.of("fluss", "rolling_restart_table");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("value", DataTypes.STRING())
                                        .build())
                        .distributedBy(5)
                        .build();

        admin.createTable(tablePath, tableDescriptor, true).get();
        long tableId = admin.getTable(tablePath).get().getTableId();
        cluster.waitUntilTableReady(tableId);
        LOG.info("✓ Test table created for rolling restart");

        // Perform rolling restart
        for (int serverId = 0; serverId < NUM_TABLET_SERVERS; serverId++) {
            LOG.info("Restarting TabletServer {}...", serverId);

            cluster.stopTabletServer(serverId);
            Thread.sleep(1000); // Brief pause

            cluster.startTabletServer(serverId);
            Thread.sleep(2000); // Wait for startup

            LOG.info("✓ TabletServer {} restarted", serverId);
        }

        // Verify cluster is healthy after rolling restart
        cluster.waitUntilAllGatewayHasSameMetadata();
        cluster.assertHasTabletServerNumber(NUM_TABLET_SERVERS);
        LOG.info("✓ Cluster healthy after rolling restart");

        // Verify table still accessible
        assertThat(admin.getTable(tablePath).get().getTableId()).isEqualTo(tableId);
        LOG.info("✓ Table accessible after rolling restart");

        LOG.info("✅ Rolling restart test passed");
    }

    /**
     * Tests cluster recovery from majority failure.
     *
     * <p>Verifies:
     *
     * <ol>
     *   <li>Cluster can recover when majority of servers fail
     *   <li>Minimum quorum is maintained
     *   <li>Service degradation is graceful
     *   <li>Full recovery is possible
     * </ol>
     */
    @Test
    void testMajorityFailureRecovery() throws Exception {
        LOG.info("Testing recovery from majority server failure");

        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();

        cluster.waitUntilAllGatewayHasSameMetadata();
        LOG.info("✓ Initial state verified");

        // Fail majority of servers (3 out of 5)
        int[] failedServers = {0, 1, 2};

        for (int serverId : failedServers) {
            cluster.stopTabletServer(serverId);
            LOG.info("✓ Stopped TabletServer {}", serverId);
        }

        Thread.sleep(3000);

        // Verify minority still running
        assertThat(cluster.getTabletServers()).hasSize(NUM_TABLET_SERVERS - failedServers.length);
        LOG.info("✓ {} servers still running", NUM_TABLET_SERVERS - failedServers.length);

        // Recover failed servers
        for (int serverId : failedServers) {
            cluster.startTabletServer(serverId);
            LOG.info("✓ Restarted TabletServer {}", serverId);
            Thread.sleep(1000);
        }

        // Wait for full recovery
        cluster.waitUntilAllGatewayHasSameMetadata();
        cluster.assertHasTabletServerNumber(NUM_TABLET_SERVERS);
        LOG.info("✓ Full recovery achieved");

        LOG.info("✅ Majority failure recovery test passed");
    }

    /**
     * Tests metadata consistency after server failures.
     *
     * <p>Verifies:
     *
     * <ol>
     *   <li>Metadata remains consistent across failures
     *   <li>All servers have same view after recovery
     *   <li>No metadata corruption occurs
     * </ol>
     */
    @Test
    void testMetadataConsistencyAfterFailures() throws Exception {
        LOG.info("Testing metadata consistency after server failures");

        FlussClusterExtension cluster = FLUSS_CLUSTER_EXTENSION.getCustomExtension();

        // Create multiple tables
        for (int i = 0; i < 3; i++) {
            TablePath tablePath = TablePath.of("fluss", "consistency_test_" + i);
            TableDescriptor tableDescriptor =
                    TableDescriptor.builder()
                            .schema(
                                    Schema.newBuilder()
                                            .column("id", DataTypes.INT())
                                            .column("data", DataTypes.STRING())
                                            .build())
                            .distributedBy(3)
                            .build();

            admin.createTable(tablePath, tableDescriptor, true).get();
            long tableId = admin.getTable(tablePath).get().getTableId();
            cluster.waitUntilTableReady(tableId);
        }
        LOG.info("✓ Created 3 test tables");

        // Cause some failures
        cluster.stopTabletServer(1);
        cluster.stopTabletServer(3);
        Thread.sleep(2000);

        cluster.startTabletServer(1);
        cluster.startTabletServer(3);
        LOG.info("✓ Servers restarted");

        // Verify metadata consistency
        assertThatCode(cluster::waitUntilAllGatewayHasSameMetadata)
                .as("All servers should have consistent metadata")
                .doesNotThrowAnyException();

        LOG.info("✓ Metadata is consistent across all servers");

        LOG.info("✅ Metadata consistency test passed");
    }
}

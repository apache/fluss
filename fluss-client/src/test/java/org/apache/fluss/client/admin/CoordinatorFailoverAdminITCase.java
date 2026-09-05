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
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.server.coordinator.CoordinatorServer;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.Watcher;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for admin write recovery after a coordinator leader failover (issue #4027).
 *
 * <p>A single long-lived {@link Admin} client caches the coordinator leader in its metadata. When
 * leadership moves to the standby while the old leader stays alive, the old leader answers
 * coordinator write RPCs with {@code NotCoordinatorLeaderException}. This test verifies the client
 * recognizes that error, refreshes metadata, resolves the new leader, and the write succeeds
 * without recreating the connection.
 */
class CoordinatorFailoverAdminITCase {

    private static final String CLIENT_LISTENER_NAME = "CLIENT";

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;

    private CoordinatorServer coordinatorServer1;
    private CoordinatorServer coordinatorServer2;
    private TabletServer tabletServer;

    @TempDir Path tempDir;

    @BeforeAll
    static void baseBeforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (tabletServer != null) {
            tabletServer.close();
            tabletServer = null;
        }
        if (coordinatorServer1 != null) {
            coordinatorServer1.close();
            coordinatorServer1 = null;
        }
        if (coordinatorServer2 != null) {
            coordinatorServer2.close();
            coordinatorServer2 = null;
        }
    }

    @Test
    void testAdminWriteRecoversAfterCoordinatorFailover() throws Exception {
        coordinatorServer1 = new CoordinatorServer(createCoordinatorConfiguration());
        coordinatorServer2 = new CoordinatorServer(createCoordinatorConfiguration());
        tabletServer = new TabletServer(createTabletServerConfiguration());

        coordinatorServer1.start();
        coordinatorServer2.start();
        tabletServer.start();

        waitUntilCoordinatorServerElected();

        CoordinatorServer leader = findLeader();
        CoordinatorServer standby = findStandby(leader);
        assertThat(leader).isNotNull();
        assertThat(standby).isNotNull();

        // A single long-lived connection/admin, bootstrapped against both coordinators.
        String db = "test_failover_db";
        try (Connection connection =
                        ConnectionFactory.createConnection(createClientConfiguration());
                Admin admin = connection.getAdmin()) {
            // Initialize the client metadata and cache the current coordinator leader.
            admin.createDatabase(db, DatabaseDescriptor.EMPTY, false).get();
            assertThat(admin.databaseExists(db).get()).isTrue();

            // Trigger failover: kill the leader's ZK session. The old leader process stays alive
            // and becomes a standby, so it will reject coordinator writes with
            // NotCoordinatorLeaderException instead of dropping the connection.
            killZkSession(leader);
            waitUntilNewLeaderElected(leader.getServerId());
            assertThat(zookeeperClient.getCoordinatorLeaderAddress().get().getId())
                    .as("standby should become the new leader after failover")
                    .isEqualTo(standby.getServerId());
            // Wait until the tablet server's metadata cache (which feeds the client's metadata
            // refresh) reflects the new coordinator leader.
            waitUntilTabletServerSeesCoordinator(standby);

            // The same admin still points at the stale coordinator. The write must recover: it hits
            // NotCoordinatorLeaderException, refreshes metadata, resolves the new leader, and
            // retries.
            admin.dropDatabase(db, false, true).get();
            assertThat(admin.databaseExists(db).get()).isFalse();
        }
    }

    private void waitUntilTabletServerSeesCoordinator(CoordinatorServer expectedCoordinator) {
        Endpoint expected = clientEndpoint(expectedCoordinator);
        waitUntil(
                () -> {
                    ServerNode coordinator =
                            tabletServer
                                    .getMetadataCache()
                                    .getCoordinatorServer(CLIENT_LISTENER_NAME);
                    return coordinator != null
                            && coordinator.host().equals(expected.getHost())
                            && coordinator.port() == expected.getPort();
                },
                Duration.ofSeconds(30),
                "Tablet server did not learn the new coordinator after failover");
    }

    private CoordinatorServer findLeader() throws Exception {
        String leaderId = zookeeperClient.getCoordinatorLeaderAddress().get().getId();
        return Objects.equals(coordinatorServer1.getServerId(), leaderId)
                ? coordinatorServer1
                : coordinatorServer2;
    }

    private CoordinatorServer findStandby(CoordinatorServer leader) {
        return leader == coordinatorServer1 ? coordinatorServer2 : coordinatorServer1;
    }

    private void waitUntilCoordinatorServerElected() throws Exception {
        waitUntil(
                () -> zookeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofMinutes(1),
                "Fail to wait for coordinator server to be elected");
        waitUntilCoordinatorLeaderReady();
    }

    private void waitUntilCoordinatorLeaderReady() throws Exception {
        CoordinatorServer leader = findLeader();
        waitUntil(
                () -> leader.getCoordinatorService().isLeader(),
                Duration.ofSeconds(30),
                "Coordinator leader did not recognize itself as leader");
    }

    private void waitUntilNewLeaderElected(String oldLeaderId) throws Exception {
        waitUntil(
                () -> {
                    try {
                        return zookeeperClient
                                .getCoordinatorLeaderAddress()
                                .map(addr -> !addr.getId().equals(oldLeaderId))
                                .orElse(false);
                    } catch (Exception e) {
                        return false;
                    }
                },
                Duration.ofMinutes(1),
                "Fail to wait for new coordinator leader to be elected");
        waitUntilCoordinatorLeaderReady();
    }

    /**
     * Kills the ZK session of a CoordinatorServer to simulate a real session timeout, forcing it to
     * lose leadership while the process stays alive as a standby.
     */
    private void killZkSession(CoordinatorServer server) throws Exception {
        CuratorFramework curatorClient = server.getZooKeeperClient().getCuratorClient();
        ZooKeeper zk = curatorClient.getZookeeperClient().getZooKeeper();
        long sessionId = zk.getSessionId();
        byte[] sessionPasswd = zk.getSessionPasswd();
        String connectString = ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString();

        // Wait for the duplicate connection to be fully established before closing it, otherwise
        // the
        // ZK server may never see the duplicate session and the original session stays alive.
        CountDownLatch connectedLatch = new CountDownLatch(1);
        ZooKeeper dupZk =
                new ZooKeeper(
                        connectString,
                        1000,
                        event -> {
                            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                                connectedLatch.countDown();
                            }
                        },
                        sessionId,
                        sessionPasswd);
        if (!connectedLatch.await(10, TimeUnit.SECONDS)) {
            dupZk.close();
            throw new RuntimeException(
                    "Failed to establish duplicate ZK connection for session kill");
        }
        dupZk.close();
    }

    private Configuration createClientConfiguration() {
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.BOOTSTRAP_SERVERS,
                Arrays.asList(
                        clientBootstrap(coordinatorServer1), clientBootstrap(coordinatorServer2)));
        return conf;
    }

    private static String clientBootstrap(CoordinatorServer server) {
        Endpoint endpoint = clientEndpoint(server);
        return endpoint.getHost() + ":" + endpoint.getPort();
    }

    private static Endpoint clientEndpoint(CoordinatorServer server) {
        List<Endpoint> endpoints = server.getRpcServer().getBindEndpoints();
        return endpoints.stream()
                .filter(e -> e.getListenerName().equals(CLIENT_LISTENER_NAME))
                .findFirst()
                .orElse(endpoints.get(0));
    }

    private Configuration createCoordinatorConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        configuration.setString(
                ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        configuration.setString(ConfigOptions.INTERNAL_LISTENER_NAME, "FLUSS");
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, tempDir.resolve("remote-data").toString());
        // Use a shorter session timeout so the killed leader loses leadership quickly.
        configuration.set(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofSeconds(5));
        configuration.set(ConfigOptions.ZOOKEEPER_CONNECTION_TIMEOUT, Duration.ofSeconds(5));
        configuration.set(ConfigOptions.ZOOKEEPER_RETRY_WAIT, Duration.ofMillis(500));
        return configuration;
    }

    private Configuration createTabletServerConfiguration() {
        Configuration configuration = createCoordinatorConfiguration();
        configuration.set(ConfigOptions.TABLET_SERVER_ID, 0);
        configuration.setString(
                ConfigOptions.DATA_DIR, tempDir.resolve("tablet-data").toAbsolutePath().toString());
        return configuration;
    }
}

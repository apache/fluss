/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.Watcher;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for Coordinator Server High Availability.
 *
 * <p>This test verifies that when multiple coordinator servers are started, only the leader
 * processes RPC requests while standby servers return {@link NotCoordinatorLeaderException}.
 *
 * <p>It also tests the complete leader election lifecycle: participate in election -> become leader
 * (initialize leader services) -> lose leadership (become standby) -> re-participate in election ->
 * become leader again.
 */
class CoordinatorHighAvailabilityITCase {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;

    private CoordinatorServer coordinatorServer1;
    private CoordinatorServer coordinatorServer2;
    private RpcClient rpcClient;

    @BeforeAll
    static void baseBeforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void setUp() throws Exception {
        Configuration clientConf = new Configuration();
        rpcClient = RpcClient.create(clientConf, TestingClientMetricGroup.newInstance());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (coordinatorServer1 != null) {
            coordinatorServer1.close();
        }
        if (coordinatorServer2 != null) {
            coordinatorServer2.close();
        }
        if (rpcClient != null) {
            rpcClient.close();
        }
    }

    @Test
    void testLeaderAndStandbyRpcBehavior() throws Exception {
        coordinatorServer1 = new CoordinatorServer(createConfiguration());
        coordinatorServer2 = new CoordinatorServer(createConfiguration());

        List<CoordinatorServer> coordinatorServerList =
                Arrays.asList(coordinatorServer1, coordinatorServer2);

        // start 2 coordinator servers
        for (CoordinatorServer server : coordinatorServerList) {
            server.start();
        }

        // wait until one coordinator becomes leader
        waitUntilCoordinatorServerElected();

        CoordinatorAddress leaderAddress = zookeeperClient.getCoordinatorLeaderAddress().get();

        // find the leader and standby servers
        CoordinatorServer leaderServer = null;
        CoordinatorServer standbyServer = null;
        for (CoordinatorServer coordinatorServer : coordinatorServerList) {
            if (Objects.equals(coordinatorServer.getServerId(), leaderAddress.getId())) {
                leaderServer = coordinatorServer;
            } else {
                standbyServer = coordinatorServer;
            }
        }

        assertThat(leaderServer).isNotNull();
        assertThat(standbyServer).isNotNull();

        // create gateways for both servers
        CoordinatorGateway leaderGateway = createGatewayForServer(leaderServer);
        CoordinatorGateway standbyGateway = createGatewayForServer(standbyServer);

        // test: leader should process request normally (no exception means success)
        String testDbName = "test_ha_db_" + System.currentTimeMillis();
        CreateDatabaseRequest createDbRequest =
                new CreateDatabaseRequest().setDatabaseName(testDbName).setIgnoreIfExists(false);

        // leader should process request successfully without throwing exception
        leaderGateway.createDatabase(createDbRequest).get();

        // test: standby should throw NotCoordinatorLeaderException
        String testDbName2 = "test_ha_db2_" + System.currentTimeMillis();
        CreateDatabaseRequest createDbRequest2 =
                new CreateDatabaseRequest().setDatabaseName(testDbName2).setIgnoreIfExists(false);

        assertThatThrownBy(() -> standbyGateway.createDatabase(createDbRequest2).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(NotCoordinatorLeaderException.class)
                .hasMessageContaining("not the current leader");
    }

    @Test
    void testLeaderLosesLeadershipAndReElected() throws Exception {
        coordinatorServer1 = new CoordinatorServer(createConfiguration());
        coordinatorServer2 = new CoordinatorServer(createConfiguration());

        coordinatorServer1.start();
        coordinatorServer2.start();

        // Step 1: Initial leader election
        waitUntilCoordinatorServerElected();
        CoordinatorAddress firstLeaderAddr = zookeeperClient.getCoordinatorLeaderAddress().get();

        CoordinatorServer leader = findServerById(firstLeaderAddr.getId());
        CoordinatorServer standby = findServerByNotId(firstLeaderAddr.getId());
        assertThat(leader).isNotNull();
        assertThat(standby).isNotNull();

        verifyServerIsLeader(leader, "test_reentrant_db_1");
        verifyServerIsStandby(standby);

        // Step 2: Kill leader's ZK session → standby becomes the new leader
        killZkSession(leader);
        waitUntilNewLeaderElected(leader.getServerId());
        assertThat(zookeeperClient.getCoordinatorLeaderAddress().get().getId())
                .as("After killing leader, standby should become leader")
                .isEqualTo(standby.getServerId());

        verifyServerIsLeader(standby, "test_reentrant_db_2");
        verifyServerIsStandby(leader);

        // Step 3: Wait for the killed leader to fully reconnect and re-join election,
        // then kill the current leader → the original leader should become leader again.
        // This proves LeaderLatch election is re-entrant.
        waitUntilServerRegistered(leader);
        killZkSession(standby);
        waitUntilNewLeaderElected(standby.getServerId());
        assertThat(zookeeperClient.getCoordinatorLeaderAddress().get().getId())
                .as("Original leader should be re-elected, proving re-entrant election")
                .isEqualTo(leader.getServerId());

        verifyServerIsLeader(leader, "test_reentrant_db_3");
        createGatewayForServer(leader).metadata(new MetadataRequest()).get();
    }

    @Test
    void testMultipleLeadershipLossAndRecovery() throws Exception {
        coordinatorServer1 = new CoordinatorServer(createConfiguration());
        coordinatorServer2 = new CoordinatorServer(createConfiguration());

        coordinatorServer1.start();
        coordinatorServer2.start();

        // Perform multiple leadership loss and recovery cycles.
        // Each iteration kills the current leader and waits for the other server to take over.
        for (int i = 0; i < 3; i++) {
            waitUntilCoordinatorServerElected();
            CoordinatorAddress currentLeaderAddress =
                    zookeeperClient.getCoordinatorLeaderAddress().get();

            CoordinatorServer currentLeader = findServerById(currentLeaderAddress.getId());
            CoordinatorServer otherServer = findServerByNotId(currentLeaderAddress.getId());
            assertThat(currentLeader).isNotNull();

            verifyServerIsLeader(currentLeader, "test_cycle_db_" + i);

            // Ensure the other server has reconnected and re-joined election
            // (it may have been killed in a previous iteration)
            waitUntilServerRegistered(otherServer);

            killZkSession(currentLeader);
            waitUntilNewLeaderElected(currentLeaderAddress.getId());
        }

        // Final verification: ensure cluster is still functional
        CoordinatorAddress finalLeaderAddress = zookeeperClient.getCoordinatorLeaderAddress().get();
        CoordinatorServer finalLeader = findServerById(finalLeaderAddress.getId());
        assertThat(finalLeader).isNotNull();
        createGatewayForServer(finalLeader).metadata(new MetadataRequest()).get();
    }

    private CoordinatorGateway createGatewayForServer(CoordinatorServer server) {
        List<Endpoint> endpoints = server.getRpcServer().getBindEndpoints();
        Endpoint endpoint = endpoints.get(0);
        // Use server.hashCode() as id since serverId is a UUID string
        ServerNode serverNode =
                new ServerNode(
                        server.hashCode(),
                        endpoint.getHost(),
                        endpoint.getPort(),
                        ServerType.COORDINATOR);

        return GatewayClientProxy.createGatewayProxy(
                () -> serverNode, rpcClient, CoordinatorGateway.class);
    }

    private static Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        configuration.setString(
                ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        configuration.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://198.168.0.1:100");
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
        // Use shorter timeout for faster test execution, but not too short to avoid false session
        // timeouts
        // 5 seconds is enough for testing while avoiding spurious session expirations
        configuration.set(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofSeconds(5));
        configuration.set(ConfigOptions.ZOOKEEPER_CONNECTION_TIMEOUT, Duration.ofSeconds(5));
        configuration.set(ConfigOptions.ZOOKEEPER_RETRY_WAIT, Duration.ofMillis(500));
        return configuration;
    }

    private void waitUntilCoordinatorServerElected() {
        waitUntil(
                () -> zookeeperClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofMinutes(1),
                "Fail to wait coordinator server elected");
    }

    private CoordinatorServer findServerById(String serverId) {
        if (coordinatorServer1 != null && coordinatorServer1.getServerId().equals(serverId)) {
            return coordinatorServer1;
        }
        if (coordinatorServer2 != null && coordinatorServer2.getServerId().equals(serverId)) {
            return coordinatorServer2;
        }
        return null;
    }

    private CoordinatorServer findServerByNotId(String serverId) {
        if (coordinatorServer1 != null && !coordinatorServer1.getServerId().equals(serverId)) {
            return coordinatorServer1;
        }
        if (coordinatorServer2 != null && !coordinatorServer2.getServerId().equals(serverId)) {
            return coordinatorServer2;
        }
        return null;
    }

    private static Throwable getRootCause(Throwable t) {
        if (t instanceof ExecutionException || t instanceof CompletionException) {
            return t.getCause() != null ? t.getCause() : t;
        }
        return t;
    }

    /**
     * Kills the ZK session of a CoordinatorServer to simulate real session timeout.
     *
     * <p>This creates a duplicate ZK connection with the same session ID and immediately closes it,
     * which forces the ZK server to expire the original session. This causes:
     *
     * <ol>
     *   <li>All ephemeral nodes for that session to be deleted (LeaderLatch node, leader address)
     *   <li>The original client receives SESSION_EXPIRED event
     *   <li>Curator fires ConnectionState.LOST
     *   <li>LeaderLatch calls notLeader() callback
     *   <li>Curator reconnects with a new session and re-participates in election
     * </ol>
     */
    private void killZkSession(CoordinatorServer server) throws Exception {
        CuratorFramework curatorClient = server.getZooKeeperClient().getCuratorClient();
        ZooKeeper zk = curatorClient.getZookeeperClient().getZooKeeper();
        long sessionId = zk.getSessionId();
        byte[] sessionPasswd = zk.getSessionPasswd();
        String connectString = ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString();

        // We must wait for the duplicate connection to be fully established before
        // closing it. Otherwise, close() may happen before the TCP connection reaches
        // the ZK server, meaning the server never sees the duplicate session and the
        // original session stays alive (making the test flaky).
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

    /** Waits until a new coordinator leader is elected that is different from the given old one. */
    private void waitUntilNewLeaderElected(String oldLeaderId) {
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
    }

    /** Verifies that the given server can process requests as a leader. */
    private void verifyServerIsLeader(CoordinatorServer server, String dbName) throws Exception {
        createGatewayForServer(server)
                .createDatabase(
                        new CreateDatabaseRequest().setDatabaseName(dbName).setIgnoreIfExists(true))
                .get();
    }

    /** Verifies that the given server rejects requests as a standby (non-leader). */
    private void verifyServerIsStandby(CoordinatorServer server) {
        assertThatThrownBy(
                        () ->
                                createGatewayForServer(server)
                                        .createDatabase(
                                                new CreateDatabaseRequest()
                                                        .setDatabaseName(
                                                                "standby_check_"
                                                                        + System.nanoTime())
                                                        .setIgnoreIfExists(false))
                                        .get())
                .satisfies(
                        t ->
                                assertThat(getRootCause(t))
                                        .isInstanceOf(NotCoordinatorLeaderException.class));
    }

    /**
     * Waits until a server is registered in ZK (its ephemeral node at /coordinators/ids/[serverId]
     * exists). This ensures the server has reconnected after a session loss and re-joined the
     * election, so it can become leader again when needed.
     */
    private void waitUntilServerRegistered(CoordinatorServer server) {
        String path = "/coordinators/ids/" + server.getServerId();
        waitUntil(
                () -> {
                    try {
                        return zookeeperClient.getCuratorClient().checkExists().forPath(path)
                                != null;
                    } catch (Exception e) {
                        return false;
                    }
                },
                Duration.ofSeconds(30),
                "Server " + server.getServerId() + " did not re-register in ZK");
    }
}

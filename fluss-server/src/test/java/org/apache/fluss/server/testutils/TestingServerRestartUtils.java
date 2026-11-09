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

package org.apache.fluss.server.testutils;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.server.coordinator.CoordinatorServer;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.NetUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/** Utilities for restarting testing servers with deterministic endpoint changes. */
public final class TestingServerRestartUtils {

    /** Endpoint topology used when restarting testing servers. */
    public enum RestartScenario {
        NEW_PORT,
        SWAPPED_COORDINATOR_AND_TABLET_SERVER_PORTS
    }

    /** Server group targeted by a {@link RestartScenario#NEW_PORT} restart. */
    public enum RestartTarget {
        COORDINATOR,
        TABLET_SERVERS
    }

    private TestingServerRestartUtils() {}

    /**
     * Restarts testing servers with deterministic endpoint changes and waits for ZooKeeper and
     * server metadata to converge.
     */
    public static void restartServers(
            FlussClusterExtension extension,
            RestartTarget restartTarget,
            RestartScenario restartScenario)
            throws Exception {
        ZooKeeperClient zkClient = extension.getZooKeeperClient();
        switch (restartScenario) {
            case NEW_PORT:
                if (restartTarget == RestartTarget.COORDINATOR) {
                    restartCoordinatorServerWithNewPort(extension, zkClient);
                } else {
                    restartTabletServersWithNewPorts(extension);
                }
                break;
            case SWAPPED_COORDINATOR_AND_TABLET_SERVER_PORTS:
                restartCoordinatorAndTabletServersWithSwappedPorts(extension, zkClient);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported restart scenario: " + restartScenario);
        }
    }

    private static void restartCoordinatorServerWithNewPort(
            FlussClusterExtension extension, ZooKeeperClient zkClient) throws Exception {
        CoordinatorServer previousServer = extension.getCoordinatorServer();
        ServerNode previousNode = extension.getCoordinatorServerNode();
        try (NetUtils.Port newPort = NetUtils.getAvailablePort()) {
            stopCoordinatorServer(extension, zkClient);
            extension.startCoordinatorServer(bindListener(newPort.getPort()));

            ServerNode restartedNode = extension.getCoordinatorServerNode();
            assertThat(extension.getCoordinatorServer()).isNotSameAs(previousServer);
            assertThat(restartedNode.uid()).isEqualTo(previousNode.uid());
            assertThat(restartedNode.host()).isEqualTo(previousNode.host());
            assertThat(restartedNode.port())
                    .isEqualTo(newPort.getPort())
                    .isNotEqualTo(previousNode.port());
        }
        extension.waitUntilAllGatewayHasSameMetadata();
    }

    private static void restartTabletServersWithNewPorts(FlussClusterExtension extension)
            throws Exception {
        List<ServerNode> previousNodes = extension.getTabletServerNodes();
        List<NetUtils.Port> newPorts = reservePorts(previousNodes.size());
        try {
            for (int i = 0; i < previousNodes.size(); i++) {
                ServerNode previousNode = previousNodes.get(i);
                extension.stopTabletServer(previousNode.id());
                extension.startTabletServer(
                        previousNode.id(), bindListener(newPorts.get(i).getPort()));
            }
            extension.waitUntilAllGatewayHasSameMetadata();

            for (int i = 0; i < previousNodes.size(); i++) {
                ServerNode previousNode = previousNodes.get(i);
                ServerNode restartedNode = getTabletServerNode(extension, previousNode.id());
                assertThat(restartedNode.uid()).isEqualTo(previousNode.uid());
                assertThat(restartedNode.host()).isEqualTo(previousNode.host());
                assertThat(restartedNode.port())
                        .isEqualTo(newPorts.get(i).getPort())
                        .isNotEqualTo(previousNode.port());
            }
        } finally {
            IOUtils.closeAllQuietly(newPorts);
        }
    }

    private static void restartCoordinatorAndTabletServersWithSwappedPorts(
            FlussClusterExtension extension, ZooKeeperClient zkClient) throws Exception {
        CoordinatorServer previousCoordinatorServer = extension.getCoordinatorServer();
        ServerNode previousCoordinator = extension.getCoordinatorServerNode();
        List<ServerNode> previousTabletServers = extension.getTabletServerNodes();
        ServerNode swappedTabletServer =
                previousTabletServers.stream()
                        .filter(tabletServer -> tabletServer.id() == 0)
                        .findFirst()
                        .orElseThrow(
                                () -> new IllegalStateException("Tablet server 0 does not exist."));
        List<NetUtils.Port> otherTabletServerPorts = reservePorts(previousTabletServers.size() - 1);

        try {
            // Stop tablet servers while the coordinator is still available for controlled
            // shutdown, then stop the coordinator before reusing their ports.
            for (ServerNode tabletServer : previousTabletServers) {
                extension.stopTabletServer(tabletServer.id());
            }
            waitUntil(
                    () -> zkClient.getSortedTabletServerList().length == 0,
                    Duration.ofMinutes(1),
                    "Tablet server nodes still exist in ZooKeeper");
            stopCoordinatorServer(extension, zkClient);

            extension.startCoordinatorServer(bindListener(swappedTabletServer.port()));
            extension.startTabletServer(
                    swappedTabletServer.id(), bindListener(previousCoordinator.port()));
            int newPortIndex = 0;
            for (ServerNode tabletServer : previousTabletServers) {
                if (tabletServer.id() != swappedTabletServer.id()) {
                    extension.startTabletServer(
                            tabletServer.id(),
                            bindListener(otherTabletServerPorts.get(newPortIndex).getPort()));
                    newPortIndex++;
                }
            }
            extension.waitUntilAllGatewayHasSameMetadata();

            ServerNode restartedCoordinator = extension.getCoordinatorServerNode();
            ServerNode restartedTabletServer =
                    getTabletServerNode(extension, swappedTabletServer.id());
            assertThat(extension.getCoordinatorServer()).isNotSameAs(previousCoordinatorServer);
            assertThat(restartedCoordinator.uid()).isEqualTo(previousCoordinator.uid());
            assertThat(restartedCoordinator.host()).isEqualTo(swappedTabletServer.host());
            assertThat(restartedCoordinator.port())
                    .isEqualTo(swappedTabletServer.port())
                    .isNotEqualTo(previousCoordinator.port());
            assertThat(restartedTabletServer.uid()).isEqualTo(swappedTabletServer.uid());
            assertThat(restartedTabletServer.host()).isEqualTo(previousCoordinator.host());
            assertThat(restartedTabletServer.port())
                    .isEqualTo(previousCoordinator.port())
                    .isNotEqualTo(swappedTabletServer.port());

            newPortIndex = 0;
            for (ServerNode tabletServer : previousTabletServers) {
                if (tabletServer.id() != swappedTabletServer.id()) {
                    ServerNode restartedNode = getTabletServerNode(extension, tabletServer.id());
                    assertThat(restartedNode.uid()).isEqualTo(tabletServer.uid());
                    assertThat(restartedNode.host()).isEqualTo(tabletServer.host());
                    assertThat(restartedNode.port())
                            .isEqualTo(otherTabletServerPorts.get(newPortIndex).getPort())
                            .isNotEqualTo(tabletServer.port());
                    newPortIndex++;
                }
            }
        } finally {
            IOUtils.closeAllQuietly(otherTabletServerPorts);
        }
    }

    private static void stopCoordinatorServer(
            FlussClusterExtension extension, ZooKeeperClient zkClient) throws Exception {
        extension.stopCoordinatorServer();
        waitUntil(
                () -> !zkClient.getCoordinatorLeaderAddress().isPresent(),
                Duration.ofMinutes(1),
                "Coordinator server node still exists in ZooKeeper");
    }

    private static List<NetUtils.Port> reservePorts(int portCount) {
        List<NetUtils.Port> ports = new ArrayList<>(portCount);
        try {
            for (int i = 0; i < portCount; i++) {
                ports.add(NetUtils.getAvailablePort());
            }
            return ports;
        } catch (RuntimeException e) {
            IOUtils.closeAllQuietly(ports);
            throw e;
        }
    }

    private static ServerNode getTabletServerNode(FlussClusterExtension extension, int serverId) {
        return extension.getTabletServerNodes().stream()
                .filter(serverNode -> serverNode.id() == serverId)
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Tablet server " + serverId + " does not exist."));
    }

    private static String bindListener(int port) {
        return String.format("FLUSS://localhost:%d", port);
    }
}

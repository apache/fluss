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

package org.apache.fluss.client.metadata;

import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.tablet.TestTabletServerGateway;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Testing class for metadata updater. */
public class TestingMetadataUpdater extends MetadataUpdater {
    private static final ServerNode COORDINATOR =
            new ServerNode(0, "localhost", 90, ServerType.COORDINATOR);
    private static final ServerNode NODE1 =
            new ServerNode(1, "localhost", 90, ServerType.TABLET_SERVER, "rack1");
    private static final ServerNode NODE2 =
            new ServerNode(2, "localhost", 91, ServerType.TABLET_SERVER, "rack2");
    private static final ServerNode NODE3 =
            new ServerNode(3, "localhost", 92, ServerType.TABLET_SERVER, "rack3");

    private final TestCoordinatorGateway coordinatorGateway;
    private final Map<Integer, TestTabletServerGateway> tabletServerGatewayMap;

    public TestingMetadataUpdater(Map<TablePath, TableInfo> tableInfos) {
        this(COORDINATOR, Arrays.asList(NODE1, NODE2, NODE3), tableInfos, null);
    }

    private TestingMetadataUpdater(
            ServerNode coordinatorServer,
            List<ServerNode> tabletServers,
            Map<TablePath, TableInfo> tableInfos,
            Map<Integer, TestTabletServerGateway> customGateways) {
        super(
                RpcClient.create(
                        new Configuration(), TestingClientMetricGroup.newInstance(), false),
                Cluster.empty());
        initializeCluster(coordinatorServer, tabletServers, tableInfos);
        coordinatorGateway = new TestCoordinatorGateway();
        if (customGateways != null) {
            tabletServerGatewayMap = customGateways;
        } else {
            tabletServerGatewayMap = new HashMap<>();
            for (ServerNode tabletServer : tabletServers) {
                tabletServerGatewayMap.put(
                        tabletServer.id(),
                        new TestTabletServerGateway(false, Collections.emptySet()));
            }
        }
    }

    /**
     * Create a builder for constructing TestingMetadataUpdater with custom gateways.
     *
     * @param tableInfos the table information map
     * @return a builder instance
     */
    public static Builder builder(Map<TablePath, TableInfo> tableInfos) {
        return new Builder(tableInfos);
    }

    /** Builder for TestingMetadataUpdater to support custom gateway configuration. */
    public static class Builder {
        private final Map<TablePath, TableInfo> tableInfos;
        private final Map<Integer, TestTabletServerGateway> customGateways = new HashMap<>();

        private Builder(Map<TablePath, TableInfo> tableInfos) {
            this.tableInfos = tableInfos;
        }

        /**
         * Set a custom gateway for a specific tablet server node.
         *
         * @param serverId the server id (1, 2, or 3 for default nodes)
         * @param gateway the custom gateway
         * @return this builder
         */
        public Builder withTabletServerGateway(int serverId, TestTabletServerGateway gateway) {
            customGateways.put(serverId, gateway);
            return this;
        }

        /**
         * Build the TestingMetadataUpdater instance.
         *
         * @return the configured TestingMetadataUpdater
         */
        public TestingMetadataUpdater build() {
            return new TestingMetadataUpdater(
                    COORDINATOR,
                    Arrays.asList(NODE1, NODE2, NODE3),
                    tableInfos,
                    customGateways.isEmpty() ? null : customGateways);
        }
    }

    public void updateCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public void setResponseLogicId(int serverId, int responseLogicId) {
        tabletServerGatewayMap.get(serverId).setResponseLogicId(responseLogicId);
    }

    @Override
    public void checkAndUpdateTableMetadata(Set<TablePath> tablePaths) {
        Set<TablePath> needUpdateTablePaths =
                tablePaths.stream()
                        .filter(tablePath -> !cluster.getTable(tablePath).isPresent())
                        .collect(Collectors.toSet());
        if (!needUpdateTablePaths.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "tables %s not found in TestingMetadataUpdater, "
                                    + "you need add it while construct updater",
                            needUpdateTablePaths));
        }
    }

    @Override
    public CoordinatorGateway newCoordinatorServerClient() {
        return coordinatorGateway;
    }

    public TabletServerGateway newRandomTabletServerClient() {
        return tabletServerGatewayMap.get(1);
    }

    @Override
    public TabletServerGateway newTabletServerClientForNode(int serverId) {
        if (cluster.getTabletServer(serverId) == null) {
            return null;
        } else {
            return tabletServerGatewayMap.get(serverId);
        }
    }

    private void initializeCluster(
            ServerNode coordinatorServer,
            List<ServerNode> tabletServers,
            Map<TablePath, TableInfo> tableInfos) {

        Map<Integer, ServerNode> tabletServerMap = new HashMap<>();
        tabletServers.forEach(tabletServer -> tabletServerMap.put(tabletServer.id(), tabletServer));

        int[] replicas = new int[tabletServers.size()];
        for (int i = 0; i < replicas.length; i++) {
            replicas[i] = tabletServers.get(i).id();
        }

        Map<PhysicalTablePath, List<BucketLocation>> tablePathToBucketLocations = new HashMap<>();
        Map<TablePath, Long> tableIdByPath = new HashMap<>();
        Map<TablePath, TableInfo> tableInfoByPath = new HashMap<>();
        tableInfos.forEach(
                (tablePath, tableInfo) -> {
                    long tableId = tableInfo.getTableId();
                    PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
                    tablePathToBucketLocations.put(
                            physicalTablePath,
                            Arrays.asList(
                                    new BucketLocation(
                                            physicalTablePath,
                                            tableId,
                                            0,
                                            tabletServers.get(0).id(),
                                            replicas),
                                    new BucketLocation(
                                            physicalTablePath,
                                            tableId,
                                            1,
                                            tabletServers.get(1).id(),
                                            replicas),
                                    new BucketLocation(
                                            physicalTablePath,
                                            tableId,
                                            2,
                                            tabletServers.get(2).id(),
                                            replicas)));
                    tableIdByPath.put(tablePath, tableId);
                    tableInfoByPath.put(tablePath, tableInfo);
                });
        cluster =
                new Cluster(
                        tabletServerMap,
                        coordinatorServer,
                        tablePathToBucketLocations,
                        tableIdByPath,
                        Collections.emptyMap(),
                        tableInfoByPath);
    }
}

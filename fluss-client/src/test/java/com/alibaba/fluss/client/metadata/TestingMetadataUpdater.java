/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.metadata;

import com.alibaba.fluss.cluster.BucketLocation;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.metrics.TestingClientMetricGroup;
import com.alibaba.fluss.server.coordinator.TestCoordinatorGateway;
import com.alibaba.fluss.server.tablet.TestTabletServerGateway;

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
            new ServerNode(1, "localhost", 90, ServerType.TABLET_SERVER);
    private static final ServerNode NODE2 =
            new ServerNode(2, "localhost", 91, ServerType.TABLET_SERVER);
    private static final ServerNode NODE3 =
            new ServerNode(3, "localhost", 92, ServerType.TABLET_SERVER);

    private final TestCoordinatorGateway coordinatorGateway;
    private final Map<Integer, TestTabletServerGateway> tabletServerGatewayMap;

    public TestingMetadataUpdater(Map<TablePath, TableInfo> tableInfos) {
        this(COORDINATOR, Arrays.asList(NODE1, NODE2, NODE3), tableInfos);
    }

    private TestingMetadataUpdater(
            ServerNode coordinatorServer,
            List<ServerNode> tabletServers,
            Map<TablePath, TableInfo> tableInfos) {
        super(
                RpcClient.create(new Configuration(), TestingClientMetricGroup.newInstance()),
                Cluster.empty());
        initializeCluster(coordinatorServer, tabletServers, tableInfos);
        coordinatorGateway = new TestCoordinatorGateway();
        tabletServerGatewayMap = new HashMap<>();
        for (ServerNode tabletServer : tabletServers) {
            tabletServerGatewayMap.put(tabletServer.id(), new TestTabletServerGateway(false));
        }
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
        return tabletServerGatewayMap.get(serverId);
    }

    private void initializeCluster(
            ServerNode coordinatorServer,
            List<ServerNode> tabletServers,
            Map<TablePath, TableInfo> tableInfos) {

        Map<Integer, ServerNode> tabletServerMap = new HashMap<>();
        tabletServers.forEach(tabletServer -> tabletServerMap.put(tabletServer.id(), tabletServer));

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
                                            tabletServers.get(0),
                                            tabletServers.toArray(new ServerNode[0])),
                                    new BucketLocation(
                                            physicalTablePath,
                                            tableId,
                                            1,
                                            tabletServers.get(1),
                                            tabletServers.toArray(new ServerNode[0])),
                                    new BucketLocation(
                                            physicalTablePath,
                                            tableId,
                                            2,
                                            tabletServers.get(2),
                                            tabletServers.toArray(new ServerNode[0]))));
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

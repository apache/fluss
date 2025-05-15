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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.coordinator.CoordinatorContext;
import com.alibaba.fluss.server.coordinator.CoordinatorServer;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** The implement of {@link ServerMetadataCache} for {@link CoordinatorServer}. */
public class CoordinatorServerMetadataCache implements ServerMetadataCache {

    private final CoordinatorContext coordinatorContext;

    public CoordinatorServerMetadataCache(CoordinatorContext coordinatorContext) {
        this.coordinatorContext = coordinatorContext;
    }

    @Override
    public boolean isAliveTabletServer(int serverId) {
        Map<Integer, ServerInfo> aliveTabletServer = coordinatorContext.getLiveTabletServers();
        return aliveTabletServer.containsKey(serverId);
    }

    @Override
    public Optional<ServerNode> getTabletServer(int serverId, String listenerName) {
        Map<Integer, ServerInfo> aliveTabletServer = coordinatorContext.getLiveTabletServers();
        return aliveTabletServer.containsKey(serverId)
                ? Optional.ofNullable(aliveTabletServer.get(serverId).node(listenerName))
                : Optional.empty();
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName) {
        Map<Integer, ServerNode> serverNodes = new HashMap<>();
        for (Map.Entry<Integer, ServerInfo> entry :
                coordinatorContext.getLiveTabletServers().entrySet()) {
            ServerNode serverNode = entry.getValue().node(listenerName);
            if (serverNode != null) {
                serverNodes.put(entry.getKey(), serverNode);
            }
        }
        return serverNodes;
    }

    @Override
    public @Nullable ServerNode getCoordinatorServer(String listenerName) {
        ServerInfo coordinatorServer = coordinatorContext.getCoordinatorServerInfo();
        return coordinatorServer == null ? null : coordinatorServer.node(listenerName);
    }

    @Override
    public Set<TabletServerInfo> getAliveTabletServerInfos() {
        Set<TabletServerInfo> tabletServerInfos = new HashSet<>();
        coordinatorContext
                .getLiveTabletServers()
                .values()
                .forEach(
                        serverInfo ->
                                tabletServerInfos.add(
                                        new TabletServerInfo(serverInfo.id(), serverInfo.rack())));
        return Collections.unmodifiableSet(tabletServerInfos);
    }

    @Override
    public Optional<TablePath> getTablePath(long tableId) {
        return Optional.ofNullable(coordinatorContext.getTablePathById(tableId));
    }

    public Optional<String> getPartitionName(long partitionId) {
        return Optional.ofNullable(coordinatorContext.getPartitionName(partitionId));
    }
}

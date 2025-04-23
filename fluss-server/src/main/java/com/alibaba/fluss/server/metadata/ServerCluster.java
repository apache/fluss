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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An immutable representation of a subset of the server nodes in the fluss cluster. Compared to
 * {@link com.alibaba.fluss.cluster.Cluster}, it includes all the endpoints of the server nodes.
 */
public class ServerCluster {
    private final @Nullable ServerInfo coordinatorServer;
    private final Map<Integer, ServerInfo> aliveTabletServers;

    public ServerCluster(
            @Nullable ServerInfo coordinatorServer, Map<Integer, ServerInfo> aliveTabletServers) {
        this.coordinatorServer = coordinatorServer;
        this.aliveTabletServers = aliveTabletServers;
    }

    /** Create an empty cluster instance with no nodes and no table-buckets. */
    public static ServerCluster empty() {
        return new ServerCluster(null, Collections.emptyMap());
    }

    public Optional<ServerNode> getCoordinatorServer(String listenerName) {
        return coordinatorServer == null
                ? Optional.empty()
                : Optional.ofNullable(coordinatorServer.node(listenerName));
    }

    public Optional<ServerNode> getAliveTabletServersById(int serverId, String listenerName) {
        return (aliveTabletServers == null || !aliveTabletServers.containsKey(serverId))
                ? Optional.empty()
                : Optional.ofNullable(aliveTabletServers.get(serverId).node(listenerName));
    }

    public Map<Integer, ServerNode> getAliveTabletServers(String listenerName) {
        Map<Integer, ServerNode> serverNodes = new HashMap<>();
        for (Map.Entry<Integer, ServerInfo> entry : aliveTabletServers.entrySet()) {
            ServerNode serverNode = entry.getValue().node(listenerName);
            if (serverNode != null) {
                serverNodes.put(entry.getKey(), serverNode);
            }
        }
        return serverNodes;
    }

    public Set<TabletServerInfo> getAliveTabletServerInfos() {
        Set<TabletServerInfo> tabletServerInfos = new HashSet<>();
        aliveTabletServers
                .values()
                .forEach(
                        serverInfo ->
                                tabletServerInfos.add(
                                        new TabletServerInfo(serverInfo.id(), serverInfo.rack())));
        return Collections.unmodifiableSet(tabletServerInfos);
    }
}

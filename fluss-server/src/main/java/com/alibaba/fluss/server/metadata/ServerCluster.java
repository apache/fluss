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

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

    public ServerNode getCoordinatorServer(String listenerName) {
        if (coordinatorServer == null || coordinatorServer.endpoint(listenerName) == null) {
            return null;
        }
        Endpoint endpoint = coordinatorServer.endpoint(listenerName);
        return new ServerNode(
                coordinatorServer.id(),
                endpoint.getHost(),
                endpoint.getPort(),
                ServerType.COORDINATOR);
    }

    public ServerNode getAliveTabletServersById(int serverId, String listenerName) {
        if (aliveTabletServers == null
                || aliveTabletServers.get(serverId) == null
                || aliveTabletServers.get(serverId).endpoint(listenerName) == null) {
            return null;
        }

        Endpoint endpoint = aliveTabletServers.get(serverId).endpoint(listenerName);
        return new ServerNode(
                serverId, endpoint.getHost(), endpoint.getPort(), ServerType.TABLET_SERVER);
    }

    public Map<Integer, ServerNode> getAliveTabletServers(String listenerName) {
        Map<Integer, ServerNode> serverNodes = new HashMap<>();
        for (Map.Entry<Integer, ServerInfo> entry : aliveTabletServers.entrySet()) {
            Endpoint endpoint = entry.getValue().endpoint(listenerName);
            if (endpoint != null) {
                serverNodes.put(
                        entry.getKey(),
                        new ServerNode(
                                entry.getKey(),
                                endpoint.getHost(),
                                endpoint.getPort(),
                                ServerType.TABLET_SERVER));
            }
        }
        return serverNodes;
    }

    public Set<Integer> getAliveTabletServerIds() {
        return Collections.unmodifiableSet(aliveTabletServers.keySet());
    }
}

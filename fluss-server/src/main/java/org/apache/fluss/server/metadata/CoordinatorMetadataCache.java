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

package org.apache.fluss.server.metadata;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.server.coordinator.CoordinatorServer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/** The implement of {@link ServerMetadataCache} for {@link CoordinatorServer}. */
public class CoordinatorMetadataCache implements ServerMetadataCache {

    private final Lock metadataLock = new ReentrantLock();

    @GuardedBy("metadataLock")
    private volatile NodeMetadataSnapshot metadataSnapshot =
            new NodeMetadataSnapshot(null, Collections.emptyMap());

    public CoordinatorMetadataCache() {}

    @Override
    public boolean isAliveTabletServer(int serverId) {
        return metadataSnapshot.aliveTabletServers.containsKey(serverId);
    }

    @Override
    public Optional<ServerNode> getTabletServer(int serverId, String listenerName) {
        Map<Integer, ServerInfo> aliveTabletServers = metadataSnapshot.aliveTabletServers;
        return aliveTabletServers.containsKey(serverId)
                ? Optional.ofNullable(aliveTabletServers.get(serverId).node(listenerName))
                : Optional.empty();
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName) {
        Map<Integer, ServerInfo> aliveTabletServers = metadataSnapshot.aliveTabletServers;
        Map<Integer, ServerNode> serverNodes = new HashMap<>();
        for (Map.Entry<Integer, ServerInfo> entry : aliveTabletServers.entrySet()) {
            ServerNode serverNode = entry.getValue().node(listenerName);
            if (serverNode != null) {
                serverNodes.put(entry.getKey(), serverNode);
            }
        }
        return serverNodes;
    }

    @Override
    public @Nullable ServerNode getCoordinatorServer(String listenerName) {
        ServerInfo coordinatorServer = metadataSnapshot.coordinatorServer;
        return coordinatorServer != null ? coordinatorServer.node(listenerName) : null;
    }

    @Override
    public Set<TabletServerInfo> getAliveTabletServerInfos() {
        Map<Integer, ServerInfo> aliveTabletServers = metadataSnapshot.aliveTabletServers;
        Set<TabletServerInfo> tabletServerInfos = new HashSet<>();
        aliveTabletServers
                .values()
                .forEach(
                        serverInfo ->
                                tabletServerInfos.add(
                                        new TabletServerInfo(serverInfo.id(), serverInfo.rack())));
        return Collections.unmodifiableSet(tabletServerInfos);
    }

    public void updateMetadata(ServerInfo coordinatorServer, Set<ServerInfo> serverInfoSet) {
        inLock(
                metadataLock,
                () -> {
                    Map<Integer, ServerInfo> newAliveTableServers = new HashMap<>();
                    for (ServerInfo tabletServer : serverInfoSet) {
                        newAliveTableServers.put(tabletServer.id(), tabletServer);
                    }

                    this.metadataSnapshot =
                            new NodeMetadataSnapshot(coordinatorServer, newAliveTableServers);
                });
    }

    private static class NodeMetadataSnapshot {
        final @Nullable ServerInfo coordinatorServer;
        final Map<Integer, ServerInfo> aliveTabletServers;

        private NodeMetadataSnapshot(
                @Nullable ServerInfo coordinatorServer,
                Map<Integer, ServerInfo> aliveTabletServers) {
            this.coordinatorServer = coordinatorServer;
            this.aliveTabletServers = aliveTabletServers;
        }
    }
}

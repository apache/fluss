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

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.TabletServerInfo;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Metadata cache for server. */
public interface ServerMetadataCache {

    /**
     * Get the coordinator server node.
     *
     * @return the coordinator server node
     */
    @Nullable
    ServerNode getCoordinatorServer(String listenerName);

    /**
     * Check whether the tablet server id related tablet server node is alive.
     *
     * @param serverId the tablet server id
     * @return true if the server is alive, false otherwise
     */
    boolean isAliveTabletServer(int serverId);

    /**
     * Get the tablet server.
     *
     * @param serverId the tablet server id
     * @return the tablet server node
     */
    Optional<ServerNode> getTabletServer(int serverId, String listenerName);

    /**
     * Get all alive tablet server nodes.
     *
     * @return all alive tablet server nodes
     */
    Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName);

    Set<TabletServerInfo> getAliveTabletServerInfos();

    /** Get ids of all alive tablet server nodes. */
    default TabletServerInfo[] getLiveServers() {
        Set<TabletServerInfo> aliveTabletServerInfos = getAliveTabletServerInfos();
        TabletServerInfo[] server = new TabletServerInfo[aliveTabletServerInfos.size()];
        Iterator<TabletServerInfo> iterator = aliveTabletServerInfos.iterator();
        for (int i = 0; i < aliveTabletServerInfos.size(); i++) {
            server[i] = iterator.next();
        }
        return server;
    }
}

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
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.server.metadata.ServerMetadataCache;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** An implement of {@link ServerMetadataCache} for testing purpose. */
public class TestingServerMetadataCache implements ServerMetadataCache {

    private final TabletServerInfo[] tabletServerInfos;

    public TestingServerMetadataCache(int serverNums) {
        TabletServerInfo[] tabletServerInfos = new TabletServerInfo[serverNums];
        for (int i = 0; i < serverNums; i++) {
            tabletServerInfos[i] = new TabletServerInfo(i, "rack" + i);
        }
        this.tabletServerInfos = tabletServerInfos;
    }

    @Override
    public ServerNode getCoordinatorServer(String listenerName) {
        return null;
    }

    @Override
    public boolean isAliveTabletServer(int serverId) {
        return false;
    }

    @Override
    public Optional<ServerNode> getTabletServer(int serverId, String listenerName) {
        return Optional.empty();
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName) {
        return Collections.emptyMap();
    }

    @Override
    public Set<TabletServerInfo> getAliveTabletServerInfos() {
        return Collections.emptySet();
    }

    public TabletServerInfo[] getLiveServers() {
        return tabletServerInfos;
    }
}

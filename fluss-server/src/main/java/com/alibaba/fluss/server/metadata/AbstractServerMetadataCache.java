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

import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.server.coordinator.CoordinatorServer;
import com.alibaba.fluss.server.tablet.TabletServer;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * The abstract server metadata cache to cache the cluster metadata info. This cache is updated
 * through UpdateMetadataRequest from the {@link CoordinatorServer}. {@link CoordinatorServer} and
 * each {@link TabletServer} maintains the same cache, asynchronously.
 */
public abstract class AbstractServerMetadataCache implements ServerMetadataCache {

    /**
     * This is cache state. every Cluster instance is immutable, and updates (performed under a
     * lock) replace the value with a completely new one. this means reads (which are not under any
     * lock) need to grab the value of this ONCE and retain that read copy for the duration of their
     * operation.
     *
     * <p>multiple reads of this value risk getting different snapshots.
     */
    protected volatile Cluster clusterMetadata;

    public AbstractServerMetadataCache() {
        // no coordinator server address while creating.
        this.clusterMetadata = Cluster.empty();
    }

    @Override
    public boolean isAliveTabletServer(int serverId) {
        Map<Integer, ServerNode> aliveTabletServersById = clusterMetadata.getAliveTabletServers();
        return aliveTabletServersById.containsKey(serverId);
    }

    @Override
    public @Nullable ServerNode getTabletServer(int serverId) {
        return clusterMetadata.getAliveTabletServerById(serverId).orElse(null);
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers() {
        return clusterMetadata.getAliveTabletServers();
    }

    @Override
    public @Nullable ServerNode getCoordinatorServer() {
        return clusterMetadata.getCoordinatorServer();
    }
}

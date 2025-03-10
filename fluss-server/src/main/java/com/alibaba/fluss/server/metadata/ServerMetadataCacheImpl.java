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

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/** The default implement of {@link ServerMetadataCache}. */
public class ServerMetadataCacheImpl extends AbstractServerMetadataCache {
    private final Lock bucketMetadataLock = new ReentrantLock();

    public ServerMetadataCacheImpl() {
        super();
    }

    @Override
    public void updateMetadata(ClusterMetadataInfo clusterMetadataInfo) {
        inLock(
                bucketMetadataLock,
                () -> {
                    // 1. update coordinator server.
                    ServerNode coordinatorServer =
                            clusterMetadataInfo.getCoordinatorServer().orElse(null);

                    // 2. Update the alive table servers. We always use the new alive table servers
                    // to replace the old alive table servers.
                    HashMap<Integer, ServerNode> newAliveTableServers = new HashMap<>();
                    Set<ServerNode> aliveTabletServers =
                            clusterMetadataInfo.getAliveTabletServers();
                    for (ServerNode tabletServer : aliveTabletServers) {
                        newAliveTableServers.put(tabletServer.id(), tabletServer);
                    }

                    clusterMetadata =
                            new Cluster(
                                    newAliveTableServers,
                                    coordinatorServer,
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap());
                });
    }
}

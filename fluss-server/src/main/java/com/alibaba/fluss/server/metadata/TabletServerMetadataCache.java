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
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.tablet.TabletServer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static com.alibaba.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_NAME;
import static com.alibaba.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static com.alibaba.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/** The implement of {@link ServerMetadataCache} for {@link TabletServer}. */
public class TabletServerMetadataCache implements ServerMetadataCache {

    private final Lock metadataLock = new ReentrantLock();

    /**
     * This is cache state. every Cluster instance is immutable, and updates (performed under a
     * lock) replace the value with a completely new one. this means reads (which are not under any
     * lock) need to grab the value of this ONCE and retain that read copy for the duration of their
     * operation.
     *
     * <p>multiple reads of this value risk getting different snapshots.
     */
    @GuardedBy("bucketMetadataLock")
    private volatile ServerMetadataSnapshot serverMetadataSnapshot;

    public TabletServerMetadataCache() {
        this.serverMetadataSnapshot = ServerMetadataSnapshot.empty();
    }

    @Override
    public boolean isAliveTabletServer(int serverId) {
        Set<TabletServerInfo> tabletServerInfoList =
                serverMetadataSnapshot.getAliveTabletServerInfos();
        for (TabletServerInfo tabletServer : tabletServerInfoList) {
            if (tabletServer.getId() == serverId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Optional<ServerNode> getTabletServer(int serverId, String listenerName) {
        return serverMetadataSnapshot.getAliveTabletServersById(serverId, listenerName);
    }

    @Override
    public Map<Integer, ServerNode> getAllAliveTabletServers(String listenerName) {
        return serverMetadataSnapshot.getAliveTabletServers(listenerName);
    }

    @Override
    public @Nullable ServerNode getCoordinatorServer(String listenerName) {
        return serverMetadataSnapshot.getCoordinatorServer(listenerName);
    }

    @Override
    public Set<TabletServerInfo> getAliveTabletServerInfos() {
        return serverMetadataSnapshot.getAliveTabletServerInfos();
    }

    public Optional<TablePath> getTablePath(long tableId) {
        return serverMetadataSnapshot.getTablePath(tableId);
    }

    public Optional<String> getPartitionName(long partitionId) {
        return serverMetadataSnapshot.getPartitionName(partitionId);
    }

    public void updateClusterMetadata(ClusterMetadata clusterMetadata) {
        inLock(
                metadataLock,
                () -> {
                    // 1. update coordinator server.
                    ServerInfo coordinatorServer = clusterMetadata.getCoordinatorServer();

                    // 2. Update the alive table servers. We always use the new alive table servers
                    // to replace the old alive table servers.
                    Map<Integer, ServerInfo> newAliveTableServers = new HashMap<>();
                    Set<ServerInfo> aliveTabletServers = clusterMetadata.getAliveTabletServers();
                    for (ServerInfo tabletServer : aliveTabletServers) {
                        newAliveTableServers.put(tabletServer.id(), tabletServer);
                    }

                    // 3. update table metadata. Always partial update.
                    // TODO Currently, it only updates the tableIdByPath, we need to update all the
                    // table metadata. Trace by: https://github.com/alibaba/fluss/issues/900
                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getTableIdByPath());
                    for (TableMetadata tableMetadata : clusterMetadata.getTableMetadataList()) {
                        TableInfo tableInfo = tableMetadata.getTableInfo();
                        TablePath tablePath = tableInfo.getTablePath();
                        long tableId = tableInfo.getTableId();
                        if (tableId == DELETED_TABLE_ID) {
                            tableIdByPath.remove(tablePath);
                        } else if (tablePath == DELETED_TABLE_PATH) {
                            serverMetadataSnapshot
                                    .getTablePath(tableId)
                                    .ifPresent(tableIdByPath::remove);
                        } else {
                            tableIdByPath.put(tablePath, tableId);
                        }
                    }

                    Map<Long, TablePath> newPathByTableId = new HashMap<>();
                    tableIdByPath.forEach(
                            ((tablePath, tableId) -> newPathByTableId.put(tableId, tablePath)));

                    // 4. update partition metadata. Always partial update.
                    // TODO Currently, it only updates the partitionIdByPath, we need to update all
                    // the partition metadata. Trace by: https://github.com/alibaba/fluss/issues/900
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getPartitionIdByPath());
                    for (PartitionMetadata partitionMetadata :
                            clusterMetadata.getPartitionMetadataList()) {
                        long tableId = partitionMetadata.getTableId();
                        TablePath tablePath = newPathByTableId.get(tableId);
                        String partitionName = partitionMetadata.getPartitionName();
                        PhysicalTablePath physicalTablePath =
                                PhysicalTablePath.of(tablePath, partitionName);
                        long partitionId = partitionMetadata.getPartitionId();
                        if (partitionId == DELETED_PARTITION_ID) {
                            partitionIdByPath.remove(physicalTablePath);
                        } else if (partitionName.equals(DELETED_PARTITION_NAME)) {
                            serverMetadataSnapshot
                                    .getPartitionName(partitionId)
                                    .ifPresent(
                                            pName ->
                                                    partitionIdByPath.remove(
                                                            PhysicalTablePath.of(
                                                                    tablePath, pName)));
                        } else {
                            partitionIdByPath.put(physicalTablePath, partitionId);
                        }
                    }

                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    coordinatorServer,
                                    newAliveTableServers,
                                    tableIdByPath,
                                    newPathByTableId,
                                    partitionIdByPath);
                });
    }
}

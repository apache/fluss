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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.Cluster;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * An immutable representation of a full set of the server nodes in the fluss cluster. Every
 * MetadataSnapshot instance is immutable, and updates (performed under a lock) replace the value
 * with a completely new one. this means reads (which are not under any lock) need to grab/the value
 * of this var (into a val) ONCE and retain that read copy for the duration of their operation.
 * multiple reads of this value risk getting different snapshots.
 *
 * <p>Compared to {@link Cluster}, it includes all the endpoints of the server nodes.
 */
public class ServerMetadataSnapshot {
    private final @Nullable ServerInfo coordinatorServer;
    private final Map<Integer, ServerInfo> aliveTabletServers;
    private final Map<TablePath, Long> tableIdByPath;
    private final Map<Long, TablePath> pathByTableId;
    // partition table.
    private final Map<PhysicalTablePath, Long> partitionIdByPath;
    private final Map<Long, PhysicalTablePath> physicalPathByPartitionId;

    // a map of bucket metadata of none-partition table, table_id -> <bucket, bucketMetadata>
    private final Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables;

    // a map of bucket metadata of partition table, partition_id -> <bucket,
    // bucketMetadata>
    private final Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions;

    public ServerMetadataSnapshot(
            @Nullable ServerInfo coordinatorServer,
            Map<Integer, ServerInfo> aliveTabletServers,
            Map<TablePath, Long> tableIdByPath,
            Map<Long, TablePath> pathByTableId,
            Map<PhysicalTablePath, Long> partitionIdByPath,
            Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables,
            Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions) {
        this.coordinatorServer = coordinatorServer;
        this.aliveTabletServers = Collections.unmodifiableMap(aliveTabletServers);

        this.tableIdByPath = Collections.unmodifiableMap(tableIdByPath);
        this.pathByTableId = Collections.unmodifiableMap(pathByTableId);

        this.partitionIdByPath = Collections.unmodifiableMap(partitionIdByPath);
        Map<Long, PhysicalTablePath> tempPhysicalPathByPartitionId = new HashMap<>();
        partitionIdByPath.forEach(
                ((physicalTablePath, partitionId) ->
                        tempPhysicalPathByPartitionId.put(partitionId, physicalTablePath)));
        this.physicalPathByPartitionId = Collections.unmodifiableMap(tempPhysicalPathByPartitionId);

        this.bucketMetadataMapForTables = Collections.unmodifiableMap(bucketMetadataMapForTables);
        this.bucketMetadataMapForPartitions =
                Collections.unmodifiableMap(bucketMetadataMapForPartitions);
    }

    /** Create an empty cluster instance with no nodes and no table-buckets. */
    public static ServerMetadataSnapshot empty() {
        return new ServerMetadataSnapshot(
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    public ServerNode getCoordinatorServer(String listenerName) {
        return coordinatorServer == null ? null : coordinatorServer.node(listenerName);
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

    public OptionalLong getTableId(TablePath tablePath) {
        Long tableId = tableIdByPath.get(tablePath);
        return tableId == null ? OptionalLong.empty() : OptionalLong.of(tableId);
    }

    public Optional<TablePath> getTablePath(long tableId) {
        return Optional.ofNullable(pathByTableId.get(tableId));
    }

    public Map<TablePath, Long> getTableIdByPath() {
        return tableIdByPath;
    }

    public Optional<Long> getPartitionId(PhysicalTablePath physicalTablePath) {
        return Optional.ofNullable(partitionIdByPath.get(physicalTablePath));
    }

    public Optional<PhysicalTablePath> getPhysicalTablePath(long partitionId) {
        return Optional.ofNullable(physicalPathByPartitionId.get(partitionId));
    }

    public Map<Integer, BucketMetadata> getBucketMetadataForTable(long tableId) {
        return bucketMetadataMapForTables.getOrDefault(tableId, Collections.emptyMap());
    }

    public Map<Integer, BucketMetadata> getBucketMetadataForPartition(long partitionId) {
        return bucketMetadataMapForPartitions.getOrDefault(partitionId, Collections.emptyMap());
    }

    public Map<PhysicalTablePath, Long> getPartitionIdByPath() {
        return partitionIdByPath;
    }

    public Map<Long, Map<Integer, BucketMetadata>> getBucketMetadataMapForTables() {
        return bucketMetadataMapForTables;
    }

    public Map<Long, Map<Integer, BucketMetadata>> getBucketMetadataMapForPartitions() {
        return bucketMetadataMapForPartitions;
    }

    @VisibleForTesting
    public @Nullable ServerInfo getCoordinatorServer() {
        return coordinatorServer;
    }

    @VisibleForTesting
    public Map<Integer, ServerInfo> getAliveTabletServers() {
        return aliveTabletServers;
    }
}

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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.TabletServerInfo;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.tablet.TabletServer;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_NAME;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

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

    /**
     * Whether any metadata update from the coordinator has been applied. Bucket-count validation
     * must not run against the empty startup snapshot.
     */
    private volatile boolean initialMetadataApplied;

    private final MetadataManager metadataManager;

    private final ServerSchemaCache serverSchemaCache;

    public TabletServerMetadataCache(MetadataManager metadataManager) {
        this.serverMetadataSnapshot = ServerMetadataSnapshot.empty();
        this.metadataManager = metadataManager;
        this.serverSchemaCache = new ServerSchemaCache(metadataManager);
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

    public Optional<PhysicalTablePath> getPhysicalTablePath(long partitionId) {
        return serverMetadataSnapshot.getPhysicalTablePath(partitionId);
    }

    public Optional<TableMetadata> getTableMetadata(TablePath tablePath) {
        // Only get data from cache, do not access ZK.
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;
        OptionalLong tableIdOpt = snapshot.getTableId(tablePath);
        if (!tableIdOpt.isPresent()) {
            return Optional.empty();
        }

        long tableId = tableIdOpt.getAsLong();

        // Try to get table info from ZK only if we have the table ID in cache
        try {
            TableInfo tableInfo = metadataManager.getTable(tablePath);
            List<BucketMetadata> bucketMetadataList =
                    new ArrayList<>(snapshot.getBucketMetadataForTable(tableId).values());
            return Optional.of(new TableMetadata(tableInfo, bucketMetadataList));
        } catch (Exception e) {
            // If table doesn't exist in ZK but exists in cache, return empty
            // This maintains backward compatibility while fixing the semantic issue
            return Optional.empty();
        }
    }

    public SchemaGetter subscribeWithInitialSchema(
            TablePath tablePath, long tableId, int initialSchemaId, Schema initialSchema) {
        return serverSchemaCache.subscribeWithInitialSchema(
                tableId, tablePath, initialSchemaId, initialSchema);
    }

    public void updateLatestSchema(long tableId, SchemaInfo schemaInfo) {
        serverSchemaCache.updateLatestSchema(
                tableId, (short) schemaInfo.getSchemaId(), schemaInfo.getSchema());
    }

    public Errors validateBucketCount(
            long tableId, @Nullable Long partitionId, int requestBucketCount) {
        if (!initialMetadataApplied) {
            // Metadata not loaded yet; reject instead of validating against an empty snapshot.
            return Errors.STALE_METADATA;
        }
        return serverMetadataSnapshot.validateBucketCount(tableId, partitionId, requestBucketCount);
    }

    public Optional<PartitionMetadata> getPartitionMetadata(PhysicalTablePath partitionPath) {
        TablePath tablePath = partitionPath.getTablePath();
        String partitionName = partitionPath.getPartitionName();
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;

        OptionalLong tableIdOpt = snapshot.getTableId(tablePath);
        Optional<Long> partitionIdOpt = snapshot.getPartitionId(partitionPath);
        if (tableIdOpt.isPresent() && partitionIdOpt.isPresent()) {
            long tableId = tableIdOpt.getAsLong();
            long partitionId = partitionIdOpt.get();
            List<BucketMetadata> bucketMetadataList =
                    new ArrayList<>(snapshot.getBucketMetadataForPartition(partitionId).values());
            // prefer the explicit bucket count sent by the coordinator; the merged bucket
            // metadata list may be transiently partial during incremental updates
            Integer bucketCount =
                    snapshot.getPartitionBucketCount(new TablePartition(tableId, partitionId));
            return Optional.of(
                    new PartitionMetadata(
                            tableId,
                            partitionName,
                            partitionId,
                            bucketMetadataList,
                            bucketCount != null ? bucketCount : bucketMetadataList.size()));
        } else {

            return Optional.empty();
        }
    }

    public boolean contains(TableBucket tableBucket) {
        return serverMetadataSnapshot.contains(tableBucket);
    }

    /**
     * Applies a partial cluster metadata update.
     *
     * @param clusterMetadata the partial cluster metadata to apply
     * @return the real table IDs identified by table-deletion tombstones in this update; partition
     *     deletion tombstones are not included
     */
    public Set<Long> updateClusterMetadata(ClusterMetadata clusterMetadata) {
        return inLock(
                metadataLock,
                () -> {
                    Set<Long> deletedTableIds = new HashSet<>();

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
                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getTableIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(serverMetadataSnapshot.getBucketMetadataMapForTables());
                    Map<Long, Long> bucketLayoutEpochByTableId =
                            new HashMap<>(serverMetadataSnapshot.getBucketLayoutEpochByTableId());

                    for (TableMetadata tableMetadata : clusterMetadata.getTableMetadataList()) {
                        TableInfo tableInfo = tableMetadata.getTableInfo();
                        TablePath tablePath = tableInfo.getTablePath();
                        long tableId = tableInfo.getTableId();

                        if (tableId == DELETED_TABLE_ID) {
                            Long removedTableId = tableIdByPath.remove(tablePath);
                            if (removedTableId != null) {
                                bucketMetadataMapForTables.remove(removedTableId);
                                deletedTableIds.add(removedTableId);
                                bucketLayoutEpochByTableId.remove(removedTableId);
                            }
                        } else if (tablePath == DELETED_TABLE_PATH) {
                            serverMetadataSnapshot
                                    .getTablePath(tableId)
                                    .ifPresent(tableIdByPath::remove);
                            bucketMetadataMapForTables.remove(tableId);
                            deletedTableIds.add(tableId);
                            bucketLayoutEpochByTableId.remove(tableId);
                        } else {
                            // Ignore an older UpdateMetadata for this table to prevent it from
                            // overwriting newer state when messages arrive out of order.
                            long newEpoch = tableInfo.getBucketLayoutEpoch();
                            long currentEpoch =
                                    bucketLayoutEpochByTableId.getOrDefault(tableId, 0L);
                            if (newEpoch < currentEpoch) {
                                continue;
                            }
                            bucketLayoutEpochByTableId.put(tableId, newEpoch);

                            // Update schema metadata.
                            // todo: apply schema id and schema info if needs
                            int schemaId = tableInfo.getSchemaId();
                            Schema schema = tableInfo.getSchema();
                            serverSchemaCache.updateLatestSchema(tableId, (short) schemaId, schema);

                            tableIdByPath.put(tablePath, tableId);
                            tableMetadata
                                    .getBucketMetadataList()
                                    .forEach(
                                            bucketMetadata ->
                                                    bucketMetadataMapForTables
                                                            .computeIfAbsent(
                                                                    tableId, k -> new HashMap<>())
                                                            .put(
                                                                    bucketMetadata.getBucketId(),
                                                                    bucketMetadata));
                        }
                    }

                    Map<Long, TablePath> newPathByTableId = new HashMap<>();
                    tableIdByPath.forEach(
                            ((tablePath, tableId) -> newPathByTableId.put(tableId, tablePath)));

                    // 4. update partition metadata. Always partial update.
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getPartitionIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(
                                    serverMetadataSnapshot.getBucketMetadataMapForPartitions());
                    Map<TablePartition, Integer> partitionBucketCounts =
                            new HashMap<>(serverMetadataSnapshot.getPartitionBucketCounts());

                    for (PartitionMetadata partitionMetadata :
                            clusterMetadata.getPartitionMetadataList()) {
                        long tableId = partitionMetadata.getTableId();
                        TablePath tablePath = newPathByTableId.get(tableId);
                        String partitionName = partitionMetadata.getPartitionName();
                        PhysicalTablePath physicalTablePath =
                                PhysicalTablePath.of(tablePath, partitionName);
                        long partitionId = partitionMetadata.getPartitionId();
                        if (partitionId == DELETED_PARTITION_ID) {
                            Long removedPartitionId = partitionIdByPath.remove(physicalTablePath);
                            if (removedPartitionId != null) {
                                bucketMetadataMapForPartitions.remove(removedPartitionId);
                                partitionBucketCounts
                                        .keySet()
                                        .removeIf(k -> k.getPartitionId() == removedPartitionId);
                            }
                        } else if (partitionName.equals(DELETED_PARTITION_NAME)) {
                            serverMetadataSnapshot
                                    .getPhysicalTablePath(partitionId)
                                    .ifPresent(partitionIdByPath::remove);
                            bucketMetadataMapForPartitions.remove(partitionId);
                            partitionBucketCounts
                                    .keySet()
                                    .removeIf(k -> k.getPartitionId() == partitionId);
                        } else {
                            partitionIdByPath.put(physicalTablePath, partitionId);
                            mergePartitionBucketCount(
                                    partitionBucketCounts, tableId, partitionId, partitionMetadata);
                            partitionMetadata
                                    .getBucketMetadataList()
                                    .forEach(
                                            bucketMetadata ->
                                                    bucketMetadataMapForPartitions
                                                            .computeIfAbsent(
                                                                    partitionId,
                                                                    k -> new HashMap<>())
                                                            .put(
                                                                    bucketMetadata.getBucketId(),
                                                                    bucketMetadata));
                        }
                    }

                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    coordinatorServer,
                                    newAliveTableServers,
                                    tableIdByPath,
                                    newPathByTableId,
                                    partitionIdByPath,
                                    bucketMetadataMapForTables,
                                    bucketMetadataMapForPartitions,
                                    partitionBucketCounts,
                                    bucketLayoutEpochByTableId);
                    initialMetadataApplied = true;
                    return deletedTableIds;
                });
    }

    @VisibleForTesting
    public void clearTableMetadata() {
        inLock(
                metadataLock,
                () -> {
                    ServerInfo coordinatorServer = serverMetadataSnapshot.getCoordinatorServer();
                    Map<Integer, ServerInfo> aliveTabletServers =
                            serverMetadataSnapshot.getAliveTabletServers();
                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    coordinatorServer,
                                    aliveTabletServers,
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    Collections.emptyMap());
                    initialMetadataApplied = false;
                });
    }

    /**
     * Update a single table metadata to the local cache. This method is thread-safe and will merge
     * the new table metadata with existing cache.
     *
     * @param tableMetadata the table metadata to update
     */
    public void updateTableMetadata(TableMetadata tableMetadata) {
        inLock(
                metadataLock,
                () -> {
                    TableInfo tableInfo = tableMetadata.getTableInfo();
                    TablePath tablePath = tableInfo.getTablePath();
                    long tableId = tableInfo.getTableId();

                    // Get current snapshot
                    ServerMetadataSnapshot currentSnapshot = serverMetadataSnapshot;

                    // Ignore an older UpdateMetadata for this table to prevent it from
                    // overwriting newer state when messages arrive out of order.
                    long newEpoch = tableInfo.getBucketLayoutEpoch();
                    long currentEpoch =
                            currentSnapshot
                                    .getBucketLayoutEpochByTableId()
                                    .getOrDefault(tableId, 0L);
                    if (newEpoch < currentEpoch) {
                        return;
                    }

                    // Create new maps based on current state
                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(currentSnapshot.getTableIdByPath());
                    Map<Long, TablePath> pathByTableId = new HashMap<>();
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForTables());

                    // Update table mapping
                    tableIdByPath.put(tablePath, tableId);
                    pathByTableId.put(tableId, tablePath);

                    // Update bucket metadata for this table
                    Map<Integer, BucketMetadata> tableBucketMetadata = new HashMap<>();
                    for (BucketMetadata bucketMetadata : tableMetadata.getBucketMetadataList()) {
                        tableBucketMetadata.put(bucketMetadata.getBucketId(), bucketMetadata);
                    }
                    bucketMetadataMapForTables.put(tableId, tableBucketMetadata);

                    // Copy other existing data
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(currentSnapshot.getPartitionIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForPartitions());

                    // Build pathByTableId from tableIdByPath
                    tableIdByPath.forEach((path, id) -> pathByTableId.put(id, path));

                    // Update epoch for this table
                    Map<Long, Long> bucketLayoutEpochByTableId =
                            new HashMap<>(currentSnapshot.getBucketLayoutEpochByTableId());
                    bucketLayoutEpochByTableId.put(tableId, tableInfo.getBucketLayoutEpoch());

                    // Create new snapshot
                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    currentSnapshot.getCoordinatorServer(),
                                    currentSnapshot.getAliveTabletServers(),
                                    tableIdByPath,
                                    pathByTableId,
                                    partitionIdByPath,
                                    bucketMetadataMapForTables,
                                    bucketMetadataMapForPartitions,
                                    currentSnapshot.getPartitionBucketCounts(),
                                    bucketLayoutEpochByTableId);
                    initialMetadataApplied = true;
                });
    }

    /**
     * Update a single partition metadata to the local cache. This method is thread-safe and will
     * merge the new partition metadata with existing cache.
     *
     * @param partitionMetadata the partition metadata to update
     */
    public void updatePartitionMetadata(PartitionMetadata partitionMetadata) {
        inLock(
                metadataLock,
                () -> {
                    long tableId = partitionMetadata.getTableId();
                    String partitionName = partitionMetadata.getPartitionName();
                    long partitionId = partitionMetadata.getPartitionId();

                    // Get current snapshot
                    ServerMetadataSnapshot currentSnapshot = serverMetadataSnapshot;

                    // Get table path from tableId
                    Optional<TablePath> tablePathOpt = currentSnapshot.getTablePath(tableId);
                    if (!tablePathOpt.isPresent()) {
                        // If table doesn't exist in cache, we can't update partition metadata
                        return;
                    }
                    TablePath tablePath = tablePathOpt.get();
                    PhysicalTablePath physicalTablePath =
                            PhysicalTablePath.of(tablePath, partitionName);

                    // Create new maps based on current state
                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(currentSnapshot.getTableIdByPath());
                    Map<Long, TablePath> pathByTableId = new HashMap<>();
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(currentSnapshot.getPartitionIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForPartitions());

                    // Update partition mapping
                    partitionIdByPath.put(physicalTablePath, partitionId);

                    // Update bucket metadata for this partition
                    Map<Integer, BucketMetadata> partitionBucketMetadata = new HashMap<>();
                    for (BucketMetadata bucketMetadata :
                            partitionMetadata.getBucketMetadataList()) {
                        partitionBucketMetadata.put(bucketMetadata.getBucketId(), bucketMetadata);
                    }
                    bucketMetadataMapForPartitions.put(partitionId, partitionBucketMetadata);

                    Map<TablePartition, Integer> partitionBucketCounts =
                            new HashMap<>(currentSnapshot.getPartitionBucketCounts());
                    mergePartitionBucketCount(
                            partitionBucketCounts, tableId, partitionId, partitionMetadata);

                    // Copy other existing data
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForTables());

                    // Build pathByTableId from tableIdByPath
                    tableIdByPath.forEach((path, id) -> pathByTableId.put(id, path));

                    // Create new snapshot
                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    currentSnapshot.getCoordinatorServer(),
                                    currentSnapshot.getAliveTabletServers(),
                                    tableIdByPath,
                                    pathByTableId,
                                    partitionIdByPath,
                                    bucketMetadataMapForTables,
                                    bucketMetadataMapForPartitions,
                                    partitionBucketCounts,
                                    currentSnapshot.getBucketLayoutEpochByTableId());
                });
    }

    /**
     * Merges the coordinator-sent per-partition bucket count into the cache map. Coordinators of
     * older versions do not send it; in that case the cache keeps no entry and readers fall back to
     * the merged bucket metadata size (see {@link #getPartitionMetadata}).
     */
    private static void mergePartitionBucketCount(
            Map<TablePartition, Integer> partitionBucketCounts,
            long tableId,
            long partitionId,
            PartitionMetadata partitionMetadata) {
        if (partitionMetadata.getBucketCount() != null) {
            partitionBucketCounts.put(
                    new TablePartition(tableId, partitionId), partitionMetadata.getBucketCount());
        }
    }

    @VisibleForTesting
    public ServerSchemaCache getServerSchemaCache() {
        return serverSchemaCache;
    }
}

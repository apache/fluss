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

    /** Returns the table ID from the in-memory metadata snapshot without accessing ZooKeeper. */
    public OptionalLong getTableId(TablePath tablePath) {
        return serverMetadataSnapshot.getTableId(tablePath);
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

    /**
     * Returns one bucket of a non-partitioned table from the in-memory snapshot only (no ZK
     * access). Empty if this server's snapshot does not know the table or bucket yet.
     */
    public Optional<BucketMetadata> getCachedBucketMetadata(TablePath tablePath, int bucketId) {
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;
        OptionalLong tableIdOpt = snapshot.getTableId(tablePath);
        if (!tableIdOpt.isPresent()) {
            return Optional.empty();
        }
        return Optional.ofNullable(
                snapshot.getBucketMetadataForTable(tableIdOpt.getAsLong()).get(bucketId));
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

    /**
     * Returns the bucket layout epoch for the table, or empty if not known (legacy table without
     * the field, read as 0).
     */
    public OptionalLong getBucketCountEpoch(long tableId) {
        return serverMetadataSnapshot.getBucketCountEpoch(tableId);
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

                    // 3. Update table metadata. Always partial update.
                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getTableIdByPath());
                    Map<Long, TablePath> newPathByTableId = new HashMap<>();
                    tableIdByPath.forEach((path, id) -> newPathByTableId.put(id, path));

                    Set<TablePath> replacedTablePaths = new HashSet<>();

                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(serverMetadataSnapshot.getBucketMetadataMapForTables());
                    Map<Long, Long> bucketCountEpochByTableId =
                            new HashMap<>(serverMetadataSnapshot.getBucketCountEpochByTableId());

                    for (TableMetadata tableMetadata : clusterMetadata.getTableMetadataList()) {
                        TableInfo tableInfo = tableMetadata.getTableInfo();
                        TablePath tablePath = tableInfo.getTablePath();
                        long tableId = tableInfo.getTableId();

                        if (DELETED_TABLE_PATH.equals(tablePath)) {
                            // Delete by the original table ID. A delayed deletion must not
                            // remove a new table that has reused the same path.
                            removeTableMapping(tableIdByPath, newPathByTableId, tableId);
                            bucketMetadataMapForTables.remove(tableId);
                            bucketCountEpochByTableId.remove(tableId);
                            deletedTableIds.add(tableId);
                        } else if (tableId == DELETED_TABLE_ID) {
                            // Legacy path-based deletion. Retain support for requests sent
                            // by older coordinators.
                            Long removedTableId = tableIdByPath.get(tablePath);
                            if (removedTableId != null) {
                                removeTableMapping(tableIdByPath, newPathByTableId, removedTableId);
                                bucketMetadataMapForTables.remove(removedTableId);
                                bucketCountEpochByTableId.remove(removedTableId);
                                deletedTableIds.add(removedTableId);
                            }
                        } else {
                            // Ignore an older bucket layout update for the same table ID.
                            long newEpoch = tableInfo.getBucketCountEpoch();
                            long currentEpoch = bucketCountEpochByTableId.getOrDefault(tableId, 0L);
                            if (newEpoch < currentEpoch) {
                                continue;
                            }

                            bucketCountEpochByTableId.put(tableId, newEpoch);

                            // Update schema metadata.
                            int schemaId = tableInfo.getSchemaId();
                            Schema schema = tableInfo.getSchema();
                            serverSchemaCache.updateLatestSchema(tableId, (short) schemaId, schema);

                            // A newly registered or recreated table must not inherit partition
                            // path mappings from a previous table with the same path.
                            Long existingTableId = tableIdByPath.get(tablePath);
                            if (existingTableId == null || existingTableId != tableId) {
                                replacedTablePaths.add(tablePath);
                            }

                            putTableMapping(tableIdByPath, newPathByTableId, tablePath, tableId);

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

                    // 4. Update partition metadata. Always partial update.
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(serverMetadataSnapshot.getPartitionIdByPath());
                    // A new table identity must not expose partitions registered under an
                    // earlier table with the same path. Preserve their ID-based bucket state
                    // until the corresponding deletion notifications arrive.
                    invalidatePartitionPaths(partitionIdByPath, replacedTablePaths);
                    Map<Long, PhysicalTablePath> pathByPartitionId = new HashMap<>();
                    partitionIdByPath.forEach((path, id) -> pathByPartitionId.put(id, path));

                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(
                                    serverMetadataSnapshot.getBucketMetadataMapForPartitions());
                    Map<TablePartition, Integer> partitionBucketCounts =
                            new HashMap<>(serverMetadataSnapshot.getPartitionBucketCounts());

                    for (PartitionMetadata partitionMetadata :
                            clusterMetadata.getPartitionMetadataList()) {
                        long tableId = partitionMetadata.getTableId();
                        String partitionName = partitionMetadata.getPartitionName();
                        long partitionId = partitionMetadata.getPartitionId();

                        if (DELETED_PARTITION_NAME.equals(partitionName)) {
                            // Delete by partition ID, independently of whether its parent table
                            // has already been deleted or its path has been reused.
                            removePartitionMapping(
                                    partitionIdByPath, pathByPartitionId, partitionId);
                            bucketMetadataMapForPartitions.remove(partitionId);
                            partitionBucketCounts
                                    .keySet()
                                    .removeIf(key -> key.getPartitionId() == partitionId);
                        } else if (partitionId == DELETED_PARTITION_ID) {
                            // Compatibility with legacy path-based partition deletions.
                            // The parent table may have been deleted earlier in this batch.
                            TablePath tablePath = newPathByTableId.get(tableId);
                            if (tablePath == null) {
                                tablePath =
                                        serverMetadataSnapshot.getTablePath(tableId).orElse(null);
                            }

                            if (tablePath == null) {
                                continue;
                            }

                            PhysicalTablePath partitionPath =
                                    PhysicalTablePath.of(tablePath, partitionName);

                            // A legacy deletion does not contain the original partition ID.
                            // Avoid removing a replacement partition inserted in this batch.
                            Long previousPartitionId =
                                    serverMetadataSnapshot
                                            .getPartitionId(partitionPath)
                                            .orElse(null);
                            Long currentPartitionId = partitionIdByPath.get(partitionPath);

                            if (previousPartitionId != null
                                    && previousPartitionId.equals(currentPartitionId)) {
                                removePartitionMapping(
                                        partitionIdByPath, pathByPartitionId, previousPartitionId);
                                bucketMetadataMapForPartitions.remove(previousPartitionId);
                                partitionBucketCounts
                                        .keySet()
                                        .removeIf(
                                                key -> key.getPartitionId() == previousPartitionId);
                            }
                        } else {
                            TablePath tablePath = newPathByTableId.get(tableId);
                            if (tablePath == null) {
                                // Do not recreate partition metadata for a deleted or unknown
                                // parent table.
                                continue;
                            }

                            PhysicalTablePath partitionPath =
                                    PhysicalTablePath.of(tablePath, partitionName);

                            putPartitionMapping(
                                    partitionIdByPath,
                                    pathByPartitionId,
                                    partitionPath,
                                    partitionId);

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
                                    bucketCountEpochByTableId);
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

                    ServerMetadataSnapshot currentSnapshot = serverMetadataSnapshot;

                    long newEpoch = tableInfo.getBucketCountEpoch();
                    long currentEpoch =
                            currentSnapshot
                                    .getBucketCountEpochByTableId()
                                    .getOrDefault(tableId, 0L);
                    if (newEpoch < currentEpoch) {
                        return;
                    }

                    Map<TablePath, Long> tableIdByPath =
                            new HashMap<>(currentSnapshot.getTableIdByPath());
                    Map<Long, TablePath> pathByTableId = new HashMap<>();
                    tableIdByPath.forEach((path, id) -> pathByTableId.put(id, path));

                    putTableMapping(tableIdByPath, pathByTableId, tablePath, tableId);

                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForTables());
                    Map<Integer, BucketMetadata> tableBucketMetadata = new HashMap<>();
                    for (BucketMetadata bucketMetadata : tableMetadata.getBucketMetadataList()) {
                        tableBucketMetadata.put(bucketMetadata.getBucketId(), bucketMetadata);
                    }
                    bucketMetadataMapForTables.put(tableId, tableBucketMetadata);

                    Map<Long, Long> bucketCountEpochByTableId =
                            new HashMap<>(currentSnapshot.getBucketCountEpochByTableId());
                    bucketCountEpochByTableId.put(tableId, newEpoch);

                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(currentSnapshot.getPartitionIdByPath());

                    Long existingTableId = currentSnapshot.getTableIdByPath().get(tablePath);
                    if (existingTableId == null || existingTableId != tableId) {
                        invalidatePartitionPaths(
                                partitionIdByPath, Collections.singleton(tablePath));
                    }

                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    currentSnapshot.getCoordinatorServer(),
                                    currentSnapshot.getAliveTabletServers(),
                                    tableIdByPath,
                                    pathByTableId,
                                    partitionIdByPath,
                                    bucketMetadataMapForTables,
                                    currentSnapshot.getBucketMetadataMapForPartitions(),
                                    currentSnapshot.getPartitionBucketCounts(),
                                    bucketCountEpochByTableId);
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

                    PhysicalTablePath partitionPath =
                            PhysicalTablePath.of(tablePathOpt.get(), partitionName);

                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(currentSnapshot.getPartitionIdByPath());
                    Map<Long, PhysicalTablePath> pathByPartitionId = new HashMap<>();
                    partitionIdByPath.forEach((path, id) -> pathByPartitionId.put(id, path));

                    putPartitionMapping(
                            partitionIdByPath, pathByPartitionId, partitionPath, partitionId);

                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForPartitions());
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

                    Map<Long, TablePath> pathByTableId = new HashMap<>();
                    currentSnapshot
                            .getTableIdByPath()
                            .forEach((path, id) -> pathByTableId.put(id, path));

                    serverMetadataSnapshot =
                            new ServerMetadataSnapshot(
                                    currentSnapshot.getCoordinatorServer(),
                                    currentSnapshot.getAliveTabletServers(),
                                    currentSnapshot.getTableIdByPath(),
                                    pathByTableId,
                                    partitionIdByPath,
                                    currentSnapshot.getBucketMetadataMapForTables(),
                                    bucketMetadataMapForPartitions,
                                    partitionBucketCounts,
                                    currentSnapshot.getBucketCountEpochByTableId());
                });
    }

    private static void putTableMapping(
            Map<TablePath, Long> tableIdByPath,
            Map<Long, TablePath> pathByTableId,
            TablePath tablePath,
            long tableId) {
        TablePath previousPath = pathByTableId.put(tableId, tablePath);
        if (previousPath != null && !previousPath.equals(tablePath)) {
            tableIdByPath.remove(previousPath, tableId);
        }
        Long previousTableId = tableIdByPath.put(tablePath, tableId);
        if (previousTableId != null && previousTableId != tableId) {
            pathByTableId.remove(previousTableId, tablePath);
        }
    }

    private static void removeTableMapping(
            Map<TablePath, Long> tableIdByPath, Map<Long, TablePath> pathByTableId, long tableId) {
        TablePath tablePath = pathByTableId.remove(tableId);
        if (tablePath != null) {
            tableIdByPath.remove(tablePath, tableId);
        }
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

    private static void putPartitionMapping(
            Map<PhysicalTablePath, Long> partitionIdByPath,
            Map<Long, PhysicalTablePath> pathByPartitionId,
            PhysicalTablePath partitionPath,
            long partitionId) {
        PhysicalTablePath previousPath = pathByPartitionId.put(partitionId, partitionPath);
        if (previousPath != null && !previousPath.equals(partitionPath)) {
            partitionIdByPath.remove(previousPath, partitionId);
        }

        Long previousPartitionId = partitionIdByPath.put(partitionPath, partitionId);
        if (previousPartitionId != null && previousPartitionId != partitionId) {
            pathByPartitionId.remove(previousPartitionId, partitionPath);
        }
    }

    private static void removePartitionMapping(
            Map<PhysicalTablePath, Long> partitionIdByPath,
            Map<Long, PhysicalTablePath> pathByPartitionId,
            long partitionId) {
        PhysicalTablePath partitionPath = pathByPartitionId.remove(partitionId);
        if (partitionPath != null) {
            partitionIdByPath.remove(partitionPath, partitionId);
        }
    }

    /**
     * Invalidates partition path mappings when a table path is associated with a different table
     * ID. Partition bucket metadata and bucket counts are retained until the corresponding
     * partition deletion notifications are processed.
     */
    private static void invalidatePartitionPaths(
            Map<PhysicalTablePath, Long> partitionIdByPath, Set<TablePath> tablePaths) {
        if (tablePaths.isEmpty()) {
            return;
        }

        partitionIdByPath
                .keySet()
                .removeIf(partitionPath -> tablePaths.contains(partitionPath.getTablePath()));
    }

    @VisibleForTesting
    public ServerSchemaCache getServerSchemaCache() {
        return serverSchemaCache;
    }
}

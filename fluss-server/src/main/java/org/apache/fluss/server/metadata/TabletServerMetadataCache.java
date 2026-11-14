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
import org.apache.fluss.server.zk.data.PartitionRegistration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
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
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_NAME;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/** The implement of {@link ServerMetadataCache} for {@link TabletServer}. */
public class TabletServerMetadataCache implements ServerMetadataCache {

    private static final Logger LOG = LoggerFactory.getLogger(TabletServerMetadataCache.class);

    private final Lock metadataLock = new ReentrantLock();

    /**
     * Immutable routing snapshot, replaced under metadataLock. Routing-only readers capture this
     * volatile reference once; metadata views capture it together with table properties under the
     * lock so one request cannot combine different updates.
     */
    @GuardedBy("metadataLock")
    private volatile ServerMetadataSnapshot serverMetadataSnapshot;

    private final MetadataManager metadataManager;

    private final ServerSchemaCache serverSchemaCache;

    @GuardedBy("metadataLock")
    private ServerTablePropertyCache tablePropertyCache;

    @GuardedBy("metadataLock")
    private final DeletedIdWatermarks deletedIdWatermarks;

    /**
     * The Coordinator epoch of the latest update, whose late updates {@link #deletedIdWatermarks}
     * and {@link #unverifiedPaths} cover.
     */
    @GuardedBy("metadataLock")
    private int currentCoordinatorEpoch = Integer.MIN_VALUE;

    /**
     * Table paths, as {@link PhysicalTablePath} without partition name, and partition paths at
     * which a tombstone of an older Coordinator, which does not send the dropped ID, found no
     * cached object, so that a late update of the dropped object cannot be rejected. Cache hits of
     * partitions at these paths are checked against ZK. When the Coordinator epoch advances, the
     * objects at these paths are removed and the paths released, as late requests of earlier epochs
     * are rejected from then on and the new Coordinator pushes the live metadata on startup.
     * Immutable, replaced under metadataLock after the snapshot it applies to is published.
     */
    private volatile Set<PhysicalTablePath> unverifiedPaths = Collections.emptySet();

    public TabletServerMetadataCache(MetadataManager metadataManager) {
        this.serverMetadataSnapshot = ServerMetadataSnapshot.empty();
        this.tablePropertyCache = ServerTablePropertyCache.empty();
        this.deletedIdWatermarks = new DeletedIdWatermarks();
        this.metadataManager = metadataManager;
        this.serverSchemaCache = new ServerSchemaCache(metadataManager);
    }

    /** Returns the routing snapshot, also used to fence in-flight cache-aside loads. */
    public ServerMetadataSnapshot getSnapshot() {
        return serverMetadataSnapshot;
    }

    /** Captures routing and table properties under the update lock for this request. */
    public MetadataView getMetadataView(String listenerName, Predicate<TablePath> authorized) {
        return inLock(
                metadataLock,
                () ->
                        new MetadataView(
                                serverMetadataSnapshot,
                                tablePropertyCache,
                                listenerName,
                                authorized));
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
        // Look up routing in the cache before loading the full table information from ZK.
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;
        OptionalLong tableIdOpt = snapshot.getTableId(tablePath);
        if (!tableIdOpt.isPresent()) {
            return Optional.empty();
        }

        long tableId = tableIdOpt.getAsLong();

        // Try to get table info from ZK only if we have the table ID in cache
        try {
            TableInfo tableInfo = metadataManager.getTable(tablePath);
            if (tableInfo.getTableId() != tableId) {
                return Optional.empty();
            }
            Map<Integer, BucketMetadata> buckets = snapshot.getBucketMetadataForTable(tableId);
            List<BucketMetadata> bucketMetadataList =
                    tableInfo.isPartitioned()
                            ? new ArrayList<>(buckets.values())
                            : getCompleteBucketMetadata(buckets, tableInfo.getNumBuckets());
            if (bucketMetadataList == null) {
                return Optional.empty();
            }
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

    /** Returns the metadata of one partition, see {@link #getPartitionsMetadata(Collection)}. */
    public Optional<PartitionMetadata> getPartitionMetadata(PhysicalTablePath partitionPath) {
        return Optional.ofNullable(
                getPartitionsMetadata(Collections.singletonList(partitionPath)).get(partitionPath));
    }

    /**
     * Returns the cached metadata of the given partitions. Partitions that are not cached or are
     * incomplete are absent from the result.
     *
     * <p>The snapshot may lag behind ZK, but never pairs a table ID with partitions of an older
     * table of the same path, and a late update cannot restore a dropped or replaced table or
     * partition, so cache hits are served without accessing ZK, except at {@link #unverifiedPaths},
     * where they are checked against their ZK registrations, with one batched ZK read per table.
     */
    public Map<PhysicalTablePath, PartitionMetadata> getPartitionsMetadata(
            Collection<PhysicalTablePath> partitionPaths) {
        // A path is checked if it is unverified before or after the snapshot is read, so that
        // neither adding nor releasing the path can race with publishing the snapshot.
        Set<PhysicalTablePath> unverifiedBefore = unverifiedPaths;
        ServerMetadataSnapshot snapshot = serverMetadataSnapshot;
        Set<PhysicalTablePath> unverifiedAfter = unverifiedPaths;
        Map<PhysicalTablePath, PartitionMetadata> result = new HashMap<>();
        Map<PhysicalTablePath, PartitionMetadata> unverified = new HashMap<>();
        for (PhysicalTablePath partitionPath : partitionPaths) {
            getCachedPartitionMetadata(snapshot, partitionPath)
                    .ifPresent(
                            metadata ->
                                    (isUnverified(unverifiedBefore, partitionPath)
                                                            || isUnverified(
                                                                    unverifiedAfter, partitionPath)
                                                    ? unverified
                                                    : result)
                                            .put(partitionPath, metadata));
        }
        if (!unverified.isEmpty()) {
            result.putAll(verifyWithZk(unverified));
        }
        return result;
    }

    public boolean contains(TableBucket tableBucket) {
        return serverMetadataSnapshot.contains(tableBucket);
    }

    /**
     * Applies a partial cluster metadata update in the Coordinator epoch of the last update.
     *
     * @see #updateClusterMetadata(ClusterMetadata, int)
     */
    @VisibleForTesting
    public Set<Long> updateClusterMetadata(ClusterMetadata clusterMetadata) {
        return inLock(
                metadataLock,
                () -> updateClusterMetadata(clusterMetadata, currentCoordinatorEpoch));
    }

    /**
     * Applies a partial cluster metadata update.
     *
     * @param clusterMetadata the partial cluster metadata to apply
     * @param coordinatorEpoch the Coordinator epoch of the request, which the caller has already
     *     checked not to be older than any epoch it has seen
     * @return the real table IDs identified by table-deletion tombstones in this update; partition
     *     deletion tombstones are not included
     */
    public Set<Long> updateClusterMetadata(ClusterMetadata clusterMetadata, int coordinatorEpoch) {
        return inLock(
                metadataLock,
                () -> {
                    // Requests of earlier epochs are rejected from now on, so their late updates
                    // no longer need to be detected.
                    boolean epochAdvanced = coordinatorEpoch > currentCoordinatorEpoch;
                    if (epochAdvanced) {
                        deletedIdWatermarks.clear();
                        currentCoordinatorEpoch = coordinatorEpoch;
                    }
                    Set<PhysicalTablePath> pathsToInvalidate =
                            epochAdvanced ? unverifiedPaths : Collections.emptySet();
                    List<PhysicalTablePath> unidentifiedDrops = new ArrayList<>();
                    ServerMetadataSnapshot currentSnapshot = serverMetadataSnapshot;
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
                            new HashMap<>(currentSnapshot.getTableIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForTables());
                    Map<Long, Long> bucketCountEpochByTableId =
                            new HashMap<>(currentSnapshot.getBucketCountEpochByTableId());

                    ServerTablePropertyCache.Builder tableProperties =
                            tablePropertyCache.toBuilder();
                    // Paths whose table is dropped or replaced; their partitions must be removed
                    // in the same snapshot.
                    Set<TablePath> invalidatedTablePaths = new HashSet<>();
                    for (PhysicalTablePath path : pathsToInvalidate) {
                        Long removedTableId =
                                path.getPartitionName() == null
                                        ? tableIdByPath.remove(path.getTablePath())
                                        : null;
                        if (removedTableId != null) {
                            bucketMetadataMapForTables.remove(removedTableId);
                            bucketCountEpochByTableId.remove(removedTableId);
                            tableProperties.remove(removedTableId);
                            invalidatedTablePaths.add(path.getTablePath());
                        }
                    }
                    Set<Long> outdatedTableIds = new HashSet<>();
                    // Updates are checked against the watermarks of earlier requests, so that a
                    // tombstone does not reject a live object of the same request. The changes of
                    // this request are applied after it has been processed without failure.
                    List<Consumer<DeletedIdWatermarks>> watermarkChanges = new ArrayList<>();
                    for (TableMetadata tableMetadata : clusterMetadata.getTableMetadataList()) {
                        TableInfo tableInfo = tableMetadata.getTableInfo();
                        TablePath tablePath = tableInfo.getTablePath();
                        long tableId = tableInfo.getTableId();

                        if (tableId == DELETED_TABLE_ID) {
                            Long deletedTableId = tableMetadata.getDeletedTableId();
                            Long removedTableId;
                            if (deletedTableId == null) {
                                // From an older Coordinator, without the dropped ID: the table
                                // cached for the path is taken to be the dropped one.
                                removedTableId = tableIdByPath.remove(tablePath);
                                if (removedTableId != null) {
                                    long removedId = removedTableId;
                                    watermarkChanges.add(w -> w.addTable(tablePath, removedId));
                                } else {
                                    unidentifiedDrops.add(PhysicalTablePath.of(tablePath));
                                }
                            } else {
                                // A late tombstone must not remove a newer table of the path.
                                removedTableId =
                                        tableIdByPath.remove(tablePath, deletedTableId)
                                                ? deletedTableId
                                                : null;
                                deletedTableIds.add(deletedTableId);
                                watermarkChanges.add(w -> w.addTable(tablePath, deletedTableId));
                            }
                            if (removedTableId != null) {
                                bucketMetadataMapForTables.remove(removedTableId);
                                deletedTableIds.add(removedTableId);
                                bucketCountEpochByTableId.remove(removedTableId);
                                tableProperties.remove(removedTableId);
                                invalidatedTablePaths.add(tablePath);
                            }
                        } else if (DELETED_TABLE_PATH.equals(tablePath)) {
                            // Compared by value, as the path is decoded from the request.
                            currentSnapshot
                                    .getTablePath(tableId)
                                    .ifPresent(
                                            path -> {
                                                tableIdByPath.remove(path, tableId);
                                                invalidatedTablePaths.add(path);
                                                watermarkChanges.add(
                                                        w -> w.addTable(path, tableId));
                                            });
                            bucketMetadataMapForTables.remove(tableId);
                            deletedTableIds.add(tableId);
                            bucketCountEpochByTableId.remove(tableId);
                            tableProperties.remove(tableId);
                        } else {
                            // A late UpdateMetadata of a replaced or dropped table. Cache hits are
                            // served without reading ZK, so neither the table nor its partitions
                            // may be applied.
                            Long cachedTableId = tableIdByPath.get(tablePath);
                            Long maxDeletedTableId =
                                    deletedIdWatermarks.getMaxDeletedTableId(tablePath);
                            if (isOutdatedOrMaybeDeleted(
                                    cachedTableId, tableId, maxDeletedTableId)) {
                                LOG.debug(
                                        "Ignore metadata of table {} with id {}, which may be"
                                                + " replaced or dropped: cached id {}, max"
                                                + " dropped id {}.",
                                        tablePath,
                                        tableId,
                                        cachedTableId,
                                        maxDeletedTableId);
                                outdatedTableIds.add(tableId);
                                continue;
                            }
                            // Ignore an older UpdateMetadata to prevent an older bucket
                            // layout (ALTER bucket.num) from replacing a newer one.
                            long newEpoch = tableInfo.getBucketCountEpoch();
                            long currentEpoch = bucketCountEpochByTableId.getOrDefault(tableId, 0L);
                            if (newEpoch < currentEpoch) {
                                continue;
                            }
                            bucketCountEpochByTableId.put(tableId, newEpoch);

                            // Update schema metadata.
                            // todo: apply schema id and schema info if needs
                            int schemaId = tableInfo.getSchemaId();
                            Schema schema = tableInfo.getSchema();
                            serverSchemaCache.updateLatestSchema(tableId, (short) schemaId, schema);

                            Long previousId = tableIdByPath.put(tablePath, tableId);
                            if (previousId != null && previousId != tableId) {
                                tableProperties.remove(previousId);
                                bucketMetadataMapForTables.remove(previousId);
                                bucketCountEpochByTableId.remove(previousId);
                                invalidatedTablePaths.add(tablePath);
                                watermarkChanges.add(w -> w.removePartitionsOf(previousId));
                            }
                            watermarkChanges.add(w -> w.onTableCached(tablePath));
                            tableProperties.update(tableId, tableInfo.getProperties());
                            // Copy only the changed bucket map. Older snapshots retain their map.
                            Map<Integer, BucketMetadata> tableBuckets =
                                    new HashMap<>(
                                            bucketMetadataMapForTables.getOrDefault(
                                                    tableId, Collections.emptyMap()));
                            for (BucketMetadata bucket : tableMetadata.getBucketMetadataList()) {
                                tableBuckets.put(bucket.getBucketId(), bucket);
                            }
                            bucketMetadataMapForTables.put(
                                    tableId, Collections.unmodifiableMap(tableBuckets));
                        }
                    }

                    Map<Long, TablePath> newPathByTableId = new HashMap<>();
                    tableIdByPath.forEach(
                            ((tablePath, tableId) -> newPathByTableId.put(tableId, tablePath)));

                    // 4. update partition metadata. Always partial update.
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(currentSnapshot.getPartitionIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForPartitions());
                    Map<TablePartition, Integer> partitionBucketCounts =
                            new HashMap<>(currentSnapshot.getPartitionBucketCounts());
                    removePartitionsOfTables(
                            invalidatedTablePaths,
                            partitionIdByPath,
                            bucketMetadataMapForPartitions,
                            partitionBucketCounts);
                    for (PhysicalTablePath path : pathsToInvalidate) {
                        Long removedPartitionId =
                                path.getPartitionName() == null
                                        ? null
                                        : partitionIdByPath.remove(path);
                        if (removedPartitionId != null) {
                            bucketMetadataMapForPartitions.remove(removedPartitionId);
                            partitionBucketCounts
                                    .keySet()
                                    .removeIf(k -> k.getPartitionId() == removedPartitionId);
                        }
                    }

                    for (PartitionMetadata partitionMetadata :
                            clusterMetadata.getPartitionMetadataList()) {
                        long tableId = partitionMetadata.getTableId();
                        if (outdatedTableIds.contains(tableId)) {
                            continue;
                        }
                        TablePath tablePath = newPathByTableId.get(tableId);
                        String partitionName = partitionMetadata.getPartitionName();
                        PhysicalTablePath physicalTablePath =
                                PhysicalTablePath.of(tablePath, partitionName);
                        long partitionId = partitionMetadata.getPartitionId();
                        if (partitionId == DELETED_PARTITION_ID) {
                            // From an older Coordinator, without the dropped ID: the partition
                            // cached for the name is taken to be the dropped one. Partitions of
                            // uncached tables are covered by the table tombstone.
                            Long removedPartitionId = partitionIdByPath.remove(physicalTablePath);
                            if (removedPartitionId != null) {
                                bucketMetadataMapForPartitions.remove(removedPartitionId);
                                partitionBucketCounts
                                        .keySet()
                                        .removeIf(k -> k.getPartitionId() == removedPartitionId);
                            }
                            if (tablePath != null) {
                                if (removedPartitionId != null) {
                                    watermarkChanges.add(
                                            w -> w.addPartition(tableId, removedPartitionId));
                                } else {
                                    unidentifiedDrops.add(physicalTablePath);
                                }
                            }
                        } else if (partitionName.equals(DELETED_PARTITION_NAME)) {
                            currentSnapshot
                                    .getPhysicalTablePath(partitionId)
                                    .ifPresent(path -> partitionIdByPath.remove(path, partitionId));
                            bucketMetadataMapForPartitions.remove(partitionId);
                            partitionBucketCounts
                                    .keySet()
                                    .removeIf(k -> k.getPartitionId() == partitionId);
                            if (tablePath != null) {
                                watermarkChanges.add(w -> w.addPartition(tableId, partitionId));
                            }
                        } else {
                            // Partitions of uncached tables share a null table path, so only a
                            // cached table can have a same-name predecessor.
                            Long previousId =
                                    tablePath == null
                                            ? null
                                            : partitionIdByPath.get(physicalTablePath);
                            // A late UpdateMetadata of a replaced or dropped partition, as with
                            // tables above.
                            Long maxDeletedPartitionId =
                                    deletedIdWatermarks.getMaxDeletedPartitionId(tableId);
                            if (isOutdatedOrMaybeDeleted(
                                    previousId, partitionId, maxDeletedPartitionId)) {
                                LOG.debug(
                                        "Ignore metadata of partition {} with id {}, which may be"
                                                + " replaced or dropped: cached id {}, max"
                                                + " dropped id {}.",
                                        physicalTablePath,
                                        partitionId,
                                        previousId,
                                        maxDeletedPartitionId);
                                continue;
                            }
                            partitionIdByPath.put(physicalTablePath, partitionId);
                            if (previousId != null && previousId != partitionId) {
                                bucketMetadataMapForPartitions.remove(previousId);
                                partitionBucketCounts.remove(
                                        new TablePartition(tableId, previousId));
                            }
                            mergePartitionBucketCount(
                                    partitionBucketCounts, tableId, partitionId, partitionMetadata);
                            Map<Integer, BucketMetadata> partitionBuckets =
                                    new HashMap<>(
                                            bucketMetadataMapForPartitions.getOrDefault(
                                                    partitionId, Collections.emptyMap()));
                            for (BucketMetadata bucket :
                                    partitionMetadata.getBucketMetadataList()) {
                                partitionBuckets.put(bucket.getBucketId(), bucket);
                            }
                            bucketMetadataMapForPartitions.put(
                                    partitionId, Collections.unmodifiableMap(partitionBuckets));
                        }
                    }

                    ServerMetadataSnapshot newSnapshot =
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
                    tablePropertyCache = tableProperties.build();
                    watermarkChanges.forEach(change -> change.accept(deletedIdWatermarks));
                    serverMetadataSnapshot = newSnapshot;
                    if (!pathsToInvalidate.isEmpty()) {
                        LOG.info(
                                "Coordinator epoch advanced to {}. Removed the cached metadata at"
                                        + " {}, which a late update may have restored after a"
                                        + " tombstone of an older Coordinator.",
                                coordinatorEpoch,
                                pathsToInvalidate);
                    }
                    updateUnverifiedPaths(epochAdvanced, unidentifiedDrops);
                    return deletedTableIds;
                });
    }

    @VisibleForTesting
    public void clearTableMetadata() {
        inLock(
                metadataLock,
                () -> {
                    ServerMetadataSnapshot currentSnapshot = serverMetadataSnapshot;
                    ServerInfo coordinatorServer = currentSnapshot.getCoordinatorServer();
                    Map<Integer, ServerInfo> aliveTabletServers =
                            currentSnapshot.getAliveTabletServers();
                    ServerMetadataSnapshot newSnapshot =
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
                    tablePropertyCache = ServerTablePropertyCache.empty();
                    deletedIdWatermarks.clear();
                    serverMetadataSnapshot = newSnapshot;
                    unverifiedPaths = Collections.emptySet();
                });
    }

    /**
     * Publishes a cache-aside load only if no metadata update occurred while ZK was being read. In
     * particular, an in-flight lookup must not resurrect a dropped table or undo an ALTER.
     */
    public void updateTableMetadata(
            Collection<TableMetadata> tables, ServerMetadataSnapshot expectedSnapshot) {
        inLock(
                metadataLock,
                () -> {
                    if (serverMetadataSnapshot == expectedSnapshot) {
                        tables.forEach(this::updateTableMetadata);
                    }
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
                    long newEpoch = tableInfo.getBucketCountEpoch();
                    long currentEpoch =
                            currentSnapshot
                                    .getBucketCountEpochByTableId()
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

                    ServerTablePropertyCache.Builder tableProperties =
                            tablePropertyCache.toBuilder();
                    Long previousId = tableIdByPath.put(tablePath, tableId);
                    boolean tableIdChanged = previousId != null && previousId != tableId;
                    if (tableIdChanged) {
                        tableProperties.remove(previousId);
                        bucketMetadataMapForTables.remove(previousId);
                        deletedIdWatermarks.removePartitionsOf(previousId);
                    }
                    deletedIdWatermarks.onTableCached(tablePath);
                    tableProperties.update(tableId, tableInfo.getProperties());
                    pathByTableId.put(tableId, tablePath);

                    // Update bucket metadata for this table
                    Map<Integer, BucketMetadata> tableBucketMetadata = new HashMap<>();
                    for (BucketMetadata bucketMetadata : tableMetadata.getBucketMetadataList()) {
                        tableBucketMetadata.put(bucketMetadata.getBucketId(), bucketMetadata);
                    }
                    bucketMetadataMapForTables.put(
                            tableId, Collections.unmodifiableMap(tableBucketMetadata));

                    // Copy other existing data
                    Map<PhysicalTablePath, Long> partitionIdByPath =
                            new HashMap<>(currentSnapshot.getPartitionIdByPath());
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForPartitions());
                    Map<TablePartition, Integer> partitionBucketCounts =
                            new HashMap<>(currentSnapshot.getPartitionBucketCounts());
                    if (tableIdChanged) {
                        removePartitionsOfTables(
                                Collections.singleton(tablePath),
                                partitionIdByPath,
                                bucketMetadataMapForPartitions,
                                partitionBucketCounts);
                    }

                    // Build pathByTableId from tableIdByPath
                    tableIdByPath.forEach((path, id) -> pathByTableId.put(id, path));

                    // Update epoch for this table
                    Map<Long, Long> bucketCountEpochByTableId =
                            new HashMap<>(currentSnapshot.getBucketCountEpochByTableId());
                    if (tableIdChanged) {
                        bucketCountEpochByTableId.remove(previousId);
                    }
                    bucketCountEpochByTableId.put(tableId, tableInfo.getBucketCountEpoch());

                    // Create new snapshot
                    ServerMetadataSnapshot newSnapshot =
                            new ServerMetadataSnapshot(
                                    currentSnapshot.getCoordinatorServer(),
                                    currentSnapshot.getAliveTabletServers(),
                                    tableIdByPath,
                                    pathByTableId,
                                    partitionIdByPath,
                                    bucketMetadataMapForTables,
                                    bucketMetadataMapForPartitions,
                                    partitionBucketCounts,
                                    bucketCountEpochByTableId);
                    tablePropertyCache = tableProperties.build();
                    serverMetadataSnapshot = newSnapshot;
                });
    }

    /**
     * Publishes a cache-aside load only if no metadata update occurred while ZK was being read. In
     * particular, an in-flight lookup must not restore a replaced partition or an outdated leader.
     */
    public void updatePartitionMetadata(
            Collection<PartitionMetadata> partitions, ServerMetadataSnapshot expectedSnapshot) {
        inLock(
                metadataLock,
                () -> {
                    if (serverMetadataSnapshot == expectedSnapshot) {
                        partitions.forEach(this::updatePartitionMetadata);
                    }
                });
    }

    /**
     * Updates one partition in the local cache, removing metadata of a same-name predecessor. This
     * method is thread-safe.
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
                    Map<TablePartition, Integer> partitionBucketCounts =
                            new HashMap<>(currentSnapshot.getPartitionBucketCounts());

                    // Update partition mapping
                    Long previousId = partitionIdByPath.put(physicalTablePath, partitionId);
                    if (previousId != null && previousId != partitionId) {
                        bucketMetadataMapForPartitions.remove(previousId);
                        partitionBucketCounts.remove(new TablePartition(tableId, previousId));
                    }

                    // Update bucket metadata for this partition
                    Map<Integer, BucketMetadata> partitionBucketMetadata = new HashMap<>();
                    for (BucketMetadata bucketMetadata :
                            partitionMetadata.getBucketMetadataList()) {
                        partitionBucketMetadata.put(bucketMetadata.getBucketId(), bucketMetadata);
                    }
                    bucketMetadataMapForPartitions.put(
                            partitionId, Collections.unmodifiableMap(partitionBucketMetadata));

                    mergePartitionBucketCount(
                            partitionBucketCounts, tableId, partitionId, partitionMetadata);

                    // Copy other existing data
                    Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForTables =
                            new HashMap<>(currentSnapshot.getBucketMetadataMapForTables());

                    // Build pathByTableId from tableIdByPath
                    tableIdByPath.forEach((path, id) -> pathByTableId.put(id, path));

                    // Create new snapshot
                    ServerMetadataSnapshot newSnapshot =
                            new ServerMetadataSnapshot(
                                    currentSnapshot.getCoordinatorServer(),
                                    currentSnapshot.getAliveTabletServers(),
                                    tableIdByPath,
                                    pathByTableId,
                                    partitionIdByPath,
                                    bucketMetadataMapForTables,
                                    bucketMetadataMapForPartitions,
                                    partitionBucketCounts,
                                    currentSnapshot.getBucketCountEpochByTableId());
                    serverMetadataSnapshot = newSnapshot;
                });
    }

    /**
     * Returns the complete bucket metadata list, or null if the bucket IDs do not cover the layout.
     */
    @Nullable
    private static List<BucketMetadata> getCompleteBucketMetadata(
            Map<Integer, BucketMetadata> buckets, int bucketCount) {
        if (bucketCount <= 0 || buckets.size() != bucketCount) {
            return null;
        }
        List<BucketMetadata> result = new ArrayList<>(bucketCount);
        // Unique map keys with this size must cover [0, bucketCount) to form a complete layout.
        // Unknown leaders do not make a bucket assignment incomplete.
        for (Map.Entry<Integer, BucketMetadata> entry : buckets.entrySet()) {
            if (entry.getKey() < 0 || entry.getKey() >= bucketCount) {
                return null;
            }
            result.add(entry.getValue());
        }
        return result;
    }

    /** Published after the snapshot of the same update, see {@link #getPartitionsMetadata}. */
    @GuardedBy("metadataLock")
    private void updateUnverifiedPaths(
            boolean epochAdvanced, List<PhysicalTablePath> unidentifiedDrops) {
        if (unidentifiedDrops.isEmpty()) {
            if (epochAdvanced) {
                unverifiedPaths = Collections.emptySet();
            }
            return;
        }
        Set<PhysicalTablePath> paths =
                new HashSet<>(epochAdvanced ? Collections.emptySet() : unverifiedPaths);
        for (PhysicalTablePath path : unidentifiedDrops) {
            if (paths.add(path)) {
                LOG.info(
                        "Received a tombstone without the dropped ID from an older Coordinator"
                                + " for {}, which is not cached. Partition metadata cache hits"
                                + " at it are checked against ZooKeeper until the Coordinator"
                                + " epoch advances.",
                        path);
            }
        }
        unverifiedPaths = Collections.unmodifiableSet(paths);
    }

    private static boolean isUnverified(
            Set<PhysicalTablePath> unverifiedPaths, PhysicalTablePath partitionPath) {
        return !unverifiedPaths.isEmpty()
                && (unverifiedPaths.contains(partitionPath)
                        || unverifiedPaths.contains(
                                PhysicalTablePath.of(partitionPath.getTablePath())));
    }

    private static Optional<PartitionMetadata> getCachedPartitionMetadata(
            ServerMetadataSnapshot snapshot, PhysicalTablePath partitionPath) {
        OptionalLong tableIdOpt = snapshot.getTableId(partitionPath.getTablePath());
        Optional<Long> partitionIdOpt = snapshot.getPartitionId(partitionPath);
        if (!tableIdOpt.isPresent() || !partitionIdOpt.isPresent()) {
            return Optional.empty();
        }
        long tableId = tableIdOpt.getAsLong();
        long partitionId = partitionIdOpt.get();
        Integer bucketCount =
                snapshot.getPartitionBucketCount(new TablePartition(tableId, partitionId));
        if (bucketCount == null) {
            return Optional.empty();
        }
        List<BucketMetadata> bucketMetadataList =
                getCompleteBucketMetadata(
                        snapshot.getBucketMetadataForPartition(partitionId), bucketCount);
        if (bucketMetadataList == null) {
            return Optional.empty();
        }
        return Optional.of(
                new PartitionMetadata(
                        tableId,
                        partitionPath.getPartitionName(),
                        partitionId,
                        bucketMetadataList,
                        bucketCount));
    }

    /**
     * Keeps the cached partitions whose ZK registration has the same table and partition ID, with
     * one batched ZK read per table. On a ZK failure, the partitions of the table are left out, so
     * that the caller reads them from ZK.
     */
    private Map<PhysicalTablePath, PartitionMetadata> verifyWithZk(
            Map<PhysicalTablePath, PartitionMetadata> cached) {
        Map<TablePath, List<PartitionMetadata>> cachedByTable = new HashMap<>();
        cached.forEach(
                (path, metadata) ->
                        cachedByTable
                                .computeIfAbsent(path.getTablePath(), k -> new ArrayList<>())
                                .add(metadata));
        Map<PhysicalTablePath, PartitionMetadata> result = new HashMap<>();
        cachedByTable.forEach(
                (tablePath, partitions) -> {
                    List<String> partitionNames = new ArrayList<>();
                    partitions.forEach(p -> partitionNames.add(p.getPartitionName()));
                    Map<String, PartitionRegistration> registrations;
                    try {
                        registrations =
                                metadataManager.getPartitionRegistrations(
                                        tablePath, partitionNames);
                    } catch (Exception e) {
                        LOG.debug("Failed to check cached partitions of {} in ZK.", tablePath, e);
                        return;
                    }
                    for (PartitionMetadata partition : partitions) {
                        PartitionRegistration registration =
                                registrations.get(partition.getPartitionName());
                        if (registration != null
                                && registration.getTableId() == partition.getTableId()
                                && registration.getPartitionId() == partition.getPartitionId()) {
                            result.put(
                                    PhysicalTablePath.of(tablePath, partition.getPartitionName()),
                                    partition);
                        }
                    }
                });
        return result;
    }

    /**
     * Returns whether a pushed update of a table or partition may come from a late UpdateMetadata.
     * IDs are allocated in increasing order, so this holds if the ID is smaller than the cached one
     * of its path, or if it is not cached and not larger than the highest dropped ID.
     */
    private static boolean isOutdatedOrMaybeDeleted(
            @Nullable Long cachedId, long id, @Nullable Long maxDeletedId) {
        if (cachedId != null && cachedId == id) {
            return false;
        }
        return (cachedId != null && id < cachedId) || (maxDeletedId != null && id <= maxDeletedId);
    }

    private static void removePartitionsOfTables(
            Set<TablePath> tablePaths,
            Map<PhysicalTablePath, Long> partitionIdByPath,
            Map<Long, Map<Integer, BucketMetadata>> bucketMetadataMapForPartitions,
            Map<TablePartition, Integer> partitionBucketCounts) {
        if (tablePaths.isEmpty()) {
            return;
        }
        Set<Long> removedPartitionIds = new HashSet<>();
        partitionIdByPath
                .entrySet()
                .removeIf(
                        entry -> {
                            if (!tablePaths.contains(entry.getKey().getTablePath())) {
                                return false;
                            }
                            removedPartitionIds.add(entry.getValue());
                            return true;
                        });
        removedPartitionIds.forEach(bucketMetadataMapForPartitions::remove);
        partitionBucketCounts
                .keySet()
                .removeIf(partition -> removedPartitionIds.contains(partition.getPartitionId()));
    }

    /**
     * Merges the coordinator-sent per-partition bucket count into the cache map. Coordinators of
     * older versions do not send it; if no count is cached, readers must load authoritative
     * metadata rather than infer a count from a potentially partial bucket list.
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

    /** Returns the number of entries kept to reject late updates of dropped objects. */
    @VisibleForTesting
    int getDeletedIdWatermarkCount() {
        return inLock(metadataLock, deletedIdWatermarks::size);
    }

    /**
     * Upper bounds of the IDs of dropped tables and partitions, used to reject a late
     * UpdateMetadata of an object after its tombstone removed it from the snapshot. Loads from ZK
     * are not checked: the Coordinator sends a tombstone only after deleting the ZK node, and a
     * load that read ZK before the tombstone is discarded by the snapshot check.
     *
     * <p>IDs are allocated in increasing order, and a path has at most one live table, so the
     * highest dropped table ID per path rejects exactly the dropped and replaced tables of the
     * path. The highest dropped partition ID per table does not grow with the dropped partitions of
     * auto-partitioned tables, but also rejects an uncached live partition with a smaller ID until
     * it is loaded from ZK.
     *
     * <p>Lifecycle: the entry of a path is removed when a table of the path is cached, and the
     * entry of a table when the table is dropped or replaced. All entries are removed when the
     * Coordinator epoch advances, as requests of earlier epochs, including late ones, are rejected
     * from then on. Until then, a path dropped and never re-created keeps one entry.
     *
     * <p>A tombstone of an older Coordinator, which does not send the dropped ID, records the ID
     * cached for its path or name, if any. Tombstones of partitions of uncached tables, e.g., of a
     * dropped table, are not recorded, as an update of such a partition comes with its table, nor
     * are ID-based tombstones of uncached tables, which only a Coordinator that has lost the table
     * sends: an update of the table comes from an earlier Coordinator and is rejected by its epoch.
     */
    @NotThreadSafe
    private static final class DeletedIdWatermarks {

        private final Map<TablePath, Long> maxDeletedTableIdByPath = new HashMap<>();
        private final Map<Long, Long> maxDeletedPartitionIdByTableId = new HashMap<>();

        @Nullable
        Long getMaxDeletedTableId(TablePath tablePath) {
            return maxDeletedTableIdByPath.get(tablePath);
        }

        @Nullable
        Long getMaxDeletedPartitionId(long tableId) {
            return maxDeletedPartitionIdByTableId.get(tableId);
        }

        void clear() {
            maxDeletedTableIdByPath.clear();
            maxDeletedPartitionIdByTableId.clear();
        }

        int size() {
            return maxDeletedTableIdByPath.size() + maxDeletedPartitionIdByTableId.size();
        }

        void addTable(TablePath tablePath, long tableId) {
            maxDeletedTableIdByPath.merge(tablePath, tableId, Math::max);
            removePartitionsOf(tableId);
        }

        /** From now on, the cached ID rejects the dropped IDs of the path. */
        void onTableCached(TablePath tablePath) {
            maxDeletedTableIdByPath.remove(tablePath);
        }

        void addPartition(long tableId, long partitionId) {
            maxDeletedPartitionIdByTableId.merge(tableId, partitionId, Math::max);
        }

        /** Called when a table is dropped or replaced, whose partitions are rejected with it. */
        void removePartitionsOf(long tableId) {
            maxDeletedPartitionIdByTableId.remove(tableId);
        }
    }
}

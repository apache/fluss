/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.cluster;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.SchemaInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An immutable representation of a subset of the server nodes, tables, and buckets and schemas in
 * the fluss cluster.
 *
 * <p>NOTE: not all tables and buckets are included in the cluster, only used tables by writer or
 * scanner will be included.
 */
@Internal
public final class Cluster {
    @Nullable private final ServerNode coordinatorServer;
    private final Map<PhysicalTablePath, List<BucketLocation>> availableLocationsByPath;
    private final Map<TableBucket, BucketLocation> availableLocationByBucket;
    private final Map<Integer, ServerNode> aliveTabletServersById;
    private final List<ServerNode> aliveTabletServers;
    private final Map<TablePath, Long> tableIdByPath;
    private final Map<Long, TablePath> pathByTableId;
    private final Map<PhysicalTablePath, Long> partitionsIdByPath;
    private final Map<Long, String> partitionNameById;

    /** Only latest schema of table will be put in it. */
    private final Map<TablePath, TableInfo> tableInfoByPath;

    public Cluster(
            Map<Integer, ServerNode> aliveTabletServersById,
            @Nullable ServerNode coordinatorServer,
            Map<PhysicalTablePath, List<BucketLocation>> bucketLocationsByPath,
            Map<TablePath, Long> tableIdByPath,
            Map<PhysicalTablePath, Long> partitionsIdByPath,
            Map<TablePath, TableInfo> tableInfoByPath) {
        this.coordinatorServer = coordinatorServer;
        this.aliveTabletServersById = Collections.unmodifiableMap(aliveTabletServersById);
        this.aliveTabletServers =
                Collections.unmodifiableList(new ArrayList<>(aliveTabletServersById.values()));
        this.tableIdByPath = Collections.unmodifiableMap(tableIdByPath);
        this.tableInfoByPath = Collections.unmodifiableMap(tableInfoByPath);
        this.partitionsIdByPath = Collections.unmodifiableMap(partitionsIdByPath);

        // Index the bucket locations by table path, and index bucket location by bucket.
        // Note that this code is performance sensitive if there are a large number of buckets,
        // so we are careful to avoid unnecessary work.
        Map<TableBucket, BucketLocation> tmpAvailableLocationByBucket = new HashMap<>();
        Map<PhysicalTablePath, List<BucketLocation>> tmpAvailableLocationsByPath =
                new HashMap<>(bucketLocationsByPath.size());
        for (Map.Entry<PhysicalTablePath, List<BucketLocation>> entry :
                bucketLocationsByPath.entrySet()) {
            PhysicalTablePath physicalTablePath = entry.getKey();
            List<BucketLocation> bucketsForTable = entry.getValue();
            // Optimise for the common case where all buckets are available.
            boolean foundUnavailableBucket = false;
            for (BucketLocation bucketLocation : bucketsForTable) {
                if (bucketLocation.getLeader() != null) {
                    tmpAvailableLocationByBucket.put(
                            bucketLocation.getTableBucket(), bucketLocation);
                } else {
                    foundUnavailableBucket = true;
                }
            }
            if (foundUnavailableBucket) {
                List<BucketLocation> availableBucketsForTable =
                        new ArrayList<>(bucketsForTable.size());
                for (BucketLocation loc : bucketsForTable) {
                    if (loc.getLeader() != null) {
                        availableBucketsForTable.add(loc);
                    }
                }
                tmpAvailableLocationsByPath.put(
                        physicalTablePath, Collections.unmodifiableList(availableBucketsForTable));
            } else {
                tmpAvailableLocationsByPath.put(
                        physicalTablePath, Collections.unmodifiableList(bucketsForTable));
            }
        }

        Map<Long, String> tmpPartitionNameById = new HashMap<>();
        for (Map.Entry<PhysicalTablePath, Long> partitionAndId : partitionsIdByPath.entrySet()) {
            tmpPartitionNameById.put(
                    partitionAndId.getValue(), partitionAndId.getKey().getPartitionName());
        }

        this.partitionNameById = Collections.unmodifiableMap(tmpPartitionNameById);
        this.availableLocationByBucket = Collections.unmodifiableMap(tmpAvailableLocationByBucket);
        this.availableLocationsByPath = Collections.unmodifiableMap(tmpAvailableLocationsByPath);

        Map<Long, TablePath> tempPathByTableId = new HashMap<>();
        tableIdByPath.forEach(((tablePath, tableId) -> tempPathByTableId.put(tableId, tablePath)));
        this.pathByTableId = Collections.unmodifiableMap(tempPathByTableId);
    }

    public Cluster invalidPhysicalTableBucketMeta(Set<PhysicalTablePath> physicalTablesToInvalid) {
        // should remove invalid tables from current availableLocationsByPath
        Map<PhysicalTablePath, List<BucketLocation>> newBucketLocationsByPath =
                new HashMap<>(availableLocationsByPath.size() - physicalTablesToInvalid.size());
        // copy the metadata from current availableLocationsByPath to newBucketLocationsByPath
        // except for the tables in physicalTablesToInvalid
        for (Map.Entry<PhysicalTablePath, List<BucketLocation>> tablePathAndBucketLocations :
                availableLocationsByPath.entrySet()) {
            if (!physicalTablesToInvalid.contains(tablePathAndBucketLocations.getKey())) {
                newBucketLocationsByPath.put(
                        tablePathAndBucketLocations.getKey(),
                        new ArrayList<>(tablePathAndBucketLocations.getValue()));
            }
        }
        return new Cluster(
                new HashMap<>(aliveTabletServersById),
                coordinatorServer,
                newBucketLocationsByPath,
                new HashMap<>(tableIdByPath),
                new HashMap<>(partitionsIdByPath),
                new HashMap<>(tableInfoByPath));
    }

    @Nullable
    public ServerNode getCoordinatorServer() {
        return coordinatorServer;
    }

    /** @return The known set of alive tablet servers. */
    public Map<Integer, ServerNode> getAliveTabletServers() {
        return aliveTabletServersById;
    }

    public List<ServerNode> getAliveTabletServerList() {
        return aliveTabletServers;
    }

    /**
     * Get the table id for this table.
     *
     * @param tablePath the table path
     * @return the table id, if metadata cache contains the table path, return the table path,
     *     otherwise return {@link TableInfo#UNKNOWN_TABLE_ID}
     */
    public long getTableId(TablePath tablePath) {
        return tableIdByPath.getOrDefault(tablePath, TableInfo.UNKNOWN_TABLE_ID);
    }

    /** Get the table path for this table id. */
    public Optional<TablePath> getTablePath(long tableId) {
        return Optional.ofNullable(pathByTableId.get(tableId));
    }

    public TablePath getTablePathOrElseThrow(long tableId) {
        return getTablePath(tableId)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "table path not found for tableId "
                                                + tableId
                                                + " in cluster"));
    }

    public int getBucketCount(TablePath tablePath) {
        return tableInfoByPath
                .get(tablePath)
                .getTableDescriptor()
                .getTableDistribution()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "table distribution is null for table: "
                                                + tablePath
                                                + " in cluster"))
                .getBucketCount()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "bucket count is null for table: "
                                                + tablePath
                                                + " in cluster"));
    }

    /** Get the bucket location for this table-bucket. */
    public Optional<BucketLocation> getBucketLocation(TableBucket tableBucket) {
        return Optional.ofNullable(availableLocationByBucket.get(tableBucket));
    }

    /** Get alive tablet server by id. */
    public Optional<ServerNode> getAliveTabletServerById(int serverId) {
        return Optional.ofNullable(aliveTabletServersById.get(serverId));
    }

    /** Get the tablet server by id. */
    @Nullable
    public ServerNode getTabletServer(int id) {
        return aliveTabletServersById.getOrDefault(id, null);
    }

    /** Get one random tablet server. */
    @Nullable
    public ServerNode getRandomTabletServer() {
        // TODO this method need to get one tablet server according to the load.
        List<ServerNode> serverNodes = new ArrayList<>(aliveTabletServersById.values());
        return !serverNodes.isEmpty() ? serverNodes.get(0) : null;
    }

    /** Get the list of available buckets for this table/partition. */
    public List<BucketLocation> getAvailableBucketsForPhysicalTablePath(
            PhysicalTablePath physicalTablePath) {
        return availableLocationsByPath.getOrDefault(physicalTablePath, Collections.emptyList());
    }

    /** Get the table info for this table. */
    public Optional<TableInfo> getTable(TablePath tablePath) {
        return Optional.ofNullable(tableInfoByPath.get(tablePath));
    }

    /** Get the partition id for this partition. */
    public Optional<Long> getPartitionId(PhysicalTablePath physicalTablePath) {
        return Optional.ofNullable(partitionsIdByPath.get(physicalTablePath));
    }

    /** Return whether the cluster contains the given physical table path or not. */
    public boolean contains(PhysicalTablePath physicalTablePath) {
        if (physicalTablePath.getPartitionName() == null) {
            return getTable(physicalTablePath.getTablePath()).isPresent();
        } else {
            return getPartitionId(physicalTablePath).isPresent();
        }
    }

    public TableInfo getTableOrElseThrow(TablePath tablePath) {
        return getTable(tablePath)
                .orElseThrow(() -> new IllegalArgumentException("table not found in cluster"));
    }

    public TableBucket getTableBucket(PhysicalTablePath physicalTablePath, int bucketId) {
        TableInfo tableInfo = getTableOrElseThrow(physicalTablePath.getTablePath());
        if (physicalTablePath.getPartitionName() != null) {
            Long partitionId = getPartitionIdOrElseThrow(physicalTablePath);
            return new TableBucket(tableInfo.getTableId(), partitionId, bucketId);
        } else {
            return new TableBucket(tableInfo.getTableId(), bucketId);
        }
    }

    public Long getPartitionIdOrElseThrow(PhysicalTablePath physicalTablePath) {
        Long partitionId = partitionsIdByPath.get(physicalTablePath);
        if (partitionId == null) {
            throw new PartitionNotExistException(
                    String.format("%s not found in cluster.", physicalTablePath));
        }
        return partitionId;
    }

    public String getPartitionNameOrElseThrow(long partitionId) {
        String partition = partitionNameById.get(partitionId);
        if (partition == null) {
            throw new PartitionNotExistException(
                    String.format(
                            "The partition's name for partition id: %d is not found in cluster.",
                            partitionId));
        }
        return partition;
    }

    public Optional<String> getPartitionName(long partitionId) {
        return Optional.ofNullable(partitionNameById.get(partitionId));
    }

    /** Get the latest schema for the given table. */
    public Optional<SchemaInfo> getSchema(TablePath tablePath) {
        return getTable(tablePath)
                .map(
                        tableInfo ->
                                new SchemaInfo(
                                        tableInfo.getTableDescriptor().getSchema(),
                                        tableInfo.getSchemaId()));
    }

    /** Get the table path to table id map. */
    public Map<TablePath, Long> getTableIdByPath() {
        return tableIdByPath;
    }

    /** Get the table info by table. */
    public Map<TablePath, TableInfo> getTableInfoByPath() {
        return tableInfoByPath;
    }

    /** Get the bucket by a physical table path. */
    public Map<PhysicalTablePath, List<BucketLocation>> getBucketLocationsByPath() {
        return availableLocationsByPath;
    }

    public Map<PhysicalTablePath, Long> getPartitionIdByPath() {
        return partitionsIdByPath;
    }

    /** Create an empty cluster instance with no nodes and no table-buckets. */
    public static Cluster empty() {
        return new Cluster(
                Collections.emptyMap(),
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    /** Get the current leader for the given table-bucket. */
    public @Nullable ServerNode leaderFor(TableBucket tableBucket) {
        BucketLocation location = availableLocationByBucket.get(tableBucket);
        if (location == null) {
            return null;
        } else {
            return location.getLeader();
        }
    }
}

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

package org.apache.fluss.client.utils;

import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Utils for metadata for client. */
public class MetadataUtils {

    private static final Random randOffset = new Random();

    /**
     * full update cluster, means we will rebuild the cluster by clearing all cached table in
     * cluster, and then send metadata request to request the input tables in tablePaths, after that
     * add those table into cluster.
     */
    public static Cluster sendMetadataRequestAndRebuildCluster(
            AdminReadOnlyGateway gateway, Set<TablePath> tablePaths)
            throws ExecutionException, InterruptedException, TimeoutException {
        return sendMetadataRequestAndRebuildCluster(gateway, false, null, tablePaths, null, null);
    }

    /**
     * Partial update cluster, means we will rebuild the cluster by sending metadata request to
     * request the input tables/partitions in physicalTablePaths, after that add those
     * tables/partitions into cluster. The origin tables/partitions in cluster will not be cleared,
     * but will be updated.
     */
    public static Cluster sendMetadataRequestAndRebuildCluster(
            Cluster cluster,
            RpcClient client,
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitionNames,
            @Nullable Collection<Long> tablePartitionIds,
            ServerNode serverNode)
            throws ExecutionException, InterruptedException, TimeoutException {
        AdminReadOnlyGateway gateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> serverNode, client, AdminReadOnlyGateway.class);
        return sendMetadataRequestAndRebuildCluster(
                gateway, true, cluster, tablePaths, tablePartitionNames, tablePartitionIds);
    }

    /** maybe partial update cluster. */
    public static Cluster sendMetadataRequestAndRebuildCluster(
            AdminReadOnlyGateway gateway,
            boolean partialUpdate,
            Cluster originCluster,
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitions,
            @Nullable Collection<Long> tablePartitionIds)
            throws ExecutionException, InterruptedException, TimeoutException {
        if (tablePartitions != null && !tablePartitions.isEmpty()) {
            tablePaths = new HashSet<>(tablePaths == null ? Collections.emptySet() : tablePaths);
            // Partition responses only carry the table ID, so uncached parents must be requested
            // together with the partitions to resolve their table paths and legacy bucket counts.
            for (PhysicalTablePath tablePartition : tablePartitions) {
                TablePath tablePath = tablePartition.getTablePath();
                if (originCluster == null || !originCluster.getTableId(tablePath).isPresent()) {
                    tablePaths.add(tablePath);
                }
            }
        }
        MetadataRequest metadataRequest =
                ClientRpcMessageUtils.makeMetadataRequest(
                        tablePaths, tablePartitions, tablePartitionIds);
        return gateway.metadata(metadataRequest)
                .thenApply(
                        response -> {
                            // Update the alive table servers.
                            Map<Integer, ServerNode> newAliveTabletServers =
                                    getAliveTabletServers(response);
                            // when talking to the startup tablet
                            // server, it maybe receive empty metadata, we'll consider it as
                            // stale metadata and throw StaleMetadataException which will cause
                            // to retry later.
                            if (newAliveTabletServers.isEmpty()) {
                                throw new StaleMetadataException("Alive tablet server is empty.");
                            }
                            ServerNode coordinatorServer = getCoordinatorServer(response);

                            Map<TablePath, Long> newTablePathToTableId;
                            Map<PhysicalTablePath, List<BucketLocation>> newBucketLocations;
                            Map<PhysicalTablePath, Long> newPartitionIdByPath;
                            Map<TableOrPartition, Integer> newBucketCountByTableOrPartition;
                            Map<Long, Long> newBucketCountEpochByTableId;

                            NewTableMetadata newTableMetadata =
                                    getTableMetadataToUpdate(originCluster, response);

                            if (partialUpdate) {
                                // If partial update, we will clear the to be updated table out ot
                                // the origin cluster.
                                newTablePathToTableId =
                                        new HashMap<>(originCluster.getTableIdByPath());
                                newBucketLocations =
                                        new HashMap<>(originCluster.getBucketLocationsByPath());
                                newPartitionIdByPath =
                                        new HashMap<>(originCluster.getPartitionIdByPath());
                                newBucketCountByTableOrPartition =
                                        new HashMap<>(
                                                originCluster.getBucketCountByTableOrPartition());
                                newBucketCountEpochByTableId =
                                        new HashMap<>(originCluster.getBucketCountEpochByTableId());

                                newTablePathToTableId.putAll(newTableMetadata.tablePathToTableId);
                                newBucketLocations.putAll(newTableMetadata.bucketLocations);
                                newPartitionIdByPath.putAll(newTableMetadata.partitionIdByPath);
                                newBucketCountByTableOrPartition.putAll(
                                        newTableMetadata.bucketCountByTableOrPartition);
                                newBucketCountEpochByTableId.putAll(
                                        newTableMetadata.bucketCountEpochByTableId);

                            } else {
                                // If full update, we will clear all tables info out ot the origin
                                // cluster.
                                newTablePathToTableId = newTableMetadata.tablePathToTableId;
                                newBucketLocations = newTableMetadata.bucketLocations;
                                newPartitionIdByPath = newTableMetadata.partitionIdByPath;
                                newBucketCountByTableOrPartition =
                                        newTableMetadata.bucketCountByTableOrPartition;
                                newBucketCountEpochByTableId =
                                        newTableMetadata.bucketCountEpochByTableId;
                            }

                            return new Cluster(
                                    newAliveTabletServers,
                                    coordinatorServer,
                                    newBucketLocations,
                                    newTablePathToTableId,
                                    newPartitionIdByPath,
                                    newBucketCountByTableOrPartition,
                                    newBucketCountEpochByTableId);
                        })
                .get(30, TimeUnit.SECONDS); // TODO currently, we don't have timeout logic in
        // RpcClient, it will let the get() block forever. So we
        // time out here
    }

    private static NewTableMetadata getTableMetadataToUpdate(
            Cluster cluster, MetadataResponse metadataResponse) {
        Map<TablePath, Long> newTablePathToTableId = new HashMap<>();
        Map<Long, TablePath> newTablePathByTableId = new HashMap<>();
        Map<PhysicalTablePath, List<BucketLocation>> newBucketLocations = new HashMap<>();
        Map<PhysicalTablePath, Long> newPartitionIdByPath = new HashMap<>();
        Map<TableOrPartition, Integer> newBucketCountByTableOrPartition = new HashMap<>();
        Map<Long, Long> newBucketCountEpochByTableId = new HashMap<>();

        // iterate all table metadata
        List<PbTableMetadata> pbTableMetadataList = metadataResponse.getTableMetadatasList();
        pbTableMetadataList.forEach(
                pbTableMetadata -> {
                    // get table info for the table
                    long tableId = pbTableMetadata.getTableId();
                    PbTablePath protoTablePath = pbTableMetadata.getTablePath();
                    TablePath tablePath =
                            new TablePath(
                                    protoTablePath.getDatabaseName(),
                                    protoTablePath.getTableName());
                    newTablePathToTableId.put(tablePath, tableId);
                    newTablePathByTableId.put(tableId, tablePath);

                    // Get all buckets for the table.
                    List<PbBucketMetadata> pbBucketMetadataList =
                            pbTableMetadata.getBucketMetadatasList();
                    newBucketLocations.put(
                            PhysicalTablePath.of(tablePath),
                            toBucketLocations(
                                    tablePath, tableId, null, null, pbBucketMetadataList));
                    if (pbTableMetadata.hasBucketCount()
                            && !pbTableMetadata.hasBucketCountEpoch()) {
                        throw new StaleMetadataException(
                                "Explicit table count has no epoch: " + tablePath);
                    }
                    int count =
                            pbTableMetadata.hasBucketCount()
                                    ? pbTableMetadata.getBucketCount()
                                    : TableDescriptor.fromJsonBytes(pbTableMetadata.getTableJson())
                                            .getTableDistribution()
                                            .flatMap(
                                                    TableDescriptor.TableDistribution
                                                            ::getBucketCount)
                                            .orElseThrow(
                                                    () ->
                                                            new StaleMetadataException(
                                                                    "Table bucket count is unavailable: "
                                                                            + tablePath));
                    long epoch =
                            pbTableMetadata.hasBucketCountEpoch()
                                    ? pbTableMetadata.getBucketCountEpoch()
                                    : 0L;
                    if (count <= 0 || epoch < 0) {
                        throw new StaleMetadataException(
                                "Invalid table bucket count or epoch: " + tablePath);
                    }
                    if (cluster != null && cluster.getBucketCountEpoch(tableId).isPresent()) {
                        long previousEpoch = cluster.getBucketCountEpoch(tableId).getAsLong();
                        Integer previousCount =
                                cluster.getBucketCount(TableOrPartition.ofTable(tableId))
                                        .orElse(null);
                        if (epoch < previousEpoch) {
                            // A delayed response must retain the newer cached count and locations.
                            epoch = previousEpoch;
                            if (previousCount == null) {
                                throw new StaleMetadataException(
                                        "A newer table layout is required: " + tablePath);
                            }
                            count = previousCount;
                            newBucketLocations.put(
                                    PhysicalTablePath.of(tablePath),
                                    cluster.getBucketLocationsByPath()
                                            .getOrDefault(
                                                    PhysicalTablePath.of(tablePath),
                                                    Collections.emptyList()));
                        } else if (epoch == previousEpoch
                                && previousCount != null
                                && count != previousCount) {
                            throw new StaleMetadataException(
                                    "Conflicting table counts at the same epoch: " + tablePath);
                        }
                    }
                    newBucketCountByTableOrPartition.put(TableOrPartition.ofTable(tableId), count);
                    newBucketCountEpochByTableId.put(tableId, epoch);
                });

        List<PbPartitionMetadata> pbPartitionMetadataList =
                metadataResponse.getPartitionMetadatasList();

        // iterate all partition metadata
        pbPartitionMetadataList.forEach(
                pbPartitionMetadata -> {
                    long tableId = pbPartitionMetadata.getTableId();
                    TablePath tablePath = newTablePathByTableId.get(tableId);
                    if (tablePath == null) {
                        tablePath =
                                cluster == null ? null : cluster.getTablePath(tableId).orElse(null);
                    }
                    if (tablePath == null) {
                        throw new StaleMetadataException(
                                "Parent table metadata is unavailable for " + tableId);
                    }
                    PhysicalTablePath physicalTablePath =
                            PhysicalTablePath.of(tablePath, pbPartitionMetadata.getPartitionName());
                    newPartitionIdByPath.put(
                            physicalTablePath, pbPartitionMetadata.getPartitionId());
                    newBucketLocations.put(
                            physicalTablePath,
                            toBucketLocations(
                                    tablePath,
                                    tableId,
                                    pbPartitionMetadata.getPartitionId(),
                                    pbPartitionMetadata.getPartitionName(),
                                    pbPartitionMetadata.getBucketMetadatasList()));
                    Integer bucketCount =
                            pbPartitionMetadata.hasBucketCount()
                                    ? pbPartitionMetadata.getBucketCount()
                                    : null;
                    if (bucketCount == null) {
                        Long epoch = newBucketCountEpochByTableId.get(tableId);
                        if (epoch == null
                                && cluster != null
                                && cluster.getBucketCountEpoch(tableId).isPresent()) {
                            epoch = cluster.getBucketCountEpoch(tableId).getAsLong();
                        }
                        if (epoch != null && epoch == 0L) {
                            // At epoch zero all partitions share the table count. The first rescale
                            // atomically persists existing partition counts and advances the epoch.
                            bucketCount =
                                    newBucketCountByTableOrPartition.get(
                                            TableOrPartition.ofTable(tableId));
                            if (bucketCount == null && cluster != null) {
                                bucketCount =
                                        cluster.getBucketCount(TableOrPartition.ofTable(tableId))
                                                .orElse(null);
                            }
                        }
                    }
                    if (bucketCount == null || bucketCount <= 0) {
                        throw new StaleMetadataException(
                                "Partition bucket count is unavailable: " + physicalTablePath);
                    }
                    newBucketCountByTableOrPartition.put(
                            TableOrPartition.ofPartition(pbPartitionMetadata.getPartitionId()),
                            bucketCount);
                });

        return new NewTableMetadata(
                newTablePathToTableId,
                newBucketLocations,
                newPartitionIdByPath,
                newBucketCountByTableOrPartition,
                newBucketCountEpochByTableId);
    }

    private static final class NewTableMetadata {
        private final Map<TablePath, Long> tablePathToTableId;
        private final Map<PhysicalTablePath, List<BucketLocation>> bucketLocations;
        private final Map<PhysicalTablePath, Long> partitionIdByPath;
        private final Map<TableOrPartition, Integer> bucketCountByTableOrPartition;
        private final Map<Long, Long> bucketCountEpochByTableId;

        public NewTableMetadata(
                Map<TablePath, Long> tablePathToTableId,
                Map<PhysicalTablePath, List<BucketLocation>> bucketLocations,
                Map<PhysicalTablePath, Long> partitionIdByPath,
                Map<TableOrPartition, Integer> bucketCountByTableOrPartition,
                Map<Long, Long> bucketCountEpochByTableId) {
            this.tablePathToTableId = tablePathToTableId;
            this.bucketLocations = bucketLocations;
            this.partitionIdByPath = partitionIdByPath;
            this.bucketCountByTableOrPartition = bucketCountByTableOrPartition;
            this.bucketCountEpochByTableId = bucketCountEpochByTableId;
        }
    }

    public static @Nullable ServerNode getOneAvailableTabletServerNode(
            Cluster cluster, Set<Integer> unavailableTabletServerIds) {
        List<ServerNode> aliveTabletServers = new ArrayList<>(cluster.getAliveTabletServerList());
        if (!unavailableTabletServerIds.isEmpty()) {
            aliveTabletServers.removeIf(
                    serverNode -> unavailableTabletServerIds.contains(serverNode.id()));
        }

        if (aliveTabletServers.isEmpty()) {
            return null;
        }
        // just pick one random server node
        int offset = randOffset.nextInt(aliveTabletServers.size());
        return aliveTabletServers.get(offset);
    }

    @Nullable
    private static ServerNode getCoordinatorServer(MetadataResponse response) {
        if (!response.hasCoordinatorServer()) {
            return null;
        } else {
            PbServerNode protoServerNode = response.getCoordinatorServer();
            return new ServerNode(
                    protoServerNode.getNodeId(),
                    protoServerNode.getHost(),
                    protoServerNode.getPort(),
                    ServerType.COORDINATOR);
        }
    }

    private static Map<Integer, ServerNode> getAliveTabletServers(MetadataResponse response) {
        Map<Integer, ServerNode> aliveTabletServers = new HashMap<>();
        response.getTabletServersList()
                .forEach(
                        serverNode -> {
                            int nodeId = serverNode.getNodeId();
                            aliveTabletServers.put(
                                    nodeId,
                                    new ServerNode(
                                            nodeId,
                                            serverNode.getHost(),
                                            serverNode.getPort(),
                                            ServerType.TABLET_SERVER,
                                            serverNode.hasRack() ? serverNode.getRack() : null));
                        });
        return aliveTabletServers;
    }

    private static List<BucketLocation> toBucketLocations(
            TablePath tablePath,
            long tableId,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            List<PbBucketMetadata> pbBucketMetadataList) {
        List<BucketLocation> bucketLocations = new ArrayList<>();
        for (PbBucketMetadata pbBucketMetadata : pbBucketMetadataList) {
            int bucketId = pbBucketMetadata.getBucketId();
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
            int[] replicas = new int[pbBucketMetadata.getReplicaIdsCount()];
            for (int i = 0; i < replicas.length; i++) {
                replicas[i] = pbBucketMetadata.getReplicaIdAt(i);
            }
            Integer leader = null;
            if (pbBucketMetadata.hasLeaderId()) {
                leader = pbBucketMetadata.getLeaderId();
            }
            PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);

            BucketLocation bucketLocation =
                    new BucketLocation(physicalTablePath, tableBucket, leader, replicas);
            bucketLocations.add(bucketLocation);
        }
        return bucketLocations;
    }
}

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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.exception.UnsupportedVersionException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableOrPartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.DescribeClusterRequest;
import org.apache.fluss.rpc.messages.DescribeClusterResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbBucketMetadata;
import org.apache.fluss.rpc.messages.PbPartitionMetadata;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.rpc.messages.PbTableMetadata;
import org.apache.fluss.rpc.messages.PbTablePath;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Utils for metadata for client. */
public class MetadataUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataUtils.class);

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

                                newTablePathToTableId.putAll(newTableMetadata.tablePathToTableId);
                                newBucketLocations.putAll(newTableMetadata.bucketLocations);
                                newPartitionIdByPath.putAll(newTableMetadata.partitionIdByPath);
                                newBucketCountByTableOrPartition.putAll(
                                        newTableMetadata.bucketCountByTableOrPartition);

                            } else {
                                // If full update, we will clear all tables info out ot the origin
                                // cluster.
                                newTablePathToTableId = newTableMetadata.tablePathToTableId;
                                newBucketLocations = newTableMetadata.bucketLocations;
                                newPartitionIdByPath = newTableMetadata.partitionIdByPath;
                                newBucketCountByTableOrPartition =
                                        newTableMetadata.bucketCountByTableOrPartition;
                            }

                            return new Cluster(
                                    newAliveTabletServers,
                                    coordinatorServer,
                                    newBucketLocations,
                                    newTablePathToTableId,
                                    newPartitionIdByPath,
                                    newBucketCountByTableOrPartition);
                        })
                .get(30, TimeUnit.SECONDS); // TODO currently, we don't have timeout logic in
        // RpcClient, it will let the get() block forever. So we
        // time out here
    }

    /**
     * Sends a {@link DescribeClusterRequest} to the coordinator. The returned server nodes are the
     * coordinator server, followed by the alive tablet servers with their {@link ServerTag}.
     */
    public static CompletableFuture<List<ServerNode>> sendDescribeClusterRequest(
            AdminGateway gateway) {
        return gateway.describeCluster(new DescribeClusterRequest())
                .thenApply(MetadataUtils::toServerNodes);
    }

    /**
     * Runs {@code request}, or {@code fallback} instead when the server does not support the
     * request, for example because it runs an older Fluss version. Any other failure of {@code
     * request} fails the returned future.
     */
    public static <T> CompletableFuture<T> fallbackIfUnsupported(
            Supplier<CompletableFuture<T>> request, Supplier<CompletableFuture<T>> fallback) {
        CompletableFuture<T> result = new CompletableFuture<>();
        CompletableFuture<T> requestFuture;
        try {
            requestFuture = request.get();
        } catch (Throwable t) {
            requestFuture = new CompletableFuture<>();
            requestFuture.completeExceptionally(t);
        }
        requestFuture.whenComplete(
                (value, error) -> {
                    if (error == null) {
                        result.complete(value);
                    } else if (!ExceptionUtils.findThrowable(
                                    error, UnsupportedVersionException.class)
                            .isPresent()) {
                        result.completeExceptionally(
                                ExceptionUtils.stripCompletionException(error));
                    } else {
                        LOG.info(
                                "The server does not support the request, falling back: {}",
                                error.getMessage());
                        try {
                            fallback.get()
                                    .whenComplete(
                                            (fallbackValue, fallbackError) -> {
                                                if (fallbackError == null) {
                                                    result.complete(fallbackValue);
                                                } else {
                                                    result.completeExceptionally(
                                                            ExceptionUtils.stripCompletionException(
                                                                    fallbackError));
                                                }
                                            });
                        } catch (Throwable t) {
                            result.completeExceptionally(t);
                        }
                    }
                });
        return result;
    }

    @VisibleForTesting
    static List<ServerNode> toServerNodes(DescribeClusterResponse response) {
        List<ServerNode> serverNodes = new ArrayList<>();
        ServerNode coordinator = getCoordinatorServer(response);
        if (coordinator != null) {
            serverNodes.add(coordinator);
        }
        for (PbServerNode serverNode : response.getTabletServersList()) {
            serverNodes.add(
                    new ServerNode(
                            serverNode.getNodeId(),
                            serverNode.getHost(),
                            serverNode.getPort(),
                            ServerType.TABLET_SERVER,
                            serverNode.hasRack() ? serverNode.getRack() : null,
                            serverNode.hasServerTag()
                                    ? toServerTag(serverNode.getServerTag())
                                    : null));
        }
        return serverNodes;
    }

    /**
     * Converts the server tag value of a response. A value this client does not know, written by a
     * newer server, is reported as no tag instead of failing the whole request.
     */
    @Nullable
    private static ServerTag toServerTag(int value) {
        try {
            return ServerTag.valueOf(value);
        } catch (IllegalArgumentException e) {
            LOG.warn(
                    "Ignoring unknown server tag {}, it was probably added by a newer Fluss version.",
                    value);
            return null;
        }
    }

    private static NewTableMetadata getTableMetadataToUpdate(
            Cluster cluster, MetadataResponse metadataResponse) {
        Map<TablePath, Long> newTablePathToTableId = new HashMap<>();
        Map<Long, TablePath> newTablePathByTableId = new HashMap<>();
        Map<PhysicalTablePath, List<BucketLocation>> newBucketLocations = new HashMap<>();
        Map<PhysicalTablePath, Long> newPartitionIdByPath = new HashMap<>();
        Map<TableOrPartition, Integer> newBucketCountByTableOrPartition = new HashMap<>();

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
                    // An empty bucket list means the assignment is not generated yet; keeping the
                    // entry out lets callers fall back to the table-level count instead of 0.
                    if (!pbBucketMetadataList.isEmpty()) {
                        newBucketCountByTableOrPartition.put(
                                TableOrPartition.ofTable(tableId), pbBucketMetadataList.size());
                    }
                });

        List<PbPartitionMetadata> pbPartitionMetadataList =
                metadataResponse.getPartitionMetadatasList();

        // iterate all partition metadata
        pbPartitionMetadataList.forEach(
                pbPartitionMetadata -> {
                    long tableId = pbPartitionMetadata.getTableId();
                    TablePath tablePath = newTablePathByTableId.get(tableId);
                    if (tablePath == null) {
                        tablePath = cluster.getTablePathOrElseThrow(tableId);
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
                    // Old servers do not send the explicit count but still send the complete
                    // assignment, so normalize both protocol versions into the same Cluster map.
                    // An empty assignment means the layout is not available yet, not zero buckets.
                    int bucketCount =
                            pbPartitionMetadata.hasBucketCount()
                                            && pbPartitionMetadata.getBucketCount() > 0
                                    ? pbPartitionMetadata.getBucketCount()
                                    : pbPartitionMetadata.getBucketMetadatasList().size();
                    if (bucketCount > 0) {
                        newBucketCountByTableOrPartition.put(
                                TableOrPartition.ofPartition(pbPartitionMetadata.getPartitionId()),
                                bucketCount);

                        // TODO: This is a temporary workaround: use the largest observed partition
                        // bucket count as the table-level count. The proper solution is to extend
                        // the metadata RPC path to return the table's bucket count and bucket count
                        // epoch together.
                        TableOrPartition table = TableOrPartition.ofTable(tableId);
                        int previousTableBucketCount =
                                cluster == null ? 0 : cluster.getBucketCount(table).orElse(0);
                        newBucketCountByTableOrPartition.merge(
                                table, Math.max(previousTableBucketCount, bucketCount), Math::max);
                    }
                });

        return new NewTableMetadata(
                newTablePathToTableId,
                newBucketLocations,
                newPartitionIdByPath,
                newBucketCountByTableOrPartition);
    }

    private static final class NewTableMetadata {
        private final Map<TablePath, Long> tablePathToTableId;
        private final Map<PhysicalTablePath, List<BucketLocation>> bucketLocations;
        private final Map<PhysicalTablePath, Long> partitionIdByPath;
        private final Map<TableOrPartition, Integer> bucketCountByTableOrPartition;

        public NewTableMetadata(
                Map<TablePath, Long> tablePathToTableId,
                Map<PhysicalTablePath, List<BucketLocation>> bucketLocations,
                Map<PhysicalTablePath, Long> partitionIdByPath,
                Map<TableOrPartition, Integer> bucketCountByTableOrPartition) {
            this.tablePathToTableId = tablePathToTableId;
            this.bucketLocations = bucketLocations;
            this.partitionIdByPath = partitionIdByPath;
            this.bucketCountByTableOrPartition = bucketCountByTableOrPartition;
        }
    }

    public static @Nullable ServerNode getOneAvailableTabletServerNode(
            Cluster cluster, Set<Integer> unavailableTabletServerIds) {
        return getOneAvailableTabletServerNode(cluster, unavailableTabletServerIds, node -> false);
    }

    /** Selects a nonexcluded node, preferring endpoints that are not backing off. */
    public static @Nullable ServerNode getOneAvailableTabletServerNode(
            Cluster cluster,
            Set<Integer> unavailableTabletServerIds,
            Predicate<ServerNode> isBackingOff) {
        List<ServerNode> aliveTabletServers = new ArrayList<>(cluster.getAliveTabletServerList());
        if (!unavailableTabletServerIds.isEmpty()) {
            aliveTabletServers.removeIf(
                    serverNode -> unavailableTabletServerIds.contains(serverNode.id()));
        }

        return getOneAvailableTabletServerNode(aliveTabletServers, isBackingOff);
    }

    /** Selects a random candidate, preferring endpoints outside reconnect backoff. */
    public static @Nullable ServerNode getOneAvailableTabletServerNode(
            List<ServerNode> candidates, Predicate<ServerNode> isBackingOff) {
        if (candidates.isEmpty()) {
            return null;
        }
        List<ServerNode> selectable = new ArrayList<>(candidates);
        selectable.removeIf(isBackingOff);
        if (selectable.isEmpty()) {
            selectable = candidates;
        }
        return selectable.get(randOffset.nextInt(selectable.size()));
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

    @Nullable
    private static ServerNode getCoordinatorServer(DescribeClusterResponse response) {
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

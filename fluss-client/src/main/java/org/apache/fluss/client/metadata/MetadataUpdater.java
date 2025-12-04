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

package org.apache.fluss.client.metadata;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.utils.ClientUtils;
import org.apache.fluss.cluster.BucketLocation;
import org.apache.fluss.cluster.Cluster;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.PartitionNotExistException;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.utils.ExceptionUtils.stripExecutionException;

/** The updater to initialize and update client metadata. */
public class MetadataUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataUpdater.class);

    private static final int MAX_RETRY_TIMES = 3;
    private static final int RETRY_INTERVAL_MS = 100;

    private final RpcClient rpcClient;
    protected volatile Cluster cluster;

    public MetadataUpdater(Configuration configuration, RpcClient rpcClient) {
        this(rpcClient, initializeCluster(configuration, rpcClient));
    }

    @VisibleForTesting
    public MetadataUpdater(RpcClient rpcClient, Cluster cluster) {
        this.rpcClient = rpcClient;
        this.cluster = cluster;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public @Nullable ServerNode getCoordinatorServer() {
        return cluster.getCoordinatorServer();
    }

    public Optional<Long> getPartitionId(PhysicalTablePath physicalTablePath) {
        return cluster.getPartitionId(physicalTablePath);
    }

    public Long getPartitionIdOrElseThrow(PhysicalTablePath physicalTablePath) {
        return cluster.getPartitionIdOrElseThrow(physicalTablePath);
    }

    public Optional<BucketLocation> getBucketLocation(TableBucket tableBucket) {
        return cluster.getBucketLocation(tableBucket);
    }

    public int leaderFor(TablePath tablePath, TableBucket tableBucket) {
        Integer serverNode = cluster.leaderFor(tableBucket);
        if (serverNode == null) {
            for (int i = 0; i < MAX_RETRY_TIMES; i++) {
                // check if bucket is for a partition
                if (tableBucket.getPartitionId() != null) {
                    updateMetadata(
                            Collections.singleton(tablePath),
                            null,
                            Collections.singleton(tableBucket.getPartitionId()));
                } else {
                    updateMetadata(Collections.singleton(tablePath), null, null);
                }
                serverNode = cluster.leaderFor(tableBucket);
                if (serverNode != null) {
                    break;
                }
            }

            if (serverNode == null) {
                throw new FlussRuntimeException(
                        "Leader not found after retry  "
                                + MAX_RETRY_TIMES
                                + " times for table bucket: "
                                + tableBucket);
            }
        }

        return serverNode;
    }

    private @Nullable ServerNode getTabletServer(int id) {
        return cluster.getTabletServer(id);
    }

    public @Nullable ServerNode getRandomTabletServer() {
        return cluster.getRandomTabletServer();
    }

    public CoordinatorGateway newCoordinatorServerClient() {
        return GatewayClientProxy.createGatewayProxy(
                this::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
    }

    public TabletServerGateway newRandomTabletServerClient() {
        return GatewayClientProxy.createGatewayProxy(
                this::getRandomTabletServer, rpcClient, TabletServerGateway.class);
    }

    public @Nullable TabletServerGateway newTabletServerClientForNode(int serverId) {
        @Nullable final ServerNode serverNode = getTabletServer(serverId);
        if (serverNode == null) {
            return null;
        } else {
            return GatewayClientProxy.createGatewayProxy(
                    () -> serverNode, rpcClient, TabletServerGateway.class);
        }
    }

    public void checkAndUpdateTableMetadata(Set<TablePath> tablePaths) {
        Set<TablePath> needUpdateTablePaths =
                tablePaths.stream()
                        .filter(tablePath -> !cluster.getTable(tablePath).isPresent())
                        .collect(Collectors.toSet());
        if (!needUpdateTablePaths.isEmpty()) {
            updateMetadata(needUpdateTablePaths, null, null);
        }
    }

    /**
     * Check the partition exists in metadata cache, if not, try to update the metadata cache, if
     * not exist yet, throw exception.
     *
     * <p>and update partition metadata .
     */
    public boolean checkAndUpdatePartitionMetadata(PhysicalTablePath physicalTablePath)
            throws PartitionNotExistException {
        if (!cluster.getPartitionId(physicalTablePath).isPresent()) {
            updateMetadata(null, Collections.singleton(physicalTablePath), null);
        }
        return cluster.getPartitionId(physicalTablePath).isPresent();
    }

    /**
     * Check the table/partition info for the given table bucket exist in metadata cache, if not,
     * try to update the metadata cache.
     */
    public void checkAndUpdateMetadata(TablePath tablePath, TableBucket tableBucket) {
        if (tableBucket.getPartitionId() == null) {
            checkAndUpdateTableMetadata(Collections.singleton(tablePath));
        } else {
            checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(tableBucket.getPartitionId()));
        }
    }

    /**
     * Check the partitions info for the given partition ids exist in metadata cache, if not, try to
     * update the metadata cache.
     *
     * <p>Note: it'll assume the partition ids belong to the given {@code tablePath}
     */
    public void checkAndUpdatePartitionMetadata(
            TablePath tablePath, Collection<Long> partitionIds) {
        Set<Long> needUpdatePartitionIds = new HashSet<>();
        for (Long partitionId : partitionIds) {
            if (!cluster.getPartitionName(partitionId).isPresent()) {
                needUpdatePartitionIds.add(partitionId);
            }
        }

        if (!needUpdatePartitionIds.isEmpty()) {
            updateMetadata(Collections.singleton(tablePath), null, needUpdatePartitionIds);
        }
    }

    public void updateTableOrPartitionMetadata(TablePath tablePath, @Nullable Long partitionId) {
        Collection<Long> partitionIds =
                partitionId == null ? null : Collections.singleton(partitionId);
        updateMetadata(Collections.singleton(tablePath), null, partitionIds);
    }

    /** Update the table or partition metadata info. */
    public void updatePhysicalTableMetadata(Set<PhysicalTablePath> physicalTablePaths) {
        Set<TablePath> updateTablePaths = new HashSet<>();
        Set<PhysicalTablePath> updatePartitionPath = new HashSet<>();
        for (PhysicalTablePath physicalTablePath : physicalTablePaths) {
            if (physicalTablePath.getPartitionName() == null) {
                updateTablePaths.add(physicalTablePath.getTablePath());
            } else {
                updatePartitionPath.add(physicalTablePath);
            }
        }
        updateMetadata(updateTablePaths, updatePartitionPath, null);
    }

    @VisibleForTesting
    public void updateMetadata(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitionNames,
            @Nullable Collection<Long> tablePartitionIds)
            throws PartitionNotExistException {
        try {
            synchronized (this) {
                cluster =
                        sendMetadataRequestAndRebuildCluster(
                                cluster,
                                rpcClient,
                                tablePaths,
                                tablePartitionNames,
                                tablePartitionIds);
            }
        } catch (Exception e) {
            Throwable t = stripExecutionException(e);
            if (t instanceof RetriableException || t instanceof TimeoutException) {
                LOG.warn("Failed to update metadata, but the exception is re-triable.", t);
            } else if (t instanceof PartitionNotExistException) {
                LOG.warn("Failed to update metadata because the partition does not exist", t);
                throw (PartitionNotExistException) t;
            } else {
                throw new FlussRuntimeException("Failed to update metadata", t);
            }
        }
    }

    /**
     * Initialize Cluster. This step just to get the coordinator server address and alive tablet
     * servers according to the config {@link ConfigOptions#BOOTSTRAP_SERVERS}.
     */
    private static Cluster initializeCluster(Configuration conf, RpcClient rpcClient) {
        List<InetSocketAddress> inetSocketAddresses =
                ClientUtils.parseAndValidateAddresses(conf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        Cluster cluster = null;
        Exception lastException = null;
        for (InetSocketAddress address : inetSocketAddresses) {
            ServerNode serverNode = null;
            try {
                serverNode =
                        new ServerNode(
                                -1,
                                address.getHostString(),
                                address.getPort(),
                                ServerType.COORDINATOR);
                ServerNode finalServerNode = serverNode;
                AdminReadOnlyGateway adminReadOnlyGateway =
                        GatewayClientProxy.createGatewayProxy(
                                () -> finalServerNode, rpcClient, AdminReadOnlyGateway.class);
                if (inetSocketAddresses.size() == 1) {
                    // if there is only one bootstrap server, we can retry to connect to it.
                    cluster =
                            tryToInitializeClusterWithRetries(
                                    rpcClient, serverNode, adminReadOnlyGateway, MAX_RETRY_TIMES);
                } else {
                    cluster = tryToInitializeCluster(adminReadOnlyGateway);
                    break;
                }
            } catch (Exception e) {
                // We should dis-connected with the bootstrap server id to make sure the next
                // retry can rebuild the connection.
                if (serverNode != null) {
                    rpcClient.disconnect(serverNode.uid());
                }
                LOG.error(
                        "Failed to initialize fluss client connection to bootstrap server: {}",
                        address,
                        e);
                lastException = e;
            }
        }

        if (cluster == null && lastException != null) {
            String errorMsg =
                    "Failed to initialize fluss client connection to bootstrap servers: "
                            + inetSocketAddresses
                            + ". \nReason: "
                            + lastException.getMessage();
            LOG.error(errorMsg);
            throw new IllegalStateException(errorMsg, lastException);
        }

        return cluster;
    }

    @VisibleForTesting
    static @Nullable Cluster tryToInitializeClusterWithRetries(
            RpcClient rpcClient,
            ServerNode serverNode,
            AdminReadOnlyGateway gateway,
            int maxRetryTimes)
            throws Exception {
        int retryCount = 0;
        while (retryCount <= maxRetryTimes) {
            try {
                return tryToInitializeCluster(gateway);
            } catch (Exception e) {
                Throwable cause = stripExecutionException(e);
                // in case of bootstrap is recovering, we should retry to connect.
                if (!(cause instanceof StaleMetadataException
                                || cause instanceof NetworkException
                                || cause instanceof TimeoutException)
                        || retryCount >= maxRetryTimes) {
                    throw e;
                }

                // We should dis-connected with the bootstrap server id to make sure the next
                // retry can rebuild the connection.
                rpcClient.disconnect(serverNode.uid());

                long delayMs = (long) (RETRY_INTERVAL_MS * Math.pow(2, retryCount));
                LOG.warn(
                        "Failed to connect to bootstrap server: {} (retry {}/{}). Retrying in {} ms.",
                        serverNode,
                        retryCount + 1,
                        maxRetryTimes,
                        delayMs,
                        e);

                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry sleep", ex);
                }
                retryCount++;
            }
        }

        return null;
    }

    private static Cluster tryToInitializeCluster(AdminReadOnlyGateway adminReadOnlyGateway)
            throws Exception {
        return sendMetadataRequestAndRebuildCluster(adminReadOnlyGateway, Collections.emptySet());
    }

    /** Invalid the bucket metadata for the given physical table paths. */
    public void invalidPhysicalTableBucketMeta(Set<PhysicalTablePath> physicalTablesToInvalid) {
        if (!physicalTablesToInvalid.isEmpty()) {
            cluster = cluster.invalidPhysicalTableBucketMeta(physicalTablesToInvalid);
        }
    }

    /** Get the table physical paths by table ids and partition ids. */
    public Set<PhysicalTablePath> getPhysicalTablePathByIds(
            @Nullable Collection<Long> tableId,
            @Nullable Collection<TablePartition> tablePartitions) {
        Set<PhysicalTablePath> physicalTablePaths = new HashSet<>();
        if (tableId != null) {
            tableId.forEach(
                    id ->
                            cluster.getTablePath(id)
                                    .ifPresent(
                                            p -> physicalTablePaths.add(PhysicalTablePath.of(p))));
        }

        if (tablePartitions != null) {
            for (TablePartition tablePartition : tablePartitions) {
                cluster.getTablePath(tablePartition.getTableId())
                        .ifPresent(
                                path -> {
                                    Optional<String> optPartition =
                                            cluster.getPartitionName(
                                                    tablePartition.getPartitionId());
                                    optPartition.ifPresent(
                                            p ->
                                                    physicalTablePaths.add(
                                                            PhysicalTablePath.of(path, p)));
                                });
            }
        }
        return physicalTablePaths;
    }
}

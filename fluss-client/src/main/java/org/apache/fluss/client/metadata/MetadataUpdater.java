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
import org.apache.fluss.exception.NotCoordinatorLeaderException;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.fluss.client.utils.MetadataUtils.getOneAvailableTabletServerNode;
import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.utils.ExceptionUtils.stripExecutionException;

/** The updater to initialize and update client metadata. */
public class MetadataUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataUpdater.class);

    private static final int MAX_RETRY_TIMES = 3;
    private static final int RETRY_INTERVAL_MS = 100;

    private final Configuration conf;
    private final RpcClient rpcClient;
    private final Set<Integer> unavailableTabletServerIds = new CopyOnWriteArraySet<>();
    protected volatile Cluster cluster;

    public MetadataUpdater(Configuration conf, RpcClient rpcClient) {
        this(rpcClient, conf, initializeCluster(conf, rpcClient));
    }

    @VisibleForTesting
    public MetadataUpdater(RpcClient rpcClient, Configuration conf, Cluster cluster) {
        this.rpcClient = rpcClient;
        this.conf = conf;
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

    public long getPartitionIdOrElseThrow(PhysicalTablePath physicalTablePath) {
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
                        .filter(tablePath -> !cluster.getTableId(tablePath).isPresent())
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

    /** Checks a partition within the writer's absolute nanoTime deadline. */
    public boolean checkAndUpdatePartitionMetadata(PhysicalTablePath path, long deadlineNanos) {
        if (!cluster.getPartitionId(path).isPresent()) {
            updateMetadata(null, Collections.singleton(path), null, deadlineNanos);
        }
        return cluster.getPartitionId(path).isPresent();
    }

    /**
     * Check the table/partition bucket info for the given table bucket exist in metadata cache, if
     * not, try to update the metadata cache.
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
        updateMetadata(tablePaths, tablePartitionNames, tablePartitionIds, null);
    }

    private void updateMetadata(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitionNames,
            @Nullable Collection<Long> tablePartitionIds,
            @Nullable Long deadlineNanos) {
        ServerNode serverNode =
                getOneAvailableTabletServerNode(cluster, unavailableTabletServerIds);
        boolean refreshingTableMetadata =
                (tablePaths != null && !tablePaths.isEmpty())
                        || (tablePartitionNames != null && !tablePartitionNames.isEmpty())
                        || (tablePartitionIds != null && !tablePartitionIds.isEmpty());
        boolean retryingCoordinator = false;
        try {
            synchronized (this) {
                if (serverNode == null) {
                    LOG.info(
                            "No available tablet server to update metadata, try to re-initialize cluster using bootstrap server.");
                    cluster = initializeCluster(conf, rpcClient, deadlineNanos);
                    if (deadlineNanos != null && refreshingTableMetadata) {
                        unavailableTabletServerIds.removeIf(
                                cluster.getAliveTabletServers()::containsKey);
                        updateMetadata(
                                tablePaths, tablePartitionNames, tablePartitionIds, deadlineNanos);
                        return;
                    }
                } else {
                    try {
                        cluster =
                                requestMetadata(
                                        GatewayClientProxy.createGatewayProxy(
                                                () -> serverNode,
                                                rpcClient,
                                                AdminReadOnlyGateway.class),
                                        tablePaths,
                                        tablePartitionNames,
                                        tablePartitionIds,
                                        deadlineNanos);
                    } catch (Exception e) {
                        if (!refreshingTableMetadata
                                || !(stripExecutionException(e)
                                        instanceof StaleMetadataException)) {
                            throw e;
                        }
                        // A tablet may still describe the previous instance of a reused name.
                        // Retry once against the coordinator, including the owning tables.
                        retryingCoordinator = true;
                        cluster =
                                fetchMetadataFromCoordinator(
                                        tablePaths,
                                        tablePartitionNames,
                                        tablePartitionIds,
                                        deadlineNanos);
                    }
                }
            }

            Map<Integer, ServerNode> aliveTabletServers = cluster.getAliveTabletServers();
            unavailableTabletServerIds.removeIf(aliveTabletServers::containsKey);
            if (!unavailableTabletServerIds.isEmpty()) {
                LOG.info(
                        "After update metadata, unavailable tabletServer set: {}",
                        unavailableTabletServerIds);
            }
        } catch (Exception e) {
            Throwable t = stripExecutionException(e);
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException("Interrupted while updating metadata", t);
            }
            if (deadlineNanos != null
                    && (t instanceof RetriableException
                            || t instanceof NotCoordinatorLeaderException
                            || t instanceof TimeoutException)) {
                if (!retryingCoordinator
                        && serverNode != null
                        && !(t instanceof StaleMetadataException)) {
                    unavailableTabletServerIds.add(serverNode.id());
                }
                StaleMetadataException failure =
                        new StaleMetadataException(
                                "Metadata could not be refreshed within the write preparation budget.");
                failure.initCause(t);
                throw failure;
            }
            if (refreshingTableMetadata && t instanceof StaleMetadataException) {
                // The authoritative retry also failed. Do not let callers interpret an
                // unsuccessful refresh as proof that a partition does not exist.
                throw (StaleMetadataException) t;
            } else if (t instanceof RetriableException
                    || t instanceof NotCoordinatorLeaderException
                    || t instanceof TimeoutException) {
                if (retryingCoordinator) {
                    StaleMetadataException failure =
                            new StaleMetadataException(
                                    "Could not confirm stale tablet metadata with the coordinator.");
                    failure.initCause(t);
                    throw failure;
                }
                if (serverNode != null) {
                    unavailableTabletServerIds.add(serverNode.id());
                    LOG.warn(
                            "tabletServer {} is unavailable for updating metadata for retriable exception. unavailable tabletServer set {}",
                            serverNode,
                            unavailableTabletServerIds);
                }
                LOG.warn("Failed to update metadata, but the exception is re-triable.", t);
            } else if (t instanceof PartitionNotExistException) {
                LOG.debug("Failed to update metadata because the partition does not exist", t);
                throw (PartitionNotExistException) t;
            } else {
                throw new FlussRuntimeException("Failed to update metadata", t);
            }
        }
    }

    /** Refreshes physical table layouts from the coordinator before rejecting stale routing. */
    public synchronized Cluster updatePhysicalTableMetadataFromCoordinator(
            Set<PhysicalTablePath> physicalTablePaths)
            throws ExecutionException, InterruptedException, TimeoutException {
        Set<TablePath> tables = new HashSet<>();
        Set<PhysicalTablePath> partitions = new HashSet<>();
        for (PhysicalTablePath path : physicalTablePaths) {
            tables.add(path.getTablePath());
            if (path.getPartitionName() != null) {
                partitions.add(path);
            }
        }
        cluster = fetchMetadataFromCoordinator(tables, partitions, null, null);
        return cluster;
    }

    private Cluster fetchMetadataFromCoordinator(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> partitions,
            @Nullable Collection<Long> partitionIds,
            @Nullable Long deadlineNanos)
            throws ExecutionException, InterruptedException, TimeoutException {
        long deadline =
                deadlineNanos == null
                        ? System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
                        : deadlineNanos;
        Set<TablePath> tables = tablePaths == null ? new HashSet<>() : new HashSet<>(tablePaths);
        if (partitions != null) {
            partitions.forEach(path -> tables.add(path.getTablePath()));
        }
        try {
            if (getCoordinatorServer() == null) {
                throw new NotCoordinatorLeaderException(
                        "Coordinator is unknown while refreshing metadata.");
            }
            return requestMetadata(
                    newCoordinatorServerClient(), tables, partitions, partitionIds, deadline);
        } catch (NotCoordinatorLeaderException
                | RetriableException
                | ExecutionException
                | TimeoutException e) {
            Throwable cause = stripExecutionException(e);
            if (!(cause instanceof NotCoordinatorLeaderException
                    || cause instanceof TimeoutException
                    || (cause instanceof RetriableException
                            && !(cause instanceof StaleMetadataException)))) {
                throw e;
            }
            // The cached coordinator may have lost leadership or moved. Reuse node discovery
            // before one authoritative retry; repeated calls must not keep using the old address.
            updateMetadata(null, null, null, deadline);
            if (getCoordinatorServer() == null) {
                throw new StaleMetadataException(
                        "Coordinator is still unknown after node discovery.");
            }
            return requestMetadata(
                    newCoordinatorServerClient(), tables, partitions, partitionIds, deadline);
        }
    }

    private Cluster requestMetadata(
            AdminReadOnlyGateway gateway,
            @Nullable Set<TablePath> tables,
            @Nullable Collection<PhysicalTablePath> partitions,
            @Nullable Collection<Long> partitionIds,
            @Nullable Long deadlineNanos)
            throws ExecutionException, InterruptedException, TimeoutException {
        long remaining =
                deadlineNanos == null
                        ? TimeUnit.SECONDS.toNanos(30)
                        : deadlineNanos - System.nanoTime();
        return sendMetadataRequestAndRebuildCluster(
                gateway, true, cluster, tables, partitions, partitionIds, remaining);
    }

    /**
     * Refreshes the cluster node list until a live server is reached, falling back to a bootstrap
     * re-initialization when no tablet server is available.
     *
     * <p>This is the recovery entry point for stale server addresses (e.g., during rolling upgrades
     * where every tablet server IP changes). Unlike {@link #updateMetadata}, which only probes a
     * single server per call, this method loops: each failed server is marked unavailable so the
     * next iteration picks a different one, and once all known servers are exhausted it
     * re-initializes the cluster from the bootstrap servers. The loop is therefore bounded by the
     * number of known servers (at most N attempts + 1 bootstrap) and always converges.
     *
     * <p>No additional backoff is introduced here on purpose: each failed attempt already pays a
     * connection timeout, and the terminal bootstrap path ({@link
     * #tryToInitializeClusterWithRetries}) carries its own exponential backoff. Keeping this method
     * a plain "loop until available or bootstrap" avoids over-engineering and stacking redundant
     * throttling layers.
     */
    public void refreshClusterUntilAvailable() {
        while (true) {
            ServerNode serverNode =
                    getOneAvailableTabletServerNode(cluster, unavailableTabletServerIds);
            if (serverNode == null) {
                // All known tablet servers are unavailable, re-initialize the cluster from the
                // bootstrap servers as the terminal recovery step and stop.
                synchronized (this) {
                    LOG.info(
                            "No available tablet server to refresh metadata, re-initializing cluster using bootstrap server.");
                    cluster = initializeCluster(conf, rpcClient);
                }
                unavailableTabletServerIds.removeIf(cluster.getAliveTabletServers()::containsKey);
                return;
            }

            try {
                synchronized (this) {
                    cluster =
                            sendMetadataRequestAndRebuildCluster(
                                    cluster, rpcClient, null, null, null, serverNode);
                }
                // Refresh succeeded against a live server, the cluster node list is now up to date.
                unavailableTabletServerIds.removeIf(cluster.getAliveTabletServers()::containsKey);
                return;
            } catch (Exception e) {
                Throwable t = stripExecutionException(e);
                if (t instanceof RetriableException || t instanceof TimeoutException) {
                    unavailableTabletServerIds.add(serverNode.id());
                    LOG.warn(
                            "tabletServer {} is unavailable for refreshing metadata for retriable exception, trying the next server. unavailable tabletServer set {}",
                            serverNode,
                            unavailableTabletServerIds,
                            t);
                    // Continue the loop to try the next available server, or fall back to
                    // bootstrap when none remain.
                } else {
                    throw new FlussRuntimeException("Failed to refresh metadata", t);
                }
            }
        }
    }

    /**
     * Initialize Cluster. This step just to get the coordinator server address and alive tablet
     * servers according to the config {@link ConfigOptions#BOOTSTRAP_SERVERS}.
     */
    private static Cluster initializeCluster(Configuration conf, RpcClient rpcClient) {
        return initializeCluster(conf, rpcClient, null);
    }

    private static Cluster initializeCluster(
            Configuration conf, RpcClient rpcClient, @Nullable Long deadlineNanos) {
        List<InetSocketAddress> inetSocketAddresses =
                ClientUtils.parseAndValidateAddresses(conf.get(ConfigOptions.BOOTSTRAP_SERVERS));
        Cluster cluster = null;
        Exception lastException = null;
        for (InetSocketAddress address : inetSocketAddresses) {
            ServerNode serverNode = null;
            try {
                serverNode =
                        new ServerNode(
                                -1, address.getHostString(), address.getPort(), ServerType.UNKNOWN);
                ServerNode finalServerNode = serverNode;
                AdminReadOnlyGateway adminReadOnlyGateway =
                        GatewayClientProxy.createGatewayProxy(
                                () -> finalServerNode, rpcClient, AdminReadOnlyGateway.class);
                if (deadlineNanos != null) {
                    cluster =
                            sendMetadataRequestAndRebuildCluster(
                                    adminReadOnlyGateway,
                                    false,
                                    Cluster.empty(),
                                    null,
                                    null,
                                    null,
                                    deadlineNanos - System.nanoTime());
                    break;
                } else if (inetSocketAddresses.size() == 1) {
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
                if (stripExecutionException(e) instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw new FlussRuntimeException(
                            "Interrupted while discovering cluster nodes", e);
                }
                lastException = e;
            }
        }

        if (cluster == null && lastException != null) {
            Throwable cause = stripExecutionException(lastException);
            if (deadlineNanos != null
                    && (cause instanceof RetriableException || cause instanceof TimeoutException)) {
                StaleMetadataException failure =
                        new StaleMetadataException("Cluster node discovery temporarily failed.");
                failure.initCause(cause);
                throw failure;
            }
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

    /** Invalid the bucket metadata and partition ID mappings for the given physical table paths. */
    public void invalidPhysicalTableBucketAndPartitionMeta(
            Set<PhysicalTablePath> physicalTablesToInvalid) {
        if (!physicalTablesToInvalid.isEmpty()) {
            cluster = cluster.invalidPhysicalTableBucketAndPartitionMeta(physicalTablesToInvalid);
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

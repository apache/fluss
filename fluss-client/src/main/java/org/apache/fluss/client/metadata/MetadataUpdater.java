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
import org.apache.fluss.client.utils.MetadataUtils;
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
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.fluss.client.utils.MetadataUtils.sendMetadataRequestAndRebuildCluster;
import static org.apache.fluss.utils.ExceptionUtils.stripExecutionException;
import static org.apache.fluss.utils.concurrent.LockUtils.inReadLock;
import static org.apache.fluss.utils.concurrent.LockUtils.inWriteLock;

/**
 * The updater to initialize and update client metadata.
 *
 * <p>This class manages metadata updates with batching and deduplication to avoid request storms.
 * Inspired by {@code AdjustIsrManager}, it uses:
 *
 * <ul>
 *   <li>Request deduplication: Multiple concurrent requests for the same resource are merged
 *   <li>Batch processing: All pending requests are sent together in one RPC call
 *   <li>Single in-flight request: Only one metadata request can be in-flight at a time
 *   <li>Asynchronous API: Provides CompletableFuture-based interface to avoid blocking
 * </ul>
 */
public class MetadataUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataUpdater.class);

    private static final int MAX_RETRY_TIMES = 3;
    private static final int RETRY_INTERVAL_MS = 100;

    /** Maximum retry times for metadata update requests before giving up. */
    private static final int MAX_METADATA_UPDATE_RETRIES = 10;

    private final Configuration conf;
    private final RpcClient rpcClient;
    private final ScheduledExecutorService scheduler;
    private final Set<Integer> unavailableTabletServerIds = new CopyOnWriteArraySet<>();

    /** The metadata fetcher, can be injected for testing. */
    private final ClusterMetadataFetcher metadataFetcher;

    /** The current cluster state, protected by read-write lock. */
    @GuardedBy("clusterRWLock")
    protected Cluster cluster;

    /** Read-write lock for cluster access. */
    protected final ReadWriteLock clusterRWLock = new ReentrantReadWriteLock();

    /**
     * Pending metadata update requests. Maps each atomic resource to the pending request info.
     * Multiple requests for the same resource share the same update. Visible for testing.
     */
    @VisibleForTesting
    protected final Map<MetadataResourceKey, PendingMetadataRequest> pendingRequests =
            MapUtils.newConcurrentHashMap();

    /** Used to allow only one in-flight request at a time. */
    private final AtomicBoolean inflightRequest = new AtomicBoolean(false);

    /** Flag to indicate if a retry is already scheduled to avoid duplicate scheduling. */
    private final AtomicBoolean retryScheduled = new AtomicBoolean(false);

    public MetadataUpdater(Configuration conf, RpcClient rpcClient) {
        this(
                rpcClient,
                conf,
                initializeCluster(conf, rpcClient),
                // Adapt MetadataUtils method reference to ClusterMetadataFetcher interface
                (gateway,
                        partialUpdate,
                        originCluster,
                        tablePaths,
                        tablePartitions,
                        tablePartitionIds) ->
                        MetadataUtils.sendMetadataRequestAndRebuildClusterAsync(
                                gateway,
                                partialUpdate,
                                originCluster,
                                tablePaths,
                                tablePartitions,
                                tablePartitionIds));
    }

    @VisibleForTesting
    public MetadataUpdater(RpcClient rpcClient, Configuration conf, Cluster cluster) {
        this(
                rpcClient,
                conf,
                cluster,
                // Adapt MetadataUtils method reference to ClusterMetadataFetcher interface
                (gateway,
                        partialUpdate,
                        originCluster,
                        tablePaths,
                        tablePartitions,
                        tablePartitionIds) ->
                        MetadataUtils.sendMetadataRequestAndRebuildClusterAsync(
                                gateway,
                                partialUpdate,
                                originCluster,
                                tablePaths,
                                tablePartitions,
                                tablePartitionIds));
    }

    @VisibleForTesting
    public MetadataUpdater(
            RpcClient rpcClient,
            Configuration conf,
            Cluster cluster,
            ClusterMetadataFetcher metadataFetcher) {
        this.rpcClient = rpcClient;
        this.conf = conf;
        this.metadataFetcher = metadataFetcher;
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "metadata-updater-scheduler");
                            t.setDaemon(true);
                            return t;
                        });
        this.cluster = cluster;
    }

    // ========== Lifecycle Methods ==========

    /**
     * Close the metadata updater and release resources. This will: 1. Shutdown the scheduler to
     * prevent new retries 2. Cancel all pending requests 3. Clear internal state
     */
    public void close() {
        LOG.info("Closing MetadataUpdater");

        // Shutdown scheduler immediately to prevent new tasks
        scheduler.shutdownNow();

        // Fail all pending requests
        FlussRuntimeException closedException =
                new FlussRuntimeException("MetadataUpdater is closed");
        for (Map.Entry<MetadataResourceKey, PendingMetadataRequest> entry :
                pendingRequests.entrySet()) {
            // Take snapshot and close to get all futures
            List<CompletableFuture<Void>> allFutures = entry.getValue().snapshotAndClose();
            for (CompletableFuture<Void> future : allFutures) {
                future.completeExceptionally(closedException);
            }
        }

        // Clear pending requests to free memory
        pendingRequests.clear();
        unavailableTabletServerIds.clear();

        LOG.info("MetadataUpdater closed");
    }

    // ========== Helper Methods ==========

    /** Await a CompletableFuture and convert exceptions appropriately for metadata operations. */
    private void awaitFuture(CompletableFuture<Void> future) {
        try {
            future.get();
        } catch (ExecutionException e) {
            Throwable t = ExceptionUtils.stripExecutionException(e);
            // Preserve PartitionNotExistException for callers to handle
            if (t instanceof PartitionNotExistException) {
                throw (PartitionNotExistException) t;
            }
            // For retriable exceptions, log warning but don't throw
            else if (t instanceof RetriableException || t instanceof TimeoutException) {
                LOG.warn("Failed to update metadata, but the exception is re-triable.", t);
            }
            // For other exceptions, wrap in FlussRuntimeException
            else {
                throw new FlussRuntimeException("Failed to update metadata", t);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlussRuntimeException("Interrupted while updating metadata", e);
        }
    }

    // ========== Cluster Metadata Access Methods ==========

    public Cluster getCluster() {
        return inReadLock(clusterRWLock, () -> cluster);
    }

    public @Nullable ServerNode getCoordinatorServer() {
        return getCluster().getCoordinatorServer();
    }

    public @Nullable ServerNode getRandomTabletServer() {
        return getCluster().getRandomTabletServer();
    }

    public Optional<Long> getPartitionId(PhysicalTablePath physicalTablePath) {
        return getCluster().getPartitionId(physicalTablePath);
    }

    public Long getPartitionIdOrElseThrow(PhysicalTablePath physicalTablePath) {
        return getCluster().getPartitionIdOrElseThrow(physicalTablePath);
    }

    public Optional<BucketLocation> getBucketLocation(TableBucket tableBucket) {
        return getCluster().getBucketLocation(tableBucket);
    }

    public int leaderFor(TablePath tablePath, TableBucket tableBucket) {
        Integer serverNode = getCluster().leaderFor(tableBucket);
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
                serverNode = getCluster().leaderFor(tableBucket);
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

    public Set<PhysicalTablePath> getPhysicalTablePathByIds(
            @Nullable Collection<Long> tableId,
            @Nullable Collection<TablePartition> tablePartitions) {
        Set<PhysicalTablePath> physicalTablePaths = new HashSet<>();
        Cluster currentCluster = getCluster();

        if (tableId != null) {
            tableId.forEach(
                    id ->
                            currentCluster
                                    .getTablePath(id)
                                    .ifPresent(
                                            p -> physicalTablePaths.add(PhysicalTablePath.of(p))));
        }

        if (tablePartitions != null) {
            for (TablePartition tablePartition : tablePartitions) {
                currentCluster
                        .getTablePath(tablePartition.getTableId())
                        .ifPresent(
                                path -> {
                                    Optional<String> optPartition =
                                            currentCluster.getPartitionName(
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

    // ========== RPC Client Creation Methods ==========

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

    private @Nullable ServerNode getTabletServer(int id) {
        return getCluster().getTabletServer(id);
    }

    // ========== Metadata Update Methods (Synchronous) ==========

    /**
     * Check and update table metadata if needed. Only updates tables that are not present in cache.
     */
    public void checkAndUpdateTableMetadata(Set<TablePath> tablePaths) {
        awaitFuture(checkAndUpdateTableMetadataAsync(tablePaths));
    }

    /**
     * Check and update partition metadata. Returns whether partition exists after update.
     *
     * @throws PartitionNotExistException if partition does not exist after update
     */
    public boolean checkAndUpdatePartitionMetadata(PhysicalTablePath physicalTablePath)
            throws PartitionNotExistException {
        Cluster currentCluster = getCluster();
        if (!currentCluster.getPartitionId(physicalTablePath).isPresent()) {
            updateMetadata(null, Collections.singleton(physicalTablePath), null);
        }
        return getCluster().getPartitionId(physicalTablePath).isPresent();
    }

    /**
     * Check and update partition metadata by partition IDs.
     *
     * <p>Note: Assumes the partition IDs belong to the given table.
     */
    public void checkAndUpdatePartitionMetadata(
            TablePath tablePath, Collection<Long> partitionIds) {
        Cluster currentCluster = getCluster();
        Set<Long> needUpdatePartitionIds = new HashSet<>();
        for (Long partitionId : partitionIds) {
            if (!currentCluster.getPartitionName(partitionId).isPresent()) {
                needUpdatePartitionIds.add(partitionId);
            }
        }

        if (!needUpdatePartitionIds.isEmpty()) {
            updateMetadata(Collections.singleton(tablePath), null, needUpdatePartitionIds);
        }
    }

    /**
     * Check and update metadata for a table bucket. Updates either table or partition metadata
     * based on bucket type.
     */
    public void checkAndUpdateMetadata(TablePath tablePath, TableBucket tableBucket) {
        if (tableBucket.getPartitionId() == null) {
            checkAndUpdateTableMetadata(Collections.singleton(tablePath));
        } else {
            checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(tableBucket.getPartitionId()));
        }
    }

    /** Update table or partition metadata. Forces a metadata update regardless of cache state. */
    public void updateTableOrPartitionMetadata(TablePath tablePath, @Nullable Long partitionId) {
        Collection<Long> partitionIds =
                partitionId == null ? null : Collections.singleton(partitionId);
        updateMetadata(Collections.singleton(tablePath), null, partitionIds);
    }

    /** Update physical table metadata. Separates tables and partitions and updates accordingly. */
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

    /**
     * Update metadata for tables, partitions, and partition IDs. This is the main synchronous
     * update method.
     */
    @VisibleForTesting
    public void updateMetadata(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitionNames,
            @Nullable Collection<Long> tablePartitionIds)
            throws PartitionNotExistException {
        awaitFuture(updateMetadataAsync(tablePaths, tablePartitionNames, tablePartitionIds));
    }

    /** Invalidate bucket metadata for given physical table paths. */
    public void invalidPhysicalTableBucketMeta(Set<PhysicalTablePath> physicalTablesToInvalid) {
        if (!physicalTablesToInvalid.isEmpty()) {
            inWriteLock(
                    clusterRWLock,
                    () -> {
                        cluster = cluster.invalidPhysicalTableBucketMeta(physicalTablesToInvalid);
                        return null;
                    });
        }
    }

    // ========== Metadata Update Methods (Asynchronous) ==========

    /**
     * Async version of checkAndUpdateTableMetadata. Only updates tables that are not present in
     * cache.
     */
    private CompletableFuture<Void> checkAndUpdateTableMetadataAsync(Set<TablePath> tablePaths) {
        Cluster currentCluster = getCluster();
        Set<TablePath> needUpdateTablePaths =
                tablePaths.stream()
                        .filter(tablePath -> !currentCluster.getTableId(tablePath).isPresent())
                        .collect(Collectors.toSet());
        if (needUpdateTablePaths.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Submit each table as atomic resource
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (TablePath tablePath : needUpdateTablePaths) {
            futures.add(submitTableUpdateAsync(tablePath));
        }

        // Wait for all to complete
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Async version that checks and updates partition metadata, returning the partition ID.
     *
     * <p>Uses request batching and deduplication to avoid metadata request storms.
     */
    public CompletableFuture<Long> checkAndUpdatePartitionMetadataAsync(
            PhysicalTablePath physicalTablePath) {
        // First check if partition already exists in current cluster (with read lock)
        Cluster currentCluster = getCluster();
        if (currentCluster.getPartitionId(physicalTablePath).isPresent()) {
            return CompletableFuture.completedFuture(
                    currentCluster.getPartitionIdOrElseThrow(physicalTablePath));
        }

        // Not in cache, need to fetch from server with batching
        return submitPartitionUpdateAsync(physicalTablePath)
                .thenApply(
                        v -> {
                            return inReadLock(
                                    clusterRWLock,
                                    () -> {
                                        if (cluster.getPartitionId(physicalTablePath).isPresent()) {
                                            return cluster.getPartitionIdOrElseThrow(
                                                    physicalTablePath);
                                        } else {
                                            throw new FlussRuntimeException(
                                                    new PartitionNotExistException(
                                                            "Partition does not exist after update: "
                                                                    + physicalTablePath));
                                        }
                                    });
                        });
    }

    // ========== Internal Batching and Request Management ==========

    /**
     * Generic method to submit a metadata update request for any resource type.
     *
     * <p>This method handles the common logic of: - Atomically adding the future to an existing or
     * new request - Handling race conditions with closed requests - Triggering metadata update
     * propagation
     *
     * @param key the metadata resource key
     * @return a future that completes when the metadata is updated
     */
    private CompletableFuture<Void> submitMetadataUpdateAsync(MetadataResourceKey key) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        // Use compute() to atomically add future or create new request if needed
        // This prevents race condition where multiple threads might overwrite each other's requests
        pendingRequests.compute(
                key,
                (k, existingRequest) -> {
                    if (existingRequest == null || existingRequest.isClosed()) {
                        // No existing request or existing request is closed, create new one
                        PendingMetadataRequest newRequest = new PendingMetadataRequest();
                        newRequest.addFuture(future);
                        return newRequest;
                    } else {
                        // Try to add to existing request
                        boolean added = existingRequest.addFuture(future);
                        if (added) {
                            // Successfully added to existing request
                            return existingRequest;
                        } else {
                            // Failed to add (request was closed concurrently), create new one
                            // This should rarely happen as we already checked isClosed() above
                            LOG.debug(
                                    "Failed to add future to existing request for {}, creating new request",
                                    key);
                            PendingMetadataRequest newRequest = new PendingMetadataRequest();
                            newRequest.addFuture(future);
                            return newRequest;
                        }
                    }
                });

        maybePropagateMetadataUpdate();
        return future;
    }

    /**
     * Submit a request to update metadata for a single table. Returns a future that completes when
     * the table metadata is updated.
     */
    private CompletableFuture<Void> submitTableUpdateAsync(TablePath tablePath) {
        return submitMetadataUpdateAsync(MetadataResourceKey.forTable(tablePath));
    }

    /**
     * Submit a request to update metadata for a single partition. Returns a future that completes
     * when the partition metadata is updated.
     */
    private CompletableFuture<Void> submitPartitionUpdateAsync(PhysicalTablePath partition) {
        return submitMetadataUpdateAsync(MetadataResourceKey.forPartition(partition));
    }

    /**
     * Submit a request to update metadata for a single partition ID. Returns a future that
     * completes when the partition metadata is updated.
     */
    private CompletableFuture<Void> submitPartitionIdUpdateAsync(Long partitionId) {
        return submitMetadataUpdateAsync(MetadataResourceKey.forPartitionId(partitionId));
    }

    /**
     * Clean up exhausted pending requests to prevent memory leaks.
     *
     * <p>A request is cleaned up if it has exceeded the maximum retry count.
     *
     * <p>Cleaned up requests will have their futures completed exceptionally.
     *
     * @param snapshot snapshot of pending requests to check and cleanup
     * @return keys that were cleaned up and should be removed from further processing
     */
    private Set<MetadataResourceKey> cleanupExhaustedRequests(
            Map<MetadataResourceKey, PendingMetadataRequest> snapshot) {
        Set<MetadataResourceKey> keysToRemove = new HashSet<>();
        for (Map.Entry<MetadataResourceKey, PendingMetadataRequest> entry : snapshot.entrySet()) {
            MetadataResourceKey key = entry.getKey();
            PendingMetadataRequest request = entry.getValue();

            if (request.getRetryCount() >= MAX_METADATA_UPDATE_RETRIES) {
                LOG.warn(
                        "Cleaning up pending metadata request for {}: exceeded maximum retries ({})",
                        key,
                        MAX_METADATA_UPDATE_RETRIES);
                // Atomically remove from pending map and close to get all futures
                pendingRequests.remove(key);
                keysToRemove.add(key);
                List<CompletableFuture<Void>> allFutures = request.snapshotAndClose();
                // Fail all futures for this resource
                TimeoutException exception =
                        new TimeoutException(
                                String.format(
                                        "Metadata update failed for %s: exceeded maximum retries (%d)",
                                        key, MAX_METADATA_UPDATE_RETRIES));
                for (CompletableFuture<Void> future : allFutures) {
                    future.completeExceptionally(exception);
                }
            }
        }
        return keysToRemove;
    }

    /** Trigger sending pending metadata requests if no request is currently in-flight. */
    private void maybePropagateMetadataUpdate() {
        // Send all pending items if there is not already a request in-flight
        if (!pendingRequests.isEmpty() && inflightRequest.compareAndSet(false, true)) {
            try {
                sendBatchedMetadataRequest();
            } catch (Throwable e) {
                // Handle any top-level exceptions
                LOG.error("Failed to send batched metadata request", e);
                clearInFlightRequest();
                // Retry in near future, avoid duplicate scheduling
                maybeRetry();
            }
        }
    }

    /** Schedule a retry if not already scheduled to avoid task accumulation. */
    private void maybeRetry() {
        if (retryScheduled.compareAndSet(false, true)) {
            try {
                scheduler.schedule(
                        () -> {
                            retryScheduled.set(false);
                            maybePropagateMetadataUpdate();
                        },
                        50,
                        TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                // If scheduling fails (e.g., scheduler is shut down), reset the flag
                // and log the error. Pending requests will be cleaned up by close() method.
                retryScheduled.set(false);
                LOG.error(
                        "Failed to schedule metadata update retry. "
                                + "Pending requests will be failed when MetadataUpdater is closed.",
                        e);
            }
        }
    }

    /** Aggregate all pending requests and send a single batched metadata request. */
    @VisibleForTesting
    protected void sendBatchedMetadataRequest() {
        // Step 1: Take a snapshot of all pending requests (by reference)
        // We intentionally don't close them yet to allow new futures to join
        // This improves batching efficiency - futures added during RPC will also be completed
        Map<MetadataResourceKey, PendingMetadataRequest> snapshot = new HashMap<>();
        for (Map.Entry<MetadataResourceKey, PendingMetadataRequest> entry :
                pendingRequests.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue());
        }

        if (snapshot.isEmpty()) {
            clearInFlightRequest();
            return;
        }

        // Wrap everything in try-catch to ensure futures are always completed
        try {
            // Clean up exhausted requests to prevent memory leaks
            Set<MetadataResourceKey> exhaustedKeys = cleanupExhaustedRequests(snapshot);
            // Remove exhausted keys from snapshot
            exhaustedKeys.forEach(snapshot::remove);

            // Re-check after cleanup
            if (snapshot.isEmpty()) {
                clearInFlightRequest();
                return;
            }

            // Continue with normal processing...
            sendBatchedMetadataRequestInternal(snapshot);
        } catch (Throwable t) {
            // Critical: if any unexpected exception occurs, fail all pending futures
            // to prevent indefinite blocking
            LOG.error(
                    "Unexpected error in sendBatchedMetadataRequest, failing all pending requests",
                    t);
            failAllRequests(snapshot, t);
            clearInFlightRequest();
            // Schedule retry for safety
            maybeRetry();
        }
    }

    /**
     * Helper method to fail all requests in a snapshot with a given exception. Ensures all futures
     * are completed to prevent memory leaks and blocking.
     */
    private void failAllRequests(
            Map<MetadataResourceKey, PendingMetadataRequest> snapshot, Throwable cause) {
        for (Map.Entry<MetadataResourceKey, PendingMetadataRequest> entry : snapshot.entrySet()) {
            // Atomically remove from pending map and close to get all futures
            pendingRequests.remove(entry.getKey());
            List<CompletableFuture<Void>> allFutures = entry.getValue().snapshotAndClose();
            FlussRuntimeException exception =
                    new FlussRuntimeException(
                            "Metadata update failed due to unexpected error", cause);
            for (CompletableFuture<Void> future : allFutures) {
                future.completeExceptionally(exception);
            }
        }
    }

    /** Internal method to send batched metadata request. Separated for better error handling. */
    private void sendBatchedMetadataRequestInternal(
            Map<MetadataResourceKey, PendingMetadataRequest> snapshot) {

        // Aggregate all atomic resources by type
        Set<TablePath> allTablePaths = new HashSet<>();
        Set<PhysicalTablePath> allPartitions = new HashSet<>();
        Set<Long> allPartitionIds = new HashSet<>();

        for (MetadataResourceKey key : snapshot.keySet()) {
            switch (key.type) {
                case TABLE:
                    allTablePaths.add(key.asTablePath());
                    break;
                case PARTITION:
                    allPartitions.add(key.asPhysicalTablePath());
                    break;
                case PARTITION_ID:
                    allPartitionIds.add(key.asPartitionId());
                    break;
            }
        }

        LOG.debug(
                "Sending batched metadata request: tables={}, partitions={}, partitionIds={}",
                allTablePaths.size(),
                allPartitions.size(),
                allPartitionIds.size());

        // Get gateway to an available tablet server
        Cluster currentSnapshot = getCluster();
        ServerNode serverNode =
                MetadataUtils.getOneAvailableTabletServerNode(
                        currentSnapshot, unavailableTabletServerIds);

        if (serverNode == null) {
            LOG.info(
                    "No available tablet server to update metadata, trying to re-initialize cluster");
            handleNoAvailableServer(snapshot);
            return;
        }

        // Create gateway and send request
        AdminReadOnlyGateway gateway =
                GatewayClientProxy.createGatewayProxy(
                        () -> serverNode, rpcClient, AdminReadOnlyGateway.class);

        // Use injected metadataFetcher to fetch metadata and rebuild cluster
        metadataFetcher
                .fetch(
                        gateway,
                        true, // partial update - merge with existing cluster
                        currentSnapshot,
                        allTablePaths.isEmpty() ? null : allTablePaths,
                        allPartitions.isEmpty() ? null : allPartitions,
                        allPartitionIds.isEmpty() ? null : allPartitionIds)
                .whenComplete(
                        (newCluster, exception) -> {
                            try {
                                if (exception != null) {
                                    // Mark this server as potentially unavailable
                                    Throwable t = stripExecutionException(exception);
                                    if (t instanceof RetriableException
                                            || t instanceof TimeoutException) {
                                        unavailableTabletServerIds.add(serverNode.id());
                                        LOG.warn(
                                                "Marking tablet server {} as unavailable due to error",
                                                serverNode.id(),
                                                t);
                                    }
                                    handleMetadataRequestError(snapshot, exception);
                                    // Retry in near future
                                    maybeRetry();
                                } else {
                                    handleMetadataResponseWithNewCluster(newCluster, snapshot);
                                }
                            } finally {
                                clearInFlightRequest();
                            }
                            // Try to send more pending requests if any
                            maybePropagateMetadataUpdate();
                        });
    }

    /**
     * Handle the case where no tablet server is available.
     *
     * <p>Important: This method must ensure that all futures in the snapshot are eventually
     * completed, either by re-initializing and retrying, or by failing them explicitly.
     */
    private void handleNoAvailableServer(
            Map<MetadataResourceKey, PendingMetadataRequest> snapshot) {
        try {
            // Try to re-initialize cluster from bootstrap servers
            Cluster newCluster = initializeCluster(conf, rpcClient);
            inWriteLock(
                    clusterRWLock,
                    () -> {
                        cluster = newCluster;
                        return null;
                    });
            // Clear unavailable server set since we have fresh cluster info
            unavailableTabletServerIds.clear();

            // Successfully re-initialized cluster, retry without incrementing retry count
            // The requests in snapshot are still in pendingRequests and will be processed
            // Note: We don't remove requests from pendingRequests or close them
            // They will be retried by the next maybePropagateMetadataUpdate() call
            clearInFlightRequest();
            maybePropagateMetadataUpdate();
        } catch (Exception e) {
            // Re-initialization failed
            // Increment retry count for all requests
            for (PendingMetadataRequest request : snapshot.values()) {
                request.incrementAndGetRetryCount();
            }
            // handleMetadataRequestError will complete futures that exceeded max retries
            // or keep them for retry if still under limit
            handleMetadataRequestError(snapshot, e);
            clearInFlightRequest();
            // Schedule retry - this ensures pending requests will be retried
            maybeRetry();
        }
    }

    /** Handle successful metadata response with the new cluster built by MetadataUtils. */
    private void handleMetadataResponseWithNewCluster(
            Cluster newCluster, Map<MetadataResourceKey, PendingMetadataRequest> snapshot) {
        try {
            // Update cluster with write lock (single writer)
            inWriteLock(
                    clusterRWLock,
                    () -> {
                        cluster = newCluster;
                        return null;
                    });

            // Complete all futures successfully
            // Important: We only remove and close the request if it's still the same object
            // in pendingRequests. This prevents completing futures from a newer request.
            for (Map.Entry<MetadataResourceKey, PendingMetadataRequest> entry :
                    snapshot.entrySet()) {
                MetadataResourceKey key = entry.getKey();
                PendingMetadataRequest request = entry.getValue();

                // Atomically remove and close the request
                List<CompletableFuture<Void>> allFutures =
                        removeAndClosePendingRequest(key, request);
                // Complete all futures for this resource (including ones added during RPC)
                // If list is empty, it means the request was replaced by a new one
                for (CompletableFuture<Void> future : allFutures) {
                    future.complete(null);
                }
            }

            // Clear unavailable servers as we successfully communicated
            Map<Integer, ServerNode> aliveServers = getCluster().getAliveTabletServers();
            unavailableTabletServerIds.removeIf(aliveServers::containsKey);

        } catch (Exception e) {
            LOG.error("Failed to process metadata response", e);
            // Increment retry count for all requests
            for (PendingMetadataRequest request : snapshot.values()) {
                request.incrementAndGetRetryCount();
            }
            handleMetadataRequestError(snapshot, e);
        }
    }

    /** Handle metadata request error. */
    private void handleMetadataRequestError(
            Map<MetadataResourceKey, PendingMetadataRequest> snapshot, Throwable error) {
        LOG.warn("Metadata request failed", error);

        Throwable cause = stripExecutionException(error);

        // For non-retriable errors, fail the requests immediately
        if (!(cause instanceof RetriableException) && !(cause instanceof TimeoutException)) {
            for (Map.Entry<MetadataResourceKey, PendingMetadataRequest> entry :
                    snapshot.entrySet()) {
                MetadataResourceKey key = entry.getKey();
                PendingMetadataRequest request = entry.getValue();

                // Atomically remove and close the request
                List<CompletableFuture<Void>> allFutures =
                        removeAndClosePendingRequest(key, request);
                // Fail all futures for this resource
                for (CompletableFuture<Void> future : allFutures) {
                    // Pass the original exception without wrapping to preserve exception type
                    // (e.g., PartitionNotExistException needs to be caught by callers)
                    future.completeExceptionally(cause);
                }
            }
        } else {
            // For retriable errors, check retry count and decide
            for (Map.Entry<MetadataResourceKey, PendingMetadataRequest> entry :
                    snapshot.entrySet()) {
                MetadataResourceKey key = entry.getKey();
                PendingMetadataRequest request = entry.getValue();
                int retryCount = request.getRetryCount();

                // Check if exceeded max retries
                if (retryCount >= MAX_METADATA_UPDATE_RETRIES) {
                    LOG.warn(
                            "Metadata request for {} exceeded max retries ({}), failing request",
                            key,
                            MAX_METADATA_UPDATE_RETRIES);
                    // Atomically remove and close the request
                    List<CompletableFuture<Void>> allFutures =
                            removeAndClosePendingRequest(key, request);
                    // Fail all futures
                    TimeoutException timeoutException =
                            new TimeoutException(
                                    String.format(
                                            "Metadata update failed after %d retries for %s",
                                            MAX_METADATA_UPDATE_RETRIES, key));
                    for (CompletableFuture<Void> future : allFutures) {
                        future.completeExceptionally(timeoutException);
                    }
                }
            }
            // Keep remaining items in the map for retry
            // They will be retried by the scheduled task
        }
    }

    /** Clear the in-flight request flag. */
    @VisibleForTesting
    protected void clearInFlightRequest() {
        if (!inflightRequest.compareAndSet(true, false)) {
            LOG.warn("Attempting to clear in-flight flag when no request is in-flight");
        }
    }

    /**
     * Atomically remove a pending request and close it to get all futures.
     *
     * <p>This helper method encapsulates the common pattern of: 1. Checking if the request is still
     * the same object in the map 2. Removing it atomically 3. Closing it to get all futures
     *
     * @return all futures from the request, or empty list if the request was already replaced
     */
    private List<CompletableFuture<Void>> removeAndClosePendingRequest(
            MetadataResourceKey key, PendingMetadataRequest request) {
        boolean removed = pendingRequests.remove(key, request);
        if (removed) {
            return request.snapshotAndClose();
        }
        return Collections.emptyList();
    }

    /**
     * Async version that updates metadata for tables, partitions, and partition IDs.
     *
     * <p>This is the core async implementation that uses request batching and deduplication to
     * avoid metadata request storms.
     */
    public CompletableFuture<Void> updateMetadataAsync(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePartitionNames,
            @Nullable Collection<Long> tablePartitionIds) {
        // Collect all futures for atomic resources
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Submit each table as atomic resource
        if (tablePaths != null) {
            for (TablePath tablePath : tablePaths) {
                futures.add(submitTableUpdateAsync(tablePath));
            }
        }

        // Submit each partition as atomic resource
        if (tablePartitionNames != null) {
            for (PhysicalTablePath partition : tablePartitionNames) {
                futures.add(submitPartitionUpdateAsync(partition));
            }
        }

        // Submit each partition ID as atomic resource
        if (tablePartitionIds != null) {
            for (Long partitionId : tablePartitionIds) {
                futures.add(submitPartitionIdUpdateAsync(partitionId));
            }
        }

        if (futures.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        // Wait for all to complete
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    // ========== Cluster Initialization Methods ==========

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

    /**
     * Tracks metadata update request with retry information to prevent memory leaks.
     *
     * <p>Thread-safety: Uses ReadWriteLock to allow concurrent reads while ensuring exclusive
     * writes. This is beneficial because:
     *
     * <ul>
     *   <li>Read operations (isClosed, getRetryCount) may be called frequently by multiple threads
     *   <li>Write operations (addFuture, incrementAndGetRetryCount) need exclusive access
     *   <li>ReadWriteLock allows multiple concurrent readers, improving throughput in high
     *       concurrency scenarios
     * </ul>
     */
    @VisibleForTesting
    protected static class PendingMetadataRequest {
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final List<CompletableFuture<Void>> futures = new ArrayList<>();
        private int retryCount = 0;
        private boolean closed = false;

        /**
         * Add a future to this request. Returns false if the request is already closed.
         *
         * <p>Uses write lock to ensure atomicity with snapshotAndClose.
         */
        boolean addFuture(CompletableFuture<Void> future) {
            return inWriteLock(
                    lock,
                    () -> {
                        if (closed) {
                            return false;
                        }
                        futures.add(future);
                        return true;
                    });
        }

        /**
         * Take a snapshot of all futures and mark this request as closed.
         *
         * <p>This operation atomically closes the request and gets all futures. After this call, no
         * new futures can be added.
         *
         * @return all futures that were added before closing
         */
        List<CompletableFuture<Void>> snapshotAndClose() {
            return inWriteLock(
                    lock,
                    () -> {
                        closed = true;
                        return new ArrayList<>(futures);
                    });
        }

        /** Increment and get retry count. Uses write lock as it modifies state. */
        int incrementAndGetRetryCount() {
            return inWriteLock(lock, () -> ++retryCount);
        }

        /**
         * Get current retry count. Uses read lock to allow concurrent access with other readers.
         */
        int getRetryCount() {
            return inReadLock(lock, () -> retryCount);
        }

        /**
         * Check if closed. Uses read lock to allow concurrent access with other readers.
         *
         * <p>This is particularly important as this method may be called frequently in {@link
         * #submitMetadataUpdateAsync} by multiple threads checking if they can add futures to an
         * existing request.
         */
        boolean isClosed() {
            return inReadLock(lock, () -> closed);
        }
    }

    /**
     * Key to identify and deduplicate atomic metadata resources. Each key represents exactly one
     * resource (table, partition, or partition ID).
     */
    @VisibleForTesting
    protected static class MetadataResourceKey {
        protected final ResourceType type;
        protected final Object resource;

        /** Types of atomic metadata resources. */
        enum ResourceType {
            TABLE, // Single table
            PARTITION, // Single partition
            PARTITION_ID // Single partition ID
        }

        private MetadataResourceKey(ResourceType type, Object resource) {
            this.type = type;
            this.resource = resource;
        }

        static MetadataResourceKey forTable(TablePath tablePath) {
            return new MetadataResourceKey(ResourceType.TABLE, tablePath);
        }

        static MetadataResourceKey forPartition(PhysicalTablePath partition) {
            return new MetadataResourceKey(ResourceType.PARTITION, partition);
        }

        static MetadataResourceKey forPartitionId(Long partitionId) {
            return new MetadataResourceKey(ResourceType.PARTITION_ID, partitionId);
        }

        TablePath asTablePath() {
            return (TablePath) resource;
        }

        PhysicalTablePath asPhysicalTablePath() {
            return (PhysicalTablePath) resource;
        }

        Long asPartitionId() {
            return (Long) resource;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MetadataResourceKey that = (MetadataResourceKey) o;
            return type == that.type && resource.equals(that.resource);
        }

        @Override
        public int hashCode() {
            return 31 * type.hashCode() + resource.hashCode();
        }

        @Override
        public String toString() {
            return type + ":" + resource;
        }
    }
}

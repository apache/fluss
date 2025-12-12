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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AcquireColumnLockRequest;
import org.apache.fluss.rpc.messages.ReleaseColumnLockRequest;
import org.apache.fluss.rpc.messages.RenewColumnLockRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Client-side column lock manager for write operations.
 *
 * <p>This class handles acquiring, renewing, and releasing column locks for tables. It maintains a
 * background thread that automatically renews locks before they expire.
 *
 * <p>Locks are table-scoped, not bucket-scoped or partition-scoped. A lock acquired for a table
 * applies to the specified columns (or all columns if none specified) across all buckets and
 * partitions in that table.
 */
@ThreadSafe
@Internal
public class ClientColumnLockManager {
    private static final Logger LOG = LoggerFactory.getLogger(ClientColumnLockManager.class);

    private static final String LOCK_RENEW_THREAD_NAME = "fluss-lock-renewer";
    // 30 seconds default TTL
    private static final long DEFAULT_LOCK_TTL_MS = 30000;
    // Check every 5 seconds
    private static final long RENEWAL_CHECK_INTERVAL_MS = 5000;
    // Renew lock when half of TTL has elapsed
    private static final double RENEWAL_THRESHOLD_RATIO = 0.5;
    // Timeout for executor shutdown during close
    private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 5;
    // Timeout for releasing all locks during close
    private static final long CLOSE_RELEASE_TIMEOUT_SECONDS = 10;

    /** Clock abstraction for testability. */
    @VisibleForTesting
    interface Clock {
        long currentTimeMillis();

        Clock SYSTEM = System::currentTimeMillis;
    }

    /** Lock state enum to track reacquisition status. */
    private enum LockState {
        ACTIVE,
        REACQUIRING
    }

    private final MetadataUpdater metadataUpdater;
    private final Map<LockKey, LockInfo> activeLocks;
    private final Map<LockKey, LockState> lockStates;
    private final ScheduledExecutorService renewalExecutor;
    private final Clock clock;
    private volatile boolean closed = false;

    public ClientColumnLockManager(MetadataUpdater metadataUpdater) {
        this(metadataUpdater, Clock.SYSTEM);
    }

    @VisibleForTesting
    ClientColumnLockManager(MetadataUpdater metadataUpdater, Clock clock) {
        this.metadataUpdater = metadataUpdater;
        this.activeLocks = MapUtils.newConcurrentHashMap();
        this.lockStates = MapUtils.newConcurrentHashMap();
        this.clock = clock;
        this.renewalExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, LOCK_RENEW_THREAD_NAME);
                            t.setDaemon(true);
                            return t;
                        });

        // Start the renewal task
        renewalExecutor.scheduleAtFixedRate(
                this::renewLocks,
                RENEWAL_CHECK_INTERVAL_MS,
                RENEWAL_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Lock key that uniquely identifies a column lock. Includes table ID and column indexes to
     * support fine-grained column-level locking.
     *
     * <p>This is used by writers to identify and release their locks. For partitioned tables, the
     * lock is table-scoped (applies to all partitions).
     */
    public static class LockKey {
        private final long tableId;
        private final int[] columnIndexes; // null means all columns
        private final int hashCode;

        LockKey(long tableId, @Nullable int[] columnIndexes) {
            this.tableId = tableId;
            // Defensive copy to prevent external modification
            this.columnIndexes =
                    columnIndexes != null
                            ? Arrays.copyOf(columnIndexes, columnIndexes.length)
                            : null;
            // Pre-compute hash code for better performance
            this.hashCode = computeHashCode(tableId, this.columnIndexes);
        }

        private static int computeHashCode(long tableId, @Nullable int[] columnIndexes) {
            int result = Long.hashCode(tableId);
            result = 31 * result + Arrays.hashCode(columnIndexes);
            return result;
        }

        public long getTableId() {
            return tableId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            LockKey lockKey = (LockKey) o;
            return tableId == lockKey.tableId
                    && Arrays.equals(columnIndexes, lockKey.columnIndexes);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return "LockKey{"
                    + "tableId="
                    + tableId
                    + ", columns="
                    + (columnIndexes == null ? "all" : Arrays.toString(columnIndexes))
                    + '}';
        }
    }

    /** Internal class to hold lock state information. */
    private static class LockInfo {
        private final long tableId;
        private final String ownerId;
        private final int schemaId;
        private final @Nullable int[] columnIndexes;
        private final long ttlMs;
        private final long acquiredTimeMs;
        private volatile long lastRenewTimeMs;
        // Store all TabletServer IDs that have been locked
        private final List<Integer> lockedServerIds;

        LockInfo(
                long tableId,
                List<Integer> lockedServerIds,
                String ownerId,
                int schemaId,
                @Nullable int[] columnIndexes,
                long ttlMs,
                long acquiredTimeMs) {
            this.tableId = tableId;
            this.lockedServerIds = lockedServerIds;
            this.ownerId = ownerId;
            this.schemaId = schemaId;
            this.columnIndexes = columnIndexes;
            this.ttlMs = ttlMs;
            this.acquiredTimeMs = acquiredTimeMs;
            this.lastRenewTimeMs = acquiredTimeMs;
        }

        public long getTableId() {
            return tableId;
        }

        public List<Integer> getLockedServerIds() {
            return lockedServerIds;
        }

        void updateRenewTime(long renewTimeMs) {
            this.lastRenewTimeMs = renewTimeMs;
        }

        boolean needsRenewal(long currentTimeMs) {
            return (currentTimeMs - lastRenewTimeMs) > (ttlMs * RENEWAL_THRESHOLD_RATIO);
        }

        boolean isExpired(long currentTimeMs) {
            return (currentTimeMs - lastRenewTimeMs) > ttlMs;
        }

        @Override
        public String toString() {
            return "LockInfo{"
                    + "tableId="
                    + tableId
                    + ", lockedServers="
                    + lockedServerIds.size()
                    + ", ownerId='"
                    + ownerId
                    + '\''
                    + ", schemaId="
                    + schemaId
                    + ", columnIndexes="
                    + (columnIndexes == null ? "all" : columnIndexes.length)
                    + ", ttlMs="
                    + ttlMs
                    + ", acquiredTimeMs="
                    + acquiredTimeMs
                    + ", lastRenewTimeMs="
                    + lastRenewTimeMs
                    + '}';
        }
    }

    /**
     * Acquire a column lock for the specified table.
     *
     * <p>Note: Column locks are table-scoped, not bucket-scoped or partition-scoped. The lock
     * acquired applies to all buckets and partitions in the table.
     *
     * <p>This method acquires locks on ALL TabletServers in the cluster in sequential order
     * (ordered by server ID) to ensure global consistency.
     *
     * @param tableId the table ID
     * @param ownerId the unique identifier of the lock owner
     * @param schemaId the schema id that the column indexes belong to
     * @param columnIndexes the column indexes to lock, null means lock all columns
     * @param ttlMs the Time-To-Live for the lock, null to use default
     * @return a CompletableFuture that completes with the LockKey when lock is acquired on all
     *     TabletServers
     */
    public CompletableFuture<LockKey> acquireLock(
            long tableId,
            String ownerId,
            int schemaId,
            @Nullable int[] columnIndexes,
            @Nullable Long ttlMs) {
        if (closed) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException("ClientColumnLockManager has been closed"));
        }

        long effectiveTtl = ttlMs != null ? ttlMs : DEFAULT_LOCK_TTL_MS;
        LockKey lockKey = new LockKey(tableId, columnIndexes);

        // Get sorted list of all alive TabletServers
        List<Integer> sortedServerIds = getSortedServerIds();
        if (sortedServerIds.isEmpty()) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException("No alive TabletServers found in cluster"));
        }

        LOG.debug(
                "Acquiring column lock for table {} on {} TabletServers: {}",
                tableId,
                sortedServerIds.size(),
                sortedServerIds);

        // Build acquire request
        AcquireColumnLockRequest request =
                buildAcquireRequest(tableId, ownerId, schemaId, columnIndexes, effectiveTtl);

        // Acquire locks sequentially with automatic rollback on failure
        return acquireLocksSequentially(sortedServerIds, request)
                .thenApply(
                        acquiredServerIds -> {
                            // Store lock info after successful acquisition
                            LockInfo lockInfo =
                                    new LockInfo(
                                            tableId,
                                            acquiredServerIds,
                                            ownerId,
                                            schemaId,
                                            columnIndexes,
                                            effectiveTtl,
                                            clock.currentTimeMillis());

                            activeLocks.put(lockKey, lockInfo);
                            lockStates.put(lockKey, LockState.ACTIVE);
                            LOG.debug(
                                    "Acquired column lock on all TabletServers: {}, lockKey: {}",
                                    lockInfo,
                                    lockKey);
                            return lockKey;
                        });
    }

    /**
     * Acquire locks on multiple TabletServers sequentially.
     *
     * <p>Locks are acquired one by one in the order of server IDs. If any acquisition fails, all
     * previously acquired locks are automatically rolled back.
     *
     * <p>Thread safety: acquiredServerIds is modified only within thenCompose() callbacks which
     * execute sequentially, ensuring thread-safe access without explicit synchronization.
     *
     * @param sortedServerIds server IDs in sorted order
     * @param request the acquire lock request
     * @return a CompletableFuture that completes with the list of successfully acquired server IDs
     */
    private CompletableFuture<List<Integer>> acquireLocksSequentially(
            List<Integer> sortedServerIds, AcquireColumnLockRequest request) {
        // Track acquired servers for rollback on failure
        // Safe to use ArrayList: modified only in sequential thenCompose callbacks
        List<Integer> acquiredServerIds = new ArrayList<>();

        // Build sequential acquisition chain
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (Integer serverId : sortedServerIds) {
            chain =
                    chain.thenCompose(
                            v -> acquireLockOnServer(serverId, request, acquiredServerIds));
        }

        // Handle failure with rollback
        return handleAcquisitionResult(chain, acquiredServerIds, request);
    }

    /**
     * Handle the result of lock acquisition chain, performing rollback on failure.
     *
     * @param chain the acquisition chain future
     * @param acquiredServerIds list of successfully acquired server IDs
     * @param request the acquire lock request
     * @return a CompletableFuture that completes with the list of acquired server IDs on success,
     *     or fails with the original error after rollback
     */
    private CompletableFuture<List<Integer>> handleAcquisitionResult(
            CompletableFuture<Void> chain,
            List<Integer> acquiredServerIds,
            AcquireColumnLockRequest request) {
        return chain.handle(
                        (result, error) -> {
                            if (error != null) {
                                return rollbackAndFail(acquiredServerIds, request, error);
                            }
                            return CompletableFuture.completedFuture(result);
                        })
                .thenCompose(f -> f)
                .thenApply(v -> acquiredServerIds);
    }

    /**
     * Rollback acquired locks and propagate the original error.
     *
     * @param acquiredServerIds list of server IDs where locks were acquired
     * @param request the acquire lock request
     * @param originalError the original error that triggered rollback
     * @return a CompletableFuture that fails with the original error after rollback completes
     */
    private CompletableFuture<Void> rollbackAndFail(
            List<Integer> acquiredServerIds,
            AcquireColumnLockRequest request,
            Throwable originalError) {
        LOG.warn(
                "Failed to acquire lock on some servers, rolling back {} acquired locks",
                acquiredServerIds.size());
        return rollbackAcquiredLocks(acquiredServerIds, request.getTableId(), request.getOwnerId())
                .thenCompose(
                        v -> {
                            // Re-throw original error after rollback
                            return FutureUtils.completedExceptionally(
                                    unwrapException(originalError));
                        });
    }

    /**
     * Acquire lock on a single TabletServer.
     *
     * @param serverId the server ID
     * @param request the acquire lock request
     * @param acquiredServerIds list to track acquired servers (modified on success)
     * @return a CompletableFuture that completes when lock is acquired
     */
    private CompletableFuture<Void> acquireLockOnServer(
            Integer serverId, AcquireColumnLockRequest request, List<Integer> acquiredServerIds) {
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(serverId);
        if (gateway == null) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(
                            formatLockError(
                                    "TabletServer gateway not found",
                                    request.getTableId(),
                                    request.getOwnerId(),
                                    serverId)));
        }

        return gateway.acquireColumnLock(request)
                .thenApply(
                        response -> {
                            validateResponse(
                                    response.hasErrorCode(),
                                    () -> Errors.forCode(response.getErrorCode()),
                                    () ->
                                            response.hasErrorMessage()
                                                    ? response.getErrorMessage()
                                                    : formatLockError(
                                                            "Failed to acquire column lock",
                                                            request.getTableId(),
                                                            request.getOwnerId(),
                                                            serverId));

                            acquiredServerIds.add(serverId);
                            LOG.debug("Acquired column lock on TabletServer {}", serverId);
                            return null;
                        });
    }

    /**
     * Rollback locks that have been acquired on some servers.
     *
     * <p>This method is called when lock acquisition fails on some servers after successfully
     * acquiring locks on other servers. It attempts to release the locks that were acquired to
     * prevent resource leakage.
     *
     * <p>Locks are released in reverse order (LIFO) to maintain consistency and avoid potential
     * deadlock scenarios. Failed releases are logged but do not fail the overall rollback
     * operation.
     *
     * @param acquiredServerIds list of server IDs where locks were successfully acquired
     * @param tableId the table ID
     * @param ownerId the owner ID of the locks
     * @return a CompletableFuture that completes when all rollback operations finish
     */
    private CompletableFuture<Void> rollbackAcquiredLocks(
            List<Integer> acquiredServerIds, long tableId, String ownerId) {
        if (acquiredServerIds.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        ReleaseColumnLockRequest request = buildReleaseRequest(tableId, ownerId);

        // Release locks in reverse order (LIFO)
        List<Integer> reversedServerIds = new ArrayList<>(acquiredServerIds);
        Collections.reverse(reversedServerIds);

        List<Integer> failedServers = new ArrayList<>();

        // Build sequential release chain in reverse order
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (Integer serverId : reversedServerIds) {
            chain =
                    chain.thenCompose(
                            v ->
                                    releaseLockOnServer(serverId, request)
                                            .handle(
                                                    (result, error) -> {
                                                        if (error != null) {
                                                            failedServers.add(serverId);
                                                            LOG.warn(
                                                                    "Failed to release lock on server {} during rollback: {}",
                                                                    serverId,
                                                                    error.getMessage());
                                                        } else {
                                                            LOG.debug(
                                                                    "Released column lock on TabletServer {} during rollback",
                                                                    serverId);
                                                        }
                                                        // Always succeed to allow other releases to
                                                        // continue
                                                        return null;
                                                    }));
        }

        // Wait for all releases and log summary
        return chain.thenRun(
                () -> {
                    if (!failedServers.isEmpty()) {
                        LOG.warn(
                                "Rollback completed but {} servers failed to release locks: {}",
                                failedServers.size(),
                                failedServers);
                    } else {
                        LOG.debug(
                                "Successfully rolled back locks on all {} servers",
                                acquiredServerIds.size());
                    }
                });
    }

    /**
     * Release a column lock identified by the lock key.
     *
     * <p>Note: Column locks are table-scoped. This will release the lock on all TabletServers for
     * the specified columns.
     *
     * <p>Locks are released in reverse order (LIFO) of acquisition to maintain consistency and
     * avoid potential deadlock scenarios.
     *
     * @param lockKey the lock key obtained from acquireLock
     * @param ownerId the owner ID of the lock
     * @return a CompletableFuture that completes when the lock is released on all TabletServers
     */
    public CompletableFuture<Void> releaseLock(LockKey lockKey, String ownerId) {
        LockInfo lockInfo = activeLocks.remove(lockKey);
        lockStates.remove(lockKey);

        if (lockInfo == null) {
            LOG.warn("Attempted to release non-existent lock for lockKey: {}", lockKey);
            return CompletableFuture.completedFuture(null);
        }

        if (!lockInfo.ownerId.equals(ownerId)) {
            LOG.warn(
                    "Attempted to release lock with mismatched owner ID. Expected: {}, Got: {}",
                    lockInfo.ownerId,
                    ownerId);
            return FutureUtils.completedExceptionally(
                    new IllegalArgumentException(
                            formatLockError(
                                    "Lock owner ID mismatch",
                                    lockInfo.getTableId(),
                                    ownerId,
                                    null)));
        }

        ReleaseColumnLockRequest request = buildReleaseRequest(lockInfo.getTableId(), ownerId);

        // Release locks in reverse order (LIFO) of acquisition
        List<Integer> reversedServerIds = new ArrayList<>(lockInfo.getLockedServerIds());
        Collections.reverse(reversedServerIds);

        // Build sequential release chain in reverse order
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (Integer serverId : reversedServerIds) {
            chain =
                    chain.thenCompose(
                            v ->
                                    releaseLockOnServer(serverId, request)
                                            .handle(
                                                    (result, error) -> {
                                                        if (error != null) {
                                                            LOG.warn(
                                                                    "Failed to release lock on server {}: {}",
                                                                    serverId,
                                                                    error.getMessage());
                                                        } else {
                                                            LOG.debug(
                                                                    "Released column lock on TabletServer {}",
                                                                    serverId);
                                                        }
                                                        return null;
                                                    }));
        }

        // Complete after all releases
        return chain.thenApply(
                v -> {
                    LOG.info("Released column lock on all TabletServers: {}", lockInfo);
                    return null;
                });
    }

    /**
     * Release lock on a single TabletServer.
     *
     * @param serverId the server ID
     * @param request the release lock request
     * @return a CompletableFuture that completes when lock is released
     */
    private CompletableFuture<Void> releaseLockOnServer(
            Integer serverId, ReleaseColumnLockRequest request) {
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(serverId);
        if (gateway == null) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(
                            formatLockError(
                                    "TabletServer gateway not found during release",
                                    request.getTableId(),
                                    request.getOwnerId(),
                                    serverId)));
        }

        return gateway.releaseColumnLock(request).thenApply(response -> null);
    }

    /**
     * Renew all active locks that need renewal.
     *
     * <p>This method is called periodically by the background renewal thread.
     *
     * <p>If renewal fails (e.g., due to TabletServer restart), it will automatically attempt to
     * re-acquire the lock with the same parameters. This ensures lock continuity even when
     * TabletServers lose in-memory lock state.
     *
     * <p>Thread safety: This method collects all renewal futures and waits for completion before
     * removing failed locks. This prevents the race condition where async callbacks execute after
     * the removal operation, which could cause memory leaks.
     */
    private void renewLocks() {
        if (closed || activeLocks.isEmpty()) {
            return;
        }

        try {
            long currentTime = clock.currentTimeMillis();

            // Collect all renewal futures to wait for completion
            List<CompletableFuture<RenewalResult>> renewalFutures = new ArrayList<>();
            List<LockKey> processedKeys = new ArrayList<>();

            for (Map.Entry<LockKey, LockInfo> entry : activeLocks.entrySet()) {
                LockKey lockKey = entry.getKey();
                LockInfo lockInfo = entry.getValue();

                try {
                    if (lockInfo.needsRenewal(currentTime)) {
                        // Submit renewal and track the future
                        CompletableFuture<RenewalResult> renewalFuture =
                                renewOrReacquireLock(lockInfo)
                                        .thenApply(v -> new RenewalResult(lockKey, true, null))
                                        .exceptionally(
                                                error -> new RenewalResult(lockKey, false, error));
                        renewalFutures.add(renewalFuture);
                        processedKeys.add(lockKey);
                    } else if (lockInfo.isExpired(currentTime)) {
                        LOG.warn(
                                "Lock has expired for table {}, owner: {}. Lock will be removed from active locks.",
                                lockInfo.getTableId(),
                                lockInfo.ownerId);
                        // Immediately remove expired locks
                        activeLocks.remove(lockKey);
                        lockStates.remove(lockKey);
                    }
                } catch (Exception e) {
                    LOG.error(
                            "Unexpected error while processing lock renewal for table {}, owner: {}",
                            lockInfo.getTableId(),
                            lockInfo.ownerId,
                            e);
                    // Remove lock on unexpected error
                    activeLocks.remove(lockKey);
                    lockStates.remove(lockKey);
                }
            }

            // Wait for all renewals to complete before processing results
            if (!renewalFutures.isEmpty()) {
                CompletableFuture.allOf(renewalFutures.toArray(new CompletableFuture[0])).join();

                // Process results after all futures complete
                for (CompletableFuture<RenewalResult> future : renewalFutures) {
                    try {
                        RenewalResult result = future.get();
                        if (!result.success) {
                            LockInfo lockInfo = activeLocks.get(result.lockKey);
                            if (lockInfo != null) {
                                LOG.error(
                                        "Failed to renew or re-acquire lock for table {}, owner: {}. "
                                                + "Lock will be removed from active locks. Error: {}",
                                        lockInfo.getTableId(),
                                        lockInfo.ownerId,
                                        result.error != null
                                                ? result.error.getMessage()
                                                : "unknown");
                            }
                            activeLocks.remove(result.lockKey);
                            lockStates.remove(result.lockKey);
                        } else {
                            LockInfo lockInfo = activeLocks.get(result.lockKey);
                            if (lockInfo != null) {
                                LOG.debug(
                                        "Successfully maintained lock for table {}, owner: {}",
                                        lockInfo.getTableId(),
                                        lockInfo.ownerId);
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error processing renewal result", e);
                    }
                }
            }
        } catch (Exception e) {
            // Catch-all to prevent the renewal thread from dying
            LOG.error("Unexpected error in renewLocks()", e);
        }
    }

    /** Result of a lock renewal operation. */
    private static class RenewalResult {
        final LockKey lockKey;
        final boolean success;
        final Throwable error;

        RenewalResult(LockKey lockKey, boolean success, Throwable error) {
            this.lockKey = lockKey;
            this.success = success;
            this.error = error;
        }
    }

    /**
     * Renew or re-acquire a specific lock.
     *
     * <p>This method first attempts to renew the lock on all TabletServers. If renewal fails on any
     * TabletServer (e.g., due to server restart causing loss of in-memory lock state), it will
     * automatically attempt to re-acquire the lock using the same parameters.
     *
     * <p>Re-acquiring the lock is safe because:
     *
     * <ul>
     *   <li>Server-side acquireLock() supports re-entry for the same owner with matching
     *       schema/columns
     *   <li>For restarted servers with empty memory, re-acquisition establishes new lock state
     *   <li>For running servers, re-acquisition updates the existing lock's TTL (equivalent to
     *       renewal)
     * </ul>
     *
     * @param lockInfo the lock info to renew
     * @return a CompletableFuture that completes successfully when lock is maintained (either via
     *     renewal or re-acquisition), or fails if both attempts fail
     */
    private CompletableFuture<Void> renewOrReacquireLock(LockInfo lockInfo) {
        // Try renewal first, use handle() + thenCompose() for Java 8 compatibility
        CompletableFuture<Void> renewalFuture = renewLock(lockInfo);

        return renewalFuture
                .handle(
                        (Void result, Throwable renewError) -> {
                            if (renewError == null) {
                                // Renewal succeeded, return completed future
                                return CompletableFuture.<Void>completedFuture(null);
                            } else {
                                // Renewal failed, try to re-acquire
                                LOG.warn(
                                        "Lock renewal failed for table {}, owner: {}. Attempting to re-acquire lock. Error: {}",
                                        lockInfo.getTableId(),
                                        lockInfo.ownerId,
                                        renewError.getMessage());

                                // This is safe: server-side allows same owner to re-acquire with
                                // matching parameters
                                return reacquireLock(lockInfo)
                                        .thenApply(lockKey -> (Void) null)
                                        .whenComplete(
                                                (reacquireResult, reacquireError) -> {
                                                    if (reacquireError == null) {
                                                        LOG.info(
                                                                "Successfully re-acquired lock after renewal failure for table {}, owner: {}",
                                                                lockInfo.getTableId(),
                                                                lockInfo.ownerId);
                                                    } else {
                                                        LOG.error(
                                                                "Failed to re-acquire lock for table {}, owner: {}. Both renewal and re-acquisition failed.",
                                                                lockInfo.getTableId(),
                                                                lockInfo.ownerId,
                                                                reacquireError);
                                                    }
                                                });
                            }
                        })
                .thenCompose(f -> f);
    }

    /**
     * Renew a specific lock.
     *
     * <p>This method renews the lock on all TabletServers that were locked. If renewal fails on any
     * TabletServer, the entire operation is considered failed (triggering re-acquisition logic).
     *
     * @param lockInfo the lock info to renew
     * @return a CompletableFuture that completes when the lock is renewed on all TabletServers, or
     *     fails if any TabletServer renewal fails
     */
    private CompletableFuture<Void> renewLock(LockInfo lockInfo) {
        RenewColumnLockRequest request = buildRenewRequest(lockInfo.getTableId(), lockInfo.ownerId);

        // Renew locks on all TabletServers in parallel
        List<CompletableFuture<Void>> renewFutures = new ArrayList<>();
        for (Integer serverId : lockInfo.getLockedServerIds()) {
            renewFutures.add(renewLockOnServer(serverId, request));
        }

        // Wait for all renewals - if any fails, the entire operation fails
        return CompletableFuture.allOf(renewFutures.toArray(new CompletableFuture[0]))
                .thenApply(
                        v -> {
                            lockInfo.updateRenewTime(clock.currentTimeMillis());
                            LOG.debug(
                                    "Renewed column lock on all TabletServers for table {}, owner: {}",
                                    lockInfo.getTableId(),
                                    lockInfo.ownerId);
                            return null;
                        });
    }

    /**
     * Renew lock on a single TabletServer.
     *
     * @param serverId the server ID
     * @param request the renew lock request
     * @return a CompletableFuture that completes when lock is renewed
     */
    private CompletableFuture<Void> renewLockOnServer(
            Integer serverId, RenewColumnLockRequest request) {
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(serverId);
        if (gateway == null) {
            LOG.warn("TabletServer gateway not found for server {} during renewal", serverId);
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(
                            formatLockError(
                                    "TabletServer gateway not found during renewal",
                                    request.getTableId(),
                                    request.getOwnerId(),
                                    serverId)));
        }

        return gateway.renewColumnLock(request)
                .thenApply(
                        response -> {
                            validateResponse(
                                    response.hasErrorCode(),
                                    () -> Errors.forCode(response.getErrorCode()),
                                    () ->
                                            response.hasErrorMessage()
                                                    ? response.getErrorMessage()
                                                    : formatLockError(
                                                            "Failed to renew lock",
                                                            request.getTableId(),
                                                            request.getOwnerId(),
                                                            serverId));

                            LOG.debug("Renewed column lock on TabletServer {}", serverId);
                            return null;
                        });
    }

    /**
     * Re-acquire an existing lock using the same parameters.
     *
     * <p>This method is called when lock renewal fails (e.g., due to TabletServer restart). It
     * attempts to re-acquire the lock with the same owner, schema, columns, and TTL.
     *
     * <p>Server-side behavior:
     *
     * <ul>
     *   <li>If the lock already exists for this owner with matching parameters, updates TTL
     *       (equivalent to renewal)
     *   <li>If the lock doesn't exist (e.g., after server restart), creates a new lock
     *   <li>If there's a conflict with another owner, rejects the request
     * </ul>
     *
     * <p>Implementation: Uses a REACQUIRING state flag to prevent concurrent operations on the same
     * lock during re-acquisition. The state machine transitions are:
     *
     * <ul>
     *   <li>ACTIVE -> REACQUIRING: Mark lock as being re-acquired
     *   <li>REACQUIRING -> ACTIVE: On success, mark lock as active again
     *   <li>REACQUIRING -> removed: On failure, remove the lock
     * </ul>
     *
     * <p>Thread safety: This method is called only from the single-threaded renewal executor. The
     * state flag prevents race conditions with user-initiated releaseLock() calls.
     *
     * @param lockInfo the lock info containing parameters for re-acquisition
     * @return a CompletableFuture that completes with the LockKey when lock is successfully
     *     re-acquired
     */
    private CompletableFuture<LockKey> reacquireLock(LockInfo lockInfo) {
        LOG.info(
                "Re-acquiring lock for table {}, owner: {}, schema: {}, columns: {}",
                lockInfo.getTableId(),
                lockInfo.ownerId,
                lockInfo.schemaId,
                lockInfo.columnIndexes == null ? "all" : lockInfo.columnIndexes.length);

        // Create lock key
        LockKey lockKey = new LockKey(lockInfo.getTableId(), lockInfo.columnIndexes);

        // Mark lock as being re-acquired to prevent concurrent operations
        lockStates.put(lockKey, LockState.REACQUIRING);

        // Get sorted list of all alive TabletServers
        List<Integer> sortedServerIds = getSortedServerIds();
        if (sortedServerIds.isEmpty()) {
            lockStates.put(lockKey, LockState.ACTIVE);
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(
                            formatLockError(
                                    "No alive TabletServers found in cluster during re-acquisition",
                                    lockInfo.getTableId(),
                                    lockInfo.ownerId,
                                    null)));
        }

        // Build acquire request
        AcquireColumnLockRequest request =
                buildAcquireRequest(
                        lockInfo.getTableId(),
                        lockInfo.ownerId,
                        lockInfo.schemaId,
                        lockInfo.columnIndexes,
                        lockInfo.ttlMs);

        // Re-acquire locks sequentially with automatic rollback on failure
        return acquireLocksSequentially(sortedServerIds, request)
                .thenApply(
                        acquiredServerIds -> {
                            // Update lock info with new server list and renewal time
                            lockInfo.getLockedServerIds().clear();
                            lockInfo.getLockedServerIds().addAll(acquiredServerIds);
                            lockInfo.updateRenewTime(clock.currentTimeMillis());

                            // Mark lock as active again
                            lockStates.put(lockKey, LockState.ACTIVE);

                            LOG.info(
                                    "Successfully re-acquired lock on {} TabletServers for table {}, owner: {}",
                                    acquiredServerIds.size(),
                                    lockInfo.getTableId(),
                                    lockInfo.ownerId);
                            return lockKey;
                        })
                .exceptionally(
                        error -> {
                            // On failure, restore to ACTIVE state to allow further renewal attempts
                            // The lock will be removed by renewLocks() after this future completes
                            lockStates.put(lockKey, LockState.ACTIVE);
                            LOG.error(
                                    "Failed to re-acquire lock for table {}, owner: {}",
                                    lockInfo.getTableId(),
                                    lockInfo.ownerId,
                                    error);
                            throw unwrapException(error);
                        });
    }

    // ==================== Helper Methods ====================

    /**
     * Get sorted list of all alive TabletServer IDs in the cluster.
     *
     * @return sorted list of server IDs
     */
    private List<Integer> getSortedServerIds() {
        Map<Integer, ServerNode> allTabletServers =
                metadataUpdater.getCluster().getAliveTabletServers();
        List<Integer> serverIds = new ArrayList<>(allTabletServers.keySet());
        Collections.sort(serverIds);
        return serverIds;
    }

    /**
     * Build an acquire lock request.
     *
     * @param tableId the table ID
     * @param ownerId the owner ID
     * @param schemaId the schema ID
     * @param columnIndexes the column indexes (null for all columns)
     * @param ttlMs the TTL in milliseconds
     * @return the acquire lock request
     */
    private AcquireColumnLockRequest buildAcquireRequest(
            long tableId, String ownerId, int schemaId, @Nullable int[] columnIndexes, long ttlMs) {
        AcquireColumnLockRequest request = new AcquireColumnLockRequest();
        request.setTableId(tableId);
        request.setOwnerId(ownerId);
        request.setSchemaId(schemaId);
        request.setTtlMs(ttlMs);

        if (columnIndexes != null) {
            for (int columnIndex : columnIndexes) {
                request.addColumnIndexe(columnIndex);
            }
        }

        return request;
    }

    /**
     * Build a release lock request.
     *
     * @param tableId the table ID
     * @param ownerId the owner ID
     * @return the release lock request
     */
    private ReleaseColumnLockRequest buildReleaseRequest(long tableId, String ownerId) {
        ReleaseColumnLockRequest request = new ReleaseColumnLockRequest();
        request.setTableId(tableId);
        request.setOwnerId(ownerId);
        return request;
    }

    /**
     * Build a renew lock request.
     *
     * @param tableId the table ID
     * @param ownerId the owner ID
     * @return the renew lock request
     */
    private RenewColumnLockRequest buildRenewRequest(long tableId, String ownerId) {
        RenewColumnLockRequest request = new RenewColumnLockRequest();
        request.setTableId(tableId);
        request.setOwnerId(ownerId);
        return request;
    }

    /**
     * Validate RPC response and throw exception if error is present.
     *
     * @param hasError whether the response has an error code
     * @param errorSupplier supplier of the error
     * @param messageSupplier supplier of the error message
     */
    private void validateResponse(
            boolean hasError,
            java.util.function.Supplier<Errors> errorSupplier,
            java.util.function.Supplier<String> messageSupplier) {
        if (hasError) {
            Errors error = errorSupplier.get();
            String errorMessage = messageSupplier.get();
            throw error.exception(errorMessage);
        }
    }

    /**
     * Unwrap exception from CompletableFuture error.
     *
     * @param error the throwable from CompletableFuture
     * @return unwrapped RuntimeException
     */
    private RuntimeException unwrapException(Throwable error) {
        if (error instanceof RuntimeException) {
            return (RuntimeException) error;
        }
        return new RuntimeException(error);
    }

    /**
     * Format a standardized lock error message with context information.
     *
     * @param operation the operation that failed
     * @param tableId the table ID
     * @param ownerId the owner ID
     * @param serverId the server ID (null if not applicable)
     * @return formatted error message
     */
    private String formatLockError(
            String operation, long tableId, String ownerId, @Nullable Integer serverId) {
        StringBuilder sb = new StringBuilder();
        sb.append(operation);
        sb.append(" [tableId=").append(tableId);
        sb.append(", ownerId=").append(ownerId);
        if (serverId != null) {
            sb.append(", serverId=").append(serverId);
        }
        sb.append("]");
        return sb.toString();
    }

    // ==================== Monitoring and Testing Methods ====================
    /**
     * Get an unmodifiable view of active locks. Visible for testing.
     *
     * @return unmodifiable map of active locks
     */
    @VisibleForTesting
    Map<LockKey, LockInfo> getActiveLocks() {
        return Collections.unmodifiableMap(activeLocks);
    }

    // ==================== Lifecycle Methods ====================

    /**
     * Close the lock manager and release all locks.
     *
     * <p>Thread safety: This method first stops the renewal thread to prevent concurrent
     * modifications, then releases all locks. The closed flag is set at the beginning to reject new
     * lock acquisition requests.
     */
    public void close() {
        if (closed) {
            return;
        }

        // Set closed flag first to prevent new operations
        closed = true;

        // Shutdown the renewal executor and wait for current renewal to complete
        // This ensures no concurrent modifications to activeLocks during release
        renewalExecutor.shutdown();
        try {
            if (!renewalExecutor.awaitTermination(
                    EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                LOG.warn(
                        "Renewal executor did not terminate within {} seconds, forcing shutdown",
                        EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
                renewalExecutor.shutdownNow();
                // Wait a bit more after forced shutdown
                if (!renewalExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                    LOG.error("Renewal executor did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for renewal executor to terminate");
            renewalExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Now it's safe to release all locks - renewal thread has stopped
        // Create a snapshot to avoid concurrent modification issues
        List<Map.Entry<LockKey, LockInfo>> locksToRelease = new ArrayList<>(activeLocks.entrySet());

        if (locksToRelease.isEmpty()) {
            LOG.info("ClientColumnLockManager closed (no active locks)");
            return;
        }

        LOG.info("Releasing {} active locks during close", locksToRelease.size());

        // Release all active locks, each lock's servers in reverse order
        List<CompletableFuture<Void>> releaseFutures = new ArrayList<>();
        for (Map.Entry<LockKey, LockInfo> entry : locksToRelease) {
            LockKey lockKey = entry.getKey();
            LockInfo lockInfo = entry.getValue();

            // Build release request directly to avoid removing from activeLocks prematurely
            ReleaseColumnLockRequest request =
                    buildReleaseRequest(lockInfo.getTableId(), lockInfo.ownerId);

            // Release locks in reverse order (LIFO) for each lock
            List<Integer> reversedServerIds = new ArrayList<>(lockInfo.getLockedServerIds());
            Collections.reverse(reversedServerIds);

            // Build sequential release chain in reverse order
            CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
            for (Integer serverId : reversedServerIds) {
                chain =
                        chain.thenCompose(
                                v ->
                                        releaseLockOnServer(serverId, request)
                                                .handle(
                                                        (result, error) -> {
                                                            if (error != null) {
                                                                LOG.warn(
                                                                        "Failed to release lock on server {} during close: {}",
                                                                        serverId,
                                                                        error.getMessage());
                                                            } else {
                                                                LOG.debug(
                                                                        "Released lock on server {} during close",
                                                                        serverId);
                                                            }
                                                            return null;
                                                        }));
            }

            CompletableFuture<Void> lockReleaseFuture =
                    chain.exceptionally(
                            e -> {
                                LOG.warn("Failed to release lock during close: {}", lockInfo, e);
                                return null;
                            });
            releaseFutures.add(lockReleaseFuture);
        }

        // Wait for all release operations to complete with timeout
        try {
            CompletableFuture.allOf(releaseFutures.toArray(new CompletableFuture[0]))
                    .get(CLOSE_RELEASE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LOG.info("Successfully released all locks during close");
        } catch (Exception e) {
            LOG.warn("Some locks failed to release within timeout during close", e);
        }

        // Clear all state
        activeLocks.clear();
        lockStates.clear();
        LOG.info("ClientColumnLockManager closed");
    }
}

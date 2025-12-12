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

package org.apache.fluss.server.lock;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Server-level column lock service.
 *
 * <p>This service manages column locks at the table level. All buckets and partitions in the same
 * table share the same lock resources.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Table-level locks: Lock granularity is at table level for both partitioned and
 *       non-partitioned tables
 *   <li>Batch lock acquisition: Validates leadership for all buckets
 *   <li>Automatic conflict detection: Same schema + overlapping columns
 *   <li>TTL-based expiration: Locks expire automatically
 * </ul>
 */
@ThreadSafe
@Internal
public class ServerColumnLockService {
    private static final Logger LOG = LoggerFactory.getLogger(ServerColumnLockService.class);

    private final int serverId;
    // Lock resources: TableId -> owner locks
    // Note: For both partitioned and non-partitioned tables, locks are table-scoped
    private final Map<Long, Map<String, ColumnLockInfo>> lockResources;
    // Fine-grained locks: one lock object per table to avoid global synchronization bottleneck
    private final Map<Long, Object> tableLocks;
    private volatile boolean closed = false;

    public ServerColumnLockService(ReplicaManager replicaManager) {
        this.serverId = replicaManager.getServerId();
        this.lockResources = MapUtils.newConcurrentHashMap();
        this.tableLocks = MapUtils.newConcurrentHashMap();
    }

    /**
     * Represents a lock request at table level.
     *
     * <p>The lock applies to all partitions and buckets in the table.
     */
    public static class TableLockRequest {
        private final long tableId;
        private final String ownerId;
        private final int schemaId;
        private final @Nullable int[] columnIndexes;
        private final long ttlMs;

        public TableLockRequest(
                long tableId,
                String ownerId,
                int schemaId,
                @Nullable int[] columnIndexes,
                long ttlMs) {
            this.tableId = tableId;
            this.ownerId = ownerId;
            this.schemaId = schemaId;
            this.columnIndexes = columnIndexes;
            this.ttlMs = ttlMs;
        }

        public long getTableId() {
            return tableId;
        }

        public String getOwnerId() {
            return ownerId;
        }

        public int getSchemaId() {
            return schemaId;
        }

        @Nullable
        public int[] getColumnIndexes() {
            return columnIndexes;
        }

        public long getTtlMs() {
            return ttlMs;
        }
    }

    /**
     * Represents the result of a lock request at table level.
     *
     * <p>The lock applies to all partitions and buckets in the table.
     */
    public static class TableLockResult {
        private final long tableId;
        private final long acquiredTime; // -1 if failed
        private final @Nullable String errorMessage;

        private TableLockResult(long tableId, long acquiredTime, @Nullable String errorMessage) {
            this.tableId = tableId;
            this.acquiredTime = acquiredTime;
            this.errorMessage = errorMessage;
        }

        public static TableLockResult success(long tableId, long acquiredTime) {
            return new TableLockResult(tableId, acquiredTime, null);
        }

        public static TableLockResult failure(long tableId, String errorMessage) {
            return new TableLockResult(tableId, -1, errorMessage);
        }

        public long getTableId() {
            return tableId;
        }

        public long getAcquiredTime() {
            return acquiredTime;
        }

        public boolean isSuccess() {
            return acquiredTime >= 0;
        }

        @Nullable
        public String getErrorMessage() {
            return errorMessage;
        }
    }

    /**
     * Attempt to acquire column lock at table level.
     *
     * @param request lock request for table
     * @return result of the lock acquisition
     */
    public TableLockResult acquireLock(TableLockRequest request) {
        if (closed) {
            LOG.warn("ServerColumnLockService is closed, rejecting lock acquisition");
            return TableLockResult.failure(request.getTableId(), "Service is closed");
        }

        long tableId = request.getTableId();
        long acquiredTime =
                acquireLock(
                        tableId,
                        request.getSchemaId(),
                        request.getColumnIndexes(),
                        request.getOwnerId(),
                        request.getTtlMs());

        if (acquiredTime < 0) {
            String errorMsg =
                    String.format(
                            "Column lock conflict: Failed to acquire lock for table %d", tableId);
            LOG.warn(errorMsg);
            return TableLockResult.failure(tableId, errorMsg);
        } else {
            LOG.info("Successfully acquired lock for table {}", tableId);
            return TableLockResult.success(tableId, acquiredTime);
        }
    }

    /**
     * Release column lock at table level.
     *
     * @param tableId the table ID to release lock for
     * @param ownerId the owner ID of the lock
     * @return result of the lock release
     */
    public TableLockResult releaseLockForTable(long tableId, String ownerId) {
        releaseLock(tableId, ownerId);
        long timestamp = System.currentTimeMillis();
        LOG.info("Released lock for table {}, owner: {}", tableId, ownerId);
        return TableLockResult.success(tableId, timestamp);
    }

    /**
     * Renew a column lock at table level.
     *
     * @param tableId the table ID
     * @param ownerId the lock owner ID
     * @return result of the lock renewal
     */
    public TableLockResult renewLockForTable(long tableId, String ownerId) {
        if (closed) {
            LOG.warn("ServerColumnLockService is closed, rejecting lock renewal");
            return TableLockResult.failure(tableId, "Service is closed");
        }

        long renewedTime = renewLock(tableId, ownerId);
        if (renewedTime < 0) {
            return TableLockResult.failure(tableId, "Lock not found or expired");
        }
        return TableLockResult.success(tableId, renewedTime);
    }

    /**
     * Validate if the owner has permission to write to the specified columns.
     *
     * @param tableBucket the table bucket being written to (used for leadership validation)
     * @param ownerId the lock owner ID to validate
     * @param schemaId the schema ID of the write operation
     * @param targetColumns the columns to be written, null means all columns
     * @return true if the owner has permission to write, false otherwise
     */
    public boolean validateWritePermission(
            TableBucket tableBucket,
            @Nullable String ownerId,
            int schemaId,
            @Nullable int[] targetColumns) {
        long tableId = tableBucket.getTableId();
        return validateWritePermission(tableId, ownerId, schemaId, targetColumns);
    }

    /**
     * Check if any active lock exists for a table/partition.
     *
     * @param tableBucket the table bucket to check
     * @return true if there is at least one active lock, false otherwise
     */
    public boolean hasActiveLocks(TableBucket tableBucket) {
        long tableId = tableBucket.getTableId();
        Map<String, ColumnLockInfo> locks = lockResources.get(tableId);
        return locks != null && !locks.isEmpty();
    }

    // ================== Internal Lock Management Methods ==================

    private long acquireLock(
            long tableId, int schemaId, @Nullable int[] columnIndexes, String ownerId, long ttlMs) {
        // Use fine-grained lock per table to avoid global synchronization bottleneck
        Object tableLock = tableLocks.computeIfAbsent(tableId, k -> new Object());
        synchronized (tableLock) {
            return acquireLockInternal(tableId, schemaId, columnIndexes, ownerId, ttlMs);
        }
    }

    private long acquireLockInternal(
            long tableId, int schemaId, @Nullable int[] columnIndexes, String ownerId, long ttlMs) {
        long currentTime = System.currentTimeMillis();

        // Get or create lock map for this table
        Map<String, ColumnLockInfo> locks =
                lockResources.computeIfAbsent(tableId, k -> MapUtils.newConcurrentHashMap());

        // Check for existing lock by the same owner
        ColumnLockInfo existingLock = locks.get(ownerId);
        if (existingLock != null) {
            if (existingLock.isExpired(currentTime)) {
                LOG.info(
                        "[Server-{}] Removing expired lock for table {}, owner: {}",
                        serverId,
                        tableId,
                        ownerId);
                locks.remove(ownerId);
            } else {
                // Same owner - check if schema and columns match
                if (existingLock.getSchemaId() != schemaId
                        || !existingLock.hasSameColumns(columnIndexes)) {
                    LOG.warn(
                            "[Server-{}] Lock re-acquisition conflict for table {}, owner: {}. Existing lock (schema={}, columns={}), requested (schema={}, columns={})",
                            serverId,
                            tableId,
                            ownerId,
                            existingLock.getSchemaId(),
                            existingLock.getColumnIndexesString(),
                            schemaId,
                            columnIndexes == null
                                    ? "all"
                                    : java.util.Arrays.toString(columnIndexes));
                    return -1;
                }
                // Same owner with same schema and columns, allow re-acquisition (renewal)
                existingLock.updateExpiration(currentTime + ttlMs);
                LOG.debug(
                        "[Server-{}] Re-acquired lock for table {}, owner: {}",
                        serverId,
                        tableId,
                        ownerId);
                return currentTime;
            }
        }

        // Check for conflicts with other owners
        Iterator<Map.Entry<String, ColumnLockInfo>> iterator = locks.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ColumnLockInfo> entry = iterator.next();
            if (!entry.getKey().equals(ownerId)) {
                ColumnLockInfo otherLock = entry.getValue();
                if (otherLock.isExpired(currentTime)) {
                    iterator.remove();
                    LOG.info(
                            "[Server-{}] Removing expired lock for table {}, owner: {}",
                            serverId,
                            tableId,
                            entry.getKey());
                } else if (otherLock.hasConflict(schemaId, columnIndexes)) {
                    LOG.warn(
                            "[Server-{}] Lock conflict for table {}, existing owner: {} (schema={}, columns={}), requested owner: {} (schema={}, columns={})",
                            serverId,
                            tableId,
                            entry.getKey(),
                            otherLock.getSchemaId(),
                            otherLock.getColumnIndexesString(),
                            ownerId,
                            schemaId,
                            columnIndexes == null
                                    ? "all"
                                    : java.util.Arrays.toString(columnIndexes));
                    return -1;
                }
            }
        }

        // Acquire new lock
        ColumnLockInfo newLock =
                new ColumnLockInfo(ownerId, schemaId, columnIndexes, ttlMs, currentTime);
        locks.put(ownerId, newLock);
        LOG.info(
                "[Server-{}] Acquired lock for table {}, owner: {} (schema={}, columns={})",
                serverId,
                tableId,
                ownerId,
                schemaId,
                columnIndexes == null ? "all" : java.util.Arrays.toString(columnIndexes));
        return currentTime;
    }

    private boolean releaseLock(long tableId, String ownerId) {
        Object tableLock = tableLocks.computeIfAbsent(tableId, k -> new Object());
        synchronized (tableLock) {
            return releaseLockInternal(tableId, ownerId);
        }
    }

    private boolean releaseLockInternal(long tableId, String ownerId) {
        Map<String, ColumnLockInfo> locks = lockResources.get(tableId);
        if (locks == null) {
            return true;
        }

        ColumnLockInfo lock = locks.remove(ownerId);
        if (locks.isEmpty()) {
            lockResources.remove(tableId);
        }

        if (lock != null) {
            LOG.info(
                    "[Server-{}] Released lock for table {}, owner: {}",
                    serverId,
                    tableId,
                    ownerId);
        }
        return true;
    }

    private long renewLock(long tableId, String ownerId) {
        if (closed) {
            return -1;
        }

        Object tableLock = tableLocks.computeIfAbsent(tableId, k -> new Object());
        synchronized (tableLock) {
            return renewLockInternal(tableId, ownerId);
        }
    }

    private long renewLockInternal(long tableId, String ownerId) {
        Map<String, ColumnLockInfo> locks = lockResources.get(tableId);
        if (locks == null) {
            LOG.warn(
                    "[Server-{}] Lock resource not found for table {}, owner: {}",
                    serverId,
                    tableId,
                    ownerId);
            return -1;
        }

        ColumnLockInfo lock = locks.get(ownerId);
        if (lock == null) {
            LOG.warn(
                    "[Server-{}] Lock not found for table {}, owner: {}",
                    serverId,
                    tableId,
                    ownerId);
            return -1;
        }

        long currentTime = System.currentTimeMillis();
        if (lock.isExpired(currentTime)) {
            LOG.warn(
                    "[Server-{}] Lock expired for table {}, owner: {}", serverId, tableId, ownerId);
            locks.remove(ownerId);
            if (locks.isEmpty()) {
                lockResources.remove(tableId);
            }
            return -1;
        }

        lock.updateExpiration(currentTime + lock.getTtlMs());
        LOG.debug("[Server-{}] Renewed lock for table {}, owner: {}", serverId, tableId, ownerId);
        return currentTime;
    }

    private boolean validateWritePermission(
            long tableId, @Nullable String ownerId, int schemaId, @Nullable int[] targetColumns) {
        Object tableLock = tableLocks.computeIfAbsent(tableId, k -> new Object());
        synchronized (tableLock) {
            return validateWritePermissionInternal(tableId, ownerId, schemaId, targetColumns);
        }
    }

    private boolean validateWritePermissionInternal(
            long tableId, @Nullable String ownerId, int schemaId, @Nullable int[] targetColumns) {
        Map<String, ColumnLockInfo> locks = lockResources.get(tableId);

        // No locks exist - allow all writes
        if (locks == null || locks.isEmpty()) {
            return true;
        }

        // Locks exist but no ownerId provided
        if (ownerId == null) {
            LOG.warn(
                    "[Server-{}] Write validation failed: No lock owner provided but {} has active locks",
                    serverId,
                    tableId);
            return false;
        }

        ColumnLockInfo lock = locks.get(ownerId);
        if (lock == null) {
            LOG.warn(
                    "[Server-{}] Write validation failed: No lock found for {}, owner: {}",
                    serverId,
                    tableId,
                    ownerId);
            return false;
        }

        long currentTime = System.currentTimeMillis();
        if (lock.isExpired(currentTime)) {
            LOG.warn(
                    "[Server-{}] Write validation failed: Lock expired for {}, owner: {}",
                    serverId,
                    tableId,
                    ownerId);
            locks.remove(ownerId);
            if (locks.isEmpty()) {
                lockResources.remove(tableId);
            }
            return false;
        }

        if (!lock.coversColumns(schemaId, targetColumns)) {
            LOG.warn(
                    "[Server-{}] Write validation failed: Lock for owner {} does not cover target columns {} (lock schema: {}, target schema: {})",
                    serverId,
                    ownerId,
                    targetColumns == null ? "all" : java.util.Arrays.toString(targetColumns),
                    lock.getSchemaId(),
                    schemaId);
            return false;
        }

        return true;
    }

    /** Close the lock service. */
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;
        int totalLocks = 0;
        for (Map<String, ColumnLockInfo> locks : lockResources.values()) {
            totalLocks += locks.size();
        }
        lockResources.clear();
        LOG.info(
                "[Server-{}] ServerColumnLockService closed, cleared {} locks",
                serverId,
                totalLocks);
    }

    // ================== Internal ColumnLockInfo Class ==================

    /** Information about a column lock held by an owner. */
    private static class ColumnLockInfo {
        private final String ownerId;
        private final int schemaId;
        private final @Nullable Set<Integer> columnIndexes; // null means all columns
        private final long ttlMs;
        private volatile long expirationTime;

        ColumnLockInfo(
                String ownerId,
                int schemaId,
                @Nullable int[] columnIndexes,
                long ttlMs,
                long acquiredTime) {
            this.ownerId = ownerId;
            this.schemaId = schemaId;
            this.columnIndexes = columnIndexes == null ? null : arrayToSet(columnIndexes);
            this.ttlMs = ttlMs;
            this.expirationTime = acquiredTime + ttlMs;
        }

        private static Set<Integer> arrayToSet(int[] array) {
            Set<Integer> set = new HashSet<>();
            for (int value : array) {
                set.add(value);
            }
            return set;
        }

        int getSchemaId() {
            return schemaId;
        }

        long getTtlMs() {
            return ttlMs;
        }

        void updateExpiration(long newExpirationTime) {
            this.expirationTime = newExpirationTime;
        }

        boolean isExpired(long currentTime) {
            return currentTime > expirationTime;
        }

        boolean hasSameColumns(@Nullable int[] otherColumns) {
            if (columnIndexes == null && otherColumns == null) {
                return true;
            }
            if (columnIndexes == null || otherColumns == null) {
                return false;
            }
            if (columnIndexes.size() != otherColumns.length) {
                return false;
            }
            Set<Integer> otherSet = arrayToSet(otherColumns);
            return columnIndexes.equals(otherSet);
        }

        String getColumnIndexesString() {
            if (columnIndexes == null) {
                return "all";
            }
            return columnIndexes.toString();
        }

        boolean coversColumns(int targetSchemaId, @Nullable int[] targetColumns) {
            if (this.schemaId != targetSchemaId) {
                return false;
            }

            if (columnIndexes == null) {
                return true; // Lock all columns
            }

            if (targetColumns == null) {
                return false; // Target is all columns but lock is specific
            }

            for (int targetCol : targetColumns) {
                if (!columnIndexes.contains(targetCol)) {
                    return false;
                }
            }

            return true;
        }

        boolean hasConflict(int requestedSchemaId, @Nullable int[] requestedColumns) {
            if (this.schemaId != requestedSchemaId) {
                return false; // Different schema versions don't conflict
            }

            // Same schema: check column overlap
            if (columnIndexes == null || requestedColumns == null) {
                return true; // Either locks all columns
            }

            Set<Integer> requestedSet = arrayToSet(requestedColumns);
            if (requestedSet.size() < columnIndexes.size()) {
                for (Integer col : requestedSet) {
                    if (columnIndexes.contains(col)) {
                        return true;
                    }
                }
            } else {
                for (Integer col : columnIndexes) {
                    if (requestedSet.contains(col)) {
                        return true;
                    }
                }
            }

            return false;
        }
    }
}

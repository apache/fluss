/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.producer;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidProducerIdException;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.producer.ProducerOffsets;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manager for producer offset snapshots lifecycle.
 *
 * <p>This manager handles:
 *
 * <ul>
 *   <li>Registering new producer snapshots with atomic "check and register" semantics
 *   <li>Retrieving producer snapshot offsets
 *   <li>Deleting producer snapshots
 *   <li>Periodic cleanup of expired snapshots
 *   <li>Cleanup of orphan files in remote storage
 * </ul>
 *
 * <p>The manager uses a background scheduler to periodically clean up expired snapshots and orphan
 * files. The cleanup interval and snapshot TTL are configurable.
 *
 * @see ProducerOffsetsStore for low-level storage operations
 */
public class ProducerOffsetsManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerOffsetsManager.class);

    /** Maximum number of attempts for snapshot registration to avoid infinite loops. */
    private static final int MAX_REGISTER_ATTEMPTS = 3;

    private final ProducerOffsetsStore offsetsStore;
    private final long defaultTtlMs;
    private final long cleanupIntervalMs;
    private final ScheduledExecutorService cleanupScheduler;

    public ProducerOffsetsManager(Configuration conf, ZooKeeperClient zkClient) {
        this(
                new ProducerOffsetsStore(zkClient, conf.getString(ConfigOptions.REMOTE_DATA_DIR)),
                conf.get(ConfigOptions.COORDINATOR_PRODUCER_OFFSETS_TTL).toMillis(),
                conf.get(ConfigOptions.COORDINATOR_PRODUCER_OFFSETS_CLEANUP_INTERVAL).toMillis());
    }

    @VisibleForTesting
    ProducerOffsetsManager(
            ProducerOffsetsStore offsetsStore, long defaultTtlMs, long cleanupIntervalMs) {
        this.offsetsStore = offsetsStore;
        this.defaultTtlMs = defaultTtlMs;
        this.cleanupIntervalMs = cleanupIntervalMs;
        this.cleanupScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("producer-snapshot-cleanup"));
    }

    /** Starts the background cleanup task. */
    public void start() {
        cleanupScheduler.scheduleAtFixedRate(
                this::runCleanup, cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);
        LOG.info(
                "Started producer snapshot manager with TTL={} ms, cleanup interval={} ms",
                defaultTtlMs,
                cleanupIntervalMs);
    }

    // ------------------------------------------------------------------------
    //  Public API
    // ------------------------------------------------------------------------

    /**
     * Registers a new producer offset snapshot with atomic "check and register" semantics.
     *
     * <p>This method uses ZooKeeper's version mechanism to handle concurrent requests safely:
     *
     * <ul>
     *   <li>Atomicity: Uses ZK's atomic create for new snapshots
     *   <li>Idempotency: Concurrent requests with same producerId will have exactly one succeed
     *       with CREATED, others return ALREADY_EXISTS
     *   <li>Version-based cleanup: Uses ZK version to safely delete expired snapshots without race
     *       conditions
     * </ul>
     *
     * @param producerId the producer ID (typically Flink job ID)
     * @param offsets map of TableBucket to offset for all tables
     * @param ttlMs TTL in milliseconds for the snapshot, or null to use default
     * @return true if a new snapshot was created (CREATED), false if snapshot already existed
     *     (ALREADY_EXISTS)
     * @throws InvalidProducerIdException if producerId is invalid (null, empty, contains invalid
     *     characters, etc.)
     * @throws Exception if the operation fails
     */
    public boolean registerSnapshot(String producerId, Map<TableBucket, Long> offsets, Long ttlMs)
            throws Exception {
        // Validate producerId as it will be used as both file name and ZK node name
        validateProducerId(producerId);

        long effectiveTtlMs = ttlMs != null ? ttlMs : defaultTtlMs;

        // Use loop instead of recursion to avoid stack overflow risk
        for (int attempt = 0; attempt < MAX_REGISTER_ATTEMPTS; attempt++) {
            long currentTimeMs = System.currentTimeMillis();
            long expirationTime = currentTimeMs + effectiveTtlMs;

            // Step 1: Try to atomically create the snapshot (common case)
            if (offsetsStore.tryStoreOffsets(producerId, offsets, expirationTime)) {
                return true;
            }

            // Step 2: Snapshot exists - check if it's valid or expired
            RegisterAttemptResult result =
                    handleExistingSnapshot(producerId, offsets, expirationTime, currentTimeMs);

            switch (result) {
                case ALREADY_EXISTS:
                    return false;
                case CREATED:
                    return true;
                case RETRY:
                    // Continue to next iteration
                    LOG.debug(
                            "Retrying snapshot registration for producer {} (attempt {}/{})",
                            producerId,
                            attempt + 1,
                            MAX_REGISTER_ATTEMPTS);
                    break;
            }
        }

        // Exhausted all attempts
        LOG.warn(
                "Failed to register snapshot for producer {} after {} attempts, "
                        + "concurrent modifications may have interfered",
                producerId,
                MAX_REGISTER_ATTEMPTS);
        return false;
    }

    /**
     * Handles the case where a snapshot already exists for the producer.
     *
     * @return the result indicating next action: ALREADY_EXISTS, CREATED, or RETRY
     */
    private RegisterAttemptResult handleExistingSnapshot(
            String producerId,
            Map<TableBucket, Long> offsets,
            long expirationTime,
            long currentTimeMs)
            throws Exception {

        Optional<Tuple2<ProducerOffsets, Integer>> existingWithVersion =
                offsetsStore.getOffsetsMetadataWithVersion(producerId);

        // Case 1: Snapshot was deleted between our create attempt and this check
        if (!existingWithVersion.isPresent()) {
            return offsetsStore.tryStoreOffsets(producerId, offsets, expirationTime)
                    ? RegisterAttemptResult.CREATED
                    : RegisterAttemptResult.RETRY;
        }

        ProducerOffsets existingSnapshot = existingWithVersion.get().f0;
        int version = existingWithVersion.get().f1;

        // Case 2: Valid (non-expired) snapshot exists
        if (!existingSnapshot.isExpired(currentTimeMs)) {
            LOG.info(
                    "Producer snapshot already exists for producer {} (expires at {}, version {})",
                    producerId,
                    existingSnapshot.getExpirationTime(),
                    version);
            return RegisterAttemptResult.ALREADY_EXISTS;
        }

        // Case 3: Expired snapshot - try to clean up and create new one
        return tryReplaceExpiredSnapshot(
                producerId, offsets, expirationTime, existingSnapshot, version, currentTimeMs);
    }

    /**
     * Attempts to replace an expired snapshot with a new one.
     *
     * @return the result indicating next action
     */
    private RegisterAttemptResult tryReplaceExpiredSnapshot(
            String producerId,
            Map<TableBucket, Long> offsets,
            long expirationTime,
            ProducerOffsets expiredSnapshot,
            int version,
            long currentTimeMs)
            throws Exception {

        LOG.info(
                "Found expired snapshot for producer {} (version {}), attempting cleanup",
                producerId,
                version);

        // Try version-based conditional delete
        boolean deleted =
                offsetsStore.deleteSnapshotIfVersion(producerId, expiredSnapshot, version);

        if (!deleted) {
            // Version mismatch - another process modified the snapshot
            LOG.debug(
                    "Version mismatch during delete for producer {}, checking current state",
                    producerId);
            return checkCurrentSnapshotState(producerId, currentTimeMs);
        }

        // Successfully deleted, try to create new snapshot
        if (offsetsStore.tryStoreOffsets(producerId, offsets, expirationTime)) {
            return RegisterAttemptResult.CREATED;
        }

        // Another concurrent request created a snapshot after our delete
        LOG.debug(
                "Concurrent creation detected for producer {} after delete, checking state",
                producerId);
        return checkCurrentSnapshotState(producerId, currentTimeMs);
    }

    /**
     * Checks the current snapshot state and returns the appropriate result.
     *
     * <p>This method is used after detecting concurrent modifications to determine whether a valid
     * snapshot now exists or if we should retry.
     *
     * @param producerId the producer ID
     * @param currentTimeMs the current time for expiration check
     * @return ALREADY_EXISTS if a valid snapshot exists, RETRY otherwise
     */
    private RegisterAttemptResult checkCurrentSnapshotState(String producerId, long currentTimeMs)
            throws Exception {
        Optional<ProducerOffsets> producerOffsets = offsetsStore.getOffsetsMetadata(producerId);

        if (isValidSnapshot(producerOffsets, currentTimeMs)) {
            LOG.info(
                    "Valid snapshot exists for producer {} after concurrent modification",
                    producerId);
            return RegisterAttemptResult.ALREADY_EXISTS;
        }

        LOG.debug("No valid snapshot for producer {} after concurrent modification", producerId);
        return RegisterAttemptResult.RETRY;
    }

    /**
     * Checks if a snapshot is present and not expired.
     *
     * @param producerOffsets the optional snapshot
     * @param currentTimeMs the current time for expiration check
     * @return true if snapshot exists and is not expired
     */
    private boolean isValidSnapshot(Optional<ProducerOffsets> producerOffsets, long currentTimeMs) {
        return producerOffsets.isPresent() && !producerOffsets.get().isExpired(currentTimeMs);
    }

    /** Result of a single registration attempt. */
    private enum RegisterAttemptResult {
        /** Snapshot was successfully created. */
        CREATED,
        /** A valid snapshot already exists. */
        ALREADY_EXISTS,
        /** Should retry the registration. */
        RETRY
    }

    /**
     * Gets the offsets snapshot metadata for a producer.
     *
     * @param producerId the producer ID
     * @return Optional containing the offsets snapshot if exists
     * @throws InvalidProducerIdException if producerId is invalid
     * @throws Exception if the operation fails
     */
    public Optional<ProducerOffsets> getOffsetsMetadata(String producerId) throws Exception {
        validateProducerId(producerId);
        return offsetsStore.getOffsetsMetadata(producerId);
    }

    /**
     * Reads all offsets for a producer.
     *
     * @param producerId the producer ID
     * @return map of TableBucket to offset, or empty map if no snapshot exists
     * @throws InvalidProducerIdException if producerId is invalid
     * @throws Exception if the operation fails
     */
    public Map<TableBucket, Long> readOffsets(String producerId) throws Exception {
        validateProducerId(producerId);
        return offsetsStore.readOffsets(producerId);
    }

    /**
     * Deletes a producer snapshot.
     *
     * @param producerId the producer ID
     * @throws InvalidProducerIdException if producerId is invalid
     * @throws Exception if the operation fails
     */
    public void deleteSnapshot(String producerId) throws Exception {
        validateProducerId(producerId);
        offsetsStore.deleteSnapshot(producerId);
    }

    /**
     * Gets the default TTL in milliseconds.
     *
     * @return the default TTL
     */
    public long getDefaultTtlMs() {
        return defaultTtlMs;
    }

    // ------------------------------------------------------------------------
    //  Validation
    // ------------------------------------------------------------------------

    /**
     * Validates that a producer ID is valid for use as both a file name and ZK node name.
     *
     * @param producerId the producer ID to validate
     * @throws InvalidProducerIdException if producerId is invalid
     */
    private void validateProducerId(String producerId) {
        String invalidReason = TablePath.detectInvalidName(producerId);
        if (invalidReason != null) {
            throw new InvalidProducerIdException(
                    "Invalid producer ID '" + producerId + "': " + invalidReason);
        }
    }

    // ------------------------------------------------------------------------
    //  Cleanup Operations
    // ------------------------------------------------------------------------

    /** Runs the cleanup task (expired snapshots and orphan files). */
    @VisibleForTesting
    void runCleanup() {
        try {
            int expiredCount = cleanupExpiredSnapshots();
            if (expiredCount > 0) {
                LOG.info("Producer snapshot cleanup: removed {} expired snapshots", expiredCount);
            }

            int orphanCount = cleanupOrphanFiles();
            if (orphanCount > 0) {
                LOG.info("Producer snapshot cleanup: removed {} orphan files", orphanCount);
            }
        } catch (Exception e) {
            LOG.warn("Failed to cleanup producer snapshots", e);
        }
    }

    /**
     * Cleans up expired producer snapshots.
     *
     * @return number of snapshots cleaned up
     * @throws Exception if the operation fails
     */
    @VisibleForTesting
    int cleanupExpiredSnapshots() throws Exception {
        List<String> producerIds = offsetsStore.listProducerIds();
        int cleanedCount = 0;
        long currentTime = System.currentTimeMillis();

        for (String producerId : producerIds) {
            try {
                Optional<Tuple2<ProducerOffsets, Integer>> snapshotWithVersion =
                        offsetsStore.getOffsetsMetadataWithVersion(producerId);
                if (!snapshotWithVersion.isPresent()) {
                    continue;
                }

                ProducerOffsets producerOffsets = snapshotWithVersion.get().f0;
                int version = snapshotWithVersion.get().f1;

                if (producerOffsets.isExpired(currentTime)) {
                    // Use version-based delete to avoid deleting snapshots updated just now
                    boolean deleted =
                            offsetsStore.deleteSnapshotIfVersion(
                                    producerId, producerOffsets, version);
                    if (deleted) {
                        cleanedCount++;
                        LOG.debug(
                                "Cleaned up expired snapshot for producer {} (version {})",
                                producerId,
                                version);
                    } else {
                        LOG.debug(
                                "Skip cleanup for producer {} due to version mismatch (snapshot was updated)",
                                producerId);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to cleanup snapshot for producer {}", producerId, e);
            }
        }

        return cleanedCount;
    }

    /**
     * Cleans up orphan files in remote storage that don't have corresponding ZK metadata.
     *
     * <p>Orphan files can occur when:
     *
     * <ul>
     *   <li>ZK metadata was deleted but remote file deletion failed
     *   <li>Process crashed after writing remote file but before creating ZK metadata
     *   <li>Network issues caused partial cleanup
     * </ul>
     *
     * @return number of orphan files cleaned up
     * @throws Exception if the operation fails
     */
    @VisibleForTesting
    int cleanupOrphanFiles() throws Exception {
        FsPath producersDir = offsetsStore.getProducersDirectory();
        FileSystem fileSystem = producersDir.getFileSystem();

        if (!fileSystem.exists(producersDir)) {
            LOG.debug("Producers directory does not exist, no orphan files to clean");
            return 0;
        }

        // Get all producer IDs from ZK (these are valid)
        Set<String> validProducerIds = new HashSet<>(offsetsStore.listProducerIds());

        // Collect all valid file paths from ZK metadata
        Set<String> validFilePaths = collectValidFilePaths(validProducerIds);

        int cleanedCount = 0;

        // List all producer directories in remote storage
        FileStatus[] producerDirs = fileSystem.listStatus(producersDir);
        if (producerDirs == null) {
            return 0;
        }

        for (FileStatus producerDirStatus : producerDirs) {
            if (!producerDirStatus.isDir()) {
                continue;
            }

            String producerId = producerDirStatus.getPath().getName();

            // Case 1: Producer ID not in ZK - entire directory is orphan
            if (!validProducerIds.contains(producerId)) {
                LOG.info(
                        "Found orphan producer directory {} (no ZK metadata), cleaning up",
                        producerId);
                cleanedCount +=
                        offsetsStore.deleteDirectoryRecursively(
                                fileSystem, producerDirStatus.getPath());
                continue;
            }

            // Case 2: Producer ID exists in ZK - check individual files
            cleanedCount +=
                    cleanupOrphanFilesForProducer(
                            fileSystem, producerDirStatus.getPath(), validFilePaths);
        }

        return cleanedCount;
    }

    private Set<String> collectValidFilePaths(Set<String> validProducerIds) {
        Set<String> validFilePaths = new HashSet<>();
        for (String producerId : validProducerIds) {
            try {
                Optional<ProducerOffsets> optSnapshot = offsetsStore.getOffsetsMetadata(producerId);
                if (optSnapshot.isPresent()) {
                    for (ProducerOffsets.TableOffsetMetadata tableMetadata :
                            optSnapshot.get().getTableOffsets()) {
                        validFilePaths.add(tableMetadata.getOffsetsPath().toString());
                    }
                }
            } catch (Exception e) {
                LOG.warn(
                        "Failed to get snapshot for producer {} during orphan cleanup, "
                                + "skipping its files",
                        producerId,
                        e);
            }
        }
        return validFilePaths;
    }

    private int cleanupOrphanFilesForProducer(
            FileSystem fileSystem, FsPath producerDir, Set<String> validFilePaths)
            throws IOException {
        int cleanedCount = 0;

        // List table directories under producer
        FileStatus[] tableDirs = fileSystem.listStatus(producerDir);
        if (tableDirs == null) {
            return 0;
        }

        for (FileStatus tableDirStatus : tableDirs) {
            if (!tableDirStatus.isDir()) {
                continue;
            }

            // List offset files under table directory
            FileStatus[] offsetFiles = fileSystem.listStatus(tableDirStatus.getPath());
            if (offsetFiles == null) {
                continue;
            }

            boolean hasValidFiles = false;
            for (FileStatus fileStatus : offsetFiles) {
                if (fileStatus.isDir()) {
                    continue;
                }

                String filePath = fileStatus.getPath().toString();
                if (!validFilePaths.contains(filePath)) {
                    // This file is not referenced by any ZK metadata - it's orphan
                    LOG.debug("Deleting orphan file: {}", filePath);
                    try {
                        fileSystem.delete(fileStatus.getPath(), false);
                        cleanedCount++;
                    } catch (IOException e) {
                        LOG.warn("Failed to delete orphan file: {}", filePath, e);
                    }
                } else {
                    hasValidFiles = true;
                }
            }

            // If table directory is now empty, delete it
            if (!hasValidFiles) {
                tryDeleteEmptyDirectory(fileSystem, tableDirStatus.getPath());
            }
        }

        // Check if producer directory is now empty
        tryDeleteEmptyDirectory(fileSystem, producerDir);

        return cleanedCount;
    }

    private void tryDeleteEmptyDirectory(FileSystem fileSystem, FsPath dir) {
        try {
            FileStatus[] remaining = fileSystem.listStatus(dir);
            if (remaining == null || remaining.length == 0) {
                fileSystem.delete(dir, false);
                LOG.debug("Deleted empty directory: {}", dir);
            }
        } catch (IOException e) {
            LOG.warn("Failed to delete empty directory: {}", dir, e);
        }
    }

    @Override
    public void close() {
        if (cleanupScheduler != null) {
            cleanupScheduler.shutdown();
            try {
                if (!cleanupScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("Producer snapshot manager closed");
    }
}

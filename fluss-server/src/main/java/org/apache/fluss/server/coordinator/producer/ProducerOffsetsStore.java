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

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.producer.ProducerOffsets;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.RetryUtils;
import org.apache.fluss.utils.json.TableBucketOffsets;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Low-level storage operations for producer offset snapshots.
 *
 * <p>This class handles:
 *
 * <ul>
 *   <li>Writing offset data to remote storage (OSS/S3/HDFS)
 *   <li>Reading offset data from remote storage
 *   <li>Registering snapshot metadata in ZooKeeper
 *   <li>Deleting snapshots from both ZK and remote storage
 * </ul>
 *
 * <p>This class is stateless and thread-safe. Lifecycle management should be handled by {@link
 * ProducerOffsetsManager}.
 */
public class ProducerOffsetsStore {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerOffsetsStore.class);

    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 1000;

    private final ZooKeeperClient zkClient;
    private final String remoteDataDir;

    public ProducerOffsetsStore(ZooKeeperClient zkClient, String remoteDataDir) {
        this.zkClient = zkClient;
        this.remoteDataDir = remoteDataDir;
    }

    // ------------------------------------------------------------------------
    //  Core CRUD Operations
    // ------------------------------------------------------------------------

    /**
     * Atomically stores a new producer offset snapshot.
     *
     * <p>This method first writes offset files to remote storage, then attempts to atomically
     * create the ZK metadata. If the ZK node already exists, this method returns false and cleans
     * up the remote files.
     *
     * <p><b>Note on potential orphan files:</b> There is a small window where orphan files can be
     * created if the process crashes after writing remote files but before creating ZK metadata, or
     * if ZK metadata creation fails. This is an acceptable trade-off because:
     *
     * <ul>
     *   <li>The alternative (ZK first, then files) would leave ZK metadata pointing to non-existent
     *       files, which is worse
     *   <li>Orphan files are harmless and will be cleaned up by {@link
     *       ProducerOffsetsManager#cleanupOrphanFiles()}
     *   <li>A unified orphan file cleanup mechanism will handle these cases in the future
     * </ul>
     *
     * @param producerId the producer ID
     * @param offsets map of TableBucket to offset
     * @param expirationTime the expiration timestamp in milliseconds
     * @return true if created, false if already existed
     * @throws IOException if file operations fail
     * @throws Exception if ZK operations fail
     */
    public boolean tryStoreOffsets(
            String producerId, Map<TableBucket, Long> offsets, long expirationTime)
            throws Exception {

        Map<Long, Map<TableBucket, Long>> offsetsByTable = groupOffsetsByTable(offsets);
        List<ProducerOffsets.TableOffsetMetadata> tableMetadatas = new ArrayList<>();
        List<FsPath> createdFiles = new ArrayList<>();

        try {
            // Write offset files to remote storage
            for (Map.Entry<Long, Map<TableBucket, Long>> entry : offsetsByTable.entrySet()) {
                FsPath path = writeOffsetsFile(producerId, entry.getKey(), entry.getValue());
                createdFiles.add(path);
                tableMetadatas.add(new ProducerOffsets.TableOffsetMetadata(entry.getKey(), path));
            }

            // Atomically create ZK metadata
            ProducerOffsets producerOffsets = new ProducerOffsets(expirationTime, tableMetadatas);
            boolean created = zkClient.tryRegisterProducerOffsets(producerId, producerOffsets);

            if (!created) {
                LOG.info(
                        "Snapshot already exists for producer {}, cleaning up {} files",
                        producerId,
                        createdFiles.size());
                cleanupFilesSafely(createdFiles);
                return false;
            }

            LOG.info(
                    "Stored snapshot for producer {} with {} tables, expires at {}",
                    producerId,
                    tableMetadatas.size(),
                    expirationTime);
            return true;

        } catch (Exception e) {
            LOG.warn(
                    "Failed to store snapshot for producer {}, cleaning up {} files",
                    producerId,
                    createdFiles.size(),
                    e);
            cleanupFilesSafely(createdFiles);
            throw e;
        }
    }

    /** Gets the offset snapshot metadata for a producer. */
    public Optional<ProducerOffsets> getOffsetsMetadata(String producerId) throws Exception {
        return zkClient.getProducerOffsets(producerId);
    }

    /**
     * Gets the offset snapshot metadata for a producer along with its ZK version.
     *
     * <p>The version can be used for conditional deletes to handle concurrent modifications safely.
     *
     * @param producerId the producer ID
     * @return Optional containing a Tuple2 of (ProducerOffsets, version) if exists
     * @throws Exception if the operation fails
     */
    public Optional<Tuple2<ProducerOffsets, Integer>> getOffsetsMetadataWithVersion(
            String producerId) throws Exception {
        return zkClient.getProducerOffsetsWithVersion(producerId);
    }

    /**
     * Reads all offsets for a producer from remote storage.
     *
     * @throws IOException if file reading fails
     * @throws Exception if ZK operations fail
     */
    public Map<TableBucket, Long> readOffsets(String producerId) throws Exception {
        Optional<ProducerOffsets> optSnapshot = zkClient.getProducerOffsets(producerId);
        if (!optSnapshot.isPresent()) {
            return new HashMap<>();
        }

        Map<TableBucket, Long> allOffsets = new HashMap<>();
        for (ProducerOffsets.TableOffsetMetadata metadata : optSnapshot.get().getTableOffsets()) {
            allOffsets.putAll(readOffsetsFile(metadata.getOffsetsPath()));
        }
        return allOffsets;
    }

    /** Deletes a producer snapshot (both ZK metadata and remote files). */
    public void deleteSnapshot(String producerId) throws Exception {
        Optional<ProducerOffsets> optSnapshot = zkClient.getProducerOffsets(producerId);
        if (!optSnapshot.isPresent()) {
            LOG.debug("No snapshot found for producer {}", producerId);
            return;
        }

        // Delete remote files
        for (ProducerOffsets.TableOffsetMetadata metadata : optSnapshot.get().getTableOffsets()) {
            deleteRemoteFile(metadata.getOffsetsPath());
        }

        // Delete ZK metadata
        zkClient.deleteProducerOffsets(producerId);
        LOG.info("Deleted snapshot for producer {}", producerId);
    }

    /**
     * Deletes a producer snapshot only if the ZK version matches.
     *
     * <p>This provides optimistic concurrency control - the delete will only succeed if no other
     * process has modified the snapshot since it was read.
     *
     * @param producerId the producer ID
     * @param producerOffsets the snapshot to delete (used to get file paths)
     * @param expectedVersion the expected ZK version
     * @return true if deleted successfully, false if version mismatch
     * @throws Exception if the operation fails for reasons other than version mismatch
     */
    public boolean deleteSnapshotIfVersion(
            String producerId, ProducerOffsets producerOffsets, int expectedVersion)
            throws Exception {
        // First try to delete ZK metadata with version check
        boolean deleted = zkClient.deleteProducerSnapshotIfVersion(producerId, expectedVersion);
        if (!deleted) {
            LOG.debug(
                    "Failed to delete snapshot for producer {} - version mismatch, "
                            + "snapshot was modified by another process",
                    producerId);
            return false;
        }

        // ZK metadata deleted successfully, now clean up remote files
        // Even if file deletion fails, the snapshot is already gone from ZK
        // Orphan files will be cleaned up by the periodic cleanup task
        for (ProducerOffsets.TableOffsetMetadata metadata : producerOffsets.getTableOffsets()) {
            deleteRemoteFile(metadata.getOffsetsPath());
        }

        LOG.info("Deleted snapshot for producer {} with version {}", producerId, expectedVersion);
        return true;
    }

    /** Lists all producer IDs with registered snapshots. */
    public List<String> listProducerIds() throws Exception {
        return zkClient.listProducerIds();
    }

    // ------------------------------------------------------------------------
    //  Remote Storage Operations
    // ------------------------------------------------------------------------

    /** Gets the base directory for producer snapshots in remote storage. */
    public FsPath getProducersDirectory() {
        return FlussPaths.remoteProducersDir(remoteDataDir);
    }

    /**
     * Deletes a remote file, logging but not throwing on failure.
     *
     * @param filePath the file path to delete
     * @return true if deleted successfully or file didn't exist, false if deletion failed
     */
    public boolean deleteRemoteFile(FsPath filePath) {
        try {
            FileSystem fs = filePath.getFileSystem();
            if (fs.exists(filePath)) {
                fs.delete(filePath, false);
                LOG.debug("Deleted remote file: {}", filePath);
            }
            return true;
        } catch (IOException e) {
            LOG.warn("Failed to delete remote file: {}", filePath, e);
            return false;
        }
    }

    /** Recursively deletes a directory, returning the count of deleted files. */
    public int deleteDirectoryRecursively(FileSystem fs, FsPath path) {
        int count = 0;
        try {
            FileStatus[] contents = fs.listStatus(path);
            if (contents != null) {
                for (FileStatus status : contents) {
                    if (status.isDir()) {
                        count += deleteDirectoryRecursively(fs, status.getPath());
                    } else if (fs.delete(status.getPath(), false)) {
                        count++;
                    }
                }
            }
            fs.delete(path, false);
        } catch (IOException e) {
            LOG.warn("Failed to delete directory: {}", path, e);
        }
        return count;
    }

    // ------------------------------------------------------------------------
    //  Private Helpers
    // ------------------------------------------------------------------------

    private Map<Long, Map<TableBucket, Long>> groupOffsetsByTable(Map<TableBucket, Long> offsets) {
        Map<Long, Map<TableBucket, Long>> result = new HashMap<>();
        for (Map.Entry<TableBucket, Long> entry : offsets.entrySet()) {
            result.computeIfAbsent(entry.getKey().getTableId(), k -> new HashMap<>())
                    .put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    private FsPath writeOffsetsFile(String producerId, long tableId, Map<TableBucket, Long> offsets)
            throws IOException {

        FsPath path =
                FlussPaths.remoteProducerOffsetsPath(
                        remoteDataDir, producerId, tableId, UUID.randomUUID());

        byte[] data = new TableBucketOffsets(tableId, offsets).toJsonBytes();

        return RetryUtils.executeIOWithRetry(
                () -> {
                    FileSystem fs = path.getFileSystem();
                    FsPath parentDir = path.getParent();
                    if (parentDir != null && !fs.exists(parentDir)) {
                        fs.mkdirs(parentDir);
                    }
                    try (FSDataOutputStream out = fs.create(path, FileSystem.WriteMode.OVERWRITE)) {
                        out.write(data);
                    }
                    LOG.debug(
                            "Wrote offsets for producer {} table {} at {}",
                            producerId,
                            tableId,
                            path);
                    return path;
                },
                String.format("write offsets file for producer %s table %d", producerId, tableId),
                MAX_RETRIES,
                INITIAL_BACKOFF_MS,
                MAX_BACKOFF_MS);
    }

    private Map<TableBucket, Long> readOffsetsFile(FsPath path) throws IOException {
        return RetryUtils.executeIOWithRetry(
                () -> {
                    FileSystem fs = path.getFileSystem();
                    if (!fs.exists(path)) {
                        // intend to use FlussRuntimeException here to avoid retrying
                        throw new FlussRuntimeException("Offsets file not found: " + path);
                    }
                    try (FSDataInputStream in = fs.open(path);
                            ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                        IOUtils.copyBytes(in, out, true);
                        return TableBucketOffsets.fromJsonBytes(out.toByteArray()).getOffsets();
                    }
                },
                "read offsets file " + path,
                MAX_RETRIES,
                INITIAL_BACKOFF_MS,
                MAX_BACKOFF_MS);
    }

    /**
     * Safely cleans up a list of files, ensuring all files are attempted even if some fail.
     *
     * <p>This method reuses {@link #deleteRemoteFile(FsPath)} for each file and logs a summary if
     * any deletions failed.
     *
     * @param files the list of files to clean up
     */
    private void cleanupFilesSafely(List<FsPath> files) {
        int failedCount = 0;
        for (FsPath file : files) {
            if (!deleteRemoteFile(file)) {
                failedCount++;
            }
        }
        if (failedCount > 0) {
            LOG.warn(
                    "Failed to cleanup {} out of {} files, orphan files will be cleaned by periodic task",
                    failedCount,
                    files.size());
        }
    }
}

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

package org.apache.fluss.server.coordinator.lease;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLeaseMetadata;
import org.apache.fluss.server.zk.data.lease.KvSnapshotTableLease;
import org.apache.fluss.server.zk.data.lease.KvSnapshotTableLeaseJsonSerde;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The manager to handle {@link KvSnapshotLeaseHandler} to register/update/delete metadata from zk
 * and remote fs.
 */
public class KvSnapshotLeaseMetadataManager {
    private static final Logger LOG = LoggerFactory.getLogger(KvSnapshotLeaseMetadataManager.class);

    private final ZooKeeperClient zkClient;
    private final String remoteDataDir;

    public KvSnapshotLeaseMetadataManager(ZooKeeperClient zkClient, String remoteDataDir) {
        this.zkClient = zkClient;
        this.remoteDataDir = remoteDataDir;
    }

    public List<String> getLeasesList() throws Exception {
        return zkClient.getKvSnapshotLeasesList();
    }

    /**
     * Register a new kv snapshot lease to zk and remote fs.
     *
     * <p>Follows the createdFiles pattern (similar to {@code ProducerOffsetsStore.tryStoreOffsets})
     * to track files created in this operation. On failure, only the newly created files are
     * cleaned up, avoiding accidental removal of previously successful metadata files.
     *
     * @param leaseId the lease id.
     * @param lease the kv snapshot lease.
     */
    public void registerLease(String leaseId, KvSnapshotLeaseHandler lease) throws Exception {
        List<FsPath> createdFiles = new ArrayList<>();
        try {
            Map<Long, FsPath> tableIdToRemoteMetadataFsPath =
                    generateMetadataFile(leaseId, lease, createdFiles);

            // generate remote fsPath of metadata.
            KvSnapshotLeaseMetadata leaseMetadata =
                    new KvSnapshotLeaseMetadata(
                            lease.getExpirationTime(), tableIdToRemoteMetadataFsPath);

            // register kv snapshot metadata to zk.
            zkClient.registerKvSnapshotLeaseMetadata(leaseId, leaseMetadata);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to register kv snapshot lease metadata for lease {}, "
                            + "cleaning up {} created files.",
                    leaseId,
                    createdFiles.size(),
                    e);
            cleanupFilesSafely(createdFiles);
            throw e;
        }
    }

    /**
     * Update a kv snapshot lease to zk and remote fs.
     *
     * <p>Follows the createdFiles pattern (similar to {@code ProducerOffsetsStore.tryStoreOffsets})
     * to track files created in this operation. On failure, only the newly created files are
     * cleaned up, avoiding accidental removal of previously successful metadata files.
     *
     * @param leaseId the lease id.
     * @param kvSnapshotLeaseHandle the kv snapshot lease.
     */
    public void updateLease(String leaseId, KvSnapshotLeaseHandler kvSnapshotLeaseHandle)
            throws Exception {
        // TODO change this to incremental update to avoid create too many remote metadata files.
        Optional<KvSnapshotLeaseMetadata> originalLeaseMetadata;
        List<FsPath> createdFiles = new ArrayList<>();
        try {
            originalLeaseMetadata = zkClient.getKvSnapshotLeaseMetadata(leaseId);

            Map<Long, FsPath> tableIdToNewRemoteMetadataFsPath =
                    generateMetadataFile(leaseId, kvSnapshotLeaseHandle, createdFiles);

            // generate new kv snapshot lease metadata.
            KvSnapshotLeaseMetadata newLeaseMetadata =
                    new KvSnapshotLeaseMetadata(
                            kvSnapshotLeaseHandle.getExpirationTime(),
                            tableIdToNewRemoteMetadataFsPath);

            zkClient.updateKvSnapshotLeaseMetadata(leaseId, newLeaseMetadata);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to update kv snapshot lease metadata for lease {}, "
                            + "cleaning up {} created files.",
                    leaseId,
                    createdFiles.size(),
                    e);
            cleanupFilesSafely(createdFiles);
            throw e;
        }

        // Best-effort cleanup of old metadata files. Failures are logged but not propagated,
        // as the ZK metadata has already been updated to point to the new files.
        try {
            originalLeaseMetadata.ifPresent(this::discardLeaseMetadata);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to discard original lease metadata for lease {}, "
                            + "orphaned files may exist.",
                    leaseId,
                    e);
        }
    }

    /**
     * Get a kv snapshot lease from zk and remote fs.
     *
     * @param leaseId the lease id.
     * @return the kv snapshot lease.
     */
    public Optional<KvSnapshotLeaseHandler> getLease(String leaseId) throws Exception {
        Optional<KvSnapshotLeaseMetadata> kvSnapshotLeaseMetadataOpt =
                zkClient.getKvSnapshotLeaseMetadata(leaseId);
        if (!kvSnapshotLeaseMetadataOpt.isPresent()) {
            return Optional.empty();
        }

        KvSnapshotLeaseMetadata kvSnapshotLeaseMetadata = kvSnapshotLeaseMetadataOpt.get();
        KvSnapshotLeaseHandler kvSnapshotLeasehandle =
                buildKvSnapshotLease(kvSnapshotLeaseMetadata);
        return Optional.of(kvSnapshotLeasehandle);
    }

    /**
     * Delete a kv snapshot lease from zk and remote fs.
     *
     * @param leaseId the lease id.
     */
    public void deleteLease(String leaseId) throws Exception {
        Optional<KvSnapshotLeaseMetadata> leaseMetadataOpt =
                zkClient.getKvSnapshotLeaseMetadata(leaseId);

        // delete zk metadata.
        zkClient.deleteKvSnapshotLease(leaseId);

        // Best-effort cleanup of remote metadata files. Failures are logged but not
        // propagated, as the ZK metadata has already been deleted.
        try {
            leaseMetadataOpt.ifPresent(this::discardLeaseMetadata);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to discard lease metadata for lease {}, " + "orphaned files may exist.",
                    leaseId,
                    e);
        }
    }

    /**
     * Generate metadata files for all tables in the lease. Each created file is tracked in the
     * provided {@code createdFiles} list for safe cleanup on failure.
     */
    private Map<Long, FsPath> generateMetadataFile(
            String leaseId, KvSnapshotLeaseHandler lease, List<FsPath> createdFiles)
            throws Exception {
        Map<Long, FsPath> tableIdToMetadataFile = new HashMap<>();
        for (Map.Entry<Long, KvSnapshotTableLease> entry :
                lease.getTableIdToTableLease().entrySet()) {
            long tableId = entry.getKey();
            FsPath path = generateMetadataFile(tableId, leaseId, entry.getValue());
            createdFiles.add(path);
            tableIdToMetadataFile.put(tableId, path);
        }
        return tableIdToMetadataFile;
    }

    private FsPath generateMetadataFile(
            long tableId, String leaseId, KvSnapshotTableLease tableLease) throws Exception {
        // get the remote file path to store the kv snapshot lease metadata of a table
        FsPath remoteKvSnapshotLeaseFile =
                FlussPaths.remoteKvSnapshotLeaseFile(remoteDataDir, leaseId, tableId);
        // check whether the parent directory exists, if not, create the directory
        FileSystem fileSystem = remoteKvSnapshotLeaseFile.getFileSystem();
        if (!fileSystem.exists(remoteKvSnapshotLeaseFile.getParent())) {
            fileSystem.mkdirs(remoteKvSnapshotLeaseFile.getParent());
        }

        // serialize table lease to json bytes, and write to file.
        byte[] jsonBytes = KvSnapshotTableLeaseJsonSerde.toJson(tableLease);
        try (FSDataOutputStream outputStream =
                fileSystem.create(remoteKvSnapshotLeaseFile, FileSystem.WriteMode.OVERWRITE)) {
            outputStream.write(jsonBytes);
        }
        return remoteKvSnapshotLeaseFile;
    }

    private KvSnapshotLeaseHandler buildKvSnapshotLease(
            KvSnapshotLeaseMetadata kvSnapshotLeaseMetadata) throws Exception {
        Map<Long, FsPath> tableIdToRemoteMetadataFilePath =
                kvSnapshotLeaseMetadata.getTableIdToRemoteMetadataFilePath();
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        for (Map.Entry<Long, FsPath> entry : tableIdToRemoteMetadataFilePath.entrySet()) {
            long tableId = entry.getKey();
            FsPath remoteMetadataFilePath = entry.getValue();
            tableIdToTableLease.put(tableId, buildKvSnapshotTableLease(remoteMetadataFilePath));
        }
        return new KvSnapshotLeaseHandler(
                kvSnapshotLeaseMetadata.getExpirationTime(), tableIdToTableLease);
    }

    private KvSnapshotTableLease buildKvSnapshotTableLease(FsPath remoteMetadataFilePath)
            throws Exception {
        checkNotNull(remoteMetadataFilePath);
        FSDataInputStream inputStream =
                remoteMetadataFilePath.getFileSystem().open(remoteMetadataFilePath);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(inputStream, outputStream, true);
            return KvSnapshotTableLeaseJsonSerde.fromJson(outputStream.toByteArray());
        }
    }

    private void discardLeaseMetadata(KvSnapshotLeaseMetadata kvSnapshotLeaseMetadata) {
        cleanupFilesSafely(
                new ArrayList<>(
                        kvSnapshotLeaseMetadata.getTableIdToRemoteMetadataFilePath().values()));
    }

    /**
     * Safely clean up a list of remote files. Each file deletion is independent - failures for
     * individual files are logged but do not prevent cleanup of remaining files.
     */
    private void cleanupFilesSafely(List<FsPath> files) {
        for (FsPath file : files) {
            try {
                FileSystem fileSystem = file.getFileSystem();
                if (fileSystem.exists(file)) {
                    fileSystem.delete(file, false);
                }
            } catch (Exception e) {
                LOG.warn(
                        "Error deleting remote file path of kv snapshot lease metadata at {}",
                        file,
                        e);
            }
        }
    }
}

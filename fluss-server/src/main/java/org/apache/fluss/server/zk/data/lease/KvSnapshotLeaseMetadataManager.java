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

package org.apache.fluss.server.zk.data.lease;

import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The manager to handle {@link KvSnapshotLease} to register/update/delete metadata from zk and
 * remote fs.
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
     * @param leaseId the lease id.
     * @param lease the kv snapshot lease.
     */
    public void registerLease(String leaseId, KvSnapshotLease lease) throws Exception {
        Map<Long, FsPath> tableIdToRemoteMetadataFsPath = generateMetadataFile(leaseId, lease);

        // generate remote fsPath of metadata.
        KvSnapshotLeaseMetadata leaseMetadata =
                new KvSnapshotLeaseMetadata(
                        lease.getExpirationTime(), tableIdToRemoteMetadataFsPath);

        // register kv snapshot metadata to zk.
        try {
            zkClient.registerKvSnapshotLeaseMetadata(leaseId, leaseMetadata);
        } catch (Exception e) {
            LOG.warn("Failed to register kv snapshot lease metadata to zk.", e);
            leaseMetadata.discard();
            throw e;
        }
    }

    /**
     * Update a kv snapshot lease to zk and remote fs.
     *
     * @param leaseId the lease id.
     * @param kvSnapshotLease the kv snapshot lease.
     */
    public void updateLease(String leaseId, KvSnapshotLease kvSnapshotLease) throws Exception {
        // TODO change this to incremental update to avoid create too many remote metadata files.

        Optional<KvSnapshotLeaseMetadata> originalLeaseMetadata =
                zkClient.getKvSnapshotLeaseMetadata(leaseId);

        Map<Long, FsPath> tableIdToNewRemoteMetadataFsPath =
                generateMetadataFile(leaseId, kvSnapshotLease);

        // generate  new kv snapshot lease metadata.
        KvSnapshotLeaseMetadata newLeaseMetadata =
                new KvSnapshotLeaseMetadata(
                        kvSnapshotLease.getExpirationTime(), tableIdToNewRemoteMetadataFsPath);
        // register new snapshot metadata to zk.
        try {
            zkClient.updateKvSnapshotLeaseMetadata(leaseId, newLeaseMetadata);
        } catch (Exception e) {
            LOG.warn("Failed to update kv snapshot lease metadata to zk.", e);
            newLeaseMetadata.discard();
            throw e;
        }

        // discard original snapshot metadata.
        originalLeaseMetadata.ifPresent(KvSnapshotLeaseMetadata::discard);
    }

    /**
     * Get a kv snapshot lease from zk and remote fs.
     *
     * @param leaseId the lease id.
     * @return the kv snapshot lease.
     */
    public Optional<KvSnapshotLease> getLease(String leaseId) throws Exception {
        Optional<KvSnapshotLeaseMetadata> kvSnapshotLeaseMetadataOpt =
                zkClient.getKvSnapshotLeaseMetadata(leaseId);
        if (!kvSnapshotLeaseMetadataOpt.isPresent()) {
            return Optional.empty();
        }

        KvSnapshotLeaseMetadata kvSnapshotLeaseMetadata = kvSnapshotLeaseMetadataOpt.get();
        KvSnapshotLease kvSnapshotLease = buildKvSnapshotLease(kvSnapshotLeaseMetadata);
        return Optional.of(kvSnapshotLease);
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

        // delete remote metadata file.
        leaseMetadataOpt.ifPresent(KvSnapshotLeaseMetadata::discard);
    }

    /**
     * Check whether the snapshot exists for the bucket in zookeeper.
     *
     * @param tableBucket the table bucket.
     * @param snapshotId the snapshot id.
     * @return true if the snapshot exists in the bucket.
     */
    public boolean isSnapshotExists(TableBucket tableBucket, long snapshotId) throws Exception {
        List<Tuple2<BucketSnapshot, Long>> allSnapshotAndIds =
                zkClient.getTableBucketAllSnapshotAndIds(tableBucket);
        for (Tuple2<BucketSnapshot, Long> snapshotAndId : allSnapshotAndIds) {
            if (snapshotAndId.f1 == snapshotId) {
                return true;
            }
        }
        return false;
    }

    private Map<Long, FsPath> generateMetadataFile(String leaseId, KvSnapshotLease lease)
            throws Exception {
        Map<Long, FsPath> tableIdToMetadataFile = new HashMap<>();
        for (Map.Entry<Long, KvSnapshotTableLease> entry :
                lease.getTableIdToTableLease().entrySet()) {
            long tableId = entry.getKey();
            tableIdToMetadataFile.put(
                    tableId, generateMetadataFile(tableId, leaseId, entry.getValue()));
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

    private KvSnapshotLease buildKvSnapshotLease(KvSnapshotLeaseMetadata kvSnapshotLeaseMetadata)
            throws Exception {
        Map<Long, FsPath> tableIdToRemoteMetadataFilePath =
                kvSnapshotLeaseMetadata.getTableIdToRemoteMetadataFilePath();
        Map<Long, KvSnapshotTableLease> tableIdToTableLease = new HashMap<>();
        for (Map.Entry<Long, FsPath> entry : tableIdToRemoteMetadataFilePath.entrySet()) {
            long tableId = entry.getKey();
            FsPath remoteMetadataFilePath = entry.getValue();
            tableIdToTableLease.put(tableId, buildKvSnapshotTableLease(remoteMetadataFilePath));
        }
        return new KvSnapshotLease(
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
}

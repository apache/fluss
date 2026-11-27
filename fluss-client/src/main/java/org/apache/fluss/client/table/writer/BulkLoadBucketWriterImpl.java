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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.metadata.BulkLoadBuildContext;
import org.apache.fluss.client.metadata.BulkLoadHandle;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.ReadableConfig;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.serializer.RowSerializer;
import org.apache.fluss.rpc.gateway.AdminGateway;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotRequest;
import org.apache.fluss.rpc.messages.AllocateBulkLoadBucketSnapshotResponse;
import org.apache.fluss.rpc.messages.CommitBulkLoadBucketSnapshotRequest;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;

import io.github.fluss_contrib.rocksdb.Checkpoint;
import io.github.fluss_contrib.rocksdb.CompressionType;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksDBException;
import io.github.fluss_contrib.rocksdb.RocksIterator;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.fluss.client.utils.ClientRpcMessageUtils.setBulkLoadHandle;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Builds and registers the checkpoint for one allocated bucket. */
@Internal
@NotThreadSafe
final class BulkLoadBucketWriterImpl implements BulkLoadBucketWriter {

    private static final int BUCKET_ALLOCATED = 0;
    private static final int BUCKET_COMMITTED = 1;
    private static final String METADATA_FILE_NAME = "_METADATA";

    private final AdminGateway gateway;
    private final BulkLoadHandle handle;
    private final TableBucket bucket;
    private final long snapshotId;
    private final long logOffset;
    private final FsPath snapshotDirectory;
    private final Path localWorkDir;
    private final Options rocksDbOptions;
    private final RocksDB db;
    private final KeyEncoder keyEncoder;
    private final RowSerializer rowSerializer;
    private final int fieldCount;
    private final short schemaId;
    private final boolean countRows;

    private boolean commitStarted;
    private boolean uploaded;
    private boolean closed;
    private boolean committed;

    private BulkLoadBucketWriterImpl(
            AdminGateway gateway,
            BulkLoadBuildContext.Target target,
            TableBucket bucket,
            AllocateBulkLoadBucketSnapshotResponse allocation,
            FsPath snapshotDirectory,
            ReadableConfig configuration)
            throws IOException {
        this.gateway = gateway;
        this.handle = target.getHandle();
        this.bucket = bucket;
        this.snapshotId = allocation.getSnapshotId();
        this.logOffset = allocation.getLogOffset();
        this.snapshotDirectory = snapshotDirectory;

        TableInfo table = target.getTableInfo();
        TableConfig tableConfig = table.getTableConfig();
        checkArgument(
                !KvValueLayout.fromTableConfig(tableConfig).hasValueTag()
                        && !tableConfig.getKvTTL().isPresent(),
                "BulkLoad bucket writers do not support tagged KV values or row TTL.");
        this.schemaId = (short) table.getSchemaId();
        this.countRows = tableConfig.getChangelogImage() == ChangelogImage.FULL;
        RowType rowType = table.getRowType();
        this.keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        rowType,
                        table.getPhysicalPrimaryKeys(),
                        tableConfig,
                        table.isDefaultBucketKey());
        this.fieldCount = rowType.getFieldCount();
        BinaryRowFormat rowFormat =
                tableConfig.getKvFormat() == KvFormat.COMPACTED
                        ? BinaryRowFormat.COMPACTED
                        : BinaryRowFormat.INDEXED;
        this.rowSerializer =
                new RowSerializer(rowType.getChildren().toArray(new DataType[0]), rowFormat);

        RocksDB.loadLibrary();
        Path workRoot = Paths.get(configuration.get(ConfigOptions.CLIENT_IO_TMP_DIR));
        if (!Files.isDirectory(workRoot)) {
            Files.createDirectories(workRoot);
        }
        this.localWorkDir = Files.createTempDirectory(workRoot, "fluss-bulkload-");
        Options options = null;
        try {
            options = new Options();
            List<CompressionType> compressionPerLevel = new ArrayList<>();
            for (ConfigOptions.KvCompressionType compression :
                    configuration.get(ConfigOptions.KV_COMPRESSION_PER_LEVEL)) {
                compressionPerLevel.add(toRocksDbCompressionType(compression));
            }
            options.setCreateIfMissing(true).setCompressionPerLevel(compressionPerLevel);
            this.db = RocksDB.open(options, localWorkDir.resolve("db").toString());
            this.rocksDbOptions = options;
        } catch (RocksDBException | RuntimeException e) {
            IOUtils.closeQuietly(options);
            FileUtils.deleteDirectoryQuietly(localWorkDir.toFile());
            throw new IOException("Failed to open local RocksDB for " + bucket + ".", e);
        }
    }

    /** Allocates a snapshot and opens a writer if the bucket still needs to be built. */
    static Optional<BulkLoadBucketWriter> tryOpen(
            BulkLoadBuildContext.Target target,
            int bucketId,
            AdminGateway gateway,
            ReadableConfig configuration)
            throws IOException {
        checkArgument(
                bucketId >= 0 && bucketId < target.getBucketCount(),
                "Invalid bucket ID %s for target %s with %s buckets",
                bucketId,
                target.getPath(),
                target.getBucketCount());

        BulkLoadHandle handle = target.getHandle();
        TableBucket bucket =
                new TableBucket(handle.getTableId(), target.getPartitionId(), bucketId);
        AllocateBulkLoadBucketSnapshotRequest request =
                new AllocateBulkLoadBucketSnapshotRequest().setBucketId(bucketId);
        setBulkLoadHandle(request.setHandle(), handle);
        if (bucket.getPartitionId() != null) {
            request.setPartitionId(bucket.getPartitionId());
        }

        AllocateBulkLoadBucketSnapshotResponse allocation =
                await(
                        gateway.allocateBulkLoadBucketSnapshot(request),
                        "Failed to allocate a BulkLoad snapshot for " + bucket + ".");
        if (allocation.getState() == BUCKET_COMMITTED) {
            return Optional.empty();
        }
        checkState(
                allocation.getState() == BUCKET_ALLOCATED,
                "Unknown BulkLoad bucket state: %s",
                allocation.getState());

        FsPath directory = snapshotDirectory(target, bucket, allocation.getSnapshotId());
        FsPath metadata = metadataPath(directory);
        if (metadata.getFileSystem().exists(metadata)) {
            commitBucket(gateway, handle, bucket);
            return Optional.empty();
        }
        return Optional.of(
                new BulkLoadBucketWriterImpl(
                        gateway, target, bucket, allocation, directory, configuration));
    }

    @Override
    public void write(InternalRow row) throws IOException {
        checkState(!closed, "BulkLoad bucket writer is closed.");
        checkState(!commitStarted, "BulkLoad bucket commit has already started.");
        checkNotNull(row, "BulkLoad input row must not be null.");
        checkArgument(
                row.getFieldCount() == fieldCount,
                "BulkLoad input row has %s fields but the table schema has %s.",
                row.getFieldCount(),
                fieldCount);
        byte[] key = keyEncoder.encodeKey(row);
        byte[] value = ValueEncoder.encodeValue(schemaId, rowSerializer.toBinaryRow(row));
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new IOException("Failed to write row to " + bucket + ".", e);
        }
    }

    @Override
    public void commit() throws IOException {
        checkState(!closed, "BulkLoad bucket writer is closed.");
        if (!uploaded) {
            checkState(
                    !commitStarted,
                    "Close the failed BulkLoad writer before recovering its bucket.");
            commitStarted = true;
            try {
                uploadSnapshot();
                uploaded = true;
            } catch (RocksDBException e) {
                throw new IOException(
                        "Failed to create a BulkLoad snapshot for " + bucket + ".", e);
            }
        }

        if (!committed) {
            commitBucket(gateway, handle, bucket);
            committed = true;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            db.close();
        } finally {
            IOUtils.closeQuietly(rocksDbOptions);
            FileUtils.deleteDirectoryQuietly(localWorkDir.toFile());
        }
    }

    private void uploadSnapshot() throws IOException, RocksDBException {
        Path checkpointPath = localWorkDir.resolve("checkpoint");
        try (Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(checkpointPath.toString());
        }

        Long rowCount = countRows ? countKeys() : null;
        List<KvSnapshotFileMetadata.FileHandle> sharedFiles = new ArrayList<>();
        List<KvSnapshotFileMetadata.FileHandle> privateFiles = new ArrayList<>();
        FsPath sharedDirectory = FlussPaths.remoteKvSharedDir(snapshotDirectory.getParent());
        FileSystem fs = snapshotDirectory.getFileSystem();
        String attempt = UUID.randomUUID().toString();
        long totalSize = 0L;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(checkpointPath)) {
            for (Path file : files) {
                String localName = file.getFileName().toString();
                boolean shared = localName.endsWith(".sst");
                FsPath remoteFile =
                        new FsPath(
                                shared ? sharedDirectory : snapshotDirectory,
                                attempt + "-" + localName);
                long fileSize;
                try (InputStream in = Files.newInputStream(file);
                        FSDataOutputStream out =
                                fs.create(remoteFile, FileSystem.WriteMode.NO_OVERWRITE)) {
                    fileSize = IOUtils.copyBytes(in, out, false);
                }
                (shared ? sharedFiles : privateFiles)
                        .add(
                                new KvSnapshotFileMetadata.FileHandle(
                                        remoteFile.toString(), fileSize, localName));
                totalSize = Math.addExact(totalSize, fileSize);
            }
        }

        KvSnapshotFileMetadata metadata =
                new KvSnapshotFileMetadata(
                        bucket,
                        snapshotId,
                        snapshotDirectory.toString(),
                        sharedFiles,
                        privateFiles,
                        totalSize,
                        logOffset,
                        rowCount,
                        null);
        byte[] bytes = KvSnapshotFileMetadataJsonSerde.toJson(metadata);
        try (FSDataOutputStream out =
                fs.create(metadataPath(snapshotDirectory), FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(bytes);
        }
    }

    private long countKeys() throws RocksDBException {
        long count = 0L;
        try (RocksIterator iterator = db.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
            iterator.status();
        }
        return count;
    }

    private static void commitBucket(
            AdminGateway gateway, BulkLoadHandle handle, TableBucket bucket) throws IOException {
        CommitBulkLoadBucketSnapshotRequest request =
                new CommitBulkLoadBucketSnapshotRequest().setBucketId(bucket.getBucket());
        setBulkLoadHandle(request.setHandle(), handle);
        if (bucket.getPartitionId() != null) {
            request.setPartitionId(bucket.getPartitionId());
        }
        await(
                gateway.commitBulkLoadBucketSnapshot(request),
                "Failed to commit the BulkLoad snapshot for " + bucket + ".");
    }

    private static <T> T await(CompletableFuture<T> result, String failureMessage)
            throws IOException {
        try {
            return result.join();
        } catch (CompletionException e) {
            throw new IOException(failureMessage, e.getCause());
        }
    }

    private static FsPath snapshotDirectory(
            BulkLoadBuildContext.Target target, TableBucket bucket, long snapshotId) {
        FsPath tabletDirectory =
                FlussPaths.remoteKvTabletDir(
                        FlussPaths.remoteKvDir(target.getRemoteDataDir()),
                        target.getPath(),
                        bucket);
        return FlussPaths.remoteKvSnapshotDir(tabletDirectory, snapshotId);
    }

    private static FsPath metadataPath(FsPath directory) {
        return new FsPath(directory, METADATA_FILE_NAME);
    }

    private static CompressionType toRocksDbCompressionType(
            ConfigOptions.KvCompressionType compression) {
        switch (compression) {
            case NO:
                return CompressionType.NO_COMPRESSION;
            case SNAPPY:
                return CompressionType.SNAPPY_COMPRESSION;
            case LZ4:
                return CompressionType.LZ4_COMPRESSION;
            case ZSTD:
                return CompressionType.ZSTD_COMPRESSION;
            default:
                throw new IllegalArgumentException("Unsupported compression type: " + compression);
        }
    }
}

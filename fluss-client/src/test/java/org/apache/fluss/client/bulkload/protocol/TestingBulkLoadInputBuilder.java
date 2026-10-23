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

package org.apache.fluss.client.bulkload.protocol;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadTargetInfo;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.rpc.messages.CommitBulkLoadRequest;
import org.apache.fluss.rpc.messages.PbBulkLoadTargetInfo;
import org.apache.fluss.shaded.guava32.com.google.common.hash.Hasher;
import org.apache.fluss.shaded.guava32.com.google.common.hash.Hashing;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;

import io.github.fluss_contrib.rocksdb.BlockBasedTableConfig;
import io.github.fluss_contrib.rocksdb.Checkpoint;
import io.github.fluss_contrib.rocksdb.CompressionType;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toPhysicalTablePath;

/** Produces real RocksDB snapshots as independent inputs to server protocol tests. */
public final class TestingBulkLoadInputBuilder {

    /** Builds final ordinary Snapshot files and returns their ordered metadata digest. */
    public CommitBulkLoadRequest build(PbBulkLoadTargetInfo target, List<Object[]> logicalRows)
            throws Exception {
        return build(target, logicalRows, Collections.emptyMap());
    }

    /**
     * Builds snapshots at explicitly supplied offsets, using final row counts for other buckets.
     */
    public CommitBulkLoadRequest build(
            PbBulkLoadTargetInfo target, List<Object[]> logicalRows, Map<Integer, Long> offsets)
            throws Exception {
        BulkLoadHandle handle =
                new BulkLoadHandle(
                        toPhysicalTablePath(target.getHandle().getTarget()),
                        target.getHandle().getTableId(),
                        target.getHandle().hasPartitionId()
                                ? target.getHandle().getPartitionId()
                                : null,
                        target.getHandle().getBulkLoadId());
        TableInfo tableInfo =
                TableInfo.of(
                        handle.getTarget().getTablePath(),
                        handle.getTableId(),
                        target.getSchemaId(),
                        TableDescriptor.fromJsonBytes(target.getTableJson()),
                        target.getRemoteDataDir(),
                        target.getCreatedTime(),
                        target.getModifiedTime());
        BulkLoadTargetInfo targetInfo =
                new BulkLoadTargetInfo(handle, tableInfo, target.getSnapshotIds());
        ChangelogImage changelogImage = tableInfo.getTableConfig().getChangelogImage();
        KeyEncoder keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        // Last value for each primary key is the state represented by the checkpoint.
        Map<String, Object[]> finalRows = new LinkedHashMap<>();
        for (Object[] values : logicalRows) {
            finalRows.put(
                    Arrays.toString(
                            keyEncoder.encodeKey(
                                    DataTestUtils.row(tableInfo.getRowType(), values))),
                    values);
        }
        List<Object[]> expectedRows = new ArrayList<>(finalRows.values());
        List<List<Object[]>> rowsByBucket = distribute(tableInfo, expectedRows);
        Hasher digest = Hashing.sha256().newHasher();
        for (int bucketId = 0; bucketId < rowsByBucket.size(); bucketId++) {
            List<Object[]> bucketRows = rowsByBucket.get(bucketId);
            long offset = offsets.getOrDefault(bucketId, (long) bucketRows.size());
            Long rowCount =
                    changelogImage == ChangelogImage.FULL ? Long.valueOf(bucketRows.size()) : null;
            digest.putBytes(writeSnapshot(targetInfo, bucketId, bucketRows, offset, rowCount));
        }
        return new CommitBulkLoadRequest()
                .setHandle(target.getHandle())
                .setInputSha256(digest.hash().toString());
    }

    private static byte[] writeSnapshot(
            BulkLoadTargetInfo info, int bucketId, List<Object[]> rows, long offset, Long rowCount)
            throws Exception {
        TableInfo table = info.getTableInfo();
        BulkLoadHandle handle = info.getHandle();
        TableBucket bucket =
                new TableBucket(handle.getTableId(), handle.getPartitionId(), bucketId);
        FsPath tabletDir =
                FlussPaths.remoteKvTabletDir(
                        new FsPath(table.getRemoteDataDir(), FlussPaths.REMOTE_KV_DIR_NAME),
                        handle.getTarget(),
                        bucket);
        FsPath snapshotDir =
                FlussPaths.remoteKvSnapshotDir(tabletDir, info.getSnapshotId(bucketId));
        Path work = Files.createTempDirectory("bulkload-server-input-");
        RocksDB.loadLibrary();
        try {
            Path checkpointDir = work.resolve("checkpoint");
            KeyEncoder keys =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            table.getRowType(),
                            table.getPhysicalPrimaryKeys(),
                            table.getTableConfig(),
                            table.isDefaultBucketKey());
            RowEncoder encoder =
                    RowEncoder.create(table.getTableConfig().getKvFormat(), table.getRowType());
            try (Options options =
                            new Options()
                                    .setCreateIfMissing(true)
                                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                                    .setTableFormatConfig(new BlockBasedTableConfig());
                    RocksDB db = RocksDB.open(options, work.resolve("db").toString());
                    FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                for (Object[] values : rows) {
                    InternalRow row = DataTestUtils.row(table.getRowType(), values);
                    encoder.startNewRow();
                    for (int field = 0; field < table.getRowType().getFieldCount(); field++) {
                        encoder.encodeField(
                                field,
                                InternalRow.createFieldGetter(
                                                table.getRowType().getTypeAt(field), field)
                                        .getFieldOrNull(row));
                    }
                    db.put(
                            keys.encodeKey(row),
                            ValueEncoder.encodeValue(
                                    (short) table.getSchemaId(), encoder.finishRow().copy()));
                }
                db.flush(flush);
                try (Checkpoint checkpoint = Checkpoint.create(db)) {
                    checkpoint.createCheckpoint(checkpointDir.toString());
                }
            }
            List<KvSnapshotFileMetadata.FileHandle> files = new ArrayList<>();
            long size = 0;
            try (Stream<Path> paths = Files.list(checkpointDir)) {
                for (Path local : paths.filter(Files::isRegularFile).collect(Collectors.toList())) {
                    FsPath remote = new FsPath(snapshotDir, local.getFileName().toString());
                    remote.getFileSystem().mkdirs(remote.getParent());
                    try (FSDataOutputStream out =
                            remote.getFileSystem()
                                    .create(remote, FileSystem.WriteMode.NO_OVERWRITE)) {
                        Files.copy(local, out);
                    }
                    long length = Files.size(local);
                    files.add(
                            new KvSnapshotFileMetadata.FileHandle(
                                    remote.toString(), length, local.getFileName().toString()));
                    size += length;
                }
            }
            byte[] bytes =
                    KvSnapshotFileMetadataJsonSerde.toJson(
                            new KvSnapshotFileMetadata(
                                    bucket,
                                    info.getSnapshotId(bucketId),
                                    snapshotDir.toString(),
                                    Collections.emptyList(),
                                    files,
                                    size,
                                    offset,
                                    rowCount,
                                    null));
            FsPath metadata = new FsPath(snapshotDir, "_METADATA");
            writeBytes(metadata, bytes);
            return Hashing.sha256().hashBytes(bytes).asBytes();
        } finally {
            FileUtils.deleteDirectoryQuietly(work.toFile());
        }
    }

    private static void writeBytes(FsPath path, byte[] bytes) throws Exception {
        path.getFileSystem().mkdirs(path.getParent());
        try (FSDataOutputStream out =
                path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
            out.write(bytes);
        }
    }

    /** Computes the bucket owning the given logical row. */
    public static int bucketOf(TableInfo tableInfo, Object[] values) {
        return bucketer(tableInfo).applyAsInt(values);
    }

    private static ToIntFunction<Object[]> bucketer(TableInfo tableInfo) {
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.ofBucketKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getBucketKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey(),
                        primaryKeyEncoder);
        BucketingFunction bucketingFunction =
                BucketingFunction.of(tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
        return values ->
                bucketingFunction.bucketing(
                        bucketKeyEncoder.encodeKey(
                                DataTestUtils.row(tableInfo.getRowType(), values)),
                        tableInfo.getNumBuckets());
    }

    private static List<List<Object[]>> distribute(TableInfo tableInfo, List<Object[]> rows) {
        List<List<Object[]>> rowsByBucket = new ArrayList<>(tableInfo.getNumBuckets());
        for (int bucket = 0; bucket < tableInfo.getNumBuckets(); bucket++) {
            rowsByBucket.add(new ArrayList<>());
        }
        ToIntFunction<Object[]> bucketer = bucketer(tableInfo);
        for (Object[] values : rows) {
            rowsByBucket.get(bucketer.applyAsInt(values)).add(values);
        }
        return rowsByBucket;
    }
}

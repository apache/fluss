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

package org.apache.fluss.server.testutils;

import org.apache.fluss.fs.FSDataOutputStream;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.types.Tuple2;

import io.github.fluss_contrib.rocksdb.BlockBasedTableConfig;
import io.github.fluss_contrib.rocksdb.Checkpoint;
import io.github.fluss_contrib.rocksdb.CompressionType;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Writes an external RocksDB checkpoint in the BulkLoad input format. */
public final class TestingBulkLoadSnapshot {
    private TestingBulkLoadSnapshot() {}

    public static CompletedSnapshotHandle writeSnapshot(
            FsPath remoteDataDir,
            PhysicalTablePath physical,
            TableBucket bucket,
            long snapshotId,
            long offset,
            Long rowCount,
            List<Tuple2<byte[], byte[]>> records)
            throws Exception {
        FsPath tabletDir =
                FlussPaths.remoteKvTabletDir(
                        new FsPath(remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME), physical, bucket);
        FsPath snapshotDir = FlussPaths.remoteKvSnapshotDir(tabletDir, snapshotId);
        Path work = Files.createTempDirectory("bulkload-server-input-");
        RocksDB.loadLibrary();
        try {
            Path checkpointDir = work.resolve("checkpoint");
            try (Options options =
                            new Options()
                                    .setCreateIfMissing(true)
                                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                                    .setTableFormatConfig(new BlockBasedTableConfig());
                    RocksDB db = RocksDB.open(options, work.resolve("db").toString());
                    FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                for (Tuple2<byte[], byte[]> entry : records) {
                    db.put(entry.f0, entry.f1);
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
                                    snapshotId,
                                    snapshotDir.toString(),
                                    Collections.emptyList(),
                                    files,
                                    size,
                                    offset,
                                    rowCount,
                                    null));
            FsPath metadata = new FsPath(snapshotDir, "_METADATA");
            writeBytes(metadata, bytes);
            return new CompletedSnapshotHandle(snapshotId, metadata, offset);
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
}

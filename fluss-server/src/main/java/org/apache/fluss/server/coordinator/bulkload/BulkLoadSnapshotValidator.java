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

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.InvalidBulkLoadSnapshotException;
import org.apache.fluss.exception.StorageException;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.kv.snapshot.KvFileHandle;
import org.apache.fluss.server.kv.snapshot.KvFileHandleAndLocalPath;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.utils.FlussPaths;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Validates one uploaded ordinary snapshot without scanning another bucket's input. */
final class BulkLoadSnapshotValidator {
    private BulkLoadSnapshotValidator() {}

    static CompletedSnapshot validate(
            TablePath tablePath,
            BulkLoadTarget target,
            TableBucket bucket,
            BulkLoadBucket allocation) {
        PhysicalTablePath physical = PhysicalTablePath.of(tablePath, target.partitionName);
        FsPath tabletDir =
                FlussPaths.remoteKvTabletDir(
                        FlussPaths.remoteKvDir(target.remoteDataDir), physical, bucket);
        FsPath directory = FlussPaths.remoteKvSnapshotDir(tabletDir, allocation.snapshotId);
        FsPath metadataPath = CompletedSnapshot.getMetadataFilePath(directory);
        byte[] metadata;
        try {
            metadata = CompletedSnapshotHandle.readMetadataBytes(metadataPath);
        } catch (IOException | UncheckedIOException e) {
            throw readFailure(metadataPath, e);
        }
        CompletedSnapshot snapshot;
        try {
            snapshot = CompletedSnapshotJsonSerde.fromJson(metadata);
        } catch (RuntimeException e) {
            throw new InvalidBulkLoadSnapshotException(
                    "Cannot decode '" + metadataPath + "' as KV snapshot metadata.", e);
        }
        if (!snapshot.getTableBucket().equals(bucket)) {
            throw invalid(
                    "%s identifies %s, but this commit is for %s.",
                    metadataPath, snapshot.getTableBucket(), bucket);
        }
        if (snapshot.getSnapshotID() != allocation.snapshotId) {
            throw invalid(
                    "%s declares snapshot_id %d, but Allocate returned %d.",
                    metadataPath, snapshot.getSnapshotID(), allocation.snapshotId);
        }
        if (snapshot.getLogOffset() != allocation.logOffset) {
            throw invalid(
                    "%s declares log_offset %d, but Allocate returned %d.",
                    metadataPath, snapshot.getLogOffset(), allocation.logOffset);
        }
        if (!snapshot.getSnapshotLocation().equals(directory)) {
            throw invalid(
                    "%s declares snapshot_location '%s'. Expected the allocated directory '%s'.",
                    metadataPath, snapshot.getSnapshotLocation(), directory);
        }
        if (snapshot.getFirstAutoIncIDRange() != null) {
            throw invalid(
                    "%s contains auto_inc_id_range. Omit this field from BulkLoad snapshots.",
                    metadataPath);
        }
        KvSnapshotHandle files = snapshot.getKvSnapshotHandle();
        Set<String> localPaths = new HashSet<>();
        long size =
                validateFiles(
                        files.getSharedKvFileHandles(),
                        FlussPaths.remoteKvSharedDir(tabletDir),
                        localPaths,
                        0L);
        size = validateFiles(files.getPrivateFileHandles(), directory, localPaths, size);
        if (files.getIncrementalSize() != size) {
            throw invalid(
                    "%s declares snapshot_incremental_size of %d bytes, but the uploaded files total %d bytes.",
                    metadataPath, files.getIncrementalSize(), size);
        }
        return snapshot;
    }

    private static long validateFiles(
            List<KvFileHandleAndLocalPath> files,
            FsPath ownerDirectory,
            Set<String> local,
            long size) {
        String prefix = ownerDirectory + "/";
        Set<String> remote = new HashSet<>();
        FsPath path = ownerDirectory;
        try {
            FileSystem fileSystem = ownerDirectory.getFileSystem();
            for (KvFileHandleAndLocalPath entry : files) {
                KvFileHandle file = entry.getKvFileHandle();
                String localPath = entry.getLocalPath();
                String remotePath = file.getFilePath();
                try {
                    if (!entry.hasSafeLocalPath()) {
                        throw invalid(
                                "Invalid local_path '%s' for '%s'. Use a normalized relative path inside the snapshot directory.",
                                localPath, remotePath);
                    }
                    path = new FsPath(remotePath);
                } catch (IllegalArgumentException e) {
                    throw new InvalidBulkLoadSnapshotException(
                            String.format(
                                    "Invalid snapshot file path (path '%s', local_path '%s').",
                                    remotePath, localPath),
                            e);
                }
                try {
                    size = Math.addExact(size, file.getSize());
                } catch (ArithmeticException e) {
                    throw new InvalidBulkLoadSnapshotException(
                            "Total snapshot size exceeds the supported range at file '"
                                    + remotePath
                                    + "'.",
                            e);
                }
                if (!remotePath.equals(path.toString())) {
                    throw invalid("File path '%s' is not normalized. Use '%s'.", remotePath, path);
                }
                if (!local.add(localPath)) {
                    throw invalid(
                            "Snapshot '%s' lists local_path '%s' more than once.",
                            ownerDirectory, localPath);
                }
                if (!remote.add(remotePath)) {
                    throw invalid(
                            "Snapshot '%s' lists remote file '%s' more than once.",
                            ownerDirectory, remotePath);
                }
                if (!remotePath.startsWith(prefix)) {
                    throw invalid(
                            "File '%s' is outside the expected directory '%s'.",
                            remotePath, ownerDirectory);
                }
                FileStatus status = fileSystem.getFileStatus(path);
                if (status.isDir()) {
                    throw invalid(
                            "Snapshot entry '%s' points to a directory. Expected a file.", path);
                }
                if (status.getLen() != file.getSize()) {
                    throw invalid(
                            "Snapshot metadata declares %d bytes for '%s', but the uploaded file is %d bytes.",
                            file.getSize(), path, status.getLen());
                }
            }
        } catch (IOException | UncheckedIOException e) {
            throw readFailure(path, e);
        }
        return size;
    }

    private static InvalidBulkLoadSnapshotException invalid(String format, Object... args) {
        return new InvalidBulkLoadSnapshotException(String.format(format, args));
    }

    private static ApiException readFailure(FsPath path, Exception cause) {
        if (cause instanceof FileNotFoundException || cause instanceof NoSuchFileException) {
            return new InvalidBulkLoadSnapshotException(
                    "Snapshot file '"
                            + path
                            + "' was not found. Check that the upload has finished before committing.",
                    cause);
        }
        if (cause instanceof EOFException) {
            return new InvalidBulkLoadSnapshotException(
                    "Snapshot file '" + path + "' ended before it could be read completely.",
                    cause);
        }
        return new StorageException("Could not access snapshot storage at '" + path + "'.", cause);
    }
}

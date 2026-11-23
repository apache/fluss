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

import org.apache.fluss.config.TableConfig;
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
            TableConfig tableConfig,
            BulkLoadTarget target,
            TableBucket bucket,
            BulkLoadBucket allocation) {
        try {
            PhysicalTablePath physical = PhysicalTablePath.of(tablePath, target.partitionName);
            FsPath tabletDir =
                    FlussPaths.remoteKvTabletDir(
                            FlussPaths.remoteKvDir(target.remoteDataDir), physical, bucket);
            FsPath directory = FlussPaths.remoteKvSnapshotDir(tabletDir, allocation.snapshotId);
            byte[] metadata =
                    CompletedSnapshotHandle.readMetadataBytes(
                            CompletedSnapshot.getMetadataFilePath(directory));
            CompletedSnapshot snapshot;
            try {
                snapshot = CompletedSnapshotJsonSerde.fromJson(metadata);
            } catch (RuntimeException e) {
                throw new InvalidBulkLoadSnapshotException(
                        "Invalid uploaded snapshot metadata.", e);
            }
            require(
                    snapshot.getTableBucket().equals(bucket),
                    "Snapshot physical identity differs.");
            require(snapshot.getSnapshotID() == allocation.snapshotId, "Snapshot ID differs.");
            require(
                    snapshot.getLogOffset() == allocation.logOffset,
                    "Snapshot recovery offset differs.");
            require(snapshot.getSnapshotLocation().equals(directory), "Snapshot location differs.");
            require(snapshot.getFirstAutoIncIDRange() == null, "Unexpected auto-increment state.");
            require(
                    (snapshot.getRowCount() != null) == tableConfig.supportsExactRowCount(),
                    "Snapshot row count does not match the table changelog configuration.");
            require(
                    snapshot.getRowCount() == null || snapshot.getRowCount() >= 0,
                    "Negative snapshot row count.");
            KvSnapshotHandle files = snapshot.getKvSnapshotHandle();
            require(
                    files.getSharedKvFileHandles().isEmpty(),
                    "BulkLoad snapshots must not reference shared files.");
            long privateSize = validateFiles(files.getPrivateFileHandles(), directory);
            require(
                    files.getIncrementalSize() == privateSize,
                    "Snapshot incremental size differs.");
            return snapshot;
        } catch (FileNotFoundException | NoSuchFileException | EOFException e) {
            throw new InvalidBulkLoadSnapshotException(
                    "Snapshot file is missing or incomplete.", e);
        } catch (IOException | UncheckedIOException e) {
            throw new StorageException("Could not read the uploaded snapshot.", e);
        }
    }

    private static long validateFiles(List<KvFileHandleAndLocalPath> files, FsPath ownerDirectory)
            throws IOException {
        String prefix = ownerDirectory + "/";
        FileSystem fileSystem = ownerDirectory.getFileSystem();
        Set<String> local = new HashSet<>();
        Set<String> remote = new HashSet<>();
        long size = 0;
        for (KvFileHandleAndLocalPath entry : files) {
            KvFileHandle file = entry.getKvFileHandle();
            FsPath filePath;
            try {
                require(entry.hasSafeLocalPath(), "Unsafe snapshot local path.");
                filePath = new FsPath(file.getFilePath());
                size = Math.addExact(size, file.getSize());
            } catch (IllegalArgumentException | ArithmeticException e) {
                throw new InvalidBulkLoadSnapshotException("Invalid snapshot file descriptor.", e);
            }
            require(
                    file.getFilePath().equals(filePath.toString()),
                    "Noncanonical snapshot file path.");
            require(
                    local.add(entry.getLocalPath()) && remote.add(file.getFilePath()),
                    "Duplicate snapshot file.");
            require(
                    file.getFilePath().startsWith(prefix),
                    "Snapshot file escapes its owning directory.");
            FileStatus fileStatus = fileSystem.getFileStatus(filePath);
            require(
                    !fileStatus.isDir() && fileStatus.getLen() == file.getSize(),
                    "Snapshot file length differs.");
        }
        return size;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new InvalidBulkLoadSnapshotException(message);
        }
    }
}

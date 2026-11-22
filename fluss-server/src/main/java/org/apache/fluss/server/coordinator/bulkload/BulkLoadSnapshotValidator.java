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

import org.apache.fluss.exception.InvalidBulkLoadSnapshotException;
import org.apache.fluss.exception.StorageException;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadBucket;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.utils.FlussPaths;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Validates one uploaded ordinary snapshot without scanning another bucket's input. */
final class BulkLoadSnapshotValidator {
    private BulkLoadSnapshotValidator() {}

    static CompletedSnapshot validate(
            TableInfo table, BulkLoadTarget target, TableBucket bucket, BulkLoadBucket allocation) {
        try {
            return validateInput(table, target, bucket, allocation);
        } catch (FileNotFoundException | NoSuchFileException | EOFException e) {
            throw new InvalidBulkLoadSnapshotException(
                    "Snapshot file is missing or incomplete.", e);
        } catch (IOException e) {
            throw new StorageException("Could not read the uploaded snapshot.", e);
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw new InvalidBulkLoadSnapshotException("Invalid uploaded snapshot metadata.", e);
        }
    }

    private static CompletedSnapshot validateInput(
            TableInfo table, BulkLoadTarget target, TableBucket bucket, BulkLoadBucket allocation)
            throws IOException {
        PhysicalTablePath physical =
                target.partitionName == null
                        ? PhysicalTablePath.of(table.getTablePath())
                        : PhysicalTablePath.of(table.getTablePath(), target.partitionName);
        FsPath tabletDir =
                FlussPaths.remoteKvTabletDir(
                        new FsPath(target.remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME),
                        physical,
                        bucket);
        FsPath directory = FlussPaths.remoteKvSnapshotDir(tabletDir, allocation.snapshotId);
        FsPath path = CompletedSnapshot.getMetadataFilePath(directory);
        FileSystem fileSystem = path.getFileSystem();
        FileStatus status = fileSystem.getFileStatus(path);
        require(
                !status.isDir() && status.getLen() > 0 && status.getLen() <= Integer.MAX_VALUE,
                "Invalid snapshot metadata file.");
        byte[] bytes = new byte[(int) status.getLen()];
        try (DataInputStream input = new DataInputStream(fileSystem.open(path))) {
            input.readFully(bytes);
            require(input.read() == -1, "Snapshot metadata changed while reading.");
        }
        KvSnapshotFileMetadata metadata;
        try {
            metadata = KvSnapshotFileMetadataJsonSerde.fromJson(bytes);
        } catch (RuntimeException e) {
            throw new InvalidBulkLoadSnapshotException("Invalid uploaded snapshot metadata.", e);
        }
        require(metadata.getTableBucket().equals(bucket), "Snapshot physical identity differs.");
        require(metadata.getSnapshotId() == allocation.snapshotId, "Snapshot ID differs.");
        require(
                metadata.getLogOffset() == allocation.logOffset,
                "Snapshot recovery offset differs.");
        require(
                new FsPath(metadata.getSnapshotLocation()).equals(directory),
                "Snapshot location differs.");
        require(
                metadata.getAutoIncrementRanges() == null
                        || metadata.getAutoIncrementRanges().isEmpty(),
                "Unexpected auto-increment state.");
        require(
                table.getTableConfig().getChangelogImage() != ChangelogImage.WAL
                                && !table.getTableConfig().getKvTTL().isPresent()
                        ? metadata.getRowCount() != null
                        : metadata.getRowCount() == null,
                "Snapshot row count does not match the table changelog configuration.");
        require(
                metadata.getRowCount() == null || metadata.getRowCount() >= 0,
                "Negative snapshot row count.");
        require(
                metadata.getSharedFiles().isEmpty(),
                "BulkLoad snapshots must not reference shared files.");
        long privateSize = validateFiles(metadata.getPrivateFiles(), directory);
        require(metadata.getIncrementalSize() == privateSize, "Snapshot incremental size differs.");
        return CompletedSnapshotJsonSerde.toCompletedSnapshot(metadata);
    }

    private static long validateFiles(
            List<KvSnapshotFileMetadata.FileHandle> files, FsPath ownerDirectory)
            throws IOException {
        Set<String> local = new HashSet<>();
        Set<String> remote = new HashSet<>();
        long size = 0;
        for (KvSnapshotFileMetadata.FileHandle file : files) {
            String name = file.getLocalPath();
            require(
                    name != null
                            && !name.isEmpty()
                            && !name.startsWith("/")
                            && !name.contains("\\"),
                    "Unsafe snapshot local path.");
            for (String segment : name.split("/", -1)) {
                require(
                        !segment.isEmpty() && !segment.equals(".") && !segment.equals(".."),
                        "Unsafe snapshot local path.");
            }
            FsPath filePath = new FsPath(file.getPath());
            require(file.getPath().equals(filePath.toString()), "Noncanonical snapshot file path.");
            require(local.add(name) && remote.add(file.getPath()), "Duplicate snapshot file.");
            requireWithin(filePath, ownerDirectory);
            FileStatus fileStatus = filePath.getFileSystem().getFileStatus(filePath);
            require(
                    !fileStatus.isDir() && fileStatus.getLen() == file.getSize(),
                    "Snapshot file length differs.");
            size = Math.addExact(size, file.getSize());
        }
        return size;
    }

    private static void requireWithin(FsPath file, FsPath parent) {
        URI child = file.toUri().normalize();
        URI root = parent.toUri().normalize();
        String prefix = root.getPath().endsWith("/") ? root.getPath() : root.getPath() + "/";
        require(
                Objects.equals(child.getScheme(), root.getScheme())
                        && Objects.equals(child.getAuthority(), root.getAuthority())
                        && child.getPath().startsWith(prefix),
                "Snapshot file escapes its owning directory.");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new InvalidBulkLoadSnapshotException(message);
        }
    }
}

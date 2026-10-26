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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.InvalidBulkLoadRequestException;
import org.apache.fluss.fs.FSDataInputStream;
import org.apache.fluss.fs.FileStatus;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.KvSnapshotFileMetadata;
import org.apache.fluss.metadata.KvSnapshotFileMetadataJsonSerde;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import org.apache.fluss.shaded.guava32.com.google.common.hash.Hasher;
import org.apache.fluss.shaded.guava32.com.google.common.hash.Hashing;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Validates the complete set of standard snapshots before the durable Commit decision. */
@Internal
public final class BulkLoadSnapshotValidator {

    /** Reads every planned snapshot and verifies the SHA-256 of its ordered metadata digests. */
    public List<CompletedSnapshot> validate(
            BulkLoadHandle handle,
            String remoteDataDir,
            String inputSha256,
            long[] snapshotIds,
            ChangelogImage changelogImage,
            long maxMetadataBytes) {
        try {
            Hasher digest = Hashing.sha256().newHasher();
            List<CompletedSnapshot> snapshots = new ArrayList<>(snapshotIds.length);
            for (int bucket = 0; bucket < snapshotIds.length; bucket++) {
                snapshots.add(
                        validateBucket(
                                handle,
                                remoteDataDir,
                                snapshotIds[bucket],
                                bucket,
                                digest,
                                changelogImage,
                                maxMetadataBytes));
            }
            require(
                    digest.hash().toString().equals(inputSha256),
                    "BulkLoad input SHA-256 differs from the frozen input.");
            return Collections.unmodifiableList(snapshots);
        } catch (InvalidBulkLoadRequestException e) {
            throw e;
        } catch (Exception e) {
            throw invalid("Invalid BulkLoad snapshot metadata.", e);
        }
    }

    private CompletedSnapshot validateBucket(
            BulkLoadHandle handle,
            String remoteDataDir,
            long snapshotId,
            int bucketId,
            Hasher digest,
            ChangelogImage changelogImage,
            long maxMetadataBytes)
            throws Exception {
        TableBucket tableBucket =
                new TableBucket(handle.getTableId(), handle.getPartitionId(), bucketId);
        FsPath snapshotDirectory =
                FlussPaths.remoteKvSnapshotDir(
                        FlussPaths.remoteKvTabletDir(
                                new FsPath(remoteDataDir, FlussPaths.REMOTE_KV_DIR_NAME),
                                handle.getTarget(),
                                tableBucket),
                        snapshotId);
        FsPath snapshotMetadataPath = new FsPath(snapshotDirectory, "_METADATA");
        byte[] snapshotBytes = readMetadata(snapshotMetadataPath, maxMetadataBytes);
        digest.putBytes(Hashing.sha256().hashBytes(snapshotBytes).asBytes());
        KvSnapshotFileMetadata snapshot = KvSnapshotFileMetadataJsonSerde.fromJson(snapshotBytes);
        require(snapshot.getTableBucket().equals(tableBucket), "Snapshot physical target differs.");
        require(snapshot.getSnapshotId() == snapshotId, "Snapshot ID differs.");
        require(
                snapshot.getSnapshotLocation().equals(snapshotDirectory.toString()),
                "Snapshot location differs from the standard path.");
        require(
                snapshot.getSharedFiles().isEmpty(),
                "BulkLoad Snapshot must not use shared files.");
        require(
                snapshot.getAutoIncrementRanges() == null
                        || snapshot.getAutoIncrementRanges().isEmpty(),
                "BulkLoad Snapshot must not contain auto-increment ranges.");
        require(snapshot.getLogOffset() >= 0, "Snapshot log offset must be non-negative.");
        require(
                changelogImage == ChangelogImage.FULL
                        ? snapshot.getRowCount() != null
                        : snapshot.getRowCount() == null,
                "Snapshot row-count shape differs from the changelog image.");
        long privateBytes = 0L;
        Set<String> localPaths = new HashSet<>();
        Set<String> remotePaths = new HashSet<>();
        for (KvSnapshotFileMetadata.FileHandle file : snapshot.getPrivateFiles()) {
            requireSafeLocalPath(file.getLocalPath());
            require(localPaths.add(file.getLocalPath()), "Duplicate Snapshot local path.");
            require(remotePaths.add(file.getPath()), "Duplicate Snapshot remote path.");
            FsPath path = new FsPath(file.getPath());
            requireWithin(path, snapshotDirectory, "Snapshot file");
            requireFileLength(path, file.getSize(), "Snapshot file");
            privateBytes = Math.addExact(privateBytes, file.getSize());
        }
        require(
                snapshot.getIncrementalSize() == privateBytes,
                "Snapshot incremental size differs from its private files.");

        return CompletedSnapshotJsonSerde.toCompletedSnapshot(snapshot);
    }

    private static byte[] readMetadata(FsPath path, long maxMetadataBytes) throws IOException {
        FileStatus status = path.getFileSystem().getFileStatus(path);
        require(!status.isDir(), "Snapshot metadata must be a file.");
        long expectedLength = status.getLen();
        require(expectedLength <= maxMetadataBytes, "Snapshot metadata exceeds its byte limit.");
        require(
                expectedLength > 0 && expectedLength <= Integer.MAX_VALUE,
                "Metadata length is invalid.");
        byte[] bytes = new byte[(int) expectedLength];
        try (FSDataInputStream input = path.getFileSystem().open(path)) {
            IOUtils.readFully(input, bytes);
            if (input.read() != -1) {
                throw new IOException("Metadata file grew while reading " + path);
            }
        }
        return bytes;
    }

    private static void requireFileLength(FsPath path, long expected, String name)
            throws IOException {
        FileStatus status = path.getFileSystem().getFileStatus(path);
        require(!status.isDir(), name + " must be a file.");
        require(status.getLen() == expected, name + " length differs.");
    }

    private static void requireWithin(FsPath candidate, FsPath parent, String name) {
        URI child = candidate.toUri().normalize();
        URI root = parent.toUri().normalize();
        String rootPath = root.getPath().endsWith("/") ? root.getPath() : root.getPath() + '/';
        require(
                Objects.equals(child.getScheme(), root.getScheme())
                        && Objects.equals(child.getAuthority(), root.getAuthority())
                        && child.getPath().startsWith(rootPath),
                name + " escapes its planned directory.");
    }

    private static void requireSafeLocalPath(String localPath) {
        require(localPath != null && !localPath.isEmpty(), "Snapshot local path is empty.");
        require(!localPath.contains("\\"), "Snapshot local path is unsafe.");
        String[] segments = localPath.split("/", -1);
        for (String segment : segments) {
            require(
                    !segment.isEmpty() && !segment.equals(".") && !segment.equals(".."),
                    "Snapshot local path is unsafe.");
        }
        Path normalized = Paths.get(localPath).normalize();
        require(
                !normalized.isAbsolute()
                        && normalized.toString().replace('\\', '/').equals(localPath),
                "Snapshot local path is unsafe.");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw invalid(message, null);
        }
    }

    private static InvalidBulkLoadRequestException invalid(String message, Throwable cause) {
        return cause == null
                ? new InvalidBulkLoadRequestException(message)
                : new InvalidBulkLoadRequestException(message, cause);
    }
}

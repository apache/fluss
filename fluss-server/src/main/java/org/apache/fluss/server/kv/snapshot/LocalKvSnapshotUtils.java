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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.utils.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/** Utilities for retaining, validating, and restoring local KV snapshot checkpoints. */
public final class LocalKvSnapshotUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LocalKvSnapshotUtils.class);

    private static final String SNAPSHOT_DIRECTORY_PREFIX = "snap-";
    private static final String RESTORE_DIRECTORY_PREFIX = ".db-restore-";

    private LocalKvSnapshotUtils() {}

    /** Returns the local checkpoint directory for the given snapshot. */
    public static File getSnapshotDirectory(File kvTabletDir, long snapshotId) {
        return new File(kvTabletDir, SNAPSHOT_DIRECTORY_PREFIX + snapshotId);
    }

    /**
     * Rebuilds the active RocksDB directory from a matching local checkpoint.
     *
     * <p>The checkpoint is accepted only when every file referenced by the committed remote
     * snapshot metadata exists locally with the expected size and there are no unexpected files.
     * SST files are hard-linked into a temporary RocksDB directory when possible; mutable metadata
     * files are copied. The retained checkpoint itself is left untouched so another local restart
     * can use it before a newer snapshot completes.
     *
     * @return whether the active RocksDB directory was rebuilt from the local checkpoint
     */
    public static boolean restore(File kvTabletDir, CompletedSnapshot completedSnapshot) {
        long snapshotId = completedSnapshot.getSnapshotID();
        File snapshotDirectory = getSnapshotDirectory(kvTabletDir, snapshotId);
        Map<String, Long> expectedFiles = getExpectedFiles(completedSnapshot);
        if (!isValid(snapshotDirectory.toPath(), expectedFiles)) {
            if (snapshotDirectory.exists()) {
                LOG.warn(
                        "Retained local KV snapshot {} does not match committed snapshot metadata. "
                                + "Falling back to remote snapshot recovery.",
                        snapshotDirectory);
            }
            return false;
        }

        Path restoreDirectory = kvTabletDir.toPath().resolve(RESTORE_DIRECTORY_PREFIX + snapshotId);
        Path activeDbDirectory = RocksDBKvBuilder.getInstanceRocksDBPath(kvTabletDir).toPath();
        try {
            FileUtils.deleteDirectory(restoreDirectory.toFile());
            Files.createDirectories(restoreDirectory);
            for (String fileName : expectedFiles.keySet()) {
                Path source = snapshotDirectory.toPath().resolve(fileName);
                Path target = restoreDirectory.resolve(fileName);
                copySnapshotFile(source, target);
            }

            FileUtils.deleteDirectory(activeDbDirectory.toFile());
            FileUtils.atomicMoveWithFallback(restoreDirectory, activeDbDirectory);
            retainOnly(kvTabletDir, snapshotId);
            return true;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to rebuild local KV directory {} from snapshot {}. "
                            + "Falling back to remote snapshot recovery.",
                    activeDbDirectory,
                    snapshotId,
                    e);
            FileUtils.deleteDirectoryQuietly(restoreDirectory.toFile());
            return false;
        }
    }

    static void retainOnly(File kvTabletDir, long snapshotId) {
        File retainedSnapshot = getSnapshotDirectory(kvTabletDir, snapshotId);
        for (File snapshotDirectory : FileUtils.listDirectories(kvTabletDir)) {
            if (isSnapshotDirectory(snapshotDirectory)
                    && !snapshotDirectory.equals(retainedSnapshot)) {
                deleteQuietly(snapshotDirectory);
            }
        }
    }

    static void discard(File kvTabletDir, long snapshotId) {
        deleteQuietly(getSnapshotDirectory(kvTabletDir, snapshotId));
    }

    private static Map<String, Long> getExpectedFiles(CompletedSnapshot completedSnapshot) {
        Map<String, Long> expectedFiles = new HashMap<>();
        KvSnapshotHandle snapshotHandle = completedSnapshot.getKvSnapshotHandle();
        for (KvFileHandleAndLocalPath file : snapshotHandle.getSharedKvFileHandles()) {
            addExpectedFile(expectedFiles, file);
        }
        for (KvFileHandleAndLocalPath file : snapshotHandle.getPrivateFileHandles()) {
            addExpectedFile(expectedFiles, file);
        }
        return expectedFiles;
    }

    private static void addExpectedFile(
            Map<String, Long> expectedFiles, KvFileHandleAndLocalPath file) {
        String localPath = file.getLocalPath();
        Long previous = expectedFiles.put(localPath, file.getKvFileHandle().getSize());
        if (previous != null) {
            // Duplicate paths cannot describe an unambiguous local checkpoint.
            expectedFiles.put(localPath, -1L);
        }
    }

    private static boolean isValid(Path snapshotDirectory, Map<String, Long> expectedFiles) {
        if (!Files.isDirectory(snapshotDirectory, LinkOption.NOFOLLOW_LINKS)
                || expectedFiles.isEmpty()) {
            return false;
        }

        try {
            Path[] actualFiles = FileUtils.listDirectory(snapshotDirectory);
            if (actualFiles.length != expectedFiles.size()) {
                return false;
            }

            for (Map.Entry<String, Long> expectedFile : expectedFiles.entrySet()) {
                Path relativePath = Paths.get(expectedFile.getKey()).normalize();
                if (relativePath.isAbsolute()
                        || relativePath.getNameCount() != 1
                        || !expectedFile.getKey().equals(relativePath.toString())
                        || expectedFile.getValue() < 0) {
                    return false;
                }

                Path localFile = snapshotDirectory.resolve(relativePath).normalize();
                if (!localFile.getParent().equals(snapshotDirectory.normalize())
                        || !Files.isRegularFile(localFile, LinkOption.NOFOLLOW_LINKS)
                        || Files.size(localFile) != expectedFile.getValue()) {
                    return false;
                }
            }

            for (Path actualFile : actualFiles) {
                if (!expectedFiles.containsKey(actualFile.getFileName().toString())) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            LOG.warn("Failed to validate local KV snapshot directory {}.", snapshotDirectory, e);
            return false;
        }
    }

    private static void copySnapshotFile(Path source, Path target) throws IOException {
        if (source.getFileName().toString().endsWith(RocksIncrementalSnapshot.SST_FILE_SUFFIX)) {
            try {
                Files.createLink(target, source);
                return;
            } catch (UnsupportedOperationException | IOException linkException) {
                try {
                    Files.copy(source, target);
                    return;
                } catch (IOException copyException) {
                    copyException.addSuppressed(linkException);
                    throw copyException;
                }
            }
        }
        Files.copy(source, target);
    }

    private static boolean isSnapshotDirectory(File directory) {
        return directory.getName().startsWith(SNAPSHOT_DIRECTORY_PREFIX);
    }

    private static void deleteQuietly(File snapshotDirectory) {
        try {
            FileUtils.deleteDirectory(snapshotDirectory);
        } catch (IOException e) {
            LOG.warn("Could not properly clean local KV snapshot {}.", snapshotDirectory, e);
        }
    }
}

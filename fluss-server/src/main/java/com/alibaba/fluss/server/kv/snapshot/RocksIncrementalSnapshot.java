/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.server.utils.ResourceGuard;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.FileUtils;

import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Implementation for snapshot operation based on RocksDB's native snapshots and creates incremental
 * snapshots.
 */
public class RocksIncrementalSnapshot implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksIncrementalSnapshot.class);

    /** File suffix of sstable files. */
    public static final String SST_FILE_SUFFIX = ".sst";

    /** RocksDB instance from the backend. */
    @Nonnull protected RocksDB db;

    /** Resource guard for the RocksDB instance. */
    @Nonnull protected final ResourceGuard rocksDBResourceGuard;

    /** Base path of the RocksDB instance. */
    @Nonnull protected final File instanceBasePath;

    /** The identifier of the last completed snapshot. */
    private long lastCompletedSnapshotId;

    /**
     * Stores the {@link KvFileHandle} and corresponding local path of uploaded SST files that build
     * the incremental history. Once the snapshot is confirmed, they can be reused for incremental
     * snapshot.
     */
    private final Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles;

    /** The help class used to upload kv snapshot files. */
    private final KvSnapshotDataUploader kvSnapshotDataUploader;

    public RocksIncrementalSnapshot(
            Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles,
            @Nonnull RocksDB db,
            ResourceGuard rocksDBResourceGuard,
            KvSnapshotDataUploader kvSnapshotDataUploader,
            @Nonnull File instanceBasePath,
            long lastCompletedSnapshotId) {
        this.uploadedSstFiles = uploadedSstFiles;
        this.db = db;
        this.rocksDBResourceGuard = rocksDBResourceGuard;
        this.kvSnapshotDataUploader = kvSnapshotDataUploader;
        this.instanceBasePath = instanceBasePath;
        this.lastCompletedSnapshotId = lastCompletedSnapshotId;
    }

    public SnapshotResultSupplier asyncSnapshot(
            NativeRocksDBSnapshotResources snapshotResources,
            long snapshotId,
            long logOffset,
            @Nonnull SnapshotLocation snapshotLocation) {
        return new RocksDBIncrementalSnapshotOperation(
                snapshotId,
                logOffset,
                snapshotLocation,
                snapshotResources.previousSnapshot,
                snapshotResources.snapshotDirectory);
    }

    public void notifySnapshotComplete(long completedSnapshotId) {
        synchronized (uploadedSstFiles) {
            uploadedSstFiles.keySet().removeIf(snapshotId -> snapshotId < completedSnapshotId);
            lastCompletedSnapshotId = completedSnapshotId;
        }
    }

    public void notifySnapshotAbort(long abortedSnapshotId) {
        synchronized (uploadedSstFiles) {
            uploadedSstFiles.remove(abortedSnapshotId);
        }
    }

    @Override
    public void close() throws Exception {
        // do nothing now
    }

    public NativeRocksDBSnapshotResources syncPrepareResources(long snapshotId) throws Exception {
        File snapshotDirectory = prepareLocalSnapshotDirectory(snapshotId);
        LOG.trace("Local RocksDB snapshot goes to backup path {}.", snapshotDirectory);

        PreviousSnapshot previousSnapshot = getPreviousSnapshot(snapshotId);

        takeDBNativeSnapshot(snapshotDirectory);

        return new NativeRocksDBSnapshotResources(snapshotDirectory, previousSnapshot);
    }

    private File prepareLocalSnapshotDirectory(long snapshotId) {
        return new File(instanceBasePath, "snap-" + snapshotId);
    }

    private PreviousSnapshot getPreviousSnapshot(long snapshotId) {
        final Collection<KvFileHandleAndLocalPath> confirmedSstFiles;
        final long lastCompletedSnapshot;
        // use the last completed snapshot as the comparison base.
        synchronized (uploadedSstFiles) {
            lastCompletedSnapshot = lastCompletedSnapshotId;
            confirmedSstFiles = uploadedSstFiles.get(lastCompletedSnapshot);
            LOG.trace("Use confirmed SST files for snapshot {}: {}", snapshotId, confirmedSstFiles);
        }

        LOG.trace(
                "Taking incremental snapshot for snapshot {}. Snapshot is based on last completed snapshot {} "
                        + "assuming the following (shared) confirmed files as base: {}.",
                snapshotId,
                lastCompletedSnapshot,
                uploadedSstFiles);
        return new PreviousSnapshot(confirmedSstFiles);
    }

    private void takeDBNativeSnapshot(@Nonnull File outputDirectory) throws Exception {
        // create hard links of living files in the output path
        try (ResourceGuard.Lease ignored = rocksDBResourceGuard.acquireResource();
                Checkpoint snapshot = Checkpoint.create(db)) {
            snapshot.createCheckpoint(outputDirectory.toString());
        } catch (Exception ex) {
            Exception exception = ex;
            try {
                FileUtils.deleteDirectory(outputDirectory);
            } catch (IOException cleanupEx) {
                exception = ExceptionUtils.firstOrSuppressed(cleanupEx, exception);
            }
            throw exception;
        }
    }

    /** Encapsulates the process to perform an incremental snapshot of RocksDB. */
    private final class RocksDBIncrementalSnapshotOperation implements SnapshotResultSupplier {

        /** All sst files that were part of the last previously completed snapshot. */
        @Nonnull private final PreviousSnapshot previousSnapshot;

        private final File localSnapshotDirectory;

        private final long snapshotId;
        private final long logOffset;

        /** The target snapshot location and factory that creates the output streams to DFS. */
        @Nonnull private final SnapshotLocation snapshotLocation;

        @Nonnull private final CloseableRegistry tmpResourcesRegistry;

        public RocksDBIncrementalSnapshotOperation(
                long snapshotId,
                long logOffset,
                @Nonnull SnapshotLocation snapshotLocation,
                PreviousSnapshot previousSnapshot,
                File localSnapshotDirectory) {
            this.snapshotId = snapshotId;
            this.logOffset = logOffset;
            this.snapshotLocation = snapshotLocation;
            this.previousSnapshot = previousSnapshot;
            this.localSnapshotDirectory = localSnapshotDirectory;
            this.tmpResourcesRegistry = new CloseableRegistry();
        }

        @Override
        public SnapshotResult get(CloseableRegistry snapshotCloseableRegistry) throws Exception {

            boolean completed = false;

            // Handles to new sst files since the last completed snapshot will go here
            final List<KvFileHandleAndLocalPath> sstFiles = new ArrayList<>();
            // Handles to the misc files in the current snapshot will go here
            final List<KvFileHandleAndLocalPath> miscFiles = new ArrayList<>();
            try {
                long snapshotIncrementalSize =
                        uploadSnapshotFiles(sstFiles, miscFiles, snapshotCloseableRegistry);
                completed = true;
                // We make the 'sstFiles' as the 'shared' in KvSnapshotHandle,
                final KvSnapshotHandle kvSnapshotHandle =
                        new KvSnapshotHandle(sstFiles, miscFiles, snapshotIncrementalSize);
                return new SnapshotResult(
                        kvSnapshotHandle, snapshotLocation.getSnapshotDirectory(), logOffset);
            } finally {
                if (!completed) {
                    cleanupIncompleteSnapshot(tmpResourcesRegistry);
                }
            }
        }

        private long uploadSnapshotFiles(
                @Nonnull List<KvFileHandleAndLocalPath> sstFiles,
                @Nonnull List<KvFileHandleAndLocalPath> miscFiles,
                CloseableRegistry closeableRegistry)
                throws Exception {
            checkState(localSnapshotDirectory.exists());

            Path[] files = FileUtils.listDirectory(localSnapshotDirectory.toPath());
            List<Path> sstFilePaths = new ArrayList<>(files.length);
            List<Path> miscFilePaths = new ArrayList<>(files.length);

            createUploadFilePaths(files, sstFiles, sstFilePaths, miscFilePaths);

            long size = 0;

            // sst should be uploaded to share directory
            List<KvFileHandleAndLocalPath> sstFilesUploadResult =
                    kvSnapshotDataUploader.uploadFilesToSnapshotLocation(
                            sstFilePaths,
                            snapshotLocation,
                            SnapshotFileScope.SHARED,
                            closeableRegistry,
                            tmpResourcesRegistry);
            size +=
                    sstFilesUploadResult.stream()
                            .mapToLong(e -> e.getKvFileHandle().getSize())
                            .sum();

            sstFiles.addAll(sstFilesUploadResult);

            // others, should be uploaded to private directory
            List<KvFileHandleAndLocalPath> miscFilesUploadResult =
                    kvSnapshotDataUploader.uploadFilesToSnapshotLocation(
                            miscFilePaths,
                            snapshotLocation,
                            SnapshotFileScope.EXCLUSIVE,
                            closeableRegistry,
                            tmpResourcesRegistry);
            size +=
                    miscFilesUploadResult.stream()
                            .mapToLong(e -> e.getKvFileHandle().getSize())
                            .sum();
            miscFiles.addAll(miscFilesUploadResult);

            uploadedSstFiles.put(snapshotId, Collections.unmodifiableList(sstFiles));
            return size;
        }

        private void createUploadFilePaths(
                Path[] files,
                List<KvFileHandleAndLocalPath> sstFiles,
                List<Path> sstFilePaths,
                List<Path> miscFilePaths) {
            for (Path filePath : files) {
                final String fileName = filePath.getFileName().toString();
                if (fileName.endsWith(SST_FILE_SUFFIX)) {
                    Optional<KvFileHandle> uploaded = previousSnapshot.getUploaded(fileName);
                    if (uploaded.isPresent()) {
                        sstFiles.add(KvFileHandleAndLocalPath.of(uploaded.get(), fileName));
                    } else {
                        sstFilePaths.add(filePath); // re-upload
                    }
                } else {
                    miscFilePaths.add(filePath);
                }
            }
        }

        private void cleanupIncompleteSnapshot(@Nonnull CloseableRegistry tmpResourcesRegistry) {
            try {
                tmpResourcesRegistry.close();
            } catch (Exception e) {
                LOG.warn("Could not properly clean tmp resources.", e);
            }
        }
    }

    /** A {@link SnapshotResources} for native rocksdb snapshot. */
    public static class NativeRocksDBSnapshotResources implements SnapshotResources {

        @Nonnull protected final File snapshotDirectory;

        @Nonnull protected final PreviousSnapshot previousSnapshot;

        protected NativeRocksDBSnapshotResources(
                File snapshotDirectory, PreviousSnapshot previousSnapshot) {
            this.snapshotDirectory = snapshotDirectory;
            this.previousSnapshot = previousSnapshot;
        }

        @Override
        public void release() {
            try {
                if (snapshotDirectory.exists()) {
                    LOG.trace(
                            "Running cleanup for local RocksDB backup directory {}.",
                            snapshotDirectory);
                    FileUtils.deleteDirectory(snapshotDirectory);
                }
            } catch (IOException e) {
                LOG.warn("Could not properly cleanup local RocksDB backup directory.", e);
            }
        }
    }

    /** Previous snapshot with uploaded sst files. */
    protected static class PreviousSnapshot {

        @Nonnull private final Map<String, KvFileHandle> confirmedSstFiles;

        private PreviousSnapshot(@Nullable Collection<KvFileHandleAndLocalPath> confirmedSstFiles) {
            this.confirmedSstFiles =
                    confirmedSstFiles != null
                            ? confirmedSstFiles.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    KvFileHandleAndLocalPath::getLocalPath,
                                                    KvFileHandleAndLocalPath::getKvFileHandle))
                            : Collections.emptyMap();
        }

        private Optional<KvFileHandle> getUploaded(String fileName) {
            if (confirmedSstFiles.containsKey(fileName)) {
                KvFileHandle handle = confirmedSstFiles.get(fileName);
                return Optional.of(new PlaceholderKvFileHandler(handle));
            } else {
                return Optional.empty();
            }
        }
    }
}

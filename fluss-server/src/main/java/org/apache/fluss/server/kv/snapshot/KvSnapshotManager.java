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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.fs.FileSystemSafetyNet;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;
import org.apache.fluss.utils.MathUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.fluss.server.kv.snapshot.RocksIncrementalSnapshot.SST_FILE_SUFFIX;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * For a leader replica of PrimaryKey Table, it is a stateless snapshot manager which will trigger
 * upload kv snapshot periodically. It'll use a {@link ScheduledExecutorService} to schedule the
 * snapshot initialization and a {@link ExecutorService} to complete async phase of snapshot.
 *
 * <p>For a standby replica of PrimaryKey Table, it will trigger by the
 * NotifyKvSnapshotOffsetRequest to incremental download sst files for remote to keep the data up to
 * the latest kv snapshot.
 *
 * <p>For a follower replica of PrimaryKey Table, it will do nothing.
 */
public class KvSnapshotManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KvSnapshotManager.class);

    /** Number of consecutive snapshot failures. */
    private final AtomicInteger numberOfConsecutiveFailures;

    /** Whether upload snapshot is started. */
    private volatile boolean isLeader = false;

    private final long initialDelay;
    /** The table bucket that the snapshot manager is for. */
    private final TableBucket tableBucket;

    private final File tabletDir;

    private final SnapshotContext snapshotContext;
    private final Clock clock;

    /** The target on which the snapshot will be done. */
    private @Nullable UploadSnapshotTarget uploadSnapshotTarget;

    /**
     * The scheduled snapshot task.
     *
     * <p>Since all reads and writes of {@code scheduledTask} are protected by synchronized, the
     * volatile modifier is not necessary here.
     */
    private ScheduledFuture<?> scheduledTask = null;

    /** The sst files downloaded for standby replicas. */
    private @Nullable Set<Path> downloadedSstFiles;

    private @Nullable Set<Path> downloadedMiscFiles;
    private long standbySnapshotSize;

    protected KvSnapshotManager(
            TableBucket tableBucket, File tabletDir, SnapshotContext snapshotContext, Clock clock) {
        this.tableBucket = tableBucket;
        this.tabletDir = tabletDir;
        this.snapshotContext = snapshotContext;
        this.numberOfConsecutiveFailures = new AtomicInteger(0);
        this.initialDelay =
                snapshotContext.getSnapshotIntervalMs() > 0
                        ? MathUtils.murmurHash(tableBucket.hashCode())
                                % snapshotContext.getSnapshotIntervalMs()
                        : 0;
        this.clock = clock;
        this.uploadSnapshotTarget = null;
        this.downloadedSstFiles = null;
        this.downloadedMiscFiles = null;
        this.standbySnapshotSize = 0;
    }

    public static KvSnapshotManager create(
            TableBucket tableBucket, File tabletDir, SnapshotContext snapshotContext, Clock clock) {
        return new KvSnapshotManager(tableBucket, tabletDir, snapshotContext, clock);
    }

    public void becomeLeader() {
        isLeader = true;
        // Clear standby download cache when leaving standby role
        clearStandbyDownloadCache();

        // make db dir.
        Path kvDbPath = tabletDir.toPath().resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
        if (!kvDbPath.toFile().exists()) {
            kvDbPath.toFile().mkdirs();
        }
    }

    public void becomeFollower() {
        isLeader = false;
        // Clear standby download cache when leaving standby role
        clearStandbyDownloadCache();
    }

    public void becomeStandby() {
        isLeader = false;
        // Clear standby download cache when new added to standby role
        clearStandbyDownloadCache();

        // make db dir.
        Path kvDbPath = tabletDir.toPath().resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
        if (!kvDbPath.toFile().exists()) {
            kvDbPath.toFile().mkdirs();
        }
    }

    @VisibleForTesting
    public @Nullable Set<Path> getDownloadedSstFiles() {
        return downloadedSstFiles;
    }

    @VisibleForTesting
    public @Nullable Set<Path> getDownloadedMiscFiles() {
        return downloadedMiscFiles;
    }

    /**
     * Clear the standby download cache.
     *
     * <p>This method should be called when a replica leaves the standby role (becomes a regular
     * follower or leader). It clears the cached state of downloaded SST files, misc files, and
     * snapshot size. This ensures that if the replica becomes standby again later, it will perform
     * a fresh download based on the actual local files, rather than reusing stale cache that
     * references deleted files.
     */
    private void clearStandbyDownloadCache() {
        downloadedSstFiles = null;
        downloadedMiscFiles = null;
        standbySnapshotSize = 0;
        LOG.info(
                "Cleared standby download cache for table bucket {}, will reload from local files on next standby promotion",
                tableBucket);
    }

    /**
     * The guardedExecutor is an executor that uses to trigger upload snapshot.
     *
     * <p>It's expected to be passed with a guarded executor to prevent any concurrent modification
     * to KvTablet during trigger snapshotting.
     */
    public void startPeriodicUploadSnapshot(
            Executor guardedExecutor, UploadSnapshotTarget uploadSnapshotTarget) {
        this.uploadSnapshotTarget = uploadSnapshotTarget;

        // disable periodic snapshot when periodicMaterializeDelay is not positive
        if (snapshotContext.getSnapshotIntervalMs() > 0) {
            LOG.info("TableBucket {} starts periodic snapshot", tableBucket);
            scheduleNextSnapshot(initialDelay, guardedExecutor);
        }
    }

    public void downloadSnapshot(long snapshotId) throws Exception {
        CompletedSnapshot completedSnapshot =
                snapshotContext.getCompletedSnapshotProvider(tableBucket, snapshotId);
        incrementalDownloadSnapshot(completedSnapshot);
        standbySnapshotSize = completedSnapshot.getSnapshotSize();
    }

    /**
     * download the latest snapshot.
     *
     * <p>For a standby replica, it will download the latest snapshot to keep the data up to the
     * latest kv snapshot.
     *
     * @return the latest snapshot
     */
    public Optional<CompletedSnapshot> downloadLatestSnapshot() throws Exception {
        // download the latest snapshot.
        Optional<CompletedSnapshot> latestSnapshot = getLatestSnapshot();
        if (latestSnapshot.isPresent()) {
            CompletedSnapshot completedSnapshot = latestSnapshot.get();
            incrementalDownloadSnapshot(completedSnapshot);
            standbySnapshotSize = completedSnapshot.getSnapshotSize();
        }
        return latestSnapshot;
    }

    private void incrementalDownloadSnapshot(CompletedSnapshot completedSnapshot) throws Exception {
        if (downloadedSstFiles == null || downloadedMiscFiles == null) {
            // first try to load all ready exists sst files.
            downloadedSstFiles = new HashSet<>();
            downloadedMiscFiles = new HashSet<>();
            loadKvLocalFiles(downloadedSstFiles, downloadedMiscFiles);
        }

        Set<Path> sstFilesToDelete = new HashSet<>();
        KvSnapshotHandle incrementalKvSnapshotHandle =
                getIncrementalKvSnapshotHandle(
                        completedSnapshot, downloadedSstFiles, sstFilesToDelete);
        CompletedSnapshot incrementalSnapshot =
                completedSnapshot.getIncrementalSnapshot(incrementalKvSnapshotHandle);

        // Use atomic download: download to temp dir first, then atomically move to final location
        atomicDownloadSnapshot(incrementalSnapshot, sstFilesToDelete);

        KvSnapshotHandle kvSnapshotHandle = completedSnapshot.getKvSnapshotHandle();
        downloadedSstFiles =
                kvSnapshotHandle.getSharedKvFileHandles().stream()
                        .map(handler -> Paths.get(handler.getLocalPath()))
                        .collect(Collectors.toSet());
        downloadedMiscFiles =
                kvSnapshotHandle.getPrivateFileHandles().stream()
                        .map(handler -> Paths.get(handler.getLocalPath()))
                        .collect(Collectors.toSet());
    }

    public long getSnapshotSize() {
        if (uploadSnapshotTarget != null) {
            return uploadSnapshotTarget.getSnapshotSize();
        } else {
            return 0L;
        }
    }

    public long getStandbySnapshotSize() {
        return standbySnapshotSize;
    }

    public File getTabletDir() {
        return tabletDir;
    }

    public Optional<CompletedSnapshot> getLatestSnapshot() {
        try {
            return Optional.ofNullable(
                    snapshotContext.getLatestCompletedSnapshotProvider().apply(tableBucket));
        } catch (Exception e) {
            LOG.warn("Get latest completed snapshot for {}  failed.", tableBucket, e);
        }
        return Optional.empty();
    }

    // schedule thread and asyncOperationsThreadPool can access this method
    private synchronized void scheduleNextSnapshot(long delay, Executor guardedExecutor) {
        ScheduledExecutorService snapshotScheduler = snapshotContext.getSnapshotScheduler();
        if (isLeader && !snapshotScheduler.isShutdown()) {
            LOG.debug(
                    "TableBucket {} schedules the next snapshot in {} seconds",
                    tableBucket,
                    delay / 1000);
            scheduledTask =
                    snapshotScheduler.schedule(
                            () -> triggerUploadSnapshot(guardedExecutor),
                            delay,
                            TimeUnit.MILLISECONDS);
        }
    }

    @VisibleForTesting
    public long currentSnapshotId() {
        return uploadSnapshotTarget.currentSnapshotId();
    }

    /** Trigger upload local snapshot to remote storage. */
    public void triggerUploadSnapshot(Executor guardedExecutor) {
        // todo: consider shrink the scope
        // of using guardedExecutor
        guardedExecutor.execute(
                () -> {
                    if (isLeader) {
                        LOG.debug("TableBucket {} triggers snapshot.", tableBucket);
                        long triggerTime = System.currentTimeMillis();

                        Optional<SnapshotRunnable> snapshotRunnableOptional;
                        try {
                            checkNotNull(uploadSnapshotTarget);
                            snapshotRunnableOptional = uploadSnapshotTarget.initSnapshot();
                        } catch (Exception e) {
                            LOG.error("Fail to init snapshot during triggering snapshot.", e);
                            return;
                        }
                        if (snapshotRunnableOptional.isPresent()) {
                            SnapshotRunnable runnable = snapshotRunnableOptional.get();
                            snapshotContext
                                    .getAsyncOperationsThreadPool()
                                    .execute(
                                            () ->
                                                    asyncUploadSnapshotPhase(
                                                            triggerTime,
                                                            runnable.getSnapshotId(),
                                                            runnable.getCoordinatorEpoch(),
                                                            runnable.getBucketLeaderEpoch(),
                                                            runnable.getSnapshotLocation(),
                                                            runnable.getSnapshotRunnable(),
                                                            guardedExecutor));
                        } else {
                            scheduleNextSnapshot(guardedExecutor);
                            LOG.debug(
                                    "TableBucket {} has no data updates since last snapshot, "
                                            + "skip this one and schedule the next one in {} seconds",
                                    tableBucket,
                                    snapshotContext.getSnapshotIntervalMs() / 1000);
                        }
                    }
                });
    }

    private void asyncUploadSnapshotPhase(
            long triggerTime,
            long snapshotId,
            int coordinatorEpoch,
            int bucketLeaderEpoch,
            SnapshotLocation snapshotLocation,
            RunnableFuture<SnapshotResult> snapshotedRunnableFuture,
            Executor guardedExecutor) {
        uploadSnapshot(snapshotedRunnableFuture)
                .whenComplete(
                        (snapshotResult, throwable) -> {
                            // if succeed
                            if (throwable == null) {
                                numberOfConsecutiveFailures.set(0);

                                try {
                                    checkNotNull(uploadSnapshotTarget);
                                    uploadSnapshotTarget.handleSnapshotResult(
                                            snapshotId,
                                            coordinatorEpoch,
                                            bucketLeaderEpoch,
                                            snapshotLocation,
                                            snapshotResult);
                                    LOG.info(
                                            "TableBucket {} snapshot {} finished successfully, cost {} ms.",
                                            tableBucket,
                                            snapshotId,
                                            System.currentTimeMillis() - triggerTime);
                                } catch (Throwable t) {
                                    LOG.warn(
                                            "Fail to handle snapshot result during snapshot of TableBucket {}",
                                            tableBucket,
                                            t);
                                }
                                scheduleNextSnapshot(guardedExecutor);
                            } else {
                                // if failed
                                notifyFailureOrCancellation(
                                        snapshotId, snapshotLocation, throwable);
                                int retryTime = numberOfConsecutiveFailures.incrementAndGet();
                                LOG.info(
                                        "TableBucket {} asynchronous part of snapshot is not completed for the {} time.",
                                        tableBucket,
                                        retryTime,
                                        throwable);

                                scheduleNextSnapshot(guardedExecutor);
                            }
                        });
    }

    private void notifyFailureOrCancellation(
            long snapshot, SnapshotLocation snapshotLocation, Throwable cause) {
        LOG.warn("TableBucket {} snapshot {} failed.", tableBucket, snapshot, cause);
        checkNotNull(uploadSnapshotTarget);
        uploadSnapshotTarget.handleSnapshotFailure(snapshot, snapshotLocation, cause);
    }

    private CompletableFuture<SnapshotResult> uploadSnapshot(
            RunnableFuture<SnapshotResult> snapshotedRunnableFuture) {

        FileSystemSafetyNet.initializeSafetyNetForThread();
        CompletableFuture<SnapshotResult> result = new CompletableFuture<>();
        try {
            FutureUtils.runIfNotDoneAndGet(snapshotedRunnableFuture);

            LOG.debug("TableBucket {} finishes asynchronous part of snapshot.", tableBucket);

            result.complete(snapshotedRunnableFuture.get());
        } catch (Exception e) {
            result.completeExceptionally(e);
            discardFailedUploads(snapshotedRunnableFuture);
        } finally {
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }

        return result;
    }

    private void discardFailedUploads(RunnableFuture<SnapshotResult> snapshotedRunnableFuture) {
        LOG.info("TableBucket {} cleanup asynchronous runnable for snapshot.", tableBucket);

        if (snapshotedRunnableFuture != null) {
            // snapshot has started
            if (!snapshotedRunnableFuture.cancel(true)) {
                try {
                    SnapshotResult snapshotResult = snapshotedRunnableFuture.get();
                    if (snapshotResult != null) {
                        snapshotResult.getKvSnapshotHandle().discard();
                        FsPath remoteSnapshotPath = snapshotResult.getSnapshotPath();
                        remoteSnapshotPath.getFileSystem().delete(remoteSnapshotPath, true);
                    }
                } catch (Exception ex) {
                    LOG.debug(
                            "TableBucket {} cancelled execution of snapshot future runnable. Cancellation produced the following exception, which is expected and can be ignored.",
                            tableBucket,
                            ex);
                }
            }
        }
    }

    private void scheduleNextSnapshot(Executor guardedExecutor) {
        scheduleNextSnapshot(snapshotContext.getSnapshotIntervalMs(), guardedExecutor);
    }

    private void loadKvLocalFiles(Set<Path> downloadedSstFiles, Set<Path> downloadedMiscFiles)
            throws Exception {
        if (tabletDir.exists()) {
            Path kvDbPath = tabletDir.toPath().resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
            Path[] files = FileUtils.listDirectory(kvDbPath);
            for (Path filePath : files) {
                final String fileName = filePath.getFileName().toString();
                if (fileName.endsWith(SST_FILE_SUFFIX)) {
                    downloadedSstFiles.add(filePath);
                } else {
                    downloadedMiscFiles.add(filePath);
                }
            }
        }
    }

    private KvSnapshotHandle getIncrementalKvSnapshotHandle(
            CompletedSnapshot completedSnapshot,
            Set<Path> downloadedSstFiles,
            Set<Path> sstFilesToDelete) {
        // get downloaded sst files name to path.
        Map<String, Path> downloadedSstFilesMap = new HashMap<>();
        for (Path sstPath : downloadedSstFiles) {
            downloadedSstFilesMap.put(sstPath.getFileName().toString(), sstPath);
        }

        KvSnapshotHandle completedSnapshotHandler = completedSnapshot.getKvSnapshotHandle();
        List<KvFileHandleAndLocalPath> sstFileHandles =
                completedSnapshotHandler.getSharedKvFileHandles();
        List<KvFileHandleAndLocalPath> privateFileHandles =
                completedSnapshotHandler.getPrivateFileHandles();

        List<KvFileHandleAndLocalPath> incrementalSstFileHandles = new ArrayList<>();
        Set<String> downloadedSstFileNames = downloadedSstFilesMap.keySet();
        for (KvFileHandleAndLocalPath sstFileHandle : sstFileHandles) {
            if (!downloadedSstFileNames.contains(sstFileHandle.getLocalPath())) {
                incrementalSstFileHandles.add(sstFileHandle);
            }
        }

        Set<String> newSstFileNames =
                completedSnapshotHandler.getSharedKvFileHandles().stream()
                        .map(KvFileHandleAndLocalPath::getLocalPath)
                        .collect(Collectors.toSet());
        for (String sstFileName : downloadedSstFileNames) {
            if (!newSstFileNames.contains(sstFileName)) {
                sstFilesToDelete.add(downloadedSstFilesMap.get(sstFileName));
            }
        }

        long incrementalSnapshotSize =
                Math.max(completedSnapshotHandler.getSnapshotSize() - standbySnapshotSize, 0L);
        LOG.debug(
                "Build incremental snapshot handler for table-bucket {}: {} sst files in remote, "
                        + "{} sst files to download, and {} sst files to delete, incremental download size: {}",
                tableBucket,
                sstFileHandles.size(),
                incrementalSstFileHandles.size(),
                sstFilesToDelete.size(),
                incrementalSnapshotSize);
        return new KvSnapshotHandle(
                incrementalSstFileHandles, privateFileHandles, incrementalSnapshotSize);
    }

    /**
     * Atomically download snapshot files to ensure consistency.
     *
     * <p>This method implements atomic snapshot download by:
     *
     * <ol>
     *   <li>Downloading all files to a temporary directory
     *   <li>Verifying the download completeness
     *   <li>Deleting obsolete files from the final directory
     *   <li>Atomically moving new files from temp to final directory
     *   <li>Cleaning up the temp directory
     * </ol>
     *
     * <p>If any step fails, the final directory remains in its original consistent state, ensuring
     * that RocksDB never sees a partially downloaded snapshot.
     *
     * @param incrementalSnapshot the incremental snapshot to download
     * @param sstFilesToDelete SST files that should be deleted from the final directory
     * @throws IOException if download or file operations fail
     */
    private void atomicDownloadSnapshot(
            CompletedSnapshot incrementalSnapshot, Set<Path> sstFilesToDelete) throws IOException {
        Path kvTabletDir = tabletDir.toPath();
        Path kvDbPath = kvTabletDir.resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
        Path tempDownloadDir =
                kvTabletDir.resolve(".tmp_snapshot_" + incrementalSnapshot.getSnapshotID());

        boolean downloadSuccessful = false;
        CloseableRegistry closeableRegistry = new CloseableRegistry();
        try {
            // Step 1: Create temporary download directory
            Files.createDirectories(tempDownloadDir);
            LOG.debug(
                    "Created temporary snapshot download directory {} for bucket {}",
                    tempDownloadDir,
                    tableBucket);

            // Step 2: Download all snapshot files to temporary directory
            KvSnapshotDownloadSpec downloadSpec =
                    new KvSnapshotDownloadSpec(
                            incrementalSnapshot.getKvSnapshotHandle(), tempDownloadDir);
            long start = clock.milliseconds();
            LOG.info(
                    "Start to download kv snapshot {} to temporary directory {}.",
                    incrementalSnapshot,
                    tempDownloadDir);

            KvSnapshotDataDownloader kvSnapshotDataDownloader =
                    snapshotContext.getSnapshotDataDownloader();
            try {
                kvSnapshotDataDownloader.transferAllDataToDirectory(
                        downloadSpec, closeableRegistry);
            } catch (Exception e) {
                if (e.getMessage()
                        .contains(CompletedSnapshot.SNAPSHOT_DATA_NOT_EXISTS_ERROR_MESSAGE)) {
                    try {
                        snapshotContext.handleSnapshotBroken(incrementalSnapshot);
                    } catch (Exception t) {
                        LOG.error("Handle broken snapshot {} failed.", incrementalSnapshot, t);
                    }
                }
                throw new IOException("Fail to download kv snapshot to temporary directory.", e);
            }

            long downloadTime = clock.milliseconds() - start;
            LOG.debug(
                    "Downloaded kv snapshot {} to temporary directory {} in {} ms.",
                    incrementalSnapshot,
                    tempDownloadDir,
                    downloadTime);

            // Step 3: Verify download completeness
            verifySnapshotCompleteness(incrementalSnapshot, tempDownloadDir);

            downloadSuccessful = true;

            // Step 4: Delete obsolete SST files from final directory
            for (Path sstFileToDelete : sstFilesToDelete) {
                try {
                    FileUtils.deleteFileOrDirectory(sstFileToDelete.toFile());
                    LOG.debug(
                            "Deleted obsolete SST file {} for bucket {}",
                            sstFileToDelete,
                            tableBucket);
                } catch (IOException e) {
                    LOG.warn(
                            "Failed to delete obsolete SST file {} for bucket {}",
                            sstFileToDelete,
                            tableBucket,
                            e);
                    // Continue deletion even if one file fails
                }
            }

            // Step 5: Delete obsolete misc files from final directory
            checkNotNull(downloadedMiscFiles, "downloadedMiscFiles is null");
            for (Path miscFileToDelete : downloadedMiscFiles) {
                try {
                    FileUtils.deleteFileOrDirectory(miscFileToDelete.toFile());
                    LOG.debug(
                            "Deleted obsolete misc file {} for bucket {}",
                            miscFileToDelete,
                            tableBucket);
                } catch (IOException e) {
                    LOG.warn(
                            "Failed to delete obsolete misc file {} for bucket {}",
                            miscFileToDelete,
                            tableBucket,
                            e);
                    // Continue deletion even if one file fails
                }
            }

            // Step 6: Atomically move downloaded files from temp to final directory
            moveSnapshotFilesToFinalDirectory(tempDownloadDir, kvDbPath);

            long totalTime = clock.milliseconds() - start;
            LOG.debug(
                    "Atomically applied kv snapshot {} to directory {} in {} ms (download: {} ms, move: {} ms).",
                    incrementalSnapshot,
                    kvDbPath,
                    totalTime,
                    downloadTime,
                    totalTime - downloadTime);

        } finally {
            // Step 7: Clean up closeable registry
            IOUtils.closeQuietly(closeableRegistry);

            // Step 8: Clean up temporary directory
            if (tempDownloadDir.toFile().exists()) {
                try {
                    FileUtils.deleteDirectory(tempDownloadDir.toFile());
                    if (downloadSuccessful) {
                        LOG.debug(
                                "Cleaned up temporary snapshot directory {} for bucket {}",
                                tempDownloadDir,
                                tableBucket);
                    } else {
                        LOG.warn(
                                "Cleaned up temporary snapshot directory {} after failed download for bucket {}",
                                tempDownloadDir,
                                tableBucket);
                    }
                } catch (IOException e) {
                    LOG.warn(
                            "Failed to clean up temporary snapshot directory {} for bucket {}",
                            tempDownloadDir,
                            tableBucket,
                            e);
                }
            }
        }
    }

    /**
     * Verify that all expected snapshot files have been downloaded to the temporary directory.
     *
     * @param snapshot the snapshot being verified
     * @param tempDir the temporary directory containing downloaded files
     * @throws IOException if verification fails
     */
    private void verifySnapshotCompleteness(CompletedSnapshot snapshot, Path tempDir)
            throws IOException {
        KvSnapshotHandle handle = snapshot.getKvSnapshotHandle();
        List<KvFileHandleAndLocalPath> allFiles = new ArrayList<>();
        allFiles.addAll(handle.getSharedKvFileHandles());
        allFiles.addAll(handle.getPrivateFileHandles());

        for (KvFileHandleAndLocalPath fileHandle : allFiles) {
            String fileName = Paths.get(fileHandle.getLocalPath()).getFileName().toString();
            Path expectedFile = tempDir.resolve(fileName);

            if (!Files.exists(expectedFile)) {
                throw new IOException(
                        String.format(
                                "Snapshot verification failed for bucket %s: expected file %s not found in temp directory %s",
                                tableBucket, fileName, tempDir));
            }

            long expectedSize = fileHandle.getKvFileHandle().getSize();
            long actualSize = Files.size(expectedFile);
            if (expectedSize != actualSize) {
                throw new IOException(
                        String.format(
                                "Snapshot verification failed for bucket %s: file %s size mismatch (expected: %d, actual: %d)",
                                tableBucket, fileName, expectedSize, actualSize));
            }
        }

        LOG.info(
                "Verified completeness of snapshot {} for bucket {}: {} files, total size {} bytes",
                snapshot.getSnapshotID(),
                tableBucket,
                allFiles.size(),
                allFiles.stream().mapToLong(f -> f.getKvFileHandle().getSize()).sum());
    }

    /**
     * Move snapshot files from temporary directory to final RocksDB directory.
     *
     * <p>This method attempts atomic moves when possible (same filesystem), falling back to
     * copy-then-delete if atomic move is not supported.
     *
     * @param tempDir the temporary directory containing downloaded files
     * @param finalDir the final RocksDB db directory
     * @throws IOException if file move operations fail
     */
    private void moveSnapshotFilesToFinalDirectory(Path tempDir, Path finalDir) throws IOException {
        File[] files = tempDir.toFile().listFiles();
        if (files == null || files.length == 0) {
            LOG.debug(
                    "No files to move from temp directory {} to final directory {} for bucket {}",
                    tempDir,
                    finalDir,
                    tableBucket);
            return;
        }

        int movedCount = 0;
        for (File file : files) {
            Path sourcePath = file.toPath();
            Path targetPath = finalDir.resolve(file.getName());

            try {
                // Try atomic move first (rename on same filesystem)
                Files.move(
                        sourcePath,
                        targetPath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
                movedCount++;
                LOG.debug(
                        "Atomically moved file {} to {} for bucket {}",
                        sourcePath.getFileName(),
                        targetPath,
                        tableBucket);
            } catch (AtomicMoveNotSupportedException e) {
                // Fallback to copy + delete if atomic move not supported
                LOG.debug(
                        "Atomic move not supported for {}, using copy+delete",
                        sourcePath.getFileName());
                Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                Files.delete(sourcePath);
                movedCount++;
            } catch (IOException e) {
                throw new IOException(
                        String.format(
                                "Failed to move file %s to final directory %s for bucket %s",
                                sourcePath.getFileName(), finalDir, tableBucket),
                        e);
            }
        }

        LOG.debug(
                "Moved {} files from temp directory {} to final directory {} for bucket {}",
                movedCount,
                tempDir,
                finalDir,
                tableBucket);
    }

    /** {@link SnapshotRunnable} provider and consumer. */
    @NotThreadSafe
    public interface UploadSnapshotTarget {

        /** Gets current snapshot id. */
        long currentSnapshotId();

        /**
         * Initialize kv snapshot.
         *
         * @return a tuple of - future snapshot result from the underlying KV.
         */
        Optional<SnapshotRunnable> initSnapshot() throws Exception;

        /**
         * Implementations should not trigger snapshot until the previous one has been confirmed or
         * failed.
         *
         * @param snapshotId the snapshot id
         * @param coordinatorEpoch the coordinator epoch
         * @param bucketLeaderEpoch the leader epoch of the bucket when the snapshot is triggered
         * @param snapshotLocation the location where the snapshot files stores
         * @param snapshotResult the snapshot result
         */
        void handleSnapshotResult(
                long snapshotId,
                int coordinatorEpoch,
                int bucketLeaderEpoch,
                SnapshotLocation snapshotLocation,
                SnapshotResult snapshotResult)
                throws Throwable;

        /** Called when the snapshot is fail. */
        void handleSnapshotFailure(
                long snapshotId, SnapshotLocation snapshotLocation, Throwable cause);

        /** Get the total size of the snapshot. */
        long getSnapshotSize();
    }

    @Override
    public void close() {
        synchronized (this) {
            // do-nothing, please make the periodicExecutor will be closed by external
            isLeader = false;
            // cancel the scheduled task if not completed yet
            if (scheduledTask != null && !scheduledTask.isDone()) {
                scheduledTask.cancel(true);
            }
        }
    }

    /** A {@link Runnable} representing the snapshot and the associated metadata. */
    public static class SnapshotRunnable {
        private final RunnableFuture<SnapshotResult> snapshotRunnable;

        private final long snapshotId;
        private final int coordinatorEpoch;
        private final int bucketLeaderEpoch;
        private final SnapshotLocation snapshotLocation;

        public SnapshotRunnable(
                RunnableFuture<SnapshotResult> snapshotRunnable,
                long snapshotId,
                int coordinatorEpoch,
                int bucketLeaderEpoch,
                SnapshotLocation snapshotLocation) {
            this.snapshotRunnable = snapshotRunnable;
            this.snapshotId = snapshotId;
            this.coordinatorEpoch = coordinatorEpoch;
            this.bucketLeaderEpoch = bucketLeaderEpoch;
            this.snapshotLocation = snapshotLocation;
        }

        RunnableFuture<SnapshotResult> getSnapshotRunnable() {
            return snapshotRunnable;
        }

        public long getSnapshotId() {
            return snapshotId;
        }

        public SnapshotLocation getSnapshotLocation() {
            return snapshotLocation;
        }

        public int getCoordinatorEpoch() {
            return coordinatorEpoch;
        }

        public int getBucketLeaderEpoch() {
            return bucketLeaderEpoch;
        }
    }
}

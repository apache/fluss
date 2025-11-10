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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
    }

    public void becomeFollower() {
        isLeader = false;
    }

    public void becomeStandby() {
        isLeader = false;
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
            incrementalDownloadSnapshot(latestSnapshot.get());
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
        downloadKvSnapshots(incrementalSnapshot, new CloseableRegistry());

        // delete the sst files that are not needed.
        for (Path sstFileToDelete : sstFilesToDelete) {
            FileUtils.deleteFileOrDirectory(sstFileToDelete.toFile());
        }

        // delete the misc files that are not needed.
        for (Path miscFileToDelete : downloadedMiscFiles) {
            FileUtils.deleteFileOrDirectory(miscFileToDelete.toFile());
        }

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

    /** Download the snapshot to target tablet dir. */
    public void downloadKvSnapshots(
            CompletedSnapshot completedSnapshot, CloseableRegistry closeableRegistry)
            throws IOException {
        Path kvTabletDir = tabletDir.toPath();
        Path kvDbPath = kvTabletDir.resolve(RocksDBKvBuilder.DB_INSTANCE_DIR_STRING);
        KvSnapshotDownloadSpec downloadSpec =
                new KvSnapshotDownloadSpec(completedSnapshot.getKvSnapshotHandle(), kvDbPath);
        long start = clock.milliseconds();
        LOG.info("Start to download kv snapshot {} to directory {}.", completedSnapshot, kvDbPath);
        KvSnapshotDataDownloader kvSnapshotDataDownloader =
                snapshotContext.getSnapshotDataDownloader();
        try {
            kvSnapshotDataDownloader.transferAllDataToDirectory(downloadSpec, closeableRegistry);
        } catch (Exception e) {
            if (e.getMessage().contains(CompletedSnapshot.SNAPSHOT_DATA_NOT_EXISTS_ERROR_MESSAGE)) {
                try {
                    snapshotContext.handleSnapshotBroken(completedSnapshot);
                } catch (Exception t) {
                    LOG.error("Handle broken snapshot {} failed.", completedSnapshot, t);
                }
            }
            throw new IOException("Fail to download kv snapshot.", e);
        }
        long end = clock.milliseconds();
        LOG.info(
                "Download kv snapshot {} to directory {} finish, cost {} ms.",
                completedSnapshot,
                kvDbPath,
                end - start);
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
            Path[] files = FileUtils.listDirectory(tabletDir.toPath());
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
        KvSnapshotHandle completedSnapshotHandler = completedSnapshot.getKvSnapshotHandle();
        List<KvFileHandleAndLocalPath> sstFileHandles =
                completedSnapshotHandler.getSharedKvFileHandles();
        List<KvFileHandleAndLocalPath> privateFileHandles =
                completedSnapshotHandler.getPrivateFileHandles();

        List<KvFileHandleAndLocalPath> incrementalSstFileHandles = new ArrayList<>();
        for (KvFileHandleAndLocalPath sstFileHandle : sstFileHandles) {
            Path sstPath = Paths.get(sstFileHandle.getLocalPath());
            if (!downloadedSstFiles.contains(sstPath)) {
                incrementalSstFileHandles.add(sstFileHandle);
            }
        }

        Set<Path> newSstFiles =
                completedSnapshotHandler.getSharedKvFileHandles().stream()
                        .map(handler -> Paths.get(handler.getLocalPath()))
                        .collect(Collectors.toSet());
        for (Path sstPath : downloadedSstFiles) {
            if (!newSstFiles.contains(sstPath)) {
                sstFilesToDelete.add(sstPath);
            }
        }

        long incrementalSnapshotSize =
                Math.max(completedSnapshotHandler.getSnapshotSize() - standbySnapshotSize, 0L);
        LOG.info(
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

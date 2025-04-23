/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.fs.FileSystemSafetyNet;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.server.metrics.group.BucketMetricGroup;
import com.alibaba.fluss.utils.MathUtils;
import com.alibaba.fluss.utils.concurrent.Executors;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Stateless snapshot manager which will trigger snapshot periodically. It'll use a {@link
 * ScheduledExecutorService} to schedule the snapshot initialization and a {@link ExecutorService}
 * to complete async phase of snapshot.
 */
public class PeriodicSnapshotManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicSnapshotManager.class);

    /**
     * An executor that uses to trigger snapshot.
     *
     * <p>It's expected to be passed with a guarded executor to prevent any concurrent modification
     * to KvTablet during trigger snapshotting.
     */
    private final Executor guardedExecutor;

    /** scheduled executor, periodically trigger snapshot. */
    private final ScheduledExecutorService periodicExecutor;

    /** Async thread pool, to complete async phase of snapshot. */
    private final ExecutorService asyncOperationsThreadPool;

    private final long periodicSnapshotDelay;

    /** Number of consecutive snapshot failures. */
    private final AtomicInteger numberOfConsecutiveFailures;

    /** The target on which the snapshot will be done. */
    private final SnapshotTarget target;

    /** Whether snapshot is started. */
    private volatile boolean started = false;

    private final long initialDelay;
    /** The table bucket that the snapshot manager is for. */
    private final TableBucket tableBucket;

    @VisibleForTesting
    protected PeriodicSnapshotManager(
            TableBucket tableBucket,
            SnapshotTarget target,
            long periodicSnapshotDelay,
            ExecutorService asyncOperationsThreadPool,
            ScheduledExecutorService periodicExecutor,
            BucketMetricGroup bucketMetricGroup) {
        this(
                tableBucket,
                target,
                periodicSnapshotDelay,
                asyncOperationsThreadPool,
                periodicExecutor,
                Executors.directExecutor(),
                bucketMetricGroup);
    }

    @VisibleForTesting
    protected PeriodicSnapshotManager(
            TableBucket tableBucket,
            SnapshotTarget target,
            long periodicSnapshotDelay,
            ExecutorService asyncOperationsThreadPool,
            ScheduledExecutorService periodicExecutor,
            Executor guardedExecutor,
            BucketMetricGroup bucketMetricGroup) {
        this.tableBucket = tableBucket;
        this.target = target;
        this.periodicSnapshotDelay = periodicSnapshotDelay;

        this.numberOfConsecutiveFailures = new AtomicInteger(0);
        this.periodicExecutor = periodicExecutor;
        this.guardedExecutor = guardedExecutor;
        this.asyncOperationsThreadPool = asyncOperationsThreadPool;
        this.initialDelay =
                periodicSnapshotDelay > 0
                        ? MathUtils.murmurHash(tableBucket.hashCode()) % periodicSnapshotDelay
                        : 0;

        registerMetrics(bucketMetricGroup);
    }

    public static PeriodicSnapshotManager create(
            TableBucket tableBucket,
            SnapshotTarget target,
            SnapshotContext snapshotContext,
            Executor guardedExecutor,
            BucketMetricGroup bucketMetricGroup) {
        return new PeriodicSnapshotManager(
                tableBucket,
                target,
                snapshotContext.getSnapshotIntervalMs(),
                snapshotContext.getAsyncOperationsThreadPool(),
                snapshotContext.getSnapshotScheduler(),
                guardedExecutor,
                bucketMetricGroup);
    }

    public void start() {
        // disable periodic snapshot when periodicMaterializeDelay is not positive
        if (!started && periodicSnapshotDelay > 0) {

            started = true;

            LOG.info("TableBucket {} starts periodic snapshot", tableBucket);

            scheduleNextSnapshot(initialDelay);
        }
    }

    private void registerMetrics(BucketMetricGroup bucketMetricGroup) {
        MetricGroup metricGroup = bucketMetricGroup.addGroup("kv").addGroup("snapshot");
        metricGroup.gauge(MetricNames.KV_LATEST_SNAPSHOT_SIZE, target::getSnapshotSize);
    }

    // schedule thread and asyncOperationsThreadPool can access this method
    private synchronized void scheduleNextSnapshot(long delay) {
        if (started && !periodicExecutor.isShutdown()) {

            LOG.debug(
                    "TableBucket {} schedules the next snapshot in {} seconds",
                    tableBucket,
                    delay / 1000);
            periodicExecutor.schedule(this::triggerSnapshot, delay, TimeUnit.MILLISECONDS);
        }
    }

    public void triggerSnapshot() {
        // todo: consider shrink the scope
        // of using guardedExecutor
        guardedExecutor.execute(
                () -> {
                    if (started) {
                        LOG.debug("TableBucket {} triggers snapshot.", tableBucket);
                        long triggerTime = System.currentTimeMillis();

                        Optional<SnapshotRunnable> snapshotRunnableOptional;
                        try {
                            snapshotRunnableOptional = target.initSnapshot();
                        } catch (Exception e) {
                            LOG.error("Fail to init snapshot during triggering snapshot.", e);
                            return;
                        }
                        if (snapshotRunnableOptional.isPresent()) {
                            SnapshotRunnable runnable = snapshotRunnableOptional.get();
                            asyncOperationsThreadPool.execute(
                                    () ->
                                            asyncSnapshotPhase(
                                                    triggerTime,
                                                    runnable.getSnapshotId(),
                                                    runnable.getCoordinatorEpoch(),
                                                    runnable.getBucketLeaderEpoch(),
                                                    runnable.getSnapshotLocation(),
                                                    runnable.getSnapshotRunnable()));
                        } else {
                            scheduleNextSnapshot();
                            LOG.debug(
                                    "TableBucket {} has no data updates since last snapshot, "
                                            + "skip this one and schedule the next one in {} seconds",
                                    tableBucket,
                                    periodicSnapshotDelay / 1000);
                        }
                    }
                });
    }

    private void asyncSnapshotPhase(
            long triggerTime,
            long snapshotId,
            int coordinatorEpoch,
            int bucketLeaderEpoch,
            SnapshotLocation snapshotLocation,
            RunnableFuture<SnapshotResult> snapshotedRunnableFuture) {
        uploadSnapshot(snapshotedRunnableFuture)
                .whenComplete(
                        (snapshotResult, throwable) -> {
                            // if succeed
                            if (throwable == null) {
                                numberOfConsecutiveFailures.set(0);

                                try {
                                    target.handleSnapshotResult(
                                            snapshotId,
                                            coordinatorEpoch,
                                            bucketLeaderEpoch,
                                            snapshotLocation,
                                            snapshotResult);
                                    LOG.info(
                                            "TableBucket {} snapshot finished successfully, cost {} ms.",
                                            tableBucket,
                                            System.currentTimeMillis() - triggerTime);
                                } catch (Throwable t) {
                                    LOG.warn(
                                            "Fail to handle snapshot result during snapshot of TableBucket {}",
                                            tableBucket,
                                            t);
                                }
                                scheduleNextSnapshot();
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

                                scheduleNextSnapshot();
                            }
                        });
    }

    private void notifyFailureOrCancellation(
            long snapshot, SnapshotLocation snapshotLocation, Throwable cause) {
        LOG.warn("TableBucket {} snapshot {} failed.", tableBucket, snapshot, cause);
        target.handleSnapshotFailure(snapshot, snapshotLocation, cause);
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

    private void scheduleNextSnapshot() {
        scheduleNextSnapshot(periodicSnapshotDelay);
    }

    /** {@link SnapshotRunnable} provider and consumer. */
    @NotThreadSafe
    public interface SnapshotTarget {
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
            started = false;
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

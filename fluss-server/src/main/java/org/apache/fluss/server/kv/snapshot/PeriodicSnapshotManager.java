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
import org.apache.fluss.server.utils.CancellationToken;
import org.apache.fluss.utils.MathUtils;
import org.apache.fluss.utils.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Snapshot manager that serializes periodic and explicit snapshots. It uses a {@link
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

    /**
     * A supplier to get the current snapshot delay. This allows dynamic reconfiguration of the
     * snapshot interval at runtime.
     */
    private final LongSupplier snapshotIntervalSupplier;

    /** Number of consecutive snapshot failures. */
    private final AtomicInteger numberOfConsecutiveFailures;

    /** The target on which the snapshot will be done. */
    private final SnapshotTarget target;

    /** Whether snapshot is started. */
    private volatile boolean started = false;

    private boolean closed;
    private Execution active;
    private Execution manual;
    private final CompletableFuture<Void> closedFuture = new CompletableFuture<>();

    private static class Execution {
        private final boolean force;
        private final CancellationToken cancellation = new CancellationToken();
        private final CompletableFuture<CompletedSnapshotHandle> result = new CompletableFuture<>();
        private final CompletableFuture<Void> exited = new CompletableFuture<>();
        private volatile SnapshotRunnable snapshot;
        private boolean started;
        private boolean skipped;
        private boolean cancellationScheduled;
        private boolean cancellationStarted;
        private CompletableFuture<Void> cancellationFinished;

        private Execution(boolean force) {
            this.force = force;
        }
    }

    /**
     * The scheduled snapshot task.
     *
     * <p>Since all reads and writes of {@code scheduledTask} are protected by synchronized, the
     * volatile modifier is not necessary here.
     */
    private ScheduledFuture<?> scheduledTask = null;

    private final long initialDelay;
    /** The table bucket that the snapshot manager is for. */
    private final TableBucket tableBucket;

    @VisibleForTesting
    protected PeriodicSnapshotManager(
            TableBucket tableBucket,
            SnapshotTarget target,
            long periodicSnapshotDelay,
            ExecutorService asyncOperationsThreadPool,
            ScheduledExecutorService periodicExecutor) {
        this(
                tableBucket,
                target,
                () -> periodicSnapshotDelay,
                asyncOperationsThreadPool,
                periodicExecutor,
                Executors.directExecutor());
    }

    @VisibleForTesting
    protected PeriodicSnapshotManager(
            TableBucket tableBucket,
            SnapshotTarget target,
            LongSupplier snapshotIntervalSupplier,
            ExecutorService asyncOperationsThreadPool,
            ScheduledExecutorService periodicExecutor) {
        this(
                tableBucket,
                target,
                snapshotIntervalSupplier,
                asyncOperationsThreadPool,
                periodicExecutor,
                Executors.directExecutor());
    }

    private PeriodicSnapshotManager(
            TableBucket tableBucket,
            SnapshotTarget target,
            LongSupplier snapshotIntervalSupplier,
            ExecutorService asyncOperationsThreadPool,
            ScheduledExecutorService periodicExecutor,
            Executor guardedExecutor) {
        this.tableBucket = tableBucket;
        this.target = target;
        this.snapshotIntervalSupplier = snapshotIntervalSupplier;

        this.numberOfConsecutiveFailures = new AtomicInteger(0);
        this.periodicExecutor = periodicExecutor;
        this.guardedExecutor = guardedExecutor;
        this.asyncOperationsThreadPool = asyncOperationsThreadPool;
        long periodicSnapshotDelay = snapshotIntervalSupplier.getAsLong();
        this.initialDelay =
                periodicSnapshotDelay > 0
                        ? MathUtils.murmurHash(tableBucket.hashCode()) % periodicSnapshotDelay
                        : 0;
    }

    public static PeriodicSnapshotManager create(
            TableBucket tableBucket,
            SnapshotTarget target,
            SnapshotContext snapshotContext,
            Executor guardedExecutor) {
        return new PeriodicSnapshotManager(
                tableBucket,
                target,
                snapshotContext::getSnapshotIntervalMs,
                snapshotContext.getAsyncOperationsThreadPool(),
                snapshotContext.getSnapshotScheduler(),
                guardedExecutor);
    }

    public synchronized void start() {
        if (!closed && !started && snapshotIntervalSupplier.getAsLong() > 0) {

            started = true;

            LOG.info("TableBucket {} starts periodic snapshot", tableBucket);

            scheduleNextSnapshot(Math.max(initialDelay, 1));
        }
    }

    public long getSnapshotSize() {
        return target.getSnapshotSize();
    }

    // schedule thread and asyncOperationsThreadPool can access this method
    private synchronized void scheduleNextSnapshot(long delay) {
        if (started
                && !closed
                && active == null
                && manual == null
                && snapshotIntervalSupplier.getAsLong() > 0
                && !periodicExecutor.isShutdown()) {
            if (scheduledTask != null && !scheduledTask.isDone()) {
                scheduledTask.cancel(false);
            }

            LOG.debug(
                    "TableBucket {} schedules the next snapshot in {} seconds",
                    tableBucket,
                    delay / 1000);
            scheduledTask =
                    periodicExecutor.schedule(
                            () -> {
                                triggerSnapshot();
                            },
                            delay,
                            TimeUnit.MILLISECONDS);
        }
    }

    @VisibleForTesting
    public long currentSnapshotId() {
        return target.currentSnapshotId();
    }

    /** Requests a new snapshot even if the flushed offset has not advanced. */
    public CompletableFuture<CompletedSnapshotHandle> triggerSnapshotAfterCompaction() {
        Execution request = new Execution(true);
        boolean launch = false;
        synchronized (this) {
            if (closed || manual != null) {
                request.result.completeExceptionally(
                        new IllegalStateException(
                                closed
                                        ? "Snapshot manager is closed."
                                        : "An explicit snapshot is already pending."));
                return request.result;
            }
            manual = request;
            if (scheduledTask != null) {
                scheduledTask.cancel(false);
            }
            if (active == null) {
                active = request;
                launch = true;
            }
        }
        if (launch) {
            execute(request);
        }
        return request.result;
    }

    /**
     * Cancels only the explicit request and returns its actual resource and commit exit.
     *
     * <p>Call outside replica and KV locks because result callbacks may run synchronously.
     */
    public CompletableFuture<Void> cancelSnapshotAfterCompaction() {
        Execution request;
        boolean waiting;
        synchronized (this) {
            request = manual;
            if (request == null) {
                return CompletableFuture.completedFuture(null);
            }
            waiting = request != active;
            if (waiting) {
                manual = null;
            }
            request.cancellation.cancel();
            if (!waiting && !request.started) {
                request.skipped = true;
            }
            prepareCancellation(request);
        }
        request.result.completeExceptionally(
                new CancellationException("Explicit snapshot cancelled."));
        if (waiting) {
            request.exited.complete(null);
        } else if (request.skipped) {
            finish(request, null, new CancellationException("Explicit snapshot cancelled."));
        } else {
            cancelUpload(request);
        }
        return request.exited;
    }

    /** Triggers a periodic snapshot whose result is null when no snapshot is admitted or needed. */
    public CompletableFuture<CompletedSnapshotHandle> triggerSnapshot() {
        Execution execution;
        synchronized (this) {
            if (!started || closed || active != null || manual != null) {
                return CompletableFuture.completedFuture(null);
            }
            execution = new Execution(false);
            active = execution;
        }
        execute(execution);
        return execution.result;
    }

    private void execute(Execution execution) {
        try {
            asyncOperationsThreadPool.execute(() -> runSnapshot(execution));
        } catch (RuntimeException e) {
            finish(execution, null, e);
        }
    }

    private void runSnapshot(Execution execution) {
        synchronized (this) {
            if (execution.skipped) {
                return;
            }
            execution.started = true;
        }
        CompletedSnapshotHandle handle = null;
        Throwable failure = null;
        SnapshotRunnable snapshot = null;
        boolean committing = false;
        FileSystemSafetyNet.initializeSafetyNetForThread();
        try {
            execution.cancellation.checkCancelled();
            Optional<SnapshotRunnable> initialized =
                    target.initSnapshot(execution.force, guardedExecutor);
            if (initialized.isPresent()) {
                snapshot = initialized.get();
                execution.snapshot = snapshot;
                execution.cancellation.checkCancelled();
                // run() returns only after the callable has exited, even when its Future was
                // cancelled.
                snapshot.getSnapshotRunnable().run();
                SnapshotResult result = snapshot.getSnapshotRunnable().get();
                execution.cancellation.checkCancelled();
                committing = true;
                target.handleSnapshotResult(
                        snapshot.getSnapshotId(),
                        snapshot.getCoordinatorEpoch(),
                        snapshot.getBucketLeaderEpoch(),
                        snapshot.getSnapshotLocation(),
                        result);
                handle =
                        new CompletedSnapshotHandle(
                                snapshot.getSnapshotId(),
                                CompletedSnapshot.getMetadataFilePath(result.getSnapshotPath()),
                                result.getTabletState().getFlushedLogOffset());
                numberOfConsecutiveFailures.set(0);
            } else if (execution.force) {
                throw new IllegalStateException("Explicit snapshot did not produce a checkpoint.");
            }
        } catch (Throwable t) {
            failure = t;
            int failures = numberOfConsecutiveFailures.incrementAndGet();
            LOG.warn(
                    "Snapshot for TableBucket {} failed for the {} consecutive time.",
                    tableBucket,
                    failures,
                    t);
        } finally {
            try {
                if (snapshot != null) {
                    if (!committing && failure != null) {
                        discardFailedUploads(snapshot.getSnapshotRunnable());
                    }
                    // Cancellation can complete FutureTask before the resource owner has exited.
                    snapshot.resourcesReleased().join();
                    if (!committing && failure != null) {
                        target.handleSnapshotFailure(
                                snapshot.getSnapshotId(), snapshot.getSnapshotLocation(), failure);
                    }
                }
            } catch (Throwable cleanupFailure) {
                if (failure == null) {
                    failure = cleanupFailure;
                } else {
                    failure.addSuppressed(cleanupFailure);
                }
            } finally {
                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
                finish(execution, handle, failure);
            }
        }
    }

    private void finish(Execution execution, CompletedSnapshotHandle handle, Throwable failure) {
        Execution next = null;
        boolean closeComplete = false;
        CompletableFuture<Void> cancellationFinished;
        synchronized (this) {
            if (active != execution) {
                return;
            }
            cancellationFinished = execution.cancellationFinished;
            if (!execution.cancellationStarted) {
                // The worker has exited; a queued cancellation must not keep shutdown waiting.
                // In particular, executor shutdown may discard that queued callback entirely.
                execution.cancellationFinished = null;
                cancellationFinished = null;
            }
            if (cancellationFinished == null || cancellationFinished.isDone()) {
                cancellationFinished = null;
                if (manual == execution) {
                    manual = null;
                }
                active = null;
                if (!closed && manual != null) {
                    next = manual;
                    active = next;
                }
                closeComplete = closed;
            }
        }
        if (cancellationFinished != null) {
            cancellationFinished.whenComplete(
                    (ignored, error) -> finish(execution, handle, failure));
            return;
        }
        execution.exited.complete(null);
        if (failure == null) {
            execution.result.complete(handle);
        } else {
            execution.result.completeExceptionally(failure);
        }
        if (closeComplete) {
            closedFuture.complete(null);
        } else if (next != null) {
            execute(next);
        } else {
            scheduleNextSnapshot();
        }
    }

    private void prepareCancellation(Execution execution) {
        if (execution.snapshot != null
                && !execution.snapshot.getSnapshotRunnable().isDone()
                && execution.cancellationFinished == null) {
            execution.cancellationFinished = new CompletableFuture<>();
        }
    }

    private void cancelUpload(Execution execution) {
        CompletableFuture<Void> cancellationFinished;
        synchronized (this) {
            cancellationFinished = execution.cancellationFinished;
            if (cancellationFinished == null || execution.cancellationScheduled) {
                return;
            }
            execution.cancellationScheduled = true;
        }
        // Closing an upload stream can block. Never do it on the caller holding a replica lock.
        try {
            periodicExecutor.execute(
                    () -> {
                        synchronized (this) {
                            if (execution.cancellationFinished == null) {
                                return;
                            }
                            execution.cancellationStarted = true;
                        }
                        try {
                            SnapshotRunnable snapshot = execution.snapshot;
                            if (snapshot != null && !snapshot.getSnapshotRunnable().isDone()) {
                                snapshot.getSnapshotRunnable().cancel(true);
                            }
                        } finally {
                            cancellationFinished.complete(null);
                        }
                    });
        } catch (RuntimeException e) {
            // The worker also observes the token before upload and commit, and owns final cleanup.
            cancellationFinished.complete(null);
            LOG.debug("Snapshot cancellation executor is unavailable for {}.", tableBucket, e);
        }
    }

    private void discardFailedUploads(RunnableFuture<SnapshotResult> future) {
        if (!future.cancel(true)) {
            try {
                SnapshotResult result = future.get();
                if (result != null) {
                    result.getKvSnapshotHandle().discard();
                    FsPath path = result.getSnapshotPath();
                    path.getFileSystem().delete(path, true);
                }
            } catch (Exception e) {
                LOG.debug("Snapshot upload did not produce a result for {}.", tableBucket, e);
            }
        }
    }

    private void scheduleNextSnapshot() {
        scheduleNextSnapshot(snapshotIntervalSupplier.getAsLong());
    }

    /** {@link SnapshotRunnable} provider and consumer. */
    @NotThreadSafe
    public interface SnapshotTarget {

        /** Gets current snapshot id. */
        long currentSnapshotId();

        /**
         * Initialize kv snapshot.
         *
         * @return a tuple of - future snapshot result from the underlying KV.
         */
        Optional<SnapshotRunnable> initSnapshot() throws Exception;

        /** Initializes a forced snapshot. */
        default Optional<SnapshotRunnable> initSnapshot(boolean force) throws Exception {
            return initSnapshot();
        }

        /** Captures local state using the supplied synchronous KV guard. */
        default Optional<SnapshotRunnable> initSnapshot(boolean force, Executor guardedExecutor)
                throws Exception {
            CompletableFuture<Optional<SnapshotRunnable>> initialized = new CompletableFuture<>();
            guardedExecutor.execute(
                    () -> {
                        try {
                            initialized.complete(initSnapshot(force));
                        } catch (Throwable t) {
                            initialized.completeExceptionally(t);
                        }
                    });
            return initialized.get();
        }

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

    /** Rejects new snapshots and only signals cancellation while a replica changes state. */
    public synchronized void requestClose() {
        closed = true;
        started = false;
        if (scheduledTask != null) {
            scheduledTask.cancel(false);
        }
        if (active != null) {
            active.cancellation.cancel();
        }
        if (manual != null && manual != active) {
            manual.cancellation.cancel();
        }
    }

    @Override
    public void close() {
        closeAsync();
    }

    /**
     * Rejects new work and completes after all snapshot resource users and commit calls exit.
     *
     * <p>Call outside replica and KV locks because result callbacks may run synchronously.
     */
    public CompletableFuture<Void> closeAsync() {
        Execution running;
        Execution waiting;
        synchronized (this) {
            closed = true;
            started = false;
            if (scheduledTask != null) {
                scheduledTask.cancel(false);
            }
            running = active;
            waiting = manual != active ? manual : null;
            if (waiting != null) {
                manual = null;
                waiting.cancellation.cancel();
            }
            if (running != null) {
                running.cancellation.cancel();
                if (!running.started) {
                    running.skipped = true;
                }
                prepareCancellation(running);
            }
        }
        if (waiting != null) {
            waiting.result.completeExceptionally(
                    new CancellationException("Snapshot manager closed."));
            waiting.exited.complete(null);
        }
        if (running == null) {
            closedFuture.complete(null);
        } else {
            running.result.completeExceptionally(
                    new CancellationException("Snapshot manager closed."));
            if (running.skipped) {
                finish(running, null, new CancellationException("Snapshot manager closed."));
            } else {
                cancelUpload(running);
            }
        }
        return closedFuture;
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

        private CompletableFuture<Void> resourcesReleased() {
            if (snapshotRunnable instanceof AsyncSnapshotCallable.AsyncSnapshotTask) {
                return ((AsyncSnapshotCallable<?>.AsyncSnapshotTask) snapshotRunnable)
                        .getResourcesReleasedFuture();
            }
            return CompletableFuture.completedFuture(null);
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

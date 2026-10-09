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

package org.apache.fluss.server.kv;

import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

/**
 * Opens KV resources on demand and releases idle tablets. Close/drop fence new requests and wait
 * for pins, callbacks and candidate cleanup before relinquishing native resources or local paths.
 */
public final class KvTabletLazyLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(KvTabletLazyLifecycle.class);

    /** Coarsen access timestamp updates to reduce volatile writes on hot path. */
    private static final long ACCESS_TIMESTAMP_GRANULARITY_MS = 1000;

    private static final long OPEN_TIMEOUT_MS = 300_000;

    /** Internal RocksDB lazy lifecycle states. */
    enum LazyState {
        LAZY,

        OPENING,

        OPEN,

        RELEASING,

        FAILED,
        /** Closing: new requests are fenced while outstanding work drains. */
        CLOSING,
        /** Terminal state; local data is deleted only on drop. */
        CLOSED
    }

    /** Creates an eager tablet, recovering local SST/WAL or a snapshot as needed. */
    @FunctionalInterface
    public interface OpenCallback {
        /** Performs the open and returns the opened RocksDB KvTablet instance. */
        KvTablet doOpen(boolean hasLocalData) throws Exception;
    }

    private final KvTablet tablet;

    private volatile LazyState lazyState = LazyState.LAZY;

    private final ReentrantLock lazyStateLock = new ReentrantLock();

    private final Condition lazyStateChanged = lazyStateLock.newCondition();

    /** Active pin count — prevents release while operations are in-flight. */
    private final AtomicInteger activePins = new AtomicInteger(0);

    /** Semaphore for throttling concurrent open operations across all tablets. */
    private @Nullable Semaphore openSemaphore;

    private long releaseDrainTimeoutMs;
    private final ExponentialBackoff failedBackoff = new ExponentialBackoff(5_000, 2, 300_000, 0);
    private Clock clock = SystemClock.getInstance();

    private long failedTimestamp;

    private int failureCount;
    private @Nullable Throwable lastFailureCause;

    /** Whether local RocksDB data directory exists from a previous open. */
    @GuardedBy("lazyStateLock")
    private boolean hasLocalData;

    /** Access timestamp for idle release decisions (coarsened to reduce volatile writes). */
    private volatile long lastAccessTimestamp;

    private @Nullable IntSupplier leaderEpochSupplier;

    private @Nullable IntSupplier bucketEpochSupplier;

    private @Nullable OpenCallback openCallback;

    private @Nullable Consumer<KvTablet> commitCallback;
    private @Nullable Consumer<KvTablet> releaseCallback;

    /**
     * Includes open callbacks, candidate cleanup and idle release cleanup. Guarded by the state
     * lock.
     */
    private boolean operationInFlight;

    /** Retains ownership if cleaning up an uncommitted open fails. */
    private @Nullable KvTablet failedOpenTablet;

    /** Guarded by the termination monitor; prevents repeated drops from deleting a reused path. */
    private boolean localDirectoryDeleted;

    KvTabletLazyLifecycle(KvTablet tablet) {
        this.tablet = tablet;
        updateStateGauge(LazyState.LAZY, 1);
    }

    /** Configures recovery before this sentinel is registered or accessed. */
    public void configure(
            IntSupplier leaderEpoch,
            IntSupplier bucketEpoch,
            OpenCallback open,
            Consumer<KvTablet> committed,
            Consumer<KvTablet> release) {
        leaderEpochSupplier = leaderEpoch;
        bucketEpochSupplier = bucketEpoch;
        openCallback = open;
        commitCallback = committed;
        releaseCallback = release;
    }

    void configureTiming(Clock clock, Semaphore semaphore, long drainTimeout) {
        this.clock = clock;
        this.openSemaphore = semaphore;
        this.releaseDrainTimeoutMs = drainTimeout;
    }

    /** Initializes row count from snapshot metadata without opening RocksDB. */
    public void initCachedRowCount(long rowCount) {
        tablet.setRowCount(rowCount);
    }

    boolean isOpen() {
        return lazyState == LazyState.OPEN;
    }

    LazyState getLazyState() {
        return lazyState;
    }

    /** Returns the row count retained across idle release. */
    public long getCachedRowCount() {
        return tablet.currentRowCount();
    }

    long getLastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    int getActivePins() {
        return activePins.get();
    }

    /**
     * Opens and pins the tablet. Call outside replica locks to avoid blocking leadership changes.
     */
    KvTablet.Guard acquireGuard() {
        if (openCallback == null) {
            throw new IllegalStateException("Lazy tablet recovery is not configured.");
        }

        for (int attempt = 0; ; attempt++) {
            KvTablet.Guard guard = tryAcquireExistingGuard();
            if (guard != null) {
                touchAccessTimestamp();
                return guard;
            }
            if (attempt >= 3) {
                throw new KvStorageException("Failed to pin KV tablet " + tablet.getTableBucket());
            }
            ensureOpen();
        }
    }

    /**
     * Pins maintenance without refreshing idle time. Recheck after incrementing so release cannot
     * miss a newly admitted request.
     */
    @Nullable
    KvTablet.Guard tryAcquireExistingGuard() {
        if (lazyState == LazyState.OPEN) {
            activePins.incrementAndGet();
            if (lazyState == LazyState.OPEN) {
                return new KvTablet.Guard(tablet.requireOpenedTablet(), this);
            }
            releasePin();
        }
        return null;
    }

    void releasePin() {
        if (activePins.decrementAndGet() == 0) {
            if (lazyState != LazyState.OPEN) {
                lazyStateLock.lock();
                try {
                    lazyStateChanged.signalAll();
                } finally {
                    lazyStateLock.unlock();
                }
            }
        }
    }

    private void touchAccessTimestamp() {
        long now = clock.milliseconds();
        if (now - lastAccessTimestamp > ACCESS_TIMESTAMP_GRANULARITY_MS) {
            lastAccessTimestamp = now;
        }
    }

    /** Counts stable states only; transitional states have no gauge. */
    private void updateStateGauge(LazyState state, int delta) {
        TabletServerMetricGroup metrics = tablet.serverMetricGroup;
        if (metrics == null) {
            return;
        }
        if (state == LazyState.LAZY) {
            metrics.kvTabletLazyCount().addAndGet(delta);
        } else if (state == LazyState.OPEN) {
            metrics.kvTabletOpenCount().addAndGet(delta);
        } else if (state == LazyState.FAILED) {
            metrics.kvTabletFailedCount().addAndGet(delta);
        }
    }

    /** Waits for ongoing work or starts a single open after failure backoff. */
    private void ensureOpen() {
        lazyStateLock.lock();
        try {
            while (true) {
                switch (lazyState) {
                    case OPEN:
                        return;

                    case OPENING:
                    case RELEASING:
                        if (!lazyStateChanged.await(OPEN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                            throw new KvStorageException(
                                    "Timed out opening KV tablet " + tablet.getTableBucket());
                        }
                        continue;

                    case FAILED:
                        long elapsed = clock.milliseconds() - failedTimestamp;
                        long cooldown = failedBackoff.backoff(failureCount);
                        if (elapsed < cooldown) {
                            throw new KvStorageException(
                                    "KvTablet open failed for "
                                            + tablet.getTableBucket()
                                            + ", remaining cooldown: "
                                            + (cooldown - elapsed)
                                            + " ms",
                                    lastFailureCause);
                        }
                        // Fall through to LAZY — cooldown expired, retry open.

                    case LAZY:
                        transitionTo(LazyState.OPENING);
                        operationInFlight = true;

                        int myLeaderEpoch = leaderEpochSupplier.getAsInt();
                        int myBucketEpoch = bucketEpochSupplier.getAsInt();
                        boolean localDataExists = hasLocalData;
                        lazyStateLock.unlock();
                        try {
                            doSlowOpen(myLeaderEpoch, myBucketEpoch, localDataExists);
                        } catch (Throwable t) {
                            // doSlowOpen completes candidate cleanup before publishing failure.
                            if (t instanceof RuntimeException) {
                                throw (RuntimeException) t;
                            }
                            throw new KvStorageException(
                                    "KvTablet open failed for " + tablet.getTableBucket(), t);
                        }
                        return;

                    case CLOSING:
                    case CLOSED:
                        throw new KvStorageException(
                                "KvTablet is closed for " + tablet.getTableBucket());

                    default:
                        throw new IllegalStateException("Unexpected state: " + lazyState);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KvStorageException(
                    "Interrupted waiting for KvTablet open: " + tablet.getTableBucket(), e);
        } finally {
            if (lazyStateLock.isHeldByCurrentThread()) {
                lazyStateLock.unlock();
            }
        }
    }

    private void doSlowOpen(int myLeaderEpoch, int myBucketEpoch, boolean localDataExists)
            throws Exception {
        KvTablet candidate = null;
        boolean committed = false;
        boolean semaphoreAcquired = false;
        Throwable failure = null;
        boolean cleanupFailed = false;
        try {
            if (openSemaphore != null) {
                semaphoreAcquired =
                        openSemaphore.tryAcquire(OPEN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (!semaphoreAcquired) {
                    throw new KvStorageException(
                            "KvTablet open semaphore timeout for " + tablet.getTableBucket());
                }
            }
            candidate = openCallback.doOpen(localDataExists);
            lazyStateLock.lock();
            try {
                int leaderEpoch = leaderEpochSupplier.getAsInt();
                int bucketEpoch = bucketEpochSupplier.getAsInt();
                if (lazyState != LazyState.OPENING
                        || leaderEpoch != myLeaderEpoch
                        || bucketEpoch != myBucketEpoch) {
                    throw new CancellationException(
                            "open result fenced by concurrent epoch or lifecycle change");
                }
                tablet.installRocksDB(candidate);
                committed = true;
                transitionTo(LazyState.OPEN);
                failureCount = 0;
                lastAccessTimestamp = clock.milliseconds();

                lazyStateChanged.signalAll();
            } finally {
                lazyStateLock.unlock();
            }
            try {
                commitCallback.accept(tablet);
            } catch (Exception e) {
                LOG.warn("Post-open commit callback failed for {}", tablet.getTableBucket(), e);
            }
        } catch (Exception | Error e) {
            failure = e;
            throw e;
        } finally {
            try {
                if (!committed && candidate != null) {
                    candidate.close();
                    candidate.deleteLocalDirectory();
                }
            } catch (Exception | Error e) {
                cleanupFailed = true;
                if (failure != null) {
                    failure.addSuppressed(e);
                } else {
                    failure = e;
                    throw e;
                }
            } finally {
                if (semaphoreAcquired) {
                    openSemaphore.release();
                }
                lazyStateLock.lock();
                try {
                    if (cleanupFailed) {
                        failedOpenTablet = candidate;

                        transitionTo(LazyState.CLOSING);
                    } else if (!committed && lazyState != LazyState.CLOSING) {
                        transitionTo(LazyState.FAILED);
                        failedTimestamp = clock.milliseconds();
                        failureCount =
                                failure instanceof CancellationException ? 0 : failureCount + 1;
                        lastFailureCause = failure;
                    }
                    operationInFlight = false;
                    lazyStateChanged.signalAll();
                } finally {
                    lazyStateLock.unlock();
                }
            }
        }
    }

    /** Must be called while holding {@code lazyStateLock}. */
    private void transitionTo(LazyState state) {
        updateStateGauge(lazyState, -1);
        updateStateGauge(state, 1);
        lazyState = state;
    }

    boolean canRelease(long closeIdleIntervalMs, long nowMs) {
        return lazyState == LazyState.OPEN
                && activePins.get() == 0
                && nowMs - lastAccessTimestamp >= closeIdleIntervalMs;
    }

    /**
     * Releases idle native resources; returns false if data, leases or callbacks prevent release.
     */
    boolean releaseKv() {
        lazyStateLock.lock();
        try {
            if (lazyState != LazyState.OPEN || operationInFlight) {
                return false;
            }

            transitionTo(LazyState.RELEASING);
            operationInFlight = true;
            if (!drainPins()
                    || lazyState == LazyState.CLOSING
                    || tablet.getFlushedLogOffset() < tablet.logTablet.localLogEndOffset()
                    || tablet.hasActiveResourceLeases()) {
                if (lazyState == LazyState.RELEASING) {
                    transitionTo(LazyState.OPEN);
                }
                operationInFlight = false;
                lazyStateChanged.signalAll();
                return false;
            }
            tablet.setRowCount(tablet.currentRowCount());
            tablet.setFlushedLogOffset(tablet.getFlushedLogOffset());
        } finally {
            lazyStateLock.unlock();
        }

        boolean releaseSucceeded = false;
        boolean nativeCloseStarted = false;
        try {
            if (releaseCallback != null) {
                releaseCallback.accept(tablet);
            }
            nativeCloseStarted = true;
            tablet.detachRocksDB(KvCloseMode.PRESERVE_LOCAL_STATE);
            releaseSucceeded = true;
        } catch (Exception e) {
            LOG.warn("Failed to release KV tablet {}", tablet.getTableBucket(), e);
        } finally {
            lazyStateLock.lock();
            try {
                if (lazyState == LazyState.RELEASING) {
                    if (releaseSucceeded) {
                        transitionTo(LazyState.LAZY);
                        hasLocalData = true;

                    } else if (!nativeCloseStarted) {
                        transitionTo(LazyState.OPEN);
                    } else {
                        transitionTo(LazyState.CLOSING);
                    }
                }
                operationInFlight = false;
                lazyStateChanged.signalAll();
            } finally {
                lazyStateLock.unlock();
            }
        }
        return releaseSucceeded;
    }

    /** Serializes close callers without holding the state lock during callbacks or native I/O. */
    synchronized void close(KvCloseMode closeMode, boolean deleteDirectory) {
        if (lazyState == LazyState.CLOSED) {
            if (deleteDirectory && !localDirectoryDeleted) {
                tablet.deleteLocalDirectory();
                localDirectoryDeleted = true;
            }
            return;
        }
        lazyStateLock.lock();
        try {

            transitionTo(LazyState.CLOSING);
            lazyStateChanged.signalAll();
            // A request timeout cannot transfer ownership of native resources or the local path.
            while (operationInFlight || activePins.get() > 0) {
                lazyStateChanged.awaitUninterruptibly();
            }
        } finally {
            lazyStateLock.unlock();
        }

        if (releaseCallback != null) {
            releaseCallback.accept(tablet);
        }
        if (failedOpenTablet != null) {
            try {
                failedOpenTablet.close();
                failedOpenTablet.deleteLocalDirectory();
                failedOpenTablet = null;
            } catch (Exception e) {
                throw new KvStorageException(
                        "Failed to clean up fenced open for " + tablet.getTableBucket(), e);
            }
        }
        tablet.detachRocksDB(closeMode);
        if (deleteDirectory) {
            tablet.deleteLocalDirectory();
            localDirectoryDeleted = true;
        }
        lazyStateLock.lock();
        try {
            transitionTo(LazyState.CLOSED);
            lastFailureCause = null;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }
    }

    private boolean drainPins() {
        long deadline = clock.milliseconds() + releaseDrainTimeoutMs;
        while (activePins.get() > 0) {
            long remaining = deadline - clock.milliseconds();
            if (remaining <= 0) {
                return false;
            }
            try {
                lazyStateChanged.await(remaining, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }
}

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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Manages the lazy lifecycle state machine for a {@link KvTablet}. Encapsulates all state
 * transitions (LAZY → OPENING → OPEN → RELEASING → LAZY), pin management, open/release/drop logic,
 * and callback orchestration.
 *
 * <p>This class is only instantiated for lazy-mode tablets. EAGER-mode tablets do not use this
 * class.
 */
public final class KvTabletLazyLifecycle {
    private static final Logger LOG = LoggerFactory.getLogger(KvTabletLazyLifecycle.class);

    /** Coarsen access timestamp updates to reduce volatile writes on hot path. */
    private static final long ACCESS_TIMESTAMP_GRANULARITY_MS = 1000;

    // ---- Lazy lifecycle states ----

    /** Internal RocksDB lazy lifecycle states. */
    enum LazyState {
        /** Lazy mode: RocksDB not yet opened. */
        LAZY,
        /** Lazy mode: RocksDB is being opened by another thread. */
        OPENING,
        /** Lazy mode: RocksDB is open and serving requests. */
        OPEN,
        /** Lazy mode: RocksDB is being released (closing without deleting data). */
        RELEASING,
        /** Lazy mode: last open attempt failed, in backoff cooldown. */
        FAILED,
        /**
         * Terminal state: tablet is closed and will not be reopened. Local data may or may not have
         * been deleted depending on whether close or drop was called.
         */
        CLOSED
    }

    // ---- Callback interfaces ----

    /**
     * Callback for the actual RocksDB open work. Provided by Replica since it requires access to
     * snapshot context, log recovery, etc.
     */
    @FunctionalInterface
    public interface OpenCallback {
        /** Performs the open and returns the opened RocksDB KvTablet instance. */
        KvTablet doOpen(boolean hasLocalData) throws Exception;
    }

    /**
     * Callback invoked after a successful open commit. Used by Replica to install the tablet
     * reference and start periodic snapshot.
     */
    @FunctionalInterface
    public interface OpenCommitCallback {
        /** Called after RocksDB state has been installed into the sentinel. */
        void onOpenCommitted(KvTablet kvTablet);
    }

    /** Callback for releasing (closing) RocksDB without destroying local data. */
    @FunctionalInterface
    public interface ReleaseCallback {
        /** Performs release-time cleanup (e.g. stop snapshots, unregister metrics). */
        void doRelease(KvTablet kvTablet);
    }

    /** Callback for dropping (destroying) a KvTablet and cleaning up associated resources. */
    @FunctionalInterface
    public interface DropCallback {
        /** Performs drop-time cleanup (e.g. unregister metrics, stop snapshots). */
        void doDrop(KvTablet kvTablet);
    }

    // ---- The owning tablet ----

    private final KvTablet tablet;

    // ---- State machine fields ----

    private volatile LazyState lazyState = LazyState.LAZY;

    /** Lock guarding lazy state transitions. */
    private final ReentrantLock lazyStateLock = new ReentrantLock();

    private final Condition lazyStateChanged = lazyStateLock.newCondition();

    /** Active pin count — prevents release while operations are in-flight. */
    private final AtomicInteger activePins = new AtomicInteger(0);

    private volatile boolean rejectNewPins;

    /** Generation counter for fencing stale open results. */
    private long openGeneration;

    /** Semaphore for throttling concurrent open operations across all tablets. */
    private @Nullable Semaphore openSemaphore;

    private long openTimeoutMs;
    private long releaseDrainTimeoutMs;
    private @Nullable ExponentialBackoff failedBackoff;
    private Clock clock = SystemClock.getInstance();

    /** FAILED state tracking. */
    private long failedTimestamp;

    private int failureCount;
    private @Nullable Throwable lastFailureCause;

    /** Cached values served when RocksDB is not open. */
    private volatile long cachedRowCount;

    private volatile long cachedFlushedLogOffset;

    /** Whether local RocksDB data directory exists from a previous open. */
    @GuardedBy("lazyStateLock")
    private boolean hasLocalData;

    /** Access timestamp for idle release decisions (coarsened to reduce volatile writes). */
    private volatile long lastAccessTimestamp;

    /** Epoch suppliers for fencing stale open results. */
    private @Nullable IntSupplier leaderEpochSupplier;

    private @Nullable IntSupplier bucketEpochSupplier;

    /** Supplier for tablet directory path — used by sentinel in LAZY/FAILED state. */
    private @Nullable Supplier<File> tabletDirSupplier;

    /** Callbacks (set by Replica after construction). */
    private @Nullable OpenCallback openCallback;

    private @Nullable OpenCommitCallback commitCallback;
    private @Nullable DropCallback dropCallback;
    private @Nullable ReleaseCallback releaseCallback;

    /** Tracks post-open finalization that runs after OPEN becomes externally visible. */
    private boolean commitCallbackInFlight;

    KvTabletLazyLifecycle(KvTablet tablet) {
        this.tablet = tablet;
    }

    // ---- Configuration (called by Replica after sentinel creation) ----

    /**
     * Configure lazy open parameters. Must be called before any {@link #acquireGuard()} calls.
     *
     * @param clock the clock for time-based operations
     * @param openSemaphore semaphore to limit concurrent open operations (can be null)
     * @param openTimeoutMs timeout for open operations in milliseconds
     * @param failedBackoffBaseMs base backoff time for failed opens in milliseconds
     * @param failedBackoffMaxMs max backoff time for failed opens in milliseconds
     * @param releaseDrainTimeoutMs timeout for draining pins during release in milliseconds
     */
    public void configureLazyOpen(
            Clock clock,
            Semaphore openSemaphore,
            long openTimeoutMs,
            long failedBackoffBaseMs,
            long failedBackoffMaxMs,
            long releaseDrainTimeoutMs) {
        this.clock = clock;
        this.openSemaphore = openSemaphore;
        this.openTimeoutMs = openTimeoutMs;
        this.failedBackoff = new ExponentialBackoff(failedBackoffBaseMs, 2, failedBackoffMaxMs, 0);
        this.releaseDrainTimeoutMs = releaseDrainTimeoutMs;
        // Register initial LAZY state in metrics
        updateStateGauges(null, LazyState.LAZY);
    }

    /**
     * Sets the callback responsible for performing the actual RocksDB open operation. This is
     * provided by Replica since it requires access to snapshot context, log recovery, etc.
     *
     * @param callback the open callback to invoke when transitioning from LAZY to OPEN
     */
    public void setOpenCallback(OpenCallback callback) {
        this.openCallback = callback;
    }

    /**
     * Sets the callback invoked after a successful open commit. Used by Replica to install the
     * tablet reference and start periodic snapshot.
     *
     * @param callback the commit callback to invoke after RocksDB state is installed
     */
    public void setCommitCallback(OpenCommitCallback callback) {
        this.commitCallback = callback;
    }

    /**
     * Sets the callback for full-state cleanup when the tablet is dropped. Handles all states,
     * waits for in-progress operations, then transitions to CLOSED.
     *
     * @param callback the drop callback to invoke during tablet deletion
     */
    public void setDropCallback(DropCallback callback) {
        this.dropCallback = callback;
    }

    /**
     * Sets the callback for releasing (closing) RocksDB without destroying local data. Called by
     * the idle release controller when transitioning from OPEN to LAZY.
     *
     * @param callback the release callback to invoke during idle release
     */
    public void setReleaseCallback(ReleaseCallback callback) {
        this.releaseCallback = callback;
    }

    /**
     * Sets the supplier for the tablet directory path. Used by sentinel in LAZY/FAILED state for
     * cleanup operations.
     *
     * @param supplier the directory supplier
     */
    public void setTabletDirSupplier(Supplier<File> supplier) {
        this.tabletDirSupplier = supplier;
    }

    /** Returns the tablet directory from the supplier, or null if not configured. */
    @Nullable
    File getTabletDir() {
        return tabletDirSupplier != null ? tabletDirSupplier.get() : null;
    }

    /**
     * Sets the supplier for the leader epoch used in fencing stale open operations.
     *
     * @param supplier the leader epoch supplier
     */
    public void setLeaderEpochSupplier(IntSupplier supplier) {
        this.leaderEpochSupplier = supplier;
    }

    /**
     * Sets the supplier for the bucket epoch used in fencing stale open operations.
     *
     * @param supplier the bucket epoch supplier
     */
    public void setBucketEpochSupplier(IntSupplier supplier) {
        this.bucketEpochSupplier = supplier;
    }

    /**
     * Initializes the cached row count from snapshot metadata. Called during sentinel creation to
     * serve read queries while in LAZY state before the first open. Also sets the sentinel's
     * rowCount field so that OPENING state reads return the correct value.
     *
     * @param rowCount the initial row count (-1 if unknown/disabled)
     */
    public void initCachedRowCount(long rowCount) {
        this.cachedRowCount = rowCount;
        tablet.setRowCount(rowCount);
    }

    // ---- State queries ----

    /** Returns whether the tablet is currently in OPEN state with RocksDB loaded and serving. */
    boolean isOpen() {
        return lazyState == LazyState.OPEN;
    }

    /** Returns the current lifecycle state of this lazy tablet. */
    LazyState getLazyState() {
        return lazyState;
    }

    /**
     * Returns the cached row count from the last flush or release operation. This value may be
     * stale but is sufficient for serving read queries while in LAZY state.
     */
    long getCachedRowCount() {
        return cachedRowCount;
    }

    /**
     * Returns the flushed log offset cached from the last flush or release operation. Used to
     * determine how much incremental log needs to be replayed when reopening from local data.
     */
    long getCachedFlushedLogOffset() {
        return cachedFlushedLogOffset;
    }

    /**
     * Returns the timestamp (in milliseconds) of the last access to this tablet. Used by the idle
     * release controller to determine eligibility for release.
     */
    long getLastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    /**
     * Returns the number of active pins that are currently preventing RocksDB from being released.
     * Each pin corresponds to a {@link KvTablet.Guard} held by an in-flight operation.
     */
    int getActivePins() {
        return activePins.get();
    }

    /**
     * Returns true if this tablet is in LAZY or FAILED state (RocksDB not open), meaning
     * KvTablet.getRowCount() should return the cached value instead of querying RocksDB.
     */
    boolean needsCachedRowCount() {
        LazyState s = lazyState;
        return s == LazyState.LAZY || s == LazyState.FAILED;
    }

    // ---- Guard / Pin management ----

    /**
     * Acquire a guard that prevents RocksDB from being released while held. Ensures RocksDB is open
     * (blocking if necessary) and pins it.
     *
     * <p>Must be called OUTSIDE any Replica-level locks (e.g. leaderIsrUpdateLock) to avoid
     * blocking leader transitions during slow opens.
     *
     * @throws IllegalStateException if the lifecycle has not been fully configured via {@link
     *     #configureLazyOpen} and callback setters
     */
    KvTablet.Guard acquireGuard() {
        // Defensive check: ensure all required callbacks are set before allowing access
        if (openCallback == null) {
            throw new IllegalStateException(
                    "KvTabletLazyLifecycle not fully configured: openCallback is null. "
                            + "Ensure configureLazyOpen() and setOpenCallback() are called before use.");
        }

        // Fast path: try to pin without blocking
        KvTablet.Guard fast = tryAcquirePinAsGuard();
        if (fast != null) {
            return fast;
        }

        // Slow path: ensure open, then pin
        ensureOpen();
        // Between ensureOpen() returning and pin, a concurrent release could happen.
        // Retry up to 3 times.
        for (int attempt = 0; ; attempt++) {
            lazyStateLock.lock();
            try {
                if (lazyState == LazyState.OPEN && !rejectNewPins) {
                    activePins.incrementAndGet();
                    touchAccessTimestamp();
                    return new KvTablet.Guard(tablet);
                }
                if (lazyState == LazyState.CLOSED) {
                    throw new KvStorageException(
                            "KvTablet is closed for " + tablet.getTableBucket());
                }
            } finally {
                lazyStateLock.unlock();
            }
            if (attempt >= 2) {
                throw new KvStorageException(
                        "Failed to pin KvTablet after open for " + tablet.getTableBucket());
            }
            // Re-open and retry
            ensureOpen();
        }
    }

    /**
     * Try to acquire a guard only if RocksDB is already OPEN.
     *
     * <p>Unlike {@link #acquireGuard()}, this method never triggers a lazy open. It is intended for
     * background maintenance paths (flush, snapshot init) that must coordinate with idle release
     * but should not reopen a lazy tablet on their own.
     */
    @Nullable
    KvTablet.Guard tryAcquireExistingGuard() {
        return tryAcquirePinAsGuard();
    }

    /**
     * Fast-path pin attempt. Returns Guard if OPEN and accepting pins; null otherwise.
     *
     * <p>Safety relies on the double-check pattern: we optimistically increment the pin count, then
     * re-verify the state. If the state changed concurrently (e.g., release started), the re-check
     * fails and we undo the increment. This is safe on all architectures (x86, ARM) regardless of
     * volatile read ordering between independent variables.
     */
    @Nullable
    private KvTablet.Guard tryAcquirePinAsGuard() {
        if (lazyState == LazyState.OPEN && !rejectNewPins) {
            activePins.incrementAndGet();
            if (lazyState == LazyState.OPEN && !rejectNewPins) {
                touchAccessTimestamp();
                return new KvTablet.Guard(tablet);
            }
            decrementAndMaybeSignal();
        }
        return null;
    }

    /** Release a pin. Called by Guard.close(). */
    void releasePin() {
        decrementAndMaybeSignal();
    }

    private void decrementAndMaybeSignal() {
        if (activePins.decrementAndGet() == 0) {
            if (rejectNewPins) {
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

    /**
     * Update state gauge metrics on state transitions. Only LAZY, OPEN, FAILED are tracked as
     * "stable" states; OPENING, RELEASING, CLOSED fall into default (no-op).
     */
    private void updateStateGauges(@Nullable LazyState from, LazyState to) {
        TabletServerMetricGroup metricGroup = tablet.serverMetricGroup;
        if (metricGroup == null) {
            return;
        }
        if (from != null) {
            switch (from) {
                case LAZY:
                    metricGroup.kvTabletLazyCount().decrementAndGet();
                    break;
                case OPEN:
                    metricGroup.kvTabletOpenCount().decrementAndGet();
                    break;
                case FAILED:
                    metricGroup.kvTabletFailedCount().decrementAndGet();
                    break;
                default:
                    break;
            }
        }
        switch (to) {
            case LAZY:
                metricGroup.kvTabletLazyCount().incrementAndGet();
                break;
            case OPEN:
                metricGroup.kvTabletOpenCount().incrementAndGet();
                break;
            case FAILED:
                metricGroup.kvTabletFailedCount().incrementAndGet();
                break;
            default:
                break;
        }
    }

    // ---- Open management ----

    /**
     * Ensures RocksDB is open. If already OPEN, returns immediately. If LAZY or FAILED (past
     * cooldown), triggers a slow-path open. If OPENING or RELEASING, blocks until state changes.
     */
    private void ensureOpen() {
        lazyStateLock.lock();
        try {
            while (true) {
                switch (lazyState) {
                    case OPEN:
                        return;

                    case OPENING:
                        if (!lazyStateChanged.await(openTimeoutMs, TimeUnit.MILLISECONDS)) {
                            throw new KvStorageException(
                                    "KvTablet open timed out for " + tablet.getTableBucket());
                        }
                        continue;

                    case RELEASING:
                        if (!lazyStateChanged.await(openTimeoutMs, TimeUnit.MILLISECONDS)) {
                            throw new KvStorageException(
                                    "KvTablet open timed out waiting for release: "
                                            + tablet.getTableBucket());
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
                        updateStateGauges(lazyState, LazyState.OPENING);
                        lazyState = LazyState.OPENING;
                        openGeneration++;
                        long myGeneration = openGeneration;
                        int myLeaderEpoch =
                                leaderEpochSupplier != null ? leaderEpochSupplier.getAsInt() : -1;
                        int myBucketEpoch =
                                bucketEpochSupplier != null ? bucketEpochSupplier.getAsInt() : -1;
                        boolean localDataExists = hasLocalData;
                        lazyStateLock.unlock();
                        try {
                            doSlowOpen(myGeneration, myLeaderEpoch, myBucketEpoch, localDataExists);
                        } catch (Throwable t) {
                            // doSlowOpen already called commitOpenResult() which transitions
                            // to FAILED, so no need to handle state transition here.
                            if (t instanceof RuntimeException) {
                                throw (RuntimeException) t;
                            }
                            throw new KvStorageException(
                                    "KvTablet open failed for " + tablet.getTableBucket(), t);
                        }
                        return;

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

    private void doSlowOpen(
            long myGeneration, int myLeaderEpoch, int myBucketEpoch, boolean localDataExists)
            throws Exception {
        KvTablet newTablet = null;
        boolean committed = false;
        boolean semaphoreAcquired = false;

        try {
            if (openSemaphore != null) {
                semaphoreAcquired = openSemaphore.tryAcquire(openTimeoutMs, TimeUnit.MILLISECONDS);
                if (!semaphoreAcquired) {
                    throw new KvStorageException(
                            "KvTablet open semaphore timeout for " + tablet.getTableBucket());
                }
            }

            newTablet = openCallback.doOpen(localDataExists);
            committed =
                    commitOpenResult(myGeneration, myLeaderEpoch, myBucketEpoch, newTablet, null);
            if (!committed) {
                throw new CancellationException(
                        "open result fenced by concurrent epoch/generation change");
            }
        } catch (Exception e) {
            commitOpenResult(myGeneration, myLeaderEpoch, myBucketEpoch, null, e);
            throw e;
        } finally {
            if (!committed && newTablet != null) {
                try {
                    newTablet.close();
                } catch (Exception ignored) {
                }
                // Clean up the directory created by the failed open attempt BEFORE
                // releasing the semaphore, so that a subsequent open cannot start
                // writing to the same canonical path while deletion is in progress.
                File failedDir = newTablet.getKvTabletDir();
                if (failedDir != null) {
                    FileUtils.deleteDirectoryQuietly(failedDir);
                }
            }
            if (semaphoreAcquired) {
                openSemaphore.release();
            }
        }
    }

    private boolean commitOpenResult(
            long myGeneration,
            int myLeaderEpoch,
            int myBucketEpoch,
            @Nullable KvTablet newTablet,
            @Nullable Exception error) {
        lazyStateLock.lock();
        try {
            if (openGeneration != myGeneration) {
                if (lazyState == LazyState.OPENING) {
                    transitionToFailed(
                            new CancellationException(
                                    "open superseded by generation " + openGeneration),
                            true);
                }
                return false;
            }

            if (error != null) {
                transitionToFailed(error, false);
                return false;
            }

            // Epoch fencing
            int currentLeaderEpoch =
                    leaderEpochSupplier != null ? leaderEpochSupplier.getAsInt() : -1;
            int currentBucketEpoch =
                    bucketEpochSupplier != null ? bucketEpochSupplier.getAsInt() : -1;
            if (currentLeaderEpoch != myLeaderEpoch || currentBucketEpoch != myBucketEpoch) {
                transitionToFailed(new CancellationException("epoch changed during open"), true);
                return false;
            }

            // Success — absorb RocksDB state from the newly opened tablet.
            // Note: we set OPEN and signal waiters before running commitCallback (which starts
            // the snapshot manager). Writes during this brief window are safe — data is in
            // RocksDB and the WAL. The commitCallbackInFlight flag prevents release until the
            // callback completes.
            tablet.installRocksDB(newTablet);
            lazyState = LazyState.OPEN;
            failureCount = 0;
            lastAccessTimestamp = clock.milliseconds();
            rejectNewPins = false;
            commitCallbackInFlight = true;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }

        try {
            updateStateGauges(LazyState.OPENING, LazyState.OPEN);

            // Invoke commit callback outside the lock to avoid blocking acquireGuard() callers.
            if (commitCallback != null) {
                commitCallback.onOpenCommitted(tablet);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Post-open commit callback failed for {}, keeping tablet OPEN",
                    tablet.getTableBucket(),
                    e);
        } finally {
            lazyStateLock.lock();
            try {
                commitCallbackInFlight = false;
                lazyStateChanged.signalAll();
            } finally {
                lazyStateLock.unlock();
            }
        }
        return true;
    }

    /** Must be called while holding {@code lazyStateLock}. */
    private void transitionToFailed(Throwable cause, boolean resetCount) {
        updateStateGauges(LazyState.OPENING, LazyState.FAILED);
        lazyState = LazyState.FAILED;
        failedTimestamp = clock.milliseconds();
        if (resetCount) {
            failureCount = 0;
        } else {
            failureCount++;
        }
        lastFailureCause = cause;
        lazyStateChanged.signalAll();
    }

    // ---- Release logic (idle release by KvManager) ----

    /** Pre-check for idle release eligibility. */
    boolean canRelease(long closeIdleIntervalMs, long nowMs) {
        if (lazyState != LazyState.OPEN) {
            return false;
        }
        if (activePins.get() > 0) {
            return false;
        }
        return nowMs - lastAccessTimestamp >= closeIdleIntervalMs;
    }

    /**
     * Release RocksDB resources: OPEN -> RELEASING -> LAZY. Caches metadata before close for
     * serving queries while in LAZY state.
     *
     * @return true if release succeeded, false if aborted
     */
    boolean releaseKv() {
        // Pre-check state outside lock as a fast-path rejection
        if (lazyState != LazyState.OPEN) {
            return false;
        }

        lazyStateLock.lock();
        try {
            if (lazyState != LazyState.OPEN) {
                return false;
            }

            // Transition to RELEASING — write rejectNewPins before lazyState so that
            // tryAcquirePinAsGuard() sees the reject flag if it reads OPEN.
            rejectNewPins = true;
            lazyState = LazyState.RELEASING;

            if (!drainPins()) {
                LOG.warn("{} release drain timeout, aborting release", tablet.getTableBucket());
                transitionBackToOpen();
                return false;
            }

            // Re-check flush progress only after blocking new pins and draining
            // all in-flight operations. This closes the race where a writer acquired
            // a guard before RELEASING but appended after an earlier LEO check.
            long logEndOffset = tablet.logTablet.localLogEndOffset();
            if (tablet.flushedLogOffset < logEndOffset) {
                transitionBackToOpen();
                return false;
            }

            if (!awaitCommitCallbackCompletion()) {
                LOG.warn(
                        "{} release waited too long for open commit finalization, aborting release",
                        tablet.getTableBucket());
                transitionBackToOpen();
                return false;
            }

            // Cache metadata AFTER drain — all pinned operations have completed,
            // so rowCount reflects the latest flushed state.
            cachedRowCount = tablet.rowCount;
            cachedFlushedLogOffset = tablet.flushedLogOffset;
        } finally {
            lazyStateLock.unlock();
        }

        // All pins drained, safe to close
        boolean releaseSucceeded = false;
        try {
            if (releaseCallback != null) {
                releaseCallback.doRelease(tablet);
            }
            tablet.detachRocksDB();
            releaseSucceeded = true;
        } catch (Exception e) {
            LOG.warn(
                    "{} release callback failed, rolling back to OPEN", tablet.getTableBucket(), e);
        } finally {
            lazyStateLock.lock();
            try {
                if (releaseSucceeded) {
                    lazyState = LazyState.LAZY;
                    hasLocalData = true;
                    updateStateGauges(LazyState.OPEN, LazyState.LAZY);
                    rejectNewPins = false;
                    lazyStateChanged.signalAll();
                } else {
                    transitionBackToOpen();
                }
            } finally {
                lazyStateLock.unlock();
            }
        }
        return releaseSucceeded;
    }

    // ---- Drop logic ----

    /**
     * Full-state cleanup for lazy mode: handles all states, waits for in-progress operations, then
     * transitions to CLOSED. Called by Replica.dropKv().
     */
    void dropKvLazy() {
        lazyStateLock.lock();
        try {
            switch (lazyState) {
                case CLOSED:
                    break;

                case LAZY:
                case FAILED:
                    tablet.deleteLocalDirectory();
                    break;

                case OPENING:
                    // Fence the in-progress open so commitOpenResult() will reject it.
                    // If awaitStateExit times out (open thread stuck on semaphore or slow I/O),
                    // the open thread will eventually call commitOpenResult(), find the generation
                    // mismatch, and transition to FAILED. The final CLOSED transition at the
                    // bottom of this method will override FAILED. The open thread's finally block
                    // in doSlowOpen will close the newTablet, preventing resource leaks.
                    openGeneration++;
                    awaitStateExit(LazyState.OPENING, openTimeoutMs * 2);
                    if (lazyState == LazyState.OPEN) {
                        rejectNewPins = true;
                        awaitCommitCallbackCompletion();
                        drainPinsForDrop();
                        dropOpenTablet();
                    } else if (lazyState != LazyState.OPENING) {
                        tablet.deleteLocalDirectory();
                    }
                    break;

                case OPEN:
                    rejectNewPins = true;
                    awaitCommitCallbackCompletion();
                    drainPinsForDrop();
                    dropOpenTablet();
                    break;

                case RELEASING:
                    awaitStateExit(LazyState.RELEASING, releaseDrainTimeoutMs * 2);
                    if (lazyState == LazyState.OPEN) {
                        rejectNewPins = true;
                        awaitCommitCallbackCompletion();
                        drainPinsForDrop();
                        dropOpenTablet();
                    } else if (lazyState != LazyState.RELEASING) {
                        tablet.deleteLocalDirectory();
                    }
                    break;

                default:
                    throw new IllegalStateException("Unexpected state in dropKvLazy: " + lazyState);
            }

            LazyState stateBeforeClose = lazyState;
            if (stateBeforeClose != LazyState.CLOSED) {
                updateStateGauges(stateBeforeClose, LazyState.CLOSED);
            }
            lazyState = LazyState.CLOSED;
            hasLocalData = false;
            rejectNewPins = false;
            failureCount = 0;
            failedTimestamp = 0;
            lastFailureCause = null;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }
    }

    private void drainPinsForDrop() {
        if (!drainPins()) {
            LOG.warn(
                    "{} drop drain timeout with {} active pins, proceeding with close",
                    tablet.getTableBucket(),
                    activePins.get());
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

    /** Waits under {@code lazyStateLock} for post-open finalization to complete. */
    private boolean awaitCommitCallbackCompletion() {
        long deadline = clock.milliseconds() + releaseDrainTimeoutMs;
        while (commitCallbackInFlight) {
            long remaining = deadline - clock.milliseconds();
            if (remaining <= 0) {
                return false;
            }
            try {
                if (!lazyStateChanged.await(remaining, TimeUnit.MILLISECONDS)) {
                    return false;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    /**
     * Wait (under lazyStateLock) until {@code lazyState} is no longer {@code state}, or timeout.
     */
    private void awaitStateExit(LazyState state, long timeoutMs) {
        long deadlineNanos = clock.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (lazyState == state) {
            try {
                long remainNanos = deadlineNanos - clock.nanoseconds();
                if (remainNanos <= 0
                        || !lazyStateChanged.await(remainNanos, TimeUnit.NANOSECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void doDropViaCallback() {
        if (dropCallback != null) {
            dropCallback.doDrop(tablet);
        }
    }

    void closeKvLazy() {
        lazyStateLock.lock();
        try {
            switch (lazyState) {
                case CLOSED:
                    return;
                case LAZY:
                case FAILED:
                    break;
                case OPENING:
                    openGeneration++;
                    awaitStateExit(LazyState.OPENING, openTimeoutMs * 2);
                    if (lazyState == LazyState.OPEN) {
                        rejectNewPins = true;
                        awaitCommitCallbackCompletion();
                        drainPins();
                        tablet.detachRocksDB();
                    }
                    break;
                case OPEN:
                    rejectNewPins = true;
                    awaitCommitCallbackCompletion();
                    drainPins();
                    tablet.detachRocksDB();
                    break;
                case RELEASING:
                    awaitStateExit(LazyState.RELEASING, releaseDrainTimeoutMs * 2);
                    if (lazyState == LazyState.OPEN) {
                        rejectNewPins = true;
                        awaitCommitCallbackCompletion();
                        drainPins();
                        tablet.detachRocksDB();
                    }
                    break;
                default:
                    throw new IllegalStateException(
                            "Unexpected state in closeKvLazy: " + lazyState);
            }

            LazyState stateBeforeClose = lazyState;
            if (stateBeforeClose != LazyState.CLOSED) {
                updateStateGauges(stateBeforeClose, LazyState.CLOSED);
            }
            lazyState = LazyState.CLOSED;
            hasLocalData = false;
            rejectNewPins = false;
            failureCount = 0;
            failedTimestamp = 0;
            lastFailureCause = null;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }
    }

    /** Must be called while holding {@code lazyStateLock} and after pins have drained. */
    private void dropOpenTablet() {
        doDropViaCallback();
        tablet.detachRocksDB();
        tablet.deleteLocalDirectory();
    }

    /** Must be called while holding {@code lazyStateLock}. */
    private void transitionBackToOpen() {
        lazyState = LazyState.OPEN;
        rejectNewPins = false;
        lazyStateChanged.signalAll();
    }

    // ---- Test helpers ----

    @VisibleForTesting
    void setLazyStateForTesting(LazyState state) {
        lazyStateLock.lock();
        try {
            this.lazyState = state;
            lazyStateChanged.signalAll();
        } finally {
            lazyStateLock.unlock();
        }
    }

    @VisibleForTesting
    void setLastAccessTimestampForTesting(long timestamp) {
        this.lastAccessTimestamp = timestamp;
    }

    @VisibleForTesting
    void setClockForTesting(Clock clock) {
        this.clock = clock;
    }
}

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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.server.coordinator.event.DropPartitionEvent;
import org.apache.fluss.server.coordinator.event.DropTableEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * Coordinator-wide manager that gates the rate at which {@link DropPartitionEvent} / {@link
 * DropTableEvent} are admitted into the coordinator event queue.
 *
 * <p>Why pre-queue throttling: a cascading {@code DROP TABLE} or auto-partition expiration can
 * delete N partition znodes from ZooKeeper synchronously, which fans out into N {@code
 * NODE_DELETED} watcher callbacks. If each callback directly enqueued a {@code DropPartitionEvent},
 * the coordinator event queue would be flooded with N drop events ahead of unrelated work like
 * leader election, heartbeat handling and metadata changes.
 *
 * <p>This manager intercepts that step. Watchers (and other drop sources) call {@link
 * #submitPartitionDrop} / {@link #submitTableDrop} which buffer a lightweight {@code PendingDrop}
 * in an in-memory FIFO queue. The manager admits <b>one</b> drop at a time into the coordinator
 * event queue. The next drop is admitted only after the current in-flight drop completes (all
 * replicas reach {@code DeletionSuccessful}) or times out.
 *
 * <p>Coordinator startup also routes through this manager via {@link #submitPartitionDropForResume}
 * / {@link #submitTableDropForResume}: stale tables/partitions detected by {@code
 * initCoordinatorContext} are submitted with a {@link Runnable} that drives {@code
 * TableManager#onDeleteTable} / {@code TableManager#onDeletePartition} directly (their {@code
 * TableInfo} is already gone, so a regular {@code DropTableEvent} is not viable).
 *
 * <p>Timeout-based abandon: the in-flight drop is timestamped at admission time. A periodic task
 * checks whether the completion callback has not arrived within the hardcoded timeout (3 minutes)
 * and abandons the drop with a WARN log. Abandonment only releases the manager's in-memory
 * tracking; any residual replica state machine entries are reconciled on the next coordinator
 * startup via {@link TableManager#resumeDeletions()}.
 */
public class ReplicaCleanupManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaCleanupManager.class);

    /** Hardcoded timeout for in-flight drops: 3 minutes. */
    private static final long INFLIGHT_TIMEOUT_MS = 3 * 60 * 1000L;

    /** Hardcoded interval for the periodic timeout check: 1 minute. */
    private static final long TIMEOUT_CHECK_INTERVAL_MS = 60 * 1000L;

    private final EventManager eventManager;
    private final Clock clock;
    private final ScheduledExecutorService timeoutChecker;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Lock lock = new ReentrantLock();

    /** Pending drops waiting to be admitted into the coordinator event queue. */
    @GuardedBy("lock")
    private final Deque<PendingDrop> pendingDrops = new ArrayDeque<>();

    /** The currently in-flight drop, or {@code null} if nothing is in-flight. */
    @GuardedBy("lock")
    @Nullable
    private InflightDrop currentInflight;

    public ReplicaCleanupManager(EventManager eventManager, Clock clock) {
        this(
                eventManager,
                clock,
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("replica-cleanup-timeout")));
    }

    @VisibleForTesting
    ReplicaCleanupManager(
            EventManager eventManager, Clock clock, ScheduledExecutorService timeoutChecker) {
        this.eventManager = eventManager;
        this.clock = clock == null ? SystemClock.getInstance() : clock;
        this.timeoutChecker = timeoutChecker;
    }

    /** Starts the periodic timeout checker and activates one-at-a-time throttling. Idempotent. */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("ReplicaCleanupManager is already closed.");
        }
        if (started.compareAndSet(false, true)) {
            timeoutChecker.scheduleWithFixedDelay(
                    this::checkTimeoutsSafely,
                    TIMEOUT_CHECK_INTERVAL_MS,
                    TIMEOUT_CHECK_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);
            LOG.info(
                    "ReplicaCleanupManager started: one-at-a-time throttling, "
                            + "inflightTimeoutMs={}, timeoutCheckIntervalMs={}",
                    INFLIGHT_TIMEOUT_MS,
                    TIMEOUT_CHECK_INTERVAL_MS);
        }
    }

    /**
     * Submits a partition drop request. The partition's ZK metadata is expected to have been
     * removed by the caller prior to this submission; the manager only governs how fast the
     * resulting {@link DropPartitionEvent} is admitted into the coordinator event queue.
     */
    public void submitPartitionDrop(long tableId, long partitionId, String partitionName) {
        admit(new PendingPartitionDrop(tableId, partitionId, partitionName, null));
    }

    /**
     * Submits a partition drop request that, when admitted, runs {@code resumeAction} instead of
     * putting a {@link DropPartitionEvent} into the coordinator event queue. Used by coordinator
     * startup to reconcile stale partitions whose {@code TableInfo} is already gone.
     */
    public void submitPartitionDropForResume(
            long tableId, long partitionId, String partitionName, Runnable resumeAction) {
        admit(new PendingPartitionDrop(tableId, partitionId, partitionName, resumeAction));
    }

    /**
     * Submits a table drop request. The table's ZK metadata is expected to have been removed by the
     * caller prior to this submission.
     */
    public void submitTableDrop(
            long tableId,
            boolean isPartitionedTable,
            boolean isAutoPartitionTable,
            boolean isDataLakeEnabled) {
        admit(
                new PendingTableDrop(
                        tableId,
                        isPartitionedTable,
                        isAutoPartitionTable,
                        isDataLakeEnabled,
                        null));
    }

    /**
     * Submits a table drop request that, when admitted, runs {@code resumeAction} instead of
     * putting a {@link DropTableEvent} into the coordinator event queue. Used by coordinator
     * startup to reconcile stale tables whose {@code TableInfo} is already gone.
     */
    public void submitTableDropForResume(long tableId, Runnable resumeAction) {
        admit(new PendingTableDrop(tableId, false, false, false, resumeAction));
    }

    /**
     * Called by {@link TableManager} when all replicas of a partition have reached the {@code
     * DeletionSuccessful} state. Releases the in-flight slot and admits the next pending drop.
     */
    public void onPartitionDropCompleted(TablePartition tablePartition) {
        PendingDrop next;
        lock.lock();
        try {
            if (currentInflight == null) {
                return;
            }
            PendingDrop inflight = currentInflight.drop;
            if (!(inflight instanceof PendingPartitionDrop)) {
                return;
            }
            PendingPartitionDrop p = (PendingPartitionDrop) inflight;
            if (p.tableId != tablePartition.getTableId()
                    || p.partitionId != tablePartition.getPartitionId()) {
                return;
            }
            LOG.debug(
                    "Partition drop completed: {} after {}ms; pending={}",
                    tablePartition,
                    clock.milliseconds() - currentInflight.submittedAtMs,
                    pendingDrops.size());
            currentInflight = null;
            next = admitNext();
        } finally {
            lock.unlock();
        }
        executeOutsideLock(next);
    }

    /**
     * Called by {@link TableManager} when all replicas of a table have reached the {@code
     * DeletionSuccessful} state. Releases the in-flight slot and admits the next pending drop.
     */
    public void onTableDropCompleted(long tableId) {
        PendingDrop next;
        lock.lock();
        try {
            if (currentInflight == null) {
                return;
            }
            PendingDrop inflight = currentInflight.drop;
            if (!(inflight instanceof PendingTableDrop)) {
                return;
            }
            PendingTableDrop t = (PendingTableDrop) inflight;
            if (t.tableId != tableId) {
                return;
            }
            LOG.debug(
                    "Table drop completed: tableId={} after {}ms; pending={}",
                    tableId,
                    clock.milliseconds() - currentInflight.submittedAtMs,
                    pendingDrops.size());
            currentInflight = null;
            next = admitNext();
        } finally {
            lock.unlock();
        }
        executeOutsideLock(next);
    }

    private void admit(PendingDrop drop) {
        if (closed.get()) {
            LOG.debug(
                    "Ignoring drop submission ({}) because ReplicaCleanupManager is closed.", drop);
            return;
        }
        // Before start() is called, throttling is inactive: execute immediately.
        // This covers early watcher callbacks that fire before the manager is activated.
        if (!started.get()) {
            drop.execute(eventManager);
            return;
        }
        PendingDrop toExecute;
        lock.lock();
        try {
            // Deduplicate: skip if the same target is already in-flight or pending.
            // This prevents stale resume drops from accumulating when resumeDeletions()
            // is called while the original watcher-triggered drop is still in progress.
            if (isDuplicate(drop)) {
                LOG.debug("Skipping duplicate drop submission: {}", drop);
                return;
            }
            pendingDrops.add(drop);
            LOG.debug(
                    "Submitted drop: {}; pending={} inflight={}",
                    drop,
                    pendingDrops.size(),
                    currentInflight != null);
            toExecute = admitNext();
        } finally {
            lock.unlock();
        }
        executeOutsideLock(toExecute);
    }

    /**
     * Returns {@code true} if a drop targeting the same table/partition is already tracked. Must be
     * called under lock.
     */
    @GuardedBy("lock")
    private boolean isDuplicate(PendingDrop drop) {
        if (currentInflight != null && currentInflight.drop.hasSameTarget(drop)) {
            return true;
        }
        for (PendingDrop pending : pendingDrops) {
            if (pending.hasSameTarget(drop)) {
                return true;
            }
        }
        return false;
    }

    /**
     * If nothing is in-flight, pulls the next pending drop and marks it as in-flight. Must be
     * called under lock.
     *
     * @return the drop to execute outside the lock, or {@code null} if nothing was admitted.
     */
    @GuardedBy("lock")
    @Nullable
    private PendingDrop admitNext() {
        if (currentInflight != null || pendingDrops.isEmpty()) {
            return null;
        }
        PendingDrop next = pendingDrops.poll();
        currentInflight = new InflightDrop(next, clock.milliseconds());
        return next;
    }

    /** Executes a drop outside the lock. No-op if {@code drop} is null. */
    private void executeOutsideLock(@Nullable PendingDrop drop) {
        if (drop == null) {
            return;
        }
        try {
            drop.execute(eventManager);
            // For fire-and-forget drops (e.g., auto-partition table drops), no completion
            // callback will ever arrive because there are no table-level replicas to delete.
            // Release the inflight slot immediately and admit the next pending drop.
            if (drop.isFireAndForget()) {
                PendingDrop next;
                lock.lock();
                try {
                    currentInflight = null;
                    next = admitNext();
                } finally {
                    lock.unlock();
                }
                executeOutsideLock(next);
            }
        } catch (Throwable t) {
            LOG.error("Failed to execute drop {}; abandoning in-memory tracking.", drop, t);
            lock.lock();
            try {
                currentInflight = null;
                // Try to admit the next pending drop despite the failure.
                PendingDrop next = admitNext();
                if (next != null) {
                    // Recursive execution outside lock; safe because admitNext() won't
                    // return non-null again until this next drop completes.
                    executeOutsideLock(next);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void checkTimeoutsSafely() {
        try {
            checkTimeouts();
        } catch (Throwable t) {
            LOG.error("Unexpected error in ReplicaCleanupManager timeout check.", t);
        }
    }

    @VisibleForTesting
    void checkTimeouts() {
        PendingDrop next;
        lock.lock();
        try {
            if (currentInflight == null) {
                return;
            }
            long elapsed = clock.milliseconds() - currentInflight.submittedAtMs;
            if (elapsed <= INFLIGHT_TIMEOUT_MS) {
                return;
            }
            LOG.warn(
                    "In-flight drop {} timed out after {}ms with no completion callback. "
                            + "Abandoning in-memory tracking; ZK metadata is already gone, "
                            + "any residual replica state will be reconciled on next "
                            + "coordinator startup.",
                    currentInflight.drop,
                    elapsed);
            currentInflight = null;
            next = admitNext();
        } finally {
            lock.unlock();
        }
        executeOutsideLock(next);
    }

    @VisibleForTesting
    int getInflightCount() {
        return inLock(lock, () -> currentInflight != null ? 1 : 0);
    }

    @VisibleForTesting
    int getPendingDropCount() {
        return inLock(lock, pendingDrops::size);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            timeoutChecker.shutdownNow();
        }
    }

    // ------------------------------------------------------------------------------------------
    // PendingDrop hierarchy
    // ------------------------------------------------------------------------------------------

    /** A unit of work queued for admission into the coordinator event queue. */
    interface PendingDrop {
        void execute(EventManager eventManager);

        /**
         * Whether this drop completes immediately without waiting for a completion callback.
         * Auto-partition table drops return {@code true} because partitioned tables have no
         * table-level replicas, so {@link #onTableDropCompleted} will never be called.
         */
        default boolean isFireAndForget() {
            return false;
        }

        /**
         * Returns {@code true} if this drop targets the same table or partition as {@code other}.
         * Used for deduplication: if a drop with the same target is already in-flight or pending, a
         * duplicate submission is skipped.
         */
        boolean hasSameTarget(PendingDrop other);
    }

    /** Pending admission of a {@link DropPartitionEvent} for a single partition. */
    static final class PendingPartitionDrop implements PendingDrop {
        final long tableId;
        final long partitionId;
        final String partitionName;
        @Nullable final Runnable resumeAction;

        PendingPartitionDrop(
                long tableId,
                long partitionId,
                String partitionName,
                @Nullable Runnable resumeAction) {
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.partitionName = partitionName;
            this.resumeAction = resumeAction;
        }

        @Override
        public void execute(EventManager eventManager) {
            if (resumeAction != null) {
                resumeAction.run();
            } else {
                eventManager.put(new DropPartitionEvent(tableId, partitionId, partitionName));
            }
        }

        @Override
        public boolean hasSameTarget(PendingDrop other) {
            if (!(other instanceof PendingPartitionDrop)) {
                return false;
            }
            PendingPartitionDrop that = (PendingPartitionDrop) other;
            return this.tableId == that.tableId && this.partitionId == that.partitionId;
        }

        @Override
        public String toString() {
            return "PendingPartitionDrop{tableId="
                    + tableId
                    + ", partitionId="
                    + partitionId
                    + ", partitionName='"
                    + partitionName
                    + "', resume="
                    + (resumeAction != null)
                    + "}";
        }
    }

    /** Pending admission of a {@link DropTableEvent} for a whole table. */
    static final class PendingTableDrop implements PendingDrop {
        final long tableId;
        final boolean isPartitionedTable;
        final boolean isAutoPartitionTable;
        final boolean isDataLakeEnabled;
        @Nullable final Runnable resumeAction;

        PendingTableDrop(
                long tableId,
                boolean isPartitionedTable,
                boolean isAutoPartitionTable,
                boolean isDataLakeEnabled,
                @Nullable Runnable resumeAction) {
            this.tableId = tableId;
            this.isPartitionedTable = isPartitionedTable;
            this.isAutoPartitionTable = isAutoPartitionTable;
            this.isDataLakeEnabled = isDataLakeEnabled;
            this.resumeAction = resumeAction;
        }

        @Override
        public boolean isFireAndForget() {
            // Partitioned tables have no table-level replicas, so the completion callback
            // (onTableDropCompleted) will never arrive. Skip inflight tracking.
            // Resume-action drops are excluded because their resumeAction triggers
            // onDeleteTable() which may send RPCs for non-vacuously-complete cases.
            return isPartitionedTable && resumeAction == null;
        }

        @Override
        public void execute(EventManager eventManager) {
            if (resumeAction != null) {
                resumeAction.run();
            } else {
                eventManager.put(
                        new DropTableEvent(tableId, isAutoPartitionTable, isDataLakeEnabled));
            }
        }

        @Override
        public boolean hasSameTarget(PendingDrop other) {
            if (!(other instanceof PendingTableDrop)) {
                return false;
            }
            PendingTableDrop that = (PendingTableDrop) other;
            return this.tableId == that.tableId;
        }

        @Override
        public String toString() {
            return "PendingTableDrop{tableId="
                    + tableId
                    + ", partitioned="
                    + isPartitionedTable
                    + ", autoPartition="
                    + isAutoPartitionTable
                    + ", dataLake="
                    + isDataLakeEnabled
                    + ", resume="
                    + (resumeAction != null)
                    + "}";
        }
    }

    /** Tracks the in-flight drop along with its admission timestamp for timeout detection. */
    private static final class InflightDrop {
        final PendingDrop drop;
        final long submittedAtMs;

        InflightDrop(PendingDrop drop, long submittedAtMs) {
            this.drop = drop;
            this.submittedAtMs = submittedAtMs;
        }
    }
}

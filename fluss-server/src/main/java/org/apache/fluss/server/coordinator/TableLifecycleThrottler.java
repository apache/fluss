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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.server.coordinator.event.DropPartitionEvent;
import org.apache.fluss.server.coordinator.event.DropTableEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.ResumeDropEvent;
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
 * Coordinator-wide throttler that gates the rate at which {@link DropPartitionEvent} / {@link
 * DropTableEvent} are admitted into the coordinator event queue.
 *
 * <p>Why pre-queue throttling: a cascading {@code DROP TABLE} or auto-partition expiration can
 * delete N partition znodes from ZooKeeper synchronously, which fans out into N {@code
 * NODE_DELETED} watcher callbacks. If each callback directly enqueued a {@code DropPartitionEvent},
 * the coordinator event queue would be flooded with N drop events ahead of unrelated work like
 * leader election, heartbeat handling and metadata changes.
 *
 * <p>This throttler intercepts that step. Watchers (and other drop sources) call {@link
 * #submitPartitionDrop} / {@link #submitTableDrop} which buffer a lightweight {@code PendingDrop}
 * in an in-memory FIFO queue. The throttler admits <b>one</b> drop at a time into the coordinator
 * event queue. The next drop is admitted only after the current in-flight drop completes (all
 * replicas reach {@code DeletionSuccessful}) or times out.
 *
 * <p>Coordinator startup also routes through this throttler via {@link
 * #submitPartitionDropForResume} / {@link #submitTableDropForResume}: stale tables/partitions
 * detected by {@code initCoordinatorContext} (whose {@code TableInfo} is already gone, so a regular
 * {@code DropTableEvent} is not viable). At admission time the throttler enqueues a {@link
 * ResumeDropEvent} carrying only identity (kind + ids); the coordinator event-thread handler routes
 * it to {@code TableManager#onDeleteTable} / {@code TableManager#onDeletePartition}. This API
 * deliberately accepts no {@link Runnable} from the submitter so the (potentially non-event-thread)
 * caller cannot smuggle in logic that would mutate the {@code @NotThreadSafe} {@code
 * CoordinatorContext} off the event thread.
 *
 * <p>Timeout-based abandon: the in-flight drop is timestamped at admission time. A periodic task
 * checks whether the completion callback has not arrived within the configured timeout and abandons
 * the drop with a WARN log. Abandonment only releases the throttler's in-memory tracking; any
 * residual replica state machine entries are reconciled on the next coordinator startup via {@link
 * TableManager#resumeDeletions()}.
 */
public class TableLifecycleThrottler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TableLifecycleThrottler.class);

    private final long inflightTimeoutMs;
    private final long timeoutCheckIntervalMs;

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

    public TableLifecycleThrottler(EventManager eventManager, Clock clock, Configuration conf) {
        this(
                eventManager,
                clock,
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("lifecycle-throttler-timeout")),
                conf.get(ConfigOptions.COORDINATOR_LIFECYCLE_THROTTLER_INFLIGHT_TIMEOUT).toMillis(),
                conf.get(ConfigOptions.COORDINATOR_LIFECYCLE_THROTTLER_TIMEOUT_CHECK_INTERVAL)
                        .toMillis());
    }

    @VisibleForTesting
    TableLifecycleThrottler(
            EventManager eventManager,
            Clock clock,
            ScheduledExecutorService timeoutChecker,
            long inflightTimeoutMs,
            long timeoutCheckIntervalMs) {
        this.eventManager = eventManager;
        this.clock = clock == null ? SystemClock.getInstance() : clock;
        this.timeoutChecker = timeoutChecker;
        this.inflightTimeoutMs = inflightTimeoutMs;
        this.timeoutCheckIntervalMs = timeoutCheckIntervalMs;
    }

    /** Starts the periodic timeout checker. Idempotent. */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("TableLifecycleThrottler is already closed.");
        }
        if (started.compareAndSet(false, true)) {
            timeoutChecker.scheduleWithFixedDelay(
                    this::checkTimeoutsSafely,
                    timeoutCheckIntervalMs,
                    timeoutCheckIntervalMs,
                    TimeUnit.MILLISECONDS);
            LOG.info(
                    "TableLifecycleThrottler timeout checker started: "
                            + "inflightTimeoutMs={}, timeoutCheckIntervalMs={}",
                    inflightTimeoutMs,
                    timeoutCheckIntervalMs);
        }
    }

    /**
     * Submits a partition drop request. The partition's ZK metadata is expected to have been
     * removed by the caller prior to this submission; the throttler only governs how fast the
     * resulting {@link DropPartitionEvent} is admitted into the coordinator event queue.
     */
    public void submitPartitionDrop(long tableId, long partitionId, String partitionName) {
        admit(new PendingPartitionDrop(tableId, partitionId, partitionName, false));
    }

    /**
     * Submits a partition drop request that, when admitted, enqueues a {@link ResumeDropEvent} (so
     * the event-thread handler in {@code CoordinatorEventProcessor} can call {@code
     * TableManager#onDeletePartition}) instead of a {@link DropPartitionEvent}. Used by coordinator
     * startup to reconcile stale partitions whose {@code TableInfo} is already gone.
     */
    public void submitPartitionDropForResume(long tableId, long partitionId, String partitionName) {
        admit(new PendingPartitionDrop(tableId, partitionId, partitionName, true));
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
                        false));
    }

    /**
     * Submits a table drop request that, when admitted, enqueues a {@link ResumeDropEvent} (so the
     * event-thread handler in {@code CoordinatorEventProcessor} can call {@code
     * TableManager#onDeleteTable}) instead of a {@link DropTableEvent}. Used by coordinator startup
     * to reconcile stale tables whose {@code TableInfo} is already gone.
     */
    public void submitTableDropForResume(long tableId) {
        admit(new PendingTableDrop(tableId, false, false, false, true));
    }

    /**
     * Called by {@link TableManager} when all replicas of a partition have reached the {@code
     * DeletionSuccessful} state. Releases the in-flight slot and admits the next pending drop.
     */
    public void onPartitionDropCompleted(TablePartition tablePartition) {
        PendingDrop next =
                inLock(
                        lock,
                        () -> {
                            if (currentInflight == null) {
                                return null;
                            }
                            PendingDrop inflight = currentInflight.drop;
                            if (!(inflight instanceof PendingPartitionDrop)) {
                                return null;
                            }
                            PendingPartitionDrop p = (PendingPartitionDrop) inflight;
                            if (p.tableId != tablePartition.getTableId()
                                    || p.partitionId != tablePartition.getPartitionId()) {
                                return null;
                            }
                            LOG.debug(
                                    "Partition drop completed: {} after {}ms; pending={}",
                                    tablePartition,
                                    clock.milliseconds() - currentInflight.submittedAtMs,
                                    pendingDrops.size());
                            currentInflight = null;
                            return admitNext();
                        });
        executeUntilBlocked(next);
    }

    /**
     * Called by {@link TableManager} when all replicas of a table have reached the {@code
     * DeletionSuccessful} state. Releases the in-flight slot and admits the next pending drop.
     */
    public void onTableDropCompleted(long tableId) {
        PendingDrop next =
                inLock(
                        lock,
                        () -> {
                            if (currentInflight == null) {
                                return null;
                            }
                            PendingDrop inflight = currentInflight.drop;
                            if (!(inflight instanceof PendingTableDrop)) {
                                return null;
                            }
                            PendingTableDrop t = (PendingTableDrop) inflight;
                            if (t.tableId != tableId) {
                                return null;
                            }
                            LOG.debug(
                                    "Table drop completed: tableId={} after {}ms; pending={}",
                                    tableId,
                                    clock.milliseconds() - currentInflight.submittedAtMs,
                                    pendingDrops.size());
                            currentInflight = null;
                            return admitNext();
                        });
        executeUntilBlocked(next);
    }

    private void admit(PendingDrop drop) {
        if (closed.get()) {
            LOG.debug(
                    "Ignoring drop submission ({}) because TableLifecycleThrottler is closed.",
                    drop);
            return;
        }
        PendingDrop toExecute =
                inLock(
                        lock,
                        () -> {
                            // Deduplicate: skip if the same target is already in-flight or pending.
                            // This prevents stale resume drops from accumulating when
                            // resumeDeletions()
                            // is called while the original watcher-triggered drop is still in
                            // progress.
                            if (isDuplicate(drop)) {
                                LOG.debug("Skipping duplicate drop submission: {}", drop);
                                return null;
                            }
                            pendingDrops.add(drop);
                            LOG.debug(
                                    "Submitted drop: {}; pending={} inflight={}",
                                    drop,
                                    pendingDrops.size(),
                                    currentInflight != null);
                            return admitNext();
                        });
        executeUntilBlocked(toExecute);
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

    /** Executes admitted drops until one needs an asynchronous completion callback. */
    private void executeUntilBlocked(@Nullable PendingDrop drop) {
        PendingDrop current = drop;
        while (current != null) {
            if (!executeDrop(current)) {
                return;
            }
            current = releaseInflightAndAdmitNext();
        }
    }

    private boolean executeDrop(PendingDrop drop) {
        drop.putEventInto(eventManager);
        return drop.isFireAndForget();
    }

    private PendingDrop releaseInflightAndAdmitNext() {
        return inLock(
                lock,
                () -> {
                    currentInflight = null;
                    return admitNext();
                });
    }

    private void checkTimeoutsSafely() {
        try {
            checkTimeouts();
        } catch (Throwable t) {
            LOG.error("Unexpected error in TableLifecycleThrottler timeout check.", t);
        }
    }

    @VisibleForTesting
    void checkTimeouts() {
        PendingDrop next =
                inLock(
                        lock,
                        () -> {
                            if (currentInflight == null) {
                                return null;
                            }
                            long elapsed = clock.milliseconds() - currentInflight.submittedAtMs;
                            if (elapsed <= inflightTimeoutMs) {
                                return null;
                            }
                            LOG.warn(
                                    "In-flight drop {} timed out after {}ms with no completion callback. "
                                            + "Abandoning in-memory tracking; ZK metadata is already gone, "
                                            + "any residual replica state will be reconciled on next "
                                            + "coordinator startup.",
                                    currentInflight.drop,
                                    elapsed);
                            currentInflight = null;
                            return admitNext();
                        });
        executeUntilBlocked(next);
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
        void putEventInto(EventManager eventManager);

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
        final boolean forResume;

        PendingPartitionDrop(
                long tableId, long partitionId, String partitionName, boolean forResume) {
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.partitionName = partitionName;
            this.forResume = forResume;
        }

        @Override
        public void putEventInto(EventManager eventManager) {
            if (forResume) {
                // Resume-mode reconciliation: dispatch identity-only event to the coordinator
                // event thread, which mutates the @NotThreadSafe CoordinatorContext. The
                // admission point can be a watcher thread or the timeout-checker thread, so
                // the handler must NOT run inline here.
                eventManager.put(ResumeDropEvent.forPartition(tableId, partitionId));
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
                    + "', forResume="
                    + forResume
                    + "}";
        }
    }

    /** Pending admission of a {@link DropTableEvent} for a whole table. */
    static final class PendingTableDrop implements PendingDrop {
        final long tableId;
        final boolean isPartitionedTable;
        final boolean isAutoPartitionTable;
        final boolean isDataLakeEnabled;
        final boolean forResume;

        PendingTableDrop(
                long tableId,
                boolean isPartitionedTable,
                boolean isAutoPartitionTable,
                boolean isDataLakeEnabled,
                boolean forResume) {
            this.tableId = tableId;
            this.isPartitionedTable = isPartitionedTable;
            this.isAutoPartitionTable = isAutoPartitionTable;
            this.isDataLakeEnabled = isDataLakeEnabled;
            this.forResume = forResume;
        }

        @Override
        public boolean isFireAndForget() {
            // Partitioned tables have no table-level replicas, so the completion callback
            // (onTableDropCompleted) will never arrive. Skip inflight tracking.
            // Resume-mode drops are excluded because the event-thread handler triggers
            // onDeleteTable() which may send RPCs for non-vacuously-complete cases.
            return isPartitionedTable && !forResume;
        }

        @Override
        public void putEventInto(EventManager eventManager) {
            if (forResume) {
                // See PendingPartitionDrop#execute.
                eventManager.put(ResumeDropEvent.forTable(tableId));
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
                    + ", forResume="
                    + forResume
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

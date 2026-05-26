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
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * Coordinator-wide manager that gates the rate at which {@link DropPartitionEvent} / {@link
 * DropTableEvent} are admitted into the coordinator event queue. The synchronous part of a drop
 * (i.e. the ZooKeeper metadata removal performed by {@link MetadataManager}) is intentionally NOT
 * handled here: by the time a drop is submitted to this manager the partition/table is already
 * invisible to clients.
 *
 * <p>Why pre-queue throttling: a cascading {@code DROP TABLE} or auto-partition expiration can
 * delete N partition znodes from ZooKeeper synchronously, which fans out into N {@code
 * NODE_DELETED} watcher callbacks. If each callback directly enqueued a {@code DropPartitionEvent},
 * the coordinator event queue would be flooded with N drop events ahead of unrelated work like
 * leader election, heartbeat handling and metadata changes.
 *
 * <p>This manager intercepts that step. Watchers (and other drop sources) call {@link
 * #submitPartitionDrop} / {@link #submitTableDrop} which buffer a lightweight {@code PendingDrop}
 * (carrying the drop's bucket count) in an in-memory FIFO queue. The manager then dispatches drops
 * to the coordinator event queue as full {@code DropPartitionEvent} / {@code DropTableEvent},
 * capped by a per-batch bucket budget. The {@code CoordinatorEventProcessor} processing logic is
 * unchanged and continues to drive {@code TableManager} state transitions on the coordinator's
 * single event thread.
 *
 * <p>Coordinator startup also routes through this manager via {@link #submitPartitionDropForResume}
 * / {@link #submitTableDropForResume}: stale tables/partitions detected by {@code
 * initCoordinatorContext} are submitted with a {@link Runnable} that drives {@code
 * TableManager#onDeleteTable} / {@code TableManager#onDeletePartition} directly (their {@code
 * TableInfo} is already gone, so a regular {@code DropTableEvent} is not viable). The same FIFO +
 * bucket budget then keeps a startup with thousands of stale tables from blasting all tablet
 * servers with stop-replica RPCs at once.
 *
 * <p>Submission flow:
 *
 * <ol>
 *   <li>A drop source ({@link
 *       org.apache.fluss.server.coordinator.event.watcher.TableChangeWatcher}, coordinator startup,
 *       etc.) calls {@link #submitPartitionDrop} / {@link #submitTableDrop} (or their {@code
 *       *ForResume} variants) together with the drop's bucket count.
 *   <li>Pending drops are buffered. {@link #collectNextBatch()} pulls drops in FIFO order while the
 *       running sum of in-flight bucket counts stays within the per-batch bucket budget. If the
 *       head-of-queue drop alone already exceeds the budget, it is admitted on its own to avoid
 *       starvation.
 *   <li>For each admitted drop, the manager either rebuilds the {@code DropPartitionEvent} / {@code
 *       DropTableEvent} and puts it into the {@link EventManager} (watcher path) or runs the resume
 *       {@link Runnable} directly (startup reconciliation path).
 *   <li>The coordinator event processor handles the event normally, which eventually drives the
 *       replica state machine to {@code DeletionSuccessful}. {@code TableManager} then notifies
 *       this manager via {@link #onPartitionDropCompleted} / {@link #onTableDropCompleted},
 *       releasing the drop's bucket budget and triggering the next batch.
 * </ol>
 *
 * <p>Timeout-based abandon: each in-flight drop is timestamped at admission time. A periodic task
 * scans for drops whose completion callback has not arrived within {@code
 * coordinator.replica-cleanup.inflight-timeout} and abandons them with a WARN log. Abandonment only
 * releases the manager's in-memory tracking; any residual replica state machine entries are
 * reconciled on the next coordinator startup via {@link TableManager#resumeDeletions()}.
 */
public class ReplicaCleanupManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaCleanupManager.class);

    private final EventManager eventManager;
    private final Clock clock;
    private final ScheduledExecutorService timeoutChecker;

    /**
     * Per-batch budget measured in number of buckets across the drops admitted in one batch. A
     * non-positive value disables the cap. If the head-of-queue drop alone already exceeds this
     * budget, it is admitted on its own to avoid starvation.
     */
    private final int dropMaxBucketsPerBatch;

    /**
     * Maximum amount of time, in milliseconds, an in-flight drop may wait for its completion
     * callback before being abandoned.
     */
    private final long inflightTimeoutMs;

    private final long timeoutCheckIntervalMs;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Lock lock = new ReentrantLock();

    /** Pending drops waiting to be admitted into the coordinator event queue. */
    @GuardedBy("lock")
    private final Deque<PendingDrop> pendingDrops = new ArrayDeque<>();

    /** Total number of buckets across drops currently admitted but not yet completed. */
    @GuardedBy("lock")
    private int inflightBuckets = 0;

    /** Number of drops currently admitted but not yet completed (for observability/tests). */
    @GuardedBy("lock")
    private int inflightCount = 0;

    /** In-flight partition drops, keyed by {@link TablePartition}. */
    @GuardedBy("lock")
    private final Map<TablePartition, InflightDrop> inflightPartitions = new HashMap<>();

    /** In-flight table drops, keyed by tableId. */
    @GuardedBy("lock")
    private final Map<Long, InflightDrop> inflightTables = new HashMap<>();

    public ReplicaCleanupManager(EventManager eventManager, Configuration conf, Clock clock) {
        this(
                eventManager,
                conf,
                clock,
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("replica-cleanup-timeout")));
    }

    @VisibleForTesting
    ReplicaCleanupManager(
            EventManager eventManager,
            Configuration conf,
            Clock clock,
            ScheduledExecutorService timeoutChecker) {
        this.eventManager = eventManager;
        this.clock = clock == null ? SystemClock.getInstance() : clock;
        this.timeoutChecker = timeoutChecker;
        this.dropMaxBucketsPerBatch =
                conf.get(ConfigOptions.COORDINATOR_REPLICA_CLEANUP_MAX_BUCKETS_PER_BATCH);
        this.inflightTimeoutMs =
                conf.get(ConfigOptions.COORDINATOR_REPLICA_CLEANUP_INFLIGHT_TIMEOUT).toMillis();
        this.timeoutCheckIntervalMs =
                conf.get(ConfigOptions.COORDINATOR_REPLICA_CLEANUP_TIMEOUT_CHECK_INTERVAL)
                        .toMillis();
    }

    /** Starts the periodic timeout checker. Idempotent; subsequent calls are no-ops. */
    public void start() {
        if (closed.get()) {
            throw new IllegalStateException("ReplicaCleanupManager is already closed.");
        }
        if (started.compareAndSet(false, true)) {
            timeoutChecker.scheduleWithFixedDelay(
                    this::checkTimeoutsSafely,
                    timeoutCheckIntervalMs,
                    timeoutCheckIntervalMs,
                    TimeUnit.MILLISECONDS);
            LOG.info(
                    "ReplicaCleanupManager started: maxBucketsPerBatch={}, inflightTimeoutMs={}, "
                            + "timeoutCheckIntervalMs={}",
                    dropMaxBucketsPerBatch,
                    inflightTimeoutMs,
                    timeoutCheckIntervalMs);
        }
    }

    /**
     * Submits a partition drop request. The partition's ZK metadata is expected to have been
     * removed by the caller prior to this submission; the manager only governs how fast the
     * resulting {@link DropPartitionEvent} is admitted into the coordinator event queue.
     *
     * @param bucketCount number of buckets owned by this partition; used to charge the per-batch
     *     bucket budget.
     */
    public void submitPartitionDrop(
            long tableId, long partitionId, String partitionName, int bucketCount) {
        admitPartitionDrop(tableId, partitionId, partitionName, bucketCount, null);
    }

    /**
     * Submits a partition drop request that, when admitted, runs {@code resumeAction} instead of
     * putting a {@link DropPartitionEvent} into the coordinator event queue. Used by coordinator
     * startup to reconcile stale partitions whose {@code TableInfo} is already gone, where the
     * regular event-queue path would NPE; the action typically drives {@link
     * TableManager#onDeletePartition} directly.
     */
    public void submitPartitionDropForResume(
            long tableId,
            long partitionId,
            String partitionName,
            int bucketCount,
            Runnable resumeAction) {
        admitPartitionDrop(tableId, partitionId, partitionName, bucketCount, resumeAction);
    }

    private void admitPartitionDrop(
            long tableId,
            long partitionId,
            String partitionName,
            int bucketCount,
            @Nullable Runnable resumeAction) {
        int normalized = Math.max(1, bucketCount);
        List<PendingDrop> batch;
        lock.lock();
        try {
            pendingDrops.add(
                    new PendingPartitionDrop(
                            tableId, partitionId, partitionName, normalized, resumeAction));
            LOG.debug(
                    "Submitted partition drop: tableId={} partitionId={} ({}) buckets={} "
                            + "resume={}; pending={} inflight={} inflightBuckets={}",
                    tableId,
                    partitionId,
                    partitionName,
                    normalized,
                    resumeAction != null,
                    pendingDrops.size(),
                    inflightCount,
                    inflightBuckets);
            batch = collectNextBatch();
        } finally {
            lock.unlock();
        }
        executeBatch(batch);
    }

    /**
     * Submits a table drop request. The table's ZK metadata is expected to have been removed by the
     * caller prior to this submission.
     *
     * @param bucketCount number of buckets owned by this table at drop time. For non-partitioned
     *     tables this is the table bucket count; for partitioned tables, partition znodes are
     *     deleted before the table znode, so by the time the table drop is submitted the remaining
     *     bucket footprint is typically the per-partition bucket count (one shard) or smaller.
     */
    public void submitTableDrop(
            long tableId,
            boolean isAutoPartitionTable,
            boolean isDataLakeEnabled,
            int bucketCount) {
        admitTableDrop(tableId, isAutoPartitionTable, isDataLakeEnabled, bucketCount, null);
    }

    /**
     * Submits a table drop request that, when admitted, runs {@code resumeAction} instead of
     * putting a {@link DropTableEvent} into the coordinator event queue. Used by coordinator
     * startup to reconcile stale tables whose {@code TableInfo} is already gone, where the regular
     * event-queue path would NPE; the action typically drives {@link TableManager#onDeleteTable}
     * directly.
     */
    public void submitTableDropForResume(long tableId, int bucketCount, Runnable resumeAction) {
        // For startup resume the auto-partition / data-lake flags would only have been used to
        // build a DropTableEvent (which we are bypassing), so they are intentionally omitted.
        admitTableDrop(tableId, false, false, bucketCount, resumeAction);
    }

    private void admitTableDrop(
            long tableId,
            boolean isAutoPartitionTable,
            boolean isDataLakeEnabled,
            int bucketCount,
            @Nullable Runnable resumeAction) {
        int normalized = Math.max(1, bucketCount);
        List<PendingDrop> batch;
        lock.lock();
        try {
            pendingDrops.add(
                    new PendingTableDrop(
                            tableId,
                            isAutoPartitionTable,
                            isDataLakeEnabled,
                            normalized,
                            resumeAction));
            LOG.debug(
                    "Submitted table drop: tableId={} autoPartition={} dataLake={} "
                            + "buckets={} resume={}; pending={} inflight={} "
                            + "inflightBuckets={}",
                    tableId,
                    isAutoPartitionTable,
                    isDataLakeEnabled,
                    normalized,
                    resumeAction != null,
                    pendingDrops.size(),
                    inflightCount,
                    inflightBuckets);
            batch = collectNextBatch();
        } finally {
            lock.unlock();
        }
        executeBatch(batch);
    }

    /**
     * Called by {@link TableManager} when all replicas of a partition have reached the {@code
     * DeletionSuccessful} state. Releases this drop's budget and triggers the next batch.
     */
    public void onPartitionDropCompleted(TablePartition tablePartition) {
        List<PendingDrop> batch;
        lock.lock();
        try {
            InflightDrop inflight = inflightPartitions.remove(tablePartition);
            if (inflight == null) {
                return;
            }
            releaseInflight(inflight);
            LOG.debug(
                    "Partition drop completed: {} after {}ms; inflight={} "
                            + "inflightBuckets={} pending={}",
                    tablePartition,
                    clock.milliseconds() - inflight.submittedAtMs,
                    inflightCount,
                    inflightBuckets,
                    pendingDrops.size());
            batch = collectNextBatch();
        } finally {
            lock.unlock();
        }
        executeBatch(batch);
    }

    /**
     * Called by {@link TableManager} when all replicas of a table have reached the {@code
     * DeletionSuccessful} state. Releases this drop's budget and triggers the next batch.
     */
    public void onTableDropCompleted(long tableId) {
        List<PendingDrop> batch;
        lock.lock();
        try {
            InflightDrop inflight = inflightTables.remove(tableId);
            if (inflight == null) {
                return;
            }
            releaseInflight(inflight);
            LOG.debug(
                    "Table drop completed: tableId={} after {}ms; inflight={} "
                            + "inflightBuckets={} pending={}",
                    tableId,
                    clock.milliseconds() - inflight.submittedAtMs,
                    inflightCount,
                    inflightBuckets,
                    pendingDrops.size());
            batch = collectNextBatch();
        } finally {
            lock.unlock();
        }
        executeBatch(batch);
    }

    /**
     * Pulls drops from {@link #pendingDrops} in FIFO order while the running sum of in-flight
     * bucket counts stays within the per-batch bucket budget. Starvation guard: when no drop is in
     * flight and the head-of-queue drop alone already exceeds the budget, it is still admitted on
     * its own. Returns the batch to be executed outside the lock.
     */
    @GuardedBy("lock")
    private List<PendingDrop> collectNextBatch() {
        if (pendingDrops.isEmpty()) {
            return Collections.emptyList();
        }

        boolean unlimitedBudget = dropMaxBucketsPerBatch <= 0;
        List<PendingDrop> batch = new ArrayList<>();
        while (!pendingDrops.isEmpty()) {
            PendingDrop next = pendingDrops.peek();
            int nextBuckets = next.bucketCount();
            if (!unlimitedBudget) {
                int projected = inflightBuckets + nextBuckets;
                boolean nothingInFlightYet = inflightCount == 0 && batch.isEmpty();
                if (projected > dropMaxBucketsPerBatch && !nothingInFlightYet) {
                    break;
                }
            }
            pendingDrops.poll();
            batch.add(next);
            inflightBuckets += nextBuckets;
            inflightCount += 1;
        }

        long now = clock.milliseconds();
        for (PendingDrop drop : batch) {
            InflightDrop inflight = new InflightDrop(drop, now);
            if (drop instanceof PendingPartitionDrop) {
                PendingPartitionDrop p = (PendingPartitionDrop) drop;
                inflightPartitions.put(new TablePartition(p.tableId, p.partitionId), inflight);
            } else {
                inflightTables.put(((PendingTableDrop) drop).tableId, inflight);
            }
        }
        return batch;
    }

    /**
     * Executes the batch of drops outside the lock. For the watcher path this puts events into the
     * coordinator event queue; for the resume path this runs the supplied {@link Runnable}.
     */
    private void executeBatch(List<PendingDrop> batch) {
        for (PendingDrop drop : batch) {
            try {
                drop.execute(eventManager);
            } catch (Throwable t) {
                LOG.error(
                        "Failed to admit drop event for {}; abandoning in-memory tracking.",
                        drop,
                        t);
                lock.lock();
                try {
                    if (drop instanceof PendingPartitionDrop) {
                        PendingPartitionDrop p = (PendingPartitionDrop) drop;
                        inflightPartitions.remove(new TablePartition(p.tableId, p.partitionId));
                    } else {
                        inflightTables.remove(((PendingTableDrop) drop).tableId);
                    }
                    releaseInflight(new InflightDrop(drop, 0));
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    @GuardedBy("lock")
    private void releaseInflight(InflightDrop inflight) {
        int newBuckets = inflightBuckets - inflight.drop.bucketCount();
        int newCount = inflightCount - 1;
        if (newBuckets < 0 || newCount < 0) {
            LOG.warn(
                    "Inflight counters went negative (buckets: {} -> {}, count: {} -> {}). "
                            + "This indicates a double-release bug; clamping to zero.",
                    inflightBuckets,
                    newBuckets,
                    inflightCount,
                    newCount);
        }
        inflightBuckets = Math.max(0, newBuckets);
        inflightCount = Math.max(0, newCount);
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
        List<PendingDrop> batch;
        lock.lock();
        try {
            long now = clock.milliseconds();
            int abandonedPartitions = abandonExpired(inflightPartitions, now, "partition");
            int abandonedTables = abandonExpired(inflightTables, now, "table");
            if (abandonedPartitions + abandonedTables > 0) {
                batch = collectNextBatch();
            } else {
                batch = Collections.emptyList();
            }
        } finally {
            lock.unlock();
        }
        executeBatch(batch);
    }

    @GuardedBy("lock")
    private <K> int abandonExpired(Map<K, InflightDrop> map, long now, String kind) {
        int abandoned = 0;
        Iterator<Map.Entry<K, InflightDrop>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<K, InflightDrop> entry = it.next();
            InflightDrop inflight = entry.getValue();
            long elapsed = now - inflight.submittedAtMs;
            if (elapsed > inflightTimeoutMs) {
                it.remove();
                releaseInflight(inflight);
                abandoned++;
                LOG.warn(
                        "{} drop {} timed out after {}ms with no completion callback. "
                                + "Abandoning in-memory tracking; ZK metadata is already gone, "
                                + "any residual replica state will be reconciled on next "
                                + "coordinator restart.",
                        kind,
                        entry.getKey(),
                        elapsed);
            }
        }
        return abandoned;
    }

    @VisibleForTesting
    int getInflightCount() {
        return inLock(lock, () -> inflightCount);
    }

    @VisibleForTesting
    int getInflightBuckets() {
        return inLock(lock, () -> inflightBuckets);
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
        /** Number of buckets this drop owns; charged against the per-batch bucket budget. */
        int bucketCount();

        void execute(EventManager eventManager);
    }

    /** Pending admission of a {@link DropPartitionEvent} for a single partition. */
    static final class PendingPartitionDrop implements PendingDrop {
        final long tableId;
        final long partitionId;
        final String partitionName;
        final int bucketCount;
        @Nullable final Runnable resumeAction;

        PendingPartitionDrop(
                long tableId,
                long partitionId,
                String partitionName,
                int bucketCount,
                @Nullable Runnable resumeAction) {
            this.tableId = tableId;
            this.partitionId = partitionId;
            this.partitionName = partitionName;
            this.bucketCount = bucketCount;
            this.resumeAction = resumeAction;
        }

        @Override
        public int bucketCount() {
            return bucketCount;
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
        public String toString() {
            return "PendingPartitionDrop{tableId="
                    + tableId
                    + ", partitionId="
                    + partitionId
                    + ", partitionName='"
                    + partitionName
                    + "', buckets="
                    + bucketCount
                    + ", resume="
                    + (resumeAction != null)
                    + "}";
        }
    }

    /** Pending admission of a {@link DropTableEvent} for a whole table. */
    static final class PendingTableDrop implements PendingDrop {
        final long tableId;
        final boolean isAutoPartitionTable;
        final boolean isDataLakeEnabled;
        final int bucketCount;
        @Nullable final Runnable resumeAction;

        PendingTableDrop(
                long tableId,
                boolean isAutoPartitionTable,
                boolean isDataLakeEnabled,
                int bucketCount,
                @Nullable Runnable resumeAction) {
            this.tableId = tableId;
            this.isAutoPartitionTable = isAutoPartitionTable;
            this.isDataLakeEnabled = isDataLakeEnabled;
            this.bucketCount = bucketCount;
            this.resumeAction = resumeAction;
        }

        @Override
        public int bucketCount() {
            return bucketCount;
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
        public String toString() {
            return "PendingTableDrop{tableId="
                    + tableId
                    + ", autoPartition="
                    + isAutoPartitionTable
                    + ", dataLake="
                    + isDataLakeEnabled
                    + ", buckets="
                    + bucketCount
                    + ", resume="
                    + (resumeAction != null)
                    + "}";
        }
    }

    /** Tracks an in-flight drop along with its admission timestamp for timeout detection. */
    private static final class InflightDrop {
        final PendingDrop drop;
        final long submittedAtMs;

        InflightDrop(PendingDrop drop, long submittedAtMs) {
            this.drop = drop;
            this.submittedAtMs = submittedAtMs;
        }
    }
}

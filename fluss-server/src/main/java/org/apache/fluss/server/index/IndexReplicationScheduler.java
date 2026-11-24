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

package org.apache.fluss.server.index;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.index.IndexReplicator.IndexReplicationState;
import org.apache.fluss.server.index.IndexReplicator.ReadGroup;

import org.apache.commons.collections4.map.LinkedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Worker-local rotation with node-wide input admission and contended construction protection.
 *
 * <p>Schedules reader work without owning replication progress or payload resources.
 */
final class IndexReplicationScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicationScheduler.class);

    private final ConcurrentMap<Integer, WorkerState> workers = new ConcurrentHashMap<>();
    private final ConcurrentMap<IndexReplicator, SourceSchedulingState> sources =
            new ConcurrentHashMap<>();

    private final Object capacityLock = new Object();
    private final int maxAdmittedSources;
    private final Set<IndexReplicator> readAdmittedSources =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private final LinkedHashMap<Long, LinkedHashSet<IndexReplicator>> readWaitersByTable =
            new LinkedHashMap<>();

    private final LinkedHashMap<Long, TableWaits> capacityWaitsByTable = new LinkedHashMap<>();
    private final Map<IndexReplicationState, CapacityWait> capacityWaitsByIndex =
            new IdentityHashMap<>();
    @Nullable private CapacityWait nextCapacityWaiter;
    private boolean nextCapacityWaiterProtected;
    @Nullable private SchedulingAttempt constructionAttempt;
    private volatile boolean capacityContended;

    IndexReplicationScheduler(int maxAdmittedSources) {
        checkArgument(maxAdmittedSources > 0, "maxAdmittedSources must be positive");
        this.maxAdmittedSources = maxAdmittedSources;
    }

    void register(int workerId, IndexReplicator source) {
        WorkerState worker = workers.computeIfAbsent(workerId, ignored -> new WorkerState());
        SourceSchedulingState previous;
        synchronized (worker) {
            long tableId = source.sourceBucket().getTableId();
            TableSources table = worker.tables.get(tableId);
            if (table == null) {
                table = new TableSources(tableId);
                worker.tables.put(tableId, table);
                worker.tableOrder.add(table);
            }
            previous = table.sources.get(source.sourceBucket());
            if (previous != null && previous.replicator == source) {
                return;
            }
            if (previous != null) {
                removeTableSource(worker, table, previous);
                sources.remove(previous.replicator);
                worker.sourceCount--;
            }
            SourceSchedulingState state = new SourceSchedulingState(source, table);
            sources.put(source, state);
            table.sources.put(source.sourceBucket(), state);
            worker.sourceCount++;
        }
        if (previous != null) {
            removeSourceWaits(previous.replicator);
            synchronized (capacityLock) {
                removeReadWaitLocked(previous.replicator);
            }
        }
        source.wakeup();
    }

    void unregister(int workerId, IndexReplicator expectedSource) {
        WorkerState worker = workers.get(workerId);
        if (worker == null) {
            return;
        }
        synchronized (worker) {
            TableSources table = worker.tables.get(expectedSource.sourceBucket().getTableId());
            SourceSchedulingState state =
                    table == null ? null : table.sources.get(expectedSource.sourceBucket());
            if (state == null || state.replicator != expectedSource) {
                return;
            }
            removeTableSource(worker, table, state);
            sources.remove(expectedSource);
            worker.sourceCount--;
            if (table.sources.isEmpty()) {
                worker.tables.remove(table.tableId);
                removeTableOrder(worker, table);
            }
        }
        removeSourceWaits(expectedSource);
        synchronized (capacityLock) {
            removeReadWaitLocked(expectedSource);
        }
    }

    /** Performs a bounded scan, stopping after the first actual read or construction attempt. */
    boolean pollWorker(int workerId) {
        WorkerState worker = workers.get(workerId);
        if (worker == null) {
            return false;
        }
        synchronized (worker) {
            finishRound(worker);
            startRound(worker);
        }
        IndexReplicator preferred = refreshCapacity();
        boolean allowRetryProbe = worker.retryProbeOwed;
        if (pollOrder(worker, preferred, allowRetryProbe)) {
            return true;
        }
        // No source could start ordinary work; probes may run without waiting for a credit.
        return !allowRetryProbe && pollOrder(worker, preferred, true);
    }

    private boolean pollOrder(
            WorkerState worker, @Nullable IndexReplicator preferred, boolean allowRetryProbe) {
        SourceSchedulingState priority = preferred == null ? null : sources.get(preferred);
        synchronized (worker) {
            worker.scanTable = 0;
            worker.lastScannedSource = null;
            worker.scanRemaining = worker.sourceCount;
            if (priority != null && worker.tables.get(priority.table.tableId) != priority.table) {
                priority = null;
            }
        }
        SourceSchedulingState state = priority == null ? nextSource(worker, null) : priority;
        while (state != null) {
            if (sources.get(state.replicator) == state) {
                SchedulingAttempt call = worker.call;
                call.sourceSchedule = state;
                call.allowRetryProbe = allowRetryProbe;
                call.normalWorkSelected = false;
                call.attempt = null;
                call.workAttempted = false;
                try {
                    state.replicator.poll(call);
                } catch (Throwable failure) {
                    LOG.error(
                            "Error polling index replicator for {}",
                            state.replicator.sourceBucket(),
                            failure);
                } finally {
                    if (call.workAttempted && sources.get(state.replicator) == state) {
                        confirmService(worker, call);
                    }
                    finishBuild(call);
                }
                if (call.workAttempted) {
                    return true;
                }
            }
            state = nextSource(worker, priority);
        }
        return false;
    }

    @Nullable
    private static SourceSchedulingState nextSource(
            WorkerState worker, @Nullable SourceSchedulingState priority) {
        synchronized (worker) {
            while (worker.scanTable < worker.tableOrder.size()) {
                TableSources table = worker.tableOrder.get(worker.scanTable);
                TableBucket bucket =
                        worker.lastScannedSource == null
                                ? table.sources.firstKey()
                                : table.sources.nextKey(worker.lastScannedSource);
                if (bucket == null) {
                    // No ordinary work attempt was available in this table during the bounded scan.
                    worker.remainingTables.remove(table);
                    finishRound(worker);
                    worker.scanTable++;
                    worker.lastScannedSource = null;
                    continue;
                }
                if (worker.scanRemaining-- <= 0) {
                    return null;
                }
                worker.lastScannedSource = bucket;
                SourceSchedulingState source = table.sources.get(bucket);
                if (source != priority) {
                    return source;
                }
            }
            return null;
        }
    }

    private void confirmService(WorkerState worker, SchedulingAttempt call) {
        SourceSchedulingState state = call.sourceSchedule;
        synchronized (worker) {
            if (state.table.sources.get(state.replicator.sourceBucket()) != state) {
                return;
            }
            TableBucket bucket = state.replicator.sourceBucket();
            state.table.sources.remove(bucket);
            state.table.sources.put(bucket, state);
            worker.tableOrder.remove(state.table);
            worker.tableOrder.add(state.table);
            RetryProbeAttempt attempt = call.attempt;
            if (attempt != null && !attempt.serviceCharged) {
                attempt.serviceCharged = true;
                worker.retryProbeOwed = false;
            }
            if (call.normalWorkSelected) {
                startRound(worker);
                worker.roundServiced = true;
                worker.remainingTables.remove(state.table);
                finishRound(worker);
            }
        }
    }

    private static void startRound(WorkerState worker) {
        if (!worker.roundOpen && !worker.tableOrder.isEmpty()) {
            // Capture membership, not readiness: every source is still checked by its real poll.
            worker.remainingTables.addAll(worker.tableOrder);
            worker.roundServiced = false;
            worker.roundOpen = true;
        }
    }

    private static void finishRound(WorkerState worker) {
        if (worker.roundOpen && worker.remainingTables.isEmpty()) {
            worker.retryProbeOwed |= worker.roundServiced;
            worker.roundOpen = false;
        }
    }

    private static void removeTableSource(
            WorkerState worker, TableSources table, SourceSchedulingState source) {
        TableBucket bucket = source.replicator.sourceBucket();
        if (worker.scanTable < worker.tableOrder.size()
                && worker.tableOrder.get(worker.scanTable) == table
                && bucket.equals(worker.lastScannedSource)) {
            // Resume after the predecessor when the last visited source is removed or replaced.
            worker.lastScannedSource = table.sources.previousKey(bucket);
        }
        table.sources.remove(bucket);
    }

    private static void removeTableOrder(WorkerState worker, TableSources table) {
        int position = worker.tableOrder.indexOf(table);
        worker.tableOrder.remove(position);
        worker.remainingTables.remove(table);
        if (position < worker.scanTable) {
            worker.scanTable--;
        } else if (position == worker.scanTable) {
            worker.lastScannedSource = null;
        }
    }

    /**
     * Selects a read group and acquires source input admission. The source retains admission
     * through reading and construction, then calls {@link #releaseReadAdmission}.
     *
     * <p>Called with the source lifecycle lock held.
     */
    @Nullable
    ReadGroup tryAdmitRead(SchedulingAttempt call, long highWatermark) {
        IndexReplicator source = call.sourceSchedule.replicator;
        SourceSchedulingState state = sources.get(source);
        if (state == null || source.isClosed()) {
            return null;
        }
        boolean probeFirst = call.allowRetryProbe;
        ReadGroup group = chooseGroup(state, highWatermark, probeFirst);
        if (group == null && probeFirst) {
            group = chooseGroup(state, highWatermark, !probeFirst);
        }
        if (group == null) {
            releaseReadAdmission(source);
            return null;
        }
        if (!acquireRead(state)) {
            return null;
        }
        boolean failed = isProbe(source, group.states());
        state.probeAttempt = failed ? new RetryProbeAttempt(group) : null;
        call.attempt = state.probeAttempt;
        call.normalWorkSelected |= !failed;
        return group;
    }

    private boolean acquireRead(SourceSchedulingState state) {
        List<IndexReplicator> wakeups = new ArrayList<>();
        boolean admitted;
        synchronized (capacityLock) {
            if (!readAdmittedSources.contains(state.replicator)) {
                LinkedHashSet<IndexReplicator> queue =
                        readWaitersByTable.computeIfAbsent(
                                state.replicator.sourceBucket().getTableId(),
                                ignored -> new LinkedHashSet<>());
                queue.add(state.replicator);
                admitReadsLocked(wakeups);
            }
            admitted = readAdmittedSources.contains(state.replicator);
        }
        wakeups.forEach(IndexReplicationScheduler::wake);
        return admitted;
    }

    private void admitReadsLocked(List<IndexReplicator> wakeups) {
        while (readAdmittedSources.size() < maxAdmittedSources && !readWaitersByTable.isEmpty()) {
            Map.Entry<Long, LinkedHashSet<IndexReplicator>> entry =
                    readWaitersByTable.entrySet().iterator().next();
            long tableId = entry.getKey();
            LinkedHashSet<IndexReplicator> queue = entry.getValue();
            IndexReplicator source = queue.iterator().next();
            queue.remove(source);
            readWaitersByTable.remove(tableId);
            if (!queue.isEmpty()) {
                readWaitersByTable.put(tableId, queue);
            }
            if (sources.containsKey(source) && !source.isClosed()) {
                readAdmittedSources.add(source);
                wakeups.add(source);
            }
        }
    }

    /** Releases input admission unless it is reserved for a capacity retry. */
    void releaseReadAdmission(IndexReplicator source) {
        List<IndexReplicator> wakeups = new ArrayList<>();
        synchronized (capacityLock) {
            // A capacity owner must be able to reread without competing for another input slot.
            if (capacityContended && capacityWaitsForSourceLocked(source) != null) {
                return;
            }
            readAdmittedSources.remove(source);
            removeReadWaitLocked(source);
            admitReadsLocked(wakeups);
        }
        wakeups.forEach(IndexReplicationScheduler::wake);
    }

    private void removeReadWaitLocked(IndexReplicator source) {
        LinkedHashSet<IndexReplicator> queue =
                readWaitersByTable.get(source.sourceBucket().getTableId());
        if (queue != null) {
            queue.remove(source);
            if (queue.isEmpty()) {
                readWaitersByTable.remove(source.sourceBucket().getTableId());
            }
        }
    }

    private boolean alignedSyncGroup(IndexReplicator source, ReadGroup group) {
        if (source.hasSourceFailure()) {
            return false;
        }
        for (IndexReplicationState index : group.states()) {
            if (!index.encoder().isSync() || index.hasFailure()) {
                return false;
            }
        }
        // A read group contains distinct members of this source.
        int syncCount = 0;
        for (IndexReplicationState index : source.indexStates()) {
            if (index.encoder().isSync()) {
                syncCount++;
            }
        }
        return syncCount == group.states().size();
    }

    private static boolean reusableRange(
            IndexReplicator source,
            IndexReplicationState index,
            IndexSourceReader.ReadResult result) {
        return index.retryWindowEndOffset() >= result.nextOffset()
                && source.readTargetBytesFor(index) >= result.bytes();
    }

    /** Selects optional ASYNC work only after the original SYNC group has been published. */
    List<IndexReplicationState> selectAsync(
            IndexReplicator source, ReadGroup syncGroup, IndexSourceReader.ReadResult result) {
        if (capacityContended || !alignedSyncGroup(source, syncGroup)) {
            return Collections.emptyList();
        }
        List<IndexReplicationState> selected = new ArrayList<>();
        for (IndexReplicationState index : source.indexStates()) {
            if (!index.encoder().isSync()
                    && !index.hasFailure()
                    && index.pushedOffset() == syncGroup.startOffset()
                    && index.canRead(result.nextOffset())
                    && reusableRange(source, index, result)) {
                selected.add(index);
            }
        }
        return selected;
    }

    @Nullable
    private ReadGroup chooseGroup(SourceSchedulingState state, long highWatermark, boolean probe) {
        IndexReplicator source = state.replicator;
        if (source.isClosed() || !source.isSourceRetryReady()) {
            return null;
        }
        List<IndexReplicationState> indexes = source.indexStates();
        long syncLimit = syncStartLimit(source);
        long asyncLimit = asyncLimit(indexes, highWatermark);

        long syncStart = Long.MAX_VALUE;
        long asyncStart = Long.MAX_VALUE;
        for (IndexReplicationState index : indexes) {
            if (!eligible(source, index, highWatermark, probe)) {
                continue;
            }
            if (index.encoder().isSync()) {
                if (index.pushedOffset() <= syncLimit) {
                    syncStart = Math.min(syncStart, index.pushedOffset());
                }
            } else if (index.pushedOffset() < asyncLimit) {
                asyncStart = Math.min(asyncStart, index.pushedOffset());
            }
        }
        long start =
                asyncStart != Long.MAX_VALUE && (state.asyncTurnOwed || syncStart == Long.MAX_VALUE)
                        ? asyncStart
                        : syncStart;
        if (start == Long.MAX_VALUE) {
            return null;
        }
        long end = highWatermark;
        List<IndexReplicationState> group = new ArrayList<>();
        for (IndexReplicationState index : indexes) {
            if (index.pushedOffset() != start || !eligible(source, index, highWatermark, probe)) {
                continue;
            }
            if (index.encoder().isSync()) {
                if (start > syncLimit) {
                    continue;
                }
            } else {
                if (start >= asyncLimit) {
                    continue;
                }
                end = Math.min(end, asyncLimit);
            }
            end = Math.min(end, index.retryWindowEndOffset());
            group.add(index);
        }
        return end > start ? new ReadGroup(start, end, group) : null;
    }

    private static long syncStartLimit(IndexReplicator source) {
        long minimum = source.getSyncIndexPushedOffset();
        long covered = Long.MAX_VALUE;
        for (IndexReplicationState index : source.indexStates()) {
            if (index.encoder().isSync() && index.pushedOffset() == minimum) {
                long windowEnd = index.healthyWindowEnd();
                if (windowEnd < 0) {
                    return minimum;
                }
                covered = Math.min(covered, windowEnd);
            }
        }
        return covered;
    }

    private static long asyncLimit(List<IndexReplicationState> indexes, long highWatermark) {
        long limit = highWatermark;
        for (IndexReplicationState index : indexes) {
            if (index.encoder().isSync()) {
                long windowEnd = index.healthyWindowEnd();
                limit = Math.min(limit, windowEnd >= 0 ? windowEnd : index.pushedOffset());
            }
        }
        return limit;
    }

    private static boolean eligible(
            IndexReplicator source,
            IndexReplicationState index,
            long highWatermark,
            boolean probe) {
        return index.canRead(highWatermark)
                && (source.hasSourceFailure() || index.hasFailure()) == probe;
    }

    private static boolean isProbe(IndexReplicator source, List<IndexReplicationState> indexes) {
        if (source.hasSourceFailure()) {
            return true;
        }
        for (IndexReplicationState index : indexes) {
            if (index.hasFailure()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Grants an immediate construction attempt and records its scheduling credit, or returns null
     * without transferring the pending result. A grant does not reserve payload pages.
     *
     * <p>Called with the source lifecycle lock held.
     */
    @Nullable
    ReadGroup tryScheduleBuild(
            SchedulingAttempt call, ReadGroup pendingGroup, IndexSourceReader.ReadResult result) {
        IndexReplicator source = call.sourceSchedule.replicator;
        SourceSchedulingState state = sources.get(source);
        if (state == null || source.isClosed()) {
            return null;
        }
        boolean failed = isProbe(source, pendingGroup.states());
        RetryProbeAttempt attempt = state.probeAttempt;
        boolean continuation =
                attempt != null && attempt.group == pendingGroup && attempt.serviceCharged;
        if (failed && !call.allowRetryProbe && !continuation) {
            return null;
        }

        ReadGroup selected;
        IndexReplicator wake = null;
        if (!capacityContended) {
            if (alignedSyncGroup(source, pendingGroup)) {
                for (IndexReplicationState index : source.indexStates()) {
                    // Drop the source lock only for an ACK already received. The original owner
                    // callback advances progress and wakes this source; no network wait is added.
                    if (!index.encoder().isSync()
                            && !index.hasFailure()
                            && index.acknowledgedWindowEnd() == pendingGroup.startOffset()
                            && reusableRange(source, index, result)) {
                        return null;
                    }
                }
            }
            selected = pendingGroup;
        } else {
            synchronized (capacityLock) {
                wake = chooseCapacityLocked();
                CapacityWait owner = nextCapacityWaiter;
                if (owner != null && owner.sourceSchedule.replicator == source) {
                    if (owner.start != pendingGroup.startOffset()
                            || !pendingGroup.states().contains(owner.index)
                            || !source.hasReadyRead(owner.index, owner.start)) {
                        return null;
                    }
                    nextCapacityWaiterProtected = true;
                    if (!source.hasPageCapacity()) {
                        constructionAttempt = null;
                        return null;
                    }
                    constructionAttempt = call;
                    selected =
                            new ReadGroup(
                                    pendingGroup.startOffset(),
                                    pendingGroup.endOffset(),
                                    Collections.singletonList(owner.index));
                } else {
                    if (nextCapacityWaiterProtected) {
                        return null;
                    }
                    selected = pendingGroup;
                }
            }
        }
        wake(wake);
        if (failed) {
            if (attempt == null || attempt.group != pendingGroup) {
                attempt = new RetryProbeAttempt(pendingGroup);
                state.probeAttempt = attempt;
            }
            call.attempt = attempt;
        } else {
            call.normalWorkSelected = true;
        }
        return selected;
    }

    void onCapacityBlocked(IndexReplicator source, IndexReplicationState index) {
        SourceSchedulingState state = sources.get(source);
        if (state == null || source.isClosed()) {
            return;
        }
        state.probeAttempt = null;
        IndexReplicator wake;
        synchronized (capacityLock) {
            CapacityWait wait = capacityWaitsByIndex.get(index);
            if (wait != null && wait.start != index.pushedOffset()) {
                removeWaitLocked(wait, false);
                wait = null;
            }
            if (wait == null) {
                wait = new CapacityWait(state, index, index.pushedOffset());
                capacityWaitsByIndex.put(index, wait);
                capacityContended = true;
                capacityWaitsByTable
                        .computeIfAbsent(
                                source.sourceBucket().getTableId(), ignored -> new TableWaits())
                        .sources
                        .computeIfAbsent(source, ignored -> new ArrayDeque<>())
                        .addLast(wait);
            }
            clearSelectionLocked(wait);
            wake = chooseCapacityLocked();
        }
        wake(wake);
    }

    void releaseCapacityWait(IndexReplicator source, IndexReplicationState index) {
        removeIndexWait(source, index, true);
    }

    void onProgress(IndexReplicator source, IndexReplicationState index) {
        SourceSchedulingState state = sources.get(source);
        if (state == null) {
            return;
        }
        state.asyncTurnOwed = index.encoder().isSync();
        removeIndexWait(source, index, false);
    }

    /** Clears probe and capacity-wait state for the source or a single index. */
    void clearProbeAndCapacityWaits(IndexReplicator source, @Nullable IndexReplicationState index) {
        SourceSchedulingState state = sources.get(source);
        if (state == null) {
            return;
        }
        if (index == null) {
            state.probeAttempt = null;
            removeSourceWaits(source);
        } else {
            RetryProbeAttempt attempt = state.probeAttempt;
            if (attempt != null && attempt.group.states().contains(index)) {
                state.probeAttempt = null;
            }
            removeIndexWait(source, index, true);
        }
    }

    private void removeIndexWait(
            IndexReplicator source, IndexReplicationState index, boolean rotate) {
        if (!capacityContended) {
            return;
        }
        IndexReplicator wake;
        synchronized (capacityLock) {
            CapacityWait wait = capacityWaitsByIndex.get(index);
            if (wait != null && wait.sourceSchedule.replicator == source) {
                removeWaitLocked(wait, rotate);
            }
            wake = chooseCapacityLocked();
        }
        wake(wake);
    }

    private void removeSourceWaits(IndexReplicator source) {
        if (!capacityContended) {
            return;
        }
        IndexReplicator wake;
        synchronized (capacityLock) {
            Deque<CapacityWait> waits = capacityWaitsForSourceLocked(source);
            while (waits != null && !waits.isEmpty()) {
                removeWaitLocked(waits.getFirst(), false);
            }
            wake = chooseCapacityLocked();
        }
        wake(wake);
    }

    @Nullable
    private Deque<CapacityWait> capacityWaitsForSourceLocked(IndexReplicator source) {
        TableWaits table = capacityWaitsByTable.get(source.sourceBucket().getTableId());
        return table == null ? null : table.sources.get(source);
    }

    private void removeWaitLocked(CapacityWait wait, boolean rotate) {
        capacityWaitsByIndex.remove(wait.index);
        capacityContended = !capacityWaitsByIndex.isEmpty();
        unlinkCapacityWaitLocked(wait, rotate);
    }

    private void unlinkCapacityWaitLocked(CapacityWait wait, boolean rotate) {
        // Only the protected builder spends the table's turn.
        rotate &= nextCapacityWaiterProtected && nextCapacityWaiter == wait;
        long tableId = wait.sourceSchedule.table.tableId;
        TableWaits table = capacityWaitsByTable.get(tableId);
        Deque<CapacityWait> source = table.sources.get(wait.sourceSchedule.replicator);
        source.remove(wait);
        if (source.isEmpty() || rotate) {
            table.sources.remove(wait.sourceSchedule.replicator);
            if (!source.isEmpty()) {
                table.sources.put(wait.sourceSchedule.replicator, source);
            }
        }
        if (table.sources.isEmpty() || rotate) {
            capacityWaitsByTable.remove(tableId);
            if (!table.sources.isEmpty()) {
                capacityWaitsByTable.put(tableId, table);
            }
        }
        clearSelectionLocked(wait);
    }

    private void clearSelectionLocked(CapacityWait wait) {
        if (nextCapacityWaiter == wait) {
            nextCapacityWaiter = null;
            clearActiveLocked();
        }
    }

    @Nullable
    private IndexReplicator refreshCapacity() {
        if (!capacityContended) {
            return null;
        }
        synchronized (capacityLock) {
            return chooseCapacityLocked();
        }
    }

    @Nullable
    private IndexReplicator chooseCapacityLocked() {
        capacityWaitsByIndex.values().removeIf(this::unlinkStaleWaitLocked);
        capacityContended = !capacityWaitsByIndex.isEmpty();
        if (nextCapacityWaiterProtected) {
            if (constructionAttempt != null || ready(nextCapacityWaiter)) {
                return nextCapacityWaiter.sourceSchedule.replicator;
            }
            clearActiveLocked();
        }
        if (nextCapacityWaiter != null && ready(nextCapacityWaiter)) {
            return nextCapacityWaiter.sourceSchedule.replicator;
        }
        nextCapacityWaiter = null;
        for (TableWaits table : capacityWaitsByTable.values()) {
            for (Deque<CapacityWait> source : table.sources.values()) {
                for (CapacityWait wait : source) {
                    if (ready(wait)) {
                        nextCapacityWaiter = wait;
                        return wait.sourceSchedule.replicator;
                    }
                }
            }
        }
        return null;
    }

    private boolean unlinkStaleWaitLocked(CapacityWait wait) {
        if (sources.get(wait.sourceSchedule.replicator) == wait.sourceSchedule
                && !wait.sourceSchedule.replicator.isClosed()
                && wait.index.pushedOffset() == wait.start) {
            return false;
        }
        unlinkCapacityWaitLocked(wait, false);
        return true;
    }

    private static boolean ready(CapacityWait wait) {
        return wait.sourceSchedule.replicator.hasReadyRead(wait.index, wait.start);
    }

    private void clearActiveLocked() {
        nextCapacityWaiterProtected = false;
        constructionAttempt = null;
    }

    private void finishBuild(SchedulingAttempt call) {
        if (!capacityContended) {
            return;
        }
        IndexReplicator wake = null;
        synchronized (capacityLock) {
            if (constructionAttempt == call && nextCapacityWaiter != null) {
                removeWaitLocked(nextCapacityWaiter, true);
                wake = chooseCapacityLocked();
            }
        }
        wake(wake);
    }

    private static void wake(@Nullable IndexReplicator source) {
        if (source != null) {
            source.wakeup();
        }
    }

    private static final class WorkerState {
        private final Map<Long, TableSources> tables = new HashMap<>();
        private final List<TableSources> tableOrder = new ArrayList<>();
        private final Set<TableSources> remainingTables = new HashSet<>();
        private final SchedulingAttempt call = new SchedulingAttempt();
        private int scanTable;
        @Nullable private TableBucket lastScannedSource;
        private int sourceCount;
        private int scanRemaining;
        private boolean roundOpen;
        private boolean roundServiced;
        private boolean retryProbeOwed;
    }

    private static final class TableSources {
        private final long tableId;
        private final LinkedMap<TableBucket, SourceSchedulingState> sources = new LinkedMap<>();

        private TableSources(long tableId) {
            this.tableId = tableId;
        }
    }

    private static final class SourceSchedulingState {
        private final IndexReplicator replicator;
        private final TableSources table;
        private volatile boolean asyncTurnOwed;
        @Nullable private RetryProbeAttempt probeAttempt;

        private SourceSchedulingState(IndexReplicator source, TableSources table) {
            this.replicator = source;
            this.table = table;
        }
    }

    static final class SchedulingAttempt {
        boolean workAttempted;
        private SourceSchedulingState sourceSchedule;
        private boolean allowRetryProbe;
        private boolean normalWorkSelected;
        @Nullable private RetryProbeAttempt attempt;
    }

    private static final class RetryProbeAttempt {
        private final ReadGroup group;
        private boolean serviceCharged;

        private RetryProbeAttempt(ReadGroup group) {
            this.group = group;
        }
    }

    private static final class TableWaits {
        private final LinkedHashMap<IndexReplicator, Deque<CapacityWait>> sources =
                new LinkedHashMap<>();
    }

    private static final class CapacityWait {
        private final SourceSchedulingState sourceSchedule;
        private final IndexReplicationState index;
        private final long start;

        private CapacityWait(
                SourceSchedulingState sourceSchedule, IndexReplicationState index, long start) {
            this.sourceSchedule = sourceSchedule;
            this.index = index;
            this.start = start;
        }
    }
}

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
import org.apache.fluss.server.index.IndexReplicator.IndexProgressState;
import org.apache.fluss.server.index.IndexReplicator.ReadGroup;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Worker-local table/source rotation and node-wide protection of contended construction. */
final class FairIndexReplicationScheduler implements IndexReplicationScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(FairIndexReplicationScheduler.class);

    private final ConcurrentMap<Integer, WorkerState> workers = new ConcurrentHashMap<>();
    private final ConcurrentMap<IndexReplicator, SourceState> sources = new ConcurrentHashMap<>();

    private final Object capacityLock = new Object();
    private final LinkedHashMap<Long, TableWaits> waitingTables = new LinkedHashMap<>();
    private final Map<IndexProgressState, CapacityWait> waits = new IdentityHashMap<>();
    @Nullable private CapacityWait designated;
    @Nullable private CapacityWait active;
    @Nullable private PollCall buildingCall;
    private boolean building;
    private volatile boolean capacityContended;

    @Override
    public void register(int workerId, IndexReplicator source) {
        WorkerState worker = workers.computeIfAbsent(workerId, ignored -> new WorkerState());
        SourceState previous;
        synchronized (worker) {
            long tableId = source.sourceBucket().getTableId();
            TableSources table = worker.tables.get(tableId);
            if (table == null) {
                table = new TableSources(tableId);
                worker.tables.put(tableId, table);
                worker.tableOrder.add(table);
            }
            previous = table.sources.get(source.sourceBucket());
            if (previous != null && previous.source == source) {
                return;
            }
            if (previous != null) {
                removeSourceOrder(worker, table, previous);
                sources.remove(previous.source);
                worker.sourceCount--;
            }
            SourceState state = new SourceState(source, table);
            sources.put(source, state);
            table.sources.put(source.sourceBucket(), state);
            table.sourceOrder.add(state);
            worker.sourceCount++;
        }
        if (previous != null) {
            removeSourceWaits(previous.source);
        }
        source.wakeup();
    }

    @Override
    public void unregister(int workerId, IndexReplicator expectedSource) {
        WorkerState worker = workers.get(workerId);
        if (worker == null) {
            return;
        }
        synchronized (worker) {
            TableSources table = worker.tables.get(expectedSource.sourceBucket().getTableId());
            SourceState state =
                    table == null ? null : table.sources.get(expectedSource.sourceBucket());
            if (state == null || state.source != expectedSource) {
                return;
            }
            table.sources.remove(expectedSource.sourceBucket());
            removeSourceOrder(worker, table, state);
            sources.remove(expectedSource);
            worker.sourceCount--;
            if (table.sources.isEmpty()) {
                worker.tables.remove(table.tableId);
                removeTableOrder(worker, table);
            }
        }
        removeSourceWaits(expectedSource);
    }

    @Override
    public boolean pollWorker(int workerId) {
        WorkerState worker = workers.get(workerId);
        if (worker == null) {
            return false;
        }
        synchronized (worker) {
            finishRound(worker);
            startRound(worker);
        }
        IndexReplicator preferred = refreshCapacity();
        boolean allowProbe = worker.probeOwed;
        if (pollOrder(worker, preferred, allowProbe)) {
            return true;
        }
        // No source could start ordinary work; probes may run without waiting for a credit.
        return !allowProbe && pollOrder(worker, preferred, true);
    }

    private boolean pollOrder(
            WorkerState worker, @Nullable IndexReplicator preferred, boolean allowProbe) {
        SourceState priority = preferred == null ? null : sources.get(preferred);
        synchronized (worker) {
            worker.scanTable = 0;
            worker.scanSource = 0;
            worker.scanRemaining = worker.sourceCount;
            if (priority != null && worker.tables.get(priority.table.tableId) != priority.table) {
                priority = null;
            }
        }
        SourceState state = priority == null ? nextSource(worker, null) : priority;
        while (state != null) {
            if (sources.get(state.source) == state) {
                PollCall call = worker.call;
                call.source = state;
                call.allowProbe = allowProbe;
                call.healthy = false;
                call.attempt = null;
                state.currentCall = call;
                boolean serviced = false;
                try {
                    serviced = state.source.poll();
                } catch (Throwable failure) {
                    LOG.error(
                            "Error polling index replicator for {}",
                            state.source.sourceBucket(),
                            failure);
                } finally {
                    serviced |= state.source.pollServiced();
                    if (serviced && sources.get(state.source) == state) {
                        confirmService(worker, call);
                    }
                    state.currentCall = null;
                    finishBuild(call);
                }
                if (serviced) {
                    return true;
                }
            }
            state = nextSource(worker, priority);
        }
        return false;
    }

    @Nullable
    private static SourceState nextSource(WorkerState worker, @Nullable SourceState priority) {
        synchronized (worker) {
            while (worker.scanTable < worker.tableOrder.size()) {
                TableSources table = worker.tableOrder.get(worker.scanTable);
                if (worker.scanSource >= table.sourceOrder.size()) {
                    // No healthy attempt was available in this table during the bounded scan.
                    worker.remainingTables.remove(table);
                    finishRound(worker);
                    worker.scanTable++;
                    worker.scanSource = 0;
                    continue;
                }
                if (worker.scanRemaining-- <= 0) {
                    return null;
                }
                SourceState source = table.sourceOrder.get(worker.scanSource++);
                if (source != priority) {
                    return source;
                }
            }
            return null;
        }
    }

    private void confirmService(WorkerState worker, PollCall call) {
        SourceState state = call.source;
        synchronized (worker) {
            if (state.table.sources.get(state.source.sourceBucket()) != state) {
                return;
            }
            state.table.sourceOrder.remove(state);
            state.table.sourceOrder.add(state);
            worker.tableOrder.remove(state.table);
            worker.tableOrder.add(state.table);
            ProbeAttempt attempt = call.attempt;
            if (attempt != null && !attempt.charged) {
                attempt.charged = true;
                worker.probeOwed = false;
            }
            if (call.healthy) {
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
            worker.probeOwed |= worker.roundServiced;
            worker.roundOpen = false;
        }
    }

    private static void removeSourceOrder(
            WorkerState worker, TableSources table, SourceState source) {
        int position = table.sourceOrder.indexOf(source);
        table.sourceOrder.remove(position);
        if (worker.scanTable < worker.tableOrder.size()
                && worker.tableOrder.get(worker.scanTable) == table
                && position < worker.scanSource) {
            worker.scanSource--;
        }
    }

    private static void removeTableOrder(WorkerState worker, TableSources table) {
        int position = worker.tableOrder.indexOf(table);
        worker.tableOrder.remove(position);
        worker.remainingTables.remove(table);
        if (position < worker.scanTable) {
            worker.scanTable--;
        } else if (position == worker.scanTable) {
            worker.scanSource = 0;
        }
    }

    @Override
    @Nullable
    public ReadGroup selectRead(IndexReplicator source, long highWatermark) {
        SourceState state = sources.get(source);
        if (state == null || source.isClosed()) {
            return null;
        }
        PollCall call = state.currentCall;
        boolean probeFirst = call != null && call.allowProbe;
        ReadGroup group = chooseGroup(state, highWatermark, probeFirst);
        if (group == null && (probeFirst || call == null)) {
            group = chooseGroup(state, highWatermark, !probeFirst);
        }
        if (group != null) {
            boolean failed = isProbe(source, group.states());
            state.probeAttempt = failed ? new ProbeAttempt(group) : null;
            if (call != null) {
                call.attempt = state.probeAttempt;
                call.healthy |= !failed;
            }
        }
        return group;
    }

    @Nullable
    private ReadGroup chooseGroup(SourceState state, long highWatermark, boolean probe) {
        IndexReplicator source = state.source;
        if (source.isClosed() || !source.isSourceRetryReady()) {
            return null;
        }
        List<IndexProgressState> indexes = source.indexStates();
        long syncLimit = syncStartLimit(indexes);
        long asyncLimit = asyncLimit(indexes, highWatermark);

        IndexProgressState sync = null;
        IndexProgressState async = null;
        for (IndexProgressState index : indexes) {
            if (!eligible(source, index, highWatermark, probe)) {
                continue;
            }
            if (index.spec().isSync()) {
                if (index.pushedOffset() <= syncLimit
                        && (sync == null || index.pushedOffset() < sync.pushedOffset())) {
                    sync = index;
                }
            } else if (index.pushedOffset() < asyncLimit
                    && (async == null || olderAsync(state, index, async))) {
                async = index;
            }
        }
        IndexProgressState selected =
                async != null && (state.asyncOwed || sync == null) ? async : sync;
        if (selected == null) {
            return null;
        }
        long start = selected.pushedOffset();
        long end = highWatermark;
        List<IndexProgressState> group = new ArrayList<>();
        for (IndexProgressState index : indexes) {
            if (index.pushedOffset() != start || !eligible(source, index, highWatermark, probe)) {
                continue;
            }
            if (index.spec().isSync()) {
                if (start > syncLimit) {
                    continue;
                }
            } else {
                if (start >= asyncLimit) {
                    continue;
                }
                end = Math.min(end, asyncLimit);
            }
            if (index.replayEndOffset() >= 0) {
                end = Math.min(end, index.replayEndOffset());
            }
            group.add(index);
        }
        return end > start ? new ReadGroup(start, end, group) : null;
    }

    private static long syncStartLimit(List<IndexProgressState> indexes) {
        long minimum = Long.MAX_VALUE;
        for (IndexProgressState index : indexes) {
            if (index.spec().isSync()) {
                minimum = Math.min(minimum, index.pushedOffset());
            }
        }
        long covered = Long.MAX_VALUE;
        for (IndexProgressState index : indexes) {
            if (index.spec().isSync() && index.pushedOffset() == minimum) {
                long windowEnd = index.healthyWindowEnd();
                if (windowEnd < 0) {
                    return minimum;
                }
                covered = Math.min(covered, windowEnd);
            }
        }
        return covered;
    }

    private static long asyncLimit(List<IndexProgressState> indexes, long highWatermark) {
        long limit = highWatermark;
        for (IndexProgressState index : indexes) {
            if (index.spec().isSync()) {
                long windowEnd = index.healthyWindowEnd();
                limit = Math.min(limit, windowEnd >= 0 ? windowEnd : index.pushedOffset());
            }
        }
        return limit;
    }

    private static boolean eligible(
            IndexReplicator source, IndexProgressState index, long highWatermark, boolean probe) {
        return index.canRead(highWatermark)
                && (source.hasSourceFailure() || index.hasFailure()) == probe;
    }

    private static boolean olderAsync(
            SourceState source, IndexProgressState left, IndexProgressState right) {
        int offsets = Long.compare(left.pushedOffset(), right.pushedOffset());
        return offsets < 0
                || (offsets == 0
                        && source.asyncService.getOrDefault(left, 0L)
                                < source.asyncService.getOrDefault(right, 0L));
    }

    private static boolean isProbe(IndexReplicator source, List<IndexProgressState> indexes) {
        if (source.hasSourceFailure()) {
            return true;
        }
        for (IndexProgressState index : indexes) {
            if (index.hasFailure()) {
                return true;
            }
        }
        return false;
    }

    @Override
    @Nullable
    public ReadGroup selectBuild(IndexReplicator source, ReadGroup pendingGroup) {
        SourceState state = sources.get(source);
        if (state == null || source.isClosed()) {
            return null;
        }
        PollCall call = state.currentCall;
        boolean failed = isProbe(source, pendingGroup.states());
        ProbeAttempt attempt = state.probeAttempt;
        boolean continuation = attempt != null && attempt.matches(pendingGroup) && attempt.charged;
        if (call != null && failed && !call.allowProbe && !continuation) {
            return null;
        }

        ReadGroup selected;
        IndexReplicator wake = null;
        if (!capacityContended) {
            selected = pendingGroup;
        } else {
            synchronized (capacityLock) {
                chooseCapacityLocked();
                CapacityWait owner = active != null ? active : designated;
                if (owner != null && owner.source.source == source) {
                    if (owner.start != pendingGroup.startOffset()
                            || !pendingGroup.states().contains(owner.index)
                            || !source.hasReadyRead(owner.index, owner.start)) {
                        return null;
                    }
                    active = owner;
                    if (!source.hasPageCapacity()) {
                        building = false;
                        buildingCall = null;
                        return null;
                    }
                    building = true;
                    buildingCall = call;
                    selected =
                            new ReadGroup(
                                    pendingGroup.startOffset(),
                                    pendingGroup.endOffset(),
                                    Collections.singletonList(owner.index));
                } else {
                    if (active != null) {
                        return null;
                    }
                    selected = pendingGroup;
                }
                wake = designated == null ? null : designated.source.source;
            }
        }
        wake(wake);
        if (failed) {
            if (attempt == null || !attempt.matches(pendingGroup)) {
                attempt = new ProbeAttempt(pendingGroup);
                state.probeAttempt = attempt;
            }
            if (call != null) {
                call.attempt = attempt;
            }
        } else if (call != null) {
            call.healthy = true;
        }
        for (IndexProgressState index : selected.states()) {
            if (!index.spec().isSync()) {
                state.asyncService.put(index, ++state.serviceSequence);
            }
        }
        return selected;
    }

    @Override
    public void onCapacityBlocked(IndexReplicator source, IndexProgressState index) {
        SourceState state = sources.get(source);
        if (state == null || source.isClosed()) {
            return;
        }
        state.probeAttempt = null;
        IndexReplicator wake;
        synchronized (capacityLock) {
            CapacityWait wait = waits.get(index);
            if (wait != null && wait.start != index.pushedOffset()) {
                removeWaitLocked(wait, false);
                wait = null;
            }
            if (wait == null) {
                wait = new CapacityWait(state, index, index.pushedOffset());
                waits.put(index, wait);
                capacityContended = true;
                waitingTables
                        .computeIfAbsent(
                                source.sourceBucket().getTableId(), ignored -> new TableWaits())
                        .sources
                        .computeIfAbsent(state, ignored -> new ArrayDeque<>())
                        .addLast(wait);
            }
            if (active == wait) {
                clearActiveLocked();
            }
            if (designated == wait) {
                designated = null;
            }
            chooseCapacityLocked();
            wake = designated == null ? null : designated.source.source;
        }
        wake(wake);
    }

    @Override
    public void onWindowPublished(IndexReplicator source, IndexProgressState index) {
        removeIndexWait(source, index, true);
    }

    @Override
    public void onProgress(IndexReplicator source, IndexProgressState index) {
        SourceState state = sources.get(source);
        if (state == null) {
            return;
        }
        state.asyncOwed = index.spec().isSync();
        removeIndexWait(source, index, false);
    }

    @Override
    public void onFailure(IndexReplicator source, @Nullable IndexProgressState index) {
        SourceState state = sources.get(source);
        if (state == null) {
            return;
        }
        if (index == null) {
            state.probeAttempt = null;
            removeSourceWaits(source);
        } else {
            ProbeAttempt attempt = state.probeAttempt;
            if (attempt != null && attempt.indexes.contains(index)) {
                state.probeAttempt = null;
            }
            removeIndexWait(source, index, true);
        }
    }

    private void removeIndexWait(IndexReplicator source, IndexProgressState index, boolean rotate) {
        if (!capacityContended) {
            return;
        }
        IndexReplicator wake;
        synchronized (capacityLock) {
            CapacityWait wait = waits.get(index);
            if (wait != null && wait.source.source == source) {
                removeWaitLocked(wait, rotate);
            }
            chooseCapacityLocked();
            wake = designated == null ? null : designated.source.source;
        }
        wake(wake);
    }

    private void removeSourceWaits(IndexReplicator source) {
        if (!capacityContended) {
            return;
        }
        IndexReplicator wake;
        synchronized (capacityLock) {
            for (CapacityWait wait : new ArrayList<>(waits.values())) {
                if (wait.source.source == source) {
                    removeWaitLocked(wait, false);
                }
            }
            chooseCapacityLocked();
            wake = designated == null ? null : designated.source.source;
        }
        wake(wake);
    }

    private void removeWaitLocked(CapacityWait wait, boolean rotate) {
        // An older ordinary build can finish while a different waiter owns protection.
        rotate &= active == wait;
        waits.remove(wait.index);
        capacityContended = !waits.isEmpty();
        long tableId = wait.source.source.sourceBucket().getTableId();
        TableWaits table = waitingTables.get(tableId);
        if (table != null) {
            Deque<CapacityWait> source = table.sources.get(wait.source);
            if (source != null) {
                source.remove(wait);
                if (source.isEmpty()) {
                    table.sources.remove(wait.source);
                } else if (rotate) {
                    table.sources.remove(wait.source);
                    table.sources.put(wait.source, source);
                }
            }
            if (table.sources.isEmpty()) {
                waitingTables.remove(tableId);
            } else if (rotate) {
                waitingTables.remove(tableId);
                waitingTables.put(tableId, table);
            }
        }
        if (active == wait) {
            clearActiveLocked();
        }
        if (designated == wait) {
            designated = null;
        }
    }

    @Nullable
    private IndexReplicator refreshCapacity() {
        if (!capacityContended) {
            return null;
        }
        synchronized (capacityLock) {
            chooseCapacityLocked();
            return designated == null ? null : designated.source.source;
        }
    }

    private void chooseCapacityLocked() {
        for (CapacityWait wait : new ArrayList<>(waits.values())) {
            if (sources.get(wait.source.source) != wait.source
                    || wait.source.source.isClosed()
                    || wait.index.pushedOffset() != wait.start) {
                removeWaitLocked(wait, false);
            }
        }
        if (active != null) {
            if (building || ready(active)) {
                designated = active;
                return;
            }
            clearActiveLocked();
        }
        if (designated != null && ready(designated)) {
            return;
        }
        designated = null;
        for (TableWaits table : waitingTables.values()) {
            for (Deque<CapacityWait> source : table.sources.values()) {
                for (CapacityWait wait : source) {
                    if (ready(wait)) {
                        designated = wait;
                        return;
                    }
                }
            }
        }
    }

    private static boolean ready(CapacityWait wait) {
        return wait.source.source.hasReadyRead(wait.index, wait.start);
    }

    private void clearActiveLocked() {
        active = null;
        building = false;
        buildingCall = null;
    }

    private void finishBuild(PollCall call) {
        if (!capacityContended) {
            return;
        }
        IndexReplicator wake = null;
        synchronized (capacityLock) {
            if (building && buildingCall == call && active != null) {
                removeWaitLocked(active, true);
                chooseCapacityLocked();
                wake = designated == null ? null : designated.source.source;
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
        private final PollCall call = new PollCall();
        private int scanTable;
        private int scanSource;
        private int sourceCount;
        private int scanRemaining;
        private boolean roundOpen;
        private boolean roundServiced;
        private boolean probeOwed;
    }

    private static final class TableSources {
        private final long tableId;
        private final Map<TableBucket, SourceState> sources = new HashMap<>();
        private final List<SourceState> sourceOrder = new ArrayList<>();

        private TableSources(long tableId) {
            this.tableId = tableId;
        }
    }

    private static final class SourceState {
        private final IndexReplicator source;
        private final TableSources table;
        private final Map<IndexProgressState, Long> asyncService = new IdentityHashMap<>();
        private volatile boolean asyncOwed;
        private long serviceSequence;
        @Nullable private ProbeAttempt probeAttempt;
        @Nullable private PollCall currentCall;

        private SourceState(IndexReplicator source, TableSources table) {
            this.source = source;
            this.table = table;
        }
    }

    private static final class PollCall {
        private SourceState source;
        private boolean allowProbe;
        private boolean healthy;
        @Nullable private ProbeAttempt attempt;
    }

    private static final class ProbeAttempt {
        private final long start;
        private final List<IndexProgressState> indexes;
        private boolean charged;

        private ProbeAttempt(ReadGroup group) {
            start = group.startOffset();
            indexes = group.states();
        }

        private boolean matches(ReadGroup group) {
            return start == group.startOffset() && indexes.equals(group.states());
        }
    }

    private static final class TableWaits {
        private final LinkedHashMap<SourceState, Deque<CapacityWait>> sources =
                new LinkedHashMap<>();
    }

    private static final class CapacityWait {
        private final SourceState source;
        private final IndexProgressState index;
        private final long start;

        private CapacityWait(SourceState source, IndexProgressState index, long start) {
            this.source = source;
            this.index = index;
            this.start = start;
        }
    }
}

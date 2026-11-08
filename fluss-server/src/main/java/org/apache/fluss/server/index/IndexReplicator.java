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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.index.IndexReplicationSupervisor.IndexProgressListener;
import org.apache.fluss.server.index.IndexRequestFactory.SizeAccumulator;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * WAL-driven index replicator that reads committed WAL entries, derives index mutations, and stages
 * them as pre-encoded {@link IndexBatch}es in the server-global {@link IndexSendBuffer}. Derived
 * mutations are appended directly to ordinary per-target-bucket {@link KvRecordBatchBuilder}s.
 *
 * <p>Each secondary index has its own pushed offset and at most one {@link IndexReplicationWindow}
 * in flight. {@link #poll()} reads one group of indexes sharing the same source offset. Window ends
 * may differ after failover because they depend on the fetched input and derived output size. The
 * source advances only completed windows and replays from persisted progress after recovery; the
 * target rejects requests behind its application-level progress record.
 *
 * <p>Driven by {@link #poll()} calls from an {@link IndexReplicationManager} worker.
 */
@Internal
final class IndexReplicator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicator.class);

    private static final int PAGE_SIZE = 4096;
    // Bound failed payload retention independently of the configured retry interval.
    private static final long RETAINED_WINDOW_FAILURE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    private final IndexSourceReader sourceReader;
    private final List<IndexReplicationState> indexStates;
    private final Map<String, IndexReplicationState> indexStatesByName;
    private final IndexSendBuffer sendBuffer;
    private final LogRecordReadContext readContext;
    private final IndexProgressListener onProgress;
    @Nullable private final TabletServerMetricGroup metrics;
    private final IndexReplicationNoProgressTracker noProgressTracker;
    private final int sourceReadTargetBytes;
    private final long maxTransportRequestBytes;

    private volatile boolean closed;
    private final IndexReplicationRetryState sourceRetry;
    @Nullable private volatile Throwable lastFailure;
    private long sourceFailureOffset;
    private final ReentrantLock lifecycleLock;

    @Nullable private volatile PendingRead pendingRead;
    private final IndexReplicationScheduler scheduler;
    private volatile boolean workAttempted;

    /** Signal fired to wake the owning read-pool worker so it polls again promptly. */
    private final Runnable wakeupSignal;

    IndexReplicator(
            IndexSourceReader sourceReader,
            List<IndexSpec> indexSpecs,
            IndexSendBuffer sendBuffer,
            LogRecordReadContext readContext,
            long replayStartOffset,
            int sourceReadTargetBytes,
            IndexProgressListener onProgress,
            IndexReplicationScheduler scheduler,
            Runnable wakeupSignal) {
        this(
                sourceReader,
                indexSpecs,
                sendBuffer,
                readContext,
                replayStartOffset,
                sourceReadTargetBytes,
                onProgress,
                scheduler,
                wakeupSignal,
                new ExponentialBackoff(100, 2, 10_000, 0.2),
                SystemClock.getInstance(),
                null,
                ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.defaultValue().getBytes());
    }

    IndexReplicator(
            IndexSourceReader sourceReader,
            List<IndexSpec> indexSpecs,
            IndexSendBuffer sendBuffer,
            LogRecordReadContext readContext,
            long replayStartOffset,
            int sourceReadTargetBytes,
            IndexProgressListener onProgress,
            IndexReplicationScheduler scheduler,
            Runnable wakeupSignal,
            ExponentialBackoff retryBackoff,
            Clock clock,
            @Nullable TabletServerMetricGroup metrics,
            long maxTransportRequestBytes) {
        checkArgument(maxTransportRequestBytes > 0, "maxTransportRequestBytes must be positive");
        if (replayStartOffset < 0) {
            throw new IllegalArgumentException(
                    "replayStartOffset must be non-negative, but was " + replayStartOffset);
        }
        checkArgument(!indexSpecs.isEmpty(), "indexSpecs must not be empty");
        this.scheduler = checkNotNull(scheduler, "scheduler");
        this.wakeupSignal = checkNotNull(wakeupSignal, "wakeupSignal");
        this.sourceReader = sourceReader;
        this.sourceRetry = new IndexReplicationRetryState(retryBackoff, clock);
        List<IndexReplicationState> states = new ArrayList<>(indexSpecs.size());
        this.indexStatesByName = new LinkedHashMap<>();
        for (IndexSpec indexSpec : indexSpecs) {
            IndexReplicationState state =
                    new IndexReplicationState(indexSpec, replayStartOffset, retryBackoff, clock);
            if (indexStatesByName.put(indexSpec.getIndexName(), state) != null) {
                throw new IllegalArgumentException(
                        "Duplicate secondary index name: " + indexSpec.getIndexName());
            }
            states.add(state);
        }
        this.indexStates = Collections.unmodifiableList(states);
        this.sendBuffer = sendBuffer;
        this.readContext = readContext;
        this.sourceReadTargetBytes = sourceReadTargetBytes;
        this.maxTransportRequestBytes = maxTransportRequestBytes;
        this.lifecycleLock = new ReentrantLock();
        this.onProgress = onProgress;
        this.metrics = metrics;
        this.noProgressTracker =
                new IndexReplicationNoProgressTracker(replayStartOffset, replayStartOffset);
    }

    void checkScheduler(IndexReplicationScheduler scheduler) {
        checkState(this.scheduler == scheduler, "source belongs to another scheduler");
    }

    /**
     * Attempts one scheduled read or construction group. Each index keeps at most one window in
     * flight, and its pushed offset advances only after every target completes.
     *
     * @return whether a read or construction was attempted, including failed attempts
     */
    boolean poll() {
        lifecycleLock.lock();
        try {
            workAttempted = false;
            try {
                pollLocked();
            } catch (RuntimeException failure) {
                Throwable cause =
                        failure instanceof CompletionException && failure.getCause() != null
                                ? failure.getCause()
                                : failure;
                backoff(cause, sourceRetry);
                scheduler.clearProbeAndCapacityWaits(this, null);
            }
            return workAttempted;
        } finally {
            try {
                if (pendingRead == null && (workAttempted || sourceRetry.hasFailure())) {
                    scheduler.releaseReadAdmission(this);
                }
            } finally {
                lifecycleLock.unlock();
            }
        }
    }

    private void pollLocked() {
        if (closed) {
            return;
        }
        boolean released = releaseStalledWindows();
        PendingRead pending = pendingRead;
        if (pending != null
                && pending.future.isDone()
                && (pending.future.isCompletedExceptionally() || pending.future.isCancelled())) {
            consumePendingRead();
            return;
        }
        if (released || sourceRetry.remainingDelayNanos() > 0) {
            return;
        }
        if (pendingRead != null) {
            consumePendingRead();
            return;
        }
        if (sendBuffer.requestCapacityWakeupIfFull()) {
            return;
        }
        long highWatermark = sourceReader.highWatermark();
        ReadGroup group = scheduler.tryAdmitRead(this, highWatermark);
        if (group != null) {
            startGroupRead(group, highWatermark);
        }
    }

    /** Starts the selected read and consumes its result immediately when already completed. */
    private void startGroupRead(ReadGroup group, long highWatermark) {
        long readOffset = group.startOffset;
        highWatermark = Math.min(highWatermark, group.endOffset);
        for (IndexReplicationState state : group.states) {
            checkState(indexStatesByName.get(state.spec.getIndexName()) == state, "foreign index");
            checkState(state.pushedOffset == readOffset, "read must start at pushed offset");
            checkState(state.inFlightWindow == null, "index already has an outstanding window");
            checkState(state.retry.remainingDelayNanos() == 0, "index is backing off");
            if (state.retryWindowEndOffset >= 0) {
                highWatermark = Math.min(highWatermark, state.retryWindowEndOffset);
            }
        }
        checkState(readOffset >= 0 && highWatermark > readOffset, "empty or invalid read range");
        if (highWatermark != group.endOffset) {
            group = new ReadGroup(readOffset, highWatermark, group.states);
        }
        int readTargetBytes = sourceReadTargetBytes;
        for (IndexReplicationState state : group.states) {
            readTargetBytes = Math.min(readTargetBytes, readTargetBytesFor(state));
        }
        workAttempted = true;
        try {
            CompletableFuture<IndexSourceReader.ReadResult> future =
                    sourceReader.read(readOffset, highWatermark, readTargetBytes);
            pendingRead = new PendingRead(group, future, readTargetBytes);
            future.whenComplete((ignored, failure) -> wakeup());
            if (future.isDone()) {
                consumePendingRead();
            }
        } catch (RuntimeException failure) {
            sourceFailureOffset = Math.max(sourceFailureOffset, readOffset);
            throw failure;
        }
    }

    private void consumePendingRead() {
        PendingRead pending = pendingRead;
        if (pending == null || !pending.future.isDone()) {
            return;
        }
        try {
            if (pending.future.isCompletedExceptionally() || pending.future.isCancelled()) {
                pendingRead = null;
                pending.future.getNow(null);
                return;
            }
            IndexSourceReader.ReadResult result = pending.future.getNow(null);
            if (result == null) {
                pendingRead = null;
                return;
            }
            // Register for capacity release notifications, then select the protected build even if
            // full.
            sendBuffer.requestCapacityWakeupIfFull();
            ReadGroup selected = scheduler.tryScheduleBuild(this, pending.group, result);
            if (selected == null) {
                return;
            }
            checkState(selected.startOffset == pending.group.startOffset, "changed read start");
            checkState(
                    selected == pending.group
                            || (selected.states.size() == 1
                                    && pending.group.states.contains(selected.states.get(0))),
                    "build must select the pending group or one member");
            checkState(
                    selected.endOffset <= pending.group.endOffset
                            && result.nextOffset() <= selected.endOffset,
                    "build exceeds the selected range");
            for (IndexReplicationState state : selected.states) {
                checkState(
                        state.pushedOffset == selected.startOffset && state.inFlightWindow == null,
                        "stale build selection");
                checkState(state.retry.remainingDelayNanos() == 0, "index is backing off");
            }
            pendingRead = null;
            workAttempted = true;
            try (IndexSourceReader.ReadResult ownedResult = result) {
                processReadResult(selected.states, ownedResult, pending.readTargetBytes, false);
                if (selected == pending.group && coversRead(selected, ownedResult.nextOffset())) {
                    List<IndexReplicationState> optional =
                            scheduler.selectAsync(this, selected, ownedResult);
                    if (!optional.isEmpty()) {
                        processReadResult(optional, ownedResult, pending.readTargetBytes, true);
                    }
                }
            }
        } catch (RuntimeException failure) {
            sourceFailureOffset = Math.max(sourceFailureOffset, pending.group.startOffset);
            throw failure;
        }
    }

    private boolean coversRead(ReadGroup group, long endOffset) {
        for (IndexReplicationState index : group.states) {
            IndexReplicationWindow window = index.inFlightWindow;
            if (index.pushedOffset < endOffset
                    && (window == null
                            || !window.isAdmitted()
                            || window.windowEndOffset() < endOffset)) {
                return false;
            }
        }
        return true;
    }

    void wakeup() {
        wakeupSignal.run();
    }

    private void processReadResult(
            List<IndexReplicationState> states,
            IndexSourceReader.ReadResult readResult,
            int readTargetBytes,
            boolean optionalAsync) {
        List<WindowBuildState> windows = new ArrayList<>(states.size());
        for (IndexReplicationState state : states) {
            windows.add(
                    new WindowBuildState(
                            state,
                            sendBuffer.newReservation(),
                            readResult,
                            readTargetBytes,
                            optionalAsync));
        }

        try {
            for (LogRecordBatch batch : readResult.batches()) {
                try (CloseableIterator<LogRecord> iter = sourceReader.records(batch, readContext)) {
                    while (iter.hasNext()) {
                        appendMutationGroup(iter, iter.next(), windows);
                    }
                }
            }

            for (WindowBuildState window : windows) {
                publishWindow(window);
                if (window.sizingFailure != null) {
                    retrySmallerWindow(window, window.sizingFailure);
                }
            }
        } finally {
            for (WindowBuildState window : windows) {
                window.close();
            }
        }
    }

    private void publishWindow(WindowBuildState build) {
        if (build.deferred) {
            return;
        }
        IndexReplicationState state = build.state;
        long endOffset = build.input.nextOffset();
        if (endOffset <= state.pushedOffset) {
            return;
        }
        if (build.builders.isEmpty()) {
            scheduler.releaseCapacityWait(this, state);
            advanceOnEmptyWindow(state, endOffset);
            return;
        }

        // Encode and validate the entire window before publishing any target batch.
        IndexReplicationWindow window =
                new IndexReplicationWindow(state, endOffset, build.builders.size(), this);
        List<IndexBatch> batches = new ArrayList<>(build.builders.size());
        try {
            for (Map.Entry<Integer, BucketBatchBuilder> entry : build.builders.entrySet()) {
                int targetBucket = entry.getKey();
                IndexSpec.IndexEntry progress;
                BytesView encoded;
                try {
                    progress = state.spec.encodeProgress(sourceBucket(), targetBucket, endOffset);
                    encoded = entry.getValue().finish(progress.key(), progress.value());
                } catch (IOException | RuntimeException failure) {
                    deferWindow(build, failure);
                    return;
                }
                batches.add(
                        new IndexBatch(
                                new TableBucket(state.spec.getIndexTableId(), targetBucket),
                                progress.key(),
                                encoded,
                                entry.getValue().retainedBytes(),
                                window));
            }
            long largestFrame = 0;
            long retainedBytes = 0;
            for (IndexBatch batch : batches) {
                SizeAccumulator size =
                        new SizeAccumulator(
                                batch.targetBucket().getTableId(),
                                batch.sourceBucket().getTableId(),
                                IndexRequestFactory.DEFAULT_REQUEST_TIMEOUT_MS);
                size.addBucket(batch);
                if (size.exceedsLimit(maxTransportRequestBytes)) {
                    throw size.oversizedFailure(batch, maxTransportRequestBytes);
                }
                largestFrame = Math.max(largestFrame, size.framedBytes());
                retainedBytes += batch.retainedBytes();
            }
            state.installWindow(window);
            if (!sendBuffer.tryAppendWindow(batches, build.reservation)) {
                state.discardUnadmittedWindow(window);
                return;
            }
            if (build.input.isByteLimited()
                    && readTargetBytesFor(state) == build.readTargetBytes
                    && largestFrame <= maxTransportRequestBytes / 2
                    && retainedBytes <= sendBuffer.capacityBytes() / 2) {
                state.adaptiveReadTargetBytes =
                        (int) Math.min(sourceReadTargetBytes, 2L * build.readTargetBytes);
            }
        } catch (RecordTooLargeException failure) {
            state.discardUnadmittedWindow(window);
            retrySmallerWindow(build, failure);
            return;
        } catch (RuntimeException | Error failure) {
            state.discardUnadmittedWindow(window);
            if (window.isAdmitted()) {
                scheduler.releaseCapacityWait(this, state);
            }
            throw failure;
        }
        scheduler.releaseCapacityWait(this, state);
    }

    private void retrySmallerWindow(WindowBuildState build, Throwable failure) {
        IndexReplicationState state = build.state;
        int smallerBytes = build.input.smallerReadBytes();
        scheduler.clearProbeAndCapacityWaits(this, state);
        if (smallerBytes > 0 && smallerBytes < readTargetBytesFor(state)) {
            // Shrink only our own infeasible output, never temporary competition for shared pages.
            state.adaptiveReadTargetBytes = smallerBytes;
            LOG.debug(
                    "Reducing index source read for {} / {} to {} bytes at offset {}",
                    sourceBucket(),
                    state.spec.getIndexName(),
                    smallerBytes,
                    state.pushedOffset);
        } else {
            backoff(failure, state.retry);
        }
    }

    int readTargetBytesFor(IndexReplicationState state) {
        return Math.min(sourceReadTargetBytes, state.adaptiveReadTargetBytes);
    }

    private void advanceOnEmptyWindow(IndexReplicationState state, long windowEndOffset) {
        if (state.advanceTo(windowEndOffset)) {
            notifyProgress(state);
        }
    }

    /**
     * Called by {@link IndexReplicationWindow} when all of its batches have been acknowledged.
     * Advances that index's pushed offset to the window end, clears the per-index in-flight window,
     * notifies the owning replica, and wakes the read-pool worker so it can poll the next ready
     * window.
     */
    void onWindowComplete(IndexReplicationWindow window, long completedBytes) {
        lifecycleLock.lock();
        try {
            if (closed) {
                return;
            }
            IndexReplicationState state = window.index();
            if (!state.ownsWindow(window)) {
                return;
            }
            boolean advanced = state.completeWindow(window);
            if (metrics != null) {
                metrics.indexReplicationCompletedBytes().inc(completedBytes);
            }
            notifyProgress(advanced ? state : null);
        } finally {
            lifecycleLock.unlock();
        }
        wakeupSignal.run();
    }

    private void backoff(Throwable failure, IndexReplicationRetryState retry) {
        if (failure instanceof IndexSendBuffer.BufferFullException) {
            retry.onCapacityUnavailable();
            return;
        }
        Throwable previous = lastFailure;
        if (previous == null
                || previous.getClass() != failure.getClass()
                || !Objects.equals(previous.getMessage(), failure.getMessage())) {
            LOG.error(
                    "Index replication for source bucket {} cannot advance at offset {}. "
                            + "Retrying with backoff until the fault is repaired",
                    sourceBucket(),
                    getAllIndexPushedOffset(),
                    failure);
        }
        lastFailure = failure;
        retry.onFailure();
        if (metrics != null) {
            metrics.indexReplicationFailures().inc();
        }
    }

    boolean hasFailure() {
        if (sourceRetry.hasFailure()) {
            return true;
        }
        for (IndexReplicationState state : indexStates) {
            if (state.retry.hasFailure()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Yields index-owned payload capacity while retained WAL and unchanged source progress keep the
     * window recoverable. Other source buckets can then compete for the shared budget.
     *
     * <p>A rebuilt window shares its predecessor's retry history. Requiring an observed failure in
     * the current window gives healthy attempts an opportunity to receive their acknowledgements.
     * The replay boundary and retry history survive yielding, followed by backoff before
     * rebuilding.
     */
    private boolean releaseStalledWindows() {
        boolean released = false;
        for (IndexReplicationState state : indexStates) {
            IndexReplicationWindow window =
                    state.retireStalledWindow(RETAINED_WINDOW_FAILURE_TIMEOUT_NS);
            if (window != null) {
                sendBuffer.dropWindow(window);
                LOG.warn(
                        "Yielding index {} window for source {} at offsets [{}, {}). "
                                + "Rebuilding after backoff",
                        state.spec.getIndexName(),
                        sourceBucket(),
                        state.pushedOffset,
                        window.windowEndOffset());
                released = true;
            }
        }
        return released;
    }

    private void retireOwnedBatchesLocked() {
        Set<IndexReplicationWindow> windows = new HashSet<>();
        for (IndexReplicationState state : indexStates) {
            IndexReplicationWindow window = state.retireWindow();
            if (window != null) {
                windows.add(window);
            }
        }
        int dropped = sendBuffer.dropWindows(windows);
        if (dropped > 0) {
            LOG.info(
                    "Discarded {} queued index batch(es) after stopping source {}",
                    dropped,
                    sourceBucket());
        }
    }

    private void appendMutationGroup(
            CloseableIterator<LogRecord> records, LogRecord first, List<WindowBuildState> windows) {
        ChangeType changeType = first.getChangeType();
        if (changeType == null) {
            throw corruption("record at offset " + first.logOffset() + " has no change type");
        }
        switch (changeType) {
            case INSERT:
                for (WindowBuildState window : windows) {
                    appendMutation(window, first.getRow());
                }
                return;
            case DELETE:
                for (WindowBuildState window : windows) {
                    if (!window.deferred) {
                        try {
                            window.captureBefore(first.getRow());
                            appendMutation(window, null);
                        } catch (RuntimeException failure) {
                            deferWindow(window, failure);
                        }
                    }
                }
                return;
            case UPDATE_AFTER:
                throw corruption(
                        "UPDATE_AFTER at offset "
                                + first.logOffset()
                                + " has no adjacent UPDATE_BEFORE in the same source batch");
            case APPEND_ONLY:
                throw corruption(
                        "unsupported source change type APPEND_ONLY at offset "
                                + first.logOffset());
            case UPDATE_BEFORE:
                break;
            default:
                throw corruption(
                        "unsupported source change type "
                                + changeType
                                + " at offset "
                                + first.logOffset());
        }
        for (WindowBuildState window : windows) {
            if (!window.deferred) {
                try {
                    window.captureBefore(first.getRow());
                } catch (RuntimeException failure) {
                    deferWindow(window, failure);
                }
            }
        }
        if (!records.hasNext()) {
            throw corruption(
                    "UPDATE_BEFORE at offset "
                            + first.logOffset()
                            + " is not completed in the same source batch");
        }
        LogRecord after = records.next();
        if (after.getChangeType() != ChangeType.UPDATE_AFTER
                || after.logOffset() != first.logOffset() + 1) {
            throw corruption(
                    "UPDATE_BEFORE at offset "
                            + first.logOffset()
                            + " is not followed by adjacent UPDATE_AFTER in the same source batch");
        }
        for (WindowBuildState window : windows) {
            appendMutation(window, after.getRow());
        }
    }

    private IndexSourceWalCorruptionException corruption(String message) {
        return new IndexSourceWalCorruptionException(
                "Corrupt source WAL for " + sourceBucket() + ": " + message);
    }

    private void appendMutation(WindowBuildState window, @Nullable InternalRow newRow) {
        if (window.deferred) {
            return;
        }
        try {
            window.appendAfter(newRow);
        } catch (RuntimeException failure) {
            deferWindow(window, failure);
        }
    }

    private void deferWindow(WindowBuildState window, Throwable failure) {
        window.deferred = true;
        boolean optionalCapacity =
                window.optionalAsync && failure instanceof IndexSendBuffer.BufferFullException;
        if (failure instanceof RecordTooLargeException) {
            window.sizingFailure = failure;
        } else if (failure instanceof IndexSendBuffer.BufferFullException) {
            // Optional reuse must not claim capacity protection or roll back published SYNC.
            // The ordinary ASYNC path remains responsible for making progress.
            if (!optionalCapacity) {
                window.state.limitRetryWindowEnd(window.input.nextOffset());
                scheduler.onCapacityBlocked(this, window.state);
            }
        } else {
            scheduler.clearProbeAndCapacityWaits(this, window.state);
        }
        window.close();
        if (window.sizingFailure == null && !optionalCapacity) {
            backoff(failure, window.state.retry);
        }
    }

    long getSyncIndexPushedOffset() {
        long min = Long.MAX_VALUE;
        boolean hasSyncIndex = false;
        for (IndexReplicationState state : indexStates) {
            if (state.spec.isSync()) {
                hasSyncIndex = true;
                min = Math.min(min, state.pushedOffset);
            }
        }
        if (hasSyncIndex) {
            return min;
        }
        return Long.MAX_VALUE;
    }

    long getAllIndexPushedOffset() {
        long min = Long.MAX_VALUE;
        for (IndexReplicationState state : indexStates) {
            min = Math.min(min, state.pushedOffset);
        }
        return min;
    }

    void onHighWatermarkAdvanced() {
        if (closed) {
            return;
        }
        noProgressTracker.update(getAllIndexPushedOffset(), sourceReader.highWatermark());
    }

    long noProgressTimeMs() {
        if (closed) {
            return 0L;
        }
        return noProgressTracker.noProgressTimeMs(
                getAllIndexPushedOffset(), sourceReader.highWatermark());
    }

    private void notifyProgress(@Nullable IndexReplicationState advancedState) {
        long allIndexPushedOffset = getAllIndexPushedOffset();
        if (allIndexPushedOffset > sourceFailureOffset) {
            sourceRetry.reset();
        }
        if (!hasFailure()) {
            lastFailure = null;
        }
        noProgressTracker.update(allIndexPushedOffset, sourceReader.highWatermark());
        if (advancedState != null) {
            scheduler.onProgress(this, advancedState);
        }
        onProgress.onProgress(getSyncIndexPushedOffset(), allIndexPushedOffset);
    }

    @Override
    public void close() {
        lifecycleLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            Throwable cleanupFailure = cleanupOwnedResourcesLocked();
            if (cleanupFailure != null) {
                LOG.warn(
                        "Failed to completely close index replication resources for source bucket"
                                + " {}",
                        sourceReader.tableBucket(),
                        cleanupFailure);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Nullable
    private Throwable cleanupOwnedResourcesLocked() {
        Throwable failure = null;
        failure = runCleanupStep(failure, () -> scheduler.clearProbeAndCapacityWaits(this, null));
        failure = runCleanupStep(failure, this::retirePendingReadLocked);
        failure = runCleanupStep(failure, this::retireOwnedBatchesLocked);
        failure = runCleanupStep(failure, sourceReader::close);
        failure = runCleanupStep(failure, this::closeReadContext);
        return runCleanupStep(failure, () -> scheduler.releaseReadAdmission(this));
    }

    @Nullable
    private static Throwable runCleanupStep(@Nullable Throwable previousFailure, Runnable step) {
        try {
            step.run();
        } catch (Throwable failure) {
            return ExceptionUtils.firstOrSuppressed(failure, previousFailure);
        }
        return previousFailure;
    }

    private void retirePendingReadLocked() {
        PendingRead pending = pendingRead;
        pendingRead = null;
        if (pending == null) {
            return;
        }
        CompletableFuture<IndexSourceReader.ReadResult> future = pending.future;
        boolean cancelled = future.cancel(true);
        if (!cancelled
                && future.isDone()
                && !future.isCompletedExceptionally()
                && !future.isCancelled()) {
            IndexSourceReader.ReadResult result = future.getNow(null);
            if (result != null) {
                result.close();
            }
        }
    }

    boolean isClosed() {
        return closed;
    }

    /** The source main-table bucket this replicator reads from. */
    TableBucket sourceBucket() {
        return sourceReader.tableBucket();
    }

    List<IndexReplicationState> indexStates() {
        return indexStates;
    }

    boolean hasSourceFailure() {
        return sourceRetry.hasFailure();
    }

    boolean isSourceRetryReady() {
        return sourceRetry.remainingDelayNanos() == 0;
    }

    long highWatermark() {
        return sourceReader.highWatermark();
    }

    boolean hasPendingRead() {
        return pendingRead != null;
    }

    boolean hasReadyRead(IndexReplicationState index, long startOffset) {
        PendingRead pending = pendingRead;
        return !closed
                && pending != null
                && pending.group.startOffset == startOffset
                && pending.group.states.contains(index)
                && pending.future.isDone()
                && !pending.future.isCompletedExceptionally()
                && !pending.future.isCancelled()
                && index.pushedOffset == startOffset
                && index.inFlightWindow == null
                && index.retry.remainingDelayNanos() == 0
                && sourceRetry.remainingDelayNanos() == 0;
    }

    boolean hasPageCapacity() {
        return sendBuffer.hasPageCapacity(PAGE_SIZE);
    }

    boolean hasAttemptedWork() {
        return workAttempted;
    }

    private void closeReadContext() {
        if (readContext != null) {
            readContext.close();
        }
    }

    static final class IndexReplicationState {
        private final IndexSpec spec;
        private volatile long pushedOffset;
        private final IndexReplicationRetryState retry;
        private volatile long retryWindowEndOffset = -1L;
        private int adaptiveReadTargetBytes = Integer.MAX_VALUE;
        @Nullable private volatile IndexReplicationWindow inFlightWindow;

        private IndexReplicationState(
                IndexSpec spec, long replayStartOffset, ExponentialBackoff backoff, Clock clock) {
            this.spec = spec;
            this.pushedOffset = replayStartOffset;
            this.retry = new IndexReplicationRetryState(backoff, clock);
        }

        IndexReplicationRetryState retryState() {
            return retry;
        }

        // Mutations run under the owning source's lifecycle lock; scheduler observations use
        // the existing volatile progress/window fields.
        void installWindow(IndexReplicationWindow window) {
            checkState(inFlightWindow == null, "index already has an outstanding window");
            checkArgument(window.index() == this, "foreign index window");
            inFlightWindow = window;
        }

        boolean ownsWindow(IndexReplicationWindow window) {
            return inFlightWindow == window;
        }

        boolean completeWindow(IndexReplicationWindow window) {
            checkState(ownsWindow(window), "stale index window");
            boolean advanced = advanceTo(window.windowEndOffset());
            inFlightWindow = null;
            return advanced;
        }

        void discardUnadmittedWindow(IndexReplicationWindow window) {
            if (window.isAdmitted()) {
                return;
            }
            if (ownsWindow(window)) {
                inFlightWindow = null;
            }
            window.tryRetire();
        }

        @Nullable
        IndexReplicationWindow retireWindow() {
            IndexReplicationWindow window = inFlightWindow;
            inFlightWindow = null;
            if (window != null) {
                window.tryRetire();
            }
            return window;
        }

        /** Retires and detaches the window; the caller must release its send-buffer payload. */
        IndexReplicationWindow retireStalledWindow(long retentionNanos) {
            IndexReplicationWindow window = inFlightWindow;
            if (window == null
                    || !window.hasFailure()
                    || !retry.shouldYieldPayload(retentionNanos)
                    || !window.tryRetire()) {
                return null;
            }
            retryWindowEndOffset = window.windowEndOffset();
            inFlightWindow = null;
            retry.deferRetryAfterYield();
            return window;
        }

        void limitRetryWindowEnd(long endOffset) {
            retryWindowEndOffset =
                    retryWindowEndOffset < 0
                            ? endOffset
                            : Math.min(retryWindowEndOffset, endOffset);
        }

        boolean advanceTo(long newOffset) {
            if (newOffset <= pushedOffset) {
                return false;
            }
            pushedOffset = newOffset;
            retry.reset();
            if (newOffset >= retryWindowEndOffset) {
                retryWindowEndOffset = -1L;
            }
            return true;
        }

        IndexSpec spec() {
            return spec;
        }

        long pushedOffset() {
            return pushedOffset;
        }

        long acknowledgedWindowEnd() {
            IndexReplicationWindow window = inFlightWindow;
            return window != null && window.isAcknowledged() && inFlightWindow == window
                    ? window.windowEndOffset()
                    : -1L;
        }

        boolean hasFailure() {
            return retry.hasFailure();
        }

        boolean canRead(long highWatermark) {
            long offset = pushedOffset;
            long replayEnd = retryWindowEndOffset;
            return inFlightWindow == null
                    && retry.remainingDelayNanos() == 0
                    && offset < highWatermark
                    && (replayEnd < 0 || replayEnd > offset);
        }

        long retryWindowEndOffset() {
            return retryWindowEndOffset;
        }

        long healthyWindowEnd() {
            IndexReplicationWindow window = inFlightWindow;
            return window != null && window.isActive() && !window.hasFailure()
                    ? window.windowEndOffset()
                    : -1L;
        }
    }

    static final class ReadGroup {
        private final long startOffset;
        private final long endOffset;
        private final List<IndexReplicationState> states;

        ReadGroup(long startOffset, long endOffset, List<IndexReplicationState> states) {
            checkArgument(!states.isEmpty(), "read group must contain an index");
            checkArgument(new HashSet<>(states).size() == states.size(), "duplicate index");
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.states = Collections.unmodifiableList(new ArrayList<>(states));
        }

        long startOffset() {
            return startOffset;
        }

        long endOffset() {
            return endOffset;
        }

        List<IndexReplicationState> states() {
            return states;
        }
    }

    private static final class PendingRead {
        private final ReadGroup group;
        private final CompletableFuture<IndexSourceReader.ReadResult> future;
        private final int readTargetBytes;

        private PendingRead(
                ReadGroup group,
                CompletableFuture<IndexSourceReader.ReadResult> future,
                int readTargetBytes) {
            this.group = group;
            this.future = future;
            this.readTargetBytes = readTargetBytes;
        }
    }

    private static final class WindowBuildState implements AutoCloseable {
        private final IndexReplicationState state;
        // The index table is fixed for this window; only the target bucket ID varies.
        private final Map<Integer, BucketBatchBuilder> builders = new HashMap<>();
        private final IndexSendBuffer.Reservation reservation;
        private final IndexSourceReader.ReadResult input;
        private final int readTargetBytes;
        private final boolean optionalAsync;
        private boolean deferred;
        @Nullable private Throwable sizingFailure;
        @Nullable private byte[] beforeKey;
        private int beforeBucket;

        private WindowBuildState(
                IndexReplicationState state,
                IndexSendBuffer.Reservation reservation,
                IndexSourceReader.ReadResult input,
                int readTargetBytes,
                boolean optionalAsync) {
            this.state = state;
            this.reservation = reservation;
            this.input = input;
            this.readTargetBytes = readTargetBytes;
            this.optionalAsync = optionalAsync;
        }

        void captureBefore(InternalRow row) {
            beforeKey = null;
            if (state.spec.hasNonNullIndexValues(row)) {
                IndexSpec.IndexEntry entry = state.spec.encodeEntry(row);
                beforeKey = entry.key();
                beforeBucket = entry.targetBucket();
            }
        }

        void appendAfter(@Nullable InternalRow row) {
            try {
                IndexSpec spec = state.spec;
                IndexSpec.IndexEntry entry =
                        row != null && spec.hasNonNullIndexValues(row)
                                ? spec.encodeEntry(row)
                                : null;
                boolean keysDiffer =
                        beforeKey != null
                                && (entry == null || !Arrays.equals(beforeKey, entry.key()));
                if (keysDiffer) {
                    bucketBuilder(beforeBucket).appendDelete(beforeKey);
                }
                if (entry != null && (beforeKey == null || keysDiffer)) {
                    bucketBuilder(entry.targetBucket()).appendUpsert(entry.key(), entry.value());
                }
            } finally {
                // Only stable keys cross the BEFORE/AFTER decoder boundary; never retain a row.
                beforeKey = null;
            }
        }

        private BucketBatchBuilder bucketBuilder(int targetBucket) {
            BucketBatchBuilder builder = builders.get(targetBucket);
            if (builder == null) {
                IndexSpec spec = state.spec;
                builder =
                        new BucketBatchBuilder(
                                (short) spec.getIndexSchemaId(),
                                spec.getIndexKvFormat(),
                                reservation);
                builders.put(targetBucket, builder);
            }
            return builder;
        }

        @Override
        public void close() {
            beforeKey = null;
            builders.clear();
            reservation.close();
        }
    }

    /** Tracks how long this source bucket has remained behind without advancing index progress. */
    private static final class IndexReplicationNoProgressTracker {

        private static final long NOT_TRACKING = Long.MIN_VALUE;

        private long highestPushedOffset;
        private long noProgressStartNanos = NOT_TRACKING;

        private IndexReplicationNoProgressTracker(
                long initialPushedOffset, long initialHighWatermark) {
            this.highestPushedOffset = initialPushedOffset;
            update(initialPushedOffset, initialHighWatermark);
        }

        private synchronized void update(long pushedOffset, long highWatermark) {
            update(pushedOffset, highWatermark, System.nanoTime());
        }

        private synchronized long noProgressTimeMs(long pushedOffset, long highWatermark) {
            long now = System.nanoTime();
            update(pushedOffset, highWatermark, now);
            if (noProgressStartNanos == NOT_TRACKING) {
                return 0L;
            }
            return TimeUnit.NANOSECONDS.toMillis(Math.max(0L, now - noProgressStartNanos));
        }

        private void update(long pushedOffset, long highWatermark, long now) {
            if (pushedOffset >= highWatermark) {
                highestPushedOffset = Math.max(highestPushedOffset, pushedOffset);
                noProgressStartNanos = NOT_TRACKING;
                return;
            }

            if (noProgressStartNanos == NOT_TRACKING || pushedOffset > highestPushedOffset) {
                noProgressStartNanos = now;
            }
            highestPushedOffset = Math.max(highestPushedOffset, pushedOffset);
        }
    }

    /** Per-target-bucket builder that directly encodes KV records. */
    static final class BucketBatchBuilder {
        private final AbstractPagedOutputView output;
        final KvRecordBatchBuilder builder;

        @VisibleForTesting
        BucketBatchBuilder(short schemaId, KvFormat kvFormat) {
            this(schemaId, kvFormat, new UnmanagedPagedOutputView(PAGE_SIZE));
        }

        BucketBatchBuilder(
                short schemaId, KvFormat kvFormat, IndexSendBuffer.Reservation reservation) {
            this(
                    schemaId,
                    kvFormat,
                    new AbstractPagedOutputView(reservation.allocatePage(PAGE_SIZE), PAGE_SIZE) {
                        @Override
                        protected MemorySegment nextSegment() {
                            return reservation.allocatePage(PAGE_SIZE);
                        }

                        @Override
                        public List<MemorySegment> allocatedPooledSegments() {
                            return Collections.emptyList();
                        }
                    });
        }

        private BucketBatchBuilder(
                short schemaId, KvFormat kvFormat, AbstractPagedOutputView output) {
            this.output = output;
            this.builder =
                    KvRecordBatchBuilder.builder(schemaId, Integer.MAX_VALUE, output, kvFormat);
        }

        void appendUpsert(byte[] key, BinaryRow value) {
            try {
                builder.append(key, value);
            } catch (IOException e) {
                throw new RuntimeException("Failed to append upsert to batch", e);
            }
        }

        void appendDelete(byte[] key) {
            try {
                builder.append(key, null);
            } catch (IOException e) {
                throw new RuntimeException("Failed to append delete to batch", e);
            }
        }

        BytesView finish(byte[] progressKey, BinaryRow progressValue) throws IOException {
            builder.append(progressKey, progressValue);
            return builder.build();
        }

        long retainedBytes() {
            return Math.multiplyExact((long) output.getWrittenSegments().size(), PAGE_SIZE);
        }
    }
}

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
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.CloseableIterator;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
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
 * <p>Driven by {@link #poll()} calls from an {@code IndexReplicatorPool} read worker.
 */
@Internal
public final class IndexReplicator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicator.class);

    private static final int PAGE_SIZE = 4096;
    // Bound failed payload retention independently of the configured retry interval.
    private static final long RETAINED_WINDOW_FAILURE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /** Receives sync and all-index progress after an index window advances. */
    @FunctionalInterface
    public interface IndexProgressListener {
        /** Called with the current sync ack watermark and conservative all-index replay floor. */
        void onProgress(long syncIndexPushedOffset, long allIndexPushedOffset);
    }

    private final IndexSourceReader sourceReader;
    private final List<IndexProgressState> indexStates;
    private final Map<String, IndexProgressState> indexStatesByName;
    private final IndexSendBuffer sendBuffer;
    private final LogRecordReadContext readContext;
    private final IndexProgressListener onProgress;
    @Nullable private final TabletServerMetricGroup metrics;
    private final IndexReplicationNoProgressTracker noProgressTracker;
    private final int maxWindowBytes;
    private final long maxTransportRequestBytes;

    /** Progress used only by empty test owners; production replicators always have index states. */
    private volatile long emptyIndexPushedOffset;

    private final AtomicBoolean closed;
    private final AtomicBoolean readContextClosed;
    private final IndexReplicationRetryState sourceRetry;
    @Nullable private volatile Throwable lastFailure;
    private long sourceFailureOffset;
    private final ReentrantLock lifecycleLock;

    @Nullable private volatile PendingRead pendingRead;
    @Nullable private IndexReplicationScheduler scheduler;
    private volatile boolean pollServiced;

    /** Signal fired to wake the owning read-pool worker so it polls again promptly. */
    @Nullable private volatile Runnable wakeupSignal;

    IndexReplicator(
            IndexSourceReader sourceReader,
            List<IndexSpec> indexSpecs,
            IndexSendBuffer sendBuffer,
            LogRecordReadContext readContext,
            long initialOffset,
            int maxWindowBytes,
            IndexProgressListener onProgress) {
        this(
                sourceReader,
                indexSpecs,
                sendBuffer,
                readContext,
                initialOffset,
                maxWindowBytes,
                onProgress,
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
            long initialOffset,
            int maxWindowBytes,
            IndexProgressListener onProgress,
            ExponentialBackoff retryBackoff,
            Clock clock,
            @Nullable TabletServerMetricGroup metrics,
            long maxTransportRequestBytes) {
        checkArgument(maxTransportRequestBytes > 0, "maxTransportRequestBytes must be positive");
        if (initialOffset < 0) {
            throw new IllegalArgumentException(
                    "initialOffset must be non-negative, but was " + initialOffset);
        }
        this.sourceReader = sourceReader;
        this.sourceRetry = new IndexReplicationRetryState(retryBackoff, clock);
        List<IndexProgressState> states = new ArrayList<>(indexSpecs.size());
        this.indexStatesByName = new LinkedHashMap<>();
        for (IndexSpec indexSpec : indexSpecs) {
            IndexProgressState state =
                    new IndexProgressState(indexSpec, initialOffset, retryBackoff, clock);
            if (indexStatesByName.put(indexSpec.getIndexName(), state) != null) {
                throw new IllegalArgumentException(
                        "Duplicate secondary index name: " + indexSpec.getIndexName());
            }
            states.add(state);
        }
        this.indexStates = Collections.unmodifiableList(states);
        this.sendBuffer = sendBuffer;
        this.readContext = readContext;
        this.emptyIndexPushedOffset = initialOffset;
        this.maxWindowBytes = maxWindowBytes;
        this.maxTransportRequestBytes = maxTransportRequestBytes;
        this.closed = new AtomicBoolean(false);
        this.readContextClosed = new AtomicBoolean(false);
        this.lifecycleLock = new ReentrantLock();
        this.onProgress = onProgress;
        this.metrics = metrics;
        this.noProgressTracker =
                new IndexReplicationNoProgressTracker(initialOffset, initialOffset);
    }

    /** Sets the wake-up signal used to nudge the read-pool worker after a window completes. */
    void setWakeupSignal(Runnable wakeupSignal) {
        this.wakeupSignal = wakeupSignal;
    }

    void setScheduler(IndexReplicationScheduler scheduler) {
        lifecycleLock.lock();
        try {
            checkArgument(scheduler != null, "scheduler must not be null");
            checkState(this.scheduler == null || this.scheduler == scheduler, "scheduler is bound");
            this.scheduler = scheduler;
        } finally {
            lifecycleLock.unlock();
        }
    }

    /**
     * Attempts one scheduled read or construction group. Each index keeps at most one window in
     * flight, and its pushed offset advances only after every target completes.
     *
     * @return whether a read or construction was attempted, including failed attempts
     */
    public boolean poll() {
        lifecycleLock.lock();
        try {
            pollServiced = false;
            checkState(scheduler != null, "index replication scheduler is not bound");
            try {
                pollLocked();
            } catch (RuntimeException failure) {
                Throwable cause =
                        failure instanceof CompletionException && failure.getCause() != null
                                ? failure.getCause()
                                : failure;
                backoff(cause, sourceRetry);
                scheduler.onFailure(this, null);
            }
            return pollServiced;
        } finally {
            try {
                if (scheduler != null
                        && pendingRead == null
                        && (pollServiced || sourceRetry.hasFailure())) {
                    scheduler.onReadClosed(this);
                }
            } finally {
                lifecycleLock.unlock();
            }
        }
    }

    private boolean pollLocked() {
        if (closed.get()) {
            return false;
        }
        boolean released = releaseStalledWindows();
        PendingRead pending = pendingRead;
        if (pending != null
                && pending.future.isDone()
                && (pending.future.isCompletedExceptionally() || pending.future.isCancelled())) {
            return consumePendingRead();
        }
        if (released || sourceRetry.remainingDelayNanos() > 0) {
            return false;
        }
        if (pendingRead != null) {
            return consumePendingRead();
        }
        if (sendBuffer.isFull()) {
            return false;
        }
        long highWatermark = sourceReader.highWatermark();
        ReadGroup group = scheduler.selectRead(this, highWatermark);
        return group != null && pollOneWindow(group, highWatermark);
    }

    /** Starts the selected read and consumes its result immediately when already completed. */
    private boolean pollOneWindow(ReadGroup group, long highWatermark) {
        long readOffset = group.startOffset;
        highWatermark = Math.min(highWatermark, group.endOffset);
        for (IndexProgressState state : group.states) {
            checkState(indexStatesByName.get(state.spec.getIndexName()) == state, "foreign index");
            checkState(state.pushedOffset == readOffset, "read must start at pushed offset");
            checkState(state.inFlightWindow == null, "index already has an outstanding window");
            checkState(state.retry.remainingDelayNanos() == 0, "index is backing off");
            if (state.replayEndOffset >= 0) {
                highWatermark = Math.min(highWatermark, state.replayEndOffset);
            }
        }
        checkState(readOffset >= 0 && highWatermark > readOffset, "empty or invalid read range");
        if (highWatermark != group.endOffset) {
            group = new ReadGroup(readOffset, highWatermark, group.states);
        }
        int readBudget = maxWindowBytes;
        for (IndexProgressState state : group.states) {
            readBudget = Math.min(readBudget, readBudgetFor(state));
        }
        pollServiced = true;
        try {
            CompletableFuture<IndexSourceReader.ReadResult> future =
                    sourceReader.read(readOffset, highWatermark, readBudget);
            pendingRead = new PendingRead(group, future, readBudget);
            future.whenComplete((ignored, failure) -> wakeup());
            return future.isDone() && consumePendingRead();
        } catch (RuntimeException failure) {
            sourceFailureOffset = Math.max(sourceFailureOffset, readOffset);
            throw failure;
        }
    }

    private boolean consumePendingRead() {
        PendingRead pending = pendingRead;
        if (pending == null || !pending.future.isDone()) {
            return false;
        }
        try {
            if (pending.future.isCompletedExceptionally() || pending.future.isCancelled()) {
                pendingRead = null;
                pending.future.getNow(null);
                return false;
            }
            IndexSourceReader.ReadResult result = pending.future.getNow(null);
            if (result == null) {
                pendingRead = null;
                return false;
            }
            // Register for capacity release notifications, then select the protected build even if
            // full.
            sendBuffer.isFull();
            ReadGroup selected = scheduler.selectBuild(this, pending.group, result);
            if (selected == null) {
                return false;
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
            for (IndexProgressState state : selected.states) {
                checkState(
                        state.pushedOffset == selected.startOffset && state.inFlightWindow == null,
                        "stale build selection");
                checkState(state.retry.remainingDelayNanos() == 0, "index is backing off");
            }
            pendingRead = null;
            pollServiced = true;
            try (IndexSourceReader.ReadResult ownedResult = result) {
                boolean processed =
                        processReadResult(
                                selected.states,
                                selected.startOffset,
                                ownedResult,
                                pending.readBudget,
                                false);
                if (selected == pending.group && coversRead(selected, ownedResult.nextOffset())) {
                    List<IndexProgressState> optional =
                            scheduler.selectAsync(this, selected, ownedResult);
                    if (!optional.isEmpty()) {
                        processed |=
                                processReadResult(
                                        optional,
                                        selected.startOffset,
                                        ownedResult,
                                        pending.readBudget,
                                        true);
                    }
                }
                return processed;
            }
        } catch (RuntimeException failure) {
            sourceFailureOffset = Math.max(sourceFailureOffset, pending.group.startOffset);
            throw failure;
        }
    }

    private boolean coversRead(ReadGroup group, long endOffset) {
        for (IndexProgressState index : group.states) {
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
        Runnable signal = this.wakeupSignal;
        if (signal != null) {
            signal.run();
        }
    }

    private boolean processReadResult(
            List<IndexProgressState> states,
            long readOffset,
            IndexSourceReader.ReadResult readResult,
            int readBudget,
            boolean optionalAsync) {
        List<WindowBuildState> windows = new ArrayList<>(states.size());
        for (IndexProgressState state : states) {
            windows.add(
                    new WindowBuildState(
                            state,
                            sendBuffer.newReservation(),
                            readResult,
                            readBudget,
                            optionalAsync));
        }

        try {
            for (IndexSourceReader.SourceBatchSlice batch : readResult.batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        LogRecord record = iter.next();
                        if (record.logOffset() < readOffset) {
                            continue;
                        }
                        if (record.logOffset() >= readResult.nextOffset()) {
                            break;
                        }
                        appendMutationGroup(iter, record, windows);
                    }
                }

                for (WindowBuildState window : windows) {
                    window.lastProcessedOffset =
                            Math.max(window.lastProcessedOffset, batch.nextOffset());
                }
            }

            boolean processed = false;
            for (WindowBuildState window : windows) {
                window.lastProcessedOffset =
                        Math.max(window.lastProcessedOffset, readResult.nextOffset());
                processed |= publishWindow(window);
            }
            return processed;
        } finally {
            for (WindowBuildState window : windows) {
                window.reservation.close();
            }
        }
    }

    private boolean publishWindow(WindowBuildState buildState) {
        if (buildState.deferred) {
            if (buildState.sizingFailure != null) {
                retrySmallerWindow(buildState, buildState.sizingFailure);
            }
            return false;
        }
        IndexProgressState state = buildState.state;
        long lastProcessedOffset = buildState.lastProcessedOffset;
        Map<TableBucket, BucketBatchBuilder> builders = buildState.builders;

        // No records advanced: nothing to do this cycle.
        if (lastProcessedOffset <= state.pushedOffset) {
            return false;
        }

        // Encode all accumulated per-bucket batches. If any bucket fails to encode, abandon the
        // whole window without advancing the pushed offset: the same WAL range is re-read and
        // re-encoded on the next cycle. Skipping the failed bucket while still advancing the offset
        // would silently drop that bucket's index mutations (permanent data loss), so a failure
        // must stall the window rather than leak past it.
        Map<TableBucket, EncodedBatch> encoded = new HashMap<>(builders.size());
        for (Map.Entry<TableBucket, BucketBatchBuilder> entry : builders.entrySet()) {
            try {
                IndexSpec.IndexEntry progress =
                        state.spec.encodeProgress(
                                sourceReader.tableBucket(),
                                entry.getKey().getBucket(),
                                lastProcessedOffset);
                encoded.put(
                        entry.getKey(),
                        new EncodedBatch(
                                entry.getValue().finish(progress.key(), progress.value()),
                                progress.key()));
            } catch (IOException | RuntimeException failure) {
                deferWindow(buildState, failure);
                if (buildState.sizingFailure != null) {
                    retrySmallerWindow(buildState, failure);
                }
                return false;
            }
        }

        // A window that derives no index mutations is trivially complete: advance immediately so
        // the pushed offset does not stall behind index-irrelevant WAL records.
        if (encoded.isEmpty()) {
            scheduler.onWindowPublished(this, state);
            advanceOnEmptyWindow(state, lastProcessedOffset);
            return true;
        }

        // Stage the window: create it first so each batch can reference it, then publish batches to
        // the sendBuffer. The per-index in-flight window is set before publishing to enforce one
        // outstanding window per index.
        IndexReplicationWindow window =
                new IndexReplicationWindow(
                        state.spec.getIndexName(), lastProcessedOffset, encoded.size(), this);
        List<IndexBatch> batches = new ArrayList<>(encoded.size());
        try {
            for (Map.Entry<TableBucket, EncodedBatch> entry : encoded.entrySet()) {
                EncodedBatch encodedBatch = entry.getValue();
                batches.add(
                        new IndexBatch(
                                entry.getKey(),
                                encodedBatch.progressKey,
                                encodedBatch.records,
                                builders.get(entry.getKey()).retainedBytes(),
                                window));
            }
            long largestFrame = 0;
            long retainedBytes = 0;
            for (IndexBatch batch : batches) {
                IndexSender.RequestSizeAccumulator size =
                        IndexSender.newRequestSizeAccumulator(
                                batch.targetBucket().getTableId(),
                                batch.sourceBucket().getTableId(),
                                IndexSender.DEFAULT_REQUEST_TIMEOUT_MS);
                size.addBucket(batch);
                largestFrame = Math.max(largestFrame, size.framedBytes());
                if (!size.codecRepresentable() || size.framedBytes() > maxTransportRequestBytes) {
                    throw new RecordTooLargeException(
                            "Index batch from "
                                    + sourceBucket()
                                    + " to "
                                    + batch.targetBucket()
                                    + " at source end "
                                    + lastProcessedOffset
                                    + " requires "
                                    + size.framedBytes()
                                    + " framed bytes, netty.server.max-request-size="
                                    + maxTransportRequestBytes
                                    + ", codecRepresentable="
                                    + size.codecRepresentable());
                }
                retainedBytes += batch.retainedBytes();
            }
            registerInFlightWindow(state.spec.getIndexName(), window);
            if (!sendBuffer.tryAppendWindow(batches, buildState.reservation)) {
                retireUnadmittedWindow(state, window);
                return false;
            }
            if (buildState.input.isByteLimited()
                    && readBudgetFor(state) == buildState.readBudget
                    && largestFrame <= maxTransportRequestBytes / 2
                    && retainedBytes <= sendBuffer.capacityBytes() / 2) {
                state.learnedReadBytes = (int) Math.min(maxWindowBytes, 2L * buildState.readBudget);
            }
        } catch (RecordTooLargeException failure) {
            retireUnadmittedWindow(state, window);
            retrySmallerWindow(buildState, failure);
            return false;
        } catch (RuntimeException | Error failure) {
            retireUnadmittedWindow(state, window);
            if (window.isAdmitted()) {
                scheduler.onWindowPublished(this, state);
            }
            throw failure;
        }
        scheduler.onWindowPublished(this, state);
        return true;
    }

    private void retrySmallerWindow(WindowBuildState build, Throwable failure) {
        IndexProgressState state = build.state;
        int smallerBytes = build.input.smallerReadBytes();
        scheduler.onFailure(this, state);
        if (smallerBytes > 0 && smallerBytes < readBudgetFor(state)) {
            // Shrink only our own infeasible output, never temporary competition for shared pages.
            state.learnedReadBytes = smallerBytes;
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

    int readBudgetFor(IndexProgressState state) {
        return Math.min(maxWindowBytes, state.learnedReadBytes);
    }

    private void retireUnadmittedWindow(IndexProgressState state, IndexReplicationWindow window) {
        if (window.isAdmitted()) {
            return;
        }
        if (state.inFlightWindow == window) {
            state.inFlightWindow = null;
        }
        window.tryRetire();
    }

    private void advanceOnEmptyWindow(IndexProgressState state, long windowEndOffset) {
        if (advanceIndexState(state, windowEndOffset)) {
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
            if (closed.get()) {
                return;
            }
            IndexProgressState state = indexStatesByName.get(window.indexName());
            boolean advanced = false;
            if (state == null) {
                if (indexStates.isEmpty() && window.windowEndOffset() > emptyIndexPushedOffset) {
                    emptyIndexPushedOffset = window.windowEndOffset();
                    sourceRetry.reset();
                    advanced = true;
                }
            } else {
                if (state.inFlightWindow != window) {
                    return;
                }
                advanced = advanceIndexState(state, window.windowEndOffset());
                state.inFlightWindow = null;
            }
            if (metrics != null) {
                metrics.indexReplicationCompletedBytes().inc(completedBytes);
            }
            notifyProgress(advanced ? state : null);
        } finally {
            lifecycleLock.unlock();
        }
        Runnable signal = this.wakeupSignal;
        if (signal != null) {
            signal.run();
        }
    }

    void registerInFlightWindow(String indexName, IndexReplicationWindow window) {
        IndexProgressState state = indexStatesByName.get(indexName);
        if (state != null) {
            state.inFlightWindow = window;
        }
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

    IndexReplicationRetryState retryState(String indexName) {
        IndexProgressState state = indexStatesByName.get(indexName);
        return state == null ? sourceRetry : state.retry;
    }

    boolean isFailed() {
        if (sourceRetry.hasFailure()) {
            return true;
        }
        for (IndexProgressState state : indexStates) {
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
        for (IndexProgressState state : indexStates) {
            IndexReplicationWindow window = state.inFlightWindow;
            if (window != null
                    && window.hasFailure()
                    && state.retry.shouldReleaseResources(RETAINED_WINDOW_FAILURE_TIMEOUT_NS)
                    && window.tryRetire()) {
                // Rebuild the admitted range, which may be shorter than a capacity retry.
                state.replayEndOffset = window.windowEndOffset();
                state.inFlightWindow = null;
                state.retry.onResourcesReleased();
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
        for (IndexProgressState state : indexStates) {
            IndexReplicationWindow window = state.inFlightWindow;
            state.inFlightWindow = null;
            if (window == null) {
                continue;
            }
            window.tryRetire();
        }
        sendBuffer.dropForSource(sourceBucket());
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
                    appendMutation(window, OldIndexEntry.EMPTY, first.getRow());
                }
                return;
            case DELETE:
                for (WindowBuildState window : windows) {
                    if (!window.deferred) {
                        try {
                            appendMutation(
                                    window,
                                    OldIndexEntry.from(window.state.spec, first.getRow()),
                                    null);
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
        List<WindowBuildState> activeWindows = new ArrayList<>(windows.size());
        List<OldIndexEntry> oldEntries = new ArrayList<>(windows.size());
        for (WindowBuildState window : windows) {
            if (!window.deferred) {
                try {
                    OldIndexEntry oldEntry = OldIndexEntry.from(window.state.spec, first.getRow());
                    activeWindows.add(window);
                    oldEntries.add(oldEntry);
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
        for (int i = 0; i < activeWindows.size(); i++) {
            WindowBuildState window = activeWindows.get(i);
            appendMutation(window, oldEntries.get(i), after.getRow());
        }
    }

    private IndexSourceWalCorruptionException corruption(String message) {
        IndexSourceWalCorruptionException failure =
                new IndexSourceWalCorruptionException(
                        "Corrupt source WAL for " + sourceReader.tableBucket() + ": " + message);
        return failure;
    }

    private IndexSourceWalCorruptionException corruption(String message, Throwable cause) {
        IndexSourceWalCorruptionException failure =
                new IndexSourceWalCorruptionException(
                        "Corrupt source WAL for " + sourceReader.tableBucket() + ": " + message,
                        cause);
        return failure;
    }

    private void appendMutation(
            WindowBuildState window, OldIndexEntry oldEntry, @Nullable InternalRow newRow) {
        if (window.deferred) {
            return;
        }
        try {
            IndexSpec spec = window.state.spec;
            boolean oldHasIdx = oldEntry.hasIndexColumns;
            boolean newHasIdx = newRow != null && spec.hasIndexColumns(newRow);
            IndexSpec.IndexEntry newEntry = newHasIdx ? spec.encodeEntry(newRow) : null;
            boolean keysDiffer =
                    oldEntry.key != null
                            && (newEntry == null || !Arrays.equals(oldEntry.key, newEntry.key()));
            if (oldHasIdx && keysDiffer) {
                bucketBuilder(
                                window,
                                new TableBucket(spec.getIndexTableId(), oldEntry.targetBucket))
                        .appendDelete(oldEntry.key);
            }
            if (newEntry != null && (!oldHasIdx || keysDiffer)) {
                bucketBuilder(
                                window,
                                new TableBucket(spec.getIndexTableId(), newEntry.targetBucket()))
                        .appendUpsert(newEntry.key(), newEntry.value());
            }
        } catch (RuntimeException failure) {
            deferWindow(window, failure);
        }
    }

    private void deferWindow(WindowBuildState window, Throwable failure) {
        window.deferred = true;
        if (failure instanceof RecordTooLargeException) {
            window.sizingFailure = failure;
        } else if (failure instanceof IndexSendBuffer.BufferFullException && window.optionalAsync) {
            // This input is a reuse opportunity. Ordinary ASYNC scheduling still owns its retry;
            // optional work must neither claim capacity protection nor roll back published SYNC.
        } else if (failure instanceof IndexSendBuffer.BufferFullException) {
            long replayEnd = window.state.replayEndOffset;
            window.state.replayEndOffset =
                    replayEnd < 0
                            ? window.input.nextOffset()
                            : Math.min(replayEnd, window.input.nextOffset());
            scheduler.onCapacityBlocked(this, window.state);
        } else {
            scheduler.onFailure(this, window.state);
        }
        window.builders.clear();
        window.reservation.close();
        if (window.sizingFailure == null
                && !(window.optionalAsync
                        && failure instanceof IndexSendBuffer.BufferFullException)) {
            backoff(failure, window.state.retry);
        }
    }

    private BucketBatchBuilder bucketBuilder(WindowBuildState window, TableBucket targetBucket) {
        BucketBatchBuilder builder = window.builders.get(targetBucket);
        if (builder == null) {
            IndexSpec spec = window.state.spec;
            builder =
                    new BucketBatchBuilder(
                            (short) spec.getIndexSchemaId(),
                            spec.getIndexKvFormat(),
                            window.reservation);
            window.builders.put(targetBucket, builder);
        }
        return builder;
    }

    long getSyncIndexPushedOffset() {
        long min = Long.MAX_VALUE;
        boolean hasSyncIndex = false;
        for (IndexProgressState state : indexStates) {
            if (state.spec.isSync()) {
                hasSyncIndex = true;
                min = Math.min(min, state.pushedOffset);
            }
        }
        if (hasSyncIndex) {
            return min;
        }
        return indexStates.isEmpty() ? emptyIndexPushedOffset : Long.MAX_VALUE;
    }

    long getAllIndexPushedOffset() {
        if (indexStates.isEmpty()) {
            return emptyIndexPushedOffset;
        }
        long min = Long.MAX_VALUE;
        for (IndexProgressState state : indexStates) {
            min = Math.min(min, state.pushedOffset);
        }
        return min;
    }

    void onHighWatermarkAdvanced() {
        if (indexStates.isEmpty() || closed.get()) {
            return;
        }
        noProgressTracker.update(getAllIndexPushedOffset(), sourceReader.highWatermark());
    }

    long noProgressTimeMs() {
        if (indexStates.isEmpty() || closed.get()) {
            return 0L;
        }
        return noProgressTracker.noProgressTimeMs(
                getAllIndexPushedOffset(), sourceReader.highWatermark());
    }

    private boolean advanceIndexState(IndexProgressState state, long newOffset) {
        if (newOffset > state.pushedOffset) {
            state.pushedOffset = newOffset;
            state.retry.reset();
            if (newOffset >= state.replayEndOffset) {
                state.replayEndOffset = -1L;
            }
            return true;
        }
        return false;
    }

    private void notifyProgress(@Nullable IndexProgressState advancedState) {
        if (getAllIndexPushedOffset() > sourceFailureOffset) {
            sourceRetry.reset();
        }
        if (!isFailed()) {
            lastFailure = null;
        }
        if (!indexStates.isEmpty()) {
            noProgressTracker.update(getAllIndexPushedOffset(), sourceReader.highWatermark());
        }
        if (advancedState != null) {
            scheduler.onProgress(this, advancedState);
        }
        onProgress.onProgress(getSyncIndexPushedOffset(), getAllIndexPushedOffset());
    }

    @Override
    public void close() {
        lifecycleLock.lock();
        try {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
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
        if (scheduler != null) {
            failure = runCleanupStep(failure, () -> scheduler.onFailure(this, null));
        }
        failure = runCleanupStep(failure, this::retirePendingReadLocked);
        failure = runCleanupStep(failure, this::retireOwnedBatchesLocked);
        failure = runCleanupStep(failure, sourceReader::close);
        failure = runCleanupStep(failure, this::closeReadContext);
        return runCleanupStep(
                failure,
                () -> {
                    if (scheduler != null) {
                        scheduler.onReadClosed(this);
                    }
                });
    }

    @Nullable
    private static Throwable runCleanupStep(@Nullable Throwable previousFailure, Runnable step) {
        try {
            step.run();
        } catch (Throwable failure) {
            if (previousFailure == null) {
                return failure;
            }
            if (previousFailure != failure) {
                previousFailure.addSuppressed(failure);
            }
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
        return closed.get();
    }

    /** The source main-table bucket this replicator reads from. */
    TableBucket sourceBucket() {
        return sourceReader.tableBucket();
    }

    List<IndexProgressState> indexStates() {
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

    boolean hasReadyRead(IndexProgressState index, long startOffset) {
        PendingRead pending = pendingRead;
        return !closed.get()
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

    boolean pollServiced() {
        return pollServiced;
    }

    private void closeReadContext() {
        if (readContext != null && readContextClosed.compareAndSet(false, true)) {
            readContext.close();
        }
    }

    static final class IndexProgressState {
        private final IndexSpec spec;
        private volatile long pushedOffset;
        private final IndexReplicationRetryState retry;
        private volatile long replayEndOffset = -1L;
        private int learnedReadBytes = Integer.MAX_VALUE;
        @Nullable private volatile IndexReplicationWindow inFlightWindow;

        private IndexProgressState(
                IndexSpec spec, long initialOffset, ExponentialBackoff backoff, Clock clock) {
            this.spec = spec;
            this.pushedOffset = initialOffset;
            this.retry = new IndexReplicationRetryState(backoff, clock);
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
            long replayEnd = replayEndOffset;
            return inFlightWindow == null
                    && retry.remainingDelayNanos() == 0
                    && offset < highWatermark
                    && (replayEnd < 0 || replayEnd > offset);
        }

        long replayEndOffset() {
            return replayEndOffset;
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
        private final List<IndexProgressState> states;

        ReadGroup(long startOffset, long endOffset, List<IndexProgressState> states) {
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

        List<IndexProgressState> states() {
            return states;
        }
    }

    private static final class PendingRead {
        private final ReadGroup group;
        private final CompletableFuture<IndexSourceReader.ReadResult> future;
        private final int readBudget;

        private PendingRead(
                ReadGroup group,
                CompletableFuture<IndexSourceReader.ReadResult> future,
                int readBudget) {
            this.group = group;
            this.future = future;
            this.readBudget = readBudget;
        }
    }

    private static final class WindowBuildState {
        private final IndexProgressState state;
        private final Map<TableBucket, BucketBatchBuilder> builders = new HashMap<>();
        private long lastProcessedOffset;
        private final IndexSendBuffer.Reservation reservation;
        private final IndexSourceReader.ReadResult input;
        private final int readBudget;
        private final boolean optionalAsync;
        private boolean deferred;
        @Nullable private Throwable sizingFailure;

        private WindowBuildState(
                IndexProgressState state,
                IndexSendBuffer.Reservation reservation,
                IndexSourceReader.ReadResult input,
                int readBudget,
                boolean optionalAsync) {
            this.state = state;
            this.reservation = reservation;
            this.lastProcessedOffset = state.pushedOffset;
            this.input = input;
            this.readBudget = readBudget;
            this.optionalAsync = optionalAsync;
        }
    }

    private static final class OldIndexEntry {
        private static final OldIndexEntry EMPTY = new OldIndexEntry(false, null, -1);

        private final boolean hasIndexColumns;
        @Nullable private final byte[] key;
        private final int targetBucket;

        private OldIndexEntry(boolean hasIndexColumns, @Nullable byte[] key, int targetBucket) {
            this.hasIndexColumns = hasIndexColumns;
            this.key = key;
            this.targetBucket = targetBucket;
        }

        private static OldIndexEntry from(IndexSpec spec, InternalRow row) {
            if (!spec.hasIndexColumns(row)) {
                return new OldIndexEntry(false, null, -1);
            }
            IndexSpec.IndexEntry entry = spec.encodeEntry(row);
            return new OldIndexEntry(true, entry.key(), entry.targetBucket());
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

    private static final class EncodedBatch {
        private final BytesView records;
        private final byte[] progressKey;

        private EncodedBatch(BytesView records, byte[] progressKey) {
            this.records = records;
            this.progressKey = progressKey;
        }
    }
}

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WAL-driven index replicator that reads committed WAL entries, derives index mutations, and stages
 * them as pre-encoded {@link IndexBatch}es in the server-global {@link IndexSendBuffer}. Derived
 * mutations are appended directly to ordinary per-target-bucket {@link KvRecordBatchBuilder}s.
 *
 * <p>Each secondary index has its own pushed offset and at most one {@link IndexReplicationWindow}
 * in flight. {@link #poll()} reads the next valid window for every index that is currently ready.
 * Window ends may differ after failover because they depend on the fetched input and derived output
 * size. The source advances only completed windows and replays from persisted progress after
 * recovery; the target rejects requests behind its application-level progress record.
 *
 * <p>Driven by {@link #poll()} calls from an {@code IndexReplicatorPool} read worker.
 */
@Internal
public final class IndexReplicator implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IndexReplicator.class);

    private static final int PAGE_SIZE = 4096;
    // A failed window's soft retention target follows the maximum retry interval.
    private static final long RETAINED_WINDOW_FAILURE_TIMEOUT_NS =
            TimeUnit.MILLISECONDS.toNanos(IndexReplicatorPool.MAX_RETRY_BACKOFF_MS);

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

    /** Progress used only by empty test owners; production replicators always have index states. */
    private volatile long emptyIndexPushedOffset;

    private final AtomicBoolean closed;
    private final AtomicBoolean readContextClosed;
    private final IndexReplicationRetryState sourceRetry;
    @Nullable private volatile Throwable lastFailure;
    private long sourceFailureOffset;
    private final ReentrantLock lifecycleLock;

    @Nullable private volatile CompletableFuture<IndexSourceReader.ReadResult> pendingRead;
    @Nullable private volatile List<IndexProgressState> pendingReadStates;
    private volatile long pendingReadOffset;

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
                new ExponentialBackoff(100, 2, IndexReplicatorPool.MAX_RETRY_BACKOFF_MS, 0.2),
                SystemClock.getInstance(),
                null);
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
            @Nullable TabletServerMetricGroup metrics) {
        if (initialOffset < 0) {
            throw new IllegalArgumentException(
                    "initialOffset must be non-negative, but was " + initialOffset);
        }
        this.sourceReader = sourceReader;
        this.sourceRetry = new IndexReplicationRetryState(retryBackoff, clock);
        this.indexStates = new ArrayList<>(indexSpecs.size());
        this.indexStatesByName = new LinkedHashMap<>();
        for (IndexSpec indexSpec : indexSpecs) {
            IndexProgressState state =
                    new IndexProgressState(indexSpec, initialOffset, retryBackoff, clock);
            if (indexStatesByName.put(indexSpec.getIndexName(), state) != null) {
                throw new IllegalArgumentException(
                        "Duplicate secondary index name: " + indexSpec.getIndexName());
            }
            indexStates.add(state);
        }
        this.sendBuffer = sendBuffer;
        this.readContext = readContext;
        this.emptyIndexPushedOffset = initialOffset;
        this.maxWindowBytes = maxWindowBytes;
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

    /**
     * Poll the WAL for one window. Strict single-window serialization: if a window is already in
     * flight, returns immediately without reading. Otherwise reads one valid window starting at
     * each index's pushed offset, derives index batches, and stages them in the {@link
     * IndexSendBuffer}. Each index's pushed offset is advanced only when every batch of that
     * index's window is acknowledged, preserving at-least-once delivery.
     *
     * @return {@code true} if a window was read (work was done), {@code false} otherwise
     */
    public boolean poll() {
        lifecycleLock.lock();
        try {
            try {
                return pollLocked();
            } catch (RuntimeException failure) {
                Throwable cause =
                        failure instanceof CompletionException && failure.getCause() != null
                                ? failure.getCause()
                                : failure;
                sourceFailureOffset = Math.max(sourceFailureOffset, pendingReadOffset);
                backoff(cause, sourceRetry);
                return false;
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private boolean pollLocked() {
        if (closed.get()) {
            return false;
        }
        // Yield before either the retry gate or the global-full gate can hide a stalled window.
        if (releaseStalledWindows() || sourceRetry.remainingDelayNanos() > 0) {
            return false;
        }

        // Total retained capacity is a hard bound; the per-producer threshold remains soft and
        // prevents one stalled source from continuing to derive windows.
        if (sendBuffer.isFull() || sendBuffer.isFull(sourceBucket())) {
            return false;
        }

        if (pendingRead != null) {
            return consumePendingRead();
        }

        long hw = sourceReader.highWatermark();
        Map<Long, List<IndexProgressState>> readyStatesByOffset = new LinkedHashMap<>();
        for (IndexProgressState state : indexStates) {
            if (state.inFlightWindow != null || state.retry.remainingDelayNanos() > 0) {
                continue;
            }
            long readOffset = nextReadOffset(state);
            if (readOffset >= hw) {
                continue;
            }
            readyStatesByOffset
                    .computeIfAbsent(readOffset, ignored -> new ArrayList<>())
                    .add(state);
        }

        boolean polled = false;
        for (Map.Entry<Long, List<IndexProgressState>> entry : readyStatesByOffset.entrySet()) {
            if (closed.get() || sendBuffer.isFull() || sendBuffer.isFull(sourceBucket())) {
                break;
            }
            polled |= pollOneWindow(entry.getValue(), entry.getKey(), hw);
            if (pendingRead != null) {
                break;
            }
        }
        return polled;
    }

    /**
     * Read and process a single window of WAL records. Returns {@code true} if a window was read.
     */
    private boolean pollOneWindow(
            List<IndexProgressState> states, long readOffset, long highWatermark) {
        pendingReadOffset = readOffset;
        for (IndexProgressState state : states) {
            if (state.replayEndOffset >= 0) {
                highWatermark = Math.min(highWatermark, state.replayEndOffset);
            }
        }
        CompletableFuture<IndexSourceReader.ReadResult> future =
                sourceReader.read(readOffset, highWatermark, maxWindowBytes);
        pendingRead = future;
        pendingReadStates = states;
        future.whenComplete((ignored, failure) -> wakeup());
        return future.isDone() && consumePendingRead();
    }

    private boolean consumePendingRead() {
        CompletableFuture<IndexSourceReader.ReadResult> future = pendingRead;
        List<IndexProgressState> states = pendingReadStates;
        if (future == null || states == null || !future.isDone()) {
            return false;
        }

        IndexSourceReader.ReadResult result;
        try {
            result = future.getNow(null);
        } finally {
            clearPendingRead(future);
        }
        if (result == null) {
            return false;
        }
        try (IndexSourceReader.ReadResult ownedResult = result) {
            return processReadResult(states, pendingReadOffset, ownedResult);
        }
    }

    private void clearPendingRead(CompletableFuture<IndexSourceReader.ReadResult> future) {
        if (pendingRead == future) {
            pendingRead = null;
            pendingReadStates = null;
        }
    }

    private void wakeup() {
        Runnable signal = this.wakeupSignal;
        if (signal != null) {
            signal.run();
        }
    }

    private boolean processReadResult(
            List<IndexProgressState> states,
            long readOffset,
            IndexSourceReader.ReadResult readResult) {
        List<WindowBuildState> windows = new ArrayList<>(states.size());
        for (IndexProgressState state : states) {
            windows.add(new WindowBuildState(state, sendBuffer.newReservation()));
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
                return false;
            }
        }

        // A window that derives no index mutations is trivially complete: advance immediately so
        // the pushed offset does not stall behind index-irrelevant WAL records.
        if (encoded.isEmpty()) {
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
        registerInFlightWindow(state.spec.getIndexName(), window);
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
            if (!sendBuffer.tryAppendWindow(batches, buildState.reservation)) {
                retireUnadmittedWindow(state, window);
                return false;
            }
        } catch (RecordTooLargeException failure) {
            retireUnadmittedWindow(state, window);
            backoff(failure, state.retry);
            return false;
        } catch (RuntimeException | Error failure) {
            retireUnadmittedWindow(state, window);
            throw failure;
        }
        return true;
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
            notifyProgress();
        }
    }

    private long nextReadOffset(IndexProgressState state) {
        return state.pushedOffset;
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
            if (state == null) {
                if (indexStates.isEmpty() && window.windowEndOffset() > emptyIndexPushedOffset) {
                    emptyIndexPushedOffset = window.windowEndOffset();
                    sourceRetry.reset();
                }
            } else {
                if (state.inFlightWindow != window) {
                    return;
                }
                advanceIndexState(state, window.windowEndOffset());
                state.inFlightWindow = null;
            }
            if (metrics != null) {
                metrics.indexReplicationCompletedBytes().inc(completedBytes);
            }
            notifyProgress();
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
                state.replayEndOffset = Math.max(state.replayEndOffset, window.windowEndOffset());
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
        window.builders.clear();
        window.reservation.close();
        backoff(failure, window.state.retry);
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

    private void notifyProgress() {
        if (getAllIndexPushedOffset() > sourceFailureOffset) {
            sourceRetry.reset();
        }
        if (!isFailed()) {
            lastFailure = null;
        }
        if (!indexStates.isEmpty()) {
            noProgressTracker.update(getAllIndexPushedOffset(), sourceReader.highWatermark());
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
        failure = runCleanupStep(failure, this::retirePendingReadLocked);
        failure = runCleanupStep(failure, this::retireOwnedBatchesLocked);
        failure = runCleanupStep(failure, sourceReader::close);
        return runCleanupStep(failure, this::closeReadContext);
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
        CompletableFuture<IndexSourceReader.ReadResult> future = pendingRead;
        pendingRead = null;
        pendingReadStates = null;
        if (future == null) {
            return;
        }
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

    private void closeReadContext() {
        if (readContext != null && readContextClosed.compareAndSet(false, true)) {
            readContext.close();
        }
    }

    private static final class IndexProgressState {
        private final IndexSpec spec;
        private volatile long pushedOffset;
        private final IndexReplicationRetryState retry;
        private long replayEndOffset = -1L;
        @Nullable private volatile IndexReplicationWindow inFlightWindow;

        private IndexProgressState(
                IndexSpec spec, long initialOffset, ExponentialBackoff backoff, Clock clock) {
            this.spec = spec;
            this.pushedOffset = initialOffset;
            this.retry = new IndexReplicationRetryState(backoff, clock);
        }
    }

    private static final class WindowBuildState {
        private final IndexProgressState state;
        private final Map<TableBucket, BucketBatchBuilder> builders = new HashMap<>();
        private long lastProcessedOffset;
        private final IndexSendBuffer.Reservation reservation;
        private boolean deferred;

        private WindowBuildState(
                IndexProgressState state, IndexSendBuffer.Reservation reservation) {
            this.state = state;
            this.reservation = reservation;
            this.lastProcessedOffset = state.pushedOffset;
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

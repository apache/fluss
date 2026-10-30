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
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultKvRecord;
import org.apache.fluss.record.DefaultKvRecordBatch;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

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

    private static final BiConsumer<IndexReplicator, Throwable> NO_TERMINAL_CALLBACK =
            (ignoredReplicator, ignoredFailure) -> {};

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
    private final BiConsumer<IndexReplicator, Throwable> onTerminalFailure;
    @Nullable private final TabletServerMetricGroup metrics;
    private final IndexReplicationNoProgressTracker noProgressTracker;
    private final int maxWindowBytes;
    private final long preferredMaxRequestBytes;

    /** Progress used only by empty test owners; production replicators always have index states. */
    private volatile long emptyIndexPushedOffset;

    private final AtomicBoolean closed;
    private final AtomicBoolean readContextClosed;
    private final AtomicReference<Throwable> terminalFailure;
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
            long preferredMaxRequestBytes,
            IndexProgressListener onProgress) {
        this(
                sourceReader,
                indexSpecs,
                sendBuffer,
                readContext,
                initialOffset,
                maxWindowBytes,
                preferredMaxRequestBytes,
                onProgress,
                NO_TERMINAL_CALLBACK);
    }

    IndexReplicator(
            IndexSourceReader sourceReader,
            List<IndexSpec> indexSpecs,
            IndexSendBuffer sendBuffer,
            LogRecordReadContext readContext,
            long initialOffset,
            int maxWindowBytes,
            long preferredMaxRequestBytes,
            IndexProgressListener onProgress,
            BiConsumer<IndexReplicator, Throwable> onTerminalFailure) {
        this(
                sourceReader,
                indexSpecs,
                sendBuffer,
                readContext,
                initialOffset,
                maxWindowBytes,
                preferredMaxRequestBytes,
                onProgress,
                onTerminalFailure,
                null);
    }

    IndexReplicator(
            IndexSourceReader sourceReader,
            List<IndexSpec> indexSpecs,
            IndexSendBuffer sendBuffer,
            LogRecordReadContext readContext,
            long initialOffset,
            int maxWindowBytes,
            long preferredMaxRequestBytes,
            IndexProgressListener onProgress,
            BiConsumer<IndexReplicator, Throwable> onTerminalFailure,
            @Nullable TabletServerMetricGroup metrics) {
        if (initialOffset < 0) {
            throw new IllegalArgumentException(
                    "initialOffset must be non-negative, but was " + initialOffset);
        }
        this.sourceReader = sourceReader;
        this.indexStates = new ArrayList<>(indexSpecs.size());
        this.indexStatesByName = new LinkedHashMap<>();
        for (IndexSpec indexSpec : indexSpecs) {
            IndexProgressState state = new IndexProgressState(indexSpec, initialOffset);
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
        this.preferredMaxRequestBytes = preferredMaxRequestBytes;
        this.closed = new AtomicBoolean(false);
        this.readContextClosed = new AtomicBoolean(false);
        this.terminalFailure = new AtomicReference<>();
        this.lifecycleLock = new ReentrantLock();
        this.onProgress = onProgress;
        this.onTerminalFailure = onTerminalFailure;
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
            } catch (IndexSourceWalCorruptionException failure) {
                transitionToTerminalLocked(failure);
                throw failure;
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private boolean pollLocked() {
        if (closed.get() || terminalFailure.get() != null) {
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
            if (state.inFlightWindow != null) {
                continue;
            }
            // Failure is published before its matching in-flight reference is cleared. Recheck
            // after observing an empty slot so this poll cannot start a successor window.
            if (terminalFailure.get() != null) {
                break;
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
            if (closed.get()
                    || terminalFailure.get() != null
                    || sendBuffer.isFull()
                    || sendBuffer.isFull(sourceBucket())) {
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
        try {
            CompletableFuture<IndexSourceReader.ReadResult> future =
                    sourceReader.read(readOffset, highWatermark, maxWindowBytes);
            pendingRead = future;
            pendingReadStates = states;
            pendingReadOffset = readOffset;
            future.whenComplete((ignored, failure) -> wakeup());
            if (!future.isDone()) {
                return false;
            }
            return consumePendingRead();
        } catch (IndexSourceWalCorruptionException failure) {
            transitionToTerminalLocked(failure);
            throw failure;
        } catch (RuntimeException failure) {
            LOG.warn("Failed to start WAL read at offset {}: {}", readOffset, failure.getMessage());
            return false;
        }
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
        } catch (CompletionException failure) {
            clearPendingRead(future);
            Throwable cause = failure.getCause() == null ? failure : failure.getCause();
            if (cause instanceof IndexSourceWalCorruptionException) {
                transitionToTerminalLocked(cause);
                throw (IndexSourceWalCorruptionException) cause;
            }
            LOG.warn("Failed to read WAL at offset {}: {}", pendingReadOffset, cause.getMessage());
            return false;
        }
        clearPendingRead(future);
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
            windows.add(new WindowBuildState(state));
        }

        for (IndexSourceReader.SourceBatchSlice batch : readResult.batches()) {
            if (allWindowsFull(windows)) {
                break;
            }
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
                if (!window.full) {
                    window.lastProcessedOffset =
                            Math.max(window.lastProcessedOffset, batch.nextOffset());
                    if (window.currentEncodedSize >= preferredMaxRequestBytes) {
                        window.full = true;
                    }
                }
            }
        }

        boolean processed = false;
        for (WindowBuildState window : windows) {
            if (!window.full) {
                window.lastProcessedOffset =
                        Math.max(window.lastProcessedOffset, readResult.nextOffset());
            }
            processed |= publishWindow(window);
            if (terminalFailure.get() != null) {
                break;
            }
        }
        return processed;
    }

    private boolean publishWindow(WindowBuildState buildState) {
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
            } catch (IOException e) {
                LOG.error(
                        "Failed to encode index batch for {} at window ending {}; abandoning the "
                                + "window and retrying without advancing the pushed offset",
                        entry.getKey(),
                        lastProcessedOffset,
                        e);
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
                                sourceReader.tableBucket(),
                                lastProcessedOffset,
                                encodedBatch.progressKey,
                                encodedBatch.records,
                                builders.get(entry.getKey()).retainedBytes(),
                                window));
            }
            if (!sendBuffer.tryAppendWindow(batches)) {
                retireUnadmittedWindow(state, window);
                return false;
            }
        } catch (RecordTooLargeException failure) {
            retireUnadmittedWindow(state, window);
            transitionToTerminalLocked(failure);
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
    void onWindowComplete(String indexName, long windowEndOffset, long completedBytes) {
        lifecycleLock.lock();
        try {
            if (closed.get() || terminalFailure.get() != null) {
                return;
            }
            IndexProgressState state = indexStatesByName.get(indexName);
            if (state == null) {
                if (indexStates.isEmpty() && windowEndOffset > emptyIndexPushedOffset) {
                    emptyIndexPushedOffset = windowEndOffset;
                }
            } else {
                advanceIndexState(state, windowEndOffset);
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

    void onWindowFailed(String indexName, IndexReplicationWindow window, Throwable failure) {
        lifecycleLock.lock();
        try {
            IndexProgressState state = indexStatesByName.get(indexName);
            if (state != null && state.inFlightWindow == window) {
                state.inFlightWindow = null;
            }
            transitionToTerminalLocked(failure);
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void transitionToTerminalLocked(Throwable failure) {
        if (!terminalFailure.compareAndSet(null, failure)) {
            return;
        }
        Throwable cleanupFailure = cleanupOwnedResourcesLocked();
        if (cleanupFailure != null && cleanupFailure != failure) {
            failure.addSuppressed(cleanupFailure);
        }
        LOG.error(
                "Index replication for source bucket {} failed terminally at pushed offset {}",
                sourceReader.tableBucket(),
                getAllIndexPushedOffset(),
                failure);
        try {
            onTerminalFailure.accept(this, failure);
        } catch (Throwable callbackFailure) {
            if (callbackFailure != failure) {
                failure.addSuppressed(callbackFailure);
            }
            LOG.warn(
                    "Failed to report terminal index replication state for {}",
                    sourceReader.tableBucket(),
                    callbackFailure);
        }
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

    private static boolean allWindowsFull(List<WindowBuildState> windows) {
        for (WindowBuildState window : windows) {
            if (!window.full) {
                return false;
            }
        }
        return true;
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
                    if (!window.full) {
                        appendMutation(window, OldIndexEntry.EMPTY, first.getRow());
                    }
                }
                return;
            case DELETE:
                for (WindowBuildState window : windows) {
                    if (!window.full) {
                        IndexSpec spec = window.state.spec;
                        appendMutation(window, OldIndexEntry.from(spec, first.getRow()), null);
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
            if (!window.full) {
                IndexSpec spec = window.state.spec;
                activeWindows.add(window);
                oldEntries.add(OldIndexEntry.from(spec, first.getRow()));
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

    private static long saturatedAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private void appendMutation(
            WindowBuildState window, OldIndexEntry oldEntry, @Nullable InternalRow newRow) {
        IndexSpec spec = window.state.spec;
        boolean oldHasIdx = oldEntry.hasIndexColumns;
        boolean newHasIdx = newRow != null && spec.hasIndexColumns(newRow);

        IndexSpec.IndexEntry newEntry = newHasIdx ? spec.encodeEntry(newRow) : null;
        boolean keysDiffer =
                oldEntry.key != null
                        && (newEntry == null || !Arrays.equals(oldEntry.key, newEntry.key()));

        if (oldHasIdx && keysDiffer) {
            appendDelete(
                    window,
                    new TableBucket(spec.getIndexTableId(), oldEntry.targetBucket),
                    oldEntry.key);
        }
        if (newEntry != null && (!oldHasIdx || keysDiffer)) {
            appendUpsert(
                    window,
                    new TableBucket(spec.getIndexTableId(), newEntry.targetBucket()),
                    newEntry.key(),
                    newEntry.value());
        }
    }

    private void appendDelete(WindowBuildState window, TableBucket targetBucket, byte[] key) {
        int recordSize = DefaultKvRecord.sizeOf(key, null);
        bucketBuilder(window, targetBucket).appendDelete(key);
        window.currentEncodedSize = saturatedAdd(window.currentEncodedSize, recordSize);
    }

    private void appendUpsert(
            WindowBuildState window, TableBucket targetBucket, byte[] key, BinaryRow value) {
        int recordSize = DefaultKvRecord.sizeOf(key, value);
        bucketBuilder(window, targetBucket).appendUpsert(key, value);
        window.currentEncodedSize = saturatedAdd(window.currentEncodedSize, recordSize);
    }

    private BucketBatchBuilder bucketBuilder(WindowBuildState window, TableBucket targetBucket) {
        BucketBatchBuilder builder = window.builders.get(targetBucket);
        if (builder == null) {
            IndexSpec spec = window.state.spec;
            builder =
                    new BucketBatchBuilder(
                            (short) spec.getIndexSchemaId(), spec.getIndexKvFormat());
            window.builders.put(targetBucket, builder);
            window.currentEncodedSize =
                    saturatedAdd(
                            window.currentEncodedSize,
                            DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE);
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
        if (indexStates.isEmpty() || closed.get() || terminalFailure.get() != null) {
            return;
        }
        noProgressTracker.update(getAllIndexPushedOffset(), sourceReader.highWatermark());
    }

    long noProgressTimeMs() {
        if (indexStates.isEmpty() || closed.get() || terminalFailure.get() != null) {
            return 0L;
        }
        return noProgressTracker.noProgressTimeMs(
                getAllIndexPushedOffset(), sourceReader.highWatermark());
    }

    private boolean advanceIndexState(IndexProgressState state, long newOffset) {
        if (newOffset > state.pushedOffset) {
            state.pushedOffset = newOffset;
            return true;
        }
        return false;
    }

    private void notifyProgress() {
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
        @Nullable private volatile IndexReplicationWindow inFlightWindow;

        private IndexProgressState(IndexSpec spec, long initialOffset) {
            this.spec = spec;
            this.pushedOffset = initialOffset;
        }
    }

    private static final class WindowBuildState {
        private final IndexProgressState state;
        private final Map<TableBucket, BucketBatchBuilder> builders = new HashMap<>();
        private long lastProcessedOffset;
        private long currentEncodedSize;
        private boolean full;

        private WindowBuildState(IndexProgressState state) {
            this.state = state;
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
        private final UnmanagedPagedOutputView output;
        final KvRecordBatchBuilder builder;

        BucketBatchBuilder(short schemaId, KvFormat kvFormat) {
            this.output = new UnmanagedPagedOutputView(PAGE_SIZE);
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

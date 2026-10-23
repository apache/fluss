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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.server.kv.RemoteLogFetcher;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Reads a continuous, high-watermark-bounded source WAL range for index replication. */
@Internal
public final class IndexSourceReader implements AutoCloseable {

    interface SourceLog {
        TableBucket tableBucket();

        long highWatermark();

        long logStartOffset();

        default long localLogStartOffset() {
            return logStartOffset();
        }

        FetchDataInfo read(
                long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage)
                throws IOException;
    }

    @FunctionalInterface
    interface RemoteFetcherFactory {
        RemoteFetcher open();
    }

    interface RemoteFetcher extends AutoCloseable {
        Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset) throws Exception;

        default RemoteRead fetchBounded(long startOffset, long localLogStartOffset)
                throws Exception {
            Iterable<LogRecordBatch> batches = fetch(startOffset, localLogStartOffset);
            return new RemoteRead() {
                @Override
                public java.util.Iterator<LogRecordBatch> iterator() {
                    return batches.iterator();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        void close();
    }

    interface RemoteRead extends Iterable<LogRecordBatch>, AutoCloseable {
        @Override
        void close();
    }

    private final SourceLog sourceLog;
    @Nullable private final RemoteFetcherFactory remoteFetcherFactory;
    private final Executor remoteExecutor;
    private final LogRecordReadContext readContext;
    @Nullable private final TabletServerMetricGroup metrics;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<CompletableFuture<ReadResult>> pendingRemoteRead =
            new AtomicReference<>();
    private final AtomicReference<RemoteFetcher> remoteFetcherSession = new AtomicReference<>();

    public IndexSourceReader(
            LogTablet logTablet,
            RemoteLogManager remoteLogManager,
            Executor remoteExecutor,
            LogRecordReadContext readContext,
            @Nullable TabletServerMetricGroup metrics) {
        this(
                new LogTabletSourceLog(logTablet),
                () ->
                        new RemoteFetcherAdapter(
                                new RemoteLogFetcher(
                                        remoteLogManager,
                                        logTablet.getTableBucket(),
                                        logTablet.getLogDir(),
                                        1,
                                        1)),
                remoteExecutor,
                readContext,
                metrics);
    }

    IndexSourceReader(
            SourceLog sourceLog,
            @Nullable RemoteFetcherFactory remoteFetcherFactory,
            Executor remoteExecutor,
            LogRecordReadContext readContext) {
        this(sourceLog, remoteFetcherFactory, remoteExecutor, readContext, null);
    }

    IndexSourceReader(
            SourceLog sourceLog,
            @Nullable RemoteFetcherFactory remoteFetcherFactory,
            Executor remoteExecutor,
            LogRecordReadContext readContext,
            @Nullable TabletServerMetricGroup metrics) {
        this.sourceLog = sourceLog;
        this.remoteFetcherFactory = remoteFetcherFactory;
        this.remoteExecutor = remoteExecutor;
        this.readContext = readContext;
        this.metrics = metrics;
    }

    /**
     * Returns a bounded continuous source range, fetching remote raw WAL asynchronously if needed.
     */
    public CompletableFuture<ReadResult> read(long nextOffset, long highWatermark, int maxBytes) {
        if (closed.get()) {
            throw new IllegalStateException("Index source reader is closed");
        }
        if (nextOffset < 0 || highWatermark < nextOffset || maxBytes <= 0) {
            throw new IllegalArgumentException(
                    "Invalid index source read range ["
                            + nextOffset
                            + ", "
                            + highWatermark
                            + ") with maxBytes="
                            + maxBytes);
        }
        if (nextOffset >= highWatermark) {
            return CompletableFuture.completedFuture(ReadResult.empty(nextOffset));
        }

        long localLogStartOffset = sourceLog.localLogStartOffset();
        if (nextOffset >= localLogStartOffset) {
            try {
                return CompletableFuture.completedFuture(
                        readLocal(nextOffset, highWatermark, maxBytes, null));
            } catch (Throwable failure) {
                return failedFuture(failure);
            }
        }

        if (remoteFetcherFactory == null) {
            return failedFuture(
                    corruption(
                            "offset "
                                    + nextOffset
                                    + " is below local log start "
                                    + localLogStartOffset
                                    + " but no raw remote WAL reader is available"));
        }

        CompletableFuture<ReadResult> future = new CompletableFuture<>();
        if (!pendingRemoteRead.compareAndSet(null, future)) {
            throw new IllegalStateException("An index source read already in progress");
        }
        try {
            remoteExecutor.execute(
                    () ->
                            readRemote(
                                    future,
                                    nextOffset,
                                    highWatermark,
                                    localLogStartOffset,
                                    maxBytes));
        } catch (Throwable failure) {
            pendingRemoteRead.compareAndSet(future, null);
            future.completeExceptionally(failure);
        }
        return future;
    }

    TableBucket tableBucket() {
        return sourceLog.tableBucket();
    }

    long highWatermark() {
        return sourceLog.highWatermark();
    }

    long logStartOffset() {
        return sourceLog.logStartOffset();
    }

    private void readRemote(
            CompletableFuture<ReadResult> future,
            long nextOffset,
            long highWatermark,
            long localLogStartOffset,
            int maxBytes) {
        RemoteFetcher remoteFetcher = null;
        RemoteRead remoteRead = null;
        Throwable taskFailure = null;
        boolean localHandoffStarted = false;
        boolean remoteFailureCounted = false;
        try {
            remoteFetcher = getOrOpenRemoteFetcher();
            if (closed.get()) {
                discardRemoteFetcher(remoteFetcher);
                future.cancel(true);
                return;
            }

            long remoteEnd = Math.min(localLogStartOffset, highWatermark);
            BatchCollector collector = new BatchCollector(nextOffset, highWatermark, maxBytes);
            remoteRead = remoteFetcher.fetchBounded(nextOffset, localLogStartOffset);
            collector.collect(remoteRead, remoteEnd);
            if (!collector.limitReached() && collector.nextOffset() < remoteEnd) {
                throw corruption(
                        "remote WAL ended at expected offset "
                                + collector.nextOffset()
                                + " before local handoff "
                                + remoteEnd);
            }

            AutoCloseable resultResource =
                    remoteReadResource(
                            remoteRead, remoteFetcher, collector.nextOffset() == remoteEnd);
            collector.attachResource(resultResource);

            ReadResult result;
            if (!collector.limitReached()
                    && collector.nextOffset() == localLogStartOffset
                    && collector.nextOffset() < highWatermark) {
                localHandoffStarted = true;
                result =
                        readLocal(
                                collector.nextOffset(),
                                highWatermark,
                                collector.remainingBytes(),
                                collector);
            } else {
                result = collector.finish(resultResource);
            }
            remoteRead = null;
            if (closed.get() || future.isCancelled()) {
                result.close();
                future.cancel(true);
            } else if (!future.complete(result)) {
                result.close();
            }
        } catch (Throwable failure) {
            taskFailure = failure;
        } finally {
            if (remoteRead != null) {
                try {
                    remoteRead.close();
                } catch (Throwable closeFailure) {
                    if (metrics != null) {
                        metrics.indexReplicationFailures().inc();
                        remoteFailureCounted = true;
                    }
                    if (taskFailure == null) {
                        taskFailure = closeFailure;
                    } else {
                        taskFailure.addSuppressed(closeFailure);
                    }
                }
            }
            if (taskFailure != null) {
                if (metrics != null && !localHandoffStarted && !remoteFailureCounted) {
                    metrics.indexReplicationFailures().inc();
                }
                if (remoteFetcher != null) {
                    discardRemoteFetcher(remoteFetcher);
                }
                pendingRemoteRead.compareAndSet(future, null);
                if (!future.isCancelled()) {
                    future.completeExceptionally(taskFailure);
                }
            }
        }
    }

    private RemoteFetcher getOrOpenRemoteFetcher() {
        RemoteFetcher existing = remoteFetcherSession.get();
        if (existing != null) {
            return existing;
        }
        RemoteFetcher opened = new CloseOnceRemoteFetcher(remoteFetcherFactory.open());
        if (remoteFetcherSession.compareAndSet(null, opened)) {
            return opened;
        }
        opened.close();
        return remoteFetcherSession.get();
    }

    private void discardRemoteFetcher(RemoteFetcher fetcher) {
        if (remoteFetcherSession.compareAndSet(fetcher, null)) {
            fetcher.close();
        }
    }

    private AutoCloseable remoteReadResource(
            RemoteRead remoteRead, RemoteFetcher remoteFetcher, boolean retireSession) {
        return () -> {
            try {
                try {
                    if (retireSession) {
                        // Closing the fetcher first prevents iterator release from caching the
                        // final segment after the result's batch views have been consumed.
                        discardRemoteFetcher(remoteFetcher);
                    }
                } finally {
                    remoteRead.close();
                }
            } catch (RuntimeException | Error failure) {
                if (metrics != null) {
                    metrics.indexReplicationFailures().inc();
                }
                throw failure;
            }
        };
    }

    private ReadResult readLocal(
            long nextOffset,
            long highWatermark,
            int maxBytes,
            @Nullable BatchCollector existingCollector)
            throws IOException {
        BatchCollector collector =
                existingCollector == null
                        ? new BatchCollector(nextOffset, highWatermark, maxBytes)
                        : existingCollector;
        long beforeReadOffset = collector.nextOffset();
        FetchDataInfo fetch =
                sourceLog.read(
                        beforeReadOffset,
                        Math.max(1, collector.remainingBytes()),
                        FetchIsolation.HIGH_WATERMARK,
                        true);
        collector.collect(fetch.getRecords().batches(), highWatermark);
        if (collector.nextOffset() == beforeReadOffset && beforeReadOffset < highWatermark) {
            if (collector.limitReached()) {
                return collector.finish(
                        existingCollector == null ? null : existingCollector.resource());
            }
            throw corruption("local WAL has no record at expected offset " + beforeReadOffset);
        }
        return collector.finish(existingCollector == null ? null : existingCollector.resource());
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture<ReadResult> future = pendingRemoteRead.getAndSet(null);
        if (future != null) {
            ReadResult completed =
                    future.isDone() && !future.isCompletedExceptionally() && !future.isCancelled()
                            ? future.getNow(null)
                            : null;
            future.cancel(true);
            if (completed != null) {
                completed.close();
            }
        }
        RemoteFetcher fetcher = remoteFetcherSession.getAndSet(null);
        if (fetcher != null) {
            fetcher.close();
        }
    }

    private IndexSourceWalCorruptionException corruption(String message) {
        return new IndexSourceWalCorruptionException(
                "Corrupt source WAL for " + sourceLog.tableBucket() + ": " + message);
    }

    private IndexSourceWalCorruptionException corruption(String message, Throwable cause) {
        return new IndexSourceWalCorruptionException(
                "Corrupt source WAL for " + sourceLog.tableBucket() + ": " + message, cause);
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable failure) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(failure);
        return future;
    }

    /** Owns the batches and any downloaded files backing them until {@link #close()}. */
    public static final class ReadResult implements AutoCloseable {
        private final List<SourceBatchSlice> batches;
        private final long nextOffset;
        @Nullable private final AutoCloseable resource;
        @Nullable private final Runnable onClose;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ReadResult(
                List<SourceBatchSlice> batches,
                long nextOffset,
                @Nullable AutoCloseable resource,
                @Nullable Runnable onClose) {
            this.batches = Collections.unmodifiableList(new ArrayList<>(batches));
            this.nextOffset = nextOffset;
            this.resource = resource;
            this.onClose = onClose;
        }

        private static ReadResult empty(long nextOffset) {
            return new ReadResult(Collections.emptyList(), nextOffset, null, null);
        }

        List<SourceBatchSlice> batches() {
            return batches;
        }

        public long nextOffset() {
            return nextOffset;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                if (resource != null) {
                    try {
                        resource.close();
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to close index source read", e);
                    }
                }
            } finally {
                if (onClose != null) {
                    onClose.run();
                }
            }
        }
    }

    private final class BatchCollector {
        private final List<SourceBatchSlice> batches = new ArrayList<>();
        private final long highWatermark;
        private final int maxBytes;
        private long bytes;
        private long nextOffset;
        private boolean byteLimitReached;
        @Nullable private AutoCloseable resource;

        private BatchCollector(long nextOffset, long highWatermark, int maxBytes) {
            this.nextOffset = nextOffset;
            this.highWatermark = highWatermark;
            this.maxBytes = maxBytes;
        }

        private void collect(Iterable<LogRecordBatch> sourceBatches, long upperBound) {
            for (LogRecordBatch batch : sourceBatches) {
                if (!batches.isEmpty() && bytes + batch.sizeInBytes() > maxBytes) {
                    byteLimitReached = true;
                    return;
                }
                validateBatch(batch);
                if (batch.baseLogOffset() > nextOffset) {
                    throw corruption(
                            "expected offset "
                                    + nextOffset
                                    + " but next batch starts at "
                                    + batch.baseLogOffset());
                }
                if (batch.nextLogOffset() <= nextOffset) {
                    continue;
                }

                long start = nextOffset;
                long end = Math.min(Math.min(batch.nextLogOffset(), upperBound), highWatermark);
                if (start < end) {
                    batches.add(new SourceBatchSlice(batch, start, end));
                    bytes += batch.sizeInBytes();
                    if (metrics != null) {
                        metrics.indexReplicationSourceBytes().inc(batch.sizeInBytes());
                    }
                    nextOffset = end;
                    if (bytes >= maxBytes
                            && nextOffset < upperBound
                            && nextOffset < highWatermark) {
                        byteLimitReached = true;
                        return;
                    }
                }
                if (nextOffset >= upperBound || nextOffset >= highWatermark) {
                    return;
                }
            }
        }

        private void validateBatch(LogRecordBatch batch) {
            try {
                batch.ensureValid();
                long baseOffset = batch.baseLogOffset();
                long nextBatchOffset = batch.nextLogOffset();
                if (baseOffset < 0 || nextBatchOffset <= baseOffset) {
                    throw corruption(
                            "invalid batch offset range ["
                                    + baseOffset
                                    + ", "
                                    + nextBatchOffset
                                    + ")");
                }

            } catch (IndexSourceWalCorruptionException failure) {
                throw failure;
            } catch (RuntimeException failure) {
                throw corruption("record batch failed integrity validation", failure);
            }
        }

        private ReadResult finish(@Nullable AutoCloseable resource) {
            this.resource = resource;
            CompletableFuture<ReadResult> future = pendingRemoteRead.get();
            Runnable onClose =
                    future == null ? null : () -> pendingRemoteRead.compareAndSet(future, null);
            return new ReadResult(batches, nextOffset, resource, onClose);
        }

        private void attachResource(AutoCloseable resource) {
            this.resource = resource;
        }

        private long nextOffset() {
            return nextOffset;
        }

        private int remainingBytes() {
            return (int) Math.max(1L, maxBytes - bytes);
        }

        private long bytes() {
            return bytes;
        }

        private boolean limitReached() {
            return byteLimitReached;
        }

        @Nullable
        private AutoCloseable resource() {
            return resource;
        }
    }

    final class SourceBatchSlice {
        private final LogRecordBatch delegate;
        private final long startOffset;
        private final long endOffset;

        private SourceBatchSlice(LogRecordBatch delegate, long startOffset, long endOffset) {
            this.delegate = delegate;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        long nextOffset() {
            return endOffset;
        }

        CloseableIterator<LogRecord> records(LogRecordBatch.ReadContext context) {
            return new OffsetBoundedIterator(
                    delegate.records(context), delegate.baseLogOffset(), startOffset, endOffset);
        }
    }

    private final class OffsetBoundedIterator implements CloseableIterator<LogRecord> {
        private final CloseableIterator<LogRecord> delegate;
        private final long startOffset;
        private final long endOffset;
        private long expectedRecordOffset;
        @Nullable private LogRecord next;
        private boolean sawRecord;
        private boolean finished;

        private OffsetBoundedIterator(
                CloseableIterator<LogRecord> delegate,
                long baseOffset,
                long startOffset,
                long endOffset) {
            this.delegate = delegate;
            this.expectedRecordOffset = baseOffset;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        @Override
        public boolean hasNext() {
            while (next == null && !finished && delegate.hasNext()) {
                LogRecord candidate = delegate.next();
                long candidateOffset = candidate.logOffset();
                if (candidateOffset != expectedRecordOffset) {
                    throw corruption(
                            "expected record offset "
                                    + expectedRecordOffset
                                    + " but found "
                                    + candidateOffset);
                }
                expectedRecordOffset++;
                sawRecord = true;
                if (candidateOffset < startOffset) {
                    continue;
                }
                if (candidateOffset >= endOffset) {
                    finished = true;
                    break;
                }
                next = candidate;
            }
            if (next == null && !finished && !delegate.hasNext()) {
                finished = true;
                if (sawRecord && expectedRecordOffset < endOffset) {
                    throw corruption(
                            "record range ended at offset "
                                    + expectedRecordOffset
                                    + " before expected offset "
                                    + endOffset);
                }
            }
            return next != null;
        }

        @Override
        public LogRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            LogRecord result = next;
            next = null;
            return result;
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class LogTabletSourceLog implements SourceLog {
        private final LogTablet logTablet;

        private LogTabletSourceLog(LogTablet logTablet) {
            this.logTablet = logTablet;
        }

        @Override
        public TableBucket tableBucket() {
            return logTablet.getTableBucket();
        }

        @Override
        public long highWatermark() {
            return logTablet.getHighWatermark();
        }

        @Override
        public long logStartOffset() {
            return logTablet.logStartOffset();
        }

        @Override
        public long localLogStartOffset() {
            return logTablet.localLogStartOffset();
        }

        @Override
        public FetchDataInfo read(
                long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage)
                throws IOException {
            return logTablet.read(offset, maxBytes, isolation, minOneMessage);
        }
    }

    private static final class RemoteFetcherAdapter implements RemoteFetcher {
        private final RemoteLogFetcher delegate;

        private RemoteFetcherAdapter(RemoteLogFetcher delegate) {
            this.delegate = delegate;
        }

        @Override
        public Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset)
                throws Exception {
            return delegate.fetch(startOffset, localLogStartOffset);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static final class CloseOnceRemoteFetcher implements RemoteFetcher {
        private final RemoteFetcher delegate;
        private final AtomicBoolean closed = new AtomicBoolean();

        private CloseOnceRemoteFetcher(RemoteFetcher delegate) {
            this.delegate = delegate;
        }

        @Override
        public Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset)
                throws Exception {
            return delegate.fetch(startOffset, localLogStartOffset);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                delegate.close();
            }
        }
    }
}

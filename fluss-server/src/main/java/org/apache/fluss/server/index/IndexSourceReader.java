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
import org.apache.fluss.server.kv.RemoteLogFetcher;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.shaded.guava32.com.google.common.collect.AbstractIterator;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Reads a continuous, high-watermark-bounded source WAL range for index replication. */
@Internal
final class IndexSourceReader implements AutoCloseable {

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

    interface RemoteFetcher extends AutoCloseable {
        Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset) throws Exception;

        @Override
        void close();
    }

    private final SourceLog sourceLog;
    /** Opens a fresh, independently owned remote WAL session. */
    @Nullable private final Supplier<RemoteFetcher> remoteFetcherFactory;

    private final Executor remoteExecutor;
    @Nullable private final TabletServerMetricGroup metrics;
    private final AtomicBoolean closed = new AtomicBoolean();
    /** Removing the session transfers responsibility for closing it. */
    private final AtomicReference<RemoteFetcher> remoteFetcherSession = new AtomicReference<>();

    IndexSourceReader(
            LogTablet logTablet,
            RemoteLogManager remoteLogManager,
            Executor remoteExecutor,
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
                metrics);
    }

    IndexSourceReader(
            SourceLog sourceLog,
            @Nullable Supplier<RemoteFetcher> remoteFetcherFactory,
            Executor remoteExecutor) {
        this(sourceLog, remoteFetcherFactory, remoteExecutor, null);
    }

    IndexSourceReader(
            SourceLog sourceLog,
            @Nullable Supplier<RemoteFetcher> remoteFetcherFactory,
            Executor remoteExecutor,
            @Nullable TabletServerMetricGroup metrics) {
        this.sourceLog = sourceLog;
        this.remoteFetcherFactory = remoteFetcherFactory;
        this.remoteExecutor = remoteExecutor;
        this.metrics = metrics;
    }

    /**
     * Returns a bounded continuous source range, fetching remote raw WAL asynchronously if needed.
     * The owning replicator serializes reads and cancels or closes the delivered result. An async
     * task remains responsible for a result that its future does not accept.
     */
    CompletableFuture<ReadResult> read(long nextOffset, long endOffsetLimit, int maxBytes) {
        if (closed.get()) {
            throw new IllegalStateException("Index source reader is closed");
        }
        if (nextOffset < 0 || endOffsetLimit < nextOffset || maxBytes <= 0) {
            throw new IllegalArgumentException(
                    "Invalid index source read range ["
                            + nextOffset
                            + ", "
                            + endOffsetLimit
                            + ") with maxBytes="
                            + maxBytes);
        }
        if (nextOffset >= endOffsetLimit) {
            return CompletableFuture.completedFuture(ReadResult.empty(nextOffset));
        }

        long localLogStartOffset = sourceLog.localLogStartOffset();
        if (nextOffset >= localLogStartOffset) {
            try {
                return CompletableFuture.completedFuture(
                        readLocal(nextOffset, endOffsetLimit, maxBytes, null));
            } catch (Throwable failure) {
                return FutureUtils.completedExceptionally(failure);
            }
        }

        if (remoteFetcherFactory == null) {
            return FutureUtils.completedExceptionally(
                    corruption(
                            "offset "
                                    + nextOffset
                                    + " is below local log start "
                                    + localLogStartOffset
                                    + " but no raw remote WAL reader is available"));
        }

        CompletableFuture<ReadResult> future = new CompletableFuture<>();
        try {
            remoteExecutor.execute(
                    () ->
                            readRemote(
                                    future,
                                    nextOffset,
                                    endOffsetLimit,
                                    localLogStartOffset,
                                    maxBytes));
        } catch (Throwable failure) {
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
            long endOffsetLimit,
            long localLogStartOffset,
            int maxBytes) {
        RemoteFetcher remoteFetcher = null;
        boolean localHandoffStarted = false;
        try {
            remoteFetcher = getOrOpenRemoteFetcher();
            if (closed.get()) {
                discardRemoteFetcher(remoteFetcher);
                future.cancel(true);
                return;
            }

            long remoteEnd = Math.min(localLogStartOffset, endOffsetLimit);
            BatchCollector collector = new BatchCollector(nextOffset, endOffsetLimit, maxBytes);
            collector.collect(remoteFetcher.fetch(nextOffset, localLogStartOffset), remoteEnd);
            if (!collector.limitReached() && collector.nextOffset() < remoteEnd) {
                throw corruption(
                        "remote WAL ended at expected offset "
                                + collector.nextOffset()
                                + " before local handoff "
                                + remoteEnd);
            }

            collector.attachResource(
                    remoteSessionResource(remoteFetcher, collector.nextOffset() == remoteEnd));

            ReadResult result;
            if (!collector.limitReached()
                    && collector.nextOffset() == localLogStartOffset
                    && collector.nextOffset() < endOffsetLimit) {
                localHandoffStarted = true;
                result =
                        readLocal(
                                collector.nextOffset(),
                                endOffsetLimit,
                                collector.remainingBytes(),
                                collector);
            } else {
                result = collector.finish();
            }
            if (closed.get() || future.isCancelled()) {
                result.close();
                future.cancel(true);
            } else if (!future.complete(result)) {
                result.close();
            }
        } catch (Throwable failure) {
            if (metrics != null && !localHandoffStarted) {
                metrics.indexReplicationFailures().inc();
            }
            if (remoteFetcher != null) {
                discardRemoteFetcher(remoteFetcher);
            }
            if (!future.isCancelled()) {
                future.completeExceptionally(failure);
            }
        }
    }

    private RemoteFetcher getOrOpenRemoteFetcher() {
        RemoteFetcher existing = remoteFetcherSession.get();
        if (existing != null) {
            return existing;
        }
        RemoteFetcher opened = remoteFetcherFactory.get();
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

    private AutoCloseable remoteSessionResource(
            RemoteFetcher remoteFetcher, boolean retireSession) {
        return () -> {
            try {
                if (retireSession) {
                    // Retire the session after the result's batch views have been consumed.
                    discardRemoteFetcher(remoteFetcher);
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
            long endOffsetLimit,
            int maxBytes,
            @Nullable BatchCollector existingCollector)
            throws IOException {
        BatchCollector collector =
                existingCollector == null
                        ? new BatchCollector(nextOffset, endOffsetLimit, maxBytes)
                        : existingCollector;
        long beforeReadOffset = collector.nextOffset();
        FetchDataInfo fetch =
                sourceLog.read(
                        beforeReadOffset,
                        Math.max(1, collector.remainingBytes()),
                        FetchIsolation.HIGH_WATERMARK,
                        true);
        collector.collect(fetch.getRecords().batches(), endOffsetLimit);
        if (collector.nextOffset() == beforeReadOffset && beforeReadOffset < endOffsetLimit) {
            if (collector.limitReached()) {
                return collector.finish();
            }
            throw corruption("local WAL has no record at expected offset " + beforeReadOffset);
        }
        return collector.finish();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
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

    /** Owns the batches and any downloaded files backing them until {@link #close()}. */
    static final class ReadResult implements AutoCloseable {
        private final List<LogRecordBatch> batches;
        private final long nextOffset;
        private final boolean byteLimited;
        private final long bytes;
        @Nullable private final AutoCloseable resource;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ReadResult(
                List<LogRecordBatch> batches,
                long nextOffset,
                boolean byteLimited,
                long bytes,
                @Nullable AutoCloseable resource) {
            // The collector no longer appends after handing this list to the result.
            this.batches = Collections.unmodifiableList(batches);
            this.bytes = bytes;
            this.nextOffset = nextOffset;
            this.byteLimited = byteLimited;
            this.resource = resource;
        }

        private static ReadResult empty(long nextOffset) {
            return new ReadResult(Collections.emptyList(), nextOffset, false, 0L, null);
        }

        List<LogRecordBatch> batches() {
            return batches;
        }

        long nextOffset() {
            return nextOffset;
        }

        long bytes() {
            return bytes;
        }

        /** A smaller complete-batch prefix, or zero when this input cannot be split. */
        int smallerReadBytes() {
            if (batches.size() <= 1) {
                return 0;
            }
            long bytes = 0;
            for (int i = 0; i < batches.size() / 2; i++) {
                bytes += batches.get(i).sizeInBytes();
            }
            return (int) Math.min(Integer.MAX_VALUE, bytes);
        }

        boolean isByteLimited() {
            return byteLimited;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to close index source read", e);
                }
            }
        }
    }

    private final class BatchCollector {
        private final List<LogRecordBatch> batches = new ArrayList<>();
        private final long endOffsetLimit;
        private final int maxBytes;
        private long bytes;
        private long nextOffset;
        private boolean byteLimitReached;
        @Nullable private AutoCloseable resource;

        private BatchCollector(long nextOffset, long endOffsetLimit, int maxBytes) {
            this.nextOffset = nextOffset;
            this.endOffsetLimit = endOffsetLimit;
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
                long end = Math.min(Math.min(batch.nextLogOffset(), upperBound), endOffsetLimit);
                if (start < end) {
                    if (start != batch.baseLogOffset() || end != batch.nextLogOffset()) {
                        throw corruption(
                                "source read range ["
                                        + start
                                        + ", "
                                        + end
                                        + ") splits record batch ["
                                        + batch.baseLogOffset()
                                        + ", "
                                        + batch.nextLogOffset()
                                        + ")");
                    }
                    batches.add(batch);
                    bytes += batch.sizeInBytes();
                    if (metrics != null) {
                        metrics.indexReplicationSourceBytes().inc(batch.sizeInBytes());
                    }
                    nextOffset = end;
                    if (bytes >= maxBytes
                            && nextOffset < upperBound
                            && nextOffset < endOffsetLimit) {
                        byteLimitReached = true;
                        return;
                    }
                }
                if (nextOffset >= upperBound || nextOffset >= endOffsetLimit) {
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

        private ReadResult finish() {
            return new ReadResult(batches, nextOffset, byteLimitReached, bytes, resource);
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

        private boolean limitReached() {
            return byteLimitReached;
        }
    }

    CloseableIterator<LogRecord> records(LogRecordBatch batch, LogRecordBatch.ReadContext context) {
        return new ValidatingRecordIterator(
                batch.records(context), batch.baseLogOffset(), batch.nextLogOffset());
    }

    private final class ValidatingRecordIterator extends AbstractIterator<LogRecord>
            implements CloseableIterator<LogRecord> {
        private final CloseableIterator<LogRecord> delegate;
        private final long endOffset;
        private long expectedRecordOffset;
        private boolean sawRecord;

        private ValidatingRecordIterator(
                CloseableIterator<LogRecord> delegate, long baseOffset, long endOffset) {
            this.delegate = delegate;
            this.expectedRecordOffset = baseOffset;
            this.endOffset = endOffset;
        }

        @Override
        protected LogRecord computeNext() {
            while (delegate.hasNext()) {
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
                return candidateOffset < endOffset ? candidate : endOfData();
            }
            if (sawRecord && expectedRecordOffset < endOffset) {
                throw corruption(
                        "record range ended at offset "
                                + expectedRecordOffset
                                + " before expected offset "
                                + endOffset);
            }
            return endOfData();
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
}

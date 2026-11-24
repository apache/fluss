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
import org.apache.fluss.server.exception.IndexSourceWalCorruptionException;
import org.apache.fluss.server.kv.RemoteLogFetcher;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.remote.RemoteLogManager;
import org.apache.fluss.shaded.guava32.com.google.common.collect.AbstractIterator;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.concurrent.FutureUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Reads a continuous, high-watermark-bounded source WAL range for index replication. */
@Internal
final class IndexSourceReader implements AutoCloseable {

    interface SourceLog {
        TableBucket tableBucket();

        long highWatermark();

        long localLogStartOffset();

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
    private final Supplier<RemoteFetcher> remoteFetcherFactory;

    private final Executor remoteExecutor;
    private volatile boolean closed;
    /** Removing the session transfers responsibility for closing it. */
    private final AtomicReference<RemoteFetcher> idleRemoteFetcher = new AtomicReference<>();

    IndexSourceReader(
            LogTablet logTablet, RemoteLogManager remoteLogManager, Executor remoteExecutor) {
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
                remoteExecutor);
    }

    IndexSourceReader(
            SourceLog sourceLog,
            Supplier<RemoteFetcher> remoteFetcherFactory,
            Executor remoteExecutor) {
        this.sourceLog = sourceLog;
        this.remoteFetcherFactory = remoteFetcherFactory;
        this.remoteExecutor = remoteExecutor;
    }

    /**
     * Returns a bounded continuous source range, fetching remote raw WAL asynchronously if needed.
     * The owning replicator serializes reads.
     */
    CompletableFuture<ReadResult> read(long nextOffset, long endOffsetLimit, int maxBytes) {
        if (closed) {
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
                        new BatchCollector(nextOffset, maxBytes)
                                .readLocal(endOffsetLimit)
                                .finish());
            } catch (Throwable failure) {
                return FutureUtils.completedExceptionally(failure);
            }
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

    private void readRemote(
            CompletableFuture<ReadResult> future,
            long nextOffset,
            long endOffsetLimit,
            long localLogStartOffset,
            int maxBytes) {
        RemoteFetcher remoteFetcher = null;
        try {
            remoteFetcher = idleRemoteFetcher.getAndSet(null);
            if (closed) {
                future.cancel(true);
                return;
            }

            if (remoteFetcher == null) {
                remoteFetcher = remoteFetcherFactory.get();
            }
            long remoteEnd = Math.min(localLogStartOffset, endOffsetLimit);
            BatchCollector collector = new BatchCollector(nextOffset, maxBytes);
            collector.collect(remoteFetcher.fetch(nextOffset, localLogStartOffset), remoteEnd);
            if (!collector.byteLimitReached && collector.nextOffset < remoteEnd) {
                throw corruption(
                        "remote WAL ended at expected offset "
                                + collector.nextOffset
                                + " before local handoff "
                                + remoteEnd);
            }

            boolean reachedHandoff = collector.nextOffset == localLogStartOffset;
            if (reachedHandoff
                    && !collector.byteLimitReached
                    && collector.nextOffset < endOffsetLimit) {
                collector.readLocal(endOffsetLimit);
            }
            // CRC validation materializes each file-backed batch before the iterator advances.
            // The consumer can decode those batches after the remote files have been closed.
            ReadResult result = collector.finish();
            if (collector.nextOffset < remoteEnd
                    && idleRemoteFetcher.compareAndSet(null, remoteFetcher)) {
                remoteFetcher = null;
                if (closed) {
                    closeIdleRemoteFetcher();
                }
            }
            future.complete(result);
        } catch (Throwable failure) {
            future.completeExceptionally(failure);
        } finally {
            if (remoteFetcher != null) {
                remoteFetcher.close();
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        closeIdleRemoteFetcher();
    }

    private void closeIdleRemoteFetcher() {
        RemoteFetcher fetcher = idleRemoteFetcher.getAndSet(null);
        if (fetcher != null) {
            fetcher.close();
        }
    }

    private IndexSourceWalCorruptionException corruption(String message) {
        return new IndexSourceWalCorruptionException(sourceLog.tableBucket(), message);
    }

    private IndexSourceWalCorruptionException corruption(String message, Throwable cause) {
        return new IndexSourceWalCorruptionException(sourceLog.tableBucket(), message, cause);
    }

    static final class ReadResult {
        private final List<LogRecordBatch> batches;
        private final long nextOffset;
        private final boolean byteLimited;
        private final long bytes;

        private ReadResult(
                List<LogRecordBatch> batches, long nextOffset, boolean byteLimited, long bytes) {
            // The collector no longer appends after handing this list to the result.
            this.batches = Collections.unmodifiableList(batches);
            this.bytes = bytes;
            this.nextOffset = nextOffset;
            this.byteLimited = byteLimited;
        }

        private static ReadResult empty(long nextOffset) {
            return new ReadResult(Collections.emptyList(), nextOffset, false, 0L);
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
    }

    private final class BatchCollector {
        private final List<LogRecordBatch> batches = new ArrayList<>();
        private final int maxBytes;
        private long bytes;
        private long nextOffset;
        private boolean byteLimitReached;

        private BatchCollector(long nextOffset, int maxBytes) {
            this.nextOffset = nextOffset;
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

                if (batch.baseLogOffset() != nextOffset || batch.nextLogOffset() > upperBound) {
                    throw corruption(
                            "source read range ["
                                    + nextOffset
                                    + ", "
                                    + Math.min(batch.nextLogOffset(), upperBound)
                                    + ") splits record batch ["
                                    + batch.baseLogOffset()
                                    + ", "
                                    + batch.nextLogOffset()
                                    + ")");
                }
                batches.add(batch);
                bytes += batch.sizeInBytes();
                nextOffset = batch.nextLogOffset();
                if (nextOffset == upperBound) {
                    return;
                }
                if (bytes >= maxBytes) {
                    byteLimitReached = true;
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

        private BatchCollector readLocal(long endOffsetLimit) throws IOException {
            long start = nextOffset;
            FetchDataInfo fetch =
                    sourceLog.read(
                            start,
                            (int) Math.max(1L, maxBytes - bytes),
                            FetchIsolation.HIGH_WATERMARK,
                            true);
            collect(fetch.getRecords().batches(), endOffsetLimit);
            if (nextOffset == start && start < endOffsetLimit && !byteLimitReached) {
                throw corruption("local WAL has no record at expected offset " + start);
            }
            return this;
        }

        private ReadResult finish() {
            return new ReadResult(batches, nextOffset, byteLimitReached, bytes);
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

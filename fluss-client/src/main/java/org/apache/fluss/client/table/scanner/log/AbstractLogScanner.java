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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.OffsetMetadata;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.log.internals.ConsumerGroupClient;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.ConsumerOffsetReset;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.exception.WakeupException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class that centralizes the shared poll loop, lightweight single-thread guard, wakeup and
 * close lifecycle for log scanners. Concrete scanners supply the result type {@code R} and an
 * enrichment step from raw {@link ScanRecords} to that type.
 *
 * <p>This class is NOT thread-safe. The lightweight guard only detects accidental concurrent use
 * and throws {@link ConcurrentModificationException}; it does not serialize callers.
 */
@Internal
abstract class AbstractLogScanner<R> implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractLogScanner.class);

    private static final long NO_CURRENT_THREAD = -1L;

    protected final String name;
    protected final LogScannerStatus logScannerStatus;
    protected final LogFetcher logFetcher;
    protected final ScannerMetricGroup scannerMetricGroup;
    @Nullable private final ConsumerGroupClient consumerGroupClient;
    private final long defaultApiTimeoutMs;
    private final ConsumerOffsetReset offsetResetPolicy;

    private volatile boolean closed = false;

    // currentThread holds the threadId of the current thread accessing the scanner
    // and is used to prevent multithreaded access.
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refCount is used to allow reentrant access by the thread that has acquired currentThread.
    private final AtomicInteger refCount = new AtomicInteger(0);

    protected AbstractLogScanner(
            String name,
            LogScannerStatus logScannerStatus,
            LogFetcher logFetcher,
            ScannerMetricGroup scannerMetricGroup,
            Configuration conf,
            MetadataUpdater metadataUpdater) {
        this.name = name;
        this.logScannerStatus = logScannerStatus;
        this.logFetcher = logFetcher;
        this.scannerMetricGroup = scannerMetricGroup;
        try {
            this.scannerMetricGroup.registerRecordsLagGauge(logScannerStatus::recordsLag);
            this.defaultApiTimeoutMs = conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis();
            this.offsetResetPolicy = conf.get(ConfigOptions.CLIENT_CONSUMER_AUTO_OFFSET_RESET);
            this.consumerGroupClient =
                    createConsumerGroupClient(conf, metadataUpdater, logScannerStatus);
        } catch (Throwable t) {
            closeFetcherAndMetrics();
            throw t;
        }
    }

    @Nullable
    private static ConsumerGroupClient createConsumerGroupClient(
            Configuration conf,
            MetadataUpdater metadataUpdater,
            LogScannerStatus logScannerStatus) {
        if (conf.getString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID) != null) {
            return new ConsumerGroupClient(conf, metadataUpdater, logScannerStatus);
        }
        if (conf.getBoolean(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED)
                && conf.containsKey(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key())) {
            throw new InvalidConfigException(
                    ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key()
                            + " cannot be true without "
                            + ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key());
        }
        return null;
    }

    /**
     * Template poll loop shared by all concrete scanners.
     *
     * <ul>
     *   <li>If {@link #pollForFetches()} returns a batch with scanner progress, {@link
     *       #toResult(ScanRecords)} is invoked to produce the concrete result type.
     *   <li>If the batch has no progress, we wait on {@link LogFetcher#awaitNotEmpty(long)} until
     *       either data arrives, the timeout elapses, or {@link #wakeup()} is called.
     *   <li>Any empty/timeout/wakeup path returns {@link #emptyResult()}.
     * </ul>
     */
    public final R poll(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            long startNanos = System.nanoTime();
            long timeoutNanos = timeout.toNanos();
            if (!logScannerStatus.prepareToPoll()) {
                throw new IllegalStateException(notSubscribedMessage());
            }

            if (!initializeCommittedOffsets(startNanos, timeoutNanos)) {
                return emptyResult();
            }
            maybeAutoCommit();
            invokeCompletedOffsetCommitCallbacks();

            scannerMetricGroup.recordPollStart(System.currentTimeMillis());
            do {
                logFetcher.resetPositionsIfNeeded();
                ScanRecords scanRecords = pollForFetches();
                if (!scanRecords.hasProgress()) {
                    try {
                        if (!logFetcher.awaitNotEmpty(startNanos + timeoutNanos)) {
                            // no data in buffer within the timeout
                            return emptyResult();
                        }
                    } catch (WakeupException e) {
                        // wakeup() was called, return empty
                        return emptyResult();
                    }
                } else {
                    // before returning the fetched records, send off the next round of fetches
                    // to enable pipelining while the user is handling the current batch.
                    logFetcher.sendFetches();
                    return toResult(scanRecords);
                }
            } while (System.nanoTime() - startNanos < timeoutNanos);

            return emptyResult();
        } finally {
            release();
            scannerMetricGroup.recordPollEnd(System.currentTimeMillis());
        }
    }

    private ScanRecords pollForFetches() {
        ScanRecords scanRecords = logFetcher.collectFetch();
        if (scanRecords.hasProgress()) {
            return scanRecords;
        }

        // send any new fetches (won't resend pending fetches).
        logFetcher.sendFetches();

        return logFetcher.collectFetch();
    }

    /** The empty sentinel for the concrete result type. */
    protected abstract R emptyResult();

    /** Convert a non-empty batch of {@link ScanRecords} into the concrete result type. */
    protected abstract R toResult(ScanRecords scanRecords);

    /** Error message for {@link IllegalStateException} when no buckets are subscribed. */
    protected abstract String notSubscribedMessage();

    public final void wakeup() {
        logFetcher.wakeup();
    }

    protected final void subscribeFromCommittedOffset(TableBucket tableBucket) {
        requireConsumerGroup();
        logScannerStatus.assignFromCommittedOffset(tableBucket);
    }

    /**
     * Initializes pending fetch positions using committed offsets and the configured reset policy.
     * Latest resets are dispatched asynchronously by the fetcher.
     *
     * @param startNanos the value of {@link System#nanoTime()} when the current poll started
     * @param timeoutNanos the total timeout for the current poll, in nanoseconds
     * @return {@code true} if no committed offsets were needed or their lookup finished with time
     *     left to fetch. Returns {@code false} if the lookup exhausted the poll timeout.
     */
    protected final boolean initializeCommittedOffsets(long startNanos, long timeoutNanos) {
        if (consumerGroupClient == null) {
            return true;
        }
        Set<TableBucket> missing = logScannerStatus.missingCommittedOffsets();
        if (missing.isEmpty()) {
            return true;
        }

        long remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
        Map<TableBucket, OffsetMetadata> committed =
                consumerGroupClient.fetchCommittedOffsets(
                        missing, Math.max(0L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
        if (committed == null) {
            return false;
        }
        logScannerStatus.resolveCommittedOffsets(committed, offsetResetPolicy);

        // Avoid starting a new fetch if offset resolution has already used the poll budget.
        return System.nanoTime() - startNanos < timeoutNanos;
    }

    protected final void maybeAutoCommit() {
        if (consumerGroupClient != null) {
            consumerGroupClient.maybeAutoCommitOffsetsAsync();
        }
    }

    public final void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    public final void commitSync(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            requireConsumerGroup();
            invokeCompletedOffsetCommitCallbacks();
            commitSyncSnapshot(logScannerStatus.allConsumed(), timeout);
        } finally {
            release();
        }
    }

    public final void commitSync(Map<TableBucket, OffsetMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    public final void commitSync(Map<TableBucket, OffsetMetadata> offsets, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            requireConsumerGroup();
            invokeCompletedOffsetCommitCallbacks();
            commitSyncSnapshot(offsets, timeout);
        } finally {
            release();
        }
    }

    private void commitSyncSnapshot(Map<TableBucket, OffsetMetadata> offsets, Duration timeout) {
        if (!consumerGroupClient.commitOffsetsSync(offsets, timeout.toMillis())) {
            throw new TimeoutException(
                    "Timed out after " + timeout.toMillis() + " ms committing offsets");
        }
    }

    public final void commitAsync() {
        commitAsync(this::logAsyncCommitFailure);
    }

    public final void commitAsync(OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            requireConsumerGroup();
            invokeCompletedOffsetCommitCallbacks();
            Map<TableBucket, OffsetMetadata> offsets = logScannerStatus.allConsumed();
            consumerGroupClient.commitOffsetsAsync(
                    Collections.unmodifiableMap(offsets),
                    callback == null ? this::logAsyncCommitFailure : callback);
        } finally {
            release();
        }
    }

    public final void commitAsync(
            Map<TableBucket, OffsetMetadata> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            requireConsumerGroup();
            invokeCompletedOffsetCommitCallbacks();
            consumerGroupClient.commitOffsetsAsync(
                    offsets, callback == null ? this::logAsyncCommitFailure : callback);
        } finally {
            release();
        }
    }

    void invokeCompletedOffsetCommitCallbacks() {
        if (consumerGroupClient != null) {
            consumerGroupClient.invokeCompletedOffsetCommitCallbacks();
        }
    }

    private void logAsyncCommitFailure(Map<TableBucket, OffsetMetadata> offsets, Exception error) {
        if (error != null) {
            LOG.error("Failed to commit consumer offsets for scanner {}", name, error);
        }
    }

    private void requireConsumerGroup() {
        if (consumerGroupClient == null) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer offset operations require "
                            + ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key());
        }
    }

    /**
     * Acquire the light lock and ensure that the scanner hasn't been closed.
     *
     * @throws IllegalStateException if the scanner has been closed
     */
    protected final void acquireAndEnsureOpen() {
        acquire();
        if (closed) {
            release();
            throw new IllegalStateException("This scanner has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this scanner from multithreaded access. Instead of blocking
     * when the lock is not available, we throw an exception (multithreaded usage is not supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        final Thread thread = Thread.currentThread();
        final long threadId = thread.getId();
        if (threadId != currentThread.get()
                && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId)) {
            throw new ConcurrentModificationException(
                    "Scanner is not safe for multithreaded access. "
                            + "currentThread(name: "
                            + thread.getName()
                            + ", id: "
                            + threadId
                            + ")"
                            + " otherThread(id: "
                            + currentThread.get()
                            + ")");
        }
        refCount.incrementAndGet();
    }

    /** Release the light lock protecting the scanner from multithreaded access. */
    protected final void release() {
        if (refCount.decrementAndGet() == 0) {
            currentThread.set(NO_CURRENT_THREAD);
        }
    }

    @Override
    public final void close() {
        acquire();
        try {
            if (!closed) {
                closed = true;
                LOG.trace("Closing log scanner: {}", name);
                if (consumerGroupClient != null) {
                    consumerGroupClient.close();
                }
                closeFetcherAndMetrics();
            }
            LOG.debug("Log scanner: {} has been closed", name);
        } finally {
            release();
        }
    }

    private void closeFetcherAndMetrics() {
        IOUtils.closeQuietly(scannerMetricGroup::close, "scanner metrics");
        IOUtils.closeQuietly(logFetcher, "log fetcher");
    }
}

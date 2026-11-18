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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.exception.TimeoutException;
import org.apache.fluss.exception.WakeupException;
import org.apache.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
    // Cache the bound supplier to avoid allocating one for each poll.
    @Nullable private final Supplier<Map<TableBucket, OffsetMetadata>> autoCommitOffsets;
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
        this.scannerMetricGroup.registerRecordsLagGauge(logScannerStatus::recordsLag);
        this.defaultApiTimeoutMs = conf.get(ConfigOptions.CLIENT_REQUEST_TIMEOUT).toMillis();
        this.offsetResetPolicy = conf.get(ConfigOptions.CLIENT_CONSUMER_AUTO_OFFSET_RESET);

        String groupId = conf.getString(ConfigOptions.CLIENT_CONSUMER_GROUP_ID);
        boolean autoCommitEnabled =
                conf.getBoolean(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED);
        if (groupId == null) {
            if (autoCommitEnabled
                    && conf.containsKey(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key())) {
                throw new InvalidConfigException(
                        ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_ENABLED.key()
                                + " cannot be true without "
                                + ConfigOptions.CLIENT_CONSUMER_GROUP_ID.key());
            }
            this.consumerGroupClient = null;
            this.autoCommitOffsets = null;
        } else {
            this.consumerGroupClient =
                    new ConsumerGroupClient(
                            groupId,
                            metadataUpdater,
                            autoCommitEnabled,
                            conf.get(ConfigOptions.CLIENT_CONSUMER_AUTO_COMMIT_INTERVAL).toMillis(),
                            defaultApiTimeoutMs);
            this.autoCommitOffsets = logScannerStatus::allConsumed;
        }
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

            scannerMetricGroup.recordPollStart(System.currentTimeMillis());
            do {
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
            try {
                scannerMetricGroup.recordPollEnd(System.currentTimeMillis());
                invokeCompletedOffsetCommitCallbacks();
            } finally {
                release();
            }
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

    protected final boolean initializeCommittedOffsets(long startNanos, long timeoutNanos) {
        Set<TableBucket> missing = logScannerStatus.missingCommittedOffsets();
        if (missing.isEmpty() && logScannerStatus.missingLatestOffsets().isEmpty()) {
            return true;
        }
        if (!missing.isEmpty()) {
            // Offset lookup and fetch waiting share the deadline set when poll began.
            long remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
            Map<TableBucket, OffsetMetadata> committed =
                    consumerGroupClient.fetchCommittedOffsets(
                            missing, Math.max(0L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
            if (committed == null) {
                return false;
            }
            logScannerStatus.resolveCommittedOffsets(committed, offsetResetPolicy);
        }

        // Both the latest reset policy and an explicit LATEST_OFFSET subscription resolve real
        // positions with ListOffsets; the sentinel is not a fetchable offset for the server.
        // Read the set again: resolving committed offsets above may have moved buckets into the
        // latest-awaiting state during this poll.
        Set<TableBucket> missingLatest = logScannerStatus.missingLatestOffsets();
        if (!missingLatest.isEmpty()) {
            long remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
            try {
                Map<TableBucket, Long> latest =
                        logFetcher.listLatestOffsets(
                                missingLatest,
                                Math.max(0L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
                if (latest == null) {
                    return false;
                }
                logScannerStatus.resolveLatestOffsets(latest);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlussRuntimeException("Interrupted while resolving latest offsets", e);
            }
        }

        // Avoid starting a new fetch if offset resolution has already used the poll budget.
        return System.nanoTime() - startNanos < timeoutNanos;
    }

    protected final void maybeAutoCommit() {
        if (consumerGroupClient != null) {
            consumerGroupClient.maybeAutoCommitOffsetsAsync(
                    System.currentTimeMillis(), autoCommitOffsets);
        }
    }

    public final void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    public final void commitSync(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            requireConsumerGroup();
            commitSyncSnapshot(logScannerStatus.allConsumed(), timeout);
        } finally {
            invokeCompletedOffsetCommitCallbacksAndRelease();
        }
    }

    public final void commitSync(Map<TableBucket, OffsetMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    public final void commitSync(Map<TableBucket, OffsetMetadata> offsets, Duration timeout) {
        acquireAndEnsureOpen();
        try {
            requireConsumerGroup();
            commitSyncSnapshot(immutableOffsets(offsets), timeout);
        } finally {
            invokeCompletedOffsetCommitCallbacksAndRelease();
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
            Map<TableBucket, OffsetMetadata> offsets = logScannerStatus.allConsumed();
            consumerGroupClient.commitOffsetsAsync(
                    Collections.unmodifiableMap(offsets),
                    callback == null ? this::logAsyncCommitFailure : callback);
        } finally {
            invokeCompletedOffsetCommitCallbacksAndRelease();
        }
    }

    public final void commitAsync(
            Map<TableBucket, OffsetMetadata> offsets, OffsetCommitCallback callback) {
        acquireAndEnsureOpen();
        try {
            requireConsumerGroup();
            consumerGroupClient.commitOffsetsAsync(
                    immutableOffsets(offsets),
                    callback == null ? this::logAsyncCommitFailure : callback);
        } finally {
            invokeCompletedOffsetCommitCallbacksAndRelease();
        }
    }

    private void invokeCompletedOffsetCommitCallbacksAndRelease() {
        try {
            invokeCompletedOffsetCommitCallbacks();
        } finally {
            release();
        }
    }

    private void invokeCompletedOffsetCommitCallbacks() {
        if (consumerGroupClient != null) {
            consumerGroupClient.invokeCompletedOffsetCommitCallbacks();
        }
    }

    private void logAsyncCommitFailure(Map<TableBucket, OffsetMetadata> offsets, Exception error) {
        if (error != null) {
            LOG.error("Failed to commit consumer offsets for scanner {}", name, error);
        }
    }

    private static Map<TableBucket, OffsetMetadata> immutableOffsets(
            Map<TableBucket, OffsetMetadata> offsets) {
        Objects.requireNonNull(offsets, "offsets must not be null");
        HashMap<TableBucket, OffsetMetadata> copy = new HashMap<>();
        for (Map.Entry<TableBucket, OffsetMetadata> entry : offsets.entrySet()) {
            copy.put(
                    Objects.requireNonNull(entry.getKey(), "table bucket must not be null"),
                    Objects.requireNonNull(entry.getValue(), "offset metadata must not be null"));
        }
        return Collections.unmodifiableMap(copy);
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
                LOG.trace("Closing log scanner: {}", name);
                // Resources close in reverse order, preserving a commit failure if cleanup fails.
                try (LogFetcher fetcherToClose = logFetcher;
                        AutoCloseable metricsToClose = scannerMetricGroup::close;
                        ConsumerGroupClient groupToClose = consumerGroupClient) {
                    if (groupToClose != null
                            && !groupToClose.maybeAutoCommitOffsetsSync(
                                    logScannerStatus.allConsumed(), defaultApiTimeoutMs)) {
                        throw new TimeoutException(
                                "Timed out after "
                                        + defaultApiTimeoutMs
                                        + " ms committing offsets on close");
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to close log scanner: " + name, e);
                }
            }
            LOG.debug("Log scanner: {} has been closed", name);
        } finally {
            closed = true;
            try {
                invokeCompletedOffsetCommitCallbacks();
            } finally {
                release();
            }
        }
    }
}

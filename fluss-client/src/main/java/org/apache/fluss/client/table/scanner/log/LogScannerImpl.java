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
import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.client.table.scanner.Scan;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.WakeupException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.Projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The default impl of {@link LogScanner}.
 *
 * <p>The {@link LogScannerImpl} is NOT thread-safe. It is the responsibility of the user to ensure
 * that multithreaded access is properly synchronized. Un-synchronized access will result in {@link
 * ConcurrentModificationException}.
 *
 * @since 0.1
 */
@PublicEvolving
public class LogScannerImpl implements LogScanner {
    private static final Logger LOG = LoggerFactory.getLogger(LogScannerImpl.class);
    private static final long NO_CURRENT_THREAD = -1L;

    private final TablePath tablePath;
    private final LogScannerStatus logScannerStatus;
    private final MetadataUpdater metadataUpdater;
    private final LogFetcher logFetcher;
    private final long tableId;
    private final boolean isPartitionedTable;
    private final boolean isArrowLogFormat;
    private final boolean isLogTable;
    private final boolean hasProjection;
    // metrics
    private final ScannerMetricGroup scannerMetricGroup;

    // currentThread holds the threadId of the current thread accessing FlussLogScanner
    // and is used to prevent multithreaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refCount is used to allow reentrant access by the thread who has acquired currentThread.
    private final AtomicInteger refCount = new AtomicInteger(0);

    private volatile boolean closed = false;

    public LogScannerImpl(
            Configuration conf,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            ClientMetricGroup clientMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            @Nullable int[] projectedFields,
            SchemaGetter schemaGetter,
            @Nullable Predicate recordBatchFilter) {
        this.tablePath = tableInfo.getTablePath();
        this.tableId = tableInfo.getTableId();
        this.isPartitionedTable = tableInfo.isPartitioned();
        this.isArrowLogFormat = tableInfo.getTableConfig().getLogFormat() == LogFormat.ARROW;
        this.isLogTable = !tableInfo.hasPrimaryKey();
        this.hasProjection = projectedFields != null;
        // add this table to metadata updater.
        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
        this.logScannerStatus = new LogScannerStatus();
        this.metadataUpdater = metadataUpdater;
        Projection projection = sanityProjection(projectedFields, tableInfo);
        this.scannerMetricGroup = new ScannerMetricGroup(clientMetricGroup, tablePath);
        this.logFetcher =
                new LogFetcher(
                        tableInfo,
                        projection,
                        recordBatchFilter,
                        logScannerStatus,
                        conf,
                        metadataUpdater,
                        scannerMetricGroup,
                        remoteFileDownloader,
                        schemaGetter);
    }

    /**
     * Check if the projected fields are valid and returns {@link Projection} if the projection is
     * not null.
     */
    @Nullable
    private Projection sanityProjection(@Nullable int[] projectedFields, TableInfo tableInfo) {
        RowType tableRowType = tableInfo.getRowType();
        if (projectedFields != null) {
            for (int projectedField : projectedFields) {
                if (projectedField < 0 || projectedField >= tableRowType.getFieldCount()) {
                    throw new IllegalArgumentException(
                            "Projected field index "
                                    + projectedField
                                    + " is out of bound for schema "
                                    + tableRowType);
                }
            }
            return Projection.of(projectedFields);
        } else {
            return null;
        }
    }

    @Override
    public ScanRecords poll(Duration timeout) {
        return doPoll(timeout, this::pollForFetches, ScanRecords::isEmpty, () -> ScanRecords.EMPTY);
    }

    /**
     * Polls Arrow record batches for internal callers.
     *
     * <p>This method is intentionally kept off the public {@link Scan} API surface for now.
     */
    @Internal
    public ArrowScanRecords pollRecordBatch(Duration timeout) {
        if (!isArrowLogFormat) {
            throw new UnsupportedOperationException(
                    "Arrow record batch polling is only supported for tables whose log format is ARROW.");
        }
        if (!isLogTable) {
            throw new UnsupportedOperationException(
                    "Arrow record batch polling is only supported for log tables. CDC scanning is not supported.");
        }
        if (hasProjection) {
            throw new UnsupportedOperationException(
                    "Arrow record batch polling does not support projection. Please create the scanner without projection.");
        }

        return doPoll(
                timeout,
                this::pollForRecordBatches,
                ArrowScanRecords::isEmpty,
                () -> ArrowScanRecords.EMPTY);
    }

    @Override
    public void subscribe(int bucket, long offset) {
        if (isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is a partitioned table, please use "
                            + "\"subscribe(long partitionId, int bucket, long offset)\" to "
                            + "subscribe a partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
            logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(long partitionId, int bucket, long offset) {
        if (!isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is not a partitioned table, please use "
                            + "\"subscribe(int bucket, long offset)\" to "
                            + "subscribe a non-partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            // we make assumption that the partition id must belong to the current table
            // if we can't find the partition id from the table path, we'll consider the table
            // is not exist
            metadataUpdater.checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(partitionId));
            logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe(long partitionId, int bucket) {
        if (!isPartitionedTable) {
            throw new IllegalStateException(
                    "Can't unsubscribe a partition for a non-partitioned table.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
        } finally {
            release();
        }
    }

    @Override
    public void unsubscribe(int bucket) {
        if (isPartitionedTable) {
            throw new IllegalStateException(
                    "The table is a partitioned table, please use "
                            + "\"unsubscribe(long partitionId, int bucket)\" to "
                            + "unsubscribe a partitioned bucket instead.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
        } finally {
            release();
        }
    }

    @Override
    public void wakeup() {
        logFetcher.wakeup();
    }

    private ScanRecords pollForFetches() {
        ScanRecords scanRecords = logFetcher.collectFetch();
        if (!scanRecords.isEmpty()) {
            return scanRecords;
        }

        // send any new fetches (won't resend pending fetches).
        logFetcher.sendFetches();
        return logFetcher.collectFetch();
    }

    private ArrowScanRecords pollForRecordBatches() {
        ArrowScanRecords scanRecords = logFetcher.collectArrowFetch();
        if (!scanRecords.isEmpty()) {
            return scanRecords;
        }

        // send any new fetches (won't resend pending fetches).
        logFetcher.sendFetches();
        return logFetcher.collectArrowFetch();
    }

    /** Shared polling loop for row and Arrow scan results. */
    private <T> T doPoll(
            Duration timeout,
            Supplier<T> pollForFetches,
            java.util.function.Predicate<T> isEmpty,
            Supplier<T> emptyResult) {
        acquireAndEnsureOpen();
        try {
            if (!logScannerStatus.prepareToPoll()) {
                throw new IllegalStateException("LogScanner is not subscribed any buckets.");
            }

            scannerMetricGroup.recordPollStart(System.currentTimeMillis());
            long timeoutNanos = timeout.toNanos();
            long startNanos = System.nanoTime();
            do {
                T scanRecords = pollForFetches.get();
                if (isEmpty.test(scanRecords)) {
                    try {
                        if (!logFetcher.awaitNotEmpty(startNanos + timeoutNanos)) {
                            // logFetcher waits for the timeout and no data in buffer,
                            // so we return empty
                            return scanRecords;
                        }
                    } catch (WakeupException e) {
                        // wakeup() is called, we need to return empty
                        return scanRecords;
                    }
                } else {
                    // before returning the fetched records, we can send off the next round of
                    // fetches and avoid block waiting for their responses to enable pipelining
                    // while the user is handling the fetched records.
                    logFetcher.sendFetches();
                    return scanRecords;
                }
            } while (System.nanoTime() - startNanos < timeoutNanos);

            return emptyResult.get();
        } finally {
            release();
            scannerMetricGroup.recordPollEnd(System.currentTimeMillis());
        }
    }

    /**
     * Acquire the light lock and ensure that the scanner hasn't been closed.
     *
     * @throws IllegalStateException If the scanner has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (closed) {
            release();
            throw new IllegalStateException("This scanner has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this scanner from multithreaded access. Instead of blocking
     * when the lock is not available, however, we just throw an exception (since multithreaded
     * usage is not supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        Thread thread = Thread.currentThread();
        long threadId = thread.getId();
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
    private void release() {
        if (refCount.decrementAndGet() == 0) {
            currentThread.set(NO_CURRENT_THREAD);
        }
    }

    @Override
    public void close() {
        acquire();
        try {
            if (!closed) {
                LOG.trace("Closing log scanner for table: {}", tablePath);
                scannerMetricGroup.close();
                logFetcher.close();
            }
            LOG.debug("Log scanner for table: {} has been closed", tablePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to close log scanner for table " + tablePath, e);
        } finally {
            closed = true;
            release();
        }
    }
}

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
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.metrics.ScannerMetricGroup;
import org.apache.fluss.client.table.scanner.RemoteFileDownloader;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.WakeupException;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.Projection;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Shared implementation for different log scanner result types. */
@Internal
abstract class AbstractLogScanner<T> {
    private static final long NO_CURRENT_THREAD = -1L;

    protected final TablePath tablePath;
    protected final LogScannerStatus logScannerStatus;
    protected final MetadataUpdater metadataUpdater;
    protected final LogFetcher logFetcher;
    protected final long tableId;
    protected final boolean isPartitionedTable;
    // metrics
    protected final ScannerMetricGroup scannerMetricGroup;

    private final Logger logger;
    private final String scannerName;
    // currentThread holds the threadId of the current thread accessing the scanner
    // and is used to prevent multithreaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refCount is used to allow reentrant access by the thread who has acquired currentThread.
    private final AtomicInteger refCount = new AtomicInteger(0);

    private volatile boolean closed = false;

    protected AbstractLogScanner(
            Configuration conf,
            TableInfo tableInfo,
            MetadataUpdater metadataUpdater,
            ClientMetricGroup clientMetricGroup,
            RemoteFileDownloader remoteFileDownloader,
            @Nullable int[] projectedFields,
            SchemaGetter schemaGetter,
            Logger logger,
            String scannerName) {
        this.tablePath = tableInfo.getTablePath();
        this.tableId = tableInfo.getTableId();
        this.isPartitionedTable = tableInfo.isPartitioned();
        this.logger = logger;
        this.scannerName = scannerName;
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
        }
        return null;
    }

    protected T doPoll(Duration timeout) {
        acquireAndEnsureOpen();
        try {
            if (!logScannerStatus.prepareToPoll()) {
                throw new IllegalStateException("LogScanner is not subscribed any buckets.");
            }

            scannerMetricGroup.recordPollStart(System.currentTimeMillis());
            long timeoutNanos = timeout.toNanos();
            long startNanos = System.nanoTime();
            do {
                T scanRecords = pollForFetches();
                if (isEmpty(scanRecords)) {
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

            return emptyResult();
        } finally {
            release();
            scannerMetricGroup.recordPollEnd(System.currentTimeMillis());
        }
    }

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
            this.metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));
            this.logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

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
            this.metadataUpdater.checkAndUpdatePartitionMetadata(
                    tablePath, Collections.singleton(partitionId));
            this.logScannerStatus.assignScanBuckets(Collections.singletonMap(tableBucket, offset));
        } finally {
            release();
        }
    }

    public void unsubscribe(long partitionId, int bucket) {
        if (!isPartitionedTable) {
            throw new IllegalStateException(
                    "Can't unsubscribe a partition for a non-partitioned table.");
        }
        acquireAndEnsureOpen();
        try {
            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
            this.logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
        } finally {
            release();
        }
    }

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
            this.logScannerStatus.unassignScanBuckets(Collections.singletonList(tableBucket));
        } finally {
            release();
        }
    }

    public void wakeup() {
        logFetcher.wakeup();
    }

    protected abstract T pollForFetches();

    protected abstract T emptyResult();

    protected abstract boolean isEmpty(T result);

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
    private void release() {
        if (refCount.decrementAndGet() == 0) {
            currentThread.set(NO_CURRENT_THREAD);
        }
    }

    public void close() {
        acquire();
        try {
            if (!closed) {
                logger.trace("Closing {} for table: {}", scannerName, tablePath);
                scannerMetricGroup.close();
                logFetcher.close();
            }
            logger.debug("{} for table: {} has been closed", scannerName, tablePath);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to close " + scannerName + " for table " + tablePath, e);
        } finally {
            closed = true;
            release();
        }
    }
}

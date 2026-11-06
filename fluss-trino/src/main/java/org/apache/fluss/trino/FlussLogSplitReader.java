/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.guava32.com.google.common.collect.ImmutableList;

import io.trino.spi.TrinoException;

import java.time.Duration;
import java.util.List;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.apache.fluss.trino.FlussErrorCode.FLUSS_READ_ERROR;
import static org.apache.fluss.utils.ExceptionUtils.firstOrSuppressed;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Bounded log reader for one physical Fluss bucket.
 *
 * <p>The underlying {@link LogScanner} is unbounded. This reader restricts it to the split's
 * exclusive-end offset range and reports {@link PollResult#FINISHED} once that range is consumed.
 */
final class FlussLogSplitReader implements FlussSplitReader {

    private static final long ESTIMATED_RECORD_OVERHEAD_BYTES = 128;

    private final FlussBucketHandle bucket;
    private final TableBucket tableBucket;
    private final FlussLogRange range;

    private final LogScanner scanner;

    private List<ScanRecord> records = ImmutableList.of();
    private int recordIndex;
    private int recordLimit;

    private long estimatedRetainedBytes;
    private long completedBytes;

    private boolean stopReached;
    private boolean finished;
    private boolean closed;

    FlussLogSplitReader(Table table, FlussBucketHandle bucket, FlussLogRange range) {
        checkNotNull(table, "table is null");
        this.bucket = checkNotNull(bucket, "bucket is null");
        this.range = checkNotNull(range, "range is null");
        this.tableBucket = bucket.toTableBucket();

        if (range.isEmpty()) {
            scanner = null;
            finished = true;
            return;
        }

        LogScanner scanner = table.newScan().createLogScanner();

        try {
            if (bucket.isPartitioned()) {
                scanner.subscribe(
                        bucket.getRequiredPartitionId(),
                        bucket.getBucketId(),
                        range.getStartOffset());
            } else {
                scanner.subscribe(bucket.getBucketId(), range.getStartOffset());
            }
        } catch (RuntimeException | Error failure) {
            try {
                scanner.close();
            } catch (Throwable closeFailure) {
                firstOrSuppressed(closeFailure, failure);
            }
            throw failure;
        }

        this.scanner = scanner;
    }

    @Override
    public PollResult poll(Duration timeout) {
        checkNotNull(timeout, "timeout is null");

        if (closed || finished) {
            return PollResult.FINISHED;
        }

        if (hasNext()) {
            throw new IllegalStateException(
                    "Cannot poll Fluss log scanner while the current batch is not consumed");
        }

        ScanRecords batch;
        try {
            batch = scanner.poll(timeout);
        } catch (RuntimeException e) {
            throw new TrinoException(FLUSS_READ_ERROR, "Failed reading Fluss log for " + bucket, e);
        }
        List<ScanRecord> batchRecords = batch.records(tableBucket);

        long retainedBytes = 0;
        int validRecordCount = 0;
        boolean reachesStop = false;

        for (ScanRecord record : batchRecords) {
            long recordBytes = Math.max(0, record.getSizeInBytes());

            // This is an estimate. LogScanner does not expose all client-side retained buffers.
            retainedBytes += ESTIMATED_RECORD_OVERHEAD_BYTES + recordBytes;
            long offset = record.logOffset();
            if (offset < range.getStartOffset()) {
                throw new TrinoException(
                        GENERIC_INTERNAL_ERROR,
                        "Fluss offset invariant failed for "
                                + bucket
                                + ": offset "
                                + offset
                                + " precedes "
                                + range);
            }

            /*
             * Scan records are ordered for a bucket. Once the exclusive stopping offset is
             * reached, no later record belongs to this split.
             */
            if (!reachesStop) {
                if (offset >= range.getStoppingOffset()) {
                    reachesStop = true;
                } else {
                    validRecordCount++;

                    if (offset == range.getStoppingOffset() - 1) {
                        reachesStop = true;
                    }
                }
            }
        }

        /*
         * The scanner may make progress through the stopping offset without returning a record at
         * exactly stoppingOffset - 1. Preserve the progress-based termination used by the existing
         * reader.
         */
        Long progress = batch.consumedUpToOffset(tableBucket);
        if (progress != null && progress >= range.getStoppingOffset()) {
            reachesStop = true;
        }

        records = batchRecords;
        recordIndex = 0;
        recordLimit = validRecordCount;
        estimatedRetainedBytes = retainedBytes;
        stopReached = stopReached || reachesStop;

        if (recordLimit == 0) {
            clearBatch();

            if (stopReached) {
                finished = true;
                return PollResult.FINISHED;
            }

            return PollResult.YIELD;
        }

        return PollResult.AVAILABLE;
    }

    @Override
    public boolean hasNext() {
        return recordIndex < recordLimit;
    }

    @Override
    public InternalRow next() {
        if (!hasNext()) {
            throw new IllegalStateException("No buffered Fluss log row");
        }

        ScanRecord record = records.get(recordIndex++);
        completedBytes += Math.max(0, record.getSizeInBytes());

        InternalRow row = record.getRow();

        if (recordIndex == recordLimit) {
            clearBatch();

            if (stopReached) {
                finished = true;
            }
        }

        return row;
    }

    @Override
    public boolean isFinished() {
        return finished && !hasNext();
    }

    @Override
    public long getRetainedSizeInBytes() {
        return estimatedRetainedBytes;
    }

    @Override
    public long getCompletedBytes() {
        return completedBytes;
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        closed = true;
        finished = true;
        clearBatch();

        if (scanner != null) {
            scanner.close();
        }
    }

    private void clearBatch() {
        records = ImmutableList.of();
        recordIndex = 0;
        recordLimit = 0;
        estimatedRetainedBytes = 0;
    }
}

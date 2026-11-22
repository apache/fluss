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
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import io.trino.spi.TrinoException;

import java.io.IOException;
import java.time.Duration;

import static org.apache.fluss.trino.FlussErrorCode.FLUSS_READ_ERROR;
import static org.apache.fluss.utils.ExceptionUtils.firstOrSuppressed;
import static org.apache.fluss.utils.ExceptionUtils.rethrowException;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Bounded current-state reader for one primary-key table bucket.
 *
 * <p>The Fluss batch scanner establishes a bucket snapshot lazily. An empty batch yields to Trino,
 * while a null batch marks the end of the snapshot.
 */
final class FlussKvSplitReader implements FlussSplitReader {

    private final FlussBucketHandle bucket;
    private final BatchScanner scanner;

    private CloseableIterator<InternalRow> records;

    private boolean finished;
    private boolean closed;

    FlussKvSplitReader(Table table, FlussBucketHandle bucket) {
        checkNotNull(table, "table is null");
        this.bucket = checkNotNull(bucket, "bucket is null");
        scanner = table.newScan().createBatchScanner(bucket.toTableBucket());
    }

    @Override
    public PollResult poll(Duration timeout) {
        checkNotNull(timeout, "timeout is null");

        if (closed || finished) {
            return PollResult.FINISHED;
        }

        if (records != null) {
            throw new IllegalStateException(
                    "Cannot poll Fluss KV scanner while the current batch is not consumed");
        }

        try {
            records = scanner.pollBatch(timeout);
        } catch (IOException e) {
            throw new TrinoException(
                    FLUSS_READ_ERROR, "Failed reading Fluss KV snapshot for " + bucket, e);
        }

        if (records == null) {
            finished = true;
            return PollResult.FINISHED;
        }

        if (!records.hasNext()) {
            closeBatch();
            return PollResult.YIELD;
        }

        return PollResult.AVAILABLE;
    }

    @Override
    public boolean hasNext() {
        return records != null;
    }

    @Override
    public InternalRow next() {
        if (records == null) {
            throw new IllegalStateException("No buffered Fluss KV row");
        }

        InternalRow row = records.next();

        // Keep records non-null iff another row can be consumed without polling.
        if (!records.hasNext()) {
            closeBatch();
        }

        return row;
    }

    @Override
    public boolean isFinished() {
        return finished && records == null;
    }

    @Override
    public long getRetainedSizeInBytes() {
        /*
         * BatchScanner does not expose the retained size of its materialized row batch or
         * in-flight continuation request. PageBuilder memory is accounted separately by
         * FlussPageSource.
         */
        return 0;
    }

    @Override
    public long getCompletedBytes() {
        // The KV batch API does not expose encoded byte counts for consumed rows.
        return 0;
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        closed = true;
        finished = true;
        CloseableIterator<InternalRow> recordsToClose = records;
        records = null;
        Throwable failure = null;

        if (recordsToClose != null) {
            try {
                recordsToClose.close();
            } catch (Exception | Error e) {
                failure = firstOrSuppressed(e, failure);
            }
        }

        try {
            scanner.close();
        } catch (Exception | Error e) {
            failure = firstOrSuppressed(e, failure);
        }

        if (failure != null) {
            rethrowException(failure, "Failed closing Fluss KV reader for " + bucket);
        }
    }

    private void closeBatch() {
        CloseableIterator<InternalRow> recordsToClose = records;
        records = null;

        if (recordsToClose != null) {
            recordsToClose.close();
        }
    }
}

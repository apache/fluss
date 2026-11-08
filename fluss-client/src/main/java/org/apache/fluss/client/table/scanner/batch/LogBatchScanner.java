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

package org.apache.fluss.client.table.scanner.batch;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Bounded log scan that counts actual rows and advances across empty batches. */
@Internal
public final class LogBatchScanner implements BatchScanner {
    private final LogScanner scanner;
    private final TableBucket bucket;
    private final CompletableFuture<Long> stoppingOffsetFuture;
    private long remaining;
    private long stoppingOffset;
    private boolean initialized;
    private boolean finished;

    /** Creates a scanner over the retained log prefix captured by the supplied end offset. */
    public LogBatchScanner(
            LogScanner scanner,
            TableBucket bucket,
            CompletableFuture<Long> stoppingOffsetFuture,
            long limit) {
        this.scanner = scanner;
        this.bucket = bucket;
        this.stoppingOffsetFuture = stoppingOffsetFuture;
        this.remaining = limit;
    }

    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
        if (finished || remaining == 0) {
            return null;
        }
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeout.toMillis());
        long startNanos = System.nanoTime();
        if (!initialized) {
            try {
                stoppingOffset = stoppingOffsetFuture.get(timeoutNanos, TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                return CloseableIterator.emptyIterator();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (ExecutionException e) {
                throw new IOException("Unable to capture bounded log end offset.", e.getCause());
            }
            if (stoppingOffset == 0) {
                finished = true;
                return null;
            }
            if (bucket.getPartitionId() == null) {
                scanner.subscribeFromBeginning(bucket.getBucket());
            } else {
                scanner.subscribeFromBeginning(bucket.getPartitionId(), bucket.getBucket());
            }
            initialized = true;
        }
        ScanRecords records =
                scanner.poll(
                        Duration.ofNanos(
                                Math.max(0, timeoutNanos - (System.nanoTime() - startNanos))));
        List<InternalRow> rows = new ArrayList<>();
        for (ScanRecord record : records.records(bucket)) {
            if (record.logOffset() >= stoppingOffset) {
                finished = true;
                break;
            }
            rows.add(record.getRow());
            if (--remaining == 0) {
                finished = true;
                break;
            }
        }
        Long progress = records.consumedUpToOffset(bucket);
        if (progress != null && progress >= stoppingOffset) {
            finished = true;
        }
        return CloseableIterator.wrap(rows.iterator());
    }

    @Override
    public void close() throws IOException {
        finished = true;
        try {
            scanner.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}

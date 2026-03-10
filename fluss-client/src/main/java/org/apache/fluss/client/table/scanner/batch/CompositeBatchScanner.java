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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

import static org.apache.fluss.client.table.scanner.batch.BatchScanUtils.collectLimitedRows;

/**
 * A {@link BatchScanner} that combines multiple {@link BatchScanner} instances into a single
 * scanner. It polls the underlying scanners in a round-robin fashion: each {@link
 * #pollBatch(Duration)} call is delegated to the next scanner in the queue, and scanners that still
 * have data are re-enqueued while exhausted scanners are closed and removed.
 *
 * <p>When a {@code limit} is specified, rows are collected eagerly across all underlying scanners
 * up to that limit and returned in a single batch.
 */
@Internal
public class CompositeBatchScanner implements BatchScanner {

    /** Queue of underlying scanners to be polled in order. */
    private final LinkedList<BatchScanner> scannerQueue;

    /** Optional row limit; when set, rows are collected eagerly up to this count. */
    private final @Nullable Integer limit;

    public CompositeBatchScanner(List<BatchScanner> scanners, @Nullable Integer limit) {
        this.scannerQueue = new LinkedList<>(scanners);
        this.limit = limit;
    }

    @Override
    public void close() throws IOException {
        // Ensure all scanners are closed on failure to avoid resource leaks
        scannerQueue.forEach(IOUtils::closeQuietly);
        scannerQueue.clear();
    }

    @Nullable
    @Override
    public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {

        while (!scannerQueue.isEmpty()) {
            // Direct return limit scan which don't have so much data.
            if (limit != null) {
                CloseableIterator<InternalRow> iterator =
                        CloseableIterator.wrap(collectLimitedRows(scannerQueue, limit).iterator());
                scannerQueue.clear();
                return iterator;
            }

            BatchScanner scanner = scannerQueue.poll();
            try {
                CloseableIterator<InternalRow> iterator = scanner.pollBatch(timeout);
                if (iterator != null) {
                    // If the scanner has more data, add it back to the queue
                    scannerQueue.add(scanner);
                    return iterator;
                } else {
                    // Close the scanner if it has no more data, and not add it back to the queue
                    scanner.close();
                }
            } catch (Exception e) {
                 scannerQueue.add(scanner);
                throw new IOException("Failed to collect rows", e);
            }
        }
        return null;
    }
}

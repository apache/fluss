/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner.batch;

import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.utils.CloseableIterator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/** Utility class for batch scan. */
public class BatchScanUtils {

    private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(10);

    /** Collect all rows from the scanner. */
    public static List<InternalRow> collectRows(BatchScanner scanner) {
        List<InternalRow> rows = new ArrayList<>();
        while (true) {
            try {
                CloseableIterator<InternalRow> iterator = scanner.pollBatch(DEFAULT_POLL_TIMEOUT);
                if (iterator == null) {
                    scanner.close();
                    break;
                }
                rows.addAll(toList(iterator));
            } catch (Exception e) {
                throw new RuntimeException("Failed to collect rows", e);
            }
        }
        return rows;
    }

    /** Collect all rows from the scanners. */
    public static List<InternalRow> collectAllRows(List<BatchScanner> scanners) {
        List<InternalRow> rows = new ArrayList<>();
        for (BatchScanner scanner : scanners) {
            rows.addAll(collectRows(scanner));
        }
        return rows;
    }

    /** Collect rows from the scanners until the number of rows reaches the limit. */
    public static List<InternalRow> collectLimitedRows(List<BatchScanner> scanners, int limit) {
        List<InternalRow> rows = new ArrayList<>();
        Queue<BatchScanner> scannerQueue = new LinkedList<>(scanners);
        while (!scannerQueue.isEmpty()) {
            BatchScanner scanner = scannerQueue.poll();
            try {
                CloseableIterator<InternalRow> iterator = scanner.pollBatch(DEFAULT_POLL_TIMEOUT);
                if (iterator != null) {
                    rows.addAll(toList(iterator));
                    // If the scanner has more data, add it back to the queue
                    scannerQueue.add(scanner);
                } else {
                    // Close the scanner if it has no more data, and not add it back to the queue
                    scanner.close();
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to collect rows", e);
            }
            if (rows.size() >= limit) {
                break;
            }
        }
        // may collect enough rows before drain all scanners, close all scanners in the queue
        for (BatchScanner scanner : scannerQueue) {
            try {
                scanner.close();
            } catch (Exception e) {
                throw new RuntimeException("Failed to close scanner", e);
            }
        }
        return rows;
    }

    /**
     * Collect all rows from the iterator and close it.
     *
     * @return return a list of rows
     */
    public static <T> List<T> toList(CloseableIterator<T> iterator) {
        List<T> rows = new ArrayList<>();
        while (iterator.hasNext()) {
            rows.add(iterator.next());
        }
        iterator.close();
        return rows;
    }
}

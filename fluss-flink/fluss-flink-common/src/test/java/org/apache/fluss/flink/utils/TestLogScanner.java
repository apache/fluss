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

package org.apache.fluss.flink.utils;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test implementation of {@link LogScanner} for testing.
 *
 * <p>Allows pre-configuring records per bucket and returns them on poll(). Supports:
 *
 * <ul>
 *   <li>Simulating empty polls for testing exponential backoff behavior
 *   <li>Batch size control for realistic multi-poll scenarios
 *   <li>Always-empty mode for testing fatal exception on max empty polls
 * </ul>
 */
public class TestLogScanner implements LogScanner {
    private final Map<TableBucket, List<ScanRecord>> recordsByBucket = new HashMap<>();
    private final Map<TableBucket, AtomicInteger> pollIndexByBucket = new HashMap<>();

    /** Number of empty polls to return before returning actual records. */
    private int emptyPollsBeforeData = 0;

    private int currentEmptyPollCount = 0;

    /** If true, always return empty records (for testing fatal exception on max empty polls). */
    private boolean alwaysReturnEmpty = false;

    /** Maximum number of records to return per bucket per poll (0 = unlimited). */
    private int batchSize = 0;

    public void setRecordsForBucket(TableBucket bucket, List<ScanRecord> records) {
        recordsByBucket.put(bucket, new ArrayList<>(records));
        pollIndexByBucket.put(bucket, new AtomicInteger(0));
    }

    /**
     * Sets whether to always return empty records (for testing fatal exception).
     *
     * @param alwaysEmpty if true, poll() always returns empty
     */
    public void setAlwaysReturnEmpty(boolean alwaysEmpty) {
        this.alwaysReturnEmpty = alwaysEmpty;
    }

    /**
     * Sets the maximum number of records to return per bucket per poll.
     *
     * <p>This simulates realistic behavior where LogScanner returns records in batches. Use this to
     * test multi-poll scenarios and ensure the executor handles partial results correctly.
     *
     * @param batchSize max records per bucket per poll (0 = unlimited, returns all remaining)
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public ScanRecords poll(Duration timeout) {
        // If configured to always return empty, do so
        if (alwaysReturnEmpty) {
            return ScanRecords.EMPTY;
        }

        // Return empty polls if configured
        if (currentEmptyPollCount < emptyPollsBeforeData) {
            currentEmptyPollCount++;
            return ScanRecords.EMPTY;
        }

        Map<TableBucket, List<ScanRecord>> result = new HashMap<>();

        for (Map.Entry<TableBucket, List<ScanRecord>> entry : recordsByBucket.entrySet()) {
            TableBucket bucket = entry.getKey();
            List<ScanRecord> allRecords = entry.getValue();
            AtomicInteger index = pollIndexByBucket.get(bucket);

            if (index.get() < allRecords.size()) {
                int startIndex = index.get();
                int endIndex;
                if (batchSize > 0) {
                    // Return at most batchSize records
                    endIndex = Math.min(startIndex + batchSize, allRecords.size());
                } else {
                    // Return all remaining records
                    endIndex = allRecords.size();
                }
                List<ScanRecord> batch = allRecords.subList(startIndex, endIndex);
                result.put(bucket, new ArrayList<>(batch));
                index.set(endIndex);
            }
        }

        return result.isEmpty() ? ScanRecords.EMPTY : new ScanRecords(result);
    }

    @Override
    public void subscribe(int bucket, long offset) {}

    @Override
    public void subscribe(long partitionId, int bucket, long offset) {}

    @Override
    public void unsubscribe(long partitionId, int bucket) {}

    @Override
    public void unsubscribe(int bucket) {}

    @Override
    public void wakeup() {}

    @Override
    public void close() {}

    public void reset() {
        recordsByBucket.clear();
        pollIndexByBucket.clear();
        emptyPollsBeforeData = 0;
        currentEmptyPollCount = 0;
        alwaysReturnEmpty = false;
        batchSize = 0;
    }
}

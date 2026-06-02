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

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.utils.CloseableIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A completed fetch backed by log records read from a lake snapshot. */
class LakeCompletedFetch extends CompletedFetch {

    private final CloseableIterator<LogRecord> records;
    private final long endOffset;

    private long nextFetchOffset;
    private boolean consumed;

    LakeCompletedFetch(
            TableBucket tableBucket,
            CloseableIterator<LogRecord> records,
            long fetchOffset,
            long endOffset,
            long highWatermark,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus) {
        super(
                tableBucket,
                ApiError.NONE,
                0,
                highWatermark,
                Collections.emptyIterator(),
                readContext,
                logScannerStatus,
                false,
                fetchOffset,
                NO_FILTERED_END_OFFSET);
        this.records = records;
        this.nextFetchOffset = fetchOffset;
        this.endOffset = endOffset;
    }

    @Override
    boolean isConsumed() {
        return consumed;
    }

    @Override
    long nextFetchOffset() {
        return nextFetchOffset;
    }

    @Override
    void drain() {
        if (!consumed) {
            records.close();
            consumed = true;
        }
        super.drain();
    }

    @Override
    public List<ScanRecord> fetchRecords(int maxRecords) {
        if (consumed) {
            return Collections.emptyList();
        }

        List<ScanRecord> scanRecords = new ArrayList<>();
        try {
            for (int i = 0; i < maxRecords && records.hasNext(); ) {
                LogRecord record = records.next();
                if (record.logOffset() < nextFetchOffset) {
                    continue;
                }
                if (record.logOffset() >= endOffset) {
                    nextFetchOffset = Math.max(nextFetchOffset, endOffset);
                    drain();
                    break;
                }

                scanRecords.add(toScanRecord(record));
                nextFetchOffset = record.logOffset() + 1;
                i++;
            }

            if (!records.hasNext()) {
                nextFetchOffset = Math.max(nextFetchOffset, endOffset);
                drain();
            }
        } catch (Exception e) {
            if (scanRecords.isEmpty()) {
                throw new FetchException(
                        "Received exception when fetching lake records from " + tableBucket, e);
            }
        }

        return scanRecords;
    }
}

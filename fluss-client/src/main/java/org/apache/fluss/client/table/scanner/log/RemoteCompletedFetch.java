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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.rpc.protocol.ApiError;

/**
 * {@link RemoteCompletedFetch} is a {@link CompletedFetch} that represents a completed fetch that
 * the log records are fetched from remote log storage as file-backed or in-memory chunks.
 */
@Internal
class RemoteCompletedFetch extends CompletedFetch {

    // recycle to notify the downloader that this chunk has been consumed
    private final Runnable recycleCallback;

    RemoteCompletedFetch(
            TableBucket tableBucket,
            LogRecords logRecords,
            long highWatermark,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean isCheckCrc,
            long fetchOffset,
            Runnable recycleCallback) {
        super(
                tableBucket,
                ApiError.NONE,
                logRecords.sizeInBytes(),
                highWatermark,
                logRecords.batches().iterator(),
                readContext,
                logScannerStatus,
                isCheckCrc,
                fetchOffset,
                CompletedFetch.NO_FILTERED_END_OFFSET);
        this.recycleCallback = recycleCallback;
    }

    @Override
    void drain() {
        // Guard against double-drain: super.drain() is idempotent (checks isConsumed internally),
        // but recycleCallback must only be called once to avoid double-incrementing chunksConsumed
        // which would trigger a spurious semaphore release and corrupt flow-control counters.
        if (isConsumed()) {
            return;
        }
        super.drain();
        // call recycle to notify the downloader and trigger next chunk read
        recycleCallback.run();
    }
}

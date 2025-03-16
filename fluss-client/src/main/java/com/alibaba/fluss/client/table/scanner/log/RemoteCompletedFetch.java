/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.FileLogRecords;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.rpc.protocol.ApiError;

import java.io.IOException;

/**
 * {@link RemoteCompletedFetch} is a {@link CompletedFetch} that represents a completed fetch that
 * the log records are fetched from remote log storage.
 */
@Internal
class RemoteCompletedFetch extends CompletedFetch {

    private final FileLogRecords fileLogRecords;

    // recycle to clean up the fetched remote log files and increment the prefetch semaphore
    private final Runnable recycleCallback;

    RemoteCompletedFetch(
            TableBucket tableBucket,
            FileLogRecords fileLogRecords,
            long highWatermark,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean isCheckCrc,
            long fetchOffset,
            Runnable recycleCallback) {
        super(
                tableBucket,
                ApiError.NONE,
                fileLogRecords.sizeInBytes(),
                highWatermark,
                fileLogRecords.batches().iterator(),
                readContext,
                logScannerStatus,
                isCheckCrc,
                fetchOffset);
        this.fileLogRecords = fileLogRecords;
        this.recycleCallback = recycleCallback;
    }

    @Override
    void drain() {
        super.drain();
        // close file channel only, don't need to flush the file which is very heavy
        try {
            fileLogRecords.closeHandlers();
        } catch (IOException e) {
            LOG.warn("Failed to close file channel for remote log records", e);
        }
        // call recycle to remove the fetched files and increment the prefetch semaphore
        recycleCallback.run();
    }
}

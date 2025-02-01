/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.FileLogRecords;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.remote.RemoteLogSegment;

/**
 * {@link RemotePendingFetch} is a {@link PendingFetch} that represents a pending fetch that waiting
 * for the remote log file fetched to local disk.
 */
class RemotePendingFetch implements PendingFetch {

    private final RemoteLogSegment remoteLogSegment;
    private final RemoteLogDownloadFuture downloadFuture;

    private final int posInLogSegment;
    private final long fetchOffset;
    private final long highWatermark;
    private final LogRecordReadContext readContext;
    private final LogScannerStatus logScannerStatus;
    private final boolean isCheckCrc;

    RemotePendingFetch(
            RemoteLogSegment remoteLogSegment,
            RemoteLogDownloadFuture downloadFuture,
            int posInLogSegment,
            long fetchOffset,
            long highWatermark,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean isCheckCrc) {
        this.remoteLogSegment = remoteLogSegment;
        this.downloadFuture = downloadFuture;
        this.posInLogSegment = posInLogSegment;
        this.fetchOffset = fetchOffset;
        this.highWatermark = highWatermark;
        this.readContext = readContext;
        this.logScannerStatus = logScannerStatus;
        this.isCheckCrc = isCheckCrc;
    }

    @Override
    public TableBucket tableBucket() {
        return remoteLogSegment.tableBucket();
    }

    @Override
    public boolean isCompleted() {
        return downloadFuture.isDone();
    }

    @Override
    public CompletedFetch toCompletedFetch() {
        FileLogRecords fileLogRecords = downloadFuture.getFileLogRecords(posInLogSegment);
        return new RemoteCompletedFetch(
                remoteLogSegment.tableBucket(),
                fileLogRecords,
                highWatermark,
                readContext,
                logScannerStatus,
                isCheckCrc,
                fetchOffset,
                downloadFuture.getRecycleCallback());
    }
}

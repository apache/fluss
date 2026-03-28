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
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import javax.annotation.Nullable;

/**
 * {@link DefaultCompletedFetch} is a {@link CompletedFetch} that represents a completed fetch that
 * the log records have been returned from the tablet server by {@link FetchLogRequest}.
 */
@Internal
class DefaultCompletedFetch extends CompletedFetch {

    /**
     * The parsed ByteBuf backing the lazily-parsed records data. This reference is retained to keep
     * the underlying network buffer alive while the records are being consumed. Released in {@link
     * #drain()}.
     */
    @Nullable private ByteBuf parsedByteBuf;

    public DefaultCompletedFetch(
            TableBucket tableBucket,
            FetchLogResultForBucket fetchLogResultForBucket,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean isCheckCrc,
            Long fetchOffset,
            @Nullable ByteBuf parsedByteBuf) {
        super(
                tableBucket,
                fetchLogResultForBucket.getError(),
                fetchLogResultForBucket.recordsOrEmpty().sizeInBytes(),
                fetchLogResultForBucket.getHighWatermark(),
                fetchLogResultForBucket.recordsOrEmpty().batches().iterator(),
                readContext,
                logScannerStatus,
                isCheckCrc,
                fetchOffset);
        this.parsedByteBuf = parsedByteBuf;
    }

    @Override
    void drain() {
        try {
            super.drain();
        } finally {
            if (parsedByteBuf != null) {
                parsedByteBuf.release();
                parsedByteBuf = null;
            }
        }
    }
}

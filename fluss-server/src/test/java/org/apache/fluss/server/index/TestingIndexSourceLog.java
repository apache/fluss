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

package org.apache.fluss.server.index;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

/** In-memory source log with explicit read data, committed progress and local/remote boundary. */
final class TestingIndexSourceLog implements IndexSourceReader.SourceLog {

    private final TableBucket bucket;
    private final LongSupplier highWatermark;
    private final long localStart;
    private final LongFunction<LogRecords> records;
    private final AtomicInteger reads;

    TestingIndexSourceLog(
            TableBucket bucket,
            LongSupplier highWatermark,
            long localStart,
            LongFunction<LogRecords> records,
            AtomicInteger reads) {
        this.bucket = bucket;
        this.highWatermark = highWatermark;
        this.localStart = localStart;
        this.records = records;
        this.reads = reads;
    }

    @Override
    public TableBucket tableBucket() {
        return bucket;
    }

    @Override
    public long highWatermark() {
        return highWatermark.getAsLong();
    }

    @Override
    public long logStartOffset() {
        return 0L;
    }

    @Override
    public long localLogStartOffset() {
        return localStart;
    }

    @Override
    public FetchDataInfo read(
            long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage) {
        reads.incrementAndGet();
        return new FetchDataInfo(records.apply(offset));
    }
}

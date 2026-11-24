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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.index.IndexSourceReader.RemoteFetcher;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.function.FunctionWithException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.assertj.core.api.Assertions.assertThat;

/** In-memory source log with explicit read data, committed progress and local/remote boundary. */
final class TestingIndexSourceLog implements IndexSourceReader.SourceLog {

    static final Supplier<RemoteFetcher> LOCAL_WAL_ONLY =
            () -> {
                throw new AssertionError("Unexpected remote WAL read in a local WAL test");
            };

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

    IndexReplicator registerReplicator(
            IndexReplicationScheduler scheduler,
            IndexSendBuffer buffer,
            LogRecordReadContext context,
            List<IndexReplicationEncoder> indexes) {
        IndexReplicator source =
                new IndexReplicator(
                        new IndexSourceReader(this, LOCAL_WAL_ONLY, Runnable::run),
                        indexes,
                        buffer,
                        context,
                        0L,
                        1024,
                        (sync, all) -> {},
                        scheduler,
                        () -> {},
                        IndexReplicationManager.createRetryBackoff(
                                ConfigOptions.INDEX_REPLICATION_RETRY_BACKOFF_MAX
                                        .defaultValue()
                                        .toMillis()),
                        SystemClock.getInstance(),
                        null,
                        ConfigOptions.NETTY_SERVER_MAX_REQUEST_SIZE.defaultValue().getBytes());
        scheduler.register(0, source);
        return source;
    }

    static MemoryLogRecords sourceRecords(RowType rowType, long offset) throws Exception {
        return createBasicMemoryLogRecords(
                rowType,
                1,
                offset,
                0L,
                CURRENT_LOG_MAGIC_VALUE,
                NO_WRITER_ID,
                NO_BATCH_SEQUENCE,
                Arrays.asList(ChangeType.INSERT, ChangeType.INSERT),
                Arrays.asList(new Object[] {1}, new Object[] {2}),
                LogFormat.INDEXED,
                DEFAULT_COMPRESSION);
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
    public long localLogStartOffset() {
        return localStart;
    }

    @Override
    public FetchDataInfo read(
            long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage) {
        reads.incrementAndGet();
        return new FetchDataInfo(records.apply(offset));
    }

    /** Opens independent sessions and rejects reads after a session is closed. */
    static final class RemoteSessions implements Supplier<RemoteFetcher> {
        private final FunctionWithException<Long, Iterable<LogRecordBatch>, IOException> read;
        private final AtomicInteger opened = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();

        RemoteSessions(FunctionWithException<Long, Iterable<LogRecordBatch>, IOException> read) {
            this.read = read;
        }

        @Override
        public RemoteFetcher get() {
            opened.incrementAndGet();
            return new RemoteFetcher() {
                private boolean sessionClosed;

                @Override
                public Iterable<LogRecordBatch> fetch(long startOffset, long localLogStartOffset)
                        throws IOException {
                    assertThat(sessionClosed).as("fetch from a closed WAL session").isFalse();
                    return read.apply(startOffset);
                }

                @Override
                public void close() {
                    assertThat(sessionClosed).as("WAL session closed twice").isFalse();
                    sessionClosed = true;
                    closed.incrementAndGet();
                }
            };
        }

        void assertClosedSessions(int count) {
            assertThat(opened).as("opened WAL sessions").hasValue(count);
            assertThat(closed).as("closed WAL sessions").hasValue(count);
        }
    }
}

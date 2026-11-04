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

import org.apache.fluss.metadata.IndexVisibility;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultLogRecordBatch;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.assertj.core.api.Assertions.assertThat;

class IndexReplicatorTest {

    @Test
    void slowerIndexAcknowledgementDoesNotClearFailureAtALaterOffset() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords first = sourceRecords(rowType, 0);
        MemoryLogRecords second = sourceRecords(rowType, 2);
        AtomicLong highWatermark = new AtomicLong(2);
        AtomicInteger reads = new AtomicInteger();
        AtomicBoolean failed = new AtomicBoolean();
        IndexSourceReader.SourceLog log =
                new IndexSourceReader.SourceLog() {
                    @Override
                    public TableBucket tableBucket() {
                        return new TableBucket(1L, 0);
                    }

                    @Override
                    public long highWatermark() {
                        return highWatermark.get();
                    }

                    @Override
                    public long logStartOffset() {
                        return 0L;
                    }

                    @Override
                    public FetchDataInfo read(
                            long offset,
                            int maxBytes,
                            FetchIsolation isolation,
                            boolean minOneMessage) {
                        reads.incrementAndGet();
                        return new FetchDataInfo(offset == 0 ? first : second);
                    }
                };
        IndexSpec delegate = indexSpec("fast", 2L, new AtomicInteger());
        IndexSpec fast =
                new IndexSpec(
                        "fast",
                        IndexVisibility.SYNC,
                        2L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        row -> {
                            if (failed.get()) {
                                throw new IllegalStateException("index needs repair");
                            }
                            return delegate.encodeEntry(row);
                        },
                        delegate::encodeProgress);
        IndexSendBuffer buffer = new IndexSendBuffer();
        ManualClock clock = new ManualClock();
        try (IndexReplicator replicator =
                replicator(
                        log,
                        buffer,
                        clock,
                        Arrays.asList(fast, indexSpec("slow", 3L, new AtomicInteger())))) {
            assertThat(replicator.poll()).isTrue();
            acknowledge(buffer, new TableBucket(2L, 0));
            highWatermark.set(4);
            failed.set(true);
            assertThat(replicator.poll()).isFalse();
            assertThat(replicator.isFailed()).isTrue();
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.isFailed()).isTrue();
            assertThat(replicator.poll()).isFalse();
            assertThat(reads).hasValue(2);
            failed.set(false);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
            acknowledge(buffer, new TableBucket(2L, 0));
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(4L);
            assertThat(replicator.isFailed()).isFalse();
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void sourceGapResumesAtTheSameOffsetAfterRepair() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords valid = sourceRecords(rowType, 0);
        MemoryLogRecords gapped = sourceRecords(rowType, 1);
        AtomicBoolean repaired = new AtomicBoolean();
        AtomicInteger reads = new AtomicInteger();
        IndexSourceReader.SourceLog log = sourceLog(() -> repaired.get() ? valid : gapped, reads);
        ManualClock clock = new ManualClock();
        IndexSendBuffer buffer = new IndexSendBuffer();
        try (IndexReplicator replicator =
                replicator(
                        log,
                        buffer,
                        clock,
                        Collections.singletonList(indexSpec("idx", 2L, new AtomicInteger())))) {
            assertThat(replicator.poll()).isFalse();
            assertThat(replicator.isFailed()).isTrue();
            assertThat(reads).hasValue(1);
            repaired.set(true);
            assertThat(replicator.poll()).isFalse();
            assertThat(reads).hasValue(1);
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(replicator.isFailed()).isFalse();
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void oversizedWindowReleasesConstructionAndRemainsRetryable() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords records = sourceRecords(rowType, 0);
        AtomicInteger reads = new AtomicInteger();
        IndexSourceReader.SourceLog log = sourceLog(() -> records, reads);
        ManualClock clock = new ManualClock();
        IndexSpec delegate = indexSpec("idx", 2L, new AtomicInteger());
        IndexSpec fanout =
                new IndexSpec(
                        "idx",
                        IndexVisibility.SYNC,
                        2L,
                        1,
                        KvFormat.COMPACTED,
                        new int[] {0},
                        row -> {
                            IndexSpec.IndexEntry entry = delegate.encodeEntry(row);
                            return new IndexSpec.IndexEntry(
                                    entry.key(), entry.value(), row.getInt(0));
                        },
                        delegate::encodeProgress);
        IndexSendBuffer small = new IndexSendBuffer(4096, 4096);
        try (IndexReplicator replicator =
                replicator(log, small, clock, Collections.singletonList(fanout))) {
            assertThat(replicator.poll()).isFalse();
            assertThat(replicator.isFailed()).isTrue();
            assertThat(small.pendingBytes()).isZero();
            assertThat(small.buckets()).isEmpty();
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isFalse();
            assertThat(reads).hasValue(2);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            assertThat(small.pendingBytes()).isZero();
        }
        IndexSendBuffer larger = new IndexSendBuffer(4096, 8192);
        try (IndexReplicator replicator =
                replicator(log, larger, clock, Collections.singletonList(fanout))) {
            assertThat(replicator.poll()).isTrue();
            assertThat(larger.pendingBytes()).isEqualTo(8192);
            acknowledge(larger, new TableBucket(2L, 1));
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(larger, new TableBucket(2L, 2));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(larger.pendingBytes()).isZero();
        }
    }

    @Test
    void indexesSharingAReadProgressWhenOnlyOneWindowFits() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords records = sourceRecords(rowType, 0);
        IndexSendBuffer buffer = new IndexSendBuffer(4096, 4096);
        ManualClock clock = new ManualClock();
        try (IndexReplicator replicator =
                replicator(
                        sourceLog(() -> records, new AtomicInteger()),
                        buffer,
                        clock,
                        Arrays.asList(
                                indexSpec("first", 2L, new AtomicInteger()),
                                indexSpec("second", 3L, new AtomicInteger())))) {
            assertThat(replicator.poll()).isTrue();
            assertThat(buffer.pendingBytes()).isEqualTo(4096);
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            clock.advanceTime(10, TimeUnit.MILLISECONDS);
            assertThat(replicator.poll()).isTrue();
            acknowledge(buffer, new TableBucket(3L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    private static MemoryLogRecords sourceRecords(RowType rowType, long offset) throws Exception {
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

    private static IndexSourceReader.SourceLog sourceLog(
            Supplier<LogRecords> records, AtomicInteger reads) {
        return new IndexSourceReader.SourceLog() {
            @Override
            public TableBucket tableBucket() {
                return new TableBucket(1L, 0);
            }

            @Override
            public long highWatermark() {
                return 2L;
            }

            @Override
            public long logStartOffset() {
                return 0L;
            }

            @Override
            public FetchDataInfo read(
                    long offset, int maxBytes, FetchIsolation isolation, boolean minOneMessage) {
                reads.incrementAndGet();
                return new FetchDataInfo(records.get());
            }
        };
    }

    private static IndexReplicator replicator(
            IndexSourceReader.SourceLog log,
            IndexSendBuffer buffer,
            ManualClock clock,
            java.util.List<IndexSpec> indexes) {
        LogRecordReadContext context =
                LogRecordReadContext.createIndexedReadContext(RowType.of(DataTypes.INT()), 1, null);
        return new IndexReplicator(
                new IndexSourceReader(log, null, Runnable::run, context),
                indexes,
                buffer,
                context,
                0L,
                1024,
                (sync, all) -> {},
                new ExponentialBackoff(10, 2, 100, 0),
                clock,
                null);
    }

    @Test
    void sourceReadFormsOneWindowAcrossCompleteBatches() throws Exception {
        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords first =
                createBasicMemoryLogRecords(
                        rowType,
                        1,
                        0L,
                        0L,
                        CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(new Object[] {1}),
                        LogFormat.INDEXED,
                        DEFAULT_COMPRESSION);
        MemoryLogRecords second =
                createBasicMemoryLogRecords(
                        rowType,
                        1,
                        1L,
                        0L,
                        CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Collections.singletonList(ChangeType.INSERT),
                        Collections.singletonList(new Object[] {2}),
                        LogFormat.INDEXED,
                        DEFAULT_COMPRESSION);
        LogRecords records =
                new LogRecords() {
                    @Override
                    public int sizeInBytes() {
                        return first.sizeInBytes() + second.sizeInBytes();
                    }

                    @Override
                    public Iterable<LogRecordBatch> batches() {
                        return Arrays.asList(
                                first.batches().iterator().next(),
                                second.batches().iterator().next());
                    }
                };
        TableBucket source = new TableBucket(1L, 0);
        IndexSourceReader.SourceLog log =
                new IndexSourceReader.SourceLog() {
                    @Override
                    public TableBucket tableBucket() {
                        return source;
                    }

                    @Override
                    public long highWatermark() {
                        return 2L;
                    }

                    @Override
                    public long logStartOffset() {
                        return 0L;
                    }

                    @Override
                    public FetchDataInfo read(
                            long offset,
                            int maxBytes,
                            org.apache.fluss.server.log.FetchIsolation isolation,
                            boolean minOneMessage) {
                        return new FetchDataInfo(records);
                    }
                };
        AtomicInteger encodes = new AtomicInteger();
        IndexSendBuffer buffer = new IndexSendBuffer();
        LogRecordReadContext context =
                LogRecordReadContext.createIndexedReadContext(rowType, 1, null);
        try (IndexReplicator replicator =
                new IndexReplicator(
                        new IndexSourceReader(log, null, Runnable::run, context),
                        Collections.singletonList(indexSpec("idx", 2L, encodes)),
                        buffer,
                        context,
                        0L,
                        1024,
                        (sync, all) -> {})) {
            assertThat(replicator.poll()).isTrue();
            assertThat(encodes).hasValue(2);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(buffer, new TableBucket(2L, 0));
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
            assertThat(buffer.pendingBytes()).isZero();
        }
    }

    @Test
    void sharesSourceDecodeAcrossIndexesWithoutSplittingSourceBatch() throws Exception {
        TableBucket sourceBucket = new TableBucket(1L, 0);
        AtomicInteger sourceReads = new AtomicInteger();
        AtomicInteger recordDecodes = new AtomicInteger();
        AtomicInteger firstIndexEncodes = new AtomicInteger();
        AtomicInteger secondIndexEncodes = new AtomicInteger();

        RowType rowType = RowType.of(DataTypes.INT());
        MemoryLogRecords sourceRecords =
                createBasicMemoryLogRecords(
                        rowType,
                        1,
                        0L,
                        0L,
                        CURRENT_LOG_MAGIC_VALUE,
                        NO_WRITER_ID,
                        NO_BATCH_SEQUENCE,
                        Arrays.asList(ChangeType.DELETE, ChangeType.DELETE),
                        Arrays.asList(new Object[] {1}, new Object[] {2}),
                        LogFormat.COMPACTED,
                        DEFAULT_COMPRESSION);
        DefaultLogRecordBatch batch =
                new DefaultLogRecordBatch() {
                    @Override
                    public CloseableIterator<LogRecord> records(ReadContext context) {
                        recordDecodes.incrementAndGet();
                        return super.records(context);
                    }
                };
        batch.pointTo(sourceRecords.getMemorySegment(), sourceRecords.getPosition());
        LogRecords records =
                new LogRecords() {
                    @Override
                    public int sizeInBytes() {
                        return sourceRecords.sizeInBytes();
                    }

                    @Override
                    public Iterable<LogRecordBatch> batches() {
                        return Collections.singletonList(batch);
                    }
                };

        IndexSourceReader.SourceLog sourceLog =
                new IndexSourceReader.SourceLog() {
                    @Override
                    public TableBucket tableBucket() {
                        return sourceBucket;
                    }

                    @Override
                    public long highWatermark() {
                        return 2L;
                    }

                    @Override
                    public long logStartOffset() {
                        return 0L;
                    }

                    @Override
                    public FetchDataInfo read(
                            long offset,
                            int maxBytes,
                            org.apache.fluss.server.log.FetchIsolation isolation,
                            boolean minOneMessage) {
                        sourceReads.incrementAndGet();
                        return new FetchDataInfo(records);
                    }
                };

        LogRecordReadContext readContext =
                LogRecordReadContext.createCompactedRowReadContext(rowType, 1, null);
        IndexSourceReader sourceReader =
                new IndexSourceReader(sourceLog, null, Runnable::run, readContext);
        IndexSpec first = indexSpec("idx_first", 2L, firstIndexEncodes);
        IndexSpec second = indexSpec("idx_second", 3L, secondIndexEncodes);
        IndexSendBuffer sendBuffer = new IndexSendBuffer();

        try (IndexReplicator replicator =
                new IndexReplicator(
                        sourceReader,
                        Arrays.asList(first, second),
                        sendBuffer,
                        readContext,
                        0L,
                        1024,
                        (syncOffset, allOffset) -> {})) {
            assertThat(replicator.poll()).isTrue();
            assertThat(sourceReads).hasValue(1);
            assertThat(recordDecodes).hasValue(1);
            assertThat(firstIndexEncodes).hasValue(2);
            assertThat(secondIndexEncodes).hasValue(2);

            TableBucket firstTarget = new TableBucket(2L, 0);
            TableBucket secondTarget = new TableBucket(3L, 0);
            assertThat(sendBuffer.buckets()).containsExactlyInAnyOrder(firstTarget, secondTarget);
            acknowledge(sendBuffer, firstTarget);
            assertThat(replicator.getAllIndexPushedOffset()).isZero();
            acknowledge(sendBuffer, secondTarget);
            assertThat(replicator.getAllIndexPushedOffset()).isEqualTo(2L);
        }
    }

    private static IndexSpec indexSpec(String name, long tableId, AtomicInteger encodes) {
        CompactedRowWriter writer = new CompactedRowWriter(1);
        writer.writeByte((byte) 1);
        CompactedRow value = new CompactedRow(new DataType[] {DataTypes.TINYINT()});
        value.pointTo(writer.segment(), 0, writer.position());
        return new IndexSpec(
                name,
                IndexVisibility.SYNC,
                tableId,
                1,
                KvFormat.COMPACTED,
                new int[] {0},
                ignored -> {
                    encodes.incrementAndGet();
                    return new IndexSpec.IndexEntry(name.getBytes(), value, 0);
                },
                (sourceBucket, targetBucket, sourceEndOffset) ->
                        new IndexSpec.IndexEntry(
                                (name + "-progress").getBytes(), value, targetBucket));
    }

    private static void acknowledge(IndexSendBuffer sendBuffer, TableBucket target) {
        IndexBatch batch = sendBuffer.claimFirstReady(target, 0L);
        assertThat(batch).isNotNull();
        assertThat(sendBuffer.acknowledgeClaim(batch)).isTrue();
        batch.window().onBatchAcked();
    }
}

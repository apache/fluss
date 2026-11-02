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
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.assertj.core.api.Assertions.assertThat;

class IndexReplicatorTest {

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
                        1,
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

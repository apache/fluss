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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.compacted.CompactedRowWriter;
import org.apache.fluss.server.log.FetchDataInfo;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IndexReplicatorTest {

    @Test
    void sharesSourceDecodeAcrossIndexesWithoutSplittingSourceBatch() throws Exception {
        TableBucket sourceBucket = new TableBucket(1L, 0);
        AtomicInteger sourceReads = new AtomicInteger();
        AtomicInteger recordDecodes = new AtomicInteger();
        AtomicInteger firstIndexEncodes = new AtomicInteger();
        AtomicInteger secondIndexEncodes = new AtomicInteger();

        InternalRow row = mock(InternalRow.class);
        when(row.isNullAt(0)).thenReturn(false);
        LogRecord record = mock(LogRecord.class);
        when(record.logOffset()).thenReturn(0L);
        when(record.getChangeType()).thenReturn(ChangeType.DELETE);
        when(record.getRow()).thenReturn(row);
        LogRecord secondRecord = mock(LogRecord.class);
        when(secondRecord.logOffset()).thenReturn(1L);
        when(secondRecord.getChangeType()).thenReturn(ChangeType.DELETE);
        when(secondRecord.getRow()).thenReturn(row);

        LogRecordBatch batch = mock(LogRecordBatch.class);
        when(batch.baseLogOffset()).thenReturn(0L);
        when(batch.nextLogOffset()).thenReturn(2L);
        when(batch.sizeInBytes()).thenReturn(1);
        when(batch.records(any()))
                .thenAnswer(
                        ignored -> {
                            recordDecodes.incrementAndGet();
                            return CloseableIterator.wrap(
                                    Arrays.asList(record, secondRecord).iterator());
                        });
        LogRecords records =
                new LogRecords() {
                    @Override
                    public int sizeInBytes() {
                        return 1;
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

        LogRecordReadContext readContext = mock(LogRecordReadContext.class);
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

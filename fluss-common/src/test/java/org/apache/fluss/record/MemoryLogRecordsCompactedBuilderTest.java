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

package org.apache.fluss.record;

import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.record.ChangeType.APPEND_ONLY;
import static org.apache.fluss.record.TestData.BASE_OFFSET;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_MAGIC;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MemoryLogRecordsCompactedBuilder}. */
class MemoryLogRecordsCompactedBuilderTest {

    @Test
    void testAppendAndBuild() throws Exception {
        MemoryLogRecordsCompactedBuilder builder = createBuilder(0, 4, 1024);

        List<CompactedRow> expected = new ArrayList<>();
        expected.add(compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"}));
        expected.add(compactedRow(DATA1_ROW_TYPE, new Object[] {2, "b"}));
        expected.add(compactedRow(DATA1_ROW_TYPE, new Object[] {3, "c"}));

        for (CompactedRow row : expected) {
            assertThat(builder.hasRoomFor(row)).isTrue();
            builder.append(APPEND_ONLY, row);
        }

        builder.setWriterState(7L, 13);
        builder.close();
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());

        Iterator<LogRecordBatch> it = records.batches().iterator();
        assertThat(it.hasNext()).isTrue();
        LogRecordBatch batch = it.next();
        assertThat(it.hasNext()).isFalse();

        assertThat(batch.getRecordCount()).isEqualTo(expected.size());
        assertThat(batch.baseLogOffset()).isEqualTo(0);
        assertThat(batch.writerId()).isEqualTo(7L);
        assertThat(batch.batchSequence()).isEqualTo(13);

        try (LogRecordReadContext ctx =
                        LogRecordReadContext.createCompactedRowReadContext(
                                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                CloseableIterator<LogRecord> recIt = batch.records(ctx)) {
            for (CompactedRow expRow : expected) {
                assertThat(recIt.hasNext()).isTrue();
                LogRecord rec = recIt.next();
                assertThat(rec.getChangeType()).isEqualTo(APPEND_ONLY);
                assertThat(rec.getRow()).isEqualTo(expRow);
            }
            assertThat(recIt.hasNext()).isFalse();
        }
    }

    @Test
    void testAbortSemantics() throws Exception {
        MemoryLogRecordsCompactedBuilder builder = createBuilder(0, 2, 512);
        builder.append(APPEND_ONLY, compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"}));
        builder.abort();

        // append after abort
        assertThatThrownBy(
                        () ->
                                builder.append(
                                        APPEND_ONLY,
                                        compactedRow(DATA1_ROW_TYPE, new Object[] {2, "b"})))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("MemoryLogRecordsCompactedBuilder has already been aborted");

        // build after abort
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Attempting to build an aborted record batch");

        // close after abort
        assertThatThrownBy(builder::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cannot close MemoryLogRecordsCompactedBuilder as it has already been aborted");
    }

    @Test
    void testNoRecordAppendAndBaseOffset() throws Exception {
        // base offset 0
        try (MemoryLogRecordsCompactedBuilder builder = createBuilder(0, 1, 1024)) {
            MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
            assertThat(records.sizeInBytes())
                    .isEqualTo(
                            LogRecordBatchFormat.recordBatchHeaderSize(
                                    DEFAULT_MAGIC)); // only batch header
            LogRecordBatch batch = records.batches().iterator().next();
            batch.ensureValid();
            assertThat(batch.getRecordCount()).isEqualTo(0);
            assertThat(batch.baseLogOffset()).isEqualTo(0);
            assertThat(batch.lastLogOffset()).isEqualTo(0);
            assertThat(batch.nextLogOffset()).isEqualTo(1);
            try (LogRecordReadContext ctx =
                            LogRecordReadContext.createCompactedRowReadContext(
                                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                    CloseableIterator<LogRecord> it = batch.records(ctx)) {
                assertThat(it.hasNext()).isFalse();
            }
        }

        // base offset 100
        try (MemoryLogRecordsCompactedBuilder builder = createBuilder(100, 1, 1024)) {
            MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
            assertThat(records.sizeInBytes())
                    .isEqualTo(LogRecordBatchFormat.recordBatchHeaderSize(DEFAULT_MAGIC));
            LogRecordBatch batch = records.batches().iterator().next();
            batch.ensureValid();
            assertThat(batch.getRecordCount()).isEqualTo(0);
            assertThat(batch.baseLogOffset()).isEqualTo(100);
            assertThat(batch.lastLogOffset()).isEqualTo(100);
            assertThat(batch.nextLogOffset()).isEqualTo(101);
            try (LogRecordReadContext ctx =
                            LogRecordReadContext.createCompactedRowReadContext(
                                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                    CloseableIterator<LogRecord> it = batch.records(ctx)) {
                assertThat(it.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testResetWriterState() throws Exception {
        MemoryLogRecordsCompactedBuilder builder = createBuilder(BASE_OFFSET, 2, 1024);
        List<CompactedRow> expected = new ArrayList<>();
        expected.add(compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"}));
        expected.add(compactedRow(DATA1_ROW_TYPE, new Object[] {2, "b"}));
        for (CompactedRow row : expected) {
            builder.append(APPEND_ONLY, row);
        }
        builder.setWriterState(5L, 0);
        builder.close();
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
        LogRecordBatch batch = records.batches().iterator().next();
        assertThat(batch.writerId()).isEqualTo(5L);
        assertThat(batch.batchSequence()).isEqualTo(0);

        // reset writer state and rebuild with new sequence
        builder.resetWriterState(5L, 1);
        records = MemoryLogRecords.pointToBytesView(builder.build());
        batch = records.batches().iterator().next();
        assertThat(batch.writerId()).isEqualTo(5L);
        assertThat(batch.batchSequence()).isEqualTo(1);
    }

    private MemoryLogRecordsCompactedBuilder createBuilder(
            long baseOffset, int maxPages, int pageSizeInBytes) throws IOException {
        return MemoryLogRecordsCompactedBuilder.builder(
                baseOffset,
                DEFAULT_SCHEMA_ID,
                maxPages * pageSizeInBytes,
                DEFAULT_MAGIC,
                new ManagedPagedOutputView(new TestingMemorySegmentPool(pageSizeInBytes)));
    }
}

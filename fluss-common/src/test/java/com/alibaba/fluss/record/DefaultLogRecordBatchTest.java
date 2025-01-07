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

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.UnmanagedPagedOutputView;
import com.alibaba.fluss.row.TestInternalRowGenerator;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.testutils.DataTestUtils;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultLogRecordBatch}. */
public class DefaultLogRecordBatchTest extends LogTestBase {

    @Test
    void testRecordBatchSize() throws Exception {
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject(TestData.DATA1);
        int totalSize = 0;
        for (LogRecordBatch logRecordBatch : memoryLogRecords.batches()) {
            totalSize += logRecordBatch.sizeInBytes();
        }
        assertThat(totalSize).isEqualTo(memoryLogRecords.sizeInBytes());
    }

    @Test
    void testIndexedRowWriteAndReadBatch() throws Exception {
        int recordNumber = 50;
        RowType allRowType = TestInternalRowGenerator.createAllRowType();
        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        baseLogOffset,
                        schemaId,
                        Integer.MAX_VALUE,
                        magic,
                        new UnmanagedPagedOutputView(100));

        List<IndexedRow> rows = new ArrayList<>();
        for (int i = 0; i < recordNumber; i++) {
            IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
            builder.append(RowKind.INSERT, row);
            rows.add(row);
        }

        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();

        logRecordBatch.ensureValid();

        assertThat(logRecordBatch.getRecordCount()).isEqualTo(recordNumber);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(baseLogOffset);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(baseLogOffset + recordNumber - 1);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(baseLogOffset + recordNumber);
        assertThat(logRecordBatch.magic()).isEqualTo(magic);
        assertThat(logRecordBatch.isValid()).isTrue();
        assertThat(logRecordBatch.schemaId()).isEqualTo(schemaId);

        // verify record.
        int i = 0;
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(allRowType, schemaId);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            while (iter.hasNext()) {
                LogRecord record = iter.next();
                assertThat(record.logOffset()).isEqualTo(i);
                assertThat(record.getRowKind()).isEqualTo(RowKind.INSERT);
                assertThat(record.getRow()).isEqualTo(rows.get(i));
                i++;
            }
        }

        builder.close();
    }

    @Test
    void testOverrideLastLogOffset() throws Exception {
        List<IndexedRow> rows1 =
                Arrays.asList(
                        row(baseRowType, new Object[] {1, "1"}),
                        row(baseRowType, new Object[] {2, "2"}),
                        row(baseRowType, new Object[] {3, "3"}));

        MemoryLogRecordsIndexedBuilder builder =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L, schemaId, Integer.MAX_VALUE, magic, outputView);
        for (IndexedRow row : rows1) {
            builder.append(RowKind.APPEND_ONLY, row);
        }

        // override lastLogOffset smaller than record counts.
        assertThatThrownBy(() -> builder.overrideLastLogOffset(1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The override lastLogOffset is less than recordCount + baseLogOffset,"
                                + " which will cause the logOffsetDelta to be negative");

        // override lastLogOffset larger than record counts.
        builder.overrideLastLogOffset(5L);
        MemoryLogRecords memoryLogRecords = builder.build();
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(3);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(5);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(6);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(0);
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(baseRowType, schemaId);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            for (IndexedRow row : rows1) {
                assertThat(iter.hasNext()).isTrue();
                LogRecord record = iter.next();
                assertThat(record.getRow()).isEqualTo(row);
            }
            assertThat(iter.hasNext()).isFalse();
        }

        // test empty record batch.
        MemoryLogRecordsIndexedBuilder builder2 =
                MemoryLogRecordsIndexedBuilder.builder(
                        0L, schemaId, Integer.MAX_VALUE, magic, outputView);
        builder2.overrideLastLogOffset(0);
        memoryLogRecords = builder2.build();
        iterator = memoryLogRecords.batches().iterator();

        assertThat(iterator.hasNext()).isTrue();
        logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(0);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(1);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(0);
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createIndexedReadContext(baseRowType, schemaId);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }
    }
}

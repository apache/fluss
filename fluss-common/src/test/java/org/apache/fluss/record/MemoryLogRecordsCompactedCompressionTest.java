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

import org.apache.fluss.compression.LogCompressionInfo;
import org.apache.fluss.compression.LogCompressionType;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.fluss.record.ChangeType.APPEND_ONLY;
import static org.apache.fluss.record.ChangeType.DELETE;
import static org.apache.fluss.record.ChangeType.INSERT;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for batch-level compression of {@link MemoryLogRecordsCompactedBuilder}. */
class MemoryLogRecordsCompactedCompressionTest {

    static Collection<LogCompressionInfo> compressions() {
        return Arrays.asList(
                new LogCompressionInfo(LogCompressionType.LZ4_FRAME, -1),
                new LogCompressionInfo(LogCompressionType.ZSTD, 1),
                new LogCompressionInfo(LogCompressionType.ZSTD, 3));
    }

    @ParameterizedTest
    @MethodSource("compressions")
    void testCompressedRoundTrip(LogCompressionInfo compression) throws Exception {
        List<CompactedRow> rows = repetitiveRows(500);
        // small pages so that the records span many segments
        MemoryLogRecordsCompactedBuilder builder = createBuilder(false, compression, 256);
        List<ChangeType> changeTypes = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            ChangeType changeType = i % 3 == 0 ? DELETE : INSERT;
            changeTypes.add(changeType);
            builder.append(changeType, rows.get(i));
        }
        builder.setWriterState(7L, 13);
        builder.close();
        int uncompressedSize = builder.getSizeInBytes();

        DefaultLogRecordBatch batch = singleBatch(builder);
        batch.ensureValid();
        assertThat(batch.magic()).isEqualTo(CURRENT_LOG_MAGIC_VALUE);
        assertThat(batch.compressionType()).isEqualTo(compression.getCompressionType());
        assertThat(batch.sizeInBytes()).isLessThan(uncompressedSize / 2);
        assertThat(batch.getRecordCount()).isEqualTo(rows.size());
        assertThat(batch.writerId()).isEqualTo(7L);
        assertThat(batch.batchSequence()).isEqualTo(13);
        assertRows(batch, rows, changeTypes);

        // a retry with new writer state only rewrites the header of the compressed batch
        builder.resetWriterState(8L, 20);
        DefaultLogRecordBatch retried = singleBatch(builder);
        retried.ensureValid();
        assertThat(retried.writerId()).isEqualTo(8L);
        assertThat(retried.batchSequence()).isEqualTo(20);
        assertThat(retried.sizeInBytes()).isEqualTo(batch.sizeInBytes());
        assertRows(retried, rows, changeTypes);
    }

    @Test
    void testIncompressibleBatchStaysUncompressed() throws Exception {
        CompactedRow row = compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"});
        MemoryLogRecordsCompactedBuilder builder =
                createBuilder(true, new LogCompressionInfo(LogCompressionType.ZSTD, 3), 1024);
        builder.append(APPEND_ONLY, row);
        builder.close();

        DefaultLogRecordBatch batch = singleBatch(builder);
        batch.ensureValid();
        assertThat(batch.compressionType()).isEqualTo(LogCompressionType.NONE);
        assertThat(batch.sizeInBytes()).isEqualTo(builder.getSizeInBytes());
        assertRows(batch, Arrays.asList(row), Arrays.asList(APPEND_ONLY));
    }

    @Test
    void testEmptyCompressedBatch() throws Exception {
        MemoryLogRecordsCompactedBuilder builder =
                createBuilder(false, new LogCompressionInfo(LogCompressionType.ZSTD, 3), 1024);
        builder.close();

        DefaultLogRecordBatch batch = singleBatch(builder);
        batch.ensureValid();
        assertThat(batch.compressionType()).isEqualTo(LogCompressionType.NONE);
        assertThat(batch.getRecordCount()).isEqualTo(0);
        assertThat(batch.sizeInBytes())
                .isEqualTo(LogRecordBatchFormat.recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE));
        assertRows(batch, new ArrayList<>(), new ArrayList<>());
    }

    @Test
    void testUncompressedAttributesUnchanged() throws Exception {
        for (boolean appendOnly : new boolean[] {true, false}) {
            MemoryLogRecordsCompactedBuilder builder =
                    createBuilder(appendOnly, LogCompressionInfo.NO_COMPRESSION, 1024);
            builder.append(
                    appendOnly ? APPEND_ONLY : INSERT,
                    compactedRow(DATA1_ROW_TYPE, new Object[] {1, "a"}));
            builder.close();
            MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
            assertThat(records.batches().iterator().next().magic())
                    .isEqualTo(LogRecordBatch.CURRENT_LOG_MAGIC_VALUE);
            byte attributes =
                    records.getMemorySegment()
                            .get(
                                    records.getPosition()
                                            + LogRecordBatchFormat.attributeOffset(
                                                    LogRecordBatch.CURRENT_LOG_MAGIC_VALUE));
            // the same attribute byte as before compression support was added
            assertThat(attributes).isEqualTo((byte) (appendOnly ? 1 : 0));
        }
    }

    private static List<CompactedRow> repetitiveRows(int count) {
        List<CompactedRow> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(
                    compactedRow(
                            DATA1_ROW_TYPE, new Object[] {i, "status-" + (i % 4) + "-shipped"}));
        }
        return rows;
    }

    private static DefaultLogRecordBatch singleBatch(MemoryLogRecordsCompactedBuilder builder)
            throws Exception {
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
        List<LogRecordBatch> batches = new ArrayList<>();
        records.batches().forEach(batches::add);
        assertThat(batches).hasSize(1);
        return (DefaultLogRecordBatch) batches.get(0);
    }

    private static void assertRows(
            LogRecordBatch batch, List<CompactedRow> rows, List<ChangeType> changeTypes)
            throws Exception {
        try (LogRecordReadContext ctx =
                        LogRecordReadContext.createCompactedRowReadContext(
                                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER);
                CloseableIterator<LogRecord> it = batch.records(ctx)) {
            for (int i = 0; i < rows.size(); i++) {
                assertThat(it.hasNext()).isTrue();
                LogRecord record = it.next();
                assertThat(record.logOffset()).isEqualTo(i);
                assertThat(record.getChangeType()).isEqualTo(changeTypes.get(i));
                assertThat(record.getRow()).isEqualTo(rows.get(i));
            }
            assertThat(it.hasNext()).isFalse();
        }
    }

    private static MemoryLogRecordsCompactedBuilder createBuilder(
            boolean appendOnly, LogCompressionInfo compression, int pageSize) throws IOException {
        return MemoryLogRecordsCompactedBuilder.builder(
                DEFAULT_SCHEMA_ID,
                Integer.MAX_VALUE,
                new ManagedPagedOutputView(new TestingMemorySegmentPool(pageSize)),
                appendOnly,
                compression);
    }
}

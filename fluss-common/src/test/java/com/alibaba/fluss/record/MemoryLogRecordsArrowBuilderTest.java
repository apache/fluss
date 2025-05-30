/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

import com.alibaba.fluss.compression.ArrowCompressionInfo;
import com.alibaba.fluss.compression.ArrowCompressionType;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.memory.ManagedPagedOutputView;
import com.alibaba.fluss.memory.TestingMemorySegmentPool;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.arrow.ArrowWriter;
import com.alibaba.fluss.row.arrow.ArrowWriterPool;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import com.alibaba.fluss.testutils.DataTestUtils;
import com.alibaba.fluss.utils.CloseableIterator;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static com.alibaba.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION;
import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;
import static com.alibaba.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static com.alibaba.fluss.row.arrow.ArrowWriter.BUFFER_USAGE_RATIO;
import static com.alibaba.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link MemoryLogRecordsArrowBuilder}. */
public class MemoryLogRecordsArrowBuilderTest {
    private BufferAllocator allocator;
    private ArrowWriterPool provider;
    private Configuration conf;

    @BeforeEach
    void setup() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.provider = new ArrowWriterPool(allocator);
        this.conf = new Configuration();
    }

    @AfterEach
    void tearDown() {
        provider.close();
        allocator.close();
    }

    @Test
    void testAppendWithEmptyRecord() throws Exception {
        int maxSizeInBytes = 1024;
        ArrowWriter writer =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, maxSizeInBytes, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder =
                createMemoryLogRecordsArrowBuilder(0, writer, 10, 100);
        assertThat(builder.isFull()).isFalse();
        assertThat(builder.getWriteLimitInBytes())
                .isEqualTo((int) (maxSizeInBytes * BUFFER_USAGE_RATIO));
        builder.close();
        builder.setWriterState(1L, 0);
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
        Iterator<LogRecordBatch> iterator = records.batches().iterator();
        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch batch = iterator.next();
        assertThat(batch.getRecordCount()).isEqualTo(0);
        assertThat(batch.sizeInBytes()).isEqualTo(48);
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testAppend() throws Exception {
        int maxSizeInBytes = 1024;
        ArrowWriter writer =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, maxSizeInBytes, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder =
                createMemoryLogRecordsArrowBuilder(0, writer, 10, 1024);
        List<ChangeType> changeTypes =
                DATA1.stream().map(row -> ChangeType.APPEND_ONLY).collect(Collectors.toList());
        List<InternalRow> rows =
                DATA1.stream().map(DataTestUtils::row).collect(Collectors.toList());
        List<Object[]> expectedResult = new ArrayList<>();
        while (!builder.isFull()) {
            int rndIndex = RandomUtils.nextInt(0, DATA1.size());
            builder.append(changeTypes.get(rndIndex), rows.get(rndIndex));
            expectedResult.add(DATA1.get(rndIndex));
        }
        assertThat(builder.isFull()).isTrue();
        assertThatThrownBy(() -> builder.append(ChangeType.APPEND_ONLY, rows.get(0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "The arrow batch size is full and it shouldn't accept writing new rows, it's a bug.");

        builder.setWriterState(1L, 0);
        builder.close();
        assertThatThrownBy(() -> builder.append(ChangeType.APPEND_ONLY, rows.get(0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Tried to append a record, but MemoryLogRecordsArrowBuilder is closed for record appends");
        assertThat(builder.isClosed()).isTrue();
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
        assertLogRecordsEquals(DATA1_ROW_TYPE, records, expectedResult);
    }

    @ParameterizedTest
    @MethodSource("compressionInfos")
    void testCompression(ArrowCompressionInfo compressionInfo) throws Exception {
        int maxSizeInBytes = 1024;
        // create a compression-able data set.
        List<Object[]> dataSet =
                Arrays.asList(
                        new Object[] {1, StringUtils.repeat("a", 10)},
                        new Object[] {2, StringUtils.repeat("b", 11)},
                        new Object[] {3, StringUtils.repeat("c", 12)},
                        new Object[] {4, StringUtils.repeat("d", 13)},
                        new Object[] {5, StringUtils.repeat("e", 14)},
                        new Object[] {6, StringUtils.repeat("f", 15)},
                        new Object[] {7, StringUtils.repeat("g", 16)},
                        new Object[] {8, StringUtils.repeat("h", 17)},
                        new Object[] {9, StringUtils.repeat("i", 18)},
                        new Object[] {10, StringUtils.repeat("j", 19)});

        // first create an un-compression batch.
        ArrowWriter writer1 =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, maxSizeInBytes, DATA1_ROW_TYPE, NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder =
                createMemoryLogRecordsArrowBuilder(0, writer1, 10, 1024);
        for (Object[] data : dataSet) {
            builder.append(ChangeType.APPEND_ONLY, row(data));
        }
        builder.close();
        MemoryLogRecords records1 = MemoryLogRecords.pointToBytesView(builder.build());
        int sizeInBytes1 = records1.sizeInBytes();
        assertLogRecordsEquals(DATA1_ROW_TYPE, records1, dataSet);

        // second create a compression batch.
        ArrowWriter writer2 =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, maxSizeInBytes, DATA1_ROW_TYPE, compressionInfo);
        MemoryLogRecordsArrowBuilder builder2 =
                createMemoryLogRecordsArrowBuilder(0, writer2, 10, 1024);
        for (Object[] data : dataSet) {
            builder2.append(ChangeType.APPEND_ONLY, row(data));
        }
        builder2.close();
        MemoryLogRecords records2 = MemoryLogRecords.pointToBytesView(builder2.build());
        int sizeInBytes2 = records2.sizeInBytes();
        assertLogRecordsEquals(DATA1_ROW_TYPE, records2, dataSet);

        // compare the size of two batches.
        assertThat(sizeInBytes1).isGreaterThan(sizeInBytes2);
    }

    @Test
    void testIllegalArgument() {
        int maxSizeInBytes = 1024;
        assertThatThrownBy(
                        () -> {
                            try (ArrowWriter writer =
                                    provider.getOrCreateWriter(
                                            1L,
                                            DEFAULT_SCHEMA_ID,
                                            maxSizeInBytes,
                                            DATA1_ROW_TYPE,
                                            DEFAULT_COMPRESSION)) {
                                createMemoryLogRecordsArrowBuilder(0, writer, 10, 30);
                            }
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The size of first segment of pagedOutputView is too small, need at least 48 bytes.");
    }

    @Test
    void testClose() throws Exception {
        int maxSizeInBytes = 1024;
        ArrowWriter writer =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, 1024, DATA1_ROW_TYPE, NO_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder =
                createMemoryLogRecordsArrowBuilder(0, writer, 10, 1024);
        List<ChangeType> changeTypes =
                DATA1.stream().map(row -> ChangeType.APPEND_ONLY).collect(Collectors.toList());
        List<InternalRow> rows =
                DATA1.stream().map(DataTestUtils::row).collect(Collectors.toList());
        while (!builder.isFull()) {
            int rndIndex = RandomUtils.nextInt(0, DATA1.size());
            builder.append(changeTypes.get(rndIndex), rows.get(rndIndex));
        }
        assertThat(builder.isFull()).isTrue();

        String tableSchemaId = 1L + "-" + 1 + "-" + "NONE";
        assertThat(provider.freeWriters().size()).isEqualTo(0);
        int sizeInBytesBeforeClose = builder.estimatedSizeInBytes();
        builder.close();
        builder.setWriterState(1L, 0);
        builder.build();
        assertThat(provider.freeWriters().get(tableSchemaId).size()).isEqualTo(1);
        int sizeInBytes = builder.estimatedSizeInBytes();
        assertThat(sizeInBytes).isEqualTo(sizeInBytesBeforeClose);
        // get writer again, writer will be initial.
        ArrowWriter writer1 =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, maxSizeInBytes, DATA1_ROW_TYPE, NO_COMPRESSION);
        assertThat(provider.freeWriters().get(tableSchemaId).size()).isEqualTo(0);

        // Even if the writer has re-initialized, the sizeInBytes should be the same.
        assertThat(builder.estimatedSizeInBytes()).isEqualTo(sizeInBytes);

        writer.close();
        writer1.close();
    }

    @Test
    void testNoRecordAppend() throws Exception {
        // 1. no record append with base offset as 0.
        ArrowWriter writer =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, 1024 * 10, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder =
                createMemoryLogRecordsArrowBuilder(0, writer, 10, 1024 * 10);
        MemoryLogRecords memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(48);
        Iterator<LogRecordBatch> iterator = memoryLogRecords.batches().iterator();
        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(0);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(1);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(0);
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createArrowReadContext(
                                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }

        // 2. no record append with base offset as 0.
        ArrowWriter writer2 =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, 1024 * 10, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
        builder = createMemoryLogRecordsArrowBuilder(100, writer2, 10, 1024 * 10);
        memoryLogRecords = MemoryLogRecords.pointToBytesView(builder.build());
        // only contains batch header.
        assertThat(memoryLogRecords.sizeInBytes()).isEqualTo(48);
        iterator = memoryLogRecords.batches().iterator();
        assertThat(iterator.hasNext()).isTrue();
        logRecordBatch = iterator.next();
        assertThat(iterator.hasNext()).isFalse();

        logRecordBatch.ensureValid();
        assertThat(logRecordBatch.getRecordCount()).isEqualTo(0);
        assertThat(logRecordBatch.lastLogOffset()).isEqualTo(100);
        assertThat(logRecordBatch.nextLogOffset()).isEqualTo(101);
        assertThat(logRecordBatch.baseLogOffset()).isEqualTo(100);
        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createArrowReadContext(
                                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                CloseableIterator<LogRecord> iter = logRecordBatch.records(readContext)) {
            assertThat(iter.hasNext()).isFalse();
        }
    }

    @Test
    void testResetWriterState() throws Exception {
        int maxSizeInBytes = 1024;
        ArrowWriter writer =
                provider.getOrCreateWriter(
                        1L, DEFAULT_SCHEMA_ID, maxSizeInBytes, DATA1_ROW_TYPE, DEFAULT_COMPRESSION);
        MemoryLogRecordsArrowBuilder builder =
                createMemoryLogRecordsArrowBuilder(0, writer, 10, 1024);
        List<ChangeType> changeTypes =
                DATA1.stream().map(row -> ChangeType.APPEND_ONLY).collect(Collectors.toList());
        List<InternalRow> rows =
                DATA1.stream().map(DataTestUtils::row).collect(Collectors.toList());
        List<Object[]> expectedResult = new ArrayList<>();
        while (!builder.isFull()) {
            int rndIndex = RandomUtils.nextInt(0, DATA1.size());
            builder.append(changeTypes.get(rndIndex), rows.get(rndIndex));
            expectedResult.add(DATA1.get(rndIndex));
        }
        assertThat(builder.isFull()).isTrue();
        builder.setWriterState(1L, 0);
        builder.close();
        assertThat(builder.isClosed()).isTrue();
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
        assertLogRecordsEquals(DATA1_ROW_TYPE, records, expectedResult);
        LogRecordBatch recordBatch = records.batches().iterator().next();
        assertThat(recordBatch.writerId()).isEqualTo(1L);
        assertThat(recordBatch.batchSequence()).isEqualTo(0);

        // test reset writer state and build (This situation will happen when the produceLog request
        // failed and the batch is re-enqueue to send with different write state).
        builder.setWriterState(1L, 1);
        records = MemoryLogRecords.pointToBytesView(builder.build());
        assertLogRecordsEquals(DATA1_ROW_TYPE, records, expectedResult);
        recordBatch = records.batches().iterator().next();
        assertThat(recordBatch.writerId()).isEqualTo(1L);
        assertThat(recordBatch.batchSequence()).isEqualTo(1);
    }

    private static List<ArrowCompressionInfo> compressionInfos() {
        return Arrays.asList(
                new ArrowCompressionInfo(ArrowCompressionType.LZ4_FRAME, -1),
                new ArrowCompressionInfo(ArrowCompressionType.ZSTD, 3),
                new ArrowCompressionInfo(ArrowCompressionType.ZSTD, 9));
    }

    private MemoryLogRecordsArrowBuilder createMemoryLogRecordsArrowBuilder(
            int baseOffset, ArrowWriter writer, int maxPages, int pageSizeInBytes)
            throws IOException {
        conf.set(
                ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE,
                new MemorySize((long) maxPages * pageSizeInBytes));
        conf.set(ConfigOptions.CLIENT_WRITER_BUFFER_PAGE_SIZE, new MemorySize(pageSizeInBytes));
        conf.set(ConfigOptions.CLIENT_WRITER_BATCH_SIZE, new MemorySize(pageSizeInBytes));
        return MemoryLogRecordsArrowBuilder.builder(
                baseOffset,
                DEFAULT_SCHEMA_ID,
                writer,
                new ManagedPagedOutputView(new TestingMemorySegmentPool(pageSizeInBytes)));
    }
}

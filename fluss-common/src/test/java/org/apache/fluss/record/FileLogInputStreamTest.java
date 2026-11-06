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

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.compression.ArrowCompressionType;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.shaded.arrow.org.apache.arrow.flatbuf.RecordBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fluss.utils.ByteBufferReadableChannel;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.Projection;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.record.LogRecordBatchFormat.recordsCountOffset;
import static org.apache.fluss.record.LogRecordBatchFormat.statisticsLengthOffset;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FileLogInputStream}. */
public class FileLogInputStreamTest extends LogTestBase {
    private @TempDir File tempDir;

    @ParameterizedTest
    @MethodSource("projectedFileModes")
    void testProjectedFileRecords(
            byte magic, ArrowCompressionType compression, boolean appendOnly, boolean checkCrc)
            throws Exception {
        List<ChangeType> changeTypes =
                appendOnly
                        ? Arrays.asList(ChangeType.APPEND_ONLY, ChangeType.APPEND_ONLY)
                        : Arrays.asList(ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER);
        List<List<Object[]>> data =
                Arrays.asList(
                        Arrays.asList(new Object[] {10, "before"}, new Object[] {20, "after"}),
                        Arrays.asList(
                                new Object[] {
                                    110, "The second batch has a longer value before the update."
                                },
                                new Object[] {
                                    220,
                                    "Its updated value also differs in content and serialized length."
                                }));
        long[] baseOffsets = {42L, 100L};
        MemoryLogRecords[] batches = new MemoryLogRecords[data.size()];
        for (int batchIndex = 0; batchIndex < batches.length; batchIndex++) {
            batches[batchIndex] =
                    createBasicMemoryLogRecords(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            baseOffsets[batchIndex],
                            1234L + batchIndex,
                            magic,
                            7L,
                            3 + batchIndex,
                            changeTypes,
                            data.get(batchIndex),
                            LogFormat.ARROW,
                            new ArrowCompressionInfo(compression, -1),
                            appendOnly);
        }
        assertThat(batches[1].sizeInBytes()).isNotEqualTo(batches[0].sizeInBytes());
        File path = new File(tempDir, "projected.log");
        try (TestingFileChannel channel =
                        new TestingFileChannel(
                                FileChannel.open(
                                        path.toPath(),
                                        StandardOpenOption.CREATE,
                                        StandardOpenOption.READ,
                                        StandardOpenOption.WRITE));
                FileLogRecords file =
                        new FileLogRecords(path, channel, 0, Integer.MAX_VALUE, false);
                LogRecordReadContext context =
                        LogRecordReadContext.createReadContext(
                                DATA1_TABLE_INFO,
                                true,
                                LogRecordReadContext.SchemaResolution.TARGET,
                                Projection.of(new int[] {0}),
                                TEST_SCHEMA_GETTER,
                                null)) {
            for (MemoryLogRecords memory : batches) {
                file.append(memory);
            }
            FileLogInputStream stream = new FileLogInputStream(file, 0, file.sizeInBytes());
            int batchPosition = 0;
            for (int batchIndex = 0; batchIndex < batches.length; batchIndex++) {
                MemoryLogRecords memory = batches[batchIndex];
                List<Object[]> expectedRows = data.get(batchIndex);
                channel.resetReadTracking();
                FileLogInputStream.FileChannelLogRecordBatch batch = stream.nextBatch();
                assertThat(batch).isNotNull();
                assertThat(batch.position()).isEqualTo(batchPosition);
                assertThat(batch.sizeInBytes()).isEqualTo(memory.sizeInBytes());
                assertThat(batch.baseLogOffset()).isEqualTo(baseOffsets[batchIndex]);
                if (checkCrc) {
                    batch.ensureValid();
                }
                try (CloseableIterator<LogRecord> records = batch.records(context)) {
                    for (int i = 0; i < expectedRows.size(); i++) {
                        assertThat(records.hasNext()).isTrue();
                        LogRecord record = records.next();
                        assertThat(record.logOffset()).isEqualTo(baseOffsets[batchIndex] + i);
                        assertThat(record.timestamp()).isEqualTo(1234L + batchIndex);
                        assertThat(record.getChangeType()).isEqualTo(changeTypes.get(i));
                        assertThat(record.getRow().getInt(0)).isEqualTo(expectedRows.get(i)[0]);
                        assertThat(record.getRow().isNullAt(1)).isTrue();
                    }
                    assertThat(records.hasNext()).isFalse();
                }
                if (!checkCrc) {
                    channel.assertReadsWithin(batchPosition, projectedReadRanges(memory, 0, 1));
                } else {
                    channel.assertReadsWithin(
                            batchPosition,
                            Collections.singletonList(new long[] {0, memory.sizeInBytes()}));
                    assertThat(channel.getBytesRead()).isGreaterThanOrEqualTo(memory.sizeInBytes());
                }
                assertThat(context.getBufferAllocator().getAllocatedMemory()).isZero();
                // Header access and a later explicit CRC validation still refer to the original
                // batch.
                assertThat(batch.writerId()).isEqualTo(7L);
                assertThat(batch.batchSequence()).isEqualTo(3 + batchIndex);
                batch.ensureValid();
                batchPosition += memory.sizeInBytes();
            }
            assertThat(stream.nextBatch()).isNull();
        }
    }

    @Test
    void testProjectedFileRecordsWithStatistics() throws Exception {
        MemoryLogRecords memory =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 100L, DEFAULT_SCHEMA_ID);
        List<long[]> ranges = projectedReadRanges(memory, 2, 3, 4);
        File path = new File(tempDir, "statistics.log");
        try (TestingFileChannel channel =
                        new TestingFileChannel(
                                FileChannel.open(
                                        path.toPath(),
                                        StandardOpenOption.CREATE,
                                        StandardOpenOption.READ,
                                        StandardOpenOption.WRITE));
                FileLogRecords file =
                        new FileLogRecords(path, channel, 0, Integer.MAX_VALUE, false);
                LogRecordReadContext context =
                        LogRecordReadContext.createReadContext(
                                DATA1_TABLE_INFO,
                                true,
                                LogRecordReadContext.SchemaResolution.TARGET,
                                Projection.of(new int[] {1}),
                                TEST_SCHEMA_GETTER,
                                null)) {
            file.append(memory);
            FileLogInputStream.FileChannelLogRecordBatch batch =
                    new FileLogInputStream(file, 0, file.sizeInBytes()).nextBatch();
            assertThat(batch.getStatistics(context)).isPresent();
            try (CloseableIterator<LogRecord> records = batch.records(context)) {
                for (int i = 0; i < DATA1.size(); i++) {
                    LogRecord record = records.next();
                    assertThat(record.logOffset()).isEqualTo(100L + i);
                    assertThat(record.getRow().getString(1).toString()).isEqualTo(DATA1.get(i)[1]);
                }
                assertThat(records.hasNext()).isFalse();
            }
            channel.assertReadsWithin(0, ranges);
            assertThat(context.getBufferAllocator().getAllocatedMemory()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, Integer.MAX_VALUE})
    void testProjectedFileRecordsRejectInvalidStatisticsLength(int statisticsLength)
            throws Exception {
        MemoryLogRecords memory = createProjectedTestBatch(LOG_MAGIC_VALUE_V2, true);
        memory.getMemorySegment()
                .putInt(
                        memory.getPosition() + statisticsLengthOffset(LOG_MAGIC_VALUE_V2),
                        statisticsLength);
        try (FileLogRecords file =
                        FileLogRecords.open(new File(tempDir, "invalid-statistics.log"));
                LogRecordReadContext context = createProjectedReadContext()) {
            file.append(memory);
            FileLogInputStream.FileChannelLogRecordBatch batch =
                    new FileLogInputStream(file, 0, file.sizeInBytes()).nextBatch();
            assertThatThrownBy(() -> batch.records(context))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid statistics length: %s.", statisticsLength);
            assertThat(context.getBufferAllocator().getAllocatedMemory()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2})
    void testProjectedFileRecordsRejectOversizedChangeTypeVector(byte magic) throws Exception {
        MemoryLogRecords memory = createProjectedTestBatch(magic, false);
        int recordCount = memory.sizeInBytes() - recordBatchHeaderSize(magic) + 1;
        memory.getMemorySegment()
                .putInt(memory.getPosition() + recordsCountOffset(magic), recordCount);
        try (FileLogRecords file =
                        FileLogRecords.open(new File(tempDir, "invalid-change-types.log"));
                LogRecordReadContext context = createProjectedReadContext()) {
            file.append(memory);
            FileLogInputStream.FileChannelLogRecordBatch batch =
                    new FileLogInputStream(file, 0, file.sizeInBytes()).nextBatch();
            assertThatThrownBy(() -> batch.records(context))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Change type vector exceeds batch size.");
            assertThat(context.getBufferAllocator().getAllocatedMemory()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 1})
    void testProjectedFileRowCountMismatchReleasesBuffersAndContextCanRetry(int rowCountDelta)
            throws Exception {
        // Append-only batches keep the IPC start fixed when the header count is corrupted.
        MemoryLogRecords invalid = createProjectedTestBatch(LOG_MAGIC_VALUE_V2, true);
        invalid.getMemorySegment()
                .putInt(
                        invalid.getPosition() + recordsCountOffset(LOG_MAGIC_VALUE_V2),
                        DATA1.size() + rowCountDelta);
        List<Object[]> validRows =
                Arrays.asList(
                        new Object[] {110, "valid-before"}, new Object[] {220, "valid-after"});
        MemoryLogRecords valid =
                createRecordsWithoutBaseLogOffset(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        100L,
                        5678L,
                        LOG_MAGIC_VALUE_V2,
                        validRows,
                        LogFormat.ARROW,
                        true);
        try (FileLogRecords file =
                        FileLogRecords.open(new File(tempDir, "mismatched-row-count.log"));
                LogRecordReadContext context = createProjectedReadContext()) {
            file.append(invalid);
            file.append(valid);
            FileLogInputStream stream = new FileLogInputStream(file, 0, file.sizeInBytes());
            FileLogInputStream.FileChannelLogRecordBatch invalidBatch = stream.nextBatch();
            assertThatThrownBy(() -> invalidBatch.records(context))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Arrow row count does not match log batch record count.");
            assertThat(context.getBufferAllocator().getAllocatedMemory()).isZero();

            FileLogInputStream.FileChannelLogRecordBatch validBatch = stream.nextBatch();
            try (CloseableIterator<LogRecord> records = validBatch.records(context)) {
                assertThat(context.getBufferAllocator().getAllocatedMemory()).isPositive();
                for (int i = 0; i < validRows.size(); i++) {
                    assertThat(records.hasNext()).isTrue();
                    LogRecord record = records.next();
                    assertThat(record.logOffset()).isEqualTo(100L + i);
                    assertThat(record.timestamp()).isEqualTo(5678L);
                    assertThat(record.getRow().getInt(0)).isEqualTo(validRows.get(i)[0]);
                    assertThat(record.getRow().isNullAt(1)).isTrue();
                }
                assertThat(records.hasNext()).isFalse();
            }
            assertThat(context.getBufferAllocator().getAllocatedMemory()).isZero();
            assertThat(stream.nextBatch()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testWriteTo(byte recordBatchMagic) throws Exception {
        try (FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            recordBatchMagic,
                            Collections.singletonList(new Object[] {0, "abc"}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(recordBatchMagic);

            LogRecordBatch recordBatch = batch.loadFullBatch();

            TestingSchemaGetter schemaGetter = new TestingSchemaGetter(schemaId, DATA1_SCHEMA);
            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(
                                    DATA1_ROW_TYPE, schemaId, schemaGetter);
                    CloseableIterator<LogRecord> iterator = recordBatch.records(readContext)) {
                assertThat(iterator.hasNext()).isTrue();
                LogRecord record = iterator.next();
                assertThat(record.getRow().getFieldCount()).isEqualTo(2);
                assertThat(iterator.hasNext()).isFalse();
            }
        }
    }

    @Test
    void testV2FormatWithStatistics() throws Exception {
        // Create test data with statistics using V2 format
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_v2.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);
            assertThat(batch.getRecordCount()).isEqualTo(TestData.DATA1.size());

            // Test statistics reading with ReadContext
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {

                // Test getStatistics method
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isPresent();

                LogRecordBatchStatistics statistics = statisticsOpt.get();

                // Verify statistics content for DATA1
                assertThat(statistics.getMinValues().getInt(0)).isEqualTo(1); // min id
                assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(10); // max id
                assertThat(statistics.getNullCounts()[0]).isEqualTo(0); // no nulls

                // Test that statistics are cached (lazy loading)
                Optional<LogRecordBatchStatistics> statisticsOpt2 =
                        batch.getStatistics(readContext);
                assertThat(statisticsOpt2).isPresent();
                assertThat(statisticsOpt2.get()).isSameAs(statisticsOpt.get());
            }

            // Test that records can still be read correctly
            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(
                                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER);
                    CloseableIterator<LogRecord> iterator = batch.records(readContext)) {
                assertThat(iterator.hasNext()).isTrue();
                int recordCount = 0;
                while (iterator.hasNext()) {
                    LogRecord record = iterator.next();
                    assertThat(record).isNotNull();
                    recordCount++;
                }
                assertThat(recordCount).isEqualTo(TestData.DATA1.size());
            }
        }
    }

    @Test
    void testV2FormatWithoutStatistics() throws Exception {
        // Create test data without statistics using V1 format (which doesn't support statistics)
        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_v1_no_stats.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            LOG_MAGIC_VALUE_V1,
                            Collections.singletonList(new Object[] {0, "abc"}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V1);

            // Test that getStatistics returns empty when magic version doesn't support statistics
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isEmpty();
            }
        }
    }

    @Test
    void testGetStatisticsWithNullContext() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_null_context.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            // Test that getStatistics returns empty when context is null
            Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(null);
            assertThat(statisticsOpt).isEmpty();
        }
    }

    @Test
    void testGetStatisticsWithMissingSchemaInGetter() throws Exception {
        // Create test data with a schemaId that is NOT registered in the schemaGetter
        int unregisteredSchemaId = 999;
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, unregisteredSchemaId);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_invalid_schema.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            // When the schemaGetter cannot find the batch's schemaId, getStatistics should
            // gracefully return empty (the exception is caught internally)
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                // The batch's schemaId (999) is not in TEST_SCHEMA_GETTER, so it should return
                // empty
                assertThat(statisticsOpt).isEmpty();
            }
        }
    }

    @Test
    void testStatisticsCreation() throws Exception {
        // Create test data with statistics using V2 format
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Get the batch
        LogRecordBatch memoryBatch = memoryLogRecords.batches().iterator().next();
        assertThat(memoryBatch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

        // Test that the memory batch has statistics
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            Optional<LogRecordBatchStatistics> memoryStatsOpt =
                    memoryBatch.getStatistics(readContext);
            assertThat(memoryStatsOpt).isPresent();

            LogRecordBatchStatistics memoryStats = memoryStatsOpt.get();

            // Verify statistics content
            assertThat(memoryStats.getMinValues().getInt(0)).isEqualTo(1);
            assertThat(memoryStats.getMaxValues().getInt(0)).isEqualTo(10);
            assertThat(memoryStats.getMinValues().getString(1).toString()).isEqualTo("a");
            assertThat(memoryStats.getMaxValues().getString(1).toString()).isEqualTo("j");
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testLoadArrowBatch(byte recordBatchMagic) throws Exception {
        List<Object[]> data = DATA1;
        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_arrow_batch.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            recordBatchMagic,
                            data,
                            LogFormat.ARROW,
                            true));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());
            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            TestingSchemaGetter schemaGetter =
                    new TestingSchemaGetter(DEFAULT_SCHEMA_ID, DATA1_SCHEMA);
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, schemaGetter)) {
                try (ArrowBatchData arrowBatchData = batch.loadArrowBatch(readContext)) {
                    assertThat(arrowBatchData.getRecordCount()).isEqualTo(data.size());
                    assertThat(arrowBatchData.getBaseLogOffset()).isEqualTo(0L);
                    assertThat(arrowBatchData.getSchemaId()).isEqualTo(DEFAULT_SCHEMA_ID);

                    org.apache.arrow.vector.VectorSchemaRoot root =
                            arrowBatchData.getVectorSchemaRoot();
                    IntVector intVector = (IntVector) root.getVector(0);
                    VarCharVector stringVector = (VarCharVector) root.getVector(1);
                    for (int i = 0; i < data.size(); i++) {
                        assertThat(intVector.get(i)).isEqualTo(data.get(i)[0]);
                        assertThat(stringVector.getObject(i).toString()).isEqualTo(data.get(i)[1]);
                    }
                }
            }
        }
    }

    private static MemoryLogRecords createProjectedTestBatch(byte magic, boolean appendOnly)
            throws Exception {
        return createRecordsWithoutBaseLogOffset(
                DATA1_ROW_TYPE,
                DEFAULT_SCHEMA_ID,
                42L,
                1234L,
                magic,
                DATA1,
                LogFormat.ARROW,
                appendOnly);
    }

    private static LogRecordReadContext createProjectedReadContext() {
        return LogRecordReadContext.createReadContext(
                DATA1_TABLE_INFO,
                true,
                LogRecordReadContext.SchemaResolution.TARGET,
                Projection.of(new int[] {0}),
                TEST_SCHEMA_GETTER,
                null);
    }

    private static List<long[]> projectedReadRanges(MemoryLogRecords memory, int... bufferIndexes)
            throws IOException {
        DefaultLogRecordBatch batch = (DefaultLogRecordBatch) memory.batches().iterator().next();
        int arrowOffset = recordBatchHeaderSize(batch.magic()) + batch.getStatisticsLength();
        if (!batch.isAppendOnly()) {
            arrowOffset += batch.getRecordCount();
        }
        try (ReadChannel metadataChannel =
                new ReadChannel(
                        new ByteBufferReadableChannel(
                                memory.getMemorySegment()
                                        .wrap(
                                                memory.getPosition() + arrowOffset,
                                                memory.sizeInBytes() - arrowOffset)))) {
            MessageMetadataResult metadata = MessageSerializer.readMessage(metadataChannel);
            long bodyOffset = arrowOffset + metadataChannel.bytesRead();
            RecordBatch arrowBatch = (RecordBatch) metadata.getMessage().header(new RecordBatch());
            List<long[]> ranges = new ArrayList<>();
            // Log headers, statistics, change types and IPC metadata precede the column buffers.
            ranges.add(new long[] {0, bodyOffset});
            for (int index : bufferIndexes) {
                long start = bodyOffset + arrowBatch.buffers(index).offset();
                ranges.add(new long[] {start, start + arrowBatch.buffers(index).length()});
            }
            return ranges;
        }
    }

    private static Stream<Arguments> projectedFileModes() {
        List<Arguments> modes = new ArrayList<>();
        for (byte magic : new byte[] {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1, LOG_MAGIC_VALUE_V2}) {
            for (ArrowCompressionType compression : ArrowCompressionType.values()) {
                for (boolean appendOnly : new boolean[] {false, true}) {
                    for (boolean checkCrc : new boolean[] {false, true}) {
                        modes.add(Arguments.of(magic, compression, appendOnly, checkCrc));
                    }
                }
            }
        }
        return modes.stream();
    }
}

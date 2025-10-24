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

import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileLogInputStream}. */
public class FileLogInputStreamTest extends LogTestBase {
    private @TempDir File tempDir;

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

            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(
                                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
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
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {

                // Test getStatistics method
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isPresent();

                LogRecordBatchStatistics statistics = statisticsOpt.get();
                assertThat(statistics.getMinValues()).isNotNull();
                assertThat(statistics.getMaxValues()).isNotNull();
                assertThat(statistics.getNullCounts()).isNotNull();

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
                                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
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
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
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
    void testGetStatisticsWithInvalidSchemaId() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_invalid_schema.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            // Test that getStatistics returns empty when schema is not found in context
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, 999)) { // Invalid schema ID
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isEmpty();
            }
        }
    }

    @Test
    void testOffsetCalculation() {
        // Check the statistics length offset in V2 format
        System.out.println("LOG_OVERHEAD: " + LogRecordBatchFormat.LOG_OVERHEAD);
        int statisticsOffsetOffset =
                LogRecordBatchFormat.statisticsOffsetOffset(
                        LogRecordBatchFormat.LOG_MAGIC_VALUE_V2);
        System.out.println("Absolute statistics offset offset: " + statisticsOffsetOffset);
        int relativeStatisticsOffsetOffset =
                statisticsOffsetOffset - LogRecordBatchFormat.LOG_OVERHEAD;
        System.out.println("Relative statistics offset offset: " + relativeStatisticsOffsetOffset);
        assertThat(relativeStatisticsOffsetOffset).isEqualTo(40); // 52 - 12 = 40
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
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
            Optional<LogRecordBatchStatistics> memoryStatsOpt =
                    memoryBatch.getStatistics(readContext);
            assertThat(memoryStatsOpt).isPresent();

            LogRecordBatchStatistics memoryStats = memoryStatsOpt.get();
            System.out.println("Memory batch statistics: " + memoryStats);

            // Verify statistics content
            assertThat(memoryStats.getMinValues().getInt(0)).isEqualTo(1);
            assertThat(memoryStats.getMaxValues().getInt(0)).isEqualTo(10);
            assertThat(memoryStats.getMinValues().getString(1).toString()).isEqualTo("a");
            assertThat(memoryStats.getMaxValues().getString(1).toString()).isEqualTo("j");
        }
    }

    /**
     * Provide test arguments for getBytesView trimStatistics parameterized test. Each argument
     * contains: magic version, trimStatistics flag, expected description
     */
    private static Stream<Arguments> provideTrimStatisticsArguments() {
        return Stream.of(
                Arguments.of(
                        LOG_MAGIC_VALUE_V0,
                        true,
                        "V0 with trimStatistics=true (should have no effect)"),
                Arguments.of(LOG_MAGIC_VALUE_V0, false, "V0 with trimStatistics=false"),
                Arguments.of(
                        LOG_MAGIC_VALUE_V1,
                        true,
                        "V1 with trimStatistics=true (should have no effect)"),
                Arguments.of(LOG_MAGIC_VALUE_V1, false, "V1 with trimStatistics=false"),
                Arguments.of(
                        LOG_MAGIC_VALUE_V2,
                        true,
                        "V2 with trimStatistics=true (should trim statistics)"),
                Arguments.of(
                        LOG_MAGIC_VALUE_V2,
                        false,
                        "V2 with trimStatistics=false (should keep statistics)"));
    }

    @ParameterizedTest
    @MethodSource("provideTrimStatisticsArguments")
    void testGetBytesViewTrimStatistics(byte magic, boolean trimStatistics, String description)
            throws Exception {
        // Create test data based on magic version
        MemoryLogRecords memoryLogRecords;
        if (magic == LOG_MAGIC_VALUE_V2) {
            // For V2, create records with statistics
            memoryLogRecords =
                    LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                            TestData.DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        } else {
            // For V0 and V1, create records without statistics
            memoryLogRecords =
                    createRecordsWithoutBaseLogOffset(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            magic,
                            TestData.DATA1,
                            LogFormat.ARROW);
        }

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(
                        new File(tempDir, "test_trim_" + magic + "_" + trimStatistics + ".tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(magic);

            // Get BytesView with and without trimming
            BytesView originalBytesView = batch.getBytesView(false);
            BytesView trimmedBytesView = batch.getBytesView(trimStatistics);

            // Verify batch size behavior based on version and trimStatistics flag
            if (magic < LOG_MAGIC_VALUE_V2 || !trimStatistics) {
                // For V0/V1, or when trimStatistics=false, sizes should be identical
                assertThat(trimmedBytesView.getBytesLength())
                        .as(description + ": BytesView size should be unchanged")
                        .isEqualTo(originalBytesView.getBytesLength());
                assertThat(trimmedBytesView.getBytesLength())
                        .as(description + ": BytesView size should equal batch sizeInBytes")
                        .isEqualTo(batch.sizeInBytes());
            } else {
                // For V2 with trimStatistics=true, check if statistics exist and affect size
                try (LogRecordReadContext readContext =
                        LogRecordReadContext.createArrowReadContext(
                                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {

                    Optional<LogRecordBatchStatistics> statisticsOpt =
                            batch.getStatistics(readContext);
                    if (statisticsOpt.isPresent()) {
                        // If statistics exist, trimmed size should be smaller
                        assertThat(trimmedBytesView.getBytesLength())
                                .as(
                                        description
                                                + ": Trimmed BytesView should be smaller when statistics exist")
                                .isLessThan(originalBytesView.getBytesLength());

                        // Verify that the trimmed size excludes statistics data
                        // For V2 format with statistics, the original size includes statistics
                        // while trimmed size should only include header + records
                        assertThat(trimmedBytesView.getBytesLength())
                                .as(
                                        description
                                                + ": Trimmed size should be less than full batch size")
                                .isLessThan(batch.sizeInBytes());
                    } else {
                        // If no statistics, trimmed and original should be the same
                        assertThat(trimmedBytesView.getBytesLength())
                                .as(
                                        description
                                                + ": BytesView size should be unchanged when no statistics")
                                .isEqualTo(originalBytesView.getBytesLength());
                    }
                }
            }

            // Verify statistics parsing capability from original batch
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {

                Optional<LogRecordBatchStatistics> originalStatisticsOpt =
                        batch.getStatistics(readContext);

                if (magic >= LOG_MAGIC_VALUE_V2) {
                    // Original V2 batch should have statistics when created with
                    // LogRecordBatchStatisticsTestUtils
                    assertThat(originalStatisticsOpt)
                            .as(description + ": Original V2 batch should have statistics")
                            .isPresent();
                } else {
                    // V0 and V1 should not have statistics
                    assertThat(originalStatisticsOpt)
                            .as(description + ": V0/V1 batch should not have statistics")
                            .isEmpty();
                }
            }

            // Verify statistics parsing from BytesView-reconstructed batch
            // This is the key test: statistics should be unavailable from trimmed BytesView
            BytesViewLogRecords trimmedLogRecords = new BytesViewLogRecords(trimmedBytesView);
            LogRecordBatch reconstructedBatch = trimmedLogRecords.batches().iterator().next();

            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {

                Optional<LogRecordBatchStatistics> reconstructedStatisticsOpt =
                        reconstructedBatch.getStatistics(readContext);

                if (magic >= LOG_MAGIC_VALUE_V2 && trimStatistics) {
                    // For V2 with trimStatistics=true, reconstructed batch should NOT have
                    // statistics
                    assertThat(reconstructedStatisticsOpt)
                            .as(description + ": Trimmed V2 batch should not have statistics")
                            .isEmpty();
                } else if (magic >= LOG_MAGIC_VALUE_V2 && !trimStatistics) {
                    // For V2 with trimStatistics=false, reconstructed batch should still have
                    // statistics
                    assertThat(reconstructedStatisticsOpt)
                            .as(description + ": Non-trimmed V2 batch should preserve statistics")
                            .isPresent();

                    LogRecordBatchStatistics statistics = reconstructedStatisticsOpt.get();
                    // Verify specific statistics content for DATA1
                    assertThat(statistics.getMinValues().getInt(0))
                            .as(description + ": Min value of first column should be 1")
                            .isEqualTo(1);
                    assertThat(statistics.getMaxValues().getInt(0))
                            .as(description + ": Max value of first column should be 10")
                            .isEqualTo(10);
                    assertThat(statistics.getNullCounts()[0])
                            .as(description + ": First column should have no nulls")
                            .isEqualTo(0);

                    // Verify string statistics
                    assertThat(statistics.getMinValues().getString(1).toString())
                            .as(description + ": Min string value should be 'a'")
                            .isEqualTo("a");
                    assertThat(statistics.getMaxValues().getString(1).toString())
                            .as(description + ": Max string value should be 'j'")
                            .isEqualTo("j");
                } else {
                    // V0 and V1 should not have statistics regardless of trimStatistics
                    assertThat(reconstructedStatisticsOpt)
                            .as(description + ": V0/V1 batch should not have statistics")
                            .isEmpty();
                }
            }

            // Additional verification: ensure the batch can still be read correctly
            // regardless of the BytesView used.
            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(
                                    DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
                    CloseableIterator<LogRecord> iterator =
                            reconstructedBatch.records(readContext)) {

                int recordCount = 0;
                while (iterator.hasNext()) {
                    LogRecord record = iterator.next();
                    assertThat(record).isNotNull();
                    assertThat(record.getRow().getFieldCount()).isEqualTo(2);
                    recordCount++;
                }

                assertThat(recordCount)
                        .as(description + ": Should read all expected records")
                        .isEqualTo(TestData.DATA1.size());
            }
        }
    }

    @Test
    void testGetBytesViewTrimStatisticsEdgeCases() throws Exception {
        // Test edge case: V2 batch without statistics (statisticsOffset = 0)
        MemoryLogRecords memoryRecordsNoStats =
                createRecordsWithoutBaseLogOffset(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0L,
                        -1L,
                        LOG_MAGIC_VALUE_V2,
                        Collections.singletonList(new Object[] {1, "test"}),
                        LogFormat.ARROW);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_v2_no_stats.tmp"))) {
            fileLogRecords.append(memoryRecordsNoStats);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

            // For V2 without statistics, trimming should have no effect
            BytesView originalBytesView = batch.getBytesView(false);
            BytesView trimmedBytesView = batch.getBytesView(true);

            assertThat(trimmedBytesView.getBytesLength())
                    .as("V2 without statistics: trimmed size should equal original")
                    .isEqualTo(originalBytesView.getBytesLength());

            // Verify no statistics are present
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID)) {

                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt)
                        .as("V2 batch without statistics should return empty")
                        .isEmpty();
            }
        }
    }
}

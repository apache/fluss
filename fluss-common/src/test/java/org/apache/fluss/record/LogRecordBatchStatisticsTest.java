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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive test for getStatistics method of both DefaultLogRecordBatch and
 * FileChannelLogRecordBatch implementations.
 */
public class LogRecordBatchStatisticsTest extends LogTestBase {

    private @TempDir File tempDir;

    // Test data with different data types for comprehensive statistics testing
    private static final List<Object[]> MIXED_DATA =
            Arrays.asList(
                    new Object[] {1, "a", 10.5, true},
                    new Object[] {2, "b", 20.3, false},
                    new Object[] {3, "c", 15.7, true},
                    new Object[] {4, "d", 8.9, false},
                    new Object[] {5, "e", 30.1, true});

    private static final RowType MIXED_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("value", DataTypes.DOUBLE()),
                    new DataField("flag", DataTypes.BOOLEAN()));

    // Test data with null values
    private static final List<Object[]> DATA_WITH_NULLS =
            Arrays.asList(
                    new Object[] {1, "a", 10.5},
                    new Object[] {null, "b", 20.3},
                    new Object[] {3, null, 15.7},
                    new Object[] {4, "d", null},
                    new Object[] {5, "e", 30.1});

    private static final RowType DATA_WITH_NULLS_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("value", DataTypes.DOUBLE()));

    @Test
    void testDefaultLogRecordBatchGetStatisticsWithValidData() throws Exception {
        // Create test data with statistics using V2 format
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Get the batch
        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isInstanceOf(DefaultLogRecordBatch.class);
        assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

        // Test getStatistics with valid context
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(MIXED_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
            Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
            assertThat(statisticsOpt).isPresent();

            LogRecordBatchStatistics statistics = statisticsOpt.get();
            assertThat(statistics.getMinValues()).isNotNull();
            assertThat(statistics.getMaxValues()).isNotNull();
            assertThat(statistics.getNullCounts()).isNotNull();

            // Verify statistics content
            assertThat(statistics.getMinValues().getInt(0)).isEqualTo(1); // min id
            assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(5); // max id
            assertThat(statistics.getMinValues().getDouble(2)).isEqualTo(8.9); // min value
            assertThat(statistics.getMaxValues().getDouble(2)).isEqualTo(30.1); // max value
            assertThat(statistics.getMinValues().getBoolean(3)).isEqualTo(false); // min flag
            assertThat(statistics.getMaxValues().getBoolean(3)).isEqualTo(true); // max flag

            // Verify null counts (should be 0 for this data)
            assertThat(statistics.getNullCounts()[0]).isEqualTo(0);
            assertThat(statistics.getNullCounts()[1]).isEqualTo(0);
            assertThat(statistics.getNullCounts()[2]).isEqualTo(0);
            assertThat(statistics.getNullCounts()[3]).isEqualTo(0);
        }
    }

    @Test
    void testDefaultLogRecordBatchGetStatisticsWithNullValues() throws Exception {
        // Create test data with null values
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        DATA_WITH_NULLS, DATA_WITH_NULLS_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isInstanceOf(DefaultLogRecordBatch.class);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA_WITH_NULLS_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
            Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
            assertThat(statisticsOpt).isPresent();

            LogRecordBatchStatistics statistics = statisticsOpt.get();
            assertThat(statistics.getNullCounts()).isNotNull();

            // Verify null counts
            assertThat(statistics.getNullCounts()[0]).isEqualTo(1); // one null in id field
            assertThat(statistics.getNullCounts()[1]).isEqualTo(1); // one null in name field
            assertThat(statistics.getNullCounts()[2]).isEqualTo(1); // one null in value field
        }
    }

    @Test
    void testDefaultLogRecordBatchGetStatisticsWithNullContext() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isInstanceOf(DefaultLogRecordBatch.class);

        // Test with null context
        Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(null);
        assertThat(statisticsOpt).isEmpty();
    }

    @Test
    void testDefaultLogRecordBatchGetStatisticsWithInvalidSchemaId() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isInstanceOf(DefaultLogRecordBatch.class);

        // Test with invalid schema ID
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        MIXED_ROW_TYPE, 999)) { // Invalid schema ID
            Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
            assertThat(statisticsOpt).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testDefaultLogRecordBatchGetStatisticsWithOldMagicVersions(byte magic) throws Exception {
        // Create test data with old magic versions (which don't support statistics)
        MemoryLogRecords memoryLogRecords =
                createRecordsWithoutBaseLogOffset(
                        MIXED_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0L,
                        -1L,
                        magic,
                        Collections.singletonList(new Object[] {1, "a", 10.5, true}),
                        LogFormat.ARROW);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isInstanceOf(DefaultLogRecordBatch.class);
        assertThat(batch.magic()).isEqualTo(magic);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(MIXED_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
            Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
            assertThat(statisticsOpt).isEmpty();
        }
    }

    @Test
    void testDefaultLogRecordBatchGetStatisticsCaching() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isInstanceOf(DefaultLogRecordBatch.class);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(MIXED_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
            // First call
            Optional<LogRecordBatchStatistics> statisticsOpt1 = batch.getStatistics(readContext);
            assertThat(statisticsOpt1).isPresent();

            // Second call - should return cached result
            Optional<LogRecordBatchStatistics> statisticsOpt2 = batch.getStatistics(readContext);
            assertThat(statisticsOpt2).isPresent();
            assertThat(statisticsOpt2.get()).isSameAs(statisticsOpt1.get());

            // Third call - should return cached result
            Optional<LogRecordBatchStatistics> statisticsOpt3 = batch.getStatistics(readContext);
            assertThat(statisticsOpt3).isPresent();
            assertThat(statisticsOpt3.get()).isSameAs(statisticsOpt1.get());
        }
    }

    @Test
    void testDefaultLogRecordBatchGetStatisticsCacheReset() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        DefaultLogRecordBatch batch =
                (DefaultLogRecordBatch) memoryLogRecords.batches().iterator().next();

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(MIXED_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
            // First call
            Optional<LogRecordBatchStatistics> statisticsOpt1 = batch.getStatistics(readContext);
            assertThat(statisticsOpt1).isPresent();

            // Point to new memory segment - should reset cache
            MemorySegment newSegment = MemorySegment.wrap(new byte[1024]);
            batch.pointTo(newSegment, 0);

            // Second call after cache reset - should return empty (new segment has no valid data)
            Optional<LogRecordBatchStatistics> statisticsOpt2 = batch.getStatistics(readContext);
            assertThat(statisticsOpt2).isEmpty();
        }
    }

    // ==================== FileChannelLogRecordBatch Tests ====================

    @Test
    void testFileChannelLogRecordBatchGetStatisticsWithValidData() throws Exception {
        // Create test data with statistics using V2 format
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_valid_data.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

            // Test getStatistics with valid context
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            MIXED_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isPresent();

                LogRecordBatchStatistics statistics = statisticsOpt.get();
                assertThat(statistics.getMinValues()).isNotNull();
                assertThat(statistics.getMaxValues()).isNotNull();
                assertThat(statistics.getNullCounts()).isNotNull();

                // Verify statistics content
                assertThat(statistics.getMinValues().getInt(0)).isEqualTo(1); // min id
                assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(5); // max id
                assertThat(statistics.getMinValues().getDouble(2)).isEqualTo(8.9); // min value
                assertThat(statistics.getMaxValues().getDouble(2)).isEqualTo(30.1); // max value
                assertThat(statistics.getMinValues().getBoolean(3)).isEqualTo(false); // min flag
                assertThat(statistics.getMaxValues().getBoolean(3)).isEqualTo(true); // max flag

                // Verify null counts (should be 0 for this data)
                assertThat(statistics.getNullCounts()[0]).isEqualTo(0);
                assertThat(statistics.getNullCounts()[1]).isEqualTo(0);
                assertThat(statistics.getNullCounts()[2]).isEqualTo(0);
                assertThat(statistics.getNullCounts()[3]).isEqualTo(0);
            }
        }
    }

    @Test
    void testFileChannelLogRecordBatchGetStatisticsWithNullValues() throws Exception {
        // Create test data with null values
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        DATA_WITH_NULLS, DATA_WITH_NULLS_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_null_values.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            DATA_WITH_NULLS_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isPresent();

                LogRecordBatchStatistics statistics = statisticsOpt.get();
                assertThat(statistics.getNullCounts()).isNotNull();

                // Verify null counts
                assertThat(statistics.getNullCounts()[0]).isEqualTo(1); // one null in id field
                assertThat(statistics.getNullCounts()[1]).isEqualTo(1); // one null in name field
                assertThat(statistics.getNullCounts()[2]).isEqualTo(1); // one null in value field
            }
        }
    }

    @Test
    void testFileChannelLogRecordBatchGetStatisticsWithNullContext() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_null_context.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            // Test with null context
            Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(null);
            assertThat(statisticsOpt).isEmpty();
        }
    }

    @Test
    void testFileChannelLogRecordBatchGetStatisticsWithInvalidSchemaId() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_invalid_schema.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            // Test with invalid schema ID
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            MIXED_ROW_TYPE, 999)) { // Invalid schema ID
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testFileChannelLogRecordBatchGetStatisticsWithOldMagicVersions(byte magic)
            throws Exception {
        // Create test data with old magic versions (which don't support statistics)
        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_old_magic.tmp"))) {
            fileLogRecords.append(
                    createRecordsWithoutBaseLogOffset(
                            MIXED_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            0L,
                            -1L,
                            magic,
                            Collections.singletonList(new Object[] {1, "a", 10.5, true}),
                            LogFormat.ARROW));
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();
            assertThat(batch.magic()).isEqualTo(magic);

            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            MIXED_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isEmpty();
            }
        }
    }

    @Test
    void testFileChannelLogRecordBatchGetStatisticsCaching() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_caching.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            MIXED_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
                // First call
                Optional<LogRecordBatchStatistics> statisticsOpt1 =
                        batch.getStatistics(readContext);
                assertThat(statisticsOpt1).isPresent();

                // Second call - should return cached result
                Optional<LogRecordBatchStatistics> statisticsOpt2 =
                        batch.getStatistics(readContext);
                assertThat(statisticsOpt2).isPresent();
                assertThat(statisticsOpt2.get()).isSameAs(statisticsOpt1.get());

                // Third call - should return cached result
                Optional<LogRecordBatchStatistics> statisticsOpt3 =
                        batch.getStatistics(readContext);
                assertThat(statisticsOpt3).isPresent();
                assertThat(statisticsOpt3.get()).isSameAs(statisticsOpt1.get());
            }
        }
    }

    @Test
    void testFileChannelLogRecordBatchGetStatisticsErrorHandling() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_error_handling.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            // Test with context that has wrong row type (should handle gracefully)
            RowType wrongRowType = DataTypes.ROW(new DataField("wrong_field", DataTypes.INT()));

            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(wrongRowType, DEFAULT_SCHEMA_ID)) {
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                // Should handle the error gracefully and return empty
                assertThat(statisticsOpt).isEmpty();
            }
        }
    }

    @Test
    void testFileChannelLogRecordBatchGetStatisticsDataIntegrity() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords =
                FileLogRecords.open(new File(tempDir, "test_data_integrity.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch).isNotNull();

            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            MIXED_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
                // Test statistics reading
                Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
                assertThat(statisticsOpt).isPresent();

                // Test that records can still be read correctly after statistics access
                try (CloseableIterator<LogRecord> iterator = batch.records(readContext)) {
                    assertThat(iterator.hasNext()).isTrue();
                    int recordCount = 0;
                    while (iterator.hasNext()) {
                        LogRecord record = iterator.next();
                        assertThat(record).isNotNull();
                        assertThat(record.getRow().getFieldCount()).isEqualTo(4);
                        recordCount++;
                    }
                    assertThat(recordCount).isEqualTo(MIXED_DATA.size());
                }

                // Test statistics again to ensure they're still accessible
                Optional<LogRecordBatchStatistics> statisticsOpt2 =
                        batch.getStatistics(readContext);
                assertThat(statisticsOpt2).isPresent();
                assertThat(statisticsOpt2.get()).isSameAs(statisticsOpt.get());
            }
        }
    }

    // ==================== Comparison Tests ====================

    @Test
    void testDefaultVsFileChannelLogRecordBatchStatisticsConsistency() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchTestUtils.createLogRecordsWithStatistics(
                        MIXED_DATA, MIXED_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Get DefaultLogRecordBatch statistics
        LogRecordBatch defaultBatch = memoryLogRecords.batches().iterator().next();
        assertThat(defaultBatch).isInstanceOf(DefaultLogRecordBatch.class);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(MIXED_ROW_TYPE, DEFAULT_SCHEMA_ID)) {
            Optional<LogRecordBatchStatistics> defaultStatsOpt =
                    defaultBatch.getStatistics(readContext);
            assertThat(defaultStatsOpt).isPresent();
            LogRecordBatchStatistics defaultStats = defaultStatsOpt.get();

            // Create FileChannelLogRecordBatch and get statistics
            try (FileLogRecords fileLogRecords =
                    FileLogRecords.open(new File(tempDir, "test_consistency.tmp"))) {
                fileLogRecords.append(memoryLogRecords);
                fileLogRecords.flush();

                FileLogInputStream logInputStream =
                        new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());

                FileLogInputStream.FileChannelLogRecordBatch fileBatch = logInputStream.nextBatch();
                assertThat(fileBatch).isNotNull();

                Optional<LogRecordBatchStatistics> fileStatsOpt =
                        fileBatch.getStatistics(readContext);
                assertThat(fileStatsOpt).isPresent();
                LogRecordBatchStatistics fileStats = fileStatsOpt.get();

                // Verify that both implementations return the same statistics
                assertThat(fileStats.getMinValues().getInt(0))
                        .isEqualTo(defaultStats.getMinValues().getInt(0));
                assertThat(fileStats.getMaxValues().getInt(0))
                        .isEqualTo(defaultStats.getMaxValues().getInt(0));
                assertThat(fileStats.getMinValues().getDouble(2))
                        .isEqualTo(defaultStats.getMinValues().getDouble(2));
                assertThat(fileStats.getMaxValues().getDouble(2))
                        .isEqualTo(defaultStats.getMaxValues().getDouble(2));
                assertThat(fileStats.getMinValues().getBoolean(3))
                        .isEqualTo(defaultStats.getMinValues().getBoolean(3));
                assertThat(fileStats.getMaxValues().getBoolean(3))
                        .isEqualTo(defaultStats.getMaxValues().getBoolean(3));

                // Verify null counts are the same
                for (int i = 0; i < fileStats.getNullCounts().length; i++) {
                    assertThat(fileStats.getNullCounts()[i])
                            .isEqualTo(defaultStats.getNullCounts()[i]);
                }
            }
        }
    }
}

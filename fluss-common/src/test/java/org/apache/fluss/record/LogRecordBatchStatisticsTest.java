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
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.aligned.AlignedRow;
import org.apache.fluss.row.aligned.AlignedRowWriter;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Comprehensive test for getStatistics method of both DefaultLogRecordBatch and
 * FileChannelLogRecordBatch implementations, and DefaultLogRecordBatchStatistics class.
 */
public class LogRecordBatchStatisticsTest extends LogTestBase {

    private @TempDir File tempDir;
    private static final int SCHEMA_ID = 1;
    private static final RowType TEST_ROW_TYPE =
            DataTypes.ROW(
                    new DataField("id", DataTypes.INT()),
                    new DataField("name", DataTypes.STRING()),
                    new DataField("value", DataTypes.DOUBLE()));

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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
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

    // ==================== DefaultLogRecordBatchStatistics Tests ====================

    @Test
    void testDefaultLogRecordBatchStatisticsConstructorAndBasicMethods() {
        // Create test data
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        int position = 0;
        int size = 100;
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int minValuesOffset = 50;
        int maxValuesOffset = 80;
        int minValuesSize = 20;
        int maxValuesSize = 15;
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        position,
                        size,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        minValuesOffset,
                        maxValuesOffset,
                        minValuesSize,
                        maxValuesSize,
                        statsIndexMapping);

        // Test basic accessors
        assertThat(stats.getSegment()).isEqualTo(segment);
        assertThat(stats.getPosition()).isEqualTo(position);
        assertThat(stats.getSize()).isEqualTo(size);
        assertThat(stats.getRowType()).isEqualTo(TEST_ROW_TYPE);
        assertThat(stats.getSchemaId()).isEqualTo(SCHEMA_ID);
        assertThat(stats.getNullCounts()).isEqualTo(nullCounts);
        assertThat(stats.hasMinValues()).isTrue();
        assertThat(stats.hasMaxValues()).isTrue();

        // Test hasColumnStatistics
        assertThat(stats.hasColumnStatistics(0)).isTrue();
        assertThat(stats.hasColumnStatistics(1)).isTrue();
        assertThat(stats.hasColumnStatistics(2)).isTrue();

        // Test getNullCount
        assertThat(stats.getNullCount(0)).isEqualTo(0L);
        assertThat(stats.getNullCount(1)).isEqualTo(1L);
        assertThat(stats.getNullCount(2)).isEqualTo(2L);
    }

    @Test
    void testDefaultLogRecordBatchStatisticsConstructorWithPartialStatistics() {
        // Create test data with partial statistics (only for columns 0 and 2)
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        int position = 0;
        int size = 100;
        Long[] nullCounts = new Long[] {0L, 2L}; // Only for columns 0 and 2
        int minValuesOffset = 50;
        int maxValuesOffset = 80;
        int minValuesSize = 20;
        int maxValuesSize = 15;
        int[] statsIndexMapping = new int[] {0, 2}; // Skip column 1

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        position,
                        size,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        minValuesOffset,
                        maxValuesOffset,
                        minValuesSize,
                        maxValuesSize,
                        statsIndexMapping);

        // Test hasColumnStatistics - should be true for 0 and 2, false for 1
        assertThat(stats.hasColumnStatistics(0)).isTrue();
        assertThat(stats.hasColumnStatistics(1)).isFalse();
        assertThat(stats.hasColumnStatistics(2)).isTrue();
    }

    @Test
    void testDefaultLogRecordBatchStatisticsConstructorWithNoMinMaxValues() {
        // Create test data with no min/max values
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        int position = 0;
        int size = 50;
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int minValuesOffset = 0;
        int maxValuesOffset = 0;
        int minValuesSize = 0;
        int maxValuesSize = 0;
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        position,
                        size,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        minValuesOffset,
                        maxValuesOffset,
                        minValuesSize,
                        maxValuesSize,
                        statsIndexMapping);

        // Test no min/max values available
        assertThat(stats.hasMinValues()).isFalse();
        assertThat(stats.hasMaxValues()).isFalse();
        assertThat(stats.getMinValues()).isNull();
        assertThat(stats.getMaxValues()).isNull();
    }

    @Test
    void testDefaultLogRecordBatchStatisticsWithSerializedStatistics() throws IOException {
        // Create test rows
        InternalRow minRow = DataTestUtils.row(new Object[] {1, "a", 10.5});
        InternalRow maxRow = DataTestUtils.row(new Object[] {100, "z", 99.9});
        Long[] nullCounts = new Long[] {0L, 2L, 1L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        // Create writer and serialize statistics
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(TEST_ROW_TYPE, statsIndexMapping);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);

        // Convert to AlignedRow for serialization
        AlignedRow minBinaryRow = createBinaryRow(minRow);
        AlignedRow maxBinaryRow = createBinaryRow(maxRow);

        writer.writeStatistics(minBinaryRow, maxBinaryRow, nullCounts, outputView);

        // Parse the written statistics
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, TEST_ROW_TYPE, SCHEMA_ID);

        // Verify parsed statistics
        assertThat(parsedStats).isNotNull();
        assertThat(parsedStats.getSchemaId()).isEqualTo(SCHEMA_ID);
        assertThat(parsedStats.getNullCounts()).isEqualTo(nullCounts);
        assertThat(parsedStats.hasMinValues()).isTrue();
        assertThat(parsedStats.hasMaxValues()).isTrue();

        // Verify min/max values can be accessed
        InternalRow parsedMinRow = parsedStats.getMinValues();
        InternalRow parsedMaxRow = parsedStats.getMaxValues();

        assertThat(parsedMinRow).isNotNull();
        assertThat(parsedMaxRow).isNotNull();

        assertThat(parsedMinRow.getInt(0)).isEqualTo(1);
        assertThat(parsedMinRow.getString(1).toString()).isEqualTo("a");
        assertThat(parsedMinRow.getDouble(2)).isEqualTo(10.5);

        assertThat(parsedMaxRow.getInt(0)).isEqualTo(100);
        assertThat(parsedMaxRow.getString(1).toString()).isEqualTo("z");
        assertThat(parsedMaxRow.getDouble(2)).isEqualTo(99.9);
    }

    @Test
    void testDefaultLogRecordBatchStatisticsFullRowWrapperBounds() throws IOException {
        // Create statistics with all columns
        InternalRow minRow = DataTestUtils.row(new Object[] {1, "a", 10.5});
        InternalRow maxRow = DataTestUtils.row(new Object[] {100, "z", 99.9});
        Long[] nullCounts = new Long[] {0L, 2L, 1L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        // Serialize and parse
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(TEST_ROW_TYPE, statsIndexMapping);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);

        AlignedRow minBinaryRow = createBinaryRow(minRow);
        AlignedRow maxBinaryRow = createBinaryRow(maxRow);

        writer.writeStatistics(minBinaryRow, maxBinaryRow, nullCounts, outputView);

        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, TEST_ROW_TYPE, SCHEMA_ID);

        // Test FullRowWrapper bounds checking
        InternalRow minValues = parsedStats.getMinValues();
        assertThat(minValues.getFieldCount()).isEqualTo(3);

        // Valid accesses should work
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(minValues.getString(1).toString()).isEqualTo("a");
        assertThat(minValues.getDouble(2)).isEqualTo(10.5);

        // Test cached min/max rows
        InternalRow cachedMinValues = parsedStats.getMinValues();
        assertThat(cachedMinValues).isSameAs(minValues); // Should return cached instance
    }

    @Test
    void testDefaultLogRecordBatchStatisticsEqualsAndHashCode() {
        MemorySegment segment1 = MemorySegment.allocateHeapMemory(512);
        MemorySegment segment2 = MemorySegment.allocateHeapMemory(512);

        // Create identical statistics objects
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats1 =
                new DefaultLogRecordBatchStatistics(
                        segment1,
                        0,
                        100,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        DefaultLogRecordBatchStatistics stats2 =
                new DefaultLogRecordBatchStatistics(
                        segment1,
                        0,
                        100,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        // Test equals
        assertThat(stats1).isEqualTo(stats2);
        assertThat(stats1.hashCode()).isEqualTo(stats2.hashCode());

        // Test not equals with different segment
        DefaultLogRecordBatchStatistics stats3 =
                new DefaultLogRecordBatchStatistics(
                        segment2,
                        0,
                        100,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        assertThat(stats1).isNotEqualTo(stats3);
    }

    @Test
    void testDefaultLogRecordBatchStatisticsToString() {
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        0,
                        100,
                        TEST_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        String toString = stats.toString();
        assertThat(toString).contains("DefaultLogRecordBatchStatistics");
        assertThat(toString).contains("hasMinValues=true");
        assertThat(toString).contains("hasMaxValues=true");
        assertThat(toString).contains("size=100");
    }

    @Test
    void testDefaultLogRecordBatchStatisticsPartialStatisticsWrapperExceptionHandling()
            throws IOException {
        // Create test data for partial statistics - only collect stats for columns 0 and 2 (skip
        // column 1)
        InternalRow minRow = DataTestUtils.row(1, 10.5); // Only id and value, skip name
        InternalRow maxRow = DataTestUtils.row(100, 99.9); // Only id and value, skip name
        Long[] nullCounts = new Long[] {0L, 1L}; // Null counts only for columns 0 and 2
        int[] statsIndexMapping = new int[] {0, 2}; // Skip column 1 (name column)

        // Create a row type that matches the partial stats
        RowType fullRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        // Serialize and parse statistics with partial columns
        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(fullRowType, statsIndexMapping);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);

        writer.writeStatistics(minRow, maxRow, nullCounts, outputView);

        // Create statistics with original TEST_ROW_TYPE (3 columns) but only partial mapping
        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, fullRowType, 0);

        // Verify that columns 0 and 2 have statistics, but column 1 doesn't
        assertThat(parsedStats.hasColumnStatistics(0)).isTrue();
        assertThat(parsedStats.hasColumnStatistics(1)).isFalse();
        assertThat(parsedStats.hasColumnStatistics(2)).isTrue();

        // Get min and max values wrapper
        InternalRow minValues = parsedStats.getMinValues();
        InternalRow maxValues = parsedStats.getMaxValues();

        // Valid accesses should work for columns with statistics (0 and 2)
        assertThat(minValues.getInt(0)).isEqualTo(1);
        assertThat(minValues.getDouble(2)).isEqualTo(10.5);
        assertThat(maxValues.getInt(0)).isEqualTo(100);
        assertThat(maxValues.getDouble(2)).isEqualTo(99.9);

        // Accessing column 1 (name) should throw IllegalArgumentException
        // since it's not available in the statistics
        assertThatThrownBy(() -> minValues.getString(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column index not available in underlying row data");

        assertThatThrownBy(() -> maxValues.getString(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column index not available in underlying row data");

        // Also test other access methods that should throw for column 1
        assertThatThrownBy(() -> minValues.isNullAt(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column index not available in underlying row data");

        // Test accessing out-of-bounds column
        assertThatThrownBy(() -> minValues.getInt(3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column index out of range");
    }

    private AlignedRow createBinaryRow(InternalRow row) {
        AlignedRow binaryRow = new AlignedRow(row.getFieldCount());
        AlignedRowWriter writer = new AlignedRowWriter(binaryRow);

        for (int i = 0; i < row.getFieldCount(); i++) {
            if (row.isNullAt(i)) {
                writer.setNullAt(i);
            } else {
                switch (TEST_ROW_TYPE.getTypeAt(i).getTypeRoot()) {
                    case INTEGER:
                        writer.writeInt(i, row.getInt(i));
                        break;
                    case STRING:
                        writer.writeString(i, row.getString(i));
                        break;
                    case DOUBLE:
                        writer.writeDouble(i, row.getDouble(i));
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported type: " + TEST_ROW_TYPE.getTypeAt(i));
                }
            }
        }

        writer.complete();
        return binaryRow;
    }
}

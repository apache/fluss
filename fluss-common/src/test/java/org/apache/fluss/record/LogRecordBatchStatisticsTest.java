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
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for getStatistics method of DefaultLogRecordBatch, FileChannelLogRecordBatch, and
 * DefaultLogRecordBatchStatistics.
 */
public class LogRecordBatchStatisticsTest extends LogTestBase {

    private @TempDir File tempDir;
    private static final int SCHEMA_ID = 1;

    // ==================== DefaultLogRecordBatch Tests ====================

    @Test
    void testDefaultLogRecordBatchGetStatisticsWithValidData() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.STATISTICS_WITH_BOOLEAN_DATA,
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        0L,
                        DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isInstanceOf(DefaultLogRecordBatch.class);
        assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        TEST_SCHEMA_GETTER)) {
            Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
            assertThat(statisticsOpt).isPresent();

            LogRecordBatchStatistics statistics = statisticsOpt.get();
            assertThat(statistics.getMinValues().getInt(0)).isEqualTo(1);
            assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(5);
            assertThat(statistics.getMinValues().getDouble(2)).isEqualTo(8.9);
            assertThat(statistics.getMaxValues().getDouble(2)).isEqualTo(30.1);
            assertThat(statistics.getMinValues().getBoolean(3)).isEqualTo(false);
            assertThat(statistics.getMaxValues().getBoolean(3)).isEqualTo(true);

            // V2 format: null counts extracted from Arrow metadata
            assertThat(statistics.getNullCounts()).containsExactly(0L, 0L, 0L, 0L);
        }
    }

    @Test
    void testDefaultLogRecordBatchGetStatisticsWithNullValues() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.STATISTICS_WITH_NULLS_DATA,
                        TestData.STATISTICS_WITH_NULLS_ROW_TYPE,
                        0L,
                        DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        TestData.STATISTICS_WITH_NULLS_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        TEST_SCHEMA_GETTER)) {
            LogRecordBatchStatistics statistics = batch.getStatistics(readContext).get();
            // V2 format: null counts extracted from Arrow metadata
            assertThat(statistics.getNullCounts()).containsExactly(1L, 1L, 1L);
        }
    }

    @Test
    void testDefaultLogRecordBatchGetStatisticsEdgeCases() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.STATISTICS_WITH_BOOLEAN_DATA,
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        0L,
                        DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();

        // Test with null context
        assertThat(batch.getStatistics(null)).isEmpty();

        // Test with invalid schema ID
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE, 999, TEST_SCHEMA_GETTER)) {
            assertThat(batch.getStatistics(readContext)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {LOG_MAGIC_VALUE_V0, LOG_MAGIC_VALUE_V1})
    void testDefaultLogRecordBatchGetStatisticsWithOldMagicVersions(byte magic) throws Exception {
        MemoryLogRecords memoryLogRecords =
                createRecordsWithoutBaseLogOffset(
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        0L,
                        -1L,
                        magic,
                        Collections.singletonList(TestData.STATISTICS_WITH_BOOLEAN_DATA.get(0)),
                        LogFormat.ARROW);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch.magic()).isEqualTo(magic);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        TEST_SCHEMA_GETTER)) {
            assertThat(batch.getStatistics(readContext)).isEmpty();
        }
    }

    @Test
    void testDefaultLogRecordBatchGetStatisticsCaching() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.STATISTICS_WITH_BOOLEAN_DATA,
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        0L,
                        DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        TEST_SCHEMA_GETTER)) {
            Optional<LogRecordBatchStatistics> stats1 = batch.getStatistics(readContext);
            Optional<LogRecordBatchStatistics> stats2 = batch.getStatistics(readContext);
            assertThat(stats2.get()).isSameAs(stats1.get());
        }
    }

    // ==================== FileChannelLogRecordBatch Tests ====================

    @Test
    void testFileChannelLogRecordBatchGetStatisticsWithValidData() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.STATISTICS_WITH_BOOLEAN_DATA,
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        0L,
                        DEFAULT_SCHEMA_ID);

        try (FileLogRecords fileLogRecords = FileLogRecords.open(new File(tempDir, "test.tmp"))) {
            fileLogRecords.append(memoryLogRecords);
            fileLogRecords.flush();

            FileLogInputStream logInputStream =
                    new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());
            FileLogInputStream.FileChannelLogRecordBatch batch = logInputStream.nextBatch();
            assertThat(batch.magic()).isEqualTo(LOG_MAGIC_VALUE_V2);

            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createArrowReadContext(
                            TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            TEST_SCHEMA_GETTER)) {
                LogRecordBatchStatistics statistics = batch.getStatistics(readContext).get();
                assertThat(statistics.getMinValues().getInt(0)).isEqualTo(1);
                assertThat(statistics.getMaxValues().getInt(0)).isEqualTo(5);
            }
        }
    }

    @Test
    void testDefaultVsFileChannelLogRecordBatchStatisticsConsistency() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        TestData.STATISTICS_WITH_BOOLEAN_DATA,
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        0L,
                        DEFAULT_SCHEMA_ID);

        LogRecordBatch defaultBatch = memoryLogRecords.batches().iterator().next();

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        TestData.STATISTICS_WITH_BOOLEAN_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        TEST_SCHEMA_GETTER)) {
            LogRecordBatchStatistics defaultStats = defaultBatch.getStatistics(readContext).get();

            try (FileLogRecords fileLogRecords =
                    FileLogRecords.open(new File(tempDir, "consistency.tmp"))) {
                fileLogRecords.append(memoryLogRecords);
                fileLogRecords.flush();

                FileLogInputStream logInputStream =
                        new FileLogInputStream(fileLogRecords, 0, fileLogRecords.sizeInBytes());
                FileLogInputStream.FileChannelLogRecordBatch fileBatch = logInputStream.nextBatch();
                LogRecordBatchStatistics fileStats = fileBatch.getStatistics(readContext).get();

                // Verify both implementations return the same statistics
                assertThat(fileStats.getMinValues().getInt(0))
                        .isEqualTo(defaultStats.getMinValues().getInt(0));
                assertThat(fileStats.getMaxValues().getInt(0))
                        .isEqualTo(defaultStats.getMaxValues().getInt(0));

                // Both paths now extract null counts from Arrow metadata
                assertThat(fileStats.getNullCounts()).isNotNull();
                assertThat(defaultStats.getNullCounts()).isNotNull();
                assertThat(defaultStats.getNullCounts()).containsExactly(fileStats.getNullCounts());
            }
        }
    }

    // ==================== DefaultLogRecordBatchStatistics Tests ====================

    @Test
    void testDefaultLogRecordBatchStatisticsBasicMethods() {
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        0,
                        100,
                        TestData.STATISTICS_BASIC_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        assertThat(stats.getSegment()).isEqualTo(segment);
        assertThat(stats.getSchemaId()).isEqualTo(SCHEMA_ID);
        assertThat(stats.getNullCounts()).isEqualTo(nullCounts);
        assertThat(stats.hasMinValues()).isTrue();
        assertThat(stats.hasMaxValues()).isTrue();
        assertThat(stats.hasColumnStatistics(0)).isTrue();
        assertThat(stats.getNullCounts()[0]).isEqualTo(0L);
        assertThat(stats.getNullCounts()[1]).isEqualTo(1L);
    }

    @Test
    void testDefaultLogRecordBatchStatisticsPartialColumns() {
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        Long[] nullCounts = new Long[] {0L, 2L};
        int[] statsIndexMapping = new int[] {0, 2}; // Skip column 1

        DefaultLogRecordBatchStatistics stats =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        0,
                        100,
                        TestData.STATISTICS_BASIC_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        assertThat(stats.hasColumnStatistics(0)).isTrue();
        assertThat(stats.hasColumnStatistics(1)).isFalse();
        assertThat(stats.hasColumnStatistics(2)).isTrue();
    }

    @Test
    void testDefaultLogRecordBatchStatisticsNoMinMaxValues() {
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        assertThatThrownBy(
                        () ->
                                new DefaultLogRecordBatchStatistics(
                                        segment,
                                        0,
                                        50,
                                        TestData.STATISTICS_BASIC_ROW_TYPE,
                                        SCHEMA_ID,
                                        nullCounts,
                                        0,
                                        0,
                                        0,
                                        10,
                                        statsIndexMapping))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("minValuesSize must be positive");

        assertThatThrownBy(
                        () ->
                                new DefaultLogRecordBatchStatistics(
                                        segment,
                                        0,
                                        50,
                                        TestData.STATISTICS_BASIC_ROW_TYPE,
                                        SCHEMA_ID,
                                        nullCounts,
                                        0,
                                        0,
                                        10,
                                        0,
                                        statsIndexMapping))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxValuesSize must be positive");
    }

    @Test
    void testDefaultLogRecordBatchStatisticsWithSerializedData() throws IOException {
        InternalRow minRow = DataTestUtils.row(new Object[] {1, "a", 10.5});
        InternalRow maxRow = DataTestUtils.row(new Object[] {100, "z", 99.9});
        int[] statsIndexMapping = new int[] {0, 1, 2};

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(
                        TestData.STATISTICS_BASIC_ROW_TYPE, statsIndexMapping);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);

        writer.writeStatistics(
                AlignedRow.from(TestData.STATISTICS_BASIC_ROW_TYPE, minRow),
                AlignedRow.from(TestData.STATISTICS_BASIC_ROW_TYPE, maxRow),
                outputView);

        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(
                        segment, 0, TestData.STATISTICS_BASIC_ROW_TYPE, SCHEMA_ID);

        assertThat(parsedStats.getSchemaId()).isEqualTo(SCHEMA_ID);
        // V2 format: null counts are null
        assertThat(parsedStats.getNullCounts()).isNull();
        assertThat(parsedStats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(parsedStats.getMaxValues().getInt(0)).isEqualTo(100);
    }

    @Test
    void testPartialStatisticsWrapperThrowsForUnavailableColumns() throws IOException {
        InternalRow minRow = DataTestUtils.row(1, 10.5);
        InternalRow maxRow = DataTestUtils.row(100, 99.9);
        int[] statsIndexMapping = new int[] {0, 2}; // Skip column 1

        RowType fullRowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()),
                        new DataField("value", DataTypes.DOUBLE()));

        LogRecordBatchStatisticsWriter writer =
                new LogRecordBatchStatisticsWriter(fullRowType, statsIndexMapping);

        MemorySegment segment = MemorySegment.allocateHeapMemory(1024);
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(segment);

        writer.writeStatistics(minRow, maxRow, outputView);

        DefaultLogRecordBatchStatistics parsedStats =
                LogRecordBatchStatisticsParser.parseStatistics(segment, 0, fullRowType, 0);

        // Valid accesses work
        assertThat(parsedStats.getMinValues().getInt(0)).isEqualTo(1);
        assertThat(parsedStats.getMinValues().getDouble(2)).isEqualTo(10.5);

        // Accessing unavailable column throws
        assertThatThrownBy(() -> parsedStats.getMinValues().getString(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column index not available");
    }

    @Test
    void testDefaultLogRecordBatchStatisticsEqualsAndHashCode() {
        MemorySegment segment = MemorySegment.allocateHeapMemory(512);
        Long[] nullCounts = new Long[] {0L, 1L, 2L};
        int[] statsIndexMapping = new int[] {0, 1, 2};

        DefaultLogRecordBatchStatistics stats1 =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        0,
                        100,
                        TestData.STATISTICS_BASIC_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        DefaultLogRecordBatchStatistics stats2 =
                new DefaultLogRecordBatchStatistics(
                        segment,
                        0,
                        100,
                        TestData.STATISTICS_BASIC_ROW_TYPE,
                        SCHEMA_ID,
                        nullCounts,
                        50,
                        80,
                        20,
                        15,
                        statsIndexMapping);

        assertThat(stats1).isEqualTo(stats2);
        assertThat(stats1.hashCode()).isEqualTo(stats2.hashCode());
    }
}

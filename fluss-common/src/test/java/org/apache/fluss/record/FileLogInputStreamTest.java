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
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V1;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V2;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
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
}

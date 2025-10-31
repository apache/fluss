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

import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.testutils.ListLogRecords;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link LogRecordBatchIterator} and {@link
 * LogRecordBatchIterator.FilteredLogRecordBatchIterator}.
 */
public class LogRecordBatchIteratorTest {

    @Test
    void testBasicLogRecordBatchIterator() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        assertThat(iterator.hasNext()).isTrue();

        // Verify that batch can be read correctly
        LogRecordBatch batch = iterator.next();
        assertThat(batch).isNotNull();
        assertThat(batch.getRecordCount()).isEqualTo(DATA1.size());
        assertThat(batch.baseLogOffset()).isEqualTo(0L);

        // Verify that batch has statistics using ReadContext
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
        assertThat(batch.getStatistics(readContext)).isPresent();
        readContext.close();

        // Verify iterator ends
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testLogRecordBatchIteratorWithTargetOffset() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Create LogRecordBatchIterator with target offset
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()),
                        5L);

        // Verify iterator has data
        assertThat(iterator.hasNext()).isTrue();

        // Verify that batch can be read correctly
        LogRecordBatch batch = iterator.next();
        assertThat(batch).isNotNull();
        assertThat(batch.lastLogOffset()).isGreaterThanOrEqualTo(5L);

        // Verify iterator ends
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithEqualPredicate() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        // Create equal predicate (first field equals 5)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has data
        assertThat(filteredIterator.hasNext()).isTrue();

        // Verify that filtered batch can be read correctly
        LogRecordBatch batch = filteredIterator.next();
        assertThat(batch).isNotNull();

        // Verify that batch has statistics using ReadContext
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Verify that the returned batch actually contains records matching the predicate
        boolean foundMatchingRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                if (record.getRow().getInt(0) == 5) {
                    foundMatchingRecord = true;
                    break;
                }
            }
        }
        assertThat(foundMatchingRecord)
                .as("Filtered batch should contain at least one record with first field equal to 5")
                .isTrue();

        // Verify statistics are correctly used for filtering decision
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Close the read context
        readContext.close();

        // Verify iterator ends
        assertThat(filteredIterator.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithGreaterThanPredicate() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        // Create greater than predicate (first field greater than 3)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 3);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(greaterThanPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has data
        assertThat(filteredIterator.hasNext()).isTrue();

        // Verify that filtered batch can be read correctly
        LogRecordBatch batch = filteredIterator.next();
        assertThat(batch).isNotNull();

        // Verify that batch has statistics using ReadContext
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Verify that the returned batch actually contains records matching the predicate
        boolean foundMatchingRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                if (record.getRow().getInt(0) > 3) {
                    foundMatchingRecord = true;
                    break;
                }
            }
        }
        assertThat(foundMatchingRecord)
                .as(
                        "Filtered batch should contain at least one record with first field greater than 3")
                .isTrue();

        assertThat(batch.getStatistics(readContext)).isPresent();

        // Close the read context
        readContext.close();

        // Verify iterator ends
        assertThat(filteredIterator.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithIsNotNullPredicate() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        // Create is not null predicate (first field is not null)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate isNotNullPredicate = builder.isNotNull(0);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(isNotNullPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has data
        assertThat(filteredIterator.hasNext()).isTrue();

        // Verify that filtered batch can be read correctly
        LogRecordBatch batch = filteredIterator.next();
        assertThat(batch).isNotNull();

        // Verify that batch has statistics using ReadContext
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Verify that the returned batch actually contains non-null records
        boolean foundNonNullRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                if (!record.getRow().isNullAt(0)) {
                    foundNonNullRecord = true;
                    break;
                }
            }
        }
        assertThat(foundNonNullRecord)
                .as("Filtered batch should contain at least one non-null record in first field")
                .isTrue();

        // Verify statistics are correctly used for filtering decision
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Close the read context
        readContext.close();

        // Verify iterator ends
        assertThat(filteredIterator.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithNoMatchingData() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        // Create equal predicate (first field equals 999, which doesn't exist in DATA1)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 999);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has no data
        assertThat(filteredIterator.hasNext()).isFalse();

        // Close the read context
        readContext.close();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithEmptyLogRecords() throws Exception {
        // Create empty LogRecords
        MemoryLogRecords emptyLogRecords = MemoryLogRecords.EMPTY;

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                emptyLogRecords.getMemorySegment(),
                                emptyLogRecords.getPosition(),
                                emptyLogRecords.sizeInBytes()));

        // Create predicate
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has no data
        assertThat(filteredIterator.hasNext()).isFalse();

        // Close the read context
        readContext.close();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithMultipleBatches() throws Exception {
        // Create test data for multiple batches
        List<Object[]> batch1Data = Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"});
        List<Object[]> batch2Data = Arrays.asList(new Object[] {3, "c"}, new Object[] {4, "d"});
        List<Object[]> batch3Data = Arrays.asList(new Object[] {5, "e"}, new Object[] {6, "f"});

        // Create multiple MemoryLogRecords with statistics
        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 2L, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch3 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch3Data, DATA1_ROW_TYPE, 4L, DEFAULT_SCHEMA_ID);

        // Combine multiple batches
        List<MemoryLogRecords> allBatches = Arrays.asList(batch1, batch2, batch3);
        ListLogRecords combinedRecords = new ListLogRecords(allBatches);

        // Create LogRecordBatchIterator
        Iterator<LogRecordBatch> batchIterator = combinedRecords.batches().iterator();
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new LogInputStream<LogRecordBatch>() {
                            @Override
                            public LogRecordBatch nextBatch() throws java.io.IOException {
                                return batchIterator.hasNext() ? batchIterator.next() : null;
                            }
                        });

        // Create greater than predicate (first field greater than 2)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 2);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(greaterThanPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has data
        assertThat(filteredIterator.hasNext()).isTrue();

        // Verify that first filtered batch can be read correctly (batch2: [3, "c"], [4, "d"])
        LogRecordBatch filteredBatch1 = filteredIterator.next();
        assertThat(filteredBatch1).isNotNull();
        assertThat(filteredBatch1.getStatistics(readContext)).isPresent();

        // Verify first batch contains matching records
        boolean batch1HasMatchingRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = filteredBatch1.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                if (record.getRow().getInt(0) > 2) {
                    batch1HasMatchingRecord = true;
                    break;
                }
            }
        }
        assertThat(batch1HasMatchingRecord)
                .as("First filtered batch should contain at least one record with first field > 2")
                .isTrue();

        // Verify that second filtered batch can be read correctly (batch3: [5, "e"], [6, "f"])
        assertThat(filteredIterator.hasNext()).isTrue();
        LogRecordBatch filteredBatch2 = filteredIterator.next();
        assertThat(filteredBatch2).isNotNull();
        assertThat(filteredBatch2.getStatistics(readContext)).isPresent();

        // Verify second batch contains matching records
        boolean batch2HasMatchingRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = filteredBatch2.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                if (record.getRow().getInt(0) > 2) {
                    batch2HasMatchingRecord = true;
                    break;
                }
            }
        }
        assertThat(batch2HasMatchingRecord)
                .as("Second filtered batch should contain at least one record with first field > 2")
                .isTrue();

        // Close the read context
        readContext.close();

        // Verify iterator ends
        assertThat(filteredIterator.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithStatistics() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        // Create predicate
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has data
        assertThat(filteredIterator.hasNext()).isTrue();

        // Verify that filtered batch can be read correctly
        LogRecordBatch batch = filteredIterator.next();
        assertThat(batch).isNotNull();

        // Verify that batch has statistics using ReadContext
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Verify that the returned batch actually contains records matching the predicate
        boolean foundMatchingRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                if (record.getRow().getInt(0) == 5) {
                    foundMatchingRecord = true;
                    break;
                }
            }
        }
        assertThat(foundMatchingRecord)
                .as("Filtered batch should contain at least one record with first field equal to 5")
                .isTrue();

        // Close the read context
        readContext.close();

        // Verify iterator ends
        assertThat(filteredIterator.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithNullStatistics() throws Exception {
        // Create a batch without statistics (using old version magic value)
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject(
                        (byte) 0, // Use V0 magic value, which doesn't contain statistics
                        DATA1);

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        // Create predicate
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator behavior (when no statistics, should return all batches)
        assertThat(filteredIterator.hasNext()).isTrue();

        // Verify that batch can be read correctly
        LogRecordBatch batch = filteredIterator.next();
        assertThat(batch).isNotNull();

        // Verify that batch has no statistics using ReadContext
        assertThat(batch.getStatistics(readContext)).isEmpty();

        // Verify that the batch contains the expected data (since no filtering was applied)
        int recordCount = 0;
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                assertThat(record).isNotNull();
                recordCount++;
            }
        }
        assertThat(recordCount).isEqualTo(DATA1.size());

        // Close the read context
        readContext.close();

        // Verify iterator ends
        assertThat(filteredIterator.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorChaining() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        // Create first predicate: greater than 3
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 3);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply first filter
        LogRecordBatchIterator<LogRecordBatch> filteredIterator1 =
                iterator.filter(new Filter(greaterThanPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Create second predicate: less than 7
        Predicate lessThanPredicate = builder.lessThan(0, 7);

        // Apply second filter
        LogRecordBatchIterator<LogRecordBatch> filteredIterator2 =
                filteredIterator1.filter(
                        new Filter(lessThanPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify final filtered iterator has data
        assertThat(filteredIterator2.hasNext()).isTrue();

        // Verify that filtered batch can be read correctly
        LogRecordBatch batch = filteredIterator2.next();
        assertThat(batch).isNotNull();

        // Verify that batch has statistics using ReadContext
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Verify that the returned batch actually contains records matching both predicates
        boolean foundMatchingRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                int value = record.getRow().getInt(0);
                if (value > 3 && value < 7) {
                    foundMatchingRecord = true;
                    break;
                }
            }
        }
        assertThat(foundMatchingRecord)
                .as(
                        "Filtered batch should contain at least one record with first field between 3 and 7")
                .isTrue();

        // Verify statistics are correctly used for filtering decision
        assertThat(batch.getStatistics(readContext).isPresent()).isTrue();

        // Close the read context
        readContext.close();

        // Verify iterator ends
        assertThat(filteredIterator2.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithComplexPredicate() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        // Create complex predicate: (first field greater than 3) AND (first field less than 7)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 3);
        Predicate lessThanPredicate = builder.lessThan(0, 7);
        Predicate complexPredicate = PredicateBuilder.and(greaterThanPredicate, lessThanPredicate);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(complexPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has data
        assertThat(filteredIterator.hasNext()).isTrue();

        // Verify that filtered batch can be read correctly
        LogRecordBatch batch = filteredIterator.next();
        assertThat(batch).isNotNull();

        // Verify that batch has statistics using ReadContext
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Verify that the returned batch actually contains records matching the complex predicate
        boolean foundMatchingRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                int value = record.getRow().getInt(0);
                if (value > 3 && value < 7) {
                    foundMatchingRecord = true;
                    break;
                }
            }
        }
        assertThat(foundMatchingRecord)
                .as(
                        "Filtered batch should contain at least one record with first field between 3 and 7")
                .isTrue();

        // Verify statistics are correctly used for filtering decision
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Close the read context
        readContext.close();

        // Verify iterator ends
        assertThat(filteredIterator.hasNext()).isFalse();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithStatisticsValidation() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Create LogRecordBatchIterator
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        // Create predicate
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(equalPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has data
        assertThat(filteredIterator.hasNext()).isTrue();

        // Verify that filtered batch can be read correctly
        LogRecordBatch batch = filteredIterator.next();
        assertThat(batch).isNotNull();

        // Verify that batch has statistics using ReadContext
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Verify that the returned batch actually contains records matching the predicate
        boolean foundMatchingRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                if (record.getRow().getInt(0) == 5) {
                    foundMatchingRecord = true;
                    break;
                }
            }
        }
        assertThat(foundMatchingRecord)
                .as("Filtered batch should contain at least one record with first field equal to 5")
                .isTrue();

        // Close the read context
        readContext.close();

        // Verify iterator ends
        assertThat(filteredIterator.hasNext()).isFalse();
    }

    @Test
    void testGetStatisticsWithNullReadContext() throws Exception {
        // Create test data with statistics
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        // Get the first batch
        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isNotNull();

        // Test with null read context
        Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(null);
        assertThat(statisticsOpt).isEmpty();
    }

    @Test
    void testGetStatisticsWithV0Magic() throws Exception {
        // Create a batch without statistics (using old version magic value)
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject(
                        (byte) 0, // Use V0 magic value, which doesn't contain statistics
                        DATA1);

        // Get the first batch
        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch).isNotNull();

        // Test with read context - should return empty for V0 format
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);
        Optional<LogRecordBatchStatistics> statisticsOpt = batch.getStatistics(readContext);
        assertThat(statisticsOpt).isEmpty();
        readContext.close();
    }

    @Test
    void testFilteredLogRecordBatchIteratorEnsuresNoUnmatchingBatches() throws Exception {
        // Test to ensure that record batches that do not satisfy the recordBatchFilter predicate
        // are never included in the filtered results at the iterator level
        // Create test data for multiple batches with different data ranges
        // Batch 1: values 1-3 (should not match predicate > 5)
        List<Object[]> batch1Data = Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"});
        // Batch 2: values 4-6 (should not match predicate > 5)
        List<Object[]> batch2Data = Arrays.asList(new Object[] {4, "c"}, new Object[] {5, "d"});
        // Batch 3: values 7-9 (should match predicate > 5)
        List<Object[]> batch3Data = Arrays.asList(new Object[] {7, "e"}, new Object[] {8, "f"});

        // Create multiple MemoryLogRecords with statistics
        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 2L, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch3 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch3Data, DATA1_ROW_TYPE, 4L, DEFAULT_SCHEMA_ID);

        // Combine multiple batches
        List<MemoryLogRecords> allBatches = Arrays.asList(batch1, batch2, batch3);
        ListLogRecords combinedRecords = new ListLogRecords(allBatches);

        // Create LogRecordBatchIterator
        Iterator<LogRecordBatch> batchIterator = combinedRecords.batches().iterator();
        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new LogInputStream<LogRecordBatch>() {
                            @Override
                            public LogRecordBatch nextBatch() throws java.io.IOException {
                                return batchIterator.hasNext() ? batchIterator.next() : null;
                            }
                        });

        // Create predicate (first field greater than 5)
        // This should only match batch 4 and batch3 (values 7-8)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 5);

        // Create read context
        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Apply filter with read context
        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(greaterThanPredicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify filtered iterator has data
        assertThat(filteredIterator.hasNext()).isTrue();

        // Verify that only batches matching the predicate are returned
        int batchCount = 0;
        while (filteredIterator.hasNext()) {
            LogRecordBatch batch = filteredIterator.next();
            batchCount++;
            assertThat(batch).isNotNull();

            // Verify that each returned batch contains at least one record matching the predicate
            boolean hasMatchingRecord = false;
            try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                while (recordIterator.hasNext()) {
                    LogRecord record = recordIterator.next();
                    if (record.getRow().getInt(0) > 5) {
                        hasMatchingRecord = true;
                        break;
                    }
                }
            }
            // Each returned batch must contain at least one record matching the predicate
            assertThat(hasMatchingRecord)
                    .as(
                            "Batch %d should contain at least one record matching predicate > 5",
                            batchCount)
                    .isTrue();
        }

        // Should have exactly one batch returned (batch3)
        assertThat(batchCount).isEqualTo(1);

        // Close the read context
        readContext.close();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithStatisticsBoundaryValues() throws Exception {
        // Test filtering with boundary values to ensure statistics are correctly used
        // Create test data with specific boundary values
        List<Object[]> boundaryData =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {5, "b"}, new Object[] {10, "c"});

        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        boundaryData, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Test 1: Equal to min value (should match)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalToMinPredicate = builder.equal(0, 1);

        LogRecordBatchIterator<LogRecordBatch> filteredIterator1 =
                iterator.filter(new Filter(equalToMinPredicate, DEFAULT_SCHEMA_ID), readContext);

        assertThat(filteredIterator1.hasNext()).isTrue();
        LogRecordBatch batch1 = filteredIterator1.next();
        assertThat(batch1.getStatistics(readContext)).isPresent();

        // Test 2: Equal to max value (should match)
        LogRecordBatchIterator<LogRecordBatch> iterator2 =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        Predicate equalToMaxPredicate = builder.equal(0, 10);

        LogRecordBatchIterator<LogRecordBatch> filteredIterator2 =
                iterator2.filter(new Filter(equalToMaxPredicate, DEFAULT_SCHEMA_ID), readContext);

        assertThat(filteredIterator2.hasNext()).isTrue();
        LogRecordBatch batch2 = filteredIterator2.next();
        assertThat(batch2.getStatistics(readContext)).isPresent();

        // Test 3: Greater than max value (should not match)
        LogRecordBatchIterator<LogRecordBatch> iterator3 =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        Predicate greaterThanMaxPredicate = builder.greaterThan(0, 10);

        LogRecordBatchIterator<LogRecordBatch> filteredIterator3 =
                iterator3.filter(
                        new Filter(greaterThanMaxPredicate, DEFAULT_SCHEMA_ID), readContext);

        assertThat(filteredIterator3.hasNext()).isFalse();

        // Test 4: Less than min value (should not match)
        LogRecordBatchIterator<LogRecordBatch> iterator4 =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        Predicate lessThanMinPredicate = builder.lessThan(0, 1);

        LogRecordBatchIterator<LogRecordBatch> filteredIterator4 =
                iterator4.filter(new Filter(lessThanMinPredicate, DEFAULT_SCHEMA_ID), readContext);

        assertThat(filteredIterator4.hasNext()).isFalse();

        readContext.close();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithNullValuesInStatistics() throws Exception {
        // Test filtering when statistics contain null values
        // Create test data with null values
        List<Object[]> dataWithNulls =
                Arrays.asList(
                        new Object[] {1, "a"},
                        new Object[] {null, "b"},
                        new Object[] {5, "c"},
                        new Object[] {null, "d"});

        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        dataWithNulls, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Test IS NOT NULL predicate
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate isNotNullPredicate = builder.isNotNull(0);

        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(isNotNullPredicate, DEFAULT_SCHEMA_ID), readContext);

        assertThat(filteredIterator.hasNext()).isTrue();
        LogRecordBatch batch = filteredIterator.next();
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Verify that the batch contains non-null records
        boolean foundNonNullRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                if (!record.getRow().isNullAt(0)) {
                    foundNonNullRecord = true;
                    break;
                }
            }
        }
        assertThat(foundNonNullRecord)
                .as("Filtered batch should contain at least one non-null record")
                .isTrue();

        // Test IS NULL predicate
        LogRecordBatchIterator<LogRecordBatch> iterator2 =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        Predicate isNullPredicate = builder.isNull(0);

        LogRecordBatchIterator<LogRecordBatch> filteredIterator2 =
                iterator2.filter(new Filter(isNullPredicate, DEFAULT_SCHEMA_ID), readContext);

        assertThat(filteredIterator2.hasNext()).isTrue();
        LogRecordBatch batch2 = filteredIterator2.next();
        assertThat(batch2.getStatistics(readContext)).isPresent();

        // Verify that the batch contains null records
        boolean foundNullRecord = false;
        try (CloseableIterator<LogRecord> recordIterator = batch2.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                if (record.getRow().isNullAt(0)) {
                    foundNullRecord = true;
                    break;
                }
            }
        }
        assertThat(foundNullRecord)
                .as("Filtered batch should contain at least one null record")
                .isTrue();

        readContext.close();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithStatisticsPrecision() throws Exception {
        // Test that statistics-based filtering is precise and doesn't miss valid batches
        // Create test data with specific ranges
        List<Object[]> preciseData =
                Arrays.asList(
                        new Object[] {3, "a"},
                        new Object[] {4, "b"},
                        new Object[] {5, "c"},
                        new Object[] {6, "d"},
                        new Object[] {7, "e"});

        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        preciseData, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        // Test range predicate that should match exactly
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate rangePredicate = builder.greaterThan(0, 4);

        LogRecordBatchIterator<LogRecordBatch> filteredIterator =
                iterator.filter(new Filter(rangePredicate, DEFAULT_SCHEMA_ID), readContext);

        assertThat(filteredIterator.hasNext()).isTrue();
        LogRecordBatch batch = filteredIterator.next();
        assertThat(batch.getStatistics(readContext)).isPresent();

        // Verify that the batch contains records matching the predicate
        List<Integer> matchingValues = new ArrayList<>();
        try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
            while (recordIterator.hasNext()) {
                LogRecord record = recordIterator.next();
                int value = record.getRow().getInt(0);
                if (value > 4) {
                    matchingValues.add(value);
                }
            }
        }
        assertThat(matchingValues).containsExactlyInAnyOrder(5, 6, 7);
        assertThat(matchingValues).hasSize(3);

        // Test that statistics correctly exclude non-matching batches
        LogRecordBatchIterator<LogRecordBatch> iterator2 =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        Predicate noMatchPredicate = builder.greaterThan(0, 10);

        LogRecordBatchIterator<LogRecordBatch> filteredIterator2 =
                iterator2.filter(new Filter(noMatchPredicate, DEFAULT_SCHEMA_ID), readContext);

        assertThat(filteredIterator2.hasNext()).isFalse();

        readContext.close();
    }

    @Test
    void testFilteredLogRecordBatchIteratorWithStatisticsConsistency() throws Exception {
        // Test that statistics-based filtering is consistent across multiple iterations
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate predicate = builder.greaterThan(0, 5);

        // Apply filter multiple times and verify consistent results
        LogRecordBatchIterator<LogRecordBatch> filteredIterator1 =
                iterator.filter(new Filter(predicate, DEFAULT_SCHEMA_ID), readContext);
        LogRecordBatchIterator<LogRecordBatch> filteredIterator2 =
                new LogRecordBatchIterator<>(
                                new MemorySegmentLogInputStream(
                                        memoryLogRecords.getMemorySegment(),
                                        memoryLogRecords.getPosition(),
                                        memoryLogRecords.sizeInBytes()))
                        .filter(new Filter(predicate, DEFAULT_SCHEMA_ID), readContext);

        // Verify both iterators have the same number of batches
        int count1 = 0;
        while (filteredIterator1.hasNext()) {
            filteredIterator1.next();
            count1++;
        }

        int count2 = 0;
        while (filteredIterator2.hasNext()) {
            filteredIterator2.next();
            count2++;
        }

        assertThat(count1).isEqualTo(count2);
        assertThat(count1).isGreaterThan(0);

        // Verify that all returned batches have consistent statistics
        LogRecordBatchIterator<LogRecordBatch> filteredIterator3 =
                new LogRecordBatchIterator<>(
                                new MemorySegmentLogInputStream(
                                        memoryLogRecords.getMemorySegment(),
                                        memoryLogRecords.getPosition(),
                                        memoryLogRecords.sizeInBytes()))
                        .filter(new Filter(predicate, count2), readContext);

        while (filteredIterator3.hasNext()) {
            LogRecordBatch batch = filteredIterator3.next();

            // Verify that the batch actually contains matching records
            boolean hasMatchingRecord = false;
            try (CloseableIterator<LogRecord> recordIterator = batch.records(readContext)) {
                while (recordIterator.hasNext()) {
                    LogRecord record = recordIterator.next();
                    if (record.getRow().getInt(0) > 5) {
                        hasMatchingRecord = true;
                        break;
                    }
                }
            }
            assertThat(hasMatchingRecord)
                    .as("Each returned batch should contain at least one matching record")
                    .isTrue();
        }

        readContext.close();
    }
}

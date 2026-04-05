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

package org.apache.fluss.server.log;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.exception.LogSegmentOffsetOverflowException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.FileLogProjection;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordBatchStatisticsTestUtils;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.ProjectionPushdownCache;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.IntPredicate;
import java.util.stream.Stream;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static org.apache.fluss.testutils.DataTestUtils.createRecordsWithoutBaseLogOffset;
import static org.apache.fluss.testutils.DataTestUtils.genLogRecordsWithBaseOffsetAndTimestamp;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithBaseOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LogSegment}. */
final class LogSegmentTest extends LogTestBase {

    private @TempDir File tempDir;

    static Stream<Arguments> offsetParameters() {
        return Stream.of(
                Arguments.of(0L, -2147483648L),
                Arguments.of(0L, 2147483648L),
                Arguments.of(0L, -2147483648L),
                Arguments.of(100L, 10L),
                Arguments.of(2147483648L, 0L),
                Arguments.of(-2147483648L, 0L),
                Arguments.of(2147483648L, 4294967296L));
    }

    @ParameterizedTest
    @MethodSource("offsetParameters")
    void testAppendForLogSegmentOffsetOverflowException(long baseOffset, long largestOffset)
            throws Exception {
        LogSegment segment = createSegment(baseOffset);
        MemoryLogRecords memoryRecords =
                genMemoryLogRecordsWithBaseOffset(
                        baseOffset, Collections.singletonList(new Object[] {1, "hello"}));
        assertThatThrownBy(
                        () ->
                                segment.append(
                                        largestOffset,
                                        System.currentTimeMillis(),
                                        baseOffset,
                                        memoryRecords))
                .isInstanceOf(LogSegmentOffsetOverflowException.class)
                .hasMessageContaining("Detected offset overflow at offset");
    }

    @Test
    void testReadOnEmptySegment() throws Exception {
        // Read beyond the last offset in the segment should be null.
        LogSegment segment = createSegment(40);
        FetchDataInfo read = segment.read(40, 300, 300, false);
        assertThat(read).isNull();
    }

    @Test
    void testReadBeforeFirstOffset() throws Exception {
        // Reading from before the first offset in the segment should return messages beginning with
        // the first message in the segment.
        LogSegment segment = createSegment(40);
        MemoryLogRecords memoryRecords =
                genMemoryLogRecordsWithBaseOffset(
                        50,
                        Arrays.asList(
                                new Object[] {1, "hello"},
                                new Object[] {2, "there"},
                                new Object[] {3, "little"},
                                new Object[] {4, "bee"}));
        segment.append(53, -1L, -1L, memoryRecords);
        FetchDataInfo read = segment.read(41, 300, segment.getSizeInBytes(), true);
        assertThat(read).isNotNull();
        LogRecords actualRecords = read.getRecords();
        assertLogRecordsEquals(actualRecords, memoryRecords);
    }

    @Test
    void testAfterLast() throws Exception {
        // If we read from an offset beyond the last offset in the segment we should get null.
        LogSegment segment = createSegment(40);
        MemoryLogRecords memoryRecords =
                genMemoryLogRecordsWithBaseOffset(
                        50, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "there"}));
        segment.append(51, -1L, -1L, memoryRecords);
        FetchDataInfo read = segment.read(52, 300, segment.getSizeInBytes(), true);
        assertThat(read).isNull();
    }

    @Test
    void testReadFromGap() throws Exception {
        // If we read from an offset which doesn't exist we should get a message set beginning with
        // the least offset greater than the given startOffset.
        LogSegment segment = createSegment(40);
        MemoryLogRecords memoryRecords =
                genMemoryLogRecordsWithBaseOffset(
                        50, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "there"}));
        segment.append(51, -1L, -1L, memoryRecords);
        MemoryLogRecords memoryRecords2 =
                genMemoryLogRecordsWithBaseOffset(
                        60, Arrays.asList(new Object[] {1, "alpha"}, new Object[] {2, "beta"}));
        segment.append(61, -1L, -1L, memoryRecords2);
        FetchDataInfo read = segment.read(55, 200, segment.getSizeInBytes(), true);
        assertThat(read).isNotNull();
        assertLogRecordsEquals(read.getRecords(), memoryRecords2);
    }

    @Test
    void testTruncate() throws Exception {
        // In a loop append two messages then truncate off the second of those messages and check
        // that we can read the first but not the second message.
        LogSegment segment = createSegment(40);
        int offset = 40;
        for (int i = 0; i < 30; i++) {
            MemoryLogRecords memoryRecords1 =
                    genMemoryLogRecordsWithBaseOffset(
                            offset, Collections.singletonList(new Object[] {1, "hello"}));
            segment.append(offset, -1L, -1L, memoryRecords1);
            MemoryLogRecords memoryRecords2 =
                    genMemoryLogRecordsWithBaseOffset(
                            offset + 1, Collections.singletonList(new Object[] {1, "hello"}));
            segment.append(offset + 1, -1L, -1L, memoryRecords2);
            // check that we can read back both messages
            FetchDataInfo read = segment.read(offset, 10000, segment.getSizeInBytes(), true);
            assertThat(read).isNotNull();
            assertLogRecordsListEquals(
                    Arrays.asList(memoryRecords1, memoryRecords2), read.getRecords());

            // now truncate off the last message
            segment.truncateTo(offset + 1);
            FetchDataInfo read2 = segment.read(offset, 10000, segment.getSizeInBytes(), true);
            assertThat(read2).isNotNull();
            assertLogRecordsEquals(read2.getRecords(), memoryRecords1);
            offset += 1;
        }
    }

    @Test
    void testTruncateEmptySegment() throws IOException {
        // This tests the scenario in which the follower truncates to an empty segment. In this
        // case we must ensure that the index is resized so that the log segment is not mistakenly
        // rolled due to a full index
        LogSegment segment = createSegment(0);
        // Force load indexes before closing the segment
        segment.offsetIndex();
        segment.close();

        LogSegment reopened = createSegment(0);
        assertThat(segment.offsetIndex().sizeInBytes()).isEqualTo(0);
        reopened.truncateTo(57);
        assertThat(reopened.offsetIndex().isFull()).isFalse();

        RollParams rollParams = new RollParams(Integer.MAX_VALUE, 100L, 1024);
        assertThat(reopened.shouldRoll(rollParams)).isFalse();

        // The segment should not be rolled even if maxSegmentMs has been exceeded
        rollParams = new RollParams(Integer.MAX_VALUE, Integer.MAX_VALUE + 200L, 1024);
        assertThat(reopened.shouldRoll(rollParams)).isTrue();
    }

    @Test
    void testReloadLargestTimestampAndNextOffsetAfterTruncation() throws Exception {
        int numMessages = 30;
        MemoryLogRecords records =
                genLogRecordsWithBaseOffsetAndTimestamp(
                        0, 0, Collections.singletonList(new Object[] {1, "hello"}));
        LogSegment segment = createSegment(40, 2 * records.sizeInBytes() - 1);
        long offset = 40L;
        for (int i = 0; i < numMessages; i++) {
            long maxTimestamp = offset;
            segment.append(
                    offset,
                    maxTimestamp,
                    offset,
                    genLogRecordsWithBaseOffsetAndTimestamp(
                            offset,
                            maxTimestamp,
                            Collections.singletonList(new Object[] {1, "hello"})));
            offset += 1;
        }
        assertThat(segment.readNextOffset()).isEqualTo(offset);

        int expectedNumEntries = numMessages / 2 - 1;
        assertThat(segment.timeIndex().entries()).isEqualTo(expectedNumEntries);

        segment.truncateTo(41);
        assertThat(segment.timeIndex().entries()).isEqualTo(0);
        assertThat(segment.maxTimestampSoFar()).isEqualTo(40L);
        assertThat(segment.readNextOffset()).isEqualTo(41);
    }

    @Test
    void testTruncateFull() throws Exception {
        // Test truncating the whole segment, and check that we can reopen with the original
        // offset.
        LogSegment segment = createSegment(40);
        segment.append(
                41,
                -1L,
                -1L,
                genMemoryLogRecordsWithBaseOffset(
                        40, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "there"})));

        segment.truncateTo(0);
        assertThat(segment.offsetIndex().isFull()).isFalse();
        assertThat(segment.read(0, 1024, 1024, false)).isNull();

        segment.append(
                41,
                -1L,
                -1L,
                genMemoryLogRecordsWithBaseOffset(
                        40, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "there"})));
    }

    @Test
    void testFindOffsetByTimestamp() throws Exception {
        int messageSize =
                genLogRecordsWithBaseOffsetAndTimestamp(
                                0, 0, Collections.singletonList(new Object[] {1, "hello"}))
                        .sizeInBytes();
        LogSegment segment = createSegment(40, messageSize * 2 - 1);
        for (int i = 40; i < 50; i++) {
            segment.append(
                    i,
                    i * 10,
                    i,
                    genLogRecordsWithBaseOffsetAndTimestamp(
                            i, i * 10, Collections.singletonList(new Object[] {1, "hello"})));
        }

        assertThat(segment.maxTimestampSoFar()).isEqualTo(490L);
        // Search for an indexed timestamp.
        assertThat(segment.findOffsetByTimestamp(420L, 0L).get().getOffset()).isEqualTo(42);
        assertThat(segment.findOffsetByTimestamp(421L, 0L).get().getOffset()).isEqualTo(43);
        // Search for an un-indexed timestamp.
        assertThat(segment.findOffsetByTimestamp(430L, 0L).get().getOffset()).isEqualTo(43);
        assertThat(segment.findOffsetByTimestamp(431L, 0L).get().getOffset()).isEqualTo(44);
        // Search beyond the last timestamp.
        assertThat(segment.findOffsetByTimestamp(491L, 0L)).isEmpty();
        // Search before the first indexed timestamp.
        assertThat(segment.findOffsetByTimestamp(401L, 0L).get().getOffset()).isEqualTo(41);
        // Search before the first timestamp.
        assertThat(segment.findOffsetByTimestamp(399L, 0L).get().getOffset()).isEqualTo(40);
    }

    @Test
    void testNextOffsetCalculation() throws Exception {
        //  Test that offsets are assigned sequentially and that the nextOffset variable is
        // incremented.
        LogSegment segment = createSegment(40);
        assertThat(segment.readNextOffset()).isEqualTo(40);
        segment.append(
                52,
                -1L,
                -1L,
                genMemoryLogRecordsWithBaseOffset(
                        50,
                        Arrays.asList(
                                new Object[] {1, "hello"},
                                new Object[] {2, "there"},
                                new Object[] {2, "you"})));
        assertThat(segment.readNextOffset()).isEqualTo(53);
    }

    @Test
    void testChangeFileSuffixes() throws IOException {
        // Test that we can change the file suffixes for the log and index files
        LogSegment segment = createSegment(40);
        File logFile = segment.getFileLogRecords().file();
        File indexFile = segment.getLazyOffsetIndex().file();
        // Ensure that files for offset has not been created eagerly.
        assertThat(segment.getLazyOffsetIndex().file().exists()).isFalse();
        segment.changeFileSuffixes("", ".deleted");
        // Ensure that attempt to change suffixes for non-existing offset indices does not
        // create new files.
        assertThat(segment.getLazyOffsetIndex().file().exists()).isFalse();

        // Ensure that file names are updated accordingly.
        assertThat(logFile.getAbsolutePath() + ".deleted")
                .isEqualTo(segment.getFileLogRecords().file().getAbsolutePath());
        assertThat(indexFile.getAbsolutePath() + ".deleted")
                .isEqualTo(segment.getLazyOffsetIndex().file().getAbsolutePath());
        assertThat(segment.getFileLogRecords().file().exists()).isTrue();
        // Ensure lazy creation of offset index file upon accessing it.
        segment.getLazyOffsetIndex().get();
        assertThat(segment.getLazyOffsetIndex().file().exists()).isTrue();
    }

    @Test
    void testRecoveryFixesCorruptIndex() throws Exception {
        // Create a segment with some data and an index. Then corrupt the index, and recover the
        // segment, the entries should all be readable.
        LogSegment segment = createSegment(0);
        for (int i = 0; i < 100; i++) {
            segment.append(
                    i,
                    -1L,
                    i,
                    genMemoryLogRecordsWithBaseOffset(
                            i, Collections.singletonList(new Object[] {i + 1, String.valueOf(i)})));
        }
        File indexFile = segment.getLazyOffsetIndex().file();
        LogTestUtils.writeNonsenseToFile(indexFile, 5, (int) indexFile.length());
        segment.recover();

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            for (int i = 0; i < 100; i++) {
                FetchDataInfo read = segment.read(i, 100, segment.getSizeInBytes(), true);
                assertThat(read).isNotNull();
                Iterable<LogRecordBatch> batches = read.getRecords().batches();
                LogRecordBatch batch = batches.iterator().next();
                LogRecord record = batch.records(readContext).next();
                assertThat(record.logOffset()).isEqualTo(i);
            }
        }
    }

    @Test
    void testRecoveryFixesCorruptTimeIndex() throws Exception {
        // Create a segment with some data and an index. Then corrupt the index, and recover the
        // segment, the entries should all be readable.
        LogSegment segment = createSegment(0);
        for (int i = 0; i < 100; i++) {
            segment.append(
                    i,
                    i * 10,
                    i,
                    genLogRecordsWithBaseOffsetAndTimestamp(
                            i,
                            i * 10,
                            Collections.singletonList(new Object[] {i + 1, String.valueOf(i)})));
        }
        File timeIndexFile = segment.timeIndexFile();
        LogTestUtils.writeNonsenseToFile(timeIndexFile, 5, (int) timeIndexFile.length());
        segment.recover();

        for (int i = 0; i < 100; i++) {
            assertThat(segment.findOffsetByTimestamp(i * 10, 0L).get().getOffset()).isEqualTo(i);
            if (i < 99) {
                assertThat(segment.findOffsetByTimestamp(i * 10 + 1, 0L).get().getOffset())
                        .isEqualTo(i + 1);
            }
        }
    }

    @Test
    void testCreateWithInitFileSizeAppendMessage() throws Exception {
        conf.setBoolean(ConfigOptions.LOG_FILE_PREALLOCATE, true);
        LogSegment segment = createSegment(40, false, 1024 * 1024);
        MemoryLogRecords memoryRecords1 =
                genMemoryLogRecordsWithBaseOffset(
                        50, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "there"}));
        segment.append(51, -1L, -1L, memoryRecords1);
        MemoryLogRecords memoryRecords2 =
                genMemoryLogRecordsWithBaseOffset(
                        60, Arrays.asList(new Object[] {1, "alpha"}, new Object[] {2, "beta"}));
        segment.append(61, -1L, -1L, memoryRecords2);
        FetchDataInfo read = segment.read(55, 200, segment.getSizeInBytes(), true);
        assertThat(read).isNotNull();
        assertLogRecordsEquals(read.getRecords(), memoryRecords2);
    }

    @Test
    void testCreateWithInitFileSizeClearShutdown() throws Exception {
        conf.setBoolean(ConfigOptions.LOG_FILE_PREALLOCATE, true);
        // create a segment with pre allocate and clearly shut down.
        LogSegment segment = createSegment(40, false, 1024 * 1024);
        MemoryLogRecords memoryRecords1 =
                genMemoryLogRecordsWithBaseOffset(
                        50, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "there"}));
        segment.append(51, -1L, -1L, memoryRecords1);
        MemoryLogRecords memoryRecords2 =
                genMemoryLogRecordsWithBaseOffset(
                        60, Arrays.asList(new Object[] {1, "alpha"}, new Object[] {2, "beta"}));
        segment.append(61, -1L, -1L, memoryRecords2);
        FetchDataInfo read = segment.read(55, 200, segment.getSizeInBytes(), true);
        assertThat(read).isNotNull();
        assertLogRecordsEquals(read.getRecords(), memoryRecords2);

        int oldSize = segment.getFileLogRecords().sizeInBytes();
        long oldPosition = segment.getFileLogRecords().channel().position();
        long oldFileSize = segment.getFileLogRecords().file().length();
        assertThat(oldFileSize).isEqualTo(1024 * 1024);

        segment.close();
        // After close, file should be trimmed
        assertThat(segment.getFileLogRecords().file().length()).isEqualTo(oldSize);
        LogSegment segmentReopen = createSegment(40, true, 1024 * 1024);
        FetchDataInfo readAgain = segmentReopen.read(55, 200, segment.getSizeInBytes(), true);
        assertThat(readAgain).isNotNull();
        assertLogRecordsEquals(readAgain.getRecords(), memoryRecords2);
        int size = segmentReopen.getFileLogRecords().sizeInBytes();
        long position = segmentReopen.getFileLogRecords().channel().position();
        assertThat(size).isEqualTo(oldSize);
        assertThat(position).isEqualTo(oldPosition);
    }

    @Test
    void testReadWithFilterEqualPredicate() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);
        assertFilteredReadFindsMatchingRecord(equalPredicate, v -> v == 5);
    }

    @Test
    void testReadWithFilterGreaterThanPredicate() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 3);
        assertFilteredReadFindsMatchingRecord(greaterThanPredicate, v -> v > 3);
    }

    @Test
    void testReadWithFilterLessThanPredicate() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate lessThanPredicate = builder.lessThan(0, 7);
        assertFilteredReadFindsMatchingRecord(lessThanPredicate, v -> v < 7);
    }

    @Test
    void testReadWithFilterComplexPredicate() throws Exception {
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThan = builder.greaterThan(0, 3);
        Predicate lessThan = builder.lessThan(0, 7);
        Predicate complexPredicate = PredicateBuilder.and(greaterThan, lessThan);
        assertFilteredReadFindsMatchingRecord(complexPredicate, v -> v > 3 && v < 7);
    }

    /**
     * Helper method to test filtered read with a given predicate and value matcher.
     *
     * @param predicate the predicate to apply
     * @param valueMatcher function to check if first field value matches expectation
     */
    private void assertFilteredReadFindsMatchingRecord(
            Predicate predicate, IntPredicate valueMatcher) throws Exception {
        LogSegment segment = createSegment(40);
        MemoryLogRecords memoryRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        segment.append(59, -1L, -1L, memoryRecords);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo read =
                    segment.read(
                            50, 300, segment.getSizeInBytes(), true, null, predicate, readContext);
            assertThat(read).isNotNull();
            assertThat(read.getRecords().sizeInBytes()).isGreaterThan(0);

            boolean foundMatchingRecord = false;
            for (LogRecordBatch batch : read.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iterator = batch.records(readContext)) {
                    while (iterator.hasNext()) {
                        LogRecord record = iterator.next();
                        if (valueMatcher.test(record.getRow().getInt(0))) {
                            foundMatchingRecord = true;
                            break;
                        }
                    }
                }
            }
            assertThat(foundMatchingRecord).isTrue();
        }
    }

    @Test
    void testReadWithFilterMultipleBatches() throws Exception {
        // Test reading with filter across multiple batches
        LogSegment segment = createSegment(40);

        // Create multiple batches with different data
        List<Object[]> batch1Data =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"}, new Object[] {3, "c"});
        List<Object[]> batch2Data =
                Arrays.asList(new Object[] {4, "d"}, new Object[] {5, "e"}, new Object[] {6, "f"});
        List<Object[]> batch3Data =
                Arrays.asList(new Object[] {7, "g"}, new Object[] {8, "h"}, new Object[] {9, "i"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 53, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch3 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch3Data, DATA1_ROW_TYPE, 56, DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);
        segment.append(58, -1L, -1L, batch3);

        // Create predicate (first field greater than 3)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate greaterThanPredicate = builder.greaterThan(0, 3);

        // Verify that filtered records contain records from multiple batches
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Read with filter
            FetchDataInfo read =
                    segment.read(
                            50,
                            1000,
                            segment.getSizeInBytes(),
                            true,
                            null,
                            greaterThanPredicate,
                            readContext);
            assertThat(read).isNotNull();

            List<Integer> returnedValues = new ArrayList<>();
            for (LogRecordBatch batch : read.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iterator = batch.records(readContext)) {
                    while (iterator.hasNext()) {
                        LogRecord record = iterator.next();
                        returnedValues.add(record.getRow().getInt(0));
                    }
                }
            }
            // batch1 (values 1,2,3) should be EXCLUDED — all values <= 3
            assertThat(returnedValues).doesNotContain(1, 2, 3);
            // batch2 (4,5,6) and batch3 (7,8,9) should be INCLUDED
            assertThat(returnedValues).containsExactly(4, 5, 6, 7, 8, 9);
            assertThat(returnedValues).hasSize(6);
        }
    }

    @Test
    void testReadWithFilterEmptySegment() throws Exception {
        // Test reading with filter on empty segment
        LogSegment segment = createSegment(40);

        // Create predicate
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate equalPredicate = builder.equal(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Read with filter on empty segment should return null
            FetchDataInfo read =
                    segment.read(
                            40,
                            300,
                            segment.getSizeInBytes(),
                            true,
                            null,
                            equalPredicate,
                            readContext);
            assertThat(read).isNull();
        }
    }

    @Test
    void testReadWithFilterRejectsAllBatchesInSegment() throws Exception {
        // Test that when a filter rejects ALL batches in a non-empty segment,
        // we get a filtered empty response with a valid filteredEndOffset.
        LogSegment segment = createSegment(40);

        // Create multiple batches with values 1-9 (all <= 9)
        List<Object[]> batch1Data =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"}, new Object[] {3, "c"});
        List<Object[]> batch2Data =
                Arrays.asList(new Object[] {4, "d"}, new Object[] {5, "e"}, new Object[] {6, "f"});
        List<Object[]> batch3Data =
                Arrays.asList(new Object[] {7, "g"}, new Object[] {8, "h"}, new Object[] {9, "i"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 53, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch3 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch3Data, DATA1_ROW_TYPE, 56, DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);
        segment.append(58, -1L, -1L, batch3);

        // Predicate: first field > 100 — no batch can match (max value is 9)
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate predicate = builder.greaterThan(0, 100);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo read =
                    segment.read(
                            50, 1000, segment.getSizeInBytes(), true, null, predicate, readContext);
            assertThat(read).isNotNull();
            // All batches filtered out — records should be empty
            assertThat(read.getRecords().sizeInBytes()).isEqualTo(0);
            // filteredEndOffset should be set to allow client to advance
            assertThat(read.hasFilteredEndOffset()).isTrue();
            assertThat(read.getFilteredEndOffset()).isGreaterThanOrEqualTo(50L);
        }
    }

    @Test
    void testReadWithFilterExceptionInPredicateFallsBackToInclude() throws Exception {
        LogSegment segment = createSegment(40);
        MemoryLogRecords records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        segment.append(59, -1L, -1L, records);

        // Create a predicate that always throws during test()
        Predicate throwingPredicate =
                new Predicate() {
                    @Override
                    public boolean test(org.apache.fluss.row.InternalRow row) {
                        throw new RuntimeException("Simulated filter failure");
                    }

                    @Override
                    public boolean test(
                            long rowCount,
                            org.apache.fluss.row.InternalRow minValues,
                            org.apache.fluss.row.InternalRow maxValues,
                            Long[] nullCounts) {
                        throw new RuntimeException("Simulated filter failure");
                    }

                    @Override
                    public java.util.Optional<Predicate> negate() {
                        return java.util.Optional.empty();
                    }

                    @Override
                    public <T> T visit(org.apache.fluss.predicate.PredicateVisitor<T> visitor) {
                        throw new RuntimeException("Simulated filter failure");
                    }
                };

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Should NOT throw — should fall back to including the batch
            FetchDataInfo read =
                    segment.read(
                            50,
                            300,
                            segment.getSizeInBytes(),
                            true,
                            null,
                            throwingPredicate,
                            readContext);
            assertThat(read).isNotNull();
            assertThat(read.getRecords().sizeInBytes()).isGreaterThan(0);
        }
    }

    @Test
    void testReadWithFilterAndProjection() throws Exception {
        // Test filter + projection combination: filter selects batches, projection selects columns
        LogSegment segment = createSegment(40);

        // Batch 1: values 1,2,3 — should be filtered out by "a > 5"
        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}),
                        DATA1_ROW_TYPE,
                        50,
                        DEFAULT_SCHEMA_ID);
        // Batch 2: values 7,8,9 — should pass filter "a > 5"
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {7, "g"},
                                new Object[] {8, "h"},
                                new Object[] {9, "i"}),
                        DATA1_ROW_TYPE,
                        53,
                        DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);

        // Create filter: a > 5
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        // Create projection: project only column 0 (field "a")
        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                TEST_SCHEMA_GETTER,
                org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION,
                new int[] {0});

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo read =
                    segment.read(
                            50,
                            1000,
                            segment.getSizeInBytes(),
                            true,
                            projection,
                            filter,
                            readContext);
            assertThat(read).isNotNull();
            assertThat(read.getRecords().sizeInBytes()).isGreaterThan(0);

            // Verify: only batch2 data returned, projected to single column
            org.apache.fluss.types.RowType projectedType = DATA1_ROW_TYPE.project(new int[] {0});
            try (LogRecordReadContext projectedContext =
                    LogRecordReadContext.createArrowReadContext(
                            projectedType, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
                int recordCount = 0;
                for (LogRecordBatch b : read.getRecords().batches()) {
                    try (CloseableIterator<LogRecord> iter = b.records(projectedContext)) {
                        while (iter.hasNext()) {
                            LogRecord record = iter.next();
                            // Projected row should have 1 field
                            assertThat(record.getRow().getFieldCount()).isEqualTo(1);
                            // Values should be from batch2 (> 5)
                            assertThat(record.getRow().getInt(0)).isGreaterThan(5);
                            recordCount++;
                        }
                    }
                }
                assertThat(recordCount).isEqualTo(3);
            }
        }
    }

    @Test
    void testReadWithFilterAndProjectionOnEmptyBatchReturnsSkipOffset() throws Exception {
        LogSegment segment = createSegment(40);
        MemoryLogRecords emptyBatch =
                createRecordsWithoutBaseLogOffset(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        50L,
                        System.currentTimeMillis(),
                        CURRENT_LOG_MAGIC_VALUE,
                        Collections.emptyList(),
                        LogFormat.ARROW);
        segment.append(49, -1L, -1L, emptyBatch);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                TEST_SCHEMA_GETTER,
                org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION,
                new int[] {0});

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo read =
                    segment.read(
                            50,
                            1000,
                            segment.getSizeInBytes(),
                            true,
                            projection,
                            filter,
                            readContext);
            assertThat(read).isNotNull();
            assertThat(read.getRecords().sizeInBytes()).isEqualTo(0);
            assertThat(read.getFilteredEndOffset()).isGreaterThanOrEqualTo(50L);
        }
    }

    @Test
    void testReadWithFilterOffsetMetadataUsesStartOffset() throws Exception {
        LogSegment segment = createSegment(40);

        // Batch 1: values 1,2,3 — will be filtered out by a > 5
        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}),
                        DATA1_ROW_TYPE,
                        50,
                        DEFAULT_SCHEMA_ID);
        // Batch 2: values 7,8,9 — passes filter
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {7, "g"},
                                new Object[] {8, "h"},
                                new Object[] {9, "i"}),
                        DATA1_ROW_TYPE,
                        53,
                        DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo read =
                    segment.read(
                            50, 1000, segment.getSizeInBytes(), true, null, filter, readContext);
            assertThat(read).isNotNull();
            // The offset metadata should use the requested startOffset (50),
            // NOT the first included batch's baseLogOffset (53)
            assertThat(read.getFetchOffsetMetadata().getMessageOffset()).isEqualTo(50);
        }
    }

    @Test
    void testReadWithFilterAndProjectionSizeLimitUsesProjectedSize() throws Exception {
        LogSegment segment = createSegment(40);

        // Create 2 batches — all values > 5 so both pass filter "a > 5"
        // Use long strings to make unprojected size much larger than projected
        List<Object[]> batch1Data =
                Arrays.asList(
                        new Object[] {6, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                        new Object[] {7, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
                        new Object[] {8, "cccccccccccccccccccccccccccccccccccccccc"});
        List<Object[]> batch2Data =
                Arrays.asList(
                        new Object[] {9, "dddddddddddddddddddddddddddddddddddddd"},
                        new Object[] {10, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"},
                        new Object[] {11, "ffffffffffffffffffffffffffffffffffffffff"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 53, DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        // Project only column 0 (int field "a") — much smaller than full row
        FileLogProjection projection = new FileLogProjection(new ProjectionPushdownCache());
        projection.setCurrentProjection(
                1L,
                TEST_SCHEMA_GETTER,
                org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION,
                new int[] {0});

        int unprojectedBatch1Size = batch1.sizeInBytes();

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Set maxSize to slightly more than one unprojected batch but less than two.
            // With the bug: only batch1 returned (break triggered by unprojected size).
            // With the fix: both batches fit (projected sizes are smaller).
            FetchDataInfo read =
                    segment.read(
                            50,
                            unprojectedBatch1Size + 1,
                            segment.getSizeInBytes(),
                            true,
                            projection,
                            filter,
                            readContext);
            assertThat(read).isNotNull();

            int batchCount = 0;
            for (LogRecordBatch b : read.getRecords().batches()) {
                batchCount++;
            }
            assertThat(batchCount).isEqualTo(2);
        }
    }

    @Test
    void testReadWithFilterSetsFilteredEndOffsetWhenTrailingBatchesFiltered() throws Exception {
        // Scenario from review #11: first batch matches filter, trailing batches are filtered out.
        // After scanning all batches, filteredEndOffset should be set so the client can skip
        // the already-scanned-and-filtered trailing batches on the next fetch.
        LogSegment segment = createSegment(40);

        // Batch 1: values [7,8,9] — passes filter "a > 5"
        List<Object[]> batch1Data =
                Arrays.asList(new Object[] {7, "a"}, new Object[] {8, "b"}, new Object[] {9, "c"});
        // Batch 2: values [1,2,3] — filtered out by "a > 5"
        List<Object[]> batch2Data =
                Arrays.asList(new Object[] {1, "d"}, new Object[] {2, "e"}, new Object[] {3, "f"});
        // Batch 3: values [1,1,1] — filtered out by "a > 5"
        List<Object[]> batch3Data =
                Arrays.asList(new Object[] {1, "g"}, new Object[] {1, "h"}, new Object[] {1, "i"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 53, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch3 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch3Data, DATA1_ROW_TYPE, 56, DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);
        segment.append(58, -1L, -1L, batch3);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo read =
                    segment.read(
                            50, 1000, segment.getSizeInBytes(), true, null, filter, readContext);
            assertThat(read).isNotNull();
            // Should return batch 1 data
            assertThat(read.getRecords().sizeInBytes()).isGreaterThan(0);

            // Verify only batch 1 records are returned
            List<Integer> values = new ArrayList<>();
            for (LogRecordBatch batch : read.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        values.add(iter.next().getRow().getInt(0));
                    }
                }
            }
            assertThat(values).containsExactly(7, 8, 9);

            // filteredEndOffset should be set to the next offset after the last scanned batch
            // (batch3's nextLogOffset = 59), so the client can skip batches 2 and 3
            assertThat(read.hasFilteredEndOffset()).isTrue();
            assertThat(read.getFilteredEndOffset()).isEqualTo(59);
        }
    }

    @Test
    void testReadWithFilterNoFilteredEndOffsetWhenAllBatchesMatch() throws Exception {
        // When all batches pass the filter, filteredEndOffset should NOT be set
        // because there are no trailing filtered batches to skip.
        LogSegment segment = createSegment(40);

        List<Object[]> batch1Data =
                Arrays.asList(new Object[] {7, "a"}, new Object[] {8, "b"}, new Object[] {9, "c"});
        List<Object[]> batch2Data =
                Arrays.asList(
                        new Object[] {10, "d"}, new Object[] {11, "e"}, new Object[] {12, "f"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 53, DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo read =
                    segment.read(
                            50, 1000, segment.getSizeInBytes(), true, null, filter, readContext);
            assertThat(read).isNotNull();
            assertThat(read.getRecords().sizeInBytes()).isGreaterThan(0);
            // No trailing filtered batches — filteredEndOffset should NOT be set
            assertThat(read.hasFilteredEndOffset()).isFalse();
        }
    }

    @Test
    void testReadWithFilterNoFilteredEndOffsetOnSizeBreak() throws Exception {
        // When the loop exits due to maxSize (size break), we haven't scanned all batches,
        // so filteredEndOffset should NOT be set — there may be unscanned matching batches.
        LogSegment segment = createSegment(40);

        // Batch 1: passes filter
        List<Object[]> batch1Data =
                Arrays.asList(new Object[] {7, "a"}, new Object[] {8, "b"}, new Object[] {9, "c"});
        // Batch 2: also passes filter but won't fit in maxSize
        List<Object[]> batch2Data =
                Arrays.asList(
                        new Object[] {10, "d"}, new Object[] {11, "e"}, new Object[] {12, "f"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 53, DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Set maxSize to only fit batch 1 — batch 2 will trigger size break
            FetchDataInfo read =
                    segment.read(
                            50,
                            batch1.sizeInBytes(),
                            segment.getSizeInBytes(),
                            false,
                            null,
                            filter,
                            readContext);
            assertThat(read).isNotNull();
            assertThat(read.getRecords().sizeInBytes()).isGreaterThan(0);
            // Size break — not all batches scanned, so no filteredEndOffset
            assertThat(read.hasFilteredEndOffset()).isFalse();
        }
    }

    @Test
    void testReadWithFilterFilteredEndOffsetWithLeadingFilteredBatches() throws Exception {
        // Leading batches filtered, middle batch matches, trailing batches filtered.
        // filteredEndOffset should be set to skip the trailing filtered batches.
        LogSegment segment = createSegment(40);

        // Batch 1: values [1,2,3] — filtered out
        List<Object[]> batch1Data =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"}, new Object[] {3, "c"});
        // Batch 2: values [7,8,9] — passes filter
        List<Object[]> batch2Data =
                Arrays.asList(new Object[] {7, "d"}, new Object[] {8, "e"}, new Object[] {9, "f"});
        // Batch 3: values [1,2,3] — filtered out (trailing)
        List<Object[]> batch3Data =
                Arrays.asList(new Object[] {1, "g"}, new Object[] {2, "h"}, new Object[] {3, "i"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 53, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch3 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch3Data, DATA1_ROW_TYPE, 56, DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);
        segment.append(58, -1L, -1L, batch3);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo read =
                    segment.read(
                            50, 1000, segment.getSizeInBytes(), true, null, filter, readContext);
            assertThat(read).isNotNull();
            assertThat(read.getRecords().sizeInBytes()).isGreaterThan(0);

            // Verify only batch 2 records returned
            List<Integer> values = new ArrayList<>();
            for (LogRecordBatch batch : read.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        values.add(iter.next().getRow().getInt(0));
                    }
                }
            }
            assertThat(values).containsExactly(7, 8, 9);

            // filteredEndOffset should be set because batch 3 was scanned and filtered
            assertThat(read.hasFilteredEndOffset()).isTrue();
            assertThat(read.getFilteredEndOffset()).isEqualTo(59);
        }
    }

    @Test
    void testReadWithFilterNoFilteredEndOffsetWhenLastBatchMatches() throws Exception {
        // Leading batches filtered, last batch matches.
        // No trailing filtered batches, so filteredEndOffset should NOT be set.
        LogSegment segment = createSegment(40);

        // Batch 1: values [1,2,3] — filtered out
        List<Object[]> batch1Data =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"}, new Object[] {3, "c"});
        // Batch 2: values [7,8,9] — passes filter (last batch)
        List<Object[]> batch2Data =
                Arrays.asList(new Object[] {7, "d"}, new Object[] {8, "e"}, new Object[] {9, "f"});

        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch1Data, DATA1_ROW_TYPE, 50, DEFAULT_SCHEMA_ID);
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        batch2Data, DATA1_ROW_TYPE, 53, DEFAULT_SCHEMA_ID);

        segment.append(52, -1L, -1L, batch1);
        segment.append(55, -1L, -1L, batch2);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo read =
                    segment.read(
                            50, 1000, segment.getSizeInBytes(), true, null, filter, readContext);
            assertThat(read).isNotNull();
            assertThat(read.getRecords().sizeInBytes()).isGreaterThan(0);
            // Last batch matches — no trailing filtered batches
            assertThat(read.hasFilteredEndOffset()).isFalse();
        }
    }

    private LogSegment createSegment(long baseOffset) throws IOException {
        return createSegment(baseOffset, 10);
    }

    private LogSegment createSegment(long baseOffset, int indexIntervalBytes) throws IOException {
        return LogTestUtils.createSegment(baseOffset, tempDir, indexIntervalBytes);
    }

    private LogSegment createSegment(long baseOffset, boolean fileAlreadyExists, int initFileSize)
            throws IOException {
        conf.set(ConfigOptions.LOG_INDEX_INTERVAL_SIZE, MemorySize.parse("10bytes"));
        conf.set(ConfigOptions.LOG_INDEX_FILE_SIZE, MemorySize.parse("1kb"));

        return LogSegment.open(
                tempDir, baseOffset, conf, fileAlreadyExists, initFileSize, LogFormat.ARROW);
    }
}

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
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
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

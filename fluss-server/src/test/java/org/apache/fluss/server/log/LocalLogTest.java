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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordBatchStatisticsTestUtils;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.log.LocalLog.SegmentDeletionReason;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.assertLogRecordsEquals;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithBaseOffset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LocalLog}. */
final class LocalLogTest extends LogTestBase {

    private @TempDir File tempDir;
    private File logTabletDir;
    private LocalLog localLog;
    private long tableId;

    @BeforeEach
    public void setup() throws IOException {
        super.before();
        tableId = 15001L;
        logTabletDir = LogTestUtils.makeRandomLogTabletDir(tempDir, "testDb", tableId, "testTable");
        TableBucket tableBucket = new TableBucket(1001, 1);
        LogOffsetMetadata logOffsetMetadata = new LogOffsetMetadata(0L, 0L, 0);
        LogSegments logSegments = new LogSegments(tableBucket);
        localLog =
                createLocalLogWithActiveSegment(
                        logTabletDir,
                        logSegments,
                        new Configuration(),
                        0L,
                        logOffsetMetadata,
                        tableBucket);
    }

    @AfterEach
    public void after() {
        try {
            localLog.close();
        } catch (FlussRuntimeException e) {
            // do nothing.
        }
    }

    @Test
    void testLogDeleteSegmentsSuccess() throws Exception {
        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        0L, Collections.singletonList(new Object[] {1, "hello"}));
        localLog.append(0, -1L, 0L, ms1);
        localLog.roll(Optional.empty());
        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(2);
        assertThat(logTabletDir.listFiles().length == 0).isFalse();
        List<LogSegment> segmentsBeforeDelete = localLog.getSegments().values();
        Iterable<LogSegment> deletedSegments = localLog.deleteAllSegments();
        assertThat(localLog.getSegments().isEmpty()).isTrue();
        assertThat(deletedSegments).isEqualTo(segmentsBeforeDelete);
        assertThatThrownBy(() -> localLog.checkIfMemoryMappedBufferClosed())
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("The memory mapped buffer for log of");
        assertThat(logTabletDir.exists()).isTrue();
    }

    @Test
    void testRollEmptyActiveSegment() throws IOException {
        LogSegment oldActiveSegment = localLog.getSegments().activeSegment();
        localLog.roll(Optional.empty());
        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(1);
        assertThat(localLog.getSegments().activeSegment()).isNotEqualTo(oldActiveSegment);
        assertThat(logTabletDir.listFiles().length == 0).isFalse();
    }

    @Test
    void testLogDeleteDirSuccessWhenEmptyAndFailureWhenNonEmpty() throws Exception {
        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        0L, Collections.singletonList(new Object[] {1, "hello"}));
        localLog.append(0, -1L, 0L, ms1);
        localLog.roll(Optional.empty());
        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(2);
        assertThat(logTabletDir.listFiles().length == 0).isFalse();

        assertThatThrownBy(() -> localLog.deleteEmptyDir())
                .isInstanceOf(IllegalStateException.class);
        assertThat(logTabletDir.exists()).isTrue();

        localLog.deleteAllSegments();
        localLog.deleteEmptyDir();
        assertThat(logTabletDir.exists()).isFalse();
    }

    @Test
    void testLogTabletDirRenameToNewDir() throws Exception {
        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        0L, Collections.singletonList(new Object[] {1, "hello"}));
        localLog.append(0, -1L, 0L, ms1);
        localLog.roll(Optional.empty());
        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(2);

        File newLogTabletDir =
                LogTestUtils.makeRandomLogTabletDir(tempDir, "testDb", tableId, "testTable");
        assertThat(localLog.renameDir(newLogTabletDir.getName())).isTrue();
        assertThat(logTabletDir.exists()).isFalse();
        assertThat(newLogTabletDir.exists()).isTrue();
        assertThat(localLog.getLogTabletDir()).isEqualTo(newLogTabletDir);
        assertThat(localLog.getLogTabletDir().getParent()).isEqualTo(newLogTabletDir.getParent());
        localLog.getSegments()
                .values()
                .forEach(
                        logSegment ->
                                assertThat(newLogTabletDir.getPath())
                                        .isEqualTo(
                                                logSegment
                                                        .getFileLogRecords()
                                                        .file()
                                                        .getParentFile()
                                                        .getPath()));
        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(2);
    }

    @Test
    void testLogTabletDirRenameToExistingDir() throws IOException {
        assertThat(localLog.renameDir(localLog.getLogTabletDir().getName())).isFalse();
    }

    @Test
    void testFlush() throws Exception {
        assertThat(localLog.getRecoveryPoint()).isEqualTo(0L);
        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        0L, Collections.singletonList(new Object[] {1, "hello"}));
        localLog.append(0, -1L, 0L, ms1);
        LogSegment newSegment = localLog.roll(Optional.empty());
        localLog.flush(newSegment.getBaseOffset());
        localLog.markFlushed(newSegment.getBaseOffset());
        assertThat(localLog.getRecoveryPoint()).isEqualTo(1L);
    }

    @Test
    void testLogAppend() throws Exception {
        FetchDataInfo read = readLog(localLog, 0L, 1);
        assertThat(read.getRecords().sizeInBytes()).isEqualTo(0);

        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        0L, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "bye"}));
        localLog.append(1, -1L, 0L, ms1);
        assertThat(localLog.getLocalLogEndOffset()).isEqualTo(2L);
        assertThat(localLog.getRecoveryPoint()).isEqualTo(0L);

        read = readLog(localLog, 0L, localLog.getSegments().activeSegment().getSizeInBytes());
        assertLogRecordsEquals(read.getRecords(), ms1);
    }

    @Test
    void testLogCloseSuccess() throws Exception {
        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        0L, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "bye"}));
        localLog.append(1, -1L, 0L, ms1);
        localLog.close();
        MemoryLogRecords ms2 =
                genMemoryLogRecordsWithBaseOffset(
                        3L, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "bye"}));
        assertThatThrownBy(() -> localLog.append(4, -1L, 0L, ms2))
                .isInstanceOf(ClosedChannelException.class);
    }

    @Test
    void testLocalLogCloseIdempotent() {
        localLog.close();
        // Check that LocalLog.close() is idempotent
        localLog.close();
    }

    @Test
    void testLogCloseFailureWhenInMemoryBufferClosed() throws Exception {
        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        0L, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "bye"}));
        localLog.append(1, -1L, 0L, ms1);
        localLog.closeHandlers();
        assertThatThrownBy(() -> localLog.close())
                .isInstanceOf(FlussRuntimeException.class)
                .hasMessageContaining("The memory mapped buffer for log of");
    }

    @Test
    void testLogCloseHandlers() throws Exception {
        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        0L, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "bye"}));
        localLog.append(1, -1L, 0L, ms1);
        localLog.closeHandlers();
        MemoryLogRecords ms2 =
                genMemoryLogRecordsWithBaseOffset(
                        3L, Arrays.asList(new Object[] {1, "hello"}, new Object[] {2, "bye"}));
        assertThatThrownBy(() -> localLog.append(4, -1L, 0L, ms2))
                .isInstanceOf(ClosedChannelException.class);
    }

    @Test
    void testLogCloseHandlersIdempotent() {
        localLog.closeHandlers();
        // Check that LocalLog.closeHandlers() is idempotent
        localLog.closeHandlers();
    }

    @Test
    void testRemoveAndDeleteSegments() throws Exception {
        for (int i = 0; i <= 8; i++) {
            localLog.append(
                    i,
                    -1L,
                    0L,
                    genMemoryLogRecordsWithBaseOffset(
                            i, Collections.singletonList(new Object[] {i + 1, String.valueOf(i)})));
            localLog.roll(Optional.empty());
        }

        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(10);
        List<LogSegment> toDelete = localLog.getSegments().values();
        LocalLog.deleteSegmentFiles(toDelete, SegmentDeletionReason.LOG_DELETION);
        toDelete.forEach(logSegment -> assertThat(logSegment.deleted()).isTrue());
    }

    @Test
    void testCreateAndDeleteSegment() throws Exception {
        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        0L, Collections.singletonList(new Object[] {1, "hello"}));
        localLog.append(0, -1L, 0L, ms1);

        long newOffset = localLog.getSegments().activeSegment().getBaseOffset() + 1;
        LogSegment oldActiveSegment = localLog.getSegments().activeSegment();
        LogSegment newActiveSegment =
                localLog.createAndDeleteSegment(
                        newOffset,
                        localLog.getSegments().activeSegment(),
                        SegmentDeletionReason.LOG_TRUNCATION);
        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(1);
        assertThat(localLog.getSegments().activeSegment()).isEqualTo(newActiveSegment);
        assertThat(localLog.getSegments().activeSegment()).isNotEqualTo(oldActiveSegment);
        assertThat(localLog.getSegments().activeSegment().getBaseOffset()).isEqualTo(newOffset);
        assertThat(localLog.getRecoveryPoint()).isEqualTo(0L);
        assertThat(localLog.getLocalLogEndOffset()).isEqualTo(newOffset);
        FetchDataInfo read =
                readLog(
                        localLog,
                        newOffset,
                        localLog.getSegments().activeSegment().getSizeInBytes());
        assertThat(read.getRecords().sizeInBytes()).isEqualTo(0);
    }

    @Test
    void testTruncateFullyAndStartAt() throws Exception {
        for (int i = 0; i <= 7; i++) {
            localLog.append(
                    i,
                    -1L,
                    0L,
                    genMemoryLogRecordsWithBaseOffset(
                            i, Collections.singletonList(new Object[] {i + 1, String.valueOf(i)})));
            if (i % 2 != 0) {
                localLog.roll(Optional.empty());
            }
        }

        for (int i = 8; i <= 12; i++) {
            localLog.append(
                    i,
                    -1L,
                    0L,
                    genMemoryLogRecordsWithBaseOffset(
                            i, Collections.singletonList(new Object[] {i + 1, String.valueOf(i)})));
        }

        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(5);
        assertThat(localLog.getSegments().activeSegment().getBaseOffset()).isNotEqualTo(10L);
        List<LogSegment> expected = localLog.getSegments().values();
        List<LogSegment> deleted = localLog.truncateFullyAndStartAt(10L);
        assertThat(deleted).isEqualTo(expected);
        assertThat(localLog.getSegments().activeSegment().getBaseOffset()).isEqualTo(10L);
        assertThat(localLog.getRecoveryPoint()).isEqualTo(0L);
        assertThat(localLog.getLocalLogEndOffset()).isEqualTo(10L);
        FetchDataInfo read =
                readLog(localLog, 10L, localLog.getSegments().activeSegment().getSizeInBytes());
        assertThat(read.getRecords().sizeInBytes()).isEqualTo(0);
    }

    @Test
    void testTruncateTo() throws Exception {
        for (int i = 0; i <= 11; i++) {
            localLog.append(
                    i,
                    -1L,
                    0L,
                    genMemoryLogRecordsWithBaseOffset(
                            i, Collections.singletonList(new Object[] {i + 1, String.valueOf(i)})));
            if (i % 3 == 2) {
                localLog.roll(Optional.empty());
            }
        }
        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(5);
        assertThat(localLog.getLocalLogEndOffset()).isEqualTo(12L);

        List<LogSegment> expected =
                localLog.getSegments().values(9L, localLog.getLocalLogEndOffset() + 1);
        // Truncate to an offset before the base offset of the active segment
        List<LogSegment> deleted = localLog.truncateTo(7L);
        assertThat(deleted).isEqualTo(expected);
        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(3);
        assertThat(localLog.getSegments().activeSegment().getBaseOffset()).isEqualTo(6L);
        assertThat(localLog.getRecoveryPoint()).isEqualTo(0L);
        assertThat(localLog.getLocalLogEndOffset()).isEqualTo(7L);
        FetchDataInfo read =
                readLog(localLog, 6L, localLog.getSegments().activeSegment().getSizeInBytes());
        MemoryLogRecords ms1 =
                genMemoryLogRecordsWithBaseOffset(
                        6, Collections.singletonList(new Object[] {6 + 1, String.valueOf(6)}));
        assertLogRecordsEquals(read.getRecords(), ms1);

        // Verify that we can still append to the active segment
        MemoryLogRecords ms2 =
                genMemoryLogRecordsWithBaseOffset(
                        7, Collections.singletonList(new Object[] {7 + 1, String.valueOf(7)}));
        localLog.append(7, -1L, 0L, ms2);
        assertThat(localLog.getLocalLogEndOffset()).isEqualTo(8L);
    }

    @Test
    void testNonActiveSegmentFrom() throws Exception {
        for (int i = 0; i < 5; i++) {
            localLog.append(
                    i,
                    -1L,
                    0L,
                    genMemoryLogRecordsWithBaseOffset(
                            i, Collections.singletonList(new Object[] {i + 1, String.valueOf(i)})));
            localLog.roll(Optional.empty());
        }

        assertThat(localLog.getSegments().activeSegment().getBaseOffset()).isEqualTo(5L);
        assertThat(nonActiveBaseOffsetsFrom(0L)).isEqualTo(Arrays.asList(0L, 1L, 2L, 3L, 4L));
        assertThat(nonActiveBaseOffsetsFrom(5L)).isEqualTo(Collections.emptyList());
        assertThat(nonActiveBaseOffsetsFrom(2L)).isEqualTo(Arrays.asList(2L, 3L, 4L));
        assertThat(nonActiveBaseOffsetsFrom(6L)).isEqualTo(Collections.emptyList());
    }

    private List<Long> nonActiveBaseOffsetsFrom(long offset) {
        return localLog.getSegments().nonActiveLogSegmentsFrom(offset).stream()
                .map(LogSegment::getBaseOffset)
                .collect(Collectors.toList());
    }

    @Test
    void testReadWithFilterAcrossMultipleSegments() throws Exception {
        // Segment 1 (offsets 0-2): values 1,2,3 — should be filtered out by "a > 5"
        MemoryLogRecords seg1Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}),
                        DATA1_ROW_TYPE,
                        0L,
                        DEFAULT_SCHEMA_ID);
        localLog.append(2, -1L, 0L, seg1Records);
        localLog.roll(Optional.empty());

        // Segment 2 (offsets 3-5): values 4,5,3 — should be filtered out by "a > 5"
        MemoryLogRecords seg2Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {4, "d"},
                                new Object[] {5, "e"},
                                new Object[] {3, "f"}),
                        DATA1_ROW_TYPE,
                        3L,
                        DEFAULT_SCHEMA_ID);
        localLog.append(5, -1L, 0L, seg2Records);
        localLog.roll(Optional.empty());

        // Segment 3 (offsets 6-8): values 7,8,9 — should PASS filter "a > 5"
        MemoryLogRecords seg3Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {7, "g"},
                                new Object[] {8, "h"},
                                new Object[] {9, "i"}),
                        DATA1_ROW_TYPE,
                        6L,
                        DEFAULT_SCHEMA_ID);
        localLog.append(8, -1L, 0L, seg3Records);

        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(3);

        // Create filter: a > 5
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo result =
                    localLog.read(
                            0,
                            Integer.MAX_VALUE,
                            false,
                            localLog.getLocalLogEndOffsetMetadata(),
                            null,
                            filter,
                            readContext,
                            null);

            assertThat(result).isNotNull();
            // Should have found data from segment 3, skipping segments 1 and 2
            assertThat(result.getRecords().sizeInBytes()).isGreaterThan(0);

            // Verify the returned records are from segment 3 (values > 5)
            int recordCount = 0;
            for (LogRecordBatch batch : result.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        LogRecord record = iter.next();
                        assertThat(record.getRow().getInt(0)).isGreaterThan(5);
                        recordCount++;
                    }
                }
            }
            assertThat(recordCount).isEqualTo(3);
        }
    }

    @Test
    void testReadWithFilterAllSegmentsFilteredOut() throws Exception {
        // Segment 1 (offsets 0-2): values 1,2,3
        MemoryLogRecords seg1Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}),
                        DATA1_ROW_TYPE,
                        0L,
                        DEFAULT_SCHEMA_ID);
        localLog.append(2, -1L, 0L, seg1Records);
        localLog.roll(Optional.empty());

        // Segment 2 (offsets 3-5): values 4,5,3
        MemoryLogRecords seg2Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {4, "d"},
                                new Object[] {5, "e"},
                                new Object[] {3, "f"}),
                        DATA1_ROW_TYPE,
                        3L,
                        DEFAULT_SCHEMA_ID);
        localLog.append(5, -1L, 0L, seg2Records);

        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(2);

        // Create filter: a > 100 — nothing matches
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 100);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo result =
                    localLog.read(
                            0,
                            Integer.MAX_VALUE,
                            false,
                            localLog.getLocalLogEndOffsetMetadata(),
                            null,
                            filter,
                            readContext,
                            null);

            assertThat(result).isNotNull();
            // All segments filtered out — should return empty records with filteredEndOffset
            assertThat(result.getRecords().sizeInBytes()).isEqualTo(0);
            assertThat(result.getFilteredEndOffset()).isEqualTo(6);
        }
    }

    @Test
    void testReadWithFilterSingleBatchSegmentFilteredNextHasData() throws Exception {
        // Segment 1: single batch with values [1,2,3] — will be filtered by a > 5
        List<Object[]> seg1Data =
                Arrays.asList(new Object[] {1, "a"}, new Object[] {2, "b"}, new Object[] {3, "c"});
        MemoryLogRecords seg1Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        seg1Data, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        localLog.append(2L, -1L, 0L, seg1Records);
        localLog.roll(Optional.empty());

        // Segment 2: single batch with values [6,7,8] — should pass filter
        List<Object[]> seg2Data =
                Arrays.asList(new Object[] {6, "d"}, new Object[] {7, "e"}, new Object[] {8, "f"});
        MemoryLogRecords seg2Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        seg2Data, DATA1_ROW_TYPE, 3L, DEFAULT_SCHEMA_ID);
        localLog.append(5L, -1L, 0L, seg2Records);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo result =
                    localLog.read(
                            0,
                            Integer.MAX_VALUE,
                            false,
                            localLog.getLocalLogEndOffsetMetadata(),
                            null,
                            filter,
                            readContext,
                            null);

            assertThat(result).isNotNull();
            assertThat(result.getRecords().sizeInBytes()).isGreaterThan(0);
            assertThat(result.getFilteredEndOffset()).isEqualTo(-1L);

            int recordCount = 0;
            for (LogRecordBatch batch : result.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        LogRecord record = iter.next();
                        assertThat(record.getRow().getInt(0)).isGreaterThan(5);
                        recordCount++;
                    }
                }
            }
            assertThat(recordCount).isEqualTo(3);
        }
    }

    @Test
    void testReadWithFilteredEndOffsetAtSegmentBoundary() throws Exception {
        // Segment 1: offsets 0-4, values [1,2,3,4,5] — all filtered by a > 10
        List<Object[]> seg1Data =
                Arrays.asList(
                        new Object[] {1, "a"},
                        new Object[] {2, "b"},
                        new Object[] {3, "c"},
                        new Object[] {4, "d"},
                        new Object[] {5, "e"});
        MemoryLogRecords seg1Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        seg1Data, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        localLog.append(4L, -1L, 0L, seg1Records);
        localLog.roll(Optional.empty());

        // Segment 2: offsets 5-9, values [6,7,8,9,10] — also filtered by a > 10
        List<Object[]> seg2Data =
                Arrays.asList(
                        new Object[] {6, "f"},
                        new Object[] {7, "g"},
                        new Object[] {8, "h"},
                        new Object[] {9, "i"},
                        new Object[] {10, "j"});
        MemoryLogRecords seg2Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        seg2Data, DATA1_ROW_TYPE, 5L, DEFAULT_SCHEMA_ID);
        localLog.append(9L, -1L, 0L, seg2Records);
        localLog.roll(Optional.empty());

        // Segment 3: offsets 10-12, values [11,12,13] — passes filter
        List<Object[]> seg3Data =
                Arrays.asList(
                        new Object[] {11, "k"}, new Object[] {12, "l"}, new Object[] {13, "m"});
        MemoryLogRecords seg3Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        seg3Data, DATA1_ROW_TYPE, 10L, DEFAULT_SCHEMA_ID);
        localLog.append(12L, -1L, 0L, seg3Records);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 10);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            FetchDataInfo result =
                    localLog.read(
                            0,
                            Integer.MAX_VALUE,
                            false,
                            localLog.getLocalLogEndOffsetMetadata(),
                            null,
                            filter,
                            readContext,
                            null);

            assertThat(result).isNotNull();
            assertThat(result.getRecords().sizeInBytes()).isGreaterThan(0);

            // Verify returned records are from segment 3 only
            int recordCount = 0;
            for (LogRecordBatch batch : result.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        LogRecord record = iter.next();
                        assertThat(record.getRow().getInt(0)).isGreaterThan(10);
                        recordCount++;
                    }
                }
            }
            assertThat(recordCount).isEqualTo(3);
        }
    }

    @Test
    void testReadWithFilterLastSegmentLastBatchFiltered() throws Exception {
        // Segment 1: offsets 0-4, values [6,7,8,9,10] — passes filter a > 5
        List<Object[]> seg1Data =
                Arrays.asList(
                        new Object[] {6, "a"},
                        new Object[] {7, "b"},
                        new Object[] {8, "c"},
                        new Object[] {9, "d"},
                        new Object[] {10, "e"});
        MemoryLogRecords seg1Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        seg1Data, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);
        localLog.append(4L, -1L, 0L, seg1Records);
        localLog.roll(Optional.empty());

        // Segment 2: offsets 5-9, values [1,2,3,4,5] — filtered by a > 5
        List<Object[]> seg2Data =
                Arrays.asList(
                        new Object[] {1, "f"},
                        new Object[] {2, "g"},
                        new Object[] {3, "h"},
                        new Object[] {4, "i"},
                        new Object[] {5, "j"});
        MemoryLogRecords seg2Records =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        seg2Data, DATA1_ROW_TYPE, 5L, DEFAULT_SCHEMA_ID);
        localLog.append(9L, -1L, 0L, seg2Records);

        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Read starting from offset 0 — should return segment 1 data
            FetchDataInfo result1 =
                    localLog.read(
                            0,
                            Integer.MAX_VALUE,
                            false,
                            localLog.getLocalLogEndOffsetMetadata(),
                            null,
                            filter,
                            readContext,
                            null);
            assertThat(result1).isNotNull();
            assertThat(result1.getRecords().sizeInBytes()).isGreaterThan(0);

            // Read starting from offset 5 (segment 2) — all filtered
            FetchDataInfo result2 =
                    localLog.read(
                            5,
                            Integer.MAX_VALUE,
                            false,
                            localLog.getLocalLogEndOffsetMetadata(),
                            null,
                            filter,
                            readContext,
                            null);
            assertThat(result2).isNotNull();
            assertThat(result2.getRecords().sizeInBytes()).isEqualTo(0);
            // filteredEndOffset should be set to advance past the filtered data
            assertThat(result2.getFilteredEndOffset()).isEqualTo(10);
        }
    }

    @Test
    void testReadWithFilterFromMidSegmentOffset() throws Exception {
        // Batch 1 (offsets 0-2): values [1,2,3] — filtered out by "a > 5"
        MemoryLogRecords batch1 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {1, "a"},
                                new Object[] {2, "b"},
                                new Object[] {3, "c"}),
                        DATA1_ROW_TYPE,
                        0L,
                        DEFAULT_SCHEMA_ID);
        localLog.append(2, -1L, 0L, batch1);

        // Batch 2 (offsets 3-5): values [4,5,3] — filtered out by "a > 5"
        MemoryLogRecords batch2 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {4, "d"},
                                new Object[] {5, "e"},
                                new Object[] {3, "f"}),
                        DATA1_ROW_TYPE,
                        3L,
                        DEFAULT_SCHEMA_ID);
        localLog.append(5, -1L, 0L, batch2);

        // Batch 3 (offsets 6-8): values [7,8,9] — passes filter "a > 5"
        MemoryLogRecords batch3 =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        Arrays.asList(
                                new Object[] {7, "g"},
                                new Object[] {8, "h"},
                                new Object[] {9, "i"}),
                        DATA1_ROW_TYPE,
                        6L,
                        DEFAULT_SCHEMA_ID);
        localLog.append(8, -1L, 0L, batch3);

        assertThat(localLog.getSegments().numberOfSegments()).isEqualTo(1);

        // Create filter: a > 5
        PredicateBuilder builder = new PredicateBuilder(DATA1_ROW_TYPE);
        Predicate filter = builder.greaterThan(0, 5);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            // Read starting from offset 3 (mid-segment), skipping batch 1 entirely
            FetchDataInfo result =
                    localLog.read(
                            3,
                            Integer.MAX_VALUE,
                            false,
                            localLog.getLocalLogEndOffsetMetadata(),
                            null,
                            filter,
                            readContext,
                            null);

            assertThat(result).isNotNull();
            assertThat(result.getRecords().sizeInBytes()).isGreaterThan(0);

            // Verify only batch 3 records are returned (values > 5)
            int recordCount = 0;
            for (LogRecordBatch batch : result.getRecords().batches()) {
                try (CloseableIterator<LogRecord> iter = batch.records(readContext)) {
                    while (iter.hasNext()) {
                        LogRecord record = iter.next();
                        assertThat(record.getRow().getInt(0)).isGreaterThan(5);
                        recordCount++;
                    }
                }
            }
            assertThat(recordCount).isEqualTo(3);
        }
    }

    private FetchDataInfo readLog(LocalLog log, long startOffset, int maxLength) throws Exception {
        return log.read(
                startOffset,
                maxLength,
                false,
                localLog.getLocalLogEndOffsetMetadata(),
                null,
                null,
                null,
                null);
    }

    private LocalLog createLocalLogWithActiveSegment(
            File dir,
            LogSegments segments,
            Configuration logConf,
            long recoverPoint,
            LogOffsetMetadata nextOffsetMetadata,
            TableBucket tableBucket)
            throws IOException {
        segments.add(LogSegment.open(dir, 0L, logConf, LogFormat.ARROW));
        return new LocalLog(
                dir,
                logConf,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                segments,
                recoverPoint,
                nextOffsetMetadata,
                tableBucket,
                LogFormat.ARROW);
    }
}

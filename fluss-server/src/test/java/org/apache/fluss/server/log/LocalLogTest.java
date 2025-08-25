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
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.log.LocalLog.SegmentDeletionReason;

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

    private FetchDataInfo readLog(LocalLog log, long startOffset, int maxLength) throws Exception {
        return log.read(
                startOffset, maxLength, false, localLog.getLocalLogEndOffsetMetadata(), null);
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
                segments,
                recoverPoint,
                nextOffsetMetadata,
                tableBucket,
                LogFormat.ARROW);
    }
}

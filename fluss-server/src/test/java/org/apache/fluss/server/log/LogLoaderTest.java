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

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.exception.CorruptIndexException;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsWithWriterId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LogLoader}. */
final class LogLoaderTest extends LogTestBase {

    private @TempDir File tempDir;
    private FlussScheduler scheduler;
    private File logDir;
    private Clock clock;

    @BeforeEach
    public void setup() throws Exception {
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("10kb"));
        conf.set(ConfigOptions.LOG_INDEX_INTERVAL_SIZE, MemorySize.parse("1b"));

        logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        DATA1_TABLE_PATH.getDatabaseName(),
                        DATA1_TABLE_ID,
                        DATA1_TABLE_PATH.getTableName());

        scheduler = new FlussScheduler(1);
        scheduler.startup();

        clock = new ManualClock();
    }

    // TODO: add more tests like Kafka LogLoaderTest

    @Test
    void testCorruptIndexRebuild() throws Exception {
        // publish the records and close the log
        int numRecords = 200;
        LogTablet logTablet = createLogTablet(true);
        appendRecords(logTablet, numRecords);
        // collect all the index files
        List<File> indexFiles = collectIndexFiles(logTablet.logSegments());
        logTablet.close();

        // corrupt all the index files
        for (File indexFile : indexFiles) {
            try (FileChannel fileChannel =
                    FileChannel.open(indexFile.toPath(), StandardOpenOption.APPEND)) {
                for (int i = 0; i < 12; i++) {
                    fileChannel.write(ByteBuffer.wrap(new byte[] {0}));
                }
            }
        }

        // test reopen the log without recovery, sanity check of index files should throw exception
        logTablet = createLogTablet(true);
        for (LogSegment segment : logTablet.logSegments()) {
            if (segment.getBaseOffset() != logTablet.activeLogSegment().getBaseOffset()) {
                assertThatThrownBy(segment.offsetIndex()::sanityCheck)
                        .isInstanceOf(CorruptIndexException.class)
                        .hasMessage(
                                String.format(
                                        "Index file %s is corrupt, found %d bytes which is neither positive nor a multiple of %d",
                                        segment.offsetIndex().file().getAbsolutePath(),
                                        segment.offsetIndex().length(),
                                        segment.offsetIndex().entrySize()));
                assertThatThrownBy(segment.timeIndex()::sanityCheck)
                        .isInstanceOf(CorruptIndexException.class)
                        .hasMessageContaining(
                                String.format(
                                        "Corrupt time index found, time index file (%s) has non-zero size but the last timestamp is 0 which is less than the first timestamp",
                                        segment.timeIndex().file().getAbsolutePath()));
            } else {
                // the offset index file of active segment will be resized, which case no corruption
                // exception when doing sanity check
                segment.offsetIndex().sanityCheck();
                assertThatThrownBy(segment.timeIndex()::sanityCheck)
                        .isInstanceOf(CorruptIndexException.class)
                        .hasMessageContaining(
                                String.format(
                                        "Corrupt time index found, time index file (%s) has non-zero size but the last timestamp is 0 which is less than the first timestamp",
                                        segment.timeIndex().file().getAbsolutePath()));
            }
        }
        logTablet.close();

        // test reopen the log with recovery, sanity check of index files should no corruption
        logTablet = createLogTablet(false);
        for (LogSegment segment : logTablet.logSegments()) {
            segment.offsetIndex().sanityCheck();
            segment.timeIndex().sanityCheck();
        }
        assertThat(numRecords).isEqualTo(logTablet.localLogEndOffset());
        for (int i = 0; i < numRecords; i++) {
            assertThat(logTablet.lookupOffsetForTimestamp(clock.milliseconds() + i * 10))
                    .isEqualTo(i);
        }
        logTablet.close();
    }

    @Test
    void testCorruptIndexRebuildWithRecoveryPoint() throws Exception {
        // publish the records and close the log
        int numRecords = 100;
        LogTablet logTablet = createLogTablet(true);
        appendRecords(logTablet, numRecords);
        // collect all the index files
        long recoveryPoint = logTablet.localLogEndOffset() / 2;
        List<File> indexFiles = collectIndexFiles(logTablet.logSegments());
        logTablet.close();

        // corrupt all the index files
        for (File indexFile : indexFiles) {
            try (FileChannel fileChannel =
                    FileChannel.open(indexFile.toPath(), StandardOpenOption.APPEND)) {
                for (int i = 0; i < 12; i++) {
                    fileChannel.write(ByteBuffer.wrap(new byte[] {0}));
                }
            }
        }

        // test reopen the log with recovery point
        logTablet = createLogTablet(false, recoveryPoint);
        List<LogSegment> logSegments = logTablet.logSegments(recoveryPoint, Long.MAX_VALUE);
        assertThat(logSegments.size() < logTablet.logSegments().size()).isTrue();
        Set<Long> recoveredSegments =
                logSegments.stream().map(LogSegment::getBaseOffset).collect(Collectors.toSet());
        for (LogSegment segment : logTablet.logSegments()) {
            if (recoveredSegments.contains(segment.getBaseOffset())) {
                segment.offsetIndex().sanityCheck();
                segment.timeIndex().sanityCheck();
            } else {
                // the segments before recovery point will not be recovered, so sanity check should
                // still throw corrupt exception
                assertThatThrownBy(segment.offsetIndex()::sanityCheck)
                        .isInstanceOf(CorruptIndexException.class)
                        .hasMessage(
                                String.format(
                                        "Index file %s is corrupt, found %d bytes which is neither positive nor a multiple of %d",
                                        segment.offsetIndex().file().getAbsolutePath(),
                                        segment.offsetIndex().length(),
                                        segment.offsetIndex().entrySize()));
                assertThatThrownBy(segment.timeIndex()::sanityCheck)
                        .isInstanceOf(CorruptIndexException.class)
                        .hasMessageContaining(
                                String.format(
                                        "Corrupt time index found, time index file (%s) has non-zero size but the last timestamp is 0 which is less than the first timestamp",
                                        segment.timeIndex().file().getAbsolutePath()));
            }
        }
    }

    @Test
    void testIndexRebuild() throws Exception {
        // publish the records and close the log
        int numRecords = 200;
        LogTablet logTablet = createLogTablet(true);
        appendRecords(logTablet, numRecords);
        // collect all index files
        List<File> indexFiles = collectIndexFiles(logTablet.logSegments());
        logTablet.close();

        // delete all the index files
        indexFiles.forEach(File::delete);

        // reopen the log
        logTablet = createLogTablet(false);
        assertThat(logTablet.localLogEndOffset()).isEqualTo(numRecords);
        // the index files should be rebuilt
        assertThat(logTablet.logSegments().get(0).offsetIndex().entries() > 0).isTrue();
        assertThat(logTablet.logSegments().get(0).timeIndex().entries() > 0).isTrue();
        for (int i = 0; i < numRecords; i++) {
            assertThat(logTablet.lookupOffsetForTimestamp(clock.milliseconds() + i * 10))
                    .isEqualTo(i);
        }
        logTablet.close();
    }

    @Test
    void testInvalidOffsetRebuild() throws Exception {
        // publish the records and close the log
        int numRecords = 200;
        LogTablet logTablet = createLogTablet(true);
        appendRecords(logTablet, numRecords);

        List<LogSegment> logSegments = logTablet.logSegments();
        int corruptSegmentIndex = logSegments.size() / 2;
        assertThat(corruptSegmentIndex < logSegments.size()).isTrue();
        LogSegment corruptSegment = logSegments.get(corruptSegmentIndex);

        // append an invalid offset batch
        List<Object[]> objects = Collections.singletonList(new Object[] {1, "a"});
        List<ChangeType> changeTypes =
                objects.stream().map(row -> ChangeType.APPEND_ONLY).collect(Collectors.toList());
        MemoryLogRecords memoryLogRecords =
                createBasicMemoryLogRecords(
                        DATA1_ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        corruptSegment.getBaseOffset(),
                        clock.milliseconds(),
                        magic,
                        System.currentTimeMillis(),
                        0,
                        changeTypes,
                        objects,
                        LogFormat.ARROW,
                        ArrowCompressionInfo.DEFAULT_COMPRESSION);
        corruptSegment.getFileLogRecords().append(memoryLogRecords);
        logTablet.close();

        // delete the index file to trigger the recovery
        corruptSegment.offsetIndex().deleteIfExists();

        logTablet = createLogTablet(false);
        // the corrupt segment should be truncated to base offset
        assertThat(logTablet.localLogEndOffset()).isEqualTo(corruptSegment.getBaseOffset());
        // segments after the corrupt segment should be removed
        assertThat(logTablet.logSegments().size()).isEqualTo(corruptSegmentIndex + 1);
    }

    @Test
    void testWriterSnapshotRecoveryFromDiscontinuousBatchSequence() throws Exception {
        LogTablet log = createLogTablet(true);
        long wid1 = 1L;

        // Use appendAsFollower to append records with batchSequence not from 0
        log.appendAsFollower(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {1, "a"}), wid1, 10, 0L));
        log.appendAsFollower(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {2, "b"}), wid1, 11, 1L));
        log.roll(Optional.empty());

        log.appendAsFollower(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {3, "c"}), wid1, 12, 2L));
        log.appendAsFollower(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {4, "d"}), wid1, 13, 3L));

        // Close the log, we should now have 2 segments
        log.close();
        assertThat(log.logSegments().size()).isEqualTo(2);
        assertThat(
                        WriterStateManager.listSnapshotFiles(logDir).stream()
                                .map(snapshotFile -> snapshotFile.offset)
                                .sorted())
                .containsExactly(2L, 4L);

        // Delete all offset index files to trigger segment recover
        deleteAllOffsetIndexFile(log);

        log = createLogTablet(false);
        assertThat(
                        WriterStateManager.listSnapshotFiles(logDir).stream()
                                .map(snapshotFile -> snapshotFile.offset)
                                .sorted())
                .containsExactly(2L, 4L);

        assertThat(log.writerStateManager().activeWriters().size()).isEqualTo(1);
        assertThat(log.writerStateManager().activeWriters().get(wid1).lastBatchSequence())
                .isEqualTo(13);
    }

    @Test
    void testWriterSnapshotsRecoveryAfterCleanShutdown() throws Exception {
        LogTablet log = createLogTablet(true);
        assertThat(log.writerStateManager().oldestSnapshotOffset()).isEmpty();

        long wid1 = 1L;
        long wid2 = 2L;
        int seq1 = 0;
        int seq2 = 0;

        // Append some records to create multiple segments,
        for (int i = 0; i <= 5; i++) {
            if (i % 2 == 0) {
                MemoryLogRecords records =
                        genMemoryLogRecordsWithWriterId(
                                Collections.singletonList(new Object[] {seq1, "a"}),
                                wid1,
                                seq1,
                                0L);
                log.appendAsLeader(records);
                seq1++;
            } else {
                MemoryLogRecords records =
                        genMemoryLogRecordsWithWriterId(
                                Collections.singletonList(new Object[] {seq2, "a"}),
                                wid2,
                                seq2,
                                0L);
                log.appendAsLeader(records);
                seq2++;
            }
            log.roll(Optional.empty());
        }

        // Append some records to the last segment
        MemoryLogRecords records =
                genMemoryLogRecordsByObject(Collections.singletonList(new Object[] {1, "a"}));
        log.appendAsLeader(records);
        log.close();

        // Test writer state recovery after clean shutdown
        log = createLogTablet(true);

        List<Long> segmentOffsets =
                log.logSegments().stream()
                        .map(LogSegment::getBaseOffset)
                        .collect(Collectors.toList());

        // verify that the snapshot files are created
        Set<Long> expectedSnapshotOffsets =
                new HashSet<>(segmentOffsets.subList(1, segmentOffsets.size()));
        expectedSnapshotOffsets.add(log.localLogEndOffset());
        Set<Long> actualSnapshotOffsets =
                WriterStateManager.listSnapshotFiles(logDir).stream()
                        .map(snapshotFile -> snapshotFile.offset)
                        .collect(Collectors.toSet());
        assertThat(actualSnapshotOffsets)
                .containsExactlyInAnyOrderElementsOf(expectedSnapshotOffsets);

        // Verify that expected writers last batch sequence
        Map<Long, Integer> actualWritersLastBatchSequence =
                log.writerStateManager().activeWriters().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> e.getValue().lastBatchSequence()));
        Map<Long, Integer> expectedWritersLastBatchSequence = new HashMap<>();
        expectedWritersLastBatchSequence.put(wid1, seq1 - 1);
        expectedWritersLastBatchSequence.put(wid2, seq2 - 1);
        assertThat(actualWritersLastBatchSequence).isEqualTo(expectedWritersLastBatchSequence);
    }

    @Test
    void testWriterSnapshotsRecoveryAfterUncleanShutdown() throws Exception {
        LogTablet log = createLogTablet(true);
        assertThat(log.writerStateManager().oldestSnapshotOffset()).isEmpty();

        long wid1 = 1L;
        long wid2 = 2L;
        int seq1 = 0;
        int seq2 = 0;

        // Append some records to create multiple segments,
        // after this step, the segments should be
        //              [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-]
        // writer id    1      2      1      2      1      2
        // se1 id       0      0      1      1      2      2
        // snapshot            1      2      3      4      5      6
        for (int i = 0; i <= 5; i++) {
            if (i % 2 == 0) {
                MemoryLogRecords records =
                        genMemoryLogRecordsWithWriterId(
                                Collections.singletonList(new Object[] {seq1, "a"}),
                                wid1,
                                seq1,
                                0L);
                log.appendAsLeader(records);
                seq1++;
            } else {
                MemoryLogRecords records =
                        genMemoryLogRecordsWithWriterId(
                                Collections.singletonList(new Object[] {seq2, "a"}),
                                wid2,
                                seq2,
                                0L);
                log.appendAsLeader(records);
                seq2++;
            }
            log.roll(Optional.empty());
        }
        // Append some records to the last segment
        MemoryLogRecords records =
                genMemoryLogRecordsByObject(Collections.singletonList(new Object[] {1, "a"}));
        log.appendAsLeader(records);

        assertThat(log.logSegments().size()).isGreaterThanOrEqualTo(5);

        List<Long> segmentOffsets =
                log.logSegments().stream()
                        .map(LogSegment::getBaseOffset)
                        .collect(Collectors.toList());

        // verify that the snapshot files are created
        Set<Long> expectedSnapshotOffsets =
                new HashSet<>(segmentOffsets.subList(1, segmentOffsets.size()));
        Set<Long> actualSnapshotOffsets =
                WriterStateManager.listSnapshotFiles(logDir).stream()
                        .map(snapshotFile -> snapshotFile.offset)
                        .collect(Collectors.toSet());
        assertThat(actualSnapshotOffsets)
                .containsExactlyInAnyOrderElementsOf(expectedSnapshotOffsets);

        // Verify that expected writers last batch sequence
        Map<Long, Integer> actualWritersLastBatchSequence =
                log.writerStateManager().activeWriters().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> e.getValue().lastBatchSequence()));
        Map<Long, Integer> expectedWritersLastBatchSequence = new HashMap<>();
        expectedWritersLastBatchSequence.put(wid1, seq1 - 1);
        expectedWritersLastBatchSequence.put(wid2, seq2 - 1);
        assertThat(actualWritersLastBatchSequence).isEqualTo(expectedWritersLastBatchSequence);
        log.close();

        //  [0-0], [1-1], [2-2], [3-3], [4-4], [5-5], [6-]
        //                               |----> offsetForSegmentAfterRecoveryPoint
        //                        |----> recoveryPoint
        long offsetForSegmentAfterRecoveryPoint = segmentOffsets.get(segmentOffsets.size() - 3);
        long recoveryPoint = segmentOffsets.get(segmentOffsets.size() - 4);
        assertThat(recoveryPoint).isLessThan(offsetForSegmentAfterRecoveryPoint);

        // 1. Test unclean shut without any recovery
        // Retain snapshots for the last 2 segments (delete snapshots before that)
        long snapshotRetentionOffset = segmentOffsets.get(segmentOffsets.size() - 2);
        log.writerStateManager().deleteSnapshotsBefore(snapshotRetentionOffset);
        log.close();

        // Reopen the log with recovery point. Although we use unclean shutdown here,
        // all the index files are correctly close, so Fluss will not trigger recover for any
        // segment.
        log = createLogTablet(false, recoveryPoint);

        // Expected snapshot offsets: last 2 segment base offsets + log end offset
        List<Long> lastTowSegmentOffsets =
                segmentOffsets.subList(
                        Math.max(0, segmentOffsets.size() - 2), segmentOffsets.size());
        expectedSnapshotOffsets = new HashSet<>(lastTowSegmentOffsets);
        expectedSnapshotOffsets.add(log.localLogEndOffset());

        // Verify that expected snapshot offsets exist
        actualSnapshotOffsets =
                WriterStateManager.listSnapshotFiles(logDir).stream()
                        .map(snapshotFile -> snapshotFile.offset)
                        .collect(Collectors.toSet());
        assertThat(actualSnapshotOffsets).isEqualTo(expectedSnapshotOffsets);

        // Verify that expected writers last batch sequence
        actualWritersLastBatchSequence =
                log.writerStateManager().activeWriters().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> e.getValue().lastBatchSequence()));
        expectedWritersLastBatchSequence = new HashMap<>();
        expectedWritersLastBatchSequence.put(wid1, seq1 - 1);
        expectedWritersLastBatchSequence.put(wid2, seq2 - 1);
        assertThat(actualWritersLastBatchSequence).isEqualTo(expectedWritersLastBatchSequence);
        log.close();

        // 2. Test unclean shut down without recover segment will rebuild writer state
        // Delete all snapshot files
        deleteAllWriterSnapshot(logDir);

        // Reopen the log with recovery point. Although Fluss will not trigger recover for any
        // segment, but the writer state should be rebuilt.
        log = createLogTablet(false, recoveryPoint);

        actualSnapshotOffsets =
                WriterStateManager.listSnapshotFiles(logDir).stream()
                        .map(snapshotFile -> snapshotFile.offset)
                        .collect(Collectors.toSet());
        // Will rebuild writer state for all segments, but only take snapshot for the last 2
        // segments and the last offset
        expectedSnapshotOffsets = new HashSet<>();
        expectedSnapshotOffsets.add(segmentOffsets.get(segmentOffsets.size() - 2));
        expectedSnapshotOffsets.add(segmentOffsets.get(segmentOffsets.size() - 1));
        expectedSnapshotOffsets.add(log.localLogEndOffset());
        assertThat(actualSnapshotOffsets).isEqualTo(expectedSnapshotOffsets);

        // Verify that expected writers last batch sequence
        actualWritersLastBatchSequence =
                log.writerStateManager().activeWriters().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> e.getValue().lastBatchSequence()));
        expectedWritersLastBatchSequence = new HashMap<>();
        expectedWritersLastBatchSequence.put(wid1, seq1 - 1);
        expectedWritersLastBatchSequence.put(wid2, seq2 - 1);
        assertThat(actualWritersLastBatchSequence).isEqualTo(expectedWritersLastBatchSequence);
        log.close();

        // 3. Test unclean shut down with recover segment will rebuild writer state for each segment
        // after recovery point.
        // Delete all snapshot files and index files (to trigger segment recover)
        deleteAllWriterSnapshot(logDir);
        deleteAllOffsetIndexFile(log);

        // Reopen the log with recovery point. All segments will be recovered since we delete all
        // the index files
        log = createLogTablet(false, recoveryPoint);

        // Writer snapshot files should be rebuilt for each segment after recovery point
        actualSnapshotOffsets =
                WriterStateManager.listSnapshotFiles(logDir).stream()
                        .map(snapshotFile -> snapshotFile.offset)
                        .collect(Collectors.toSet());
        expectedSnapshotOffsets =
                log.logSegments(recoveryPoint, log.localLogEndOffset()).stream()
                        .map(LogSegment::getBaseOffset)
                        .collect(Collectors.toSet());
        expectedSnapshotOffsets.add(log.localLogEndOffset());
        assertThat(actualSnapshotOffsets).isEqualTo(expectedSnapshotOffsets);

        actualWritersLastBatchSequence =
                log.writerStateManager().activeWriters().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> e.getValue().lastBatchSequence()));
        expectedWritersLastBatchSequence = new HashMap<>();
        expectedWritersLastBatchSequence.put(wid1, seq1 - 1);
        expectedWritersLastBatchSequence.put(wid2, seq2 - 1);
        assertThat(actualWritersLastBatchSequence).isEqualTo(expectedWritersLastBatchSequence);
        log.close();
    }

    @Test
    void testLoadingLogKeepsLargestStrayWriterStateSnapshot() throws Exception {
        LogTablet log = createLogTablet(true);
        long wid1 = 1L;

        log.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {1, "a"}), wid1, 0, 0L));
        log.roll(Optional.empty());
        log.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {2, "b"}), wid1, 1, 0L));
        log.roll(Optional.empty());

        log.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {3, "c"}), wid1, 2, 0L));
        log.appendAsLeader(
                genMemoryLogRecordsWithWriterId(
                        Collections.singletonList(new Object[] {4, "d"}), wid1, 3, 0L));

        // Close the log, we should now have 3 segments
        log.close();
        assertThat(log.logSegments().size()).isEqualTo(3);
        // We expect 3 snapshot files, two of which are for the first two segments, the last was
        // written out during log closing.
        assertThat(
                        WriterStateManager.listSnapshotFiles(logDir).stream()
                                .map(snapshotFile -> snapshotFile.offset)
                                .sorted())
                .containsExactly(1L, 2L, 4L);
        // Inject a stray snapshot file within the bounds of the log at offset 3, it should be
        // cleaned up after loading the log
        Path path = FlussPaths.writerSnapshotFile(logDir, 3L).toPath();
        Files.createFile(path);
        assertThat(
                        WriterStateManager.listSnapshotFiles(logDir).stream()
                                .map(snapshotFile -> snapshotFile.offset)
                                .sorted())
                .containsExactly(1L, 2L, 3L, 4L);

        createLogTablet(false);
        // We should clean up the stray writer state snapshot file, but keep the largest snapshot
        // file (4)
        assertThat(
                        WriterStateManager.listSnapshotFiles(logDir).stream()
                                .map(snapshotFile -> snapshotFile.offset)
                                .sorted())
                .containsExactly(1L, 2L, 4L);
    }

    private LogTablet createLogTablet(boolean isCleanShutdown) throws Exception {
        return createLogTablet(isCleanShutdown, 0);
    }

    private LogTablet createLogTablet(boolean isCleanShutdown, long recoveryPoint)
            throws Exception {
        return LogTablet.create(
                PhysicalTablePath.of(DATA1_TABLE_PATH),
                logDir,
                conf,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                recoveryPoint,
                scheduler,
                LogFormat.ARROW,
                1,
                false,
                SystemClock.getInstance(),
                isCleanShutdown);
    }

    private void appendRecords(LogTablet logTablet, int numRecords) throws Exception {
        int baseOffset = 0;
        int batchSequence = 0;
        for (int i = 0; i < numRecords; i++) {
            List<Object[]> objects = Collections.singletonList(new Object[] {1, "a"});
            List<ChangeType> changeTypes =
                    objects.stream()
                            .map(row -> ChangeType.APPEND_ONLY)
                            .collect(Collectors.toList());
            MemoryLogRecords memoryLogRecords =
                    createBasicMemoryLogRecords(
                            DATA1_ROW_TYPE,
                            DEFAULT_SCHEMA_ID,
                            baseOffset,
                            clock.milliseconds() + i * 10L,
                            magic,
                            System.currentTimeMillis(),
                            batchSequence,
                            changeTypes,
                            objects,
                            LogFormat.ARROW,
                            ArrowCompressionInfo.DEFAULT_COMPRESSION);
            logTablet.appendAsFollower(memoryLogRecords);
            baseOffset++;
            batchSequence++;
        }
    }

    private List<File> collectIndexFiles(List<LogSegment> logSegments) throws IOException {
        List<File> indexFiles = new ArrayList<>();
        for (LogSegment segment : logSegments) {
            indexFiles.add(segment.offsetIndex().file());
            indexFiles.add(segment.timeIndex().file());
        }
        return indexFiles;
    }

    private void deleteAllOffsetIndexFile(LogTablet logTablet) throws IOException {
        List<LogSegment> logSegments = logTablet.logSegments();
        for (LogSegment segment : logSegments) {
            segment.offsetIndex().deleteIfExists();
        }
    }

    private void deleteAllWriterSnapshot(File logDir) throws Exception {
        List<SnapshotFile> snapshotFiles = WriterStateManager.listSnapshotFiles(logDir);
        for (SnapshotFile snapshotFile : snapshotFiles) {
            snapshotFile.deleteIfExists();
        }
    }
}

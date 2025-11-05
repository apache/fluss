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
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.testutils.DataTestUtils.createBasicMemoryLogRecords;
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
        int numRecords = 200;
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

        logTablet = createLogTablet(false);
        // the corrupt segment should be truncated to base offset
        assertThat(logTablet.localLogEndOffset()).isEqualTo(corruptSegment.getBaseOffset());
        // segments after the corrupt segment should be removed
        assertThat(logTablet.logSegments().size()).isEqualTo(corruptSegmentIndex + 1);
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
}

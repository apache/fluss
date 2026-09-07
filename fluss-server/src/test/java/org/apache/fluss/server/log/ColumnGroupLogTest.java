/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.fluss.exception.InvalidColumnGroupOffsetException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.LogTestBase;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the column-group shadow log ({@link ColumnGroupLog}) hosted by a {@link LogTablet}. */
class ColumnGroupLogTest extends LogTestBase {

    private static final String GROUP = "geo";

    private @TempDir File tempDir;
    private File logDir;
    private FlussScheduler scheduler;
    private LogTablet logTablet;

    @BeforeEach
    void setup() throws Exception {
        super.before();
        // small segments so group logs roll within a test
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("2kb"));
        conf.set(ConfigOptions.LOG_INDEX_INTERVAL_SIZE, MemorySize.parse("256b"));
        logDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempDir,
                        DATA1_TABLE_PATH.getDatabaseName(),
                        DATA1_TABLE_ID,
                        DATA1_TABLE_PATH.getTableName());
        scheduler = new FlussScheduler(1);
        scheduler.startup();
        logTablet = createLogTablet(true);
    }

    @AfterEach
    void teardown() throws Exception {
        if (logTablet != null) {
            logTablet.close();
        }
        scheduler.shutdown();
    }

    private LogTablet createLogTablet(boolean cleanShutdown) throws Exception {
        return LogTablet.create(
                tempDir,
                PhysicalTablePath.of(DATA1_TABLE_PATH),
                logDir,
                conf,
                new AtomicBoolean(false),
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0,
                scheduler,
                LogFormat.ARROW,
                1,
                false,
                SystemClock.getInstance(),
                cleanShutdown);
    }

    private static MemoryLogRecords rows(int from, int toExclusive) throws Exception {
        List<Object[]> objects = new ArrayList<>();
        for (int i = from; i < toExclusive; i++) {
            objects.add(new Object[] {i, "row-" + i});
        }
        return genMemoryLogRecordsByObject(objects);
    }

    /** Appends {@code count} base rows in one batch and moves the high watermark to the end. */
    private void appendBase(int count) throws Exception {
        long from = logTablet.localLogEndOffset();
        logTablet.appendAsLeader(rows((int) from, (int) from + count));
        logTablet.updateHighWatermark(logTablet.localLogEndOffset());
    }

    private static List<long[]> batchRanges(LogRecords records) {
        List<long[]> ranges = new ArrayList<>();
        for (LogRecordBatch batch : records.batches()) {
            ranges.add(new long[] {batch.baseLogOffset(), batch.lastLogOffset()});
        }
        return ranges;
    }

    @Test
    void testAppendStampsBaseOffsetsAndAdvancesWatermark() throws Exception {
        appendBase(10);

        ColumnGroupAppendInfo info = logTablet.appendColumnsAsLeader(GROUP, rows(0, 4), 0L);
        assertThat(info.firstOffset()).isEqualTo(0L);
        assertThat(info.lastOffset()).isEqualTo(3L);
        assertThat(info.rowCount()).isEqualTo(4);
        assertThat(info.isDuplicated()).isFalse();
        assertThat(logTablet.getColumnGroupLogEndOffset(GROUP)).isEqualTo(4L);
        // the high watermark is the replica's business; the log alone leaves it at 0
        assertThat(logTablet.getColumnGroupHighWatermark(GROUP)).isEqualTo(0L);

        info = logTablet.appendColumnsAsLeader(GROUP, rows(4, 10), 4L);
        assertThat(info.lastOffset()).isEqualTo(9L);
        assertThat(logTablet.getColumnGroupLogEndOffset(GROUP)).isEqualTo(10L);

        // batches carry the base offsets they fill and a real commit timestamp
        LogRecords all = logTablet.readColumnGroup(GROUP, 0L, 9L, Integer.MAX_VALUE);
        assertThat(batchRanges(all)).containsExactly(new long[] {0, 3}, new long[] {4, 9});
        for (LogRecordBatch batch : all.batches()) {
            assertThat(batch.commitTimestamp()).isGreaterThan(0L);
            assertThat(batch.schemaId()).isEqualTo(schemaId);
        }
        assertThat(logTablet.getColumnGroupLog(GROUP).highWatermark()).isEqualTo(0L);
        assertThat(FlussPaths.columnGroupLogDir(logDir, GROUP)).isDirectory();
    }

    @Test
    void testValidation() throws Exception {
        appendBase(10);

        // gap
        assertThatThrownBy(() -> logTablet.appendColumnsAsLeader(GROUP, rows(2, 4), 2L))
                .isInstanceOf(InvalidColumnGroupOffsetException.class)
                .satisfies(
                        e ->
                                assertThat(
                                                ((InvalidColumnGroupOffsetException) e)
                                                        .getExpectedSourceOffset())
                                        .isEqualTo(0L));
        // past the base high watermark
        assertThatThrownBy(() -> logTablet.appendColumnsAsLeader(GROUP, rows(0, 11), 0L))
                .isInstanceOf(InvalidColumnGroupOffsetException.class)
                .hasMessageContaining("high watermark");

        logTablet.appendColumnsAsLeader(GROUP, rows(0, 5), 0L);

        // whole-batch replay is a no-op
        ColumnGroupAppendInfo dup = logTablet.appendColumnsAsLeader(GROUP, rows(1, 3), 1L);
        assertThat(dup.isDuplicated()).isTrue();
        assertThat(logTablet.getColumnGroupLogEndOffset(GROUP)).isEqualTo(5L);

        // straddling batch carries the expected offset
        assertThatThrownBy(() -> logTablet.appendColumnsAsLeader(GROUP, rows(3, 8), 3L))
                .isInstanceOf(InvalidColumnGroupOffsetException.class)
                .satisfies(
                        e ->
                                assertThat(
                                                ((InvalidColumnGroupOffsetException) e)
                                                        .getExpectedSourceOffset())
                                        .isEqualTo(5L));
    }

    @Test
    void testRollAndRangedReadAcrossSegments() throws Exception {
        appendBase(200);
        for (int i = 0; i < 200; i += 10) {
            logTablet.appendColumnsAsLeader(GROUP, rows(i, i + 10), i);
        }
        ColumnGroupLog columnGroupLog = logTablet.getColumnGroupLog(GROUP);
        assertThat(columnGroupLog.logEndOffset()).isEqualTo(200L);
        assertThat(columnGroupLog.segments().size()).isGreaterThan(1);

        // a range inside one batch returns just that batch
        assertThat(batchRanges(logTablet.readColumnGroup(GROUP, 42L, 47L, Integer.MAX_VALUE)))
                .containsExactly(new long[] {40, 49});
        // a range spanning segments returns every batch covering it and nothing beyond
        List<long[]> ranges =
                batchRanges(logTablet.readColumnGroup(GROUP, 95L, 133L, Integer.MAX_VALUE));
        assertThat(ranges.get(0)).containsExactly(90L, 99L);
        assertThat(ranges.get(ranges.size() - 1)).containsExactly(130L, 139L);
        assertThat(ranges).hasSize(5);
        // a byte budget cuts at whole batches but always returns at least one
        List<long[]> budgeted = batchRanges(logTablet.readColumnGroup(GROUP, 0L, 199L, 1));
        assertThat(budgeted).hasSize(1);
        assertThat(budgeted.get(0)).containsExactly(0L, 9L);
        // nothing beyond the log end
        assertThat(logTablet.readColumnGroup(GROUP, 200L, 250L, Integer.MAX_VALUE).sizeInBytes())
                .isEqualTo(0);
    }

    @Test
    void testTruncateFollowsBaseLog() throws Exception {
        appendBase(50);
        for (int i = 0; i < 50; i += 10) {
            logTablet.appendColumnsAsLeader(GROUP, rows(i, i + 10), i);
        }
        assertThat(logTablet.getColumnGroupLogEndOffset(GROUP)).isEqualTo(50L);

        // like the base log, batches are truncated whole (the batch [20, 29] is dropped) while
        // the end offset becomes the requested one, so both logs stay aligned
        logTablet.truncateTo(25L);
        assertThat(logTablet.localLogEndOffset()).isEqualTo(25L);
        assertThat(logTablet.getColumnGroupLogEndOffset(GROUP)).isEqualTo(25L);
        assertThat(batchRanges(logTablet.readColumnGroup(GROUP, 0L, 49L, Integer.MAX_VALUE)))
                .containsExactly(new long[] {0, 9}, new long[] {10, 19});

        // the group continues from the truncation point
        logTablet.appendAsLeader(rows(25, 30));
        logTablet.updateHighWatermark(30L);
        logTablet.appendColumnsAsLeader(GROUP, rows(25, 30), 25L);
        assertThat(logTablet.getColumnGroupLogEndOffset(GROUP)).isEqualTo(30L);
    }

    @Test
    void testReopenRecoversGroupLog() throws Exception {
        appendBase(60);
        for (int i = 0; i < 60; i += 10) {
            logTablet.appendColumnsAsLeader(GROUP, rows(i, i + 10), i);
        }
        logTablet.flush(true);
        logTablet.close();

        // an unclean reopen re-scans the last segment and restores the watermark
        logTablet = createLogTablet(false);
        assertThat(logTablet.getColumnGroupLogs()).containsKey(GROUP);
        assertThat(logTablet.getColumnGroupLogEndOffset(GROUP)).isEqualTo(60L);
        assertThat(batchRanges(logTablet.readColumnGroup(GROUP, 55L, 59L, Integer.MAX_VALUE)))
                .containsExactly(new long[] {50, 59});

        // and appends continue where they stopped: a replay of filled rows is a no-op, a gap is
        // rejected with the recovered offset
        logTablet.updateHighWatermark(60L);
        assertThat(logTablet.appendColumnsAsLeader(GROUP, rows(0, 5), 0L).isDuplicated()).isTrue();
        assertThat(logTablet.getColumnGroupLogEndOffset(GROUP)).isEqualTo(60L);
        assertThatThrownBy(() -> logTablet.appendColumnsAsLeader(GROUP, rows(0, 5), 70L))
                .isInstanceOf(InvalidColumnGroupOffsetException.class)
                .hasMessageContaining("expects rows to start at offset 60")
                .satisfies(
                        e ->
                                assertThat(
                                                ((InvalidColumnGroupOffsetException) e)
                                                        .getExpectedSourceOffset())
                                        .isEqualTo(60L));
    }
}

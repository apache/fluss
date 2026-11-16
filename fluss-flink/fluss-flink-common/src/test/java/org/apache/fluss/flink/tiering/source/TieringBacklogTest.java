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

package org.apache.fluss.flink.tiering.source;

import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSnapshotSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TieringBacklog}. */
class TieringBacklogTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "table");

    @Test
    void testLogSplitBacklogIsItsOffsetRange() {
        assertThat(TieringBacklog.estimate(logSplit(0, 100, 250))).isEqualTo(150L);
        // nothing new since the last round
        assertThat(TieringBacklog.estimate(logSplit(0, 250, 250))).isEqualTo(0L);
    }

    @Test
    void testLogSplitFromEarliestOffsetCountsFromZero() {
        assertThat(TieringBacklog.estimate(logSplit(0, EARLIEST_OFFSET, 80))).isEqualTo(80L);
    }

    @Test
    void testSnapshotSplitBacklogIsItsLogOffset() {
        TieringSplit snapshotSplit =
                new TieringSnapshotSplit(TABLE_PATH, new TableBucket(1L, 0), null, 3L, 500L, 1);
        assertThat(TieringBacklog.estimate(snapshotSplit)).isEqualTo(500L);
    }

    @Test
    void testSkippedSplitHasNoBacklog() {
        TieringSplit split = logSplit(0, 0, 100);
        split.skipCurrentRound();
        assertThat(TieringBacklog.estimate(split)).isEqualTo(0L);
    }

    @Test
    void testBacklogOfSeveralSplits() {
        assertThat(TieringBacklog.estimate(Collections.<TieringSplit>emptyList())).isEqualTo(0L);
        assertThat(
                        TieringBacklog.estimate(
                                Arrays.asList(
                                        logSplit(0, 0, 10),
                                        logSplit(1, 5, 25),
                                        logSplit(2, 30, 30))))
                .isEqualTo(30L);
    }

    @Test
    void testRemainingNeverNegative() {
        assertThat(TieringBacklog.remaining(10, 25)).isEqualTo(15L);
        assertThat(TieringBacklog.remaining(25, 25)).isEqualTo(0L);
        assertThat(TieringBacklog.remaining(30, 25)).isEqualTo(0L);
    }

    private static TieringSplit logSplit(int bucket, long startingOffset, long stoppingOffset) {
        return new TieringLogSplit(
                TABLE_PATH, new TableBucket(1L, bucket), null, startingOffset, stoppingOffset, 3);
    }
}

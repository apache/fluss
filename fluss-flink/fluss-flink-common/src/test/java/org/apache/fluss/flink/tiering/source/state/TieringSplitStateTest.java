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

package org.apache.fluss.flink.tiering.source.state;

import org.apache.fluss.flink.tiering.source.split.TieringLogSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSnapshotSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TieringSplitState} . */
class TieringSplitStateTest {

    @Test
    void testTieringSnapshotSplit() {
        TablePath tablePath = TablePath.of("test_db_1", "test_table_1");
        TableBucket tableBucket = new TableBucket(1, 1024L, 2);

        // verify conversion between TieringSnapshotSplitState and TieringSnapshotSplit
        TieringSnapshotSplit tieringSnapshotSplit =
                new TieringSnapshotSplit(
                        tablePath, tableBucket, "partition1", 0L, 200L, 10, 0, 1000L);
        tieringSnapshotSplit.skipCurrentRound();
        TieringSplitState tieringSnapshotSplitState = new TieringSplitState(tieringSnapshotSplit);
        TieringSnapshotSplit restoredSplit =
                (TieringSnapshotSplit) tieringSnapshotSplitState.toSourceSplit();
        assertThat(restoredSplit).isEqualTo(tieringSnapshotSplit);
        assertThat(restoredSplit.shouldSkipCurrentRound()).isTrue();
        assertThat(restoredSplit.getSplitIndex()).isZero();
        assertThat(restoredSplit.getTieringRoundTimestamp()).isEqualTo(1000L);
    }

    @Test
    void testTieringLogSplit() {
        TablePath tablePath = TablePath.of("test_db_1", "test_table_1");
        TableBucket tableBucket = new TableBucket(1, 1024L, 2);

        // verify conversion between TieringLogSplitState and TieringLogSplit
        TieringLogSplit tieringLogSplit =
                new TieringLogSplit(tablePath, tableBucket, "partition1", 100L, 200L, 20, 1, 2000L);
        tieringLogSplit.skipCurrentRound();
        TieringSplitState tieringLogSplitState = new TieringSplitState(tieringLogSplit);
        TieringLogSplit restoredSplit = (TieringLogSplit) tieringLogSplitState.toSourceSplit();
        assertThat(restoredSplit).isEqualTo(tieringLogSplit);
        assertThat(restoredSplit.shouldSkipCurrentRound()).isTrue();
        assertThat(restoredSplit.getSplitIndex()).isEqualTo(1);
        assertThat(restoredSplit.getTieringRoundTimestamp()).isEqualTo(2000L);
    }
}

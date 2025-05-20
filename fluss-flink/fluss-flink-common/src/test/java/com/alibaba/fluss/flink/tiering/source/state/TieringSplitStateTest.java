/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.source.state;

import com.alibaba.fluss.flink.tiering.source.split.TieringKvSplit;
import com.alibaba.fluss.flink.tiering.source.split.TieringLogSplit;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TieringKvSplitState} and {@link TieringLogSplitState}. */
class TieringSplitStateTest {

    @Test
    void testTieringKvSplitState() {
        TablePath tablePath = TablePath.of("test_db_1", "test_table_1");
        TableBucket tableBucket = new TableBucket(1, 1024L, 2);

        // verify with records to skip
        TieringKvSplit tieringKvSplit =
                new TieringKvSplit(tablePath, tableBucket, "partition1", 0L, 200L, 5L);
        TieringKvSplitState tieringKvSplitState = new TieringKvSplitState(tieringKvSplit, 5L);
        assertThat(tieringKvSplitState.toSourceSplit()).isEqualTo(tieringKvSplit);

        // advance records to skip of tiering kv split state
        tieringKvSplitState.setRecordsToSkip(10L);
        TieringKvSplit expectedTieringKvSplit =
                new TieringKvSplit(tablePath, tableBucket, "partition1", 0L, 200L, 10L);
        assertThat(tieringKvSplitState.toSourceSplit()).isEqualTo(expectedTieringKvSplit);
    }

    @Test
    void testTieringLogSplitState() {
        TablePath tablePath = TablePath.of("test_db_1", "test_table_1");
        TableBucket tableBucket = new TableBucket(1, 1024L, 2);

        // verify with starting offset
        TieringLogSplit tieringLogSplit =
                new TieringLogSplit(tablePath, tableBucket, "partition1", 100L, 200L);
        TieringLogSplitState tieringLogSplitState = new TieringLogSplitState(tieringLogSplit, 100L);
        assertThat(tieringLogSplitState.toSourceSplit()).isEqualTo(tieringLogSplit);

        // advance next offset of tiering log split state
        tieringLogSplitState.nextOffset(105L);
        TieringLogSplit expectedTieringLogSplit =
                new TieringLogSplit(tablePath, tableBucket, "partition1", 105L, 200L);
        assertThat(tieringLogSplitState.toSourceSplit()).isEqualTo(expectedTieringLogSplit);
    }
}

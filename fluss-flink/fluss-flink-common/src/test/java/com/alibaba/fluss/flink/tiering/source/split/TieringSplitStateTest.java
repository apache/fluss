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

package com.alibaba.fluss.flink.tiering.source.split;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TieringSplitState}. */
class TieringSplitStateTest {

    @Test
    void testTieringSplitState() {
        TablePath tablePath = TablePath.of("test_db_1", "test_table_1");
        TableBucket tableBucket = new TableBucket(1, 1024L, 2);

        // verify with starting offset
        TieringSplit tieringSplit =
                new TieringSplit(tablePath, tableBucket, "partition1", 100L, 200L);
        TieringSplitState tieringSplitState = new TieringSplitState(tieringSplit);
        assertThat(tieringSplitState.toSplit()).isEqualTo(tieringSplit);

        // advance next offset of tiering split state
        tieringSplitState.setNextOffset(105L);
        TieringSplit expectedTieringSplit =
                new TieringSplit(tablePath, tableBucket, "partition1", 105L, 200L);
        assertThat(tieringSplitState.toSplit()).isEqualTo(expectedTieringSplit);

        // verify with stopping offset
        tieringSplit = new TieringSplit(tablePath, tableBucket, "partition1", 105L, 2000L);
        tieringSplitState = new TieringSplitState(tieringSplit);
        assertThat(tieringSplitState.toSplit()).isEqualTo(tieringSplit);
    }
}

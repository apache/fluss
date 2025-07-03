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

package com.alibaba.fluss.flink.tiering.source.state;

import com.alibaba.fluss.flink.tiering.source.split.TieringLogSplit;
import com.alibaba.fluss.flink.tiering.source.split.TieringSnapshotSplit;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

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
                new TieringSnapshotSplit(tablePath, tableBucket, "partition1", 0L, 200L, 10);
        TieringSplitState tieringSnapshotSplitState = new TieringSplitState(tieringSnapshotSplit);
        assertThat(tieringSnapshotSplitState.toSourceSplit()).isEqualTo(tieringSnapshotSplit);
    }

    @Test
    void testTieringLogSplit() {
        TablePath tablePath = TablePath.of("test_db_1", "test_table_1");
        TableBucket tableBucket = new TableBucket(1, 1024L, 2);

        // verify conversion between TieringLogSplitState and TieringLogSplit
        TieringLogSplit tieringLogSplit =
                new TieringLogSplit(tablePath, tableBucket, "partition1", 100L, 200L, 20);
        TieringSplitState tieringLogSplitState = new TieringSplitState(tieringLogSplit);
        assertThat(tieringLogSplitState.toSourceSplit()).isEqualTo(tieringLogSplit);
    }
}

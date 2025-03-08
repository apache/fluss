/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.source.state;

import com.alibaba.fluss.lakehouse.paimon.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.lakehouse.paimon.source.split.HybridSnapshotLogSplitState;
import com.alibaba.fluss.lakehouse.paimon.source.split.LogSplit;
import com.alibaba.fluss.lakehouse.paimon.source.split.LogSplitState;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogSplitState} and {@link HybridSnapshotLogSplitState} . */
class SourceSplitStateTest {

    @Test
    void testLogSplitState() {
        TablePath tablePath = new TablePath("db1", "tbl1");
        TableBucket tableBucket = new TableBucket(0, 0L, 0);

        LogSplit logSplit = new LogSplit(tablePath, tableBucket, "partition1", 100L);
        LogSplitState logSplitState = new LogSplitState(logSplit);
        assertThat(logSplitState.toSourceSplit()).isEqualTo(logSplit);

        // advance next offset of log split state, and verify
        logSplitState.setNextOffset(200L);
        LogSplit expectedLogSplit = new LogSplit(tablePath, tableBucket, "partition1", 200L);
        assertThat(logSplitState.toSourceSplit()).isEqualTo(expectedLogSplit);

        // advance next offset of log split state, and verify
        logSplitState.setNextOffset(300L);
        expectedLogSplit = new LogSplit(tablePath, tableBucket, "partition1", 300L);
        assertThat(logSplitState.toSourceSplit()).isEqualTo(expectedLogSplit);
    }

    @Test
    void testHybridSnapshotLogSplitState() {
        TablePath tablePath = new TablePath("db1", "tbl1");
        TableBucket tableBucket = new TableBucket(0, 0L, 0);

        // verify the split that snapshot is not finished
        HybridSnapshotLogSplit hybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(tablePath, tableBucket, "partition1", 1L, 100L);
        HybridSnapshotLogSplitState hybridSnapshotLogSplitState =
                new HybridSnapshotLogSplitState(hybridSnapshotLogSplit);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit()).isEqualTo(hybridSnapshotLogSplit);

        // set record to skip and verify
        hybridSnapshotLogSplitState.setRecordsToSkip(200L);
        HybridSnapshotLogSplit expectedHybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(
                        tablePath, tableBucket, "partition1", 1L, 200L, false, 100L);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit())
                .isEqualTo(expectedHybridSnapshotLogSplit);

        // set next offset and verify
        hybridSnapshotLogSplitState.setNextOffset(500L);
        expectedHybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(
                        tablePath, tableBucket, "partition1", 1L, 200L, true, 500L);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit())
                .isEqualTo(expectedHybridSnapshotLogSplit);

        // verify the split that snapshot is finished
        hybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(
                        tablePath, tableBucket, "partition1", 1L, 100L, true, 100L);
        hybridSnapshotLogSplitState = new HybridSnapshotLogSplitState(hybridSnapshotLogSplit);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit()).isEqualTo(hybridSnapshotLogSplit);

        // set next offset and verify
        hybridSnapshotLogSplitState = new HybridSnapshotLogSplitState(hybridSnapshotLogSplit);
        hybridSnapshotLogSplitState.setNextOffset(500L);
        expectedHybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(
                        tablePath, tableBucket, "partition1", 1L, 100L, true, 500L);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit())
                .isEqualTo(expectedHybridSnapshotLogSplit);
    }
}

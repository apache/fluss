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

package org.apache.fluss.flink.source.state;

import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogSplitState} and {@link HybridSnapshotLogSplitState} . */
class SourceSplitStateTest {

    @Test
    void testLogSplitState() {
        TableBucket tableBucket = new TableBucket(0, 0L, 0);

        // verify if no stopping offset
        LogSplit logSplit = new LogSplit(tableBucket, "partition1", 100L);
        LogSplitState logSplitState = new LogSplitState(logSplit);
        assertThat(logSplitState.toSourceSplit()).isEqualTo(logSplit);

        // advance next offset of log split state, and verify
        logSplitState.setNextOffset(200L);
        LogSplit expectedLogSplit = new LogSplit(tableBucket, "partition1", 200L);
        assertThat(logSplitState.toSourceSplit()).isEqualTo(expectedLogSplit);

        // verify with stopping offset
        logSplit = new LogSplit(tableBucket, "partition1", 100L, 2000L);
        logSplitState = new LogSplitState(logSplit);
        assertThat(logSplitState.toSourceSplit()).isEqualTo(logSplit);

        // advance next offset of log split state, and verify
        logSplitState.setNextOffset(300L);
        expectedLogSplit = new LogSplit(tableBucket, "partition1", 300L, 2000L);
        assertThat(logSplitState.toSourceSplit()).isEqualTo(expectedLogSplit);
    }

    @Test
    void testHybridSnapshotLogSplitState() {
        TableBucket tableBucket = new TableBucket(0, 0L, 0);

        // verify the split that snapshot is not finished
        HybridSnapshotLogSplit hybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(tableBucket, "partition1", 1L, 100L);
        HybridSnapshotLogSplitState hybridSnapshotLogSplitState =
                new HybridSnapshotLogSplitState(hybridSnapshotLogSplit);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit()).isEqualTo(hybridSnapshotLogSplit);

        // set record to skip and verify
        hybridSnapshotLogSplitState.setRecordsToSkip(200L);
        HybridSnapshotLogSplit expectedHybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(tableBucket, "partition1", 1L, 200L, false, 100L);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit())
                .isEqualTo(expectedHybridSnapshotLogSplit);

        // set next offset and verify
        hybridSnapshotLogSplitState.setNextOffset(500L);
        expectedHybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(tableBucket, "partition1", 1L, 200L, true, 500L);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit())
                .isEqualTo(expectedHybridSnapshotLogSplit);

        // verify the split that snapshot is finished
        hybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(tableBucket, "partition1", 1L, 100L, true, 100L);
        hybridSnapshotLogSplitState = new HybridSnapshotLogSplitState(hybridSnapshotLogSplit);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit()).isEqualTo(hybridSnapshotLogSplit);

        // set next offset and verify
        hybridSnapshotLogSplitState = new HybridSnapshotLogSplitState(hybridSnapshotLogSplit);
        hybridSnapshotLogSplitState.setNextOffset(500L);
        expectedHybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(tableBucket, "partition1", 1L, 100L, true, 500L);
        assertThat(hybridSnapshotLogSplitState.toSourceSplit())
                .isEqualTo(expectedHybridSnapshotLogSplit);
    }
}

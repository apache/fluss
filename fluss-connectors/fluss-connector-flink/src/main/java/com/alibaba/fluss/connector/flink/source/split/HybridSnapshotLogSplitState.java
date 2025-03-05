/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.source.split;

/** The state of {@link HybridSnapshotLogSplit}. */
public class HybridSnapshotLogSplitState extends SourceSplitState {

    /** The records to skip while reading a snapshot. */
    private long recordsToSkip;
    /** Whether the snapshot reading is finished. */
    private boolean snapshotFinished;

    /** The next log offset to read. */
    private long nextOffset;

    public HybridSnapshotLogSplitState(HybridSnapshotLogSplit hybridSnapshotLogSplit) {
        super(hybridSnapshotLogSplit);
        this.recordsToSkip = hybridSnapshotLogSplit.recordsToSkip();
        this.snapshotFinished = hybridSnapshotLogSplit.isSnapshotFinished();
        this.nextOffset = hybridSnapshotLogSplit.getLogStartingOffset();
    }

    @Override
    public HybridSnapshotLogSplit toSourceSplit() {
        final HybridSnapshotLogSplit hybridSnapshotLogSplit = split.asHybridSnapshotLogSplit();
        return new HybridSnapshotLogSplit(
                hybridSnapshotLogSplit.tableBucket,
                hybridSnapshotLogSplit.getPartitionName(),
                hybridSnapshotLogSplit.getSnapshotId(),
                recordsToSkip,
                snapshotFinished,
                nextOffset);
    }

    public void setRecordsToSkip(long recordsToSkip) {
        this.recordsToSkip = recordsToSkip;
    }

    public void setNextOffset(long nextOffset) {
        // if set offset, means snapshot is finished
        snapshotFinished = true;
        this.nextOffset = nextOffset;
    }
}

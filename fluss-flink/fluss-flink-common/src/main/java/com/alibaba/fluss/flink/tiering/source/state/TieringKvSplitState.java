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
import com.alibaba.fluss.flink.tiering.source.split.TieringSplit;

/** The state for {@link TieringKvSplit}. */
public class TieringKvSplitState extends TieringSplitState {

    /** The records to skip while reading a snapshot. */
    private long recordsToSkip;

    public TieringKvSplitState(TieringSplit tieringSplit, long recordsToSkip) {
        super(tieringSplit);
        this.recordsToSkip = recordsToSkip;
    }

    public void setRecordsToSkip(long recordsToSkip) {
        this.recordsToSkip = recordsToSkip;
    }

    @Override
    public TieringSplit toSourceSplit() {
        TieringKvSplit tieringKvSplit = (TieringKvSplit) this.tieringSplit;
        return new TieringKvSplit(
                tieringKvSplit.getTablePath(),
                tieringKvSplit.getTableBucket(),
                tieringKvSplit.getPartitionName(),
                tieringKvSplit.getSnapshotId(),
                tieringKvSplit.getLogOffsetOfSnapshot(),
                recordsToSkip);
    }
}

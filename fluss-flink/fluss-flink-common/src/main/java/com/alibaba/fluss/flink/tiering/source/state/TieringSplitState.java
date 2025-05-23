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

import com.alibaba.fluss.flink.tiering.source.split.TieringLogSplit;
import com.alibaba.fluss.flink.tiering.source.split.TieringSnapshotSplit;
import com.alibaba.fluss.flink.tiering.source.split.TieringSplit;

/**
 * The state of a {@link TieringSplit}.
 *
 * <p>Note: The tiering service adopts a stateless design and does not store any progress
 * information in state during checkpoints. All splits are re-requested from the Fluss cluster in
 * case of failover.
 */
public class TieringSplitState {

    protected final TieringSplit tieringSplit;

    public TieringSplitState(TieringSplit tieringSplit) {
        this.tieringSplit = tieringSplit;
    }

    public TieringSplit toSourceSplit() {
        if (tieringSplit.isTieringSnapshotSplit()) {
            final TieringSnapshotSplit split = (TieringSnapshotSplit) this.tieringSplit;
            return new TieringSnapshotSplit(
                    split.getTablePath(),
                    split.getTableBucket(),
                    split.getPartitionName(),
                    split.getSnapshotId(),
                    split.getLogOffsetOfSnapshot());
        } else {
            final TieringLogSplit split = (TieringLogSplit) tieringSplit;
            return new TieringLogSplit(
                    split.getTablePath(),
                    split.getTableBucket(),
                    split.getPartitionName(),
                    split.getStartingOffset(),
                    split.getStoppingOffset());
        }
    }
}

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

/** The state for {@link TieringLogSplit}. */
public class TieringLogSplitState extends TieringSplitState {

    /** The next log offset to sync(tier) to lake, should only be monotonically increasing. */
    private long nextOffset;

    public TieringLogSplitState(TieringLogSplit tieringSplit, long nextOffset) {
        super(tieringSplit);
        this.nextOffset = nextOffset;
    }

    public void nextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    @Override
    public TieringLogSplit toSourceSplit() {
        final TieringLogSplit split = (TieringLogSplit) tieringSplit;
        return new TieringLogSplit(
                split.getTablePath(),
                split.getTableBucket(),
                split.getPartitionName(),
                nextOffset,
                split.getStoppingOffset());
    }
}

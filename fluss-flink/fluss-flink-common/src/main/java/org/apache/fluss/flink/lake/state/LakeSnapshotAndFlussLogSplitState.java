/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.lake.state;

import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;

/** The state of {@link LakeSnapshotAndFlussLogSplit}. */
public class LakeSnapshotAndFlussLogSplitState extends SourceSplitState {

    private long recordsToSkip;
    private final LakeSnapshotAndFlussLogSplit split;
    private int currentLakeSplitIndex;
    private long nextLogOffset;

    private boolean isLakeSplitFinished;

    public LakeSnapshotAndFlussLogSplitState(LakeSnapshotAndFlussLogSplit split) {
        super(split);
        this.recordsToSkip = split.getRecordsToSkip();
        this.split = split;
        this.currentLakeSplitIndex = split.getCurrentLakeSplitIndex();
        this.nextLogOffset = split.getStartingOffset();
        this.isLakeSplitFinished = split.isLakeSplitFinished();
    }

    public void setRecordsToSkip(long recordsToSkip) {
        this.recordsToSkip = recordsToSkip;
    }

    public void setCurrentLakeSplitIndex(int currentLakeSplitIndex) {
        this.currentLakeSplitIndex = currentLakeSplitIndex;
    }

    public void setNextLogOffset(long nextOffset) {
        // if set offset, means lake splits is finished
        isLakeSplitFinished = true;
        this.nextLogOffset = nextOffset;
    }

    @Override
    public SourceSplitBase toSourceSplit() {
        return split.updateWithCurrentLakeSplitIndex(currentLakeSplitIndex)
                .updateWithRecordsToSkip(recordsToSkip)
                .updateWithStartingOffset(nextLogOffset)
                .updateWithLakeSplitFinished(isLakeSplitFinished);
    }
}

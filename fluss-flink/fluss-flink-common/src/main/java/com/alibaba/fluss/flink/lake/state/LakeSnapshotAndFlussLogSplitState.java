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

package com.alibaba.fluss.flink.lake.state;

import com.alibaba.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import com.alibaba.fluss.flink.source.split.SourceSplitBase;
import com.alibaba.fluss.flink.source.split.SourceSplitState;

/** The state of {@link LakeSnapshotAndFlussLogSplit}. */
public class LakeSnapshotAndFlussLogSplitState extends SourceSplitState {

    private long recordsToSkip;
    private final LakeSnapshotAndFlussLogSplit split;

    public LakeSnapshotAndFlussLogSplitState(LakeSnapshotAndFlussLogSplit split) {
        super(split);
        this.recordsToSkip = split.getRecordsToSkip();
        this.split = split;
    }

    public void setRecordsToSkip(long recordsToSkip) {
        this.recordsToSkip = recordsToSkip;
    }

    @Override
    public SourceSplitBase toSourceSplit() {
        return split.updateWithRecordsToSkip(recordsToSkip);
    }
}

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

package org.apache.fluss.flink.lake.state;

import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;

/** The state of {@link LakeSnapshotSplit}. */
public class LakeSnapshotSplitState extends SourceSplitState {

    private final LakeSnapshotSplit split;
    private long recordsToSplit;

    public LakeSnapshotSplitState(LakeSnapshotSplit split) {
        super(split);
        this.split = split;
        this.recordsToSplit = split.getRecordsToSplit();
    }

    public void setRecordsToSkip(long recordsToSkip) {
        this.recordsToSplit = recordsToSkip;
    }

    @Override
    public SourceSplitBase toSourceSplit() {
        return new LakeSnapshotSplit(
                split.getTableBucket(),
                split.getPartitionName(),
                split.getLakeSplit(),
                recordsToSplit);
    }
}

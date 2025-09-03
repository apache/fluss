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

package org.apache.fluss.flink.lake;

import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.lake.state.LakeSnapshotAndFlussLogSplitState;
import org.apache.fluss.flink.lake.state.LakeSnapshotSplitState;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.flink.source.split.SourceSplitState;

/** The state initializer for lake split. */
public class LakeSplitStateInitializer {

    public static SourceSplitState initializedState(SourceSplitBase split) {
        if (split instanceof LakeSnapshotSplit) {
            return new LakeSnapshotSplitState((LakeSnapshotSplit) split);
        } else if (split instanceof LakeSnapshotAndFlussLogSplit) {
            return new LakeSnapshotAndFlussLogSplitState((LakeSnapshotAndFlussLogSplit) split);
        } else {
            throw new UnsupportedOperationException("Unsupported split type: " + split);
        }
    }
}

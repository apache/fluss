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

package com.alibaba.fluss.flink.lakehouse.paimon.split;

import com.alibaba.fluss.flink.source.split.SourceSplitState;

/** State of {@link PaimonSnapshotAndFlussLogSplit}. */
public class PaimonSnapshotAndFlussLogSplitState extends SourceSplitState {

    private final PaimonSnapshotAndFlussLogSplit paimonSnapshotAndFlussLogSplit;

    /** The records to skip while reading a snapshot. */
    private long recordsToSkip;

    public PaimonSnapshotAndFlussLogSplitState(
            PaimonSnapshotAndFlussLogSplit paimonSnapshotAndFlussLogSplit) {
        super(paimonSnapshotAndFlussLogSplit);
        this.paimonSnapshotAndFlussLogSplit = paimonSnapshotAndFlussLogSplit;
        this.recordsToSkip = paimonSnapshotAndFlussLogSplit.getRecordsToSkip();
    }

    public void setRecordsToSkip(long recordsToSkip) {
        this.recordsToSkip = recordsToSkip;
    }

    @Override
    public PaimonSnapshotAndFlussLogSplit toSourceSplit() {
        return paimonSnapshotAndFlussLogSplit.updateWithRecordsToSkip(recordsToSkip);
    }
}

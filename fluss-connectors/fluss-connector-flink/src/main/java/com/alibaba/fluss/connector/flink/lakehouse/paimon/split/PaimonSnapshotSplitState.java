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

package com.alibaba.fluss.connector.flink.lakehouse.paimon.split;

import com.alibaba.fluss.connector.flink.source.split.SourceSplitState;

import org.apache.paimon.flink.source.FileStoreSourceSplit;

/** The state of {@link PaimonSnapshotSplit}. */
public class PaimonSnapshotSplitState extends SourceSplitState {

    private final PaimonSnapshotSplit paimonSnapshotSplit;
    /** The records to skip while reading a snapshot. */
    private long recordsToSkip;

    public PaimonSnapshotSplitState(PaimonSnapshotSplit paimonSnapshotSplit) {
        super(paimonSnapshotSplit);
        this.paimonSnapshotSplit = paimonSnapshotSplit;
        this.recordsToSkip = paimonSnapshotSplit.getFileStoreSourceSplit().recordsToSkip();
    }

    public void setRecordsToSkip(long recordsToSkip) {
        this.recordsToSkip = recordsToSkip;
    }

    @Override
    public PaimonSnapshotSplit toSourceSplit() {
        FileStoreSourceSplit fileStoreSourceSplit =
                paimonSnapshotSplit
                        .getFileStoreSourceSplit()
                        .updateWithRecordsToSkip(recordsToSkip);
        return new PaimonSnapshotSplit(
                paimonSnapshotSplit.getTableBucket(),
                paimonSnapshotSplit.getPartitionName(),
                fileStoreSourceSplit);
    }
}

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

package com.alibaba.fluss.connector.flink.lakehouse;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.connector.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplitState;
import com.alibaba.fluss.connector.flink.lakehouse.paimon.split.PaimonSnapshotSplitState;
import com.alibaba.fluss.connector.flink.source.reader.RecordAndPos;
import com.alibaba.fluss.connector.flink.source.split.SourceSplitState;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.table.data.RowData;

import java.util.function.BiConsumer;

/** The emitter to emit record from lake split. */
public class LakeRecordRecordEmitter {

    private final BiConsumer<ScanRecord, SourceOutput<RowData>> sourceOutputFunc;

    public LakeRecordRecordEmitter(BiConsumer<ScanRecord, SourceOutput<RowData>> sourceOutputFunc) {
        this.sourceOutputFunc = sourceOutputFunc;
    }

    public void emitRecord(
            SourceSplitState splitState,
            SourceOutput<RowData> sourceOutput,
            RecordAndPos recordAndPos) {
        if (splitState instanceof PaimonSnapshotSplitState) {
            ((PaimonSnapshotSplitState) splitState)
                    .setRecordsToSkip(recordAndPos.readRecordsCount());
            sourceOutputFunc.accept(recordAndPos.record(), sourceOutput);
        } else if (splitState instanceof PaimonSnapshotAndFlussLogSplitState) {
            ((PaimonSnapshotAndFlussLogSplitState) splitState)
                    .setRecordsToSkip(recordAndPos.readRecordsCount());
            sourceOutputFunc.accept(recordAndPos.record(), sourceOutput);
        } else {
            throw new UnsupportedOperationException(
                    "Unknown split state type: " + splitState.getClass());
        }
    }
}

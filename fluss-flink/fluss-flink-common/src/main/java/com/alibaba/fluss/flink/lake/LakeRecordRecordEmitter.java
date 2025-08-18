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

package com.alibaba.fluss.flink.lake;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.flink.lake.state.LakeSnapshotAndFlussLogSplitState;
import com.alibaba.fluss.flink.lake.state.LakeSnapshotSplitState;
import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotAndFlussLogSplitState;
import com.alibaba.fluss.flink.lakehouse.paimon.split.PaimonSnapshotSplitState;
import com.alibaba.fluss.flink.source.reader.RecordAndPos;
import com.alibaba.fluss.flink.source.split.SourceSplitState;

import org.apache.flink.api.connector.source.SourceOutput;

import java.util.function.BiConsumer;

/** The emitter to emit record from lake split. */
public class LakeRecordRecordEmitter<OUT> {

    private final BiConsumer<ScanRecord, SourceOutput<OUT>> sourceOutputFunc;

    public LakeRecordRecordEmitter(BiConsumer<ScanRecord, SourceOutput<OUT>> sourceOutputFunc) {
        this.sourceOutputFunc = sourceOutputFunc;
    }

    public void emitRecord(
            SourceSplitState splitState,
            SourceOutput<OUT> sourceOutput,
            RecordAndPos recordAndPos) {
        if (splitState instanceof PaimonSnapshotSplitState) {
            ((PaimonSnapshotSplitState) splitState)
                    .setRecordsToSkip(recordAndPos.readRecordsCount());
            sourceOutputFunc.accept(recordAndPos.record(), sourceOutput);
        } else if (splitState instanceof PaimonSnapshotAndFlussLogSplitState) {
            ((PaimonSnapshotAndFlussLogSplitState) splitState)
                    .setRecordsToSkip(recordAndPos.readRecordsCount());
            sourceOutputFunc.accept(recordAndPos.record(), sourceOutput);
        } else if (splitState instanceof LakeSnapshotSplitState) {
            ((LakeSnapshotSplitState) splitState).setRecordsToSkip(recordAndPos.readRecordsCount());
            sourceOutputFunc.accept(recordAndPos.record(), sourceOutput);
        } else if (splitState instanceof LakeSnapshotAndFlussLogSplitState) {
            ((LakeSnapshotAndFlussLogSplitState) splitState)
                    .setRecordsToSkip(recordAndPos.readRecordsCount());
            sourceOutputFunc.accept(recordAndPos.record(), sourceOutput);
        } else {
            throw new UnsupportedOperationException(
                    "Unknown split state type: " + splitState.getClass());
        }
    }
}

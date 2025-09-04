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

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.lake.state.LakeSnapshotAndFlussLogSplitState;
import org.apache.fluss.flink.lake.state.LakeSnapshotSplitState;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.SourceSplitState;

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
        if (splitState instanceof LakeSnapshotSplitState) {
            ((LakeSnapshotSplitState) splitState).setRecordsToSkip(recordAndPos.readRecordsCount());
            sourceOutputFunc.accept(recordAndPos.record(), sourceOutput);
        } else if (splitState instanceof LakeSnapshotAndFlussLogSplitState) {
            LakeSnapshotAndFlussLogSplitState lakeSnapshotAndFlussLogSplitState =
                    (LakeSnapshotAndFlussLogSplitState) splitState;

            // set current split index
            lakeSnapshotAndFlussLogSplitState.setCurrentLakeSplitIndex(
                    recordAndPos.getCurrentSplitIndex());

            // set records to skip to state
            if (recordAndPos.readRecordsCount() > 0) {
                lakeSnapshotAndFlussLogSplitState.setRecordsToSkip(recordAndPos.readRecordsCount());
            }

            ScanRecord scanRecord = recordAndPos.record();
            // todo: may need a state to mark snapshot phase is finished
            // just like what we do for HybridSnapshotLogSplitState
            if (scanRecord.logOffset() >= 0) {
                // record is with a valid offset, means it's in incremental phase,
                // update the log offset
                lakeSnapshotAndFlussLogSplitState.setNextLogOffset(scanRecord.logOffset() + 1);
            }
            sourceOutputFunc.accept(recordAndPos.record(), sourceOutput);
        } else {
            throw new UnsupportedOperationException(
                    "Unknown split state type: " + splitState.getClass());
        }
    }
}

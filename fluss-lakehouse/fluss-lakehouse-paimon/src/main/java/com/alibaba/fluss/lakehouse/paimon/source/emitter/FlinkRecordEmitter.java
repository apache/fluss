/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.lakehouse.paimon.source.emitter;

import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.lakehouse.paimon.source.reader.FlinkSourceReader;
import com.alibaba.fluss.lakehouse.paimon.source.reader.MultiplexCdcRecordAndPos;
import com.alibaba.fluss.lakehouse.paimon.source.split.HybridSnapshotLogSplitState;
import com.alibaba.fluss.lakehouse.paimon.source.split.SourceSplitState;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link RecordEmitter} implementation for {@link FlinkSourceReader}.
 *
 * <p>During emitting records:
 *
 * <p>when the record is from snapshot data, it'll update the records number to skip which helps to
 * skip the records has been read while restoring in reading snapshot data phase.
 *
 * <p>when the record is from log data, it'll update the offset
 */
public class FlinkRecordEmitter
        implements RecordEmitter<MultiplexCdcRecordAndPos, MultiplexCdcRecord, SourceSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkRecordEmitter.class);

    @Override
    public void emitRecord(
            MultiplexCdcRecordAndPos recordAndPos,
            SourceOutput<MultiplexCdcRecord> sourceOutput,
            SourceSplitState splitState) {
        if (splitState instanceof HybridSnapshotLogSplitState) {
            MultiplexCdcRecord cdcRecord = recordAndPos.record();
            HybridSnapshotLogSplitState hybridSnapshotLogSplitState =
                    (HybridSnapshotLogSplitState) splitState;
            if (cdcRecord.getOffset() >= 0) {
                hybridSnapshotLogSplitState.setNextOffset(cdcRecord.getOffset() + 1);
            } else {
                hybridSnapshotLogSplitState.setRecordsToSkip(recordAndPos.readRecordsCount());
            }
            sourceOutput.collect(cdcRecord);
        } else if (splitState.isLogSplitState()) {
            splitState.asLogSplitState().setNextOffset(recordAndPos.record().getOffset() + 1);
            sourceOutput.collect(recordAndPos.record());
        } else {
            LOG.warn("Unknown split state type: {}", splitState.getClass());
        }
    }
}

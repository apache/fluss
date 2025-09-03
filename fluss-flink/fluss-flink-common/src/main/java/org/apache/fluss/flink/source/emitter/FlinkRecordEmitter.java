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

package org.apache.fluss.flink.source.emitter;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.lake.LakeRecordRecordEmitter;
import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.flink.source.reader.FlinkSourceReader;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitState;

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
public class FlinkRecordEmitter<OUT> implements RecordEmitter<RecordAndPos, OUT, SourceSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkRecordEmitter.class);

    private final FlussDeserializationSchema<OUT> deserializationSchema;
    private LakeRecordRecordEmitter<OUT> lakeRecordRecordEmitter;

    public FlinkRecordEmitter(FlussDeserializationSchema<OUT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            RecordAndPos recordAndPosition,
            SourceOutput<OUT> sourceOutput,
            SourceSplitState splitState) {
        if (splitState.isHybridSnapshotLogSplitState()) {
            // if it's hybrid split, we need to update the records number to skip(if in snapshot
            // phase) or log offset(in incremental phase)
            HybridSnapshotLogSplitState hybridSnapshotLogSplitState =
                    splitState.asHybridSnapshotLogSplitState();

            ScanRecord scanRecord = recordAndPosition.record();
            if (scanRecord.logOffset() >= 0) {
                // record is with a valid offset, means it's in incremental phase,
                // update the log offset
                hybridSnapshotLogSplitState.setNextOffset(scanRecord.logOffset() + 1);
            } else {
                // record is with an invalid offset, means it's in snapshot phase,
                // update the records number to skip
                hybridSnapshotLogSplitState.setRecordsToSkip(recordAndPosition.readRecordsCount());
            }
            processAndEmitRecord(scanRecord, sourceOutput);
        } else if (splitState.isLogSplitState()) {
            splitState.asLogSplitState().setNextOffset(recordAndPosition.record().logOffset() + 1);
            processAndEmitRecord(recordAndPosition.record(), sourceOutput);
        } else if (splitState.isLakeSplit()) {
            if (lakeRecordRecordEmitter == null) {
                lakeRecordRecordEmitter = new LakeRecordRecordEmitter<>(this::processAndEmitRecord);
            }
            lakeRecordRecordEmitter.emitRecord(splitState, sourceOutput, recordAndPosition);
        } else {
            LOG.warn("Unknown split state type: {}", splitState.getClass());
        }
    }

    private void processAndEmitRecord(ScanRecord scanRecord, SourceOutput<OUT> sourceOutput) {
        OUT record;
        try {
            record = deserializationSchema.deserialize(scanRecord);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to deserialize record: " + scanRecord + ". Cause: " + e.getMessage(),
                    e);
        }

        if (record != null) {
            long timestamp = scanRecord.timestamp();
            if (timestamp > 0) {
                sourceOutput.collect(record, timestamp);
            } else {
                sourceOutput.collect(record);
            }
        }
    }
}

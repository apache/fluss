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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.KvRecordBatchBuilder;
import org.apache.fluss.record.MutationType;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.MergeMode;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A batch of kv records that is or will be sent to server by {@link PutKvRequest}.
 *
 * <p>This class is not thread safe and external synchronization must be used when modifying it.
 */
@NotThreadSafe
@Internal
public class KvWriteBatch extends WriteBatch {
    private final AbstractPagedOutputView outputView;
    private final KvRecordBatchBuilder recordsBuilder;
    private final @Nullable int[] targetColumns;
    private final int schemaId;
    private final MergeMode mergeMode;
    private final boolean v2Format;

    public KvWriteBatch(
            int bucketId,
            PhysicalTablePath physicalTablePath,
            int schemaId,
            KvFormat kvFormat,
            int writeLimit,
            AbstractPagedOutputView outputView,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            boolean v2Format,
            long createdMs) {
        super(bucketId, physicalTablePath, createdMs);
        this.outputView = outputView;
        this.recordsBuilder =
                KvRecordBatchBuilder.builder(schemaId, writeLimit, outputView, kvFormat, v2Format);
        this.targetColumns = targetColumns;
        this.schemaId = schemaId;
        this.mergeMode = mergeMode;
        this.v2Format = v2Format;
    }

    @Override
    public boolean isLogBatch() {
        return false;
    }

    @Override
    public AppendResult tryAppend(WriteRecord writeRecord, WriteCallback callback)
            throws Exception {
        validateRecordConsistency(writeRecord);

        // V0 batch cannot carry RETRACT records — caller must create a new V2 batch.
        if (!v2Format && writeRecord.getMutationType() == MutationType.RETRACT) {
            return AppendResult.FORMAT_MISMATCH;
        }

        byte[] key = writeRecord.getKey();
        checkNotNull(key, "key must be not null for kv record");
        checkNotNull(callback, "write callback must be not null");
        BinaryRow row = checkRow(writeRecord.getRow());

        if (v2Format) {
            if (!recordsBuilder.hasRoomForV2(key, row) || isClosed()) {
                return AppendResult.BATCH_FULL;
            } else {
                recordsBuilder.appendV2(writeRecord.getMutationType(), key, row);
                callbacks.add(callback);
                recordCount++;
                return AppendResult.APPENDED;
            }
        } else {
            if (!recordsBuilder.hasRoomFor(key, row) || isClosed()) {
                return AppendResult.BATCH_FULL;
            } else {
                recordsBuilder.append(key, row);
                callbacks.add(callback);
                recordCount++;
                return AppendResult.APPENDED;
            }
        }
    }

    private void validateRecordConsistency(WriteRecord writeRecord) {
        if (schemaId != writeRecord.getSchemaId()) {
            throw new IllegalStateException(
                    String.format(
                            "schema id %d of the write record to append is not the same as the current schema id %d in the batch.",
                            writeRecord.getSchemaId(), schemaId));
        }
        if (!Arrays.equals(targetColumns, writeRecord.getTargetColumns())) {
            throw new IllegalStateException(
                    String.format(
                            "target columns %s of the write record to append are not the same as the current target columns %s in the batch.",
                            Arrays.toString(writeRecord.getTargetColumns()),
                            Arrays.toString(targetColumns)));
        }
        if (writeRecord.getMergeMode() != this.mergeMode) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot mix records with different mergeMode in the same batch. "
                                    + "Batch mergeMode: %s, Record mergeMode: %s",
                            this.mergeMode, writeRecord.getMergeMode()));
        }
    }

    @Nullable
    public int[] getTargetColumns() {
        return targetColumns;
    }

    public MergeMode getMergeMode() {
        return mergeMode;
    }

    @Override
    public BytesView build() {
        try {
            return recordsBuilder.build();
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to build kv record batch.", e);
        }
    }

    @Override
    public void close() throws Exception {
        recordsBuilder.close();
        reopened = false;
    }

    @Override
    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    @Override
    public int estimatedSizeInBytes() {
        return recordsBuilder.getSizeInBytes();
    }

    @Override
    public List<MemorySegment> pooledMemorySegments() {
        return outputView.allocatedPooledSegments();
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public long writerId() {
        return recordsBuilder.writerId();
    }

    @Override
    public int batchSequence() {
        return recordsBuilder.batchSequence();
    }

    @Override
    public void abortRecordAppends() {
        recordsBuilder.abort();
    }

    @Override
    public void resetWriterState(long writerId, int batchSequence) {
        super.resetWriterState(writerId, batchSequence);
        recordsBuilder.resetWriterState(writerId, batchSequence);
    }

    private static BinaryRow checkRow(@Nullable InternalRow row) {
        if (row != null) {
            checkArgument(row instanceof BinaryRow, "row must be BinaryRow for kv record");
            return (BinaryRow) row;
        } else {
            return null;
        }
    }
}

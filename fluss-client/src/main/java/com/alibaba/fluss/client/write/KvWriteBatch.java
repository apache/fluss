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

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.memory.AbstractPagedOutputView;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.KvRecordBatchBuilder;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.rpc.messages.PutKvRequest;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

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

    public KvWriteBatch(
            TableBucket tableBucket,
            PhysicalTablePath physicalTablePath,
            int schemaId,
            KvFormat kvFormat,
            int writeLimit,
            AbstractPagedOutputView outputView,
            @Nullable int[] targetColumns,
            long createdMs) {
        super(tableBucket, physicalTablePath, createdMs);
        this.outputView = outputView;
        this.recordsBuilder =
                KvRecordBatchBuilder.builder(schemaId, writeLimit, outputView, kvFormat);
        this.targetColumns = targetColumns;
    }

    @Override
    public boolean tryAppend(WriteRecord writeRecord, WriteCallback callback) throws Exception {
        // currently, we throw exception directly when the target columns of the write record is
        // not the same as the current target columns in the batch.
        // this should be quite fast as they should be the same objects.
        if (!Arrays.equals(targetColumns, writeRecord.getTargetColumns())) {
            throw new IllegalStateException(
                    String.format(
                            "target columns %s of the write record to append are not the same as the current target columns %s in the batch.",
                            Arrays.toString(writeRecord.getTargetColumns()),
                            Arrays.toString(targetColumns)));
        }

        byte[] key = writeRecord.getKey();
        checkNotNull(key, "key must be not null for kv record");
        checkNotNull(callback, "write callback must be not null");
        BinaryRow row = checkRow(writeRecord.getRow());
        if (!recordsBuilder.hasRoomFor(key, row) || isClosed()) {
            return false;
        } else {
            recordsBuilder.append(key, row);
            callbacks.add(callback);
            recordCount++;
            return true;
        }
    }

    @Nullable
    public int[] getTargetColumns() {
        return targetColumns;
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

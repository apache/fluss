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
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.MemoryLogRecordsIndexedBuilder;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.utils.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.List;

/**
 * A batch of log records managed in INDEXED format that is or will be sent to server by {@link
 * ProduceLogRequest}.
 *
 * <p>This class is not thread safe and external synchronization must be used when modifying it.
 */
@NotThreadSafe
@Internal
public final class IndexedLogWriteBatch extends WriteBatch {
    private final AbstractPagedOutputView outputView;
    private final MemoryLogRecordsIndexedBuilder recordsBuilder;

    public IndexedLogWriteBatch(
            TableBucket tableBucket,
            PhysicalTablePath physicalTablePath,
            int schemaId,
            int writeLimit,
            AbstractPagedOutputView outputView) {
        super(tableBucket, physicalTablePath);
        this.outputView = outputView;
        this.recordsBuilder =
                MemoryLogRecordsIndexedBuilder.builder(schemaId, writeLimit, outputView);
    }

    @Override
    public boolean tryAppend(WriteRecord writeRecord, WriteCallback callback) throws Exception {
        InternalRow row = writeRecord.getRow();
        Preconditions.checkArgument(
                writeRecord.getTargetColumns() == null,
                "target columns must be null for log record");
        Preconditions.checkArgument(
                writeRecord.getKey() == null, "key must be null for log record");
        Preconditions.checkNotNull(row != null, "row must not be null for log record");
        Preconditions.checkNotNull(callback, "write callback must be not null");
        if (!recordsBuilder.hasRoomFor(row) || isClosed()) {
            return false;
        } else {
            recordsBuilder.append(RowKind.APPEND_ONLY, row);
            recordCount++;
            callbacks.add(callback);
            return true;
        }
    }

    @Override
    public BytesView build() {
        try {
            return recordsBuilder.build();
        } catch (IOException e) {
            throw new FlussRuntimeException("Failed to build indexed log record batch.", e);
        }
    }

    @Override
    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    @Override
    public void close() throws Exception {
        recordsBuilder.close();
        reopened = false;
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

    @Override
    public int estimatedSizeInBytes() {
        return recordsBuilder.getSizeInBytes();
    }
}

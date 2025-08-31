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
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecordsIndexedBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.rpc.messages.ProduceLogRequest;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

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
            int bucketId,
            PhysicalTablePath physicalTablePath,
            int schemaId,
            int writeLimit,
            AbstractPagedOutputView outputView,
            long createdMs) {
        super(bucketId, physicalTablePath, createdMs);
        this.outputView = outputView;
        this.recordsBuilder =
                MemoryLogRecordsIndexedBuilder.builder(schemaId, writeLimit, outputView, true);
    }

    @Override
    public boolean tryAppend(WriteRecord writeRecord, WriteCallback callback) throws Exception {
        checkNotNull(callback, "write callback must be not null");
        checkNotNull(writeRecord.getRow(), "row must not be null for log record");
        checkArgument(writeRecord.getKey() == null, "key must be null for log record");
        checkArgument(
                writeRecord.getTargetColumns() == null,
                "target columns must be null for log record");
        checkArgument(
                writeRecord.getRow() instanceof IndexedRow,
                "row must not be IndexRow for indexed log table");
        IndexedRow row = (IndexedRow) writeRecord.getRow();
        if (!recordsBuilder.hasRoomFor(row) || isClosed()) {
            return false;
        } else {
            recordsBuilder.append(ChangeType.APPEND_ONLY, row);
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

    @Override
    public void abortRecordAppends() {
        recordsBuilder.abort();
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

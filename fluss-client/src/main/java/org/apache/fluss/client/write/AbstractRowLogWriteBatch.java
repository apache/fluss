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

import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.InternalRow;

import java.io.IOException;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Abstract base class to deduplicate logic for row-based log write batches backed by in-memory
 * builders. Concrete subclasses only need to provide a row-type validator/caster and a
 * RecordsBuilderAdapter implementation.
 */
abstract class AbstractRowLogWriteBatch<R> extends WriteBatch {

    interface RecordsBuilderAdapter<T> {
        boolean hasRoomFor(T row);

        void append(ChangeType changeType, T row) throws Exception;

        BytesView build() throws IOException;

        boolean isClosed();

        void close() throws Exception;

        void setWriterState(long writerId, int batchSequence);

        long writerId();

        int batchSequence();

        void abort();

        void resetWriterState(long writerId, int batchSequence);

        int getSizeInBytes();
    }

    private final AbstractPagedOutputView outputView;
    private final RecordsBuilderAdapter<R> builder;
    private final String buildErrorMessage;

    protected AbstractRowLogWriteBatch(
            int bucketId,
            PhysicalTablePath physicalTablePath,
            long createdMs,
            AbstractPagedOutputView outputView,
            RecordsBuilderAdapter<R> builder,
            String buildErrorMessage) {
        super(bucketId, physicalTablePath, createdMs);
        this.outputView = outputView;
        this.builder = builder;
        this.buildErrorMessage = buildErrorMessage;
    }

    @Override
    public boolean tryAppend(WriteRecord writeRecord, WriteCallback callback) throws Exception {
        checkNotNull(callback, "write callback must be not null");
        InternalRow rowObj = writeRecord.getRow();
        checkNotNull(rowObj, "row must not be null for log record");
        checkArgument(writeRecord.getKey() == null, "key must be null for log record");
        checkArgument(
                writeRecord.getTargetColumns() == null,
                "target columns must be null for log record");

        R row = requireAndCastRow(rowObj);
        if (!builder.hasRoomFor(row) || isClosed()) {
            return false;
        }
        builder.append(ChangeType.APPEND_ONLY, row);
        recordCount++;
        callbacks.add(callback);
        return true;
    }

    protected abstract R requireAndCastRow(InternalRow row);

    @Override
    public BytesView build() {
        try {
            return builder.build();
        } catch (IOException e) {
            throw new FlussRuntimeException(buildErrorMessage, e);
        }
    }

    @Override
    public boolean isClosed() {
        return builder.isClosed();
    }

    @Override
    public void close() throws Exception {
        builder.close();
        reopened = false;
    }

    @Override
    public List<MemorySegment> pooledMemorySegments() {
        return outputView.allocatedPooledSegments();
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        builder.setWriterState(writerId, batchSequence);
    }

    @Override
    public long writerId() {
        return builder.writerId();
    }

    @Override
    public int batchSequence() {
        return builder.batchSequence();
    }

    @Override
    public void abortRecordAppends() {
        builder.abort();
    }

    @Override
    public void resetWriterState(long writerId, int batchSequence) {
        super.resetWriterState(writerId, batchSequence);
        builder.resetWriterState(writerId, batchSequence);
    }

    @Override
    public int estimatedSizeInBytes() {
        return builder.getSizeInBytes();
    }
}

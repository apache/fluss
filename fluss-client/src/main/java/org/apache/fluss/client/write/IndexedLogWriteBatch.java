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

/**
 * A batch of log records managed in INDEXED format that is or will be sent to server by {@link
 * ProduceLogRequest}.
 *
 * <p>This class is not thread safe and external synchronization must be used when modifying it.
 */
@NotThreadSafe
@Internal
public final class IndexedLogWriteBatch extends AbstractRowLogWriteBatch<IndexedRow> {
    private final AbstractPagedOutputView outputView;

    public IndexedLogWriteBatch(
            int bucketId,
            PhysicalTablePath physicalTablePath,
            int schemaId,
            int writeLimit,
            AbstractPagedOutputView outputView,
            long createdMs) {
        super(
                bucketId,
                physicalTablePath,
                createdMs,
                outputView,
                new RecordsBuilderAdapter<IndexedRow>() {
                    private final MemoryLogRecordsIndexedBuilder delegate =
                            MemoryLogRecordsIndexedBuilder.builder(
                                    schemaId, writeLimit, outputView, true);

                    @Override
                    public boolean hasRoomFor(IndexedRow row) {
                        return delegate.hasRoomFor(row);
                    }

                    @Override
                    public void append(ChangeType changeType, IndexedRow row) throws Exception {
                        delegate.append(changeType, row);
                    }

                    @Override
                    public BytesView build() throws IOException {
                        return delegate.build();
                    }

                    @Override
                    public boolean isClosed() {
                        return delegate.isClosed();
                    }

                    @Override
                    public void close() throws Exception {
                        delegate.close();
                    }

                    @Override
                    public void setWriterState(long writerId, int batchSequence) {
                        delegate.setWriterState(writerId, batchSequence);
                    }

                    @Override
                    public long writerId() {
                        return delegate.writerId();
                    }

                    @Override
                    public int batchSequence() {
                        return delegate.batchSequence();
                    }

                    @Override
                    public void abort() {
                        delegate.abort();
                    }

                    @Override
                    public void resetWriterState(long writerId, int batchSequence) {
                        delegate.resetWriterState(writerId, batchSequence);
                    }

                    @Override
                    public int getSizeInBytes() {
                        return delegate.getSizeInBytes();
                    }
                },
                "Failed to build indexed log record batch.");
        this.outputView = outputView;
    }

    @Override
    protected IndexedRow requireAndCastRow(org.apache.fluss.row.InternalRow row) {
        checkArgument(row instanceof IndexedRow, "row must be IndexRow for indexed log table");
        return (IndexedRow) row;
    }

    @Override
    public List<MemorySegment> pooledMemorySegments() {
        return outputView.allocatedPooledSegments();
    }
}

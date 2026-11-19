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

package org.apache.fluss.server.kv.wal;

import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.record.MemoryLogRecordsArrowBuilder;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowWriter;

import java.io.IOException;

/** A {@link WalBuilder} implementation that builds WAL logs in Arrow format. */
public class ArrowWalBuilder implements WalBuilder {

    private final MemorySegmentPool memorySegmentPool;
    private final ManagedPagedOutputView outputView;
    private final MemoryLogRecordsArrowBuilder recordsBuilder;
    private final boolean gatherWrites;

    /** Creates a WAL builder with optional append-only encoding (without a change-type vector). */
    public ArrowWalBuilder(
            int schemaId,
            ArrowWriter writer,
            MemorySegmentPool memorySegmentPool,
            boolean appendOnly)
            throws IOException {
        this(schemaId, writer, memorySegmentPool, appendOnly, false);
    }

    /** Creates a WAL builder that can write the output pages directly during synchronous append. */
    public ArrowWalBuilder(
            int schemaId,
            ArrowWriter writer,
            MemorySegmentPool memorySegmentPool,
            boolean appendOnly,
            boolean gatherWrites)
            throws IOException {
        this.memorySegmentPool = memorySegmentPool;
        this.gatherWrites = gatherWrites;
        this.outputView = new ManagedPagedOutputView(memorySegmentPool);
        this.recordsBuilder =
                MemoryLogRecordsArrowBuilder.builder(
                        schemaId, writer, outputView, appendOnly, null);
    }

    @Override
    public void append(ChangeType changeType, InternalRow row) throws Exception {
        recordsBuilder.append(changeType, row);
    }

    /** Builds the WAL batch using pages owned by this builder until {@link #deallocate()}. */
    @Override
    public MemoryLogRecords build() throws Exception {
        boolean alreadyBuilt = recordsBuilder.isClosed();
        recordsBuilder.close();
        if (!gatherWrites) {
            return MemoryLogRecords.pointToByteBuffer(
                    recordsBuilder.build().getByteBuf().nioBuffer());
        }
        if (alreadyBuilt) {
            // A later build can refresh the writer state on the same pages.
            recordsBuilder.build();
        }
        return MemoryLogRecords.pointToSingleBatch(outputView.getWrittenSegments());
    }

    @Override
    public void setWriterState(long writerId, int batchSequence) {
        recordsBuilder.setWriterState(writerId, batchSequence);
    }

    @Override
    public void deallocate() {
        recordsBuilder.recycleArrowWriter();
        memorySegmentPool.returnAll(outputView.allocatedPooledSegments());
    }
}

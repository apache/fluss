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

package com.alibaba.fluss.server.kv.wal;

import com.alibaba.fluss.memory.ManagedPagedOutputView;
import com.alibaba.fluss.memory.MemorySegmentPool;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.MemoryLogRecordsArrowBuilder;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.arrow.ArrowWriter;

import java.io.IOException;

/** A {@link WalBuilder} implementation that builds WAL logs in Arrow format. */
public class ArrowWalBuilder implements WalBuilder {

    private final MemorySegmentPool memorySegmentPool;
    private final ManagedPagedOutputView outputView;
    private final MemoryLogRecordsArrowBuilder recordsBuilder;

    public ArrowWalBuilder(int schemaId, ArrowWriter writer, MemorySegmentPool memorySegmentPool)
            throws IOException {
        this.memorySegmentPool = memorySegmentPool;
        this.outputView = new ManagedPagedOutputView(memorySegmentPool);
        this.recordsBuilder =
                MemoryLogRecordsArrowBuilder.builder(schemaId, writer, outputView, false);
    }

    @Override
    public void append(ChangeType changeType, InternalRow row) throws Exception {
        recordsBuilder.append(changeType, row);
    }

    @Override
    public MemoryLogRecords build() throws Exception {
        recordsBuilder.close();
        MultiBytesView bytesView = recordsBuilder.build();
        // netty nioBuffer() will deep copy bytes only when the underlying ByteBuf is composite
        // TODO: this is a heavy operation, avoid copy bytes,
        //  MemoryLogRecords supports cross segments
        return MemoryLogRecords.pointToByteBuffer(bytesView.getByteBuf().nioBuffer());
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

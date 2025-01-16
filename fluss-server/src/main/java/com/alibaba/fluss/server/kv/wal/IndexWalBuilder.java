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

package com.alibaba.fluss.server.kv.wal;

import com.alibaba.fluss.memory.ManagedPagedOutputView;
import com.alibaba.fluss.memory.MemorySegmentPool;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.MemoryLogRecordsIndexedBuilder;
import com.alibaba.fluss.record.RowKind;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.row.InternalRow;

import java.io.IOException;

/** A {@link WalBuilder} that builds a {@link MemoryLogRecords} with Indexed log format. */
public class IndexWalBuilder implements WalBuilder {

    private final MemoryLogRecordsIndexedBuilder recordsBuilder;
    private final MemorySegmentPool memorySegmentPool;
    private final ManagedPagedOutputView outputView;

    public IndexWalBuilder(int schemaId, MemorySegmentPool memorySegmentPool) throws IOException {
        this.memorySegmentPool = memorySegmentPool;
        this.outputView = new ManagedPagedOutputView(memorySegmentPool);
        // unlimited write size as we don't know the WAL size in advance
        this.recordsBuilder =
                MemoryLogRecordsIndexedBuilder.builder(schemaId, Integer.MAX_VALUE, outputView);
    }

    @Override
    public void append(RowKind rowKind, InternalRow row) throws Exception {
        recordsBuilder.append(rowKind, row);
    }

    @Override
    public MemoryLogRecords build() throws Exception {
        recordsBuilder.close();
        BytesView bytesView = recordsBuilder.build();
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
        memorySegmentPool.returnAll(outputView.allocatedPooledSegments());
    }
}

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
import org.apache.fluss.record.MemoryLogRecordsCompactedBuilder;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.io.IOException;

/** A {@link WalBuilder} that builds a {@link MemoryLogRecords} with Compacted log format. */
public class CompactedWalBuilder implements WalBuilder {

    private final MemorySegmentPool memorySegmentPool;
    private final ManagedPagedOutputView outputView;
    private final MemoryLogRecordsCompactedBuilder recordsBuilder;

    private final CompactedRowEncoder rowEncoder;
    private final FieldGetter[] fieldGetters;
    private final int fieldCount;

    public CompactedWalBuilder(int schemaId, RowType rowType, MemorySegmentPool memorySegmentPool)
            throws IOException {
        this.memorySegmentPool = memorySegmentPool;
        this.outputView = new ManagedPagedOutputView(memorySegmentPool);
        // unlimited write size as we don't know the WAL size in advance
        this.recordsBuilder =
                MemoryLogRecordsCompactedBuilder.builder(
                        schemaId, Integer.MAX_VALUE, outputView, /*appendOnly*/ false);
        DataType[] fieldTypes = rowType.getChildren().toArray(new DataType[0]);
        this.rowEncoder = new CompactedRowEncoder(fieldTypes);
        this.fieldGetters = InternalRow.createFieldGetters(rowType);
        this.fieldCount = rowType.getFieldCount();
    }

    @Override
    public void append(ChangeType changeType, InternalRow row) throws Exception {
        final CompactedRow compactedRow;
        if (row instanceof CompactedRow) {
            compactedRow = (CompactedRow) row;
        } else {
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldCount; i++) {
                rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
            }
            compactedRow = rowEncoder.finishRow();
        }
        recordsBuilder.append(changeType, compactedRow);
    }

    @Override
    public MemoryLogRecords build() throws Exception {
        recordsBuilder.close();
        BytesView bytesView = recordsBuilder.build();
        // Convert BytesView to MemoryLogRecords (may copy if composite)
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

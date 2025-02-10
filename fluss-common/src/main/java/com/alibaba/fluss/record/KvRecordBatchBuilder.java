/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.AbstractPagedOutputView;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.KvFormat;
import com.alibaba.fluss.record.bytesview.BytesView;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.row.BinaryRow;
import com.alibaba.fluss.row.compacted.CompactedRow;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.utils.Preconditions;
import com.alibaba.fluss.utils.crc.Crc32C;

import javax.annotation.Nullable;

import java.io.IOException;

import static com.alibaba.fluss.record.DefaultKvRecordBatch.CRC_OFFSET;
import static com.alibaba.fluss.record.DefaultKvRecordBatch.LENGTH_LENGTH;
import static com.alibaba.fluss.record.DefaultKvRecordBatch.RECORD_BATCH_HEADER_SIZE;
import static com.alibaba.fluss.record.DefaultKvRecordBatch.SCHEMA_ID_OFFSET;
import static com.alibaba.fluss.record.KvRecordBatch.CURRENT_KV_MAGIC_VALUE;

/** Builder for {@link DefaultKvRecordBatch} memory bytes. */
public class KvRecordBatchBuilder implements AutoCloseable {

    private final int schemaId;
    private final byte magic;
    // The max bytes can be appended.
    private final int writeLimit;
    private final AbstractPagedOutputView pagedOutputView;
    private final MemorySegment firstSegment;

    private BytesView builtBuffer = null;
    private long writerId;
    private int batchSequence;
    private int currentRecordNumber;
    private int sizeInBytes;
    private boolean isClosed;
    private final KvFormat kvFormat;

    private KvRecordBatchBuilder(
            int schemaId,
            byte magic,
            int writeLimit,
            AbstractPagedOutputView pagedOutputView,
            KvFormat kvFormat) {
        Preconditions.checkArgument(
                schemaId <= Short.MAX_VALUE,
                "schemaId shouldn't be greater than the max value of short: " + Short.MAX_VALUE);
        this.schemaId = schemaId;
        this.magic = magic;
        this.writeLimit = writeLimit;
        this.pagedOutputView = pagedOutputView;
        this.firstSegment = pagedOutputView.getCurrentSegment();
        this.writerId = LogRecordBatch.NO_WRITER_ID;
        this.batchSequence = LogRecordBatch.NO_BATCH_SEQUENCE;
        this.currentRecordNumber = 0;
        this.isClosed = false;
        // We don't need to write header information while the builder creating,
        // we'll skip it first.
        pagedOutputView.setPosition(RECORD_BATCH_HEADER_SIZE);
        this.sizeInBytes = RECORD_BATCH_HEADER_SIZE;
        this.kvFormat = kvFormat;
    }

    public static KvRecordBatchBuilder builder(
            int schemaId, int writeLimit, AbstractPagedOutputView outputView, KvFormat kvFormat) {
        return new KvRecordBatchBuilder(
                schemaId, CURRENT_KV_MAGIC_VALUE, writeLimit, outputView, kvFormat);
    }

    /**
     * Check if we have room for a new record containing the given row. If no records have been
     * appended, then this returns true.
     */
    public boolean hasRoomFor(byte[] key, @Nullable BinaryRow row) {
        return sizeInBytes + DefaultKvRecord.sizeOf(key, row) <= writeLimit;
    }

    /**
     * Wrap a KvRecord with the given key, value and append the KvRecord to DefaultKvRecordBatch.
     *
     * @param key the key in the KvRecord to be appended
     * @param row the value in the KvRecord to be appended. If the value is null, it means the
     *     KvRecord is for delete the corresponding key.
     */
    public void append(byte[] key, @Nullable BinaryRow row) throws IOException {
        if (isClosed) {
            throw new IllegalStateException(
                    "Tried to put a record, but KvRecordBatchBuilder is closed for record puts.");
        }
        int recordByteSizes = DefaultKvRecord.writeTo(pagedOutputView, key, validateRowFormat(row));
        currentRecordNumber++;
        if (currentRecordNumber == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Maximum number of records per batch exceeded, max records: "
                            + Integer.MAX_VALUE);
        }
        sizeInBytes += recordByteSizes;
    }

    public void setWriterState(long writerId, int batchBaseSequence) {
        this.writerId = writerId;
        this.batchSequence = batchBaseSequence;
    }

    public void resetWriterState(long writerId, int batchSequence) {
        // trigger to rewrite batch header
        this.builtBuffer = null;
        this.writerId = writerId;
        this.batchSequence = batchSequence;
    }

    public BytesView build() throws IOException {
        if (builtBuffer != null) {
            return builtBuffer;
        }

        writeBatchHeader();
        builtBuffer =
                MultiBytesView.builder()
                        .addMemorySegmentByteViewList(pagedOutputView.getWrittenSegments())
                        .build();
        return builtBuffer;
    }

    public long writerId() {
        return writerId;
    }

    public int batchSequence() {
        return batchSequence;
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    // ----------------------- internal methods -------------------------------
    private void writeBatchHeader() throws IOException {
        // pagedOutputView doesn't support seek to previous segment,
        // so we create a new output view on the first segment
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(firstSegment);
        outputView.setPosition(0);
        // update header.
        outputView.writeInt(sizeInBytes - LENGTH_LENGTH);
        outputView.writeByte(magic);
        // write empty crc first.
        outputView.writeUnsignedInt(0);
        outputView.writeShort((short) schemaId);
        outputView.writeByte(computeAttributes());
        outputView.writeLong(writerId);
        outputView.writeInt(batchSequence);
        outputView.writeInt(currentRecordNumber);
        // Update crc.
        long crc = Crc32C.compute(pagedOutputView.getWrittenSegments(), SCHEMA_ID_OFFSET);
        outputView.setPosition(CRC_OFFSET);
        outputView.writeUnsignedInt(crc);
    }

    private byte computeAttributes() {
        return 0;
    }

    /** Validate the row instance according to the kv format. */
    private BinaryRow validateRowFormat(BinaryRow row) {
        if (row == null) {
            return null;
        }
        if (kvFormat == KvFormat.COMPACTED) {
            if (row instanceof CompactedRow) {
                return row;
            } else {
                // currently, we don't support to do row conversion for simplicity,
                // just throw exception
                throw new IllegalArgumentException(
                        "The row to be appended to kv record batch with compacted format "
                                + "should be a compacted row, but got a "
                                + row.getClass().getSimpleName());
            }
        } else if (kvFormat == KvFormat.INDEXED) {
            if (row instanceof IndexedRow) {
                return row;
            } else {
                throw new IllegalArgumentException(
                        "The row to be appended to kv record batch "
                                + "with indexed format should be a indexed row, but got "
                                + row.getClass().getSimpleName());
            }
        } else {
            throw new UnsupportedOperationException("Unsupported kv format: " + kvFormat);
        }
    }
}

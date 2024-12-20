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

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.memory.AbstractPagedOutputView;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.memory.MemorySegmentOutputView;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.record.bytesview.MemorySegmentBytesView;
import com.alibaba.fluss.record.bytesview.MultiBytesView;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.arrow.ArrowWriter;
import com.alibaba.fluss.utils.crc.Crc32C;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static com.alibaba.fluss.record.LogRecordBatch.NO_BATCH_SEQUENCE;
import static com.alibaba.fluss.record.LogRecordBatch.NO_WRITER_ID;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.ARROW_ROWKIND_OFFSET;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.BASE_OFFSET_LENGTH;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.CRC_OFFSET;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.LENGTH_LENGTH;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.SCHEMA_ID_OFFSET;
import static com.alibaba.fluss.record.MemoryLogRecordBatch.WRITE_CLIENT_ID_OFFSET;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Builder for {@link MemoryLogRecords} of log records in {@link LogFormat#ARROW} format. */
public class MemoryLogRecordsArrowBuilder implements AutoCloseable {
    private static final int BUILDER_DEFAULT_OFFSET = 0;

    private final long baseLogOffset;
    private final int schemaId;
    private final byte magic;
    private final ArrowWriter arrowWriter;
    private final long writerEpoch;
    private final RowKindVectorWriter rowKindWriter;
    private final MemorySegment firstSegment;
    private final AbstractPagedOutputView pagedOutputView;

    private final AtomicBoolean serializationLock = new AtomicBoolean(false);
    private volatile boolean serialized = false;

    private MultiBytesView bytesView = null;
    private long writerId;
    private int batchSequence;
    private int sizeInBytes;
    private int recordCount;
    private boolean isClosed;
    private boolean reCalculateSizeInBytes = false;

    private MemoryLogRecordsArrowBuilder(
            long baseLogOffset,
            int schemaId,
            byte magic,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView pagedOutputView) {
        checkArgument(
                schemaId <= Short.MAX_VALUE,
                "schemaId shouldn't be greater than the max value of short: " + Short.MAX_VALUE);
        this.baseLogOffset = baseLogOffset;
        this.schemaId = schemaId;
        this.magic = magic;
        this.arrowWriter = checkNotNull(arrowWriter);
        this.writerEpoch = arrowWriter.getEpoch();

        this.writerId = NO_WRITER_ID;
        this.batchSequence = NO_BATCH_SEQUENCE;
        this.isClosed = false;

        this.pagedOutputView = pagedOutputView;
        this.firstSegment = pagedOutputView.getCurrentSegment();
        checkArgument(
                firstSegment.size() >= ARROW_ROWKIND_OFFSET,
                "The size of first segment of pagedOutputView is too small, need at least "
                        + ARROW_ROWKIND_OFFSET
                        + " bytes.");
        this.rowKindWriter = new RowKindVectorWriter(firstSegment, ARROW_ROWKIND_OFFSET);
        this.sizeInBytes = ARROW_ROWKIND_OFFSET;
        this.recordCount = 0;
    }

    public static MemoryLogRecordsArrowBuilder builder(
            long baseLogOffset,
            int schemaId,
            ArrowWriter arrowWriter,
            AbstractPagedOutputView outputView) {
        return new MemoryLogRecordsArrowBuilder(
                baseLogOffset, schemaId, CURRENT_LOG_MAGIC_VALUE, arrowWriter, outputView);
    }

    /** Builder with limited write size and the memory segment used to serialize records. */
    public static MemoryLogRecordsArrowBuilder builder(
            int schemaId, ArrowWriter arrowWriter, AbstractPagedOutputView outputView) {
        return new MemoryLogRecordsArrowBuilder(
                BUILDER_DEFAULT_OFFSET, schemaId, CURRENT_LOG_MAGIC_VALUE, arrowWriter, outputView);
    }

    public void serialize() throws IOException {
        // use CAS to make sure there is only one thread to serialize the arrow batch
        // and only serialize once
        if (serializationLock.compareAndSet(false, true)) {
            if (!isClosed) {
                throw new IllegalStateException(
                        "Tried to build Arrow batch into memory before it is closed.");
            }
            // serialize the arrow batch to dynamically allocated memory segments
            arrowWriter.serializeToOutputView(
                    pagedOutputView,
                    firstSegment,
                    ARROW_ROWKIND_OFFSET + rowKindWriter.sizeInBytes(),
                    true);
            arrowWriter.recycle(writerEpoch);
            serialized = true;
        }
    }

    public boolean trySerialize() {
        // use CAS to make sure there is only one thread to serialize the arrow batch
        // and only serialize once
        if (serializationLock.compareAndSet(false, true)) {
            if (!isClosed) {
                throw new IllegalStateException(
                        "Tried to build Arrow batch into memory before it is closed.");
            }
            // serialize the arrow batch to dynamically allocated memory segments, no waiting mem
            try {
                arrowWriter.serializeToOutputView(
                        pagedOutputView,
                        firstSegment,
                        ARROW_ROWKIND_OFFSET + rowKindWriter.sizeInBytes(),
                        false);
                arrowWriter.recycle(writerEpoch);
                serialized = true;
                return true;
            } catch (IOException e) {
                // reset all state if failed to serialize (e.g., EOFException when no more memory)
                pagedOutputView.recycleAllocated();
                pagedOutputView.clear();
                // make pagedOutputView holds the ref of first segment to can recycle it later
                pagedOutputView.seekOutput(
                        firstSegment, ARROW_ROWKIND_OFFSET + rowKindWriter.sizeInBytes(), true);
                serializationLock.set(false);
                return false;
            }
        } else {
            // this builder has already been serialized or is serializing
            return serialized;
        }
    }

    public MultiBytesView build() throws IOException {
        if (bytesView != null) {
            return bytesView;
        }

        // build() must be happen after serialize() or trySerialize()
        if (!serialized) {
            throw new IllegalStateException(
                    "Tried to collect memory segments before the Arrow batch is serialized.");
        }

        writeBatchHeader();
        bytesView =
                MultiBytesView.builder()
                        .addMemorySegmentByteViewList(pagedOutputView.getSegmentBytesViewList())
                        .build();
        return bytesView;
    }

    /** Check if the builder is full. */
    public boolean isFull() {
        return arrowWriter.isFull();
    }

    /**
     * Try to append a record to the builder. Return true if the record is appended successfully,
     * false if the builder is full.
     */
    public void append(RowKind rowKind, InternalRow row) throws Exception {
        if (isClosed) {
            throw new IllegalStateException(
                    "Tried to append a record, but MemoryLogRecordsArrowBuilder is closed for record appends");
        }

        arrowWriter.writeRow(row);
        rowKindWriter.writeRowKind(rowKind);
        reCalculateSizeInBytes = true;
    }

    public long writerId() {
        return writerId;
    }

    public int batchSequence() {
        return batchSequence;
    }

    public void setWriterState(long writerId, int batchBaseSequence) {
        this.writerId = writerId;
        this.batchSequence = batchBaseSequence;
    }

    public void resetWriterState(long writerId, int batchSequence) {
        // trigger to rewrite batch header
        this.bytesView = null;
        this.writerId = writerId;
        this.batchSequence = batchSequence;
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws Exception {
        if (isClosed) {
            return;
        }

        // update sizeInBytes and recordCount if needed
        getSizeInBytes();
        isClosed = true;
    }

    public void deallocate() {
        arrowWriter.recycle(writerEpoch);
        pagedOutputView.recycleAll();
    }

    public int getSizeInBytes() {
        if (reCalculateSizeInBytes) {
            // make size in bytes up-to-date
            sizeInBytes =
                    ARROW_ROWKIND_OFFSET + rowKindWriter.sizeInBytes() + arrowWriter.sizeInBytes();
            recordCount = arrowWriter.getRecordsCount();
        }

        reCalculateSizeInBytes = false;
        return sizeInBytes;
    }

    // ----------------------- internal methods -------------------------------
    private void writeBatchHeader() throws IOException {
        // pagedOutputView doesn't support seek to previous segment,
        // so we create a new output view on the first segment
        MemorySegmentOutputView outputView = new MemorySegmentOutputView(firstSegment);
        outputView.setPosition(0);
        // update header.
        outputView.writeLong(baseLogOffset);
        outputView.writeInt(sizeInBytes - BASE_OFFSET_LENGTH - LENGTH_LENGTH);
        outputView.writeByte(magic);

        // write empty timestamp which will be overridden on server side
        outputView.writeLong(0);
        // write empty crc first.
        outputView.writeUnsignedInt(0);

        outputView.writeShort((short) schemaId);
        // skip write attributes
        outputView.setPosition(WRITE_CLIENT_ID_OFFSET);
        outputView.writeLong(writerId);
        outputView.writeInt(batchSequence);
        outputView.writeInt(recordCount);

        // Update crc.
        outputView.setPosition(CRC_OFFSET);
        outputView.writeUnsignedInt(calculateCrc());
    }

    private long calculateCrc() {
        // pagedOutputView contains the first segment (including the batch header)
        List<MemorySegmentBytesView> bytesViewList = pagedOutputView.getSegmentBytesViewList();
        ByteBuffer[] buffers = new ByteBuffer[bytesViewList.size()];
        int[] offsets = new int[bytesViewList.size()];
        int[] sizes = new int[bytesViewList.size()];
        for (int i = 0; i < bytesViewList.size(); i++) {
            MemorySegmentBytesView bytesView = bytesViewList.get(i);
            buffers[i] = bytesView.getMemorySegment().wrap(0, bytesView.getBytesLength());
            if (i == 0) {
                offsets[i] = SCHEMA_ID_OFFSET;
                sizes[i] = bytesView.getBytesLength() - SCHEMA_ID_OFFSET;
            } else {
                offsets[i] = bytesView.getPosition();
                sizes[i] = bytesView.getBytesLength();
            }
        }

        return Crc32C.compute(buffers, offsets, sizes);
    }

    @VisibleForTesting
    int getMaxSizeInBytes() {
        return arrowWriter.getMaxSizeInBytes();
    }
}

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

package org.apache.fluss.record;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.utils.AbstractIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An entity used to describe multiple {@link LogRecordBatch}s in memory.
 *
 * @since 0.1
 */
@PublicEvolving
public class MemoryLogRecords implements LogRecords {

    public static final MemoryLogRecords EMPTY = new MemoryLogRecords(null, -1, 0);

    private final MemorySegment memorySegment;
    private final int position;
    private int sizeInBytes;

    private MemoryLogRecords(MemorySegment remaining, int position, int sizeInBytes) {
        this.memorySegment = remaining;
        this.position = position;
        this.sizeInBytes = sizeInBytes;
    }

    /**
     * Write all records to the given channel (including partial records).
     *
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException For any IO errors writing to the channel
     */
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        ByteBuffer buffer = memorySegment.wrap(position, sizeInBytes);
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes) {
            written += channel.write(buffer);
        }
        return written;
    }

    public void clear() {
        sizeInBytes = 0;
    }

    public void ensureValid() {
        if (sizeInBytes < DefaultLogRecordBatch.RECORD_BATCH_HEADER_SIZE) {
            throw new RuntimeException(
                    "Record batch is corrupt (the size "
                            + sizeInBytes
                            + " is smaller than the minimum allowed overhead "
                            + DefaultLogRecordBatch.RECORD_BATCH_HEADER_SIZE
                            + ")");
        }
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public Iterable<LogRecordBatch> batches() {
        return this::batchIterator;
    }

    public AbstractIterator<LogRecordBatch> batchIterator() {
        return new LogRecordBatchIterator<>(
                new MemorySegmentLogInputStream(memorySegment, position, sizeInBytes));
    }

    public MemorySegment getMemorySegment() {
        return memorySegment;
    }

    public int getPosition() {
        return position;
    }

    public static MemoryLogRecords readableRecords(MemoryLogRecords records, int sizeInBytes) {
        return new MemoryLogRecords(records.memorySegment, records.position, sizeInBytes);
    }

    /** Make a {@link MemoryLogRecords} instance from the given bytes. */
    public static MemoryLogRecords pointToBytes(byte[] bytes) {
        return pointToBytes(bytes, 0, bytes.length);
    }

    /** Make a {@link MemoryLogRecords} instance from the given bytes. */
    public static MemoryLogRecords pointToBytes(byte[] bytes, int offset, int length) {
        return pointToMemory(MemorySegment.wrap(bytes), offset, length);
    }

    /** Make a {@link MemoryLogRecords} instance from the given segment without copying bytes. */
    public static MemoryLogRecords pointToMemory(MemorySegment segment, int position, int length) {
        return new MemoryLogRecords(segment, position, length);
    }

    /**
     * Make a {@link MemoryLogRecords} instance from the given buffer without copying bytes if
     * possible. The buffer can be either direct or non-direct.
     */
    public static MemoryLogRecords pointToByteBuffer(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            MemorySegment segment = MemorySegment.wrapOffHeapMemory(buffer);
            return pointToMemory(segment, buffer.position(), buffer.limit() - buffer.position());
        } else if (buffer.hasArray()) {
            byte[] bytes = buffer.array();
            int offset = buffer.arrayOffset() + buffer.position();
            int length = buffer.remaining();
            return pointToBytes(bytes, offset, length);
        } else {
            // fallback to copy bytes
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return pointToBytes(bytes);
        }
    }

    /**
     * Make a {@link MemoryLogRecords} instance from the given {@link BytesView}.
     *
     * <p>Note: this is a heavy operation involving copy bytes, only used for testing.
     */
    @VisibleForTesting
    public static MemoryLogRecords pointToBytesView(BytesView bytesView) {
        return pointToByteBuffer(bytesView.getByteBuf().nioBuffer());
    }
}

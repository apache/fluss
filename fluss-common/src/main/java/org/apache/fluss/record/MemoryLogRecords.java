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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.utils.AbstractIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.record.LogRecordBatchFormat.LOG_MAGIC_VALUE_V0;
import static org.apache.fluss.record.LogRecordBatchFormat.MAGIC_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.utils.Preconditions.checkArgument;

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
    @Nullable private final List<MemorySegmentBytesView> batchSegments;
    private int sizeInBytes;

    private MemoryLogRecords(MemorySegment remaining, int position, int sizeInBytes) {
        this(remaining, position, sizeInBytes, null);
    }

    private MemoryLogRecords(
            MemorySegment segment,
            int position,
            int sizeInBytes,
            @Nullable List<MemorySegmentBytesView> batchSegments) {
        this.memorySegment = segment;
        this.position = position;
        this.sizeInBytes = sizeInBytes;
        this.batchSegments = batchSegments;
    }

    /**
     * Write all records to the given channel (including partial records).
     *
     * @param channel The channel to write to
     * @return The number of bytes written
     * @throws IOException For any IO errors writing to the channel
     */
    public int writeFullyTo(GatheringByteChannel channel) throws IOException {
        if (batchSegments != null) {
            return writeBatchSegments(channel);
        }
        ByteBuffer buffer = memorySegment.wrap(position, sizeInBytes);
        buffer.mark();
        int written = 0;
        while (written < sizeInBytes) {
            written += channel.write(buffer);
        }
        return written;
    }

    private int writeBatchSegments(GatheringByteChannel channel) throws IOException {
        ByteBuffer[] buffers = new ByteBuffer[batchSegments.size()];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = batchSegments.get(i).getByteBuffer();
        }
        int first = 0;
        int written = 0;
        while (written < sizeInBytes) {
            written += (int) channel.write(buffers, first, buffers.length - first);
            while (first < buffers.length && !buffers[first].hasRemaining()) {
                first++;
            }
        }
        return written;
    }

    public void clear() {
        sizeInBytes = 0;
    }

    public void ensureValid(byte recordBatchMagic) {
        int recordBatchHeaderSize = recordBatchHeaderSize(recordBatchMagic);
        if (sizeInBytes < recordBatchHeaderSize) {
            throw new RuntimeException(
                    "Record batch is corrupt (the size "
                            + sizeInBytes
                            + " is smaller than the minimum allowed overhead "
                            + recordBatchHeaderSize
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
        if (batchSegments == null) {
            return new LogRecordBatchIterator<>(
                    new MemorySegmentLogInputStream(memorySegment, position, sizeInBytes));
        }
        return new AbstractIterator<LogRecordBatch>() {
            private boolean returned;

            @Override
            protected LogRecordBatch makeNext() {
                if (returned || sizeInBytes == 0) {
                    return allDone();
                }
                returned = true;
                return DefaultLogRecordBatch.pointToBatchSegments(batchSegments);
            }
        };
    }

    public MemorySegment getMemorySegment() {
        return batchSegments == null
                ? memorySegment
                : copyBatchSegments(batchSegments, sizeInBytes);
    }

    public int getPosition() {
        return position;
    }

    public static MemoryLogRecords readableRecords(MemoryLogRecords records, int sizeInBytes) {
        if (records.batchSegments != null && records.sizeInBytes == sizeInBytes) {
            return new MemoryLogRecords(null, 0, sizeInBytes, records.batchSegments);
        }
        return new MemoryLogRecords(records.getMemorySegment(), records.position, sizeInBytes);
    }

    /**
     * Wraps one complete V0 batch for synchronous append. The first segment contains the mutable
     * batch header. The caller owns the backing memory until the append completes. Callers that
     * require a contiguous view receive a copy.
     */
    @Internal
    public static MemoryLogRecords pointToSingleBatch(List<MemorySegmentBytesView> segments) {
        checkArgument(!segments.isEmpty(), "A record batch must have a header");
        MemorySegmentBytesView first = segments.get(0);
        MemorySegment header = first.getMemorySegment();
        int position = first.getPosition();
        checkArgument(
                first.getBytesLength() >= V0_RECORD_BATCH_HEADER_SIZE
                        && header.get(position + MAGIC_OFFSET) == LOG_MAGIC_VALUE_V0,
                "Expected a complete V0 batch header in the first segment");
        int size = 0;
        for (MemorySegmentBytesView segment : segments) {
            size = Math.addExact(size, segment.getBytesLength());
        }
        checkArgument(
                (long) header.getInt(position + LogRecordBatchFormat.LENGTH_OFFSET)
                                + LogRecordBatchFormat.LOG_OVERHEAD
                        == size,
                "Batch length does not match the supplied segments");
        if (segments.size() == 1) {
            return pointToMemory(header, position, size);
        }
        return new MemoryLogRecords(null, 0, size, new ArrayList<>(segments));
    }

    static MemorySegment copyBatchSegments(List<MemorySegmentBytesView> segments, int size) {
        MemorySegment target = MemorySegment.allocateHeapMemory(size);
        if (size == 0) {
            return target;
        }
        int offset = 0;
        for (MemorySegmentBytesView segment : segments) {
            int length = segment.getBytesLength();
            segment.getMemorySegment().copyTo(segment.getPosition(), target, offset, length);
            offset += length;
        }
        return target;
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

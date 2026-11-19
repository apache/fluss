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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.bytesview.MemorySegmentBytesView;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.testutils.LogRecordsAssert;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;
import java.util.function.Function;

import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;
import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MemoryLogRecords}. */
class MemoryLogRecordsTest {

    @Test
    void testPointToDirectByteBuffer() throws Exception {
        verifyPointToByteBuffer(ByteBuffer::allocateDirect);
    }

    @Test
    void testPointToDirectByteBufferWithShift() throws Exception {
        verifyPointToByteBuffer(
                size -> {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(size + 3);
                    buffer.put(new byte[] {1, 2, 3});
                    buffer.position(3);
                    return buffer;
                });
    }

    @Test
    void testPointToHeapByteBuffer() throws Exception {
        verifyPointToByteBuffer(ByteBuffer::allocate);
    }

    @Test
    void testPointToHeapByteBufferWithShift() throws Exception {
        verifyPointToByteBuffer(
                size -> {
                    ByteBuffer buffer = ByteBuffer.allocate(size + 3);
                    buffer.put(new byte[] {1, 2, 3});
                    buffer.position(3);
                    return buffer;
                });
    }

    @Test
    void testGatherWritePreservesHeaderAndPayloadAcrossPartialWrites() throws Exception {
        MemoryLogRecords original =
                DataTestUtils.genMemoryLogRecordsWithWriterId(DATA1, 1000, 6, 0);
        MemorySegment segment = original.getMemorySegment();
        int position = original.getPosition();
        int size = original.sizeInBytes();
        int firstLength = V0_RECORD_BATCH_HEADER_SIZE;
        int payloadLength = size - firstLength;
        int payloadPosition = 7;
        MemorySegment payload =
                MemorySegment.allocateOffHeapMemory(payloadPosition + payloadLength);
        segment.copyTo(position + firstLength, payload, payloadPosition, payloadLength);
        MemoryLogRecords segmented =
                MemoryLogRecords.pointToSingleBatch(
                        Arrays.asList(
                                new MemorySegmentBytesView(segment, position, firstLength),
                                new MemorySegmentBytesView(
                                        payload, payloadPosition, payloadLength)));
        DefaultLogRecordBatch batch = (DefaultLogRecordBatch) segmented.batchIterator().next();
        batch.setBaseLogOffset(123);
        batch.setCommitTimestamp(456);
        batch.ensureValid();

        byte[] expected = new byte[size];
        segment.get(position, expected);
        try (ShortGatheringChannel channel = new ShortGatheringChannel(size)) {
            assertThat(segmented.writeFullyTo(channel)).isEqualTo(size);
            assertThat(channel.destination.position()).isEqualTo(size);
            assertThat(channel.destination.array()).isEqualTo(expected);
            LogRecordBatch written =
                    MemoryLogRecords.pointToBytes(channel.destination.array())
                            .batchIterator()
                            .next();
            assertThat(written.baseLogOffset()).isEqualTo(123);
            assertThat(written.commitTimestamp()).isEqualTo(456);
            written.ensureValid();
            assertThat(batch).isEqualTo(written).hasSameHashCodeAs(written);
            assertThat(written).isEqualTo(batch);
        }

        int lastByte = payloadPosition + payloadLength - 1;
        payload.put(lastByte, (byte) (payload.get(lastByte) ^ 1));
        assertThat(batch.isValid()).isFalse();
    }

    void verifyPointToByteBuffer(Function<Integer, ByteBuffer> bufferSupplier) throws Exception {
        int writerId = 1000;
        int seqno = 6;
        long baseOffset = 123L;
        MemoryLogRecords records =
                DataTestUtils.genMemoryLogRecordsWithWriterId(DATA1, writerId, seqno, baseOffset);
        ByteBuffer buffer = bufferSupplier.apply(records.sizeInBytes());

        int originPos = buffer.position();
        records.getMemorySegment().get(records.getPosition(), buffer, records.sizeInBytes());
        buffer.position(originPos);

        MemoryLogRecords newRecords = MemoryLogRecords.pointToByteBuffer(buffer);
        LogRecordsAssert.assertThatLogRecords(newRecords)
                .hasBatchesCount(1)
                .withSchema(DATA1_ROW_TYPE)
                .withSchemaGetter(TEST_SCHEMA_GETTER)
                .isEqualTo(records);
    }

    private static final class ShortGatheringChannel implements GatheringByteChannel {
        private final ByteBuffer destination;
        private boolean open = true;

        private ShortGatheringChannel(int size) {
            destination = ByteBuffer.allocate(size);
        }

        @Override
        public long write(ByteBuffer[] sources, int offset, int length) {
            int remaining = 13;
            for (int i = offset; i < offset + length && remaining > 0; i++) {
                ByteBuffer source = sources[i];
                int bytes = Math.min(remaining, source.remaining());
                ByteBuffer part = source.duplicate();
                part.limit(part.position() + bytes);
                destination.put(part);
                source.position(part.limit());
                remaining -= bytes;
            }
            return 13 - remaining;
        }

        @Override
        public long write(ByteBuffer[] sources) {
            return write(sources, 0, sources.length);
        }

        @Override
        public int write(ByteBuffer source) {
            return (int) write(new ByteBuffer[] {source});
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public void close() {
            open = false;
        }
    }
}

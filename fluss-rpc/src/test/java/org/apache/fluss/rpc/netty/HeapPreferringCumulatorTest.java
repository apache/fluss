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

package org.apache.fluss.rpc.netty;

import org.apache.fluss.rpc.netty.NettyChannelInitializer.HeapPreferringCumulator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HeapPreferringCumulator}. */
class HeapPreferringCumulatorTest {

    private static final int LENGTH_FIELD_SIZE = 4;
    private final HeapPreferringCumulator cumulator =
            new HeapPreferringCumulator(LENGTH_FIELD_SIZE);
    private final ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;

    // -------------------------------------------------------------------------
    //  Empty cumulation
    // -------------------------------------------------------------------------

    @Test
    void testCumulationSameAsIn() {
        ByteBuf buf = alloc.heapBuffer(4).writeBytes(new byte[] {1, 2, 3, 4});
        // Retain once extra because cumulate() will release `in` when cumulation == in.
        buf.retain();

        ByteBuf result = cumulator.cumulate(alloc, buf, buf);
        assertThat(result).isSameAs(buf);
        // refCnt should be 1 after cumulate released one reference.
        assertThat(result.refCnt()).isEqualTo(1);
        result.release();
    }

    @Test
    void testEmptyCumulationCopiesDirectBufferToHeap() {
        ByteBuf cumulation = Unpooled.EMPTY_BUFFER;
        // Write data without a valid frame-length header.
        ByteBuf in = alloc.directBuffer(3).writeBytes(new byte[] {10, 20, 30});

        ByteBuf result = cumulator.cumulate(alloc, cumulation, in);

        // Result should be a new heap buffer with the same content.
        assertThat(result).isNotSameAs(in);
        assertThat(result.isDirect()).isFalse();
        assertThat(result.readableBytes()).isEqualTo(3);
        assertThat(readBytes(result)).isEqualTo(new byte[] {10, 20, 30});
        // The incoming direct buffer should have been released.
        assertThat(in.refCnt()).isEqualTo(0);
        result.release();
    }

    @Test
    void testEmptyCumulationPreSizesHeapBufferFromFrameLength() {
        ByteBuf cumulation = Unpooled.EMPTY_BUFFER;
        // Build a direct buffer that starts with a 4-byte frame-length field.
        int frameLength = 100;
        ByteBuf in = alloc.directBuffer(LENGTH_FIELD_SIZE + 8);
        in.writeInt(frameLength);
        in.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

        ByteBuf result = cumulator.cumulate(alloc, cumulation, in);

        assertThat(result.isDirect()).isFalse();
        // The allocated capacity should be at least LENGTH_FIELD_SIZE + frameLength.
        assertThat(result.capacity()).isEqualTo(LENGTH_FIELD_SIZE + frameLength);
        assertThat(result.readableBytes()).isEqualTo(LENGTH_FIELD_SIZE + 8);
        // Verify the frame-length field was preserved.
        assertThat(result.getInt(result.readerIndex())).isEqualTo(frameLength);
        result.release();
    }

    @Test
    void testEmptyCumulationFallsBackWhenFrameLengthNonPositive() {
        ByteBuf cumulation = Unpooled.EMPTY_BUFFER;
        // Frame length of 0 should fall back to readable-bytes sizing.
        ByteBuf in = alloc.directBuffer(LENGTH_FIELD_SIZE);
        in.writeInt(0);

        ByteBuf result = cumulator.cumulate(alloc, cumulation, in);

        assertThat(result.isDirect()).isFalse();
        assertThat(result.readableBytes()).isEqualTo(LENGTH_FIELD_SIZE);
        assertThat(in.refCnt()).isEqualTo(0);
        result.release();
    }

    // -------------------------------------------------------------------------
    //  Non-empty cumulation: append path
    // -------------------------------------------------------------------------

    @Test
    void testAppendToHeapCumulationWithSufficientSpace() {
        ByteBuf cumulation = alloc.heapBuffer(16).writeBytes(new byte[] {1, 2, 3});
        ByteBuf in = alloc.heapBuffer(2).writeBytes(new byte[] {4, 5});

        ByteBuf result = cumulator.cumulate(alloc, cumulation, in);

        // Should reuse the existing cumulation.
        assertThat(result).isSameAs(cumulation);
        assertThat(result.isDirect()).isFalse();
        assertThat(result.readableBytes()).isEqualTo(5);
        assertThat(readBytes(result)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(in.refCnt()).isEqualTo(0);
        result.release();
    }

    @Test
    void testAppendExpandsWhenInsufficientSpace() {
        // Allocate a heap buffer with exact capacity so appending requires expansion.
        ByteBuf cumulation = alloc.heapBuffer(3, 3).writeBytes(new byte[] {1, 2, 3});
        ByteBuf in = alloc.heapBuffer(2).writeBytes(new byte[] {4, 5});

        ByteBuf result = cumulator.cumulate(alloc, cumulation, in);

        // Should have expanded to a new heap buffer.
        assertThat(result).isNotSameAs(cumulation);
        assertThat(result.isDirect()).isFalse();
        assertThat(result.readableBytes()).isEqualTo(5);
        assertThat(readBytes(result)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        // Old cumulation released, incoming released.
        assertThat(cumulation.refCnt()).isEqualTo(0);
        assertThat(in.refCnt()).isEqualTo(0);
        result.release();
    }

    @Test
    void testAppendAlwaysExpandsDirectCumulation() {
        // Even when the direct cumulation has enough writable space, it should be replaced by heap.
        ByteBuf cumulation = alloc.directBuffer(64).writeBytes(new byte[] {1, 2, 3});
        ByteBuf in = alloc.heapBuffer(2).writeBytes(new byte[] {4, 5});

        ByteBuf result = cumulator.cumulate(alloc, cumulation, in);

        // The direct cumulation must be replaced.
        assertThat(result).isNotSameAs(cumulation);
        assertThat(result.isDirect()).isFalse();
        assertThat(result.readableBytes()).isEqualTo(5);
        assertThat(readBytes(result)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(cumulation.refCnt()).isEqualTo(0);
        assertThat(in.refCnt()).isEqualTo(0);
        result.release();
    }

    @Test
    void testAppendExpandsReadOnlyCumulation() {
        ByteBuf cumulation = alloc.heapBuffer(16).writeBytes(new byte[] {1, 2, 3}).asReadOnly();
        ByteBuf in = alloc.heapBuffer(2).writeBytes(new byte[] {4, 5});

        ByteBuf result = cumulator.cumulate(alloc, cumulation, in);

        assertThat(result).isNotSameAs(cumulation);
        assertThat(result.isDirect()).isFalse();
        assertThat(result.readableBytes()).isEqualTo(5);
        assertThat(readBytes(result)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(in.refCnt()).isEqualTo(0);
        result.release();
    }

    private static byte[] readBytes(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        return bytes;
    }
}

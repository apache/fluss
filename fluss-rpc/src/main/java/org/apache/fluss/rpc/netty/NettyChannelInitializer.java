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

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;

import static java.lang.Integer.MAX_VALUE;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A basic {@link ChannelInitializer} for initializing {@link SocketChannel} instances to support
 * netty logging and add common handlers.
 */
public class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final int maxIdleTimeSeconds;

    private static final NettyLogger nettyLogger = new NettyLogger();

    public NettyChannelInitializer(long maxIdleTimeSeconds) {
        checkArgument(maxIdleTimeSeconds <= Integer.MAX_VALUE, "maxIdleTimeSeconds too large");
        this.maxIdleTimeSeconds = (int) maxIdleTimeSeconds;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (nettyLogger.getLoggingHandler() != null) {
            ch.pipeline().addLast("loggingHandler", nettyLogger.getLoggingHandler());
        }
    }

    public void addFrameDecoder(
            SocketChannel ch, int maxFrameLength, int initialBytesToStrip, boolean preferHeap) {
        LengthFieldBasedFrameDecoder frameDecoder =
                new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, initialBytesToStrip);
        if (preferHeap) {
            frameDecoder.setCumulator(new HeapPreferringCumulator(4));
        }
        ch.pipeline().addLast("frameDecoder", frameDecoder);
    }

    public void addIdleStateHandler(SocketChannel ch) {
        ch.pipeline().addLast("idle", new IdleStateHandler(0, 0, maxIdleTimeSeconds));
    }

    /**
     * A custom {@link ByteToMessageDecoder.Cumulator} that keeps the cumulation buffer on the JVM
     * heap to avoid off-heap memory pressure from large partial messages.
     *
     * <p>On native transports (epoll / kqueue), {@code EpollRecvByteAllocatorHandle.allocate()}
     * always forces the read buffer to be direct regardless of the channel allocator. The default
     * {@code ByteToMessageDecoder#MERGE_CUMULATOR}'s fast-path would adopt this direct buffer as
     * the cumulation. For large partial messages, the cumulation can grow up to the maximum frame
     * size (e.g. 32 MB), so multiple connections could consume significant off-heap memory.
     *
     * <p><b>Origin:</b> Derived from Netty 4.1's {@code ByteToMessageDecoder.MERGE_CUMULATOR} with
     * the following modifications to enforce heap-backed cumulation:
     *
     * <ol>
     *   <li><b>Heap-only adoption on empty cumulation:</b> When the cumulation is empty and the
     *       incoming buffer is contiguous, Netty's MERGE_CUMULATOR adopts any incoming buffer
     *       directly. This class only adopts heap buffers; for direct buffers, it allocates a new
     *       heap cumulation pre-sized to the expected frame length (via {@link #allocateForFrame}).
     *   <li><b>Direct cumulation triggers expansion:</b> Added {@code cumulation.isDirect()} as an
     *       additional condition to trigger {@link #expandCumulation}, ensuring direct cumulations
     *       are always replaced with heap ones during appends.
     *   <li><b>Frame-length-aware pre-allocation:</b> New {@link #allocateForFrame} method reads
     *       the 4-byte length field from the incoming buffer to pre-size the heap buffer to the
     *       exact frame size, avoiding repeated re-allocations.
     *   <li><b>Forced heap in expandCumulation:</b> The companion {@link
     *       NettyChannelInitializer#expandCumulation} uses {@code alloc.heapBuffer()} instead of
     *       Netty's {@code alloc.buffer()} so the replacement cumulation is always on the JVM heap.
     * </ol>
     */
    static final class HeapPreferringCumulator implements ByteToMessageDecoder.Cumulator {

        /** Length-field size in bytes used by the frame protocol. */
        private final int lengthFieldSize;

        public HeapPreferringCumulator(int lengthFieldSize) {
            this.lengthFieldSize = lengthFieldSize;
        }

        @Override
        public ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in) {
            if (cumulation == in) {
                in.release();
                return cumulation;
            }
            if (!cumulation.isReadable() && in.isContiguous()) {
                // Adopt the incoming buffer only when it is already heap-backed. Direct buffers are
                // always copied into a heap cumulation so downstream decoding continues to operate
                // on heap memory.
                if (!in.isDirect()) {
                    cumulation.release();
                    return in;
                }
                // Direct buffer — allocate a heap cumulation pre-sized to the expected frame length
                // to avoid repeated expansion.
                cumulation.release();
                ByteBuf heapCumulation = null;
                try {
                    heapCumulation = allocateForFrame(alloc, in);
                    heapCumulation.writeBytes(in);
                    return heapCumulation;
                } catch (Exception e) {
                    if (heapCumulation != null) {
                        heapCumulation.release();
                    }
                    throw e;
                } finally {
                    in.release();
                }
            }
            try {
                final int required = in.readableBytes();
                if (required > cumulation.maxWritableBytes()
                        || required > cumulation.maxFastWritableBytes() && cumulation.refCnt() > 1
                        || cumulation.isReadOnly()
                        || cumulation.isDirect()) {
                    return expandCumulation(alloc, cumulation, in);
                }
                cumulation.writeBytes(in, in.readerIndex(), required);
                in.readerIndex(in.writerIndex());
                return cumulation;
            } finally {
                in.release();
            }
        }

        /**
         * Allocates a heap buffer pre-sized for the expected frame. If the length field (first 4
         * bytes) is readable, the buffer is sized to exactly {@code LENGTH_FIELD_SIZE +
         * frameLength}, so no expansion is needed during subsequent reads. Otherwise falls back to
         * the current readable size.
         */
        private ByteBuf allocateForFrame(ByteBufAllocator alloc, ByteBuf in) {
            if (in.readableBytes() >= lengthFieldSize) {
                int frameLength = in.getInt(in.readerIndex());
                if (frameLength > 0) {
                    return alloc.heapBuffer(lengthFieldSize + frameLength);
                }
            }
            return alloc.heapBuffer(in.readableBytes());
        }
    }

    /**
     * Replaces the current cumulation with a new heap buffer containing both the old data and the
     * new incoming data. Used when the existing cumulation cannot accommodate the new data (e.g.
     * capacity exceeded, shared buffer, or direct buffer that needs to be converted to heap).
     */
    private static ByteBuf expandCumulation(
            ByteBufAllocator alloc, ByteBuf oldCumulation, ByteBuf in) {
        int oldBytes = oldCumulation.readableBytes();
        int newBytes = in.readableBytes();
        int totalBytes = oldBytes + newBytes;
        ByteBuf newCumulation = alloc.heapBuffer(alloc.calculateNewCapacity(totalBytes, MAX_VALUE));
        ByteBuf toRelease = newCumulation;
        try {
            newCumulation
                    .setBytes(0, oldCumulation, oldCumulation.readerIndex(), oldBytes)
                    .setBytes(oldBytes, in, in.readerIndex(), newBytes)
                    .writerIndex(totalBytes);
            in.readerIndex(in.writerIndex());
            toRelease = oldCumulation;
            return newCumulation;
        } finally {
            toRelease.release();
        }
    }
}

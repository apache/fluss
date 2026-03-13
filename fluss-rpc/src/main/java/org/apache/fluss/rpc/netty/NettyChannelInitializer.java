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
        LengthFieldBasedFrameDecoder lengthFieldBasedFrameDecoder =
                new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, initialBytesToStrip);
        lengthFieldBasedFrameDecoder.setCumulator(new FlussMergeCumulator(preferHeap));
        ch.pipeline().addLast("frameDecoder", lengthFieldBasedFrameDecoder);
    }

    public void addIdleStateHandler(SocketChannel ch) {
        ch.pipeline().addLast("idle", new IdleStateHandler(0, 0, maxIdleTimeSeconds));
    }

    /**
     * A custom {@link ByteToMessageDecoder.Cumulator} that ensures the cumulation buffer stays on
     * the JVM heap when heap memory is preferred.
     *
     * <p>Why this is needed — the two pitfalls in Netty's default {@code MERGE_CUMULATOR}:
     *
     * <ol>
     *   <li><b>Fast-path "use in directly" problem:</b> When the cumulation is empty (e.g. the very
     *       first read, where cumulation starts as {@code EMPTY_BUFFER}), {@code MERGE_CUMULATOR}
     *       short-circuits and returns the incoming {@code in} buffer as the new cumulation without
     *       copying. On <b>native transports (epoll / kqueue)</b>, {@code
     *       EpollRecvByteAllocatorHandle.allocate()} always forces the read buffer to be direct
     *       regardless of the channel's configured allocator (it wraps it with {@code
     *       PreferredDirectByteBufAllocator} internally, because JNI requires direct buffers). So
     *       even with {@code PreferHeapByteBufAllocator} set on the channel, the first {@code in}
     *       is direct, and the default cumulator would make a direct buffer the cumulation for the
     *       rest of the connection lifetime. This cumulator blocks that by skipping the fast-path
     *       when {@code isPreferHeap=true}.
     *   <li><b>Subsequent reads — write-into-direct cumulation problem:</b> If for any reason a
     *       direct cumulation was established (e.g. before this fix, or on NIO when the first read
     *       buffer happened to be direct), subsequent {@code in} buffers with small sizes will pass
     *       the {@code required <= maxWritableBytes()} check and be written into the existing
     *       direct cumulation via {@code writeBytes}, bypassing {@code expandCumulation} entirely.
     *       Setting {@code ch.config().setAllocator(preferHeapAllocator)} alone does NOT fix this
     *       because it only affects newly allocated buffers, not writes into an existing one.
     * </ol>
     *
     * <p>By blocking the fast-path, the first {@code in} (direct) triggers {@code
     * expandCumulation}, which calls {@code alloc.buffer(...)}. Since {@code alloc} is the channel
     * allocator ({@code PreferHeapByteBufAllocator}), the new cumulation buffer will be a heap
     * buffer. All subsequent writes then accumulate into that heap buffer.
     */
    static final class FlussMergeCumulator implements ByteToMessageDecoder.Cumulator {
        private final boolean isPreferHeap;

        public FlussMergeCumulator(boolean isPreferHeap) {
            this.isPreferHeap = isPreferHeap;
        }

        @Override
        public ByteBuf cumulate(ByteBufAllocator alloc, ByteBuf cumulation, ByteBuf in) {
            if (cumulation == in) {
                // when the in buffer is the same as the cumulation it is doubly retained, release
                // it once
                in.release();
                return cumulation;
            }
            if (!cumulation.isReadable() && in.isContiguous()) {
                // If cumulation is empty and input buffer is contiguous, use it directly
                cumulation.release();
                return in;
            }
            try {
                final int required = in.readableBytes();
                if (required > cumulation.maxWritableBytes()
                        || required > cumulation.maxFastWritableBytes() && cumulation.refCnt() > 1
                        || cumulation.isReadOnly()
                        || (cumulation.isDirect() && isPreferHeap)) {
                    // Expand cumulation (by replacing it) under the following conditions:
                    // - cumulation cannot be resized to accommodate the additional data
                    // - cumulation can be expanded with a reallocation operation to accommodate but
                    // the buffer is
                    //   assumed to be shared (e.g. refCnt() > 1) and the reallocation may not be
                    // safe.
                    return expandCumulation(alloc, cumulation, in);
                }
                cumulation.writeBytes(in, in.readerIndex(), required);
                in.readerIndex(in.writerIndex());
                return cumulation;
            } finally {
                // We must release in all cases as otherwise it may produce a leak if
                // writeBytes(...) throw
                // for whatever release (for example because of OutOfMemoryError)
                in.release();
            }
        }
    }

    static ByteBuf expandCumulation(ByteBufAllocator alloc, ByteBuf oldCumulation, ByteBuf in) {
        int oldBytes = oldCumulation.readableBytes();
        int newBytes = in.readableBytes();
        int totalBytes = oldBytes + newBytes;
        ByteBuf newCumulation = alloc.buffer(alloc.calculateNewCapacity(totalBytes, MAX_VALUE));
        ByteBuf toRelease = newCumulation;
        try {
            // This avoids redundant checks and stack depth compared to calling writeBytes(...)
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

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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.CorruptedFrameException;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.apache.fluss.shaded.netty4.io.netty.util.ReferenceCountUtil;

import java.util.function.IntPredicate;

import static org.apache.fluss.rpc.protocol.MessageCodec.REQUEST_HEADER_LENGTH;

/**
 * Gates complete RPC frames before parsing while keeping read state on the connection EventLoop.
 */
final class FairRequestFrameDecoder extends ChannelInboundHandlerAdapter {
    private final int maxFrameLength;
    private final RequestChannel requestChannel;
    private final IntPredicate handlesInline;
    private final Runnable resume = this::resumeRead;

    private ChannelHandlerContext ctx;
    private ByteBuf input;
    private boolean waiting;
    private boolean closed;

    FairRequestFrameDecoder(
            int maxFrameLength, RequestChannel requestChannel, IntPredicate handlesInline) {
        this.maxFrameLength = maxFrameLength;
        this.requestChannel = requestChannel;
        this.handlesInline = handlesInline;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        if (closed) {
            ReferenceCountUtil.release(message);
            return;
        }
        ByteBuf bytes = (ByteBuf) message;
        input =
                ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(
                        ctx.alloc(), input == null ? Unpooled.EMPTY_BUFFER : input, bytes);
        if (!waiting) {
            drain(false);
        }
    }

    private void drain(boolean useWakeGrant) {
        try {
            while (!closed && input != null && input.readableBytes() >= Integer.BYTES) {
                int start = input.readerIndex();
                long length = input.getUnsignedInt(start);
                long frameLength = Integer.BYTES + length;
                if (frameLength > maxFrameLength) {
                    throw new TooLongFrameException(
                            "RPC frame length " + frameLength + " exceeds " + maxFrameLength);
                }
                if (length < REQUEST_HEADER_LENGTH) {
                    throw new CorruptedFrameException(
                            "RPC frame is shorter than its header: " + length);
                }
                if (input.readableBytes() < frameLength) {
                    break;
                }
                boolean inline = handlesInline.test(input.getShort(start + Integer.BYTES));
                boolean granted = useWakeGrant && requestChannel.hasReadGrant(ctx.channel());
                useWakeGrant = false;
                if (!inline
                        && !granted
                        && !requestChannel.acquireRead(ctx, resume, (int) frameLength)) {
                    pauseRead();
                    return;
                }

                ByteBuf frame = input.readRetainedSlice((int) frameLength);
                try {
                    ctx.fireChannelRead(frame);
                } finally {
                    if (!inline || granted) {
                        // Parsing and admission are synchronous. An unconsumed grant is returned.
                        requestChannel.releaseRead(ctx.channel(), false);
                    }
                }
            }
        } catch (RuntimeException e) {
            closeInput();
            ctx.fireExceptionCaught(e);
        } finally {
            if (input != null) {
                if (!input.isReadable()) {
                    input.release();
                    input = null;
                } else if (input.refCnt() == 1) {
                    // Reclaim consumed bytes without moving outstanding lazy request slices.
                    input.discardSomeReadBytes();
                }
            }
        }
    }

    private void pauseRead() {
        waiting = true;
        ctx.channel().config().setAutoRead(false);
    }

    private void resumeRead() {
        if (!waiting || closed || !ctx.channel().isActive()) {
            return;
        }
        waiting = false;
        // A socket read cannot wake a frame that is already entirely buffered here.
        drain(true);
        ctx.fireChannelReadComplete();
        if (!waiting && !closed && ctx.channel().isActive()) {
            ctx.channel().config().setAutoRead(true);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        closeInput();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        closeInput();
    }

    private void closeInput() {
        if (!closed) {
            closed = true;
            requestChannel.releaseRead(ctx.channel(), true);
            ReferenceCountUtil.release(input);
            input = null;
        }
    }
}

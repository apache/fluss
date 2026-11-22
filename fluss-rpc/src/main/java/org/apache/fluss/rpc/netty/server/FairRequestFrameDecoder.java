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
import org.apache.fluss.shaded.netty4.io.netty.buffer.DefaultByteBufHolder;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.fluss.utils.clock.Clock;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.function.IntPredicate;

/**
 * Gates complete RPC frames before parsing while keeping read state on the connection EventLoop.
 */
final class FairRequestFrameDecoder extends ChannelInboundHandlerAdapter {
    private final RequestChannel requestChannel;
    private final IntPredicate handlesInline;
    private final Clock clock;
    private final Runnable resume = this::resumeRead;
    private ArrayDeque<BufferedFrames> pending;
    private ChannelHandlerContext ctx;
    private boolean closed;
    @Nullable private RuntimeException framingFailure;

    private FairRequestFrameDecoder(
            RequestChannel requestChannel, IntPredicate handlesInline, Clock clock) {
        this.requestChannel = requestChannel;
        this.handlesInline = handlesInline;
        this.clock = clock;
    }

    static void addAroundIdleHandler(
            ChannelPipeline pipeline,
            int maxFrameLength,
            RequestChannel requestChannel,
            IntPredicate handlesInline,
            Clock clock) {
        FairRequestFrameDecoder admission =
                new FairRequestFrameDecoder(requestChannel, handlesInline, clock);
        // Only complete network reads reach the idle detector. Admission wakeups run downstream.
        pipeline.addBefore(
                "idle", "frameDecoder", new RequestFrameBatchDecoder(maxFrameLength, admission));
        pipeline.addAfter("idle", "frameAdmission", admission);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        ByteBuf batch = (ByteBuf) message;
        try {
            if (closed) {
                return;
            }
            long receivedAtMs = clock.milliseconds();
            if (pending == null || pending.isEmpty()) {
                if (drain(batch, null)) {
                    return;
                }
                ctx.channel().config().setAutoRead(false);
                if (pending == null) {
                    pending = new ArrayDeque<>();
                }
            }
            pending.addLast(new BufferedFrames(batch.retain(), receivedAtMs));
        } finally {
            batch.release();
        }
    }

    private boolean drain(ByteBuf batch, @Nullable BufferedFrames buffered) {
        boolean useWakeGrant = buffered != null;
        while (!closed && batch.isReadable()) {
            int start = batch.readerIndex();
            int frameLength = Integer.BYTES + batch.getInt(start);
            boolean inline = handlesInline.test(batch.getShort(start + Integer.BYTES));
            boolean granted = useWakeGrant && requestChannel.hasReadGrant(ctx.channel());
            useWakeGrant = false;
            if (!inline && !granted && !requestChannel.acquireRead(ctx, resume, frameLength)) {
                return false;
            }
            boolean last = batch.readableBytes() == frameLength;
            ByteBuf frame = last ? batch.retain() : batch.readRetainedSlice(frameLength);
            try {
                if (buffered == null) {
                    ctx.fireChannelRead(frame);
                } else {
                    ctx.fireChannelRead(
                            last ? buffered : new BufferedFrames(frame, buffered.receivedAtMs));
                }
            } finally {
                if (!inline || granted) {
                    requestChannel.releaseRead(ctx.channel(), false);
                }
            }
            if (last) {
                return true;
            }
        }
        return true;
    }

    private void resumeRead() {
        if (closed || pending == null || pending.isEmpty() || !ctx.channel().isActive()) {
            return;
        }
        try {
            while (!pending.isEmpty()) {
                BufferedFrames batch = pending.peekFirst();
                if (!drain(batch.content(), batch)) {
                    break;
                }
                if (closed) {
                    return;
                }
                pending.removeFirst().release();
            }
        } catch (RuntimeException e) {
            closeInput();
            ctx.fireExceptionCaught(e);
            return;
        }
        ctx.fireChannelReadComplete();
        if (pending.isEmpty() && !closed && ctx.channel().isActive()) {
            if (framingFailure != null) {
                rejectAfterPending(framingFailure);
            } else {
                ctx.channel().config().setAutoRead(true);
            }
        }
    }

    void rejectAfterPending(RuntimeException failure) {
        // A malformed suffix must not discard valid frames ahead of it waiting for admission.
        if (pending != null && !pending.isEmpty()) {
            framingFailure = failure;
        } else {
            closeInput();
            ctx.fireExceptionCaught(failure);
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
            framingFailure = null;
            requestChannel.releaseRead(ctx.channel(), true);
            if (pending != null) {
                while (!pending.isEmpty()) {
                    pending.removeFirst().release();
                }
            }
        }
    }

    static final class BufferedFrames extends DefaultByteBufHolder {
        final long receivedAtMs;

        BufferedFrames(ByteBuf frame, long receivedAtMs) {
            super(frame);
            this.receivedAtMs = receivedAtMs;
        }
    }
}

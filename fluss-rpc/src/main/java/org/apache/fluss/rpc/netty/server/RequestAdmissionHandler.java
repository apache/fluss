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
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.CorruptedFrameException;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.TooLongFrameException;

import java.util.ArrayDeque;

/** Admits complete frames to a connection FIFO before the protocol parses them. */
public abstract class RequestAdmissionHandler extends ChannelInboundHandlerAdapter {
    private final RequestChannel requests;
    private final RequestChannel.Connection connection;
    private final Runnable resume = this::resumeReads;
    private ChannelHandlerContext input;
    private ArrayDeque<ByteBuf> buffered;
    private Throwable framingFailure;
    private boolean permit;
    private boolean inputClosed;

    protected RequestAdmissionHandler(RequestChannel requests) {
        this.requests = requests;
        this.connection = requests.newConnection(() -> input.executor().execute(resume));
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        input = ctx;
    }

    @Override
    public final void channelRead(ChannelHandlerContext ctx, Object message) {
        ByteBuf frame = (ByteBuf) message;
        if (inputClosed || framingFailure != null) {
            frame.release();
        } else if (!hasBufferedFrames() && acquire(frame)) {
            dispatch(frame);
        } else {
            if (buffered == null) {
                buffered = new ArrayDeque<>(1);
            }
            buffered.addLast(frame);
            ctx.channel().config().setAutoRead(false);
        }
    }

    @Override
    public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (hasBufferedFrames()
                && (cause instanceof CorruptedFrameException
                        || cause instanceof TooLongFrameException)) {
            // Netty can deliver valid frames before reporting a malformed suffix of the same read.
            if (framingFailure == null) {
                framingFailure = cause;
            }
            return;
        }
        handleFailure(ctx, cause);
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

    /** Reports whether this frame is handled on the EventLoop without claiming a worker. */
    protected abstract boolean handlesInline(ByteBuf frame);

    /** Parses a frame and takes responsibility for its reference. */
    protected abstract void handleFrame(ChannelHandlerContext ctx, ByteBuf frame);

    /** Handles an inbound failure using the protocol's existing failure response. */
    protected abstract void handleFailure(ChannelHandlerContext ctx, Throwable cause);

    /** Transfers a parsed RPC and its permit to the connection FIFO. */
    protected final void enqueue(RpcRequest request) {
        boolean full = requests.putRequest(connection, request);
        permit = false;
        if (full) {
            input.channel().config().setAutoRead(false);
        }
    }

    private boolean acquire(ByteBuf frame) {
        if (handlesInline(frame)) {
            return true;
        }
        permit = requests.acquireRead(connection);
        return permit;
    }

    private void dispatch(ByteBuf frame) {
        try {
            handleFrame(input, frame);
        } finally {
            if (permit) {
                permit = false;
                requests.returnRead(connection);
            }
        }
    }

    private void resumeReads() {
        if (inputClosed) {
            return;
        }
        while (!inputClosed && hasBufferedFrames()) {
            if (handlesInline(buffered.peekFirst())) {
                // The authentication state may have changed since this frame was buffered.
                requests.returnRead(connection);
            }
            if (!acquire(buffered.peekFirst())) {
                return;
            }
            dispatch(buffered.removeFirst());
        }
        if (inputClosed) {
            return;
        }
        if (framingFailure != null) {
            handleFailure(input, framingFailure);
        } else if (requests.canReadSocket(connection)) {
            input.channel().config().setAutoRead(true);
        }
    }

    private boolean hasBufferedFrames() {
        return buffered != null && !buffered.isEmpty();
    }

    private void closeInput() {
        if (inputClosed) {
            return;
        }
        inputClosed = true;
        requests.closeConnection(connection);
        if (buffered != null) {
            ByteBuf frame;
            while ((frame = buffered.pollFirst()) != null) {
                frame.release();
            }
        }
        framingFailure = null;
    }
}

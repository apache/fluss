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

import static org.apache.fluss.rpc.protocol.MessageCodec.REQUEST_HEADER_LENGTH;

/** Forwards complete frames together so one socket read does not allocate a queue entry per RPC. */
final class RequestFrameBatchDecoder extends ChannelInboundHandlerAdapter {
    private final int maxFrameLength;
    private final FairRequestFrameDecoder admission;
    private ByteBuf input;
    private boolean failed;

    RequestFrameBatchDecoder(int maxFrameLength, FairRequestFrameDecoder admission) {
        this.maxFrameLength = maxFrameLength;
        this.admission = admission;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        ByteBuf bytes = (ByteBuf) message;
        if (failed || !bytes.isReadable()) {
            bytes.release();
            return;
        }
        input =
                ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(
                        ctx.alloc(), input == null ? Unpooled.EMPTY_BUFFER : input, bytes);
        int start = input.readerIndex();
        int end = start;
        while (input.writerIndex() - end >= Integer.BYTES) {
            long length = input.getUnsignedInt(end);
            long frameLength = Integer.BYTES + length;
            if (frameLength > maxFrameLength || length < REQUEST_HEADER_LENGTH) {
                if (end > start) {
                    ctx.fireChannelRead(input.readRetainedSlice(end - start));
                }
                failed = true;
                releaseInput();
                admission.rejectAfterPending(
                        frameLength > maxFrameLength
                                ? new TooLongFrameException(
                                        "RPC frame length "
                                                + frameLength
                                                + " exceeds "
                                                + maxFrameLength)
                                : new CorruptedFrameException(
                                        "RPC frame is shorter than its header: " + length));
                return;
            }
            if (input.writerIndex() - end < frameLength) {
                break;
            }
            end += (int) frameLength;
        }
        if (end == input.writerIndex() && end > start) {
            ByteBuf complete = input;
            input = null;
            ctx.fireChannelRead(complete);
        } else if (end > start) {
            ctx.fireChannelRead(input.readRetainedSlice(end - start));
            if (input != null && input.refCnt() == 1) {
                input.discardSomeReadBytes();
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseInput();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        releaseInput();
    }

    private void releaseInput() {
        ReferenceCountUtil.release(input);
        input = null;
    }
}

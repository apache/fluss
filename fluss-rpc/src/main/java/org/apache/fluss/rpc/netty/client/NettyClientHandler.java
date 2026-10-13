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

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ErrorResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.ApiMethod;
import org.apache.fluss.rpc.protocol.ResponseType;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleState;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.ClosedChannelException;

import static org.apache.fluss.rpc.protocol.MessageCodec.RESPONSE_HEADER_LENGTH;
import static org.apache.fluss.rpc.protocol.MessageCodec.SERVER_FAILURE_HEADER_LENGTH;

/**
 * Implementation of the channel handler to process inbound requests for RPC client. The client
 * handler is not shared, there is a client handler instance for each channel connection.
 */
public final class NettyClientHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(NettyClientHandler.class);

    private final ClientHandlerCallback callback;

    public NettyClientHandler(ClientHandlerCallback callback) {
        this.callback = callback;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buffer = (ByteBuf) msg;
        boolean needRelease = true;
        try {
            int frameLength = buffer.readInt();
            ResponseType respType = ResponseType.forId(buffer.readByte());
            if (respType == ResponseType.SUCCESS_RESPONSE) {
                int requestId = buffer.readInt();
                int messageSize = frameLength - RESPONSE_HEADER_LENGTH;
                if (messageSize < 0) {
                    callback.onRequestFailure(
                            requestId,
                            new CorruptMessageException(
                                    "Invalid response frame length "
                                            + frameLength
                                            + " which is must greater than "
                                            + RESPONSE_HEADER_LENGTH));
                }
                ApiMethod apiMethod = callback.getRequestApiMethod(requestId);
                if (apiMethod == null) {
                    callback.onRequestFailure(
                            requestId,
                            new IllegalStateException(
                                    "Unknown request, this might be caused by the"
                                            + " request has been timeout."));
                    return;
                }
                ApiMessage response = apiMethod.getResponseConstructor().get();
                if (response.isLazilyParsed()) {
                    // Parse lazily from the original buffer without copying. The
                    // consumer is responsible for releasing the buffer via
                    // ApiMessage#getParsedByteBuf().release() after the response
                    // has been fully consumed.
                    response.parseFrom(buffer, messageSize);
                } else {
                    response.parseFrom(buffer, messageSize);
                    // eagerly release the buffer to make the buffer recycle faster
                    buffer.release();
                }
                needRelease = false;
                callback.onRequestResult(requestId, response);
            } else if (respType == ResponseType.ERROR_RESPONSE) {
                int requestId = buffer.readInt();
                int messageSize = frameLength - RESPONSE_HEADER_LENGTH;
                if (messageSize < 0) {
                    callback.onRequestFailure(
                            requestId,
                            new CorruptMessageException(
                                    "Invalid response frame length "
                                            + frameLength
                                            + " which is must greater than "
                                            + RESPONSE_HEADER_LENGTH));
                }
                ErrorResponse errorResponse = new ErrorResponse();
                errorResponse.parseFrom(buffer, messageSize);
                ApiError error = ApiError.fromErrorMessage(errorResponse);
                callback.onRequestFailure(requestId, error.exception());

            } else if (respType == ResponseType.SERVER_FAILURE) {
                int messageSize = frameLength - SERVER_FAILURE_HEADER_LENGTH;
                if (messageSize < 0) {
                    throw new CorruptMessageException(
                            "Invalid server failure frame length "
                                    + frameLength
                                    + " which is must greater than "
                                    + SERVER_FAILURE_HEADER_LENGTH);
                }
                ErrorResponse errorResponse = new ErrorResponse();
                errorResponse.parseFrom(buffer, messageSize);
                ApiError error = ApiError.fromErrorMessage(errorResponse);
                throw error.exception();

            } else {
                throw new IllegalStateException("Unexpected response type '" + respType + "'");
            }

        } catch (Throwable t1) {
            try {
                callback.onFailure(t1);
            } catch (Throwable t2) {
                LOG.error("Failed to notify callback about failure", t2);
            }
        } finally {
            if (needRelease) {
                buffer.release();
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // Only the client is expected to close the channel. Otherwise it
        // indicates a failure. Note that this will be invoked in both cases
        // though. If the callback closed the channel, the callback must be
        // ignored.
        try {
            callback.onFailure(new ClosedChannelException());
        } catch (Throwable t) {
            LOG.error("Failed to notify callback about failure", t);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state().equals(IdleState.ALL_IDLE)) {
                LOG.warn("Connection {} is idle, closing...", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        try {
            callback.onFailure(cause);
        } catch (Throwable t) {
            LOG.error("Failed to notify callback about failure", t);
        }
    }
}

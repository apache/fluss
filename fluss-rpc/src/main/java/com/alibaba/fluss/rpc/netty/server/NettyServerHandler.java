/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.rpc.protocol.ApiMethod;
import com.alibaba.fluss.rpc.protocol.MessageCodec;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelFutureListener;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandler;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.timeout.IdleState;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import com.alibaba.fluss.utils.ExceptionUtils;
import com.alibaba.fluss.utils.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeErrorResponse;
import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeServerFailure;

/** Implementation of the channel handler to process inbound requests for RPC server. */
@ChannelHandler.Sharable
public final class NettyServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(NettyServerHandler.class);

    private final RequestChannel[] requestChannels;
    private final int numChannels;
    private final ApiManager apiManager;

    public NettyServerHandler(RequestChannel[] requestChannels, ApiManager apiManager) {
        this.requestChannels = requestChannels;
        this.numChannels = requestChannels.length;
        this.apiManager = apiManager;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf buffer = (ByteBuf) msg;
        int frameLength = buffer.readInt();
        short apiKey = buffer.readShort();
        short apiVersion = buffer.readShort();
        int requestId = buffer.readInt();
        int messageSize = frameLength - MessageCodec.REQUEST_HEADER_LENGTH;

        boolean needRelease = false;
        try {
            ApiMethod api = apiManager.getApi(apiKey);
            if (api == null) {
                LOG.warn("Received unknown API key {}.", apiKey);
                needRelease = true;
                return;
            }

            ApiMessage requestMessage = api.getRequestConstructor().get();
            requestMessage.parseFrom(buffer, messageSize);
            // Most request types are parsed entirely into objects at this point. For those we can
            // release the underlying buffer.
            // However, some (like produce) retain a reference to the buffer. For those requests we
            // cannot release the buffer early, but only when request processing is done.
            if (!requestMessage.isLazilyParsed()) {
                needRelease = true;
            }

            RpcRequest request =
                    new RpcRequest(apiKey, apiVersion, requestId, api, requestMessage, buffer, ctx);
            // TODO: we can introduce a smarter and dynamic strategy to distribute requests to
            //  channels
            int channelIndex =
                    MathUtils.murmurHash(ctx.channel().id().asLongText().hashCode()) % numChannels;
            requestChannels[channelIndex].putRequest(request);
        } catch (Throwable t) {
            needRelease = true;
            LOG.error("Error while parsing request.", t);
            ApiError error = ApiError.fromThrowable(t);
            ctx.writeAndFlush(encodeErrorResponse(ctx.alloc(), requestId, error));
        } finally {
            if (needRelease) {
                buffer.release();
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        // TODO: connection metrics (count, client tags, receive request avg idle time, etc.)
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
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
        // debug level to avoid too many logs if NLB(Network Load Balancer is mounted, see
        // more detail in #377
        // may revert to warn level if we found warn level is necessary
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Connection [{}] got exception in Netty server pipeline: \n{}",
                    ctx.channel().remoteAddress(),
                    ExceptionUtils.stringifyException(cause));
        }
        ByteBuf byteBuf = encodeServerFailure(ctx.alloc(), ApiError.fromThrowable(cause));
        ctx.writeAndFlush(byteBuf).addListener(ChannelFutureListener.CLOSE);
    }
}

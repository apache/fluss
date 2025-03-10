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
import com.alibaba.fluss.rpc.protocol.ApiMethod;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * An RPC request message sent from client to server which encapsulating the API key, API version
 * and message.
 */
public final class RpcRequest {
    public static final RpcRequest SHUTDOWN_REQUEST =
            new RpcRequest((short) -1, (short) -1, -1, null);

    private final short apiKey;
    private final short apiVersion;
    private final int requestId;
    private final ApiMethod apiMethod;
    private final ApiMessage message;
    private final ByteBuf buffer;
    private final ChannelHandlerContext channelContext;

    // the time when the request is received by server
    private final long startTimeMs;

    public RpcRequest(
            short apiKey,
            short apiVersion,
            int requestId,
            ApiMethod apiMethod,
            ApiMessage message,
            ByteBuf buffer,
            ChannelHandlerContext channelContext) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.requestId = requestId;
        this.apiMethod = apiMethod;
        this.message = message;
        this.buffer = checkNotNull(buffer);
        this.channelContext = channelContext;
        this.startTimeMs = System.currentTimeMillis();
    }

    // only used for the SHUTDOWN_REQUEST instance.
    RpcRequest(short apiKey, short apiVersion, int requestId, ApiMethod apiMethod) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.requestId = requestId;
        this.apiMethod = apiMethod;
        this.message = null;
        this.buffer = null;
        this.channelContext = null;
        this.startTimeMs = System.currentTimeMillis();
    }

    public short getApiKey() {
        return apiKey;
    }

    public short getApiVersion() {
        return apiVersion;
    }

    public int getRequestId() {
        return requestId;
    }

    public ApiMethod getApiMethod() {
        return apiMethod;
    }

    public ApiMessage getMessage() {
        return message;
    }

    public void releaseBuffer() {
        if (message.isLazilyParsed()) {
            buffer.release();
        }
    }

    public ChannelHandlerContext getChannelContext() {
        return channelContext;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    @Override
    public String toString() {
        return "RpcRequest{"
                + "apiKey="
                + apiKey
                + ", apiVersion="
                + apiVersion
                + ", requestId="
                + requestId
                + '}';
    }
}

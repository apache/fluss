/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.protocol.ApiMethod;
import com.alibaba.fluss.rpc.protocol.RequestType;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/** Represents a request received from Fluss protocol channel. */
public final class FlussRequest implements RpcRequest {

    private final short apiKey;
    private final short apiVersion;
    private final int requestId;
    private final ApiMethod apiMethod;
    private final ApiMessage message;
    private final ByteBuf buffer;
    private final String listenerName;
    private final CompletableFuture<ApiMessage> responseFuture;

    // the time when the request is received by server
    private final long startTimeMs;
    private volatile long requestDequeTimeMs;
    private volatile long requestCompletedTimeMs;
    private volatile boolean cancelled = false;

    public FlussRequest(
            short apiKey,
            short apiVersion,
            int requestId,
            ApiMethod apiMethod,
            ApiMessage message,
            ByteBuf buffer,
            String listenerName,
            CompletableFuture<ApiMessage> responseFuture) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.requestId = requestId;
        this.apiMethod = apiMethod;
        this.message = message;
        this.buffer = checkNotNull(buffer);
        this.listenerName = listenerName;
        this.responseFuture = responseFuture;
        this.startTimeMs = System.currentTimeMillis();
    }

    @Override
    public RequestType getRequestType() {
        return RequestType.FLUSS;
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

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public String getListenerName() {
        return listenerName;
    }

    public CompletableFuture<ApiMessage> getResponseFuture() {
        return responseFuture;
    }

    public void complete(ApiMessage response) {
        responseFuture.complete(response);
    }

    public void fail(Throwable t) {
        responseFuture.completeExceptionally(t);
    }

    public void setRequestDequeTimeMs(long requestDequeTimeMs) {
        this.requestDequeTimeMs = requestDequeTimeMs;
    }

    public long getRequestDequeTimeMs() {
        return requestDequeTimeMs;
    }

    public void setRequestCompletedTimeMs(long requestCompletedTimeMs) {
        this.requestCompletedTimeMs = requestCompletedTimeMs;
    }

    public long getRequestCompletedTimeMs() {
        return requestCompletedTimeMs;
    }

    public void cancel() {
        cancelled = true;
    }

    public boolean cancelled() {
        return cancelled;
    }

    @Override
    public String toString() {
        return "FlussRequest{"
                + "apiKey="
                + apiKey
                + ", apiVersion="
                + apiVersion
                + ", requestId="
                + requestId
                + ", listenerName="
                + listenerName
                + '}';
    }
}

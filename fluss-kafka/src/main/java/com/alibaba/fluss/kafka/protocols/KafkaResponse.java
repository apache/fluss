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

package com.alibaba.fluss.kafka.protocols;

import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import java.util.concurrent.CompletableFuture;

public class KafkaResponse extends KafkaRequest {
    public final ResponseHeader responseHeader;
    public final CompletableFuture<AbstractResponse> future;

    public KafkaResponse(ApiKeys apiKey, short apiVersion, RequestHeader requestHeader, AbstractRequest request,
                         ByteBuf buffer, ChannelHandlerContext ctx, CompletableFuture<AbstractResponse> future) {
        super(apiKey, apiVersion, requestHeader, request, buffer, ctx, System.currentTimeMillis());
        this.responseHeader = requestHeader.toResponseHeader();
        this.future = future;
    }

    public AbstractResponse createResponse() {
        try {
            return future.get();
        } catch (Throwable t) {
            return request.getErrorResponse(t);
        }
    }
}

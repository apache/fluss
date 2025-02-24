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
import org.apache.kafka.common.requests.RequestHeader;

import java.util.concurrent.atomic.AtomicLong;

public class KafkaRequest {
    private final AtomicLong ID_GENERATOR = new AtomicLong(0);

    public final ApiKeys apiKey;
    public final short apiVersion;
    public final long requestId = ID_GENERATOR.getAndIncrement();
    public final RequestHeader header;
    public final AbstractRequest request;
    public final ByteBuf buffer;
    public final ChannelHandlerContext ctx;
    public final long startTimeMs;

    protected KafkaRequest(ApiKeys apiKey, short apiVersion, RequestHeader header, AbstractRequest request,
                           ByteBuf buffer, ChannelHandlerContext ctx, long startTimeMs) {
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.header = header;
        this.request = request;
        this.buffer = buffer;
        this.ctx = ctx;
        this.startTimeMs = startTimeMs;
    }
}

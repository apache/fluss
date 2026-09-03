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

package org.apache.fluss.kafka;

import org.apache.fluss.rpc.netty.NettyChannelInitializer;
import org.apache.fluss.rpc.netty.server.RequestChannel;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;
import org.apache.fluss.shaded.netty4.io.netty.handler.flow.FlowControlHandler;

import javax.annotation.Nullable;

import java.util.function.Supplier;

/**
 * A {@link ChannelInitializer} for initializing {@link SocketChannel} instances that will be used
 * by the server to handle the Kafka requests for the client.
 */
public class KafkaChannelInitializer extends NettyChannelInitializer {

    private final RequestChannel[] requestChannels;
    private final String listenerName;
    private final int maxRequestSize;
    private final @Nullable Supplier<ServerAuthenticator> authenticatorSupplier;
    private final LengthFieldPrepender prepender = new LengthFieldPrepender(4);
    private final boolean preferHeap;

    /** Creates a PLAINTEXT channel initializer. */
    public KafkaChannelInitializer(
            RequestChannel[] requestChannels,
            String listenerName,
            long maxIdleTimeSeconds,
            int maxRequestSize,
            boolean preferHeap) {
        this(requestChannels, listenerName, maxIdleTimeSeconds, maxRequestSize, preferHeap, null);
    }

    /** Creates a channel initializer with an optional per-connection authenticator supplier. */
    public KafkaChannelInitializer(
            RequestChannel[] requestChannels,
            String listenerName,
            long maxIdleTimeSeconds,
            int maxRequestSize,
            boolean preferHeap,
            @Nullable Supplier<ServerAuthenticator> authenticatorSupplier) {
        super(maxIdleTimeSeconds);
        this.requestChannels = requestChannels;
        this.listenerName = listenerName;
        this.maxRequestSize = maxRequestSize;
        this.preferHeap = preferHeap;
        this.authenticatorSupplier = authenticatorSupplier;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        super.initChannel(ch);
        // NettyLogger dumps full buffers at TRACE. A SASL/PLAIN frame contains the clear-text
        // credential token, so authenticated listeners must never install the payload logger.
        if (authenticatorSupplier != null && ch.pipeline().get("loggingHandler") != null) {
            ch.pipeline().remove("loggingHandler");
        }
        addIdleStateHandler(ch);
        ch.pipeline().addLast(prepender);
        addFrameDecoder(ch, maxRequestSize, 4, preferHeap);
        ch.pipeline().addLast("flowController", new FlowControlHandler());
        ch.pipeline()
                .addLast(
                        new KafkaCommandDecoder(
                                requestChannels, listenerName, authenticatorSupplier));
    }
}

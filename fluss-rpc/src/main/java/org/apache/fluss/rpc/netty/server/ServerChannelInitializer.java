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

import org.apache.fluss.rpc.netty.NettyChannelInitializer;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.security.auth.ServerAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.utils.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * A specialized {@link ChannelInitializer} for initializing {@link SocketChannel} instances that
 * will be used by the server to handle the init request for the client.
 */
final class ServerChannelInitializer extends NettyChannelInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(ServerChannelInitializer.class);

    private final RequestChannel[] requestChannels;
    private final ApiManager apiManager;
    private final String endpointListenerName;
    private final boolean isInternal;
    private final RequestsMetrics requestsMetrics;
    private final Supplier<ServerAuthenticator> authenticatorSupplier;

    public ServerChannelInitializer(
            RequestChannel[] requestChannels,
            ApiManager apiManager,
            String endpointListenerName,
            boolean isInternal,
            RequestsMetrics requestsMetrics,
            long maxIdleTimeSeconds,
            Supplier<ServerAuthenticator> authenticatorSupplier) {
        super(maxIdleTimeSeconds);
        this.requestChannels = requestChannels;
        this.apiManager = apiManager;
        this.endpointListenerName = endpointListenerName;
        this.isInternal = isInternal;
        this.requestsMetrics = requestsMetrics;
        this.authenticatorSupplier = authenticatorSupplier;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        super.initChannel(ch);
        // initialBytesToStrip=0 to include the frame size field after decoding
        addFrameDecoder(ch, Integer.MAX_VALUE, 0);
        addIdleStateHandler(ch);
        ServerAuthenticator serverAuthenticator = authenticatorSupplier.get();
        LOG.debug(
                "initial a channel for listener {} with protocol {}",
                endpointListenerName,
                serverAuthenticator.protocol());
        // TODO: we can introduce a smarter and dynamic strategy to distribute requests to channels
        ch.pipeline()
                .addLast(
                        "handler",
                        new NettyServerHandler(
                                requestChannels[
                                        MathUtils.murmurHash(ch.id().hashCode())
                                                % requestChannels.length],
                                apiManager,
                                endpointListenerName,
                                isInternal,
                                requestsMetrics,
                                serverAuthenticator));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Unexpected exception caught in server channel initializer.", cause);
        super.exceptionCaught(ctx, cause);
    }
}

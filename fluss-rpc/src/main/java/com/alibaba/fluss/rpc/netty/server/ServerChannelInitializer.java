/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
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

import com.alibaba.fluss.rpc.netty.NettyLogger;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.security.auth.ServerAuthenticator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;
import com.alibaba.fluss.utils.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * A specialized {@link ChannelInitializer} for initializing {@link SocketChannel} instances that
 * will be used by the server to handle the init request for the client.
 */
final class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private static final Logger LOG = LoggerFactory.getLogger(ServerChannelInitializer.class);

    private final int maxIdleTimeSeconds;
    private final RequestChannel[] requestChannels;
    private final ApiManager apiManager;
    private final String endpointListenerName;
    private final RequestsMetrics requestsMetrics;
    private final Supplier<ServerAuthenticator> authenticatorSupplier;

    private static final NettyLogger nettyLogger = new NettyLogger();

    public ServerChannelInitializer(
            RequestChannel[] requestChannels,
            ApiManager apiManager,
            String endpointListenerName,
            RequestsMetrics requestsMetrics,
            long maxIdleTimeSeconds,
            Supplier<ServerAuthenticator> authenticatorSupplier) {
        checkArgument(maxIdleTimeSeconds <= Integer.MAX_VALUE, "maxIdleTimeSeconds too large");
        this.requestChannels = requestChannels;
        this.apiManager = apiManager;
        this.endpointListenerName = endpointListenerName;
        this.requestsMetrics = requestsMetrics;
        this.maxIdleTimeSeconds = (int) maxIdleTimeSeconds;
        this.authenticatorSupplier = authenticatorSupplier;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        if (nettyLogger.getLoggingHandler() != null) {
            ch.pipeline().addLast("loggingHandler", nettyLogger.getLoggingHandler());
        }

        // TODO: we can introduce a smarter and dynamic strategy to distribute requests to
        //  channels
        int channelIndex = MathUtils.murmurHash(ch.id().hashCode()) % requestChannels.length;

        ch.pipeline()
                .addLast(
                        "frameDecoder",
                        // initialBytesToStrip=0 to include the frame size field after decoding
                        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 0));
        ch.pipeline().addLast("idle", new IdleStateHandler(0, 0, maxIdleTimeSeconds));
        ServerAuthenticator serverAuthenticator = authenticatorSupplier.get();
        LOG.debug(
                "initial a channel for listener {} with protocol {}",
                endpointListenerName,
                serverAuthenticator.protocol());

        ch.pipeline()
                .addLast(
                        "handler",
                        new NettyServerHandler(
                                requestChannels[channelIndex],
                                apiManager,
                                endpointListenerName,
                                requestsMetrics,
                                serverAuthenticator));
    }
}

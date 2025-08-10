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

package com.alibaba.fluss.rpc.netty;

import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * A basic {@link ChannelInitializer} for initializing {@link SocketChannel} instances to support
 * netty logging and add common handlers.
 */
public class NettyChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final int maxIdleTimeSeconds;

    private static final NettyLogger nettyLogger = new NettyLogger();

    public NettyChannelInitializer(long maxIdleTimeSeconds) {
        checkArgument(maxIdleTimeSeconds <= Integer.MAX_VALUE, "maxIdleTimeSeconds too large");
        this.maxIdleTimeSeconds = (int) maxIdleTimeSeconds;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (nettyLogger.getLoggingHandler() != null) {
            ch.pipeline().addLast("loggingHandler", nettyLogger.getLoggingHandler());
        }
    }

    public void addFrameDecoder(SocketChannel ch, int maxFrameLength, int initialBytesToStrip) {
        ch.pipeline()
                .addLast(
                        "frameDecoder",
                        new LengthFieldBasedFrameDecoder(
                                maxFrameLength, 0, 4, 0, initialBytesToStrip));
    }

    public void addIdleStateHandler(SocketChannel ch) {
        ch.pipeline().addLast("idle", new IdleStateHandler(0, 0, maxIdleTimeSeconds));
    }
}

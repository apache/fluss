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

package com.alibaba.fluss.fs.gs;

import com.alibaba.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelPipeline;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.logging.LogLevel;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.logging.LoggingHandler;

import java.io.Closeable;

/** Mock Netty Auth Server for facilitating the Google auth token generation. */
public class MockAuthServer implements Closeable {

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private ChannelFuture channelFuture;

    MockAuthServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.channelFuture = run();
    }

    public ChannelFuture run() {
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) {
                                    ChannelPipeline p = ch.pipeline();
                                    p.addLast(new HttpServerCodec());
                                    p.addLast(new HttpServerExpectContinueHandler());
                                    p.addLast(new AuthServerHandler());
                                }
                            });

            channelFuture = b.bind(8080).sync();
            return channelFuture;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static MockAuthServer create() {
        return new MockAuthServer(new NioEventLoopGroup(1), new NioEventLoopGroup());
    }

    @Override
    public void close() {
        try {
            bossGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}

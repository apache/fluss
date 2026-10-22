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

package org.apache.fluss.fs.gs;

import org.apache.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.apache.fluss.shaded.netty4.io.netty.handler.logging.LoggingHandler;

import java.io.Closeable;
import java.net.InetSocketAddress;

/** Mock Netty Auth Server for facilitating the Google auth token generation. */
public class MockAuthServer implements Closeable {

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private ChannelFuture channelFuture;

    private final int port;

    MockAuthServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.channelFuture = run();
        this.port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
    }

    /** Port the mock token server is bound to on {@code 127.0.0.1}. */
    public int getPort() {
        return port;
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

            // Bind the loopback interface on an ephemeral port. A fixed port 8080 collides on
            // shared CI runners, and "localhost" may resolve to IPv6 while this channel is IPv4.
            channelFuture = b.bind(new InetSocketAddress("127.0.0.1", 0)).sync();
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

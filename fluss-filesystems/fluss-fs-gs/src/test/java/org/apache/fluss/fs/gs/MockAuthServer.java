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
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.fluss.shaded.netty4.io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import org.apache.fluss.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.apache.fluss.shaded.netty4.io.netty.handler.logging.LoggingHandler;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Mock Netty Auth Server for facilitating the Google auth token generation. */
public class MockAuthServer implements Closeable {

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private ChannelFuture channelFuture;

    private final int port;

    /**
     * Scopes seen on token requests, in arrival order. An empty string means the scope was absent.
     */
    private final List<String> requestedScopes = new CopyOnWriteArrayList<>();

    /** JWT {@code iss} values for the same token requests. Empty for Compute Engine metadata. */
    private final List<String> requestedIssuers = new CopyOnWriteArrayList<>();

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

    /** Drops recorded token requests so a test can assert only the calls it makes. */
    void clearRecordedTokenRequests() {
        requestedScopes.clear();
        requestedIssuers.clear();
    }

    /** Scopes of token requests recorded since the last {@link #clearRecordedTokenRequests()}. */
    List<String> requestedScopes() {
        return Collections.unmodifiableList(new ArrayList<>(requestedScopes));
    }

    /**
     * Service-account emails ({@code iss}) of token requests recorded since the last {@link
     * #clearRecordedTokenRequests()}. Compute Engine requests contribute an empty string.
     */
    List<String> requestedIssuers() {
        return Collections.unmodifiableList(new ArrayList<>(requestedIssuers));
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
                                    // Aggregate the body so the JWT assertion, which carries the
                                    // requested scope, is visible to the handler.
                                    p.addLast(new HttpObjectAggregator(1024 * 1024));
                                    p.addLast(
                                            new AuthServerHandler(
                                                    requestedScopes, requestedIssuers));
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

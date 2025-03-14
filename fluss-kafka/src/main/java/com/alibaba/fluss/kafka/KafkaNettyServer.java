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

package com.alibaba.fluss.kafka;

import com.alibaba.fluss.cluster.MetadataCache;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.RpcServer;
import com.alibaba.fluss.rpc.gateway.AdminGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.netty.NettyUtils;
import com.alibaba.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.AdaptiveRecvByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.Channel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelInitializer;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.socket.SocketChannel;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.codec.LengthFieldPrepender;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.flow.FlowControlHandler;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.flush.FlushConsolidationHandler;
import com.alibaba.fluss.shaded.netty4.io.netty.handler.timeout.IdleStateHandler;
import com.alibaba.fluss.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.fluss.rpc.netty.NettyUtils.shutdownChannel;
import static com.alibaba.fluss.utils.Preconditions.checkState;

public final class KafkaNettyServer implements RpcServer {
    private static final Logger log = LoggerFactory.getLogger(KafkaNettyServer.class);

    private final String hostname;
    private final int port;
    private final int serverId;
    private final Configuration conf;
    private final AdminGateway admin;
    private final MetadataCache metadataCache;
    private final TabletServerGateway tablet;

    private volatile boolean ready = false;
    private volatile EventLoopGroup acceptor;
    private volatile EventLoopGroup worker;
    private volatile Channel serverChannel;
    private volatile InetSocketAddress localAddress;

    private KafkaNettyServer(
            String hostname,
            int port,
            int serverId,
            Configuration conf,
            AdminGateway admin,
            MetadataCache metadataCache,
            TabletServerGateway tablet) {
        this.hostname = hostname;
        this.port = port;
        this.serverId = serverId;
        this.conf = conf;
        this.admin = admin;
        this.metadataCache = metadataCache;
        this.tablet = tablet;
    }

    public static KafkaNettyServer create(
            String hostname,
            int port,
            int serverId,
            Configuration conf,
            AdminGateway admin,
            MetadataCache metadataCache,
            TabletServerGateway tablet) {
        return new KafkaNettyServer(hostname, port, serverId, conf, admin, metadataCache, tablet);
    }

    @Override
    public void start() throws IOException {
        throw new UnsupportedOperationException("KafkaNettyServer does not support this method.");
    }

    @Override
    public void start(EventLoopGroup acceptor, EventLoopGroup worker) throws IOException {
        if (ready) {
            return;
        }

        this.acceptor = acceptor;
        this.worker = worker;

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.group(acceptor, worker);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(
                ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1024 * 1024));
        bootstrap.channel(NettyUtils.getServerSocketChannelClass(acceptor));

        // child channel pipeline for accepted connections
        bootstrap.childHandler(new KafkaChannelInitializer());

        try {
            if (StringUtils.isNullOrWhitespaceOnly(hostname)) {
                this.serverChannel = bootstrap.bind(port).syncUninterruptibly().channel();
            } else {
                this.serverChannel = bootstrap.bind(hostname, port).syncUninterruptibly().channel();
            }
            this.localAddress = (InetSocketAddress) serverChannel.localAddress();
            log.info("Kafka server started at {}", localAddress);
            ready = true;
        } catch (Throwable t) {
            log.error("Failed to start Kafka server", t);
            throw t;
        }
    }

    @Override
    public String getHostname() {
        checkState(ready, "Netty server has not been started yet.");
        return this.hostname;
    }

    @Override
    public int getPort() {
        checkState(ready, "Netty server has not been started yet.");
        return this.port;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!ready) {
            return CompletableFuture.completedFuture(null);
        }

        ready = false;

        return shutdownChannel(serverChannel);
    }

    @Override
    public ScheduledExecutorService getScheduledExecutor() {
        checkState(ready, "Netty server has not been started yet.");
        return this.worker;
    }

    /** Channel initializer for Kafka server. */
    class KafkaChannelInitializer extends ChannelInitializer<SocketChannel> {
        public static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024; // 100MB

        private final LengthFieldPrepender prepender = new LengthFieldPrepender(4);

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast("flush", new FlushConsolidationHandler(1024, true));
            ch.pipeline()
                    .addLast("idleStateHandler", new IdleStateHandler(0, 0, 60, TimeUnit.SECONDS));
            ch.pipeline().addLast(prepender);
            ch.pipeline()
                    .addLast(
                            "frameDecoder",
                            new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
            ch.pipeline().addLast("flowController", new FlowControlHandler());
            ch.pipeline()
                    .addLast(
                            new KafkaRequestHandler(
                                    hostname, port, serverId, conf, admin, metadataCache, tablet));
        }
    }
}

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

package com.alibaba.fluss.rpc.netty.server;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.rpc.RpcGateway;
import com.alibaba.fluss.rpc.RpcGatewayService;
import com.alibaba.fluss.rpc.RpcServer;
import com.alibaba.fluss.rpc.netty.NettyMetrics;
import com.alibaba.fluss.rpc.netty.NettyUtils;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.AdaptiveRecvByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.Channel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import com.alibaba.fluss.utils.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static com.alibaba.fluss.rpc.netty.NettyUtils.isBindFailure;
import static com.alibaba.fluss.rpc.netty.NettyUtils.shutdownChannel;
import static com.alibaba.fluss.rpc.netty.NettyUtils.shutdownGroup;
import static com.alibaba.fluss.utils.Preconditions.checkNotNull;
import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * Netty based {@link RpcServer} implementation. The RPC server starts a handler to receive RPC
 * invocations from a {@link RpcGateway}.
 */
public final class NettyServer implements RpcServer {

    private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

    private final Configuration conf;
    private final String hostname;
    private final String portRange;
    private final RequestProcessorPool workerPool;
    private final ApiManager apiManager;
    private final MetricGroup serverMetricGroup;

    private EventLoopGroup acceptorGroup;
    private EventLoopGroup selectorGroup;
    private InetSocketAddress bindAddress;
    private Channel bindChannel;

    private volatile boolean isRunning;

    public NettyServer(
            Configuration conf,
            String hostname,
            String portRange,
            RpcGatewayService service,
            MetricGroup serverMetricGroup,
            RequestsMetrics requestsMetrics) {
        this.conf = checkNotNull(conf, "conf");
        this.hostname = checkNotNull(hostname, "hostname");
        this.portRange = checkNotNull(portRange, "portRange");
        this.serverMetricGroup = checkNotNull(serverMetricGroup, "serverMetricGroup");
        this.apiManager = new ApiManager(service.providerType());

        this.workerPool =
                new RequestProcessorPool(
                        conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS),
                        conf.getInt(ConfigOptions.NETTY_SERVER_MAX_QUEUED_REQUESTS),
                        service,
                        requestsMetrics);
    }

    @Override
    public void start() throws IOException {
        checkState(bindChannel == null, "Netty server has already been initialized.");
        int numNetworkThreads = conf.getInt(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS);
        int numWorkerThreads = conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS);
        LOG.info(
                "Starting Netty server on address {} and port range {} with {} network threads and {} worker threads.",
                hostname,
                portRange,
                numNetworkThreads,
                numWorkerThreads);

        final long start = System.nanoTime();

        this.acceptorGroup =
                NettyUtils.newEventLoopGroup(
                        1, // always use single thread for acceptor
                        "fluss-netty-server-acceptor");
        this.selectorGroup =
                NettyUtils.newEventLoopGroup(numNetworkThreads, "fluss-netty-server-selector");

        ServerBootstrap bootstrap = new ServerBootstrap();
        PooledByteBufAllocator pooledBufAllocator = PooledByteBufAllocator.DEFAULT;
        bootstrap.childOption(ChannelOption.ALLOCATOR, pooledBufAllocator);
        bootstrap.group(acceptorGroup, selectorGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(
                ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1024 * 1024));
        bootstrap.channel(NettyUtils.getServerSocketChannelClass(selectorGroup));

        // child channel pipeline for accepted connections
        bootstrap.childHandler(
                new ServerChannelInitializer(
                        new NettyServerHandler(workerPool.getRequestChannels(), apiManager),
                        conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds()));

        // --------------------------------------------------------------------
        // Start Server
        // --------------------------------------------------------------------
        LOG.debug(
                "Trying to start Netty server on address: {} and port range {}",
                hostname,
                portRange);

        Iterator<Integer> portsIterator = NetUtils.getPortRangeFromString(portRange);
        while (portsIterator.hasNext() && bindChannel == null) {
            Integer port = portsIterator.next();
            LOG.debug("Trying to bind Netty server to port: {}", port);

            bootstrap.localAddress(hostname, port);
            try {
                bindChannel = bootstrap.bind().syncUninterruptibly().channel();
            } catch (Exception e) {
                // syncUninterruptibly() throws checked exceptions via Unsafe
                // continue if the exception is due to the port being in use, fail early
                // otherwise
                if (isBindFailure(e)) {
                    LOG.debug("Failed to bind Netty server on port {}: {}", port, e.getMessage());
                } else {
                    throw e;
                }
            }
        }

        if (bindChannel == null) {
            throw new BindException(
                    "Could not start Netty server on any port in port range " + portRange);
        }

        bindAddress = (InetSocketAddress) bindChannel.localAddress();

        // setup worker thread pool
        workerPool.start();

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info(
                "Successfully start Netty server (took {} ms). Listening on SocketAddress {}.",
                duration,
                bindAddress);

        isRunning = true;
        NettyMetrics.registerNettyMetrics(serverMetricGroup, pooledBufAllocator);
    }

    @Override
    public String getHostname() {
        checkState(isRunning, "Netty server has not been started yet.");
        return bindAddress.getAddress().getHostAddress();
    }

    @Override
    public int getPort() {
        checkState(isRunning, "Netty server has not been started yet.");
        return bindAddress.getPort();
    }

    @Override
    public ScheduledExecutorService getScheduledExecutor() {
        checkState(isRunning, "Netty server has not been started yet.");
        return selectorGroup;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (!isRunning) {
            return CompletableFuture.completedFuture(null);
        }

        isRunning = false;

        CompletableFuture<Void> acceptorShutdownFuture = shutdownGroup(acceptorGroup);
        CompletableFuture<Void> selectorShutdownFuture = shutdownGroup(selectorGroup);
        CompletableFuture<Void> channelShutdownFuture = shutdownChannel(bindChannel);
        CompletableFuture<Void> workerShutdownFuture;
        if (workerPool != null) {
            workerShutdownFuture = workerPool.closeAsync();
        } else {
            workerShutdownFuture = CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.allOf(
                acceptorShutdownFuture,
                selectorShutdownFuture,
                channelShutdownFuture,
                workerShutdownFuture);
    }
}

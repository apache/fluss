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

import com.alibaba.fluss.cluster.Endpoint;
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
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

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
    private final Collection<Endpoint> endpoints;
    private final RequestProcessorPool workerPool;
    private final ApiManager apiManager;
    private final MetricGroup serverMetricGroup;
    private final List<Channel> bindChannels;
    private final List<Endpoint> bindEndpoints;

    private EventLoopGroup acceptorGroup;
    private EventLoopGroup selectorGroup;

    private volatile boolean isRunning;

    public NettyServer(
            Configuration conf,
            Collection<Endpoint> endpoints,
            RpcGatewayService service,
            MetricGroup serverMetricGroup,
            RequestsMetrics requestsMetrics) {
        this.conf = checkNotNull(conf, "conf");
        this.serverMetricGroup = checkNotNull(serverMetricGroup, "serverMetricGroup");
        this.apiManager = new ApiManager(service.providerType());
        this.endpoints = checkNotNull(endpoints, "endpoints");

        this.workerPool =
                new RequestProcessorPool(
                        conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS),
                        conf.getInt(ConfigOptions.NETTY_SERVER_MAX_QUEUED_REQUESTS),
                        service,
                        requestsMetrics);
        this.bindChannels = new CopyOnWriteArrayList<>();
        this.bindEndpoints = new CopyOnWriteArrayList<>();
    }

    @Override
    public void start() throws IOException {
        checkState(bindChannels.isEmpty(), "Netty server has already been initialized.");
        int numNetworkThreads = conf.getInt(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS);
        int numWorkerThreads = conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS);
        LOG.info(
                "Starting Netty server on endpoints {} with {} network threads and {} worker threads.",
                endpoints,
                numNetworkThreads,
                numWorkerThreads);

        final long start = System.nanoTime();

        this.acceptorGroup =
                NettyUtils.newEventLoopGroup(
                        1, // always use single thread for acceptor
                        "fluss-netty-server-acceptor");
        this.selectorGroup =
                NettyUtils.newEventLoopGroup(numNetworkThreads, "fluss-netty-server-selector");
        PooledByteBufAllocator pooledBufAllocator = PooledByteBufAllocator.DEFAULT;

        // setup worker thread pool
        workerPool.start();
        for (Endpoint endpoint : endpoints) {
            startEndpoint(endpoint);
        }

        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info(
                "Successfully start Netty server (took {} ms). Listening on endpoints {}.",
                duration,
                endpoints);
        isRunning = true;
        NettyMetrics.registerNettyMetrics(serverMetricGroup, pooledBufAllocator);
    }

    @Override
    public List<Endpoint> getBindEndpoints() {
        checkState(isRunning, "Netty server has not been started yet.");
        return bindEndpoints;
    }

    private void startEndpoint(Endpoint endpoint) throws IOException {

        ServerBootstrap bootstrap = new ServerBootstrap();

        bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.group(acceptorGroup, selectorGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(
                ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1024 * 1024));
        bootstrap.channel(NettyUtils.getServerSocketChannelClass(selectorGroup));

        // child channel pipeline for accepted connections
        bootstrap.childHandler(
                new ServerChannelInitializer(
                        new NettyServerHandler(
                                workerPool.getRequestChannels(),
                                apiManager,
                                endpoint.getListenerName()),
                        conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds()));

        // --------------------------------------------------------------------
        // Start Server
        // --------------------------------------------------------------------
        String hostname = endpoint.getHost();
        int port = endpoint.getPort();
        LOG.debug("Trying to start Netty server on address: {} and port {}", hostname, port);

        try {
            bootstrap.localAddress(hostname, port);
            Channel bindChannel = bootstrap.bind().syncUninterruptibly().channel();
            if (bindChannel == null) {
                throw new BindException(
                        String.format(
                                "Could not start Netty server on address: %s and port %s ",
                                hostname, port));
            }
            LOG.info("Listening on address {} and port {}", hostname, port);
            bindChannels.add(bindChannel);
            InetSocketAddress bindAddress = (InetSocketAddress) bindChannel.localAddress();
            bindEndpoints.add(
                    new Endpoint(
                            bindAddress.getAddress().getHostAddress(),
                            bindAddress.getPort(),
                            endpoint.getListenerName()));
        } catch (Exception e) {
            // syncUninterruptibly() throws checked exceptions via Unsafe
            // continue if the exception is due to the port being in use, fail early
            throw new IOException("Failed to start Netty server on endpoint " + endpoint, e);
        }
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
        CompletableFuture<Void> channelShutdownFuture =
                FutureUtils.completeAll(
                        bindChannels.stream()
                                .map(NettyUtils::shutdownChannel)
                                .collect(Collectors.toList()));
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

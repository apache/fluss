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

package com.alibaba.fluss.rpc.netty.client;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.rpc.netty.NettyMetrics;
import com.alibaba.fluss.rpc.netty.NettyUtils;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.security.auth.AuthenticationFactory;
import com.alibaba.fluss.security.auth.ClientAuthenticator;
import com.alibaba.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelOption;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;
import com.alibaba.fluss.utils.MapUtils;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to
 * implement the user-facing reader and writer.
 */
@ThreadSafe
public final class NettyClient implements RpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyClient.class);

    /** Netty's Bootstrap. */
    private final Bootstrap bootstrap;

    /** Netty's event group. */
    private final EventLoopGroup eventGroup;

    /**
     * Managed connections to Netty servers. The key is the server uid (e.g., "cs-2", "ts-3"), the
     * value is the connection.
     */
    private final Map<String, ServerConnection> connections;

    /** Metric groups for client. */
    private final ClientMetricGroup clientMetricGroup;

    private final Supplier<ClientAuthenticator> authenticatorSupplier;

    private volatile boolean isClosed = false;

    public NettyClient(Configuration conf, ClientMetricGroup clientMetricGroup) {
        this.connections = MapUtils.newConcurrentHashMap();

        // build bootstrap
        this.eventGroup =
                NettyUtils.newEventLoopGroup(
                        conf.getInt(ConfigOptions.NETTY_CLIENT_NUM_NETWORK_THREADS),
                        "fluss-netty-client");
        int connectTimeoutMs = (int) conf.get(ConfigOptions.CLIENT_CONNECT_TIMEOUT).toMillis();
        int connectionMaxIdle =
                (int) conf.get(ConfigOptions.NETTY_CONNECTION_MAX_IDLE_TIME).getSeconds();
        PooledByteBufAllocator pooledAllocator = PooledByteBufAllocator.DEFAULT;
        this.bootstrap =
                new Bootstrap()
                        .group(eventGroup)
                        .channel(NettyUtils.getClientSocketChannelClass(eventGroup))
                        .option(ChannelOption.ALLOCATOR, pooledAllocator)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .handler(new ClientChannelInitializer(connectionMaxIdle));
        this.clientMetricGroup = clientMetricGroup;
        this.authenticatorSupplier = AuthenticationFactory.loadClientAuthenticatorSupplier(conf);
        NettyMetrics.registerNettyMetrics(clientMetricGroup, pooledAllocator);
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send
     * to that node.
     *
     * @param node The server node to check
     * @return True if we are ready to send to the given node.
     */
    @Override
    public boolean connect(ServerNode node) {
        checkArgument(!isClosed, "Netty client is closed.");
        return getOrCreateConnection(node).isReady();
    }

    /**
     * Disconnects the connection to the given server node, if there is one. Any inflight/pending
     * requests for this connection will receive disconnections.
     *
     * @param serverUid The uid of the server node
     * @return A future that completes when the connection is fully closed
     */
    @Override
    public CompletableFuture<Void> disconnect(String serverUid) {
        LOG.debug("Disconnecting from server {}.", serverUid);
        checkArgument(!isClosed, "Netty client is closed.");
        ServerConnection connection = connections.remove(serverUid);
        if (connection != null) {
            return connection.close();
        }
        return FutureUtils.completedVoidFuture();
    }

    /**
     * Check if we are currently ready to send another request to the given server but don't attempt
     * to connect if we aren't.
     *
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(String serverUid) {
        checkArgument(!isClosed, "Netty client is closed.");
        ServerConnection connection = connections.get(serverUid);
        if (connection == null) {
            return false;
        }
        return connection.isReady();
    }

    /** Send an RPC request to the given server and return a future for the response. */
    @Override
    public CompletableFuture<ApiMessage> sendRequest(
            ServerNode node, ApiKeys apiKey, ApiMessage request) {
        checkArgument(!isClosed, "Netty client is closed.");
        return getOrCreateConnection(node).send(apiKey, request);
    }

    @Override
    public void close() throws Exception {
        try {
            isClosed = true;
            final List<CompletableFuture<Void>> shutdownFutures = new ArrayList<>();
            for (Map.Entry<String, ServerConnection> conn : connections.entrySet()) {
                if (connections.remove(conn.getKey(), conn.getValue())) {
                    shutdownFutures.add(conn.getValue().close());
                }
            }
            shutdownFutures.add(NettyUtils.shutdownGroup(eventGroup));
            CompletableFuture.allOf(shutdownFutures.toArray(new CompletableFuture<?>[0]))
                    .get(10, TimeUnit.SECONDS);
            LOG.info("Netty client was shutdown successfully.");
        } catch (Exception e) {
            LOG.warn("Netty client shutdown failed: ", e);
        }
    }

    private ServerConnection getOrCreateConnection(ServerNode node) {
        String serverId = node.uid();
        return connections.computeIfAbsent(
                serverId,
                ignored -> {
                    LOG.debug("Creating connection to server {}.", node);
                    ServerConnection connection =
                            new ServerConnection(
                                    bootstrap,
                                    node,
                                    clientMetricGroup,
                                    authenticatorSupplier.get());
                    connection.whenClose(ignore -> connections.remove(serverId, connection));
                    return connection;
                });
    }

    @VisibleForTesting
    Map<String, ServerConnection> connections() {
        return connections;
    }
}

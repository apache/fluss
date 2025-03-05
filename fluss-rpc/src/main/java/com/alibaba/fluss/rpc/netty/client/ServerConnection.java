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

package com.alibaba.fluss.rpc.netty.client;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.exception.DisconnectException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.NetworkException;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.rpc.metrics.ConnectionMetricGroup;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.ApiManager;
import com.alibaba.fluss.rpc.protocol.ApiMethod;
import com.alibaba.fluss.rpc.protocol.MessageCodec;
import com.alibaba.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.Channel;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelFutureListener;
import com.alibaba.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** Connection to a Netty server used by the {@link NettyClient}. */
@ThreadSafe
final class ServerConnection {
    private static final Logger LOG = LoggerFactory.getLogger(ServerConnection.class);

    private final ServerNode node;

    // TODO: add max inflight requests limit like Kafka's "max.in.flight.requests.per.connection"
    private final Map<Integer, InflightRequest> inflightRequests = MapUtils.newConcurrentHashMap();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final ConnectionMetricGroup connectionMetricGroup;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private final ArrayDeque<PendingRequest> pendingRequests = new ArrayDeque<>();

    @GuardedBy("lock")
    private Channel channel;

    /** Current request number used to assign unique request IDs. */
    @GuardedBy("lock")
    private int requestCount = 0;

    /** server connection state. */
    @GuardedBy("lock")
    private ConnectionState state;

    /** The api versions supported by the remote server. */
    @GuardedBy("lock")
    private ServerApiVersions serverApiVersions;

    ServerConnection(Bootstrap bootstrap, ServerNode node, ClientMetricGroup clientMetricGroup) {
        this.node = node;
        this.state = ConnectionState.CONNECTING;
        bootstrap
                .connect(node.host(), node.port())
                .addListener((ChannelFutureListener) this::establishConnection);
        this.connectionMetricGroup = clientMetricGroup.createConnectionMetricGroup(node.uid());
    }

    public ServerNode getServerNode() {
        return node;
    }

    public boolean isReady() {
        synchronized (lock) {
            return state == ConnectionState.READY && channel != null && channel.isActive();
        }
    }

    /** Send an RPC request to the server and return a future for the response. */
    public CompletableFuture<ApiMessage> send(ApiKeys apikey, ApiMessage request) {
        return doSend(apikey, request, new CompletableFuture<>(), false);
    }

    /** Register a callback to be called when the connection is closed. */
    public void whenClose(Consumer<Throwable> closeCallback) {
        closeFuture.whenComplete((v, throwable) -> closeCallback.accept(throwable));
    }

    /** Close the connection. */
    public CompletableFuture<Void> close() {
        return close(new ClosedChannelException());
    }

    private CompletableFuture<Void> close(Throwable cause) {
        synchronized (lock) {
            if (state.isDisconnected()) {
                // the connection has been closed/closing.
                return closeFuture;
            }
            state = ConnectionState.DISCONNECTED;

            // when the remote server shutdowns, the exception may be
            // AnnotatedConnectException/ClosedChannelException/
            // IOException with error message "Connection reset by peer"
            // we warp it to NetworkException which is re-triable
            // which enables clients to retry write/poll
            Throwable requestCause = cause;
            if (cause instanceof IOException) {
                requestCause = new NetworkException("Disconnected from node " + node, cause);
            }
            // notify all the inflight requests
            for (int requestId : inflightRequests.keySet()) {
                InflightRequest request = inflightRequests.remove(requestId);
                request.responseFuture.completeExceptionally(requestCause);
            }

            // notify all the pending requests
            PendingRequest pending;
            while ((pending = pendingRequests.pollFirst()) != null) {
                pending.responseFuture.completeExceptionally(requestCause);
            }

            if (channel != null) {
                channel.close()
                        .addListener(
                                (ChannelFutureListener)
                                        future -> {

                                            // when finishing, if netty successfully closes the
                                            // channel, then the provided exception is used as
                                            // the reason for the closing. If there was something
                                            // wrong at the netty side, then that exception is
                                            // prioritized over the provided one.
                                            if (future.isSuccess()) {
                                                if (cause instanceof ClosedChannelException) {
                                                    // the ClosedChannelException is expected
                                                    closeFuture.complete(null);
                                                } else {
                                                    closeFuture.completeExceptionally(cause);
                                                }
                                            } else {
                                                LOG.warn(
                                                        "Something went wrong when trying to close connection due to : ",
                                                        cause);
                                                closeFuture.completeExceptionally(future.cause());
                                            }
                                        });
            } else {
                // TODO all return completeExceptionally will let some test cases blocked, so we
                // need to find why the test cases are blocked and remove the if statement.
                if (cause.getCause() instanceof ConnectException) {
                    // the ConnectException is expected
                    closeFuture.complete(null);
                } else {
                    closeFuture.completeExceptionally(cause);
                }
            }

            connectionMetricGroup.close();
        }

        return closeFuture;
    }

    // ------------------------------------------------------------------------------------------

    /**
     * Callback when the channel receiving a response or error from server.
     *
     * @see NettyClientHandler
     */
    private final class ResponseCallback implements ClientHandlerCallback {

        @Override
        public ApiMethod getRequestApiMethod(int requestId) {
            InflightRequest inflightRequest = inflightRequests.get(requestId);
            if (inflightRequest != null) {
                return ApiManager.forApiKey(inflightRequest.apiKey);
            } else {
                return null;
            }
        }

        @Override
        public void onRequestResult(int requestId, ApiMessage response) {
            InflightRequest request = inflightRequests.remove(requestId);
            if (request != null && !request.responseFuture.isDone()) {
                connectionMetricGroup.updateMetricsAfterGetResponse(
                        ApiKeys.forId(request.apiKey),
                        request.requestStartTime,
                        response.totalSize());
                request.responseFuture.complete(response);
            }
        }

        @Override
        public void onRequestFailure(int requestId, Throwable cause) {
            InflightRequest request = inflightRequests.remove(requestId);
            if (request != null && !request.responseFuture.isDone()) {
                connectionMetricGroup.updateMetricsAfterGetResponse(
                        ApiKeys.forId(request.apiKey), request.requestStartTime, 0);
                request.responseFuture.completeExceptionally(cause);
            }
        }

        @Override
        public void onFailure(Throwable cause) {
            close(cause);
        }
    }

    // ------------------------------------------------------------------------------------------

    private void establishConnection(ChannelFuture future) {
        synchronized (lock) {
            if (future.isSuccess()) {
                LOG.debug("Established connection to server {}.", node);
                channel = future.channel();
                channel.pipeline()
                        .addLast("handler", new NettyClientHandler(new ResponseCallback()));
                // start checking api versions
                state = ConnectionState.CHECKING_API_VERSIONS;
                // TODO: set correct client software name and version, used for metrics in server
                ApiVersionsRequest request =
                        new ApiVersionsRequest()
                                .setClientSoftwareName("fluss")
                                .setClientSoftwareVersion("0.1.0");
                doSend(ApiKeys.API_VERSIONS, request, new CompletableFuture<>(), true)
                        .whenComplete(this::handleApiVersionsResponse);
            } else {
                LOG.error("Failed to establish connection to server {}.", node, future.cause());
                close(future.cause());
            }
        }
    }

    private CompletableFuture<ApiMessage> doSend(
            ApiKeys apiKey,
            ApiMessage rawRequest,
            CompletableFuture<ApiMessage> responseFuture,
            boolean isInternalRequest) {
        synchronized (lock) {
            if (state.isDisconnected()) {
                Exception exception =
                        new NetworkException(
                                new DisconnectException(
                                        "Cannot send request to server "
                                                + node
                                                + " because it is disconnected."));
                responseFuture.completeExceptionally(exception);
                return responseFuture;
            }
            // 1. connection is not established: all requests are queued
            // 2. connection is established but not ready: internal requests are processed, other
            // requests are queued
            if (!state.isEstablished() || (!state.isReady() && !isInternalRequest)) {
                pendingRequests.add(
                        new PendingRequest(apiKey, rawRequest, isInternalRequest, responseFuture));
                return responseFuture;
            }

            // version equals highestSupportedVersion might happen when requesting api version check
            // before serverApiVersions is  initialized. We always use the highest version for api
            // version checking.
            short version = apiKey.highestSupportedVersion;
            if (serverApiVersions != null) {
                try {
                    version = serverApiVersions.highestAvailableVersion(apiKey);
                } catch (Exception e) {
                    responseFuture.completeExceptionally(e);
                }
            }

            InflightRequest inflight =
                    new InflightRequest(
                            apiKey.id, version, requestCount++, rawRequest, responseFuture);
            inflightRequests.put(inflight.requestId, inflight);

            // TODO: maybe we need to add timeout for the inflight requests
            ByteBuf byteBuf;
            try {
                byteBuf = inflight.toByteBuf(channel.alloc());
            } catch (Exception e) {
                LOG.error("Failed to encode request for '{}'.", ApiKeys.forId(inflight.apiKey), e);
                inflightRequests.remove(inflight.requestId);
                responseFuture.completeExceptionally(
                        new FlussRuntimeException(
                                String.format(
                                        "Failed to encode request for '%s'",
                                        ApiKeys.forId(inflight.apiKey)),
                                e));
                return responseFuture;
            }

            connectionMetricGroup.updateMetricsBeforeSendRequest(apiKey, rawRequest.totalSize());

            channel.writeAndFlush(byteBuf)
                    .addListener(
                            (ChannelFutureListener)
                                    future -> {
                                        if (!future.isSuccess()) {
                                            connectionMetricGroup.updateMetricsAfterGetResponse(
                                                    apiKey, inflight.requestStartTime, 0);
                                            Throwable cause = future.cause();
                                            if (cause instanceof IOException) {
                                                // when server close the channel, the cause will be
                                                // IOException, if the cause is IOException, we wrap
                                                // it as retryable NetworkException to retry to
                                                // connect
                                                cause = new NetworkException(cause);
                                            }
                                            inflight.responseFuture.completeExceptionally(cause);
                                            inflightRequests.remove(inflight.requestId);
                                        }
                                    });
            return inflight.responseFuture;
        }
    }

    private void handleApiVersionsResponse(ApiMessage response, Throwable cause) {
        if (cause != null) {
            close(cause);
            return;
        }
        if (!(response instanceof ApiVersionsResponse)) {
            close(new IllegalStateException("Unexpected response type " + response.getClass()));
            return;
        }

        synchronized (lock) {
            serverApiVersions =
                    new ServerApiVersions(((ApiVersionsResponse) response).getApiVersionsList());
            state = ConnectionState.READY;
            // process pending requests
            PendingRequest pending;
            while ((pending = pendingRequests.pollFirst()) != null) {
                doSend(
                        pending.apikey,
                        pending.request,
                        pending.responseFuture,
                        pending.isInternalRequest);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Internal Classes
    // ------------------------------------------------------------------------------------------

    /**
     * The states of a server connection.
     * <li>CONNECTING: connection is under progress.
     * <li>CHECKING_API_VERSIONS: connection has been established and api versions check is in
     *     progress. Failure of this check will cause connection to close.
     * <li>READY: connection is ready to send requests.
     * <li>DISCONNECTED: connection is failed to establish.
     */
    private enum ConnectionState {
        CONNECTING,
        CHECKING_API_VERSIONS,
        READY,
        DISCONNECTED;

        public boolean isDisconnected() {
            return this == DISCONNECTED;
        }

        public boolean isEstablished() {
            return this == CHECKING_API_VERSIONS || this == READY;
        }

        public boolean isReady() {
            return this == READY;
        }
    }

    /** A pending request that is waiting for the connection to be ready. */
    private static class PendingRequest {
        final ApiKeys apikey;
        final ApiMessage request;
        final boolean isInternalRequest;
        final CompletableFuture<ApiMessage> responseFuture;

        public PendingRequest(
                ApiKeys apikey,
                ApiMessage request,
                boolean isInternalRequest,
                CompletableFuture<ApiMessage> responseFuture) {
            this.apikey = apikey;
            this.request = request;
            this.isInternalRequest = isInternalRequest;
            this.responseFuture = responseFuture;
        }
    }

    /** A request has been sent to server but waiting for response. */
    private static class InflightRequest {

        final short apiKey;
        final short apiVersion;
        final int requestId;
        final ApiMessage request;
        final long requestStartTime;
        final CompletableFuture<ApiMessage> responseFuture;

        InflightRequest(
                short apiKey,
                short apiVersion,
                int requestId,
                ApiMessage request,
                CompletableFuture<ApiMessage> responseFuture) {
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
            this.requestId = requestId;
            this.request = request;
            this.responseFuture = responseFuture;
            this.requestStartTime = System.currentTimeMillis();
        }

        ByteBuf toByteBuf(ByteBufAllocator allocator) {
            return MessageCodec.encodeRequest(allocator, apiKey, apiVersion, requestId, request);
        }
    }
}

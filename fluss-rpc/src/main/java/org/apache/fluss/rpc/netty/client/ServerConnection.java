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

package org.apache.fluss.rpc.netty.client;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.exception.DisconnectException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.exception.RetriableAuthenticationException;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.AuthenticateRequest;
import org.apache.fluss.rpc.messages.AuthenticateResponse;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.metrics.ConnectionMetrics;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.ApiManager;
import org.apache.fluss.rpc.protocol.ApiMethod;
import org.apache.fluss.rpc.protocol.MessageCodec;
import org.apache.fluss.security.auth.ClientAuthenticator;
import org.apache.fluss.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.fluss.shaded.netty4.io.netty.channel.Channel;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.fluss.utils.IOUtils.closeQuietly;

/** Connection to a Netty server used by the {@link NettyClient}. */
@ThreadSafe
final class ServerConnection {
    private static final Logger LOG = LoggerFactory.getLogger(ServerConnection.class);

    private final ServerNode node;

    // TODO: add max inflight requests limit like Kafka's "max.in.flight.requests.per.connection"
    private final Map<Integer, InflightRequest> inflightRequests = MapUtils.newConcurrentHashMap();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private final ConnectionMetrics connectionMetrics;
    private final ClientAuthenticator authenticator;
    private final ExponentialBackoff backoff;

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

    @GuardedBy("lock")
    private int retryAuthCount = 0;

    ServerConnection(
            Bootstrap bootstrap,
            ServerNode node,
            ClientMetricGroup clientMetricGroup,
            ClientAuthenticator authenticator,
            BiConsumer<ServerConnection, Throwable> closeCallback,
            boolean isInnerClient) {
        this.node = node;
        this.state = ConnectionState.CONNECTING;
        this.connectionMetrics = clientMetricGroup.createConnectionMetricGroup(node.uid());
        this.authenticator = authenticator;
        this.backoff = new ExponentialBackoff(100L, 2, 5000L, 0.2);
        whenClose(closeCallback);

        // connect and handle should be last in case of other variables are nullable and close
        // callback is not registered when connection established.
        bootstrap
                .connect(node.host(), node.port())
                .addListener(future -> establishConnection((ChannelFuture) future, isInnerClient));
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
    private void whenClose(BiConsumer<ServerConnection, Throwable> closeCallback) {
        closeFuture.whenComplete((v, throwable) -> closeCallback.accept(this, throwable));
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

            switchState(ConnectionState.DISCONNECTED);

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
                // Close the channel directly, without waiting for the channel to close properly.
                channel.close();
            }

            // TODO all return completeExceptionally will let some test cases blocked, so we
            // need to find why the test cases are blocked and remove the if statement.
            if (cause instanceof ClosedChannelException
                    || cause.getCause() instanceof ConnectException) {
                // the ClosedChannelException and ConnectException is expected.
                closeFuture.complete(null);
            } else {
                closeFuture.completeExceptionally(cause);
            }

            connectionMetrics.close();
        }

        closeQuietly(authenticator);
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
                connectionMetrics.updateMetricsAfterGetResponse(
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
                connectionMetrics.updateMetricsAfterGetResponse(
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

    private void establishConnection(ChannelFuture future, boolean isInnerClient) {
        synchronized (lock) {
            if (future.isSuccess()) {
                LOG.debug("Established connection to server {}.", node);
                channel = future.channel();
                channel.pipeline()
                        .addLast(
                                "handler",
                                new NettyClientHandler(new ResponseCallback(), isInnerClient));
                // start checking api versions
                switchState(ConnectionState.CHECKING_API_VERSIONS);
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

            connectionMetrics.updateMetricsBeforeSendRequest(apiKey, rawRequest.totalSize());

            channel.writeAndFlush(byteBuf)
                    .addListener(
                            (ChannelFutureListener)
                                    future -> {
                                        if (!future.isSuccess()) {
                                            connectionMetrics.updateMetricsAfterGetResponse(
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
            // send initial token
            sendInitialToken();
        }
    }

    private void sendInitialToken() {
        try {
            authenticator.initialize(new DefaultAuthenticateContext());
        } catch (Exception e) {
            LOG.error("Failed to initialize authenticator", e);
            close(e);
            return;
        }

        LOG.debug("Begin to authenticate with protocol {}", authenticator.protocol());
        sendAuthenticateRequest(new byte[0], true);
    }

    private void sendAuthenticateRequest(byte[] challenge, boolean initialToken) {
        try {
            if (!authenticator.isCompleted()) {
                byte[] token =
                        initialToken && !authenticator.hasInitialTokenResponse()
                                ? challenge
                                : authenticator.authenticate(challenge);
                if (token != null) {
                    switchState(ConnectionState.AUTHENTICATING);
                    AuthenticateRequest request =
                            new AuthenticateRequest()
                                    .setToken(token)
                                    .setProtocol(authenticator.protocol());
                    doSend(ApiKeys.AUTHENTICATE, request, new CompletableFuture<>(), true)
                            .whenComplete(this::handleAuthenticateResponse);
                    return;
                }
            }

            assert authenticator.isCompleted();
            switchState(ConnectionState.READY);

        } catch (Exception e) {
            LOG.error("Authentication failed when authenticating challenge。", e);
            close(
                    new FlussRuntimeException(
                            "Authentication failed when authenticating challenge", e));
        }
    }

    private void handleAuthenticateResponse(ApiMessage response, Throwable cause) {
        synchronized (lock) {
            if (cause != null) {
                if (cause instanceof RetriableAuthenticationException) {
                    LOG.warn("Authentication failed, retrying {} times", retryAuthCount, cause);
                    channel.eventLoop()
                            .schedule(
                                    this::sendInitialToken,
                                    backoff.backoff(retryAuthCount++),
                                    TimeUnit.MILLISECONDS);
                } else {
                    close(cause);
                }
                return;
            }
            if (!(response instanceof AuthenticateResponse)) {
                close(new IllegalStateException("Unexpected response type " + response.getClass()));
                return;
            }

            AuthenticateResponse authenticateResponse = (AuthenticateResponse) response;
            if (authenticateResponse.hasChallenge()) {
                sendAuthenticateRequest(((AuthenticateResponse) response).getChallenge(), false);
            } else if (authenticator.isCompleted()) {
                switchState(ConnectionState.READY);
            } else {
                close(
                        new IllegalStateException(
                                "client authenticator is not completed while server generate no challenge."));
            }
        }
    }

    private void switchState(ConnectionState targetState) {
        if (state != ConnectionState.DISCONNECTED) {
            LOG.debug("switch state form {} to {}", state, targetState);
            state = targetState;
        }
        if (targetState == ConnectionState.READY) {
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
    @VisibleForTesting
    enum ConnectionState {
        CONNECTING,
        CHECKING_API_VERSIONS,
        AUTHENTICATING,
        READY,
        DISCONNECTED;

        public boolean isDisconnected() {
            return this == DISCONNECTED;
        }

        public boolean isEstablished() {
            return this == CHECKING_API_VERSIONS || this == AUTHENTICATING || this == READY;
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

    private class DefaultAuthenticateContext implements ClientAuthenticator.AuthenticateContext {
        @Override
        public String ipAddress() {
            return ((InetSocketAddress) channel.remoteAddress()).getAddress().getHostAddress();
        }
    }

    @VisibleForTesting
    ConnectionState getConnectionState() {
        return state;
    }
}

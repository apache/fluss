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

import com.alibaba.fluss.record.send.Send;
import com.alibaba.fluss.rpc.RpcGatewayService;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.ApiKeys;
import com.alibaba.fluss.rpc.protocol.ApiMethod;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeErrorResponse;
import static com.alibaba.fluss.rpc.protocol.MessageCodec.encodeSuccessResponse;
import static com.alibaba.fluss.utils.ExceptionUtils.stripException;

/** A thread that processes and answer incoming {@link RpcRequest}. */
final class RequestProcessor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessor.class);

    private final int processorId;
    private final RpcGatewayService service;
    private final RequestChannel requestChannel;
    private final CompletableFuture<Void> shutdownComplete = new CompletableFuture<>();

    private final RequestsMetrics requestsMetrics;

    private volatile boolean isRunning;

    public RequestProcessor(
            int processorId,
            RequestChannel requestChannel,
            RpcGatewayService service,
            RequestsMetrics requestsMetrics) {
        this.processorId = processorId;
        this.service = service;
        this.requestChannel = requestChannel;
        this.isRunning = true;
        this.requestsMetrics = requestsMetrics;
    }

    @Override
    public void run() {
        while (isRunning) {
            RpcRequest request = requestChannel.pollRequest(300);
            if (request != null) {
                if (request == RpcRequest.SHUTDOWN_REQUEST) {
                    LOG.debug(
                            "Fluss request processor {} on server {} received shutdown command.",
                            service.name(),
                            processorId);
                    completeShutdown();
                } else {
                    LOG.debug(
                            "Fluss request processor {} on server {} processing request {}.",
                            processorId,
                            service.name(),
                            request);
                    try {
                        processRequest(request);
                    } catch (Throwable t) {
                        LOG.error("Error while processing request.", t);
                    } finally {
                        // this releases the lazily parsed buffer (e.g. for produce request).
                        // RpcGateway shouldn't hold the reference to the request/ByteBuf after
                        // the invoking of RPC method, i.e. async execution shouldn't retain the
                        // request/ByteBuf. The request/ByteBuf is released after the RPC method.
                        request.releaseBuffer();
                    }
                }
            }
        }
        completeShutdown();
    }

    private void processRequest(RpcRequest request) {
        long requestDequeTimeMs = System.currentTimeMillis();
        ApiMethod api = request.getApiMethod();
        ApiMessage message = request.getMessage();
        try {
            service.setCurrentApiVersion(request.getApiVersion());
            // invoke the corresponding method on RpcGateway instance.
            CompletableFuture<?> responseFuture =
                    (CompletableFuture<?>) api.getMethod().invoke(service, message);
            responseFuture.whenComplete(
                    (response, throwable) -> {
                        if (throwable != null) {
                            sendError(request, throwable);
                        } else {
                            if (response instanceof ApiMessage) {
                                sendResponse(request, requestDequeTimeMs, (ApiMessage) response);
                            } else {
                                sendError(
                                        request,
                                        new ClassCastException(
                                                "The response "
                                                        + response.getClass().getName()
                                                        + " is not an instance of ApiMessage."));
                            }
                        }
                    });
        } catch (Throwable t) {
            LOG.debug("Error while executing RPC {}", api, t);
            sendError(request, stripException(t, InvocationTargetException.class));
        }
    }

    private void sendResponse(
            RpcRequest request, long requestDequeTimeMs, ApiMessage responseMessage) {
        long requestCompletedTimeMs = System.currentTimeMillis();
        // TODO: use a memory managed allocator
        ChannelHandlerContext channelContext = request.getChannelContext();
        ByteBufAllocator alloc = channelContext.alloc();
        try {
            Send send = encodeSuccessResponse(alloc, request.getRequestId(), responseMessage);
            send.writeTo(channelContext);
            channelContext.flush();
            long requestEndTimeMs = System.currentTimeMillis();
            updateRequestMetrics(
                    request, requestDequeTimeMs, requestCompletedTimeMs, requestEndTimeMs);
        } catch (Throwable t) {
            LOG.error("Failed to send response to client.", t);
            sendError(request, t);
        }
    }

    private void updateRequestMetrics(
            RpcRequest request,
            long requestDequeTimeMs,
            long requestCompletedTimeMs,
            long requestEndTimeMs) {
        // get the metrics to be updated for this kind of request
        Optional<RequestsMetrics.Metrics> optMetrics = getMetrics(request);
        // no any metrics registered for the kind of request
        if (!optMetrics.isPresent()) {
            return;
        }

        // now, we need to update metrics
        RequestsMetrics.Metrics metrics = optMetrics.get();

        metrics.getRequestsCount().inc();
        metrics.getRequestBytes().update(request.getMessage().totalSize());

        // update metrics related to time
        metrics.getRequestQueueTimeMs().update(requestDequeTimeMs - request.getStartTimeMs());
        metrics.getRequestProcessTimeMs().update(requestCompletedTimeMs - requestDequeTimeMs);
        metrics.getResponseSendTimeMs().update(requestEndTimeMs - requestCompletedTimeMs);
        metrics.getTotalTimeMs().update(requestEndTimeMs - request.getStartTimeMs());
    }

    private void sendError(RpcRequest request, Throwable t) {
        ApiError error = ApiError.fromThrowable(t);
        // TODO: use a memory managed allocator
        ByteBufAllocator alloc = request.getChannelContext().alloc();
        ByteBuf byteBuf = encodeErrorResponse(alloc, request.getRequestId(), error);
        request.getChannelContext().writeAndFlush(byteBuf);

        getMetrics(request).ifPresent(metrics -> metrics.getErrorsCount().inc());
    }

    private Optional<RequestsMetrics.Metrics> getMetrics(RpcRequest request) {
        boolean isFromFollower = false;
        ApiMessage requestMessage = request.getMessage();
        if (request.getApiKey() == ApiKeys.FETCH_LOG.id) {
            // for fetch, we need to identify it's from client or follower
            FetchLogRequest fetchLogRequest = (FetchLogRequest) requestMessage;
            isFromFollower = fetchLogRequest.getFollowerServerId() >= 0;
        }
        return requestsMetrics.getMetrics(request.getApiKey(), isFromFollower);
    }

    public void initiateShutdown() {
        try {
            requestChannel.putShutdownRequest();
        } catch (Exception e) {
            LOG.warn("Failed to send shutdown request to request channel.", e);
        }
    }

    public CompletableFuture<Void> getShutdownFuture() {
        return shutdownComplete;
    }

    private void completeShutdown() {
        shutdownComplete.complete(null);
        isRunning = false;
    }
}

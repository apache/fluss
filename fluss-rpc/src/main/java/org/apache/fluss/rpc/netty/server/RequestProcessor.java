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

package org.apache.fluss.rpc.netty.server;

import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.protocol.RequestType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/** A thread that processes and answer incoming {@link RpcRequest}. */
final class RequestProcessor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessor.class);

    private final int processorId;
    private final RpcGatewayService service;
    private final RequestChannel requestChannel;
    private final CompletableFuture<Void> shutdownComplete = new CompletableFuture<>();
    private final RequestHandler<?>[] requestHandlers;

    private volatile boolean isRunning;

    public RequestProcessor(
            int processorId,
            RequestChannel requestChannel,
            RpcGatewayService service,
            RequestHandler<?>[] requestHandlers) {
        this.processorId = processorId;
        this.service = service;
        this.requestChannel = requestChannel;
        this.requestHandlers = requestHandlers;
        this.isRunning = true;
    }

    @Override
    public void run() {
        while (isRunning) {
            RpcRequest request = requestChannel.pollRequest(300);
            if (request != null) {
                if (request == ShutdownRequest.INSTANCE) {
                    LOG.debug(
                            "Request processor {} on server {} received shutdown command.",
                            service.name(),
                            processorId);
                    completeShutdown();
                } else {
                    LOG.debug(
                            "Request processor {} on server {} processing request {}.",
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
        RequestType requestType = request.getRequestType();
        if (requestType.id >= requestHandlers.length) {
            LOG.error("Invalid request type {}", requestType);
            return;
        }
        RequestHandler handler = requestHandlers[requestType.id];
        if (handler == null) {
            LOG.error("No handler found for request type {}", requestType);
            return;
        }
        //noinspection unchecked
        handler.processRequest(request);
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

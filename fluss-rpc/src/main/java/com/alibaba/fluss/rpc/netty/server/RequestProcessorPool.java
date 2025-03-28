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

import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.rpc.RpcGatewayService;
import com.alibaba.fluss.rpc.protocol.NetworkProtocolPlugin;
import com.alibaba.fluss.rpc.protocol.RequestType;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;
import com.alibaba.fluss.utils.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A worker thread pool that contains a fixed number of threads to process incoming requests. */
final class RequestProcessorPool {
    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorPool.class);

    private final RequestChannel[] requestChannels;
    private final RequestProcessor[] processors;

    private ExecutorService workerPool;

    public RequestProcessorPool(
            int numProcessors,
            int totalQueueCapacity,
            RpcGatewayService service,
            List<NetworkProtocolPlugin> protocols,
            RequestsMetrics requestsMetrics) {
        this.processors = new RequestProcessor[numProcessors];
        this.requestChannels = new RequestChannel[numProcessors];

        RequestHandler<?>[] requestHandlers = initializeRequestHandlers(protocols, service);
        for (int i = 0; i < numProcessors; i++) {
            requestChannels[i] = new RequestChannel(totalQueueCapacity / numProcessors);
            // bind processor to a single channel to make requests from the
            // same channel processed serializable
            processors[i] = new RequestProcessor(i, requestChannels[i], service, requestHandlers);
        }
        // register requestQueueSize metrics
        requestsMetrics.gauge(MetricNames.REQUEST_QUEUE_SIZE, this::getRequestQueueSize);
    }

    public int getRequestQueueSize() {
        // sum all the requests in all the requestChannels
        return Arrays.stream(requestChannels)
                .map(RequestChannel::requestsCount)
                .reduce(Integer::sum)
                .orElse(0);
    }

    public RequestChannel[] getRequestChannels() {
        return requestChannels;
    }

    public synchronized void start() {
        this.workerPool =
                Executors.newFixedThreadPool(
                        processors.length, new ExecutorThreadFactory("fluss-netty-server-worker"));
        for (RequestProcessor processor : processors) {
            workerPool.execute(processor);
        }
    }

    public synchronized CompletableFuture<Void> closeAsync() {
        if (workerPool == null) {
            // the processor poll is not started yet.
            return CompletableFuture.completedFuture(null);
        }
        LOG.info("Shutting down Fluss request processor pool.");
        List<CompletableFuture<Void>> shutdownFutures = new ArrayList<>();
        for (RequestProcessor processor : processors) {
            if (processor != null) {
                processor.initiateShutdown();
                shutdownFutures.add(processor.getShutdownFuture());
            }
        }
        return FutureUtils.runAfterwards(
                FutureUtils.completeAll(shutdownFutures),
                () -> {
                    if (workerPool != null) {
                        workerPool.shutdown();
                    }
                });
        // service and requestChannel shutdown is handled outside.
    }

    private RequestHandler<?>[] initializeRequestHandlers(
            List<NetworkProtocolPlugin> protocolPlugins, RpcGatewayService service) {
        int maxRequestTypeId =
                Arrays.stream(RequestType.values())
                        .mapToInt(type -> type.id)
                        .max()
                        .orElseThrow(() -> new IllegalStateException("No response type found."));
        RequestHandler<?>[] requestHandlers = new RequestHandler[maxRequestTypeId + 1];
        requestHandlers[RequestType.FLUSS.id] = new FlussRequestHandler(service);
        for (NetworkProtocolPlugin protocol : protocolPlugins) {
            RequestHandler<?> requestHandler = protocol.createRequestHandler(service);
            int id = requestHandler.requestType().id;
            if (requestHandlers[id] != null) {
                throw new IllegalStateException(
                        "Duplicate protocol found for request type id " + id + ".");
            }
            requestHandlers[id] = requestHandler;
        }
        return requestHandlers;
    }
}

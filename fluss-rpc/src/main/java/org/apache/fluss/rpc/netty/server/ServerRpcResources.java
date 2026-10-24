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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.RpcGatewayService;
import org.apache.fluss.rpc.netty.NettyUtils;
import org.apache.fluss.shaded.netty4.io.netty.channel.EventLoopGroup;

import java.util.concurrent.CompletableFuture;

/** Network threads, request processors and backpressure queues shared by a group of listeners. */
final class ServerRpcResources {

    private final RequestProcessorPool workerPool;
    private final int numNetworkThreads;
    private final String threadSuffix;

    private EventLoopGroup selectorGroup;

    ServerRpcResources(
            Configuration conf,
            RpcGatewayService service,
            RequestHandler<?>[] requestHandlers,
            RequestsMetrics requestsMetrics,
            int processorIndexOffset,
            String threadSuffix) {
        this.numNetworkThreads = conf.getInt(ConfigOptions.NETTY_SERVER_NUM_NETWORK_THREADS);
        this.threadSuffix = threadSuffix;
        this.workerPool =
                new RequestProcessorPool(
                        conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS),
                        conf.getInt(ConfigOptions.NETTY_SERVER_MAX_QUEUED_REQUESTS),
                        service,
                        requestHandlers,
                        requestsMetrics,
                        processorIndexOffset,
                        "fluss-netty-server-worker" + threadSuffix);
    }

    void start() {
        selectorGroup =
                NettyUtils.newEventLoopGroup(
                        numNetworkThreads, "fluss-netty-server-selector" + threadSuffix);
        workerPool.start();
    }

    EventLoopGroup selectorGroup() {
        return selectorGroup;
    }

    RequestChannel[] requestChannels() {
        return workerPool.getRequestChannels();
    }

    int requestsCount() {
        return workerPool.getRequestQueueSize();
    }

    CompletableFuture<Void> closeAsync() {
        return CompletableFuture.allOf(
                workerPool.closeAsync(), NettyUtils.shutdownGroup(selectorGroup));
    }
}

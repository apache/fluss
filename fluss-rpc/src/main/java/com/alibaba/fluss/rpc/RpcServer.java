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

package com.alibaba.fluss.rpc;

import com.alibaba.fluss.cluster.Endpoint;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.groups.MetricGroup;
import com.alibaba.fluss.rpc.netty.server.NettyServer;
import com.alibaba.fluss.rpc.netty.server.RequestsMetrics;
import com.alibaba.fluss.utils.AutoCloseableAsync;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/** Handles new connections, requests and responses to and from coordinator/tablet server. */
public interface RpcServer extends AutoCloseableAsync {

    /**
     * Creates a new RPC server that can bind to the given address and port and uses the given
     * {@link RpcGatewayService} to handle incoming requests.
     *
     * @param conf The configuration for the RPC server.
     * @param endpoints The endpoints to bind to.
     * @param service The service to handle incoming requests.
     * @param serverMetricGroup The metric group of server to report.
     * @param requestsMetrics the requests metrics to report.
     * @return The new RPC server.
     */
    static RpcServer create(
            Configuration conf,
            List<Endpoint> endpoints,
            RpcGatewayService service,
            MetricGroup serverMetricGroup,
            RequestsMetrics requestsMetrics)
            throws IOException {
        return new NettyServer(conf, endpoints, service, serverMetricGroup, requestsMetrics);
    }

    /** Starts the RPC server by binding to the configured bind address and port (blocking). */
    void start() throws IOException;

    /**
     * Gets the bind address of the RPC server. If maybe different from the configured endpoints.
     * For example, if port of endpoint is 0, pick up an ephemeral port in a bind operation.
     *
     * @return The bind address of the RPC server.
     */
    List<Endpoint> getBindEndpoints();

    CompletableFuture<Void> closeAsync();

    /**
     * Gets a scheduled executor from the RPC server. This executor can be used to schedule tasks to
     * be executed in the future.
     *
     * <p><b>IMPORTANT:</b> This executor does not isolate the method invocations against any
     * concurrent invocations and is therefore not suitable to run completion methods of futures
     * that modify state of an {@link RpcGatewayService}. For such operations, one needs to use the
     * {@code RpcGatewayService#getMainThreadExecutor() MainThreadExecutionContext} of that {@code
     * RpcGatewayService}.
     *
     * @return The RPC server provided scheduled executor
     */
    ScheduledExecutorService getScheduledExecutor();
}

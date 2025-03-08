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

package com.alibaba.fluss.rpc;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.rpc.netty.client.NettyClient;
import com.alibaba.fluss.rpc.protocol.ApiKeys;

import java.util.concurrent.CompletableFuture;

/**
 * A network client interface for asynchronous request/response network i/o. This is an internal
 * class used to implement the user-facing reader and writer.
 */
public interface RpcClient extends AutoCloseable {

    /**
     * Create a new RPC client that can be used to send requests to the {@link RpcServer}.
     *
     * @param conf The configuration to use.
     * @param clientMetricGroup The client metric group
     * @return The RPC client.
     */
    static RpcClient create(Configuration conf, ClientMetricGroup clientMetricGroup) {
        return new NettyClient(conf, clientMetricGroup);
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send
     * to that node.
     *
     * @param node The server node to check
     * @return True if we are ready to send to the given node.
     */
    boolean connect(ServerNode node);

    /**
     * Disconnects the connection to the given server node, if there is one. Any in-flight/pending
     * requests for this connection will receive disconnections.
     *
     * @param serverUid The uid of the server node
     * @return A future that is completed when the disconnection is complete
     */
    CompletableFuture<Void> disconnect(String serverUid);

    /**
     * Check if we are currently ready to send another request to the given server but don't attempt
     * to connect if we aren't.
     *
     * @return true if the node is ready
     */
    boolean isReady(String serverUid);

    /**
     * Send an RPC request to the given server and return a future for the response. If the
     * requested node is not connected yet, it will try to {@link #connect(ServerNode)} the node
     * first.
     */
    CompletableFuture<ApiMessage> sendRequest(ServerNode node, ApiKeys apiKey, ApiMessage request);
}

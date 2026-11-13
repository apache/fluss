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

package org.apache.fluss.client.utils;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.protocol.ApiKeys;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/** RPC client with configurable responses and recorded endpoint and UID disconnections. */
public class TestingRpcClient implements RpcClient {

    private final BiFunction<ServerNode, ApiKeys, CompletableFuture<ApiMessage>> response;
    private final List<ServerNode> disconnectedNodes = new ArrayList<>();
    private final List<String> disconnectedServerUids = new ArrayList<>();

    /** Creates a client that obtains responses from the given handler. */
    public TestingRpcClient(
            BiFunction<ServerNode, ApiKeys, CompletableFuture<ApiMessage>> response) {
        this.response = response;
    }

    @Override
    public boolean connect(ServerNode node) {
        return true;
    }

    @Override
    public CompletableFuture<Void> disconnect(ServerNode node) {
        disconnectedNodes.add(node);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> disconnect(String serverUid) {
        disconnectedServerUids.add(serverUid);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isReady(ServerNode node) {
        return true;
    }

    @Override
    public CompletableFuture<ApiMessage> sendRequest(
            ServerNode node, ApiKeys apiKey, ApiMessage request) {
        return response.apply(node, apiKey);
    }

    /** Returns the endpoints passed to {@link #disconnect(ServerNode)}. */
    public List<ServerNode> getDisconnectedNodes() {
        return Collections.unmodifiableList(disconnectedNodes);
    }

    /** Returns the logical server UIDs passed to {@link #disconnect(String)}. */
    public List<String> getDisconnectedServerUids() {
        return Collections.unmodifiableList(disconnectedServerUids);
    }

    @Override
    public void close() {}
}

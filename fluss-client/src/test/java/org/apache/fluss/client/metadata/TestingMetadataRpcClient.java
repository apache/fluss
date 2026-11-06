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

package org.apache.fluss.client.metadata;

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** A metadata-only RPC client with explicitly controlled responses and no network resources. */
class TestingMetadataRpcClient implements RpcClient {
    private final Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler;
    private final List<MetadataRequest> requests = new ArrayList<>();

    TestingMetadataRpcClient(
            Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
        this.handler = handler;
    }

    List<MetadataRequest> getRequests() {
        return Collections.unmodifiableList(requests);
    }

    @Override
    public boolean connect(ServerNode node) {
        return true;
    }

    @Override
    public CompletableFuture<Void> disconnect(ServerNode node) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> disconnect(String serverUid) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isReady(ServerNode node) {
        return true;
    }

    @Override
    public CompletableFuture<ApiMessage> sendRequest(
            ServerNode node, ApiKeys apiKey, ApiMessage request) {
        if (apiKey != ApiKeys.GET_METADATA) {
            throw new UnsupportedOperationException("Unsupported test RPC: " + apiKey);
        }
        MetadataRequest metadataRequest = (MetadataRequest) request;
        requests.add(metadataRequest);
        return handler.apply(metadataRequest).thenApply(response -> response);
    }

    @Override
    public void close() {
        // This client has no network resources.
    }
}

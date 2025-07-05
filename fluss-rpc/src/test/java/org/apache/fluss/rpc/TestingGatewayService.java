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

package org.apache.fluss.rpc;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.PbApiVersion;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.rpc.protocol.ApiManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Test gateway service. */
public class TestingGatewayService extends RpcGatewayService {

    private final List<String> processorThreadNames =
            Collections.synchronizedList(new ArrayList<>());

    public List<String> getProcessorThreadNames() {
        return new ArrayList<>(processorThreadNames);
    }

    @Override
    public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
        ApiManager apiManager = new ApiManager(providerType());
        Set<ApiKeys> apiKeys = apiManager.enabledApis();
        List<PbApiVersion> apiVersions = new ArrayList<>();
        for (ApiKeys api : apiKeys) {
            apiVersions.add(
                    new PbApiVersion()
                            .setApiKey(api.id)
                            .setMinVersion(api.lowestSupportedVersion)
                            .setMaxVersion(api.highestSupportedVersion));
        }
        ApiVersionsResponse response = new ApiVersionsResponse();
        response.addAllApiVersions(apiVersions);
        processorThreadNames.add(Thread.currentThread().getName());
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public String name() {
        return "testing";
    }

    @Override
    public ServerType providerType() {
        return ServerType.COORDINATOR;
    }

    @Override
    public void shutdown() {
        // do nothing.
    }
}

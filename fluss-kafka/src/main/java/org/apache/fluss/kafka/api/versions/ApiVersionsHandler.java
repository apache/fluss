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

package org.apache.fluss.kafka.api.versions;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiRegistry;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;

import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Implements ApiVersions from the capabilities actually registered on this server. */
@Internal
public final class ApiVersionsHandler implements KafkaApiHandler<ApiVersionsRequest> {

    private static final KafkaApiSpec API_SPEC =
            new KafkaApiSpec(
                    ApiKeys.API_VERSIONS,
                    ApiKeys.API_VERSIONS.oldestVersion(),
                    ApiKeys.API_VERSIONS.latestVersion(),
                    true);

    private final KafkaApiRegistry registry;

    /** Creates an ApiVersions handler backed by the server capability registry. */
    public ApiVersionsHandler(KafkaApiRegistry registry) {
        this.registry = checkNotNull(registry);
    }

    @Override
    public KafkaApiSpec apiSpec() {
        return API_SPEC;
    }

    @Override
    public CompletableFuture<? extends AbstractResponse> handle(
            KafkaRequestContext context, ApiVersionsRequest request) {
        if (!request.isValid()) {
            return CompletableFuture.completedFuture(
                    request.getErrorResponse(Errors.INVALID_REQUEST.exception()));
        }
        ApiVersionsResponseData data = new ApiVersionsResponseData();
        for (KafkaApiSpec spec : registry.advertisedApiSpecs()) {
            data.apiKeys()
                    .add(
                            new ApiVersionsResponseData.ApiVersion()
                                    .setApiKey(spec.apiKey().id)
                                    .setMinVersion(spec.minVersion())
                                    .setMaxVersion(spec.maxVersion()));
        }
        return CompletableFuture.completedFuture(new ApiVersionsResponse(data));
    }
}

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

import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiRegistry;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/** Tests registry-backed capability advertisement by {@link ApiVersionsHandler}. */
class ApiVersionsHandlerTest {

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2, 3, 4})
    void testAdvertiseRegisteredRangesAndExcludeHiddenApis(short version) {
        KafkaApiRegistry registry = new KafkaApiRegistry();
        ApiVersionsHandler handler = new ApiVersionsHandler(registry);
        registry.register(handler);
        registry.register(
                new TestingApiHandler(
                        new KafkaApiSpec(ApiKeys.METADATA, (short) 2, (short) 7, true)));
        registry.register(
                new TestingApiHandler(
                        new KafkaApiSpec(ApiKeys.FETCH, (short) 0, (short) 3, false)));
        registry.freeze();

        ApiVersionsResponse response =
                (ApiVersionsResponse)
                        handler.handle(null, new ApiVersionsRequest.Builder().build(version))
                                .join();

        assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
        assertThat(response.data().apiKeys())
                .extracting(ApiVersion::apiKey, ApiVersion::minVersion, ApiVersion::maxVersion)
                .containsExactly(
                        tuple(ApiKeys.METADATA.id, (short) 2, (short) 7),
                        tuple(ApiKeys.API_VERSIONS.id, (short) 0, (short) 4));
    }

    private static final class TestingApiHandler implements KafkaApiHandler<AbstractRequest> {
        private final KafkaApiSpec spec;

        private TestingApiHandler(KafkaApiSpec spec) {
            this.spec = spec;
        }

        @Override
        public KafkaApiSpec apiSpec() {
            return spec;
        }

        @Override
        public CompletableFuture<? extends AbstractResponse> handle(
                KafkaRequestContext context, AbstractRequest request) {
            throw new AssertionError(
                    "Capability advertisement must not invoke other API handlers.");
        }
    }
}

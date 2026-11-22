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

import org.apache.fluss.kafka.KafkaRequest;
import org.apache.fluss.kafka.KafkaRequestContext;
import org.apache.fluss.kafka.TestingChannelHandlerContext;
import org.apache.fluss.kafka.api.produce.ProduceHandler;
import org.apache.fluss.kafka.api.sasl.SaslAuthenticateHandler;
import org.apache.fluss.kafka.api.sasl.SaslHandshakeHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiHandler;
import org.apache.fluss.kafka.dispatcher.KafkaApiRegistry;
import org.apache.fluss.kafka.dispatcher.KafkaApiSpec;
import org.apache.fluss.kafka.security.KafkaSaslConnection;
import org.apache.fluss.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.fluss.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/** Tests registry-backed capability advertisement by {@link ApiVersionsHandler}. */
class ApiVersionsHandlerTest {

    @ParameterizedTest
    @CsvSource({
        "0, false",
        "1, false",
        "2, false",
        "3, false",
        "4, false",
        "0, true",
        "1, true",
        "2, true",
        "3, true",
        "4, true"
    })
    void testAdvertiseRegisteredRangesAndExcludeHiddenApis(short version, boolean saslEnabled) {
        AtomicReference<KafkaApiRegistry> registryReference = new AtomicReference<>();
        ApiVersionsHandler handler =
                new ApiVersionsHandler(
                        () -> {
                            assertThat(registryReference.get()).isNotNull();
                            return registryReference.get();
                        });
        TestingApiHandler metadataHandler =
                new TestingApiHandler(
                        new KafkaApiSpec(ApiKeys.METADATA, (short) 2, (short) 7, true));
        KafkaApiRegistry registry =
                new KafkaApiRegistry(
                        Arrays.asList(
                                handler,
                                new SaslHandshakeHandler(),
                                new SaslAuthenticateHandler(),
                                new ProduceHandler(
                                        command -> {
                                            throw new AssertionError(
                                                    "Capability advertisement must not invoke the Produce backend.");
                                        }),
                                metadataHandler,
                                new TestingApiHandler(
                                        new KafkaApiSpec(
                                                ApiKeys.FETCH, (short) 0, (short) 3, false))));
        registryReference.set(registry);

        assertThat(registry.lookup(ApiKeys.API_VERSIONS)).isSameAs(handler);
        assertThat(registry.lookup(ApiKeys.METADATA)).isSameAs(metadataHandler);
        assertThat(registry.lookup(ApiKeys.FETCH)).isNull();

        ApiVersionsResponse response =
                (ApiVersionsResponse)
                        handler.handle(
                                        context(version, saslEnabled),
                                        new ApiVersionsRequest.Builder().build(version))
                                .join();

        assertThat(response.data().errorCode()).isEqualTo(Errors.NONE.code());
        if (!saslEnabled) {
            assertThat(response.data().apiKeys())
                    .extracting(ApiVersion::apiKey, ApiVersion::minVersion, ApiVersion::maxVersion)
                    .containsExactly(
                            tuple(ApiKeys.PRODUCE.id, (short) 3, (short) 11),
                            tuple(ApiKeys.METADATA.id, (short) 2, (short) 7),
                            tuple(ApiKeys.API_VERSIONS.id, (short) 0, (short) 4));
            return;
        }
        assertThat(response.data().apiKeys())
                .extracting(ApiVersion::apiKey, ApiVersion::minVersion, ApiVersion::maxVersion)
                .containsExactly(
                        tuple(ApiKeys.PRODUCE.id, (short) 3, (short) 11),
                        tuple(ApiKeys.METADATA.id, (short) 2, (short) 7),
                        tuple(ApiKeys.SASL_HANDSHAKE.id, (short) 0, (short) 1),
                        tuple(ApiKeys.API_VERSIONS.id, (short) 0, (short) 4),
                        tuple(ApiKeys.SASL_AUTHENTICATE.id, (short) 0, (short) 2));
    }

    private static KafkaRequestContext context(short version, boolean saslEnabled) {
        ByteBuf buffer = Unpooled.buffer(1);
        KafkaSaslConnection connection =
                saslEnabled
                        ? KafkaSaslConnection.sasl(
                                () -> {
                                    throw new AssertionError(
                                            "Capability advertisement must not initialize authentication.");
                                })
                        : KafkaSaslConnection.plaintext();
        KafkaRequest request =
                new KafkaRequest(
                        ApiKeys.API_VERSIONS,
                        version,
                        new RequestHeader(ApiKeys.API_VERSIONS, version, "client", 1),
                        new ApiVersionsRequest.Builder().build(version),
                        "KAFKA",
                        connection,
                        buffer,
                        new TestingChannelHandlerContext(),
                        new CompletableFuture<>()) {};
        buffer.release();
        request.releaseBuffer();
        return KafkaRequestContext.fromRequest(request);
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

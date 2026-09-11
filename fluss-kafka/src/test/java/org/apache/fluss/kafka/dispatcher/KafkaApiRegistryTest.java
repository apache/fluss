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

package org.apache.fluss.kafka.dispatcher;

import org.apache.fluss.kafka.KafkaRequestContext;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaApiRegistry}. */
public class KafkaApiRegistryTest {

    @Test
    public void testRejectDuplicateRegistrationAndRegistrationAfterFreeze() {
        KafkaApiRegistry registry = brokerRegistry();
        TestingApiVersionsHandler handler = new TestingApiVersionsHandler(true);
        registry.register(handler);

        assertThatThrownBy(() -> registry.register(handler))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already registered");

        registry.freeze();
        assertThatThrownBy(() -> registry.register(handler))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already frozen");
    }

    @Test
    public void testOnlyAdvertiseEnabledHandlers() {
        KafkaApiRegistry registry = brokerRegistry();
        registry.register(new TestingApiVersionsHandler(true));
        assertThat(registry.advertisedApiSpecs()).hasSize(1);

        KafkaApiRegistry hiddenRegistry = brokerRegistry();
        hiddenRegistry.register(new TestingApiVersionsHandler(false));
        assertThat(hiddenRegistry.advertisedApiSpecs()).isEmpty();
        assertThat(hiddenRegistry.lookup(ApiKeys.API_VERSIONS)).isNull();
    }

    @Test
    public void testAdvertisedSpecIsSameSpecUsedForRouting() {
        KafkaApiRegistry registry = brokerRegistry();
        TestingApiVersionsHandler handler = new TestingApiVersionsHandler(true);
        registry.register(handler);
        registry.freeze();

        KafkaApiSpec advertisedSpec = registry.advertisedApiSpecs().get(0);
        KafkaApiHandler<?> routedHandler = registry.lookup(ApiKeys.API_VERSIONS);

        assertThat(routedHandler).isSameAs(handler);
        assertThat(routedHandler.apiSpec()).isSameAs(advertisedSpec);
        for (short version : ApiKeys.API_VERSIONS.allVersions()) {
            assertThat(advertisedSpec.supportsVersion(version)).isTrue();
        }
        assertThat(
                        advertisedSpec.supportsVersion(
                                (short) (ApiKeys.API_VERSIONS.latestVersion() + 1)))
                .isFalse();
    }

    @Test
    public void testRejectInvalidVersionRange() {
        assertThatThrownBy(() -> new KafkaApiSpec(ApiKeys.API_VERSIONS, (short) 1, (short) 0, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new KafkaApiSpec(
                                        ApiKeys.API_VERSIONS,
                                        ApiKeys.API_VERSIONS.oldestVersion(),
                                        (short) (ApiKeys.API_VERSIONS.latestVersion() + 1),
                                        true))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static KafkaApiRegistry brokerRegistry() {
        return new KafkaApiRegistry();
    }

    private static final class TestingApiVersionsHandler
            implements KafkaApiHandler<ApiVersionsRequest> {

        private final KafkaApiSpec spec;

        private TestingApiVersionsHandler(boolean advertised) {
            this.spec =
                    new KafkaApiSpec(
                            ApiKeys.API_VERSIONS,
                            ApiKeys.API_VERSIONS.oldestVersion(),
                            ApiKeys.API_VERSIONS.latestVersion(),
                            advertised);
        }

        @Override
        public KafkaApiSpec apiSpec() {
            return spec;
        }

        @Override
        public CompletableFuture<? extends AbstractResponse> handle(
                KafkaRequestContext context, ApiVersionsRequest request) {
            throw new UnsupportedOperationException();
        }
    }
}

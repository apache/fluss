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
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaApiRegistry}. */
public class KafkaApiRegistryTest {

    @Test
    public void testRejectDuplicateApiKeys() {
        assertThatThrownBy(
                        () ->
                                new KafkaApiRegistry(
                                        Arrays.asList(
                                                new TestingApiHandler(ApiKeys.API_VERSIONS, true),
                                                new TestingApiHandler(
                                                        ApiKeys.API_VERSIONS, false))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already registered");
    }

    @Test
    public void testChangesToInputCollectionDoNotAffectRegistry() {
        TestingApiHandler handler = new TestingApiHandler(ApiKeys.API_VERSIONS, true);
        List<KafkaApiHandler<?>> handlers = new ArrayList<>();
        handlers.add(handler);
        KafkaApiRegistry registry = new KafkaApiRegistry(handlers);

        handlers.clear();
        handlers.add(new TestingApiHandler(ApiKeys.METADATA, true));

        assertThat(registry.lookup(ApiKeys.API_VERSIONS)).isSameAs(handler);
        assertThat(registry.lookup(ApiKeys.METADATA)).isNull();
        assertThat(registry.advertisedApiSpecs()).containsExactly(handler.apiSpec());
    }

    @Test
    public void testOnlyEnabledApisAreAdvertisedAndRoutable() {
        TestingApiHandler apiVersions = new TestingApiHandler(ApiKeys.API_VERSIONS, true);
        TestingApiHandler metadata = new TestingApiHandler(ApiKeys.METADATA, true);
        TestingApiHandler hidden = new TestingApiHandler(ApiKeys.PRODUCE, false);
        KafkaApiRegistry registry =
                new KafkaApiRegistry(Arrays.asList(apiVersions, hidden, metadata));

        List<KafkaApiSpec> specs = registry.advertisedApiSpecs();

        assertThat(specs)
                .extracting(KafkaApiSpec::apiKey)
                .containsExactly(ApiKeys.METADATA, ApiKeys.API_VERSIONS);
        assertThat(registry.lookup(ApiKeys.PRODUCE)).isNull();
        for (KafkaApiSpec spec : specs) {
            KafkaApiHandler<?> handler = registry.lookup(spec.apiKey());
            assertThat(handler).isNotNull();
            assertThat(handler.apiSpec().apiKey()).isEqualTo(spec.apiKey());
            assertThat(spec.minVersion()).isEqualTo(handler.apiSpec().minVersion());
            assertThat(spec.maxVersion()).isEqualTo(handler.apiSpec().maxVersion());
        }
        assertThatThrownBy(specs::clear).isInstanceOf(UnsupportedOperationException.class);
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

    private static final class TestingApiHandler implements KafkaApiHandler<AbstractRequest> {

        private final KafkaApiSpec spec;

        private TestingApiHandler(ApiKeys apiKey, boolean advertised) {
            this.spec =
                    new KafkaApiSpec(
                            apiKey, apiKey.oldestVersion(), apiKey.latestVersion(), advertised);
        }

        @Override
        public KafkaApiSpec apiSpec() {
            return spec;
        }

        @Override
        public CompletableFuture<? extends AbstractResponse> handle(
                KafkaRequestContext context, AbstractRequest request) {
            throw new UnsupportedOperationException();
        }
    }
}

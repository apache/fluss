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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.protocol.ApiKeys;
import org.apache.fluss.server.coordinator.event.TabletServerApiVersionsEvent;
import org.apache.fluss.server.metadata.ServerInfo;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests API-version query recovery and admission through coordinator events. */
class CoordinatorApiVersionsTest extends CoordinatorEventProcessorTestBase {
    private final Map<Integer, Integer> attempts = new HashMap<>();

    @Override
    protected TestCoordinatorChannelManager createCoordinatorChannelManager() {
        return new TestCoordinatorChannelManager() {
            @Override
            public void sendApiVersionsRequest(
                    int serverId, BiConsumer<ApiVersionsResponse, ? super Throwable> callback) {
                int attempt = attempts.merge(serverId, 1, Integer::sum);
                if (attempt == 1) {
                    callback.accept(null, new IllegalStateException("temporary query failure"));
                } else {
                    callback.accept(versions(0, 0), null);
                }
            }
        };
    }

    @Test
    void testFailedQueriesRecoverAndMissingOrIncompatibleApisBlockCreation() {
        CoordinatorContext context = eventProcessor.getCoordinatorContext();
        retry(
                Duration.ofSeconds(10),
                () ->
                        assertThatCode(context::checkIndexTableCreationSupported)
                                .doesNotThrowAnyException());
        assertThat(attempts).hasSize(3);
        assertThat(attempts.values()).containsOnly(2);

        ServerInfo registration = context.getLiveTabletServers().get(0);
        for (ApiVersionsResponse incompatible :
                new ApiVersionsResponse[] {new ApiVersionsResponse(), versions(1, 1)}) {
            eventProcessor
                    .getCoordinatorEventManager()
                    .put(new TabletServerApiVersionsEvent(registration, incompatible));
            retry(
                    Duration.ofSeconds(10),
                    () ->
                            assertThatThrownBy(context::checkIndexTableCreationSupported)
                                    .hasMessageContaining("[0]"));
            // Restore support through the same event path before the next response.
            eventProcessor
                    .getCoordinatorEventManager()
                    .put(new TabletServerApiVersionsEvent(registration, versions(0, 0)));
            retry(
                    Duration.ofSeconds(10),
                    () ->
                            assertThatCode(context::checkIndexTableCreationSupported)
                                    .doesNotThrowAnyException());
        }
    }

    private static ApiVersionsResponse versions(int minVersion, int maxVersion) {
        ApiVersionsResponse response = new ApiVersionsResponse();
        response.addApiVersion()
                .setApiKey(ApiKeys.PUT_INDEX.id)
                .setMinVersion(minVersion)
                .setMaxVersion(maxVersion);
        return response;
    }
}

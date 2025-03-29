/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.admin.internal;

import com.alibaba.fluss.exception.InvalidBucketsException;
import com.alibaba.fluss.exception.InvalidOffsetException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AdminApiWorkflow}. */
public class AdminApiWorkflowTest {

    @Test
    void testNormal() throws ExecutionException, InterruptedException {
        Set<String> lookupKeys = new HashSet<>(Arrays.asList("bucket1", "bucket2"));
        AdminApiFuture.SimpleAdminApiFuture<String, Long> future =
                AdminApiFuture.forKeys(lookupKeys);
        MockLookupStrategy<String> lookupStrategy = new MockLookupStrategy<>();
        MockAdminApiHandler<String, Long> adminApiHandler =
                new MockAdminApiHandler<>(lookupStrategy);
        AdminApiWorkflow<String, Long> driver = new AdminApiWorkflow<>(future, adminApiHandler);

        lookupStrategy.expectLookupResult("bucket1", 1).expectLookupResult("bucket2", 2);
        adminApiHandler.expectResult(1, 1L).expectResult(2, 2L);
        driver.maybeSendRequests();
        assertThat(future.get("bucket1").get()).isEqualTo(1);
        assertThat(future.get("bucket2").get()).isEqualTo(2);
    }

    @Test
    void testKeyLookupFailure() {
        Set<String> lookupKeys = Collections.singleton("bucket1");
        AdminApiFuture.SimpleAdminApiFuture<String, Long> future =
                AdminApiFuture.forKeys(lookupKeys);
        MockLookupStrategy<String> lookupStrategy = new MockLookupStrategy<>();
        MockAdminApiHandler<String, Long> adminApiHandler =
                new MockAdminApiHandler<>(lookupStrategy);
        AdminApiWorkflow<String, Long> driver = new AdminApiWorkflow<>(future, adminApiHandler);
        lookupStrategy.expectLookupException(
                "bucket1", new InvalidBucketsException("Test lookup failure."));
        driver.maybeSendRequests();

        assertThatThrownBy(() -> future.get("bucket1").get())
                .cause()
                .isExactlyInstanceOf(InvalidBucketsException.class)
                .hasMessageContaining("Test lookup failure.");
    }

    @Test
    void testKeyLookupRetriable() throws ExecutionException, InterruptedException {
        Set<String> lookupKeys = new HashSet<>(Arrays.asList("bucket1", "bucket2"));
        AdminApiFuture.SimpleAdminApiFuture<String, Long> future =
                AdminApiFuture.forKeys(lookupKeys);
        MockLookupStrategy<String> lookupStrategy = new MockLookupStrategy<>();
        MockAdminApiHandler<String, Long> adminApiHandler =
                new MockAdminApiHandler<>(lookupStrategy);
        AdminApiWorkflow<String, Long> driver = new AdminApiWorkflow<>(future, adminApiHandler);

        // server of bucket1 is ready while server of bucket2 is not ready
        lookupStrategy.expectLookupResult("bucket1", 1);
        adminApiHandler.expectResult(1, 1L).expectResult(2, 2L);

        driver.maybeSendRequests();
        assertThat(future.get("bucket1").get()).isEqualTo(1);
        assertThat(future.get("bucket2").isDone()).isFalse();

        // partition of bucket2 is ready
        lookupStrategy.expectLookupResult("bucket2", 2);
        assertThat(future.get("bucket2").get()).isEqualTo(2);
    }

    @Test
    void testFulfillmentFailure() throws ExecutionException, InterruptedException {
        Set<String> lookupKeys = new HashSet<>(Arrays.asList("bucket1", "bucket2"));
        AdminApiFuture.SimpleAdminApiFuture<String, Long> future =
                AdminApiFuture.forKeys(lookupKeys);
        MockLookupStrategy<String> lookupStrategy = new MockLookupStrategy<>();
        MockAdminApiHandler<String, Long> adminApiHandler =
                new MockAdminApiHandler<>(lookupStrategy);
        AdminApiWorkflow<String, Long> driver = new AdminApiWorkflow<>(future, adminApiHandler);

        lookupStrategy.expectLookupResult("bucket1", 1).expectLookupResult("bucket2", 2);
        adminApiHandler
                .expectResult(1, 1L)
                .expectException(2, new InvalidOffsetException("Test fulfillment failure."));

        driver.maybeSendRequests();
        assertThat(future.get("bucket1").get()).isEqualTo(1);
        assertThatThrownBy(() -> future.get("bucket2").get())
                .cause()
                .isExactlyInstanceOf(InvalidOffsetException.class)
                .hasMessageContaining("Test fulfillment failure.");
    }

    @Test
    void testFulfillmentRetriable() throws ExecutionException, InterruptedException {
        Set<String> lookupKeys = new HashSet<>(Arrays.asList("bucket1", "bucket2"));
        AdminApiFuture.SimpleAdminApiFuture<String, Long> future =
                AdminApiFuture.forKeys(lookupKeys);
        MockLookupStrategy<String> lookupStrategy = new MockLookupStrategy<>();
        MockAdminApiHandler<String, Long> adminApiHandler =
                new MockAdminApiHandler<>(lookupStrategy);
        AdminApiWorkflow<String, Long> driver = new AdminApiWorkflow<>(future, adminApiHandler);

        // Though both server of bucket1 and bucket2 can be lookup, server 2 is not ready now(maybe
        // in recovery).
        lookupStrategy.expectLookupResult("bucket1", 1).expectLookupResult("bucket2", 2);
        adminApiHandler.expectResult(1, 1L);
        driver.maybeSendRequests();
        assertThat(future.get("bucket1").get()).isEqualTo(1);
        assertThat(future.get("bucket2").isDone()).isFalse();

        // server 2 is ready now.
        adminApiHandler.expectResult(2, 2L);
        assertThat(future.get("bucket2").get()).isEqualTo(2);
    }

    private static class MockLookupStrategy<K> implements TabletServerLookupStrategy<K> {
        private final Map<K, Integer> expectedLookups = new HashMap<>();
        private final Map<K, Throwable> expectedLookupExceptions = new HashMap<>();

        @Override
        public CompletableFuture<LookupServerResult<K>> lookup(Set<K> keys) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        Map<K, Integer> mappedKeys = new HashMap<>();
                        Map<K, Throwable> failedKeys = new HashMap<>();
                        for (K key : keys) {
                            if (expectedLookups.containsKey(key)) {
                                mappedKeys.put(key, expectedLookups.get(key));
                            } else if (expectedLookupExceptions.containsKey(key)) {
                                failedKeys.put(key, expectedLookupExceptions.get(key));
                            }
                        }

                        return new LookupServerResult<>(mappedKeys, failedKeys);
                    });
        }

        public MockLookupStrategy<K> expectLookupResult(K key, Integer serverId) {
            expectedLookups.put(key, serverId);
            return this;
        }

        public MockLookupStrategy<K> expectLookupException(K key, Throwable throwable) {
            expectedLookupExceptions.put(key, throwable);
            return this;
        }
    }

    private static class MockAdminApiHandler<K, V> implements AdminApiHandler<K, V> {
        private final Map<Integer, V> expectedResults = new HashMap<>();
        private final Map<Integer, Throwable> expectedExceptions = new HashMap<>();
        private final MockLookupStrategy<K> lookupStrategy;

        private MockAdminApiHandler(MockLookupStrategy<K> lookupStrategy) {
            this.lookupStrategy = lookupStrategy;
        }

        @Override
        public TabletServerLookupStrategy<K> lookupStrategy() {
            return lookupStrategy;
        }

        @Override
        public CompletableFuture<ApiResult<K, V>> handle(int node, Set<K> keys) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        Map<K, V> completedKeys = new HashMap<>();
                        Map<K, Throwable> failedKeys = new HashMap<>();
                        Set<K> unmappedKeys = new HashSet<>();
                        if (expectedResults.containsKey(node)) {
                            completedKeys.put(keys.iterator().next(), expectedResults.get(node));
                        } else if (expectedExceptions.containsKey(node)) {
                            failedKeys.put(keys.iterator().next(), expectedExceptions.get(node));
                        } else {
                            unmappedKeys.addAll(keys);
                        }
                        return new AdminApiHandler.ApiResult<K, V>(
                                completedKeys, failedKeys, unmappedKeys);
                    });
        }

        public MockAdminApiHandler<K, V> expectResult(int node, V result) {
            expectedResults.put(node, result);
            return this;
        }

        public MockAdminApiHandler<K, V> expectException(int node, Throwable exception) {
            expectedExceptions.put(node, exception);
            return this;
        }
    }
}

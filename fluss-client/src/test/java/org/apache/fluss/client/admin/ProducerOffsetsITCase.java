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

package org.apache.fluss.client.admin;

import org.apache.fluss.metadata.TableBucket;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Producer Offset Snapshot APIs.
 *
 * <p>These tests verify the end-to-end functionality of:
 *
 * <ul>
 *   <li>{@link Admin#registerProducerOffsets(String, Map)}
 *   <li>{@link Admin#getProducerOffsets(String)}
 *   <li>{@link Admin#deleteProducerOffsets(String)}
 * </ul>
 */
class ProducerOffsetsITCase extends ClientToServerITCaseBase {

    // ------------------------------------------------------------------------
    //  Basic CRUD Tests
    // ------------------------------------------------------------------------

    @Test
    void testRegisterAndGetProducerOffsets() throws Exception {
        String producerId = "test-producer-" + System.currentTimeMillis();
        Map<TableBucket, Long> offsets = createTestOffsets();

        // Register offsets
        RegisterResult result = admin.registerProducerOffsets(producerId, offsets).get();
        assertThat(result).isEqualTo(RegisterResult.CREATED);

        // Get offsets
        ProducerOffsetsResult offsetsResult = admin.getProducerOffsets(producerId).get();
        assertThat(offsetsResult).isNotNull();
        assertThat(offsetsResult.getProducerId()).isEqualTo(producerId);
        assertThat(offsetsResult.getExpirationTime()).isGreaterThan(System.currentTimeMillis());

        // Verify offsets content
        Map<Long, Map<TableBucket, Long>> tableOffsets = offsetsResult.getTableOffsets();
        assertThat(tableOffsets).isNotEmpty();

        // Flatten and verify all offsets are present
        Map<TableBucket, Long> allOffsets = flattenTableOffsets(tableOffsets);
        assertThat(allOffsets).hasSize(offsets.size());
        for (Map.Entry<TableBucket, Long> entry : offsets.entrySet()) {
            assertThat(allOffsets).containsEntry(entry.getKey(), entry.getValue());
        }

        // Cleanup
        admin.deleteProducerOffsets(producerId).get();
    }

    @Test
    void testRegisterProducerOffsetsAlreadyExists() throws Exception {
        String producerId = "test-producer-exists-" + System.currentTimeMillis();
        Map<TableBucket, Long> offsets1 = new HashMap<>();
        offsets1.put(new TableBucket(1L, 0), 100L);

        Map<TableBucket, Long> offsets2 = new HashMap<>();
        offsets2.put(new TableBucket(2L, 0), 200L);

        try {
            // First registration should succeed with CREATED
            RegisterResult result1 = admin.registerProducerOffsets(producerId, offsets1).get();
            assertThat(result1).isEqualTo(RegisterResult.CREATED);

            // Second registration should return ALREADY_EXISTS
            RegisterResult result2 = admin.registerProducerOffsets(producerId, offsets2).get();
            assertThat(result2).isEqualTo(RegisterResult.ALREADY_EXISTS);

            // Verify original offsets are preserved
            ProducerOffsetsResult offsetsResult = admin.getProducerOffsets(producerId).get();
            Map<TableBucket, Long> allOffsets =
                    flattenTableOffsets(offsetsResult.getTableOffsets());
            assertThat(allOffsets).containsKey(new TableBucket(1L, 0));
            assertThat(allOffsets).doesNotContainKey(new TableBucket(2L, 0));
        } finally {
            admin.deleteProducerOffsets(producerId).get();
        }
    }

    @Test
    void testGetProducerOffsetsNotFound() throws Exception {
        String producerId = "non-existent-producer-" + System.currentTimeMillis();

        ProducerOffsetsResult result = admin.getProducerOffsets(producerId).get();
        assertThat(result).isNull();
    }

    @Test
    void testDeleteProducerOffsets() throws Exception {
        String producerId = "test-producer-delete-" + System.currentTimeMillis();
        Map<TableBucket, Long> offsets = createTestOffsets();

        // Register offsets
        admin.registerProducerOffsets(producerId, offsets).get();
        assertThat(admin.getProducerOffsets(producerId).get()).isNotNull();

        // Delete offsets
        admin.deleteProducerOffsets(producerId).get();

        // Verify deleted
        assertThat(admin.getProducerOffsets(producerId).get()).isNull();
    }

    @Test
    void testDeleteNonExistentProducerOffsets() throws Exception {
        String producerId = "non-existent-producer-delete-" + System.currentTimeMillis();

        // Should not throw exception
        admin.deleteProducerOffsets(producerId).get();
    }

    // ------------------------------------------------------------------------
    //  Partitioned Table Tests
    // ------------------------------------------------------------------------

    @Test
    void testRegisterOffsetsWithPartitionedTable() throws Exception {
        String producerId = "test-producer-partitioned-" + System.currentTimeMillis();
        Map<TableBucket, Long> offsets = new HashMap<>();

        // Non-partitioned table
        offsets.put(new TableBucket(1L, 0), 100L);
        offsets.put(new TableBucket(1L, 1), 101L);

        // Partitioned table with different partitions
        offsets.put(new TableBucket(2L, 10L, 0), 200L);
        offsets.put(new TableBucket(2L, 10L, 1), 201L);
        offsets.put(new TableBucket(2L, 20L, 0), 300L);
        offsets.put(new TableBucket(2L, 20L, 1), 301L);

        try {
            // Register
            RegisterResult result = admin.registerProducerOffsets(producerId, offsets).get();
            assertThat(result).isEqualTo(RegisterResult.CREATED);

            // Get and verify
            ProducerOffsetsResult offsetsResult = admin.getProducerOffsets(producerId).get();
            assertThat(offsetsResult).isNotNull();

            Map<TableBucket, Long> allOffsets =
                    flattenTableOffsets(offsetsResult.getTableOffsets());
            assertThat(allOffsets).hasSize(6);

            // Verify non-partitioned table offsets
            assertThat(allOffsets.get(new TableBucket(1L, 0))).isEqualTo(100L);
            assertThat(allOffsets.get(new TableBucket(1L, 1))).isEqualTo(101L);

            // Verify partitioned table offsets
            assertThat(allOffsets.get(new TableBucket(2L, 10L, 0))).isEqualTo(200L);
            assertThat(allOffsets.get(new TableBucket(2L, 10L, 1))).isEqualTo(201L);
            assertThat(allOffsets.get(new TableBucket(2L, 20L, 0))).isEqualTo(300L);
            assertThat(allOffsets.get(new TableBucket(2L, 20L, 1))).isEqualTo(301L);
        } finally {
            admin.deleteProducerOffsets(producerId).get();
        }
    }

    // ------------------------------------------------------------------------
    //  Concurrent Registration Tests
    // ------------------------------------------------------------------------

    @Test
    void testConcurrentRegistrationSameProducer() throws Exception {
        String producerId = "test-concurrent-same-" + System.currentTimeMillis();
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger createdCount = new AtomicInteger(0);
        AtomicInteger existsCount = new AtomicInteger(0);

        try {
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                startLatch.await();
                                Map<TableBucket, Long> offsets = new HashMap<>();
                                offsets.put(new TableBucket(1L, threadId), (long) threadId);

                                RegisterResult result =
                                        admin.registerProducerOffsets(producerId, offsets).get();
                                if (result == RegisterResult.CREATED) {
                                    createdCount.incrementAndGet();
                                } else {
                                    existsCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                doneLatch.countDown();
                            }
                        },
                        executor);
            }

            // Start all threads simultaneously
            startLatch.countDown();
            assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();

            // Exactly one should succeed with CREATED
            assertThat(createdCount.get()).isEqualTo(1);
            assertThat(existsCount.get()).isEqualTo(numThreads - 1);

            // Verify snapshot exists
            assertThat(admin.getProducerOffsets(producerId).get()).isNotNull();
        } finally {
            executor.shutdown();
            admin.deleteProducerOffsets(producerId).get();
        }
    }

    @Test
    void testConcurrentRegistrationDifferentProducers() throws Exception {
        String prefix = "test-concurrent-diff-" + System.currentTimeMillis() + "-";
        int numProducers = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numProducers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numProducers);
        AtomicInteger successCount = new AtomicInteger(0);
        List<String> producerIds = new ArrayList<>();

        try {
            for (int i = 0; i < numProducers; i++) {
                final int producerNum = i;
                final String producerId = prefix + producerNum;
                producerIds.add(producerId);

                CompletableFuture.runAsync(
                        () -> {
                            try {
                                startLatch.await();
                                Map<TableBucket, Long> offsets = new HashMap<>();
                                offsets.put(new TableBucket(1L, producerNum), (long) producerNum);

                                RegisterResult result =
                                        admin.registerProducerOffsets(producerId, offsets).get();
                                if (result == RegisterResult.CREATED) {
                                    successCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                doneLatch.countDown();
                            }
                        },
                        executor);
            }

            // Start all threads simultaneously
            startLatch.countDown();
            assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();

            // All should succeed since they're different producers
            assertThat(successCount.get()).isEqualTo(numProducers);

            // Verify all snapshots exist
            for (String producerId : producerIds) {
                assertThat(admin.getProducerOffsets(producerId).get()).isNotNull();
            }
        } finally {
            executor.shutdown();
            // Cleanup all producers
            for (String producerId : producerIds) {
                admin.deleteProducerOffsets(producerId).get();
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Edge Cases
    // ------------------------------------------------------------------------

    @Test
    void testRegisterEmptyOffsets() throws Exception {
        String producerId = "test-producer-empty-" + System.currentTimeMillis();
        Map<TableBucket, Long> offsets = new HashMap<>();

        try {
            RegisterResult result = admin.registerProducerOffsets(producerId, offsets).get();
            assertThat(result).isEqualTo(RegisterResult.CREATED);

            ProducerOffsetsResult offsetsResult = admin.getProducerOffsets(producerId).get();
            assertThat(offsetsResult).isNotNull();
            assertThat(offsetsResult.getTableOffsets()).isEmpty();
        } finally {
            admin.deleteProducerOffsets(producerId).get();
        }
    }

    @Test
    void testRegisterLargeNumberOfOffsets() throws Exception {
        String producerId = "test-producer-large-" + System.currentTimeMillis();
        Map<TableBucket, Long> offsets = new HashMap<>();

        // Create offsets for 100 tables with 10 buckets each
        for (long tableId = 1; tableId <= 100; tableId++) {
            for (int bucket = 0; bucket < 10; bucket++) {
                offsets.put(new TableBucket(tableId, bucket), tableId * 1000 + bucket);
            }
        }

        try {
            RegisterResult result = admin.registerProducerOffsets(producerId, offsets).get();
            assertThat(result).isEqualTo(RegisterResult.CREATED);

            ProducerOffsetsResult offsetsResult = admin.getProducerOffsets(producerId).get();
            assertThat(offsetsResult).isNotNull();

            Map<TableBucket, Long> allOffsets =
                    flattenTableOffsets(offsetsResult.getTableOffsets());
            assertThat(allOffsets).hasSize(1000);

            // Verify some random offsets
            assertThat(allOffsets.get(new TableBucket(1L, 0))).isEqualTo(1000L);
            assertThat(allOffsets.get(new TableBucket(50L, 5))).isEqualTo(50005L);
            assertThat(allOffsets.get(new TableBucket(100L, 9))).isEqualTo(100009L);
        } finally {
            admin.deleteProducerOffsets(producerId).get();
        }
    }

    // ------------------------------------------------------------------------
    //  Helper Methods
    // ------------------------------------------------------------------------

    private Map<TableBucket, Long> createTestOffsets() {
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);
        offsets.put(new TableBucket(1L, 1), 200L);
        offsets.put(new TableBucket(2L, 0), 300L);
        return offsets;
    }

    private Map<TableBucket, Long> flattenTableOffsets(
            Map<Long, Map<TableBucket, Long>> tableOffsets) {
        Map<TableBucket, Long> result = new HashMap<>();
        for (Map<TableBucket, Long> bucketOffsets : tableOffsets.values()) {
            result.putAll(bucketOffsets);
        }
        return result;
    }
}

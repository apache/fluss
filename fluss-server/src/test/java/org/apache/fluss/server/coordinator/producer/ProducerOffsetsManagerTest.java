/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.producer;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidProducerIdException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.producer.ProducerOffsets;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ProducerOffsetsManager}. */
class ProducerOffsetsManagerTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(new Configuration())
                    .build();

    @TempDir Path tempDir;

    private ProducerOffsetsManager manager;
    private ProducerOffsetsStore store;
    private ZooKeeperClient zkClient;

    private static final long DEFAULT_TTL_MS = 3600000; // 1 hour
    private static final long CLEANUP_INTERVAL_MS = 60000; // 1 minute

    @BeforeEach
    void setUp() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        store = new ProducerOffsetsStore(zkClient, tempDir.toString());
        manager = new ProducerOffsetsManager(store, DEFAULT_TTL_MS, CLEANUP_INTERVAL_MS);
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.close();
        }
    }

    // ------------------------------------------------------------------------
    //  Basic Registration Tests
    // ------------------------------------------------------------------------

    @Test
    void testRegisterSnapshotSuccess() throws Exception {
        String producerId = "test-manager-register";
        Map<TableBucket, Long> offsets = createTestOffsets();

        boolean created = manager.registerSnapshot(producerId, offsets, null);

        assertThat(created).isTrue();
        Optional<ProducerOffsets> snapshot = manager.getOffsetsMetadata(producerId);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getExpirationTime())
                .isGreaterThanOrEqualTo(System.currentTimeMillis());
    }

    @Test
    void testRegisterSnapshotAlreadyExists() throws Exception {
        String producerId = "test-manager-exists";
        Map<TableBucket, Long> offsets1 = createTestOffsets();
        Map<TableBucket, Long> offsets2 = new HashMap<>();
        offsets2.put(new TableBucket(99L, 0), 999L);

        // First registration should succeed
        assertThat(manager.registerSnapshot(producerId, offsets1, null)).isTrue();

        // Second registration should return false (already exists)
        assertThat(manager.registerSnapshot(producerId, offsets2, null)).isFalse();

        // Original offsets should be preserved
        Map<TableBucket, Long> retrieved = manager.readOffsets(producerId);
        assertThat(retrieved).containsKey(new TableBucket(1L, 0));
        assertThat(retrieved).doesNotContainKey(new TableBucket(99L, 0));
    }

    @Test
    void testRegisterSnapshotWithCustomTtl() throws Exception {
        String producerId = "test-manager-custom-ttl";
        Map<TableBucket, Long> offsets = createTestOffsets();
        long customTtlMs = 1000; // 1 second

        long beforeRegister = System.currentTimeMillis();
        manager.registerSnapshot(producerId, offsets, customTtlMs);
        long afterRegister = System.currentTimeMillis();

        Optional<ProducerOffsets> snapshot = manager.getOffsetsMetadata(producerId);
        assertThat(snapshot).isPresent();
        long expirationTime = snapshot.get().getExpirationTime();
        assertThat(expirationTime).isGreaterThanOrEqualTo(beforeRegister + customTtlMs);
        assertThat(expirationTime).isLessThanOrEqualTo(afterRegister + customTtlMs);
    }

    // ------------------------------------------------------------------------
    //  Expired Snapshot Replacement Tests
    // ------------------------------------------------------------------------

    @Test
    void testRegisterSnapshotReplacesExpired() throws Exception {
        String producerId = "test-manager-replace-expired";
        Map<TableBucket, Long> offsets1 = new HashMap<>();
        offsets1.put(new TableBucket(1L, 0), 100L);

        Map<TableBucket, Long> offsets2 = new HashMap<>();
        offsets2.put(new TableBucket(2L, 0), 200L);

        // Register with very short TTL (already expired)
        long expiredTtlMs = -1000; // Already expired
        store.tryStoreOffsets(producerId, offsets1, System.currentTimeMillis() + expiredTtlMs);

        // Verify it's expired
        Optional<ProducerOffsets> expiredSnapshot = manager.getOffsetsMetadata(producerId);
        assertThat(expiredSnapshot).isPresent();
        assertThat(expiredSnapshot.get().isExpired(System.currentTimeMillis())).isTrue();

        // New registration should replace the expired one
        boolean created = manager.registerSnapshot(producerId, offsets2, null);
        assertThat(created).isTrue();

        // Verify new offsets
        Map<TableBucket, Long> retrieved = manager.readOffsets(producerId);
        assertThat(retrieved).containsKey(new TableBucket(2L, 0));
        assertThat(retrieved).doesNotContainKey(new TableBucket(1L, 0));
    }

    // ------------------------------------------------------------------------
    //  Get and Delete Tests
    // ------------------------------------------------------------------------

    @Test
    void testGetOffsetsMetadata() throws Exception {
        String producerId = "test-manager-get-metadata";
        Map<TableBucket, Long> offsets = createTestOffsets();

        manager.registerSnapshot(producerId, offsets, null);

        Optional<ProducerOffsets> snapshot = manager.getOffsetsMetadata(producerId);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getTableOffsets()).isNotEmpty();
    }

    @Test
    void testGetOffsetsMetadataNonExistent() throws Exception {
        Optional<ProducerOffsets> snapshot = manager.getOffsetsMetadata("non-existent-producer");
        assertThat(snapshot).isEmpty();
    }

    @Test
    void testReadOffsets() throws Exception {
        String producerId = "test-manager-get-offsets";
        Map<TableBucket, Long> offsets = createTestOffsets();

        manager.registerSnapshot(producerId, offsets, null);

        Map<TableBucket, Long> retrieved = manager.readOffsets(producerId);
        assertThat(retrieved).hasSize(offsets.size());
        assertThat(retrieved.get(new TableBucket(1L, 0))).isEqualTo(100L);
    }

    @Test
    void testReadOffsetsNonExistent() throws Exception {
        Map<TableBucket, Long> offsets = manager.readOffsets("non-existent-producer");
        assertThat(offsets).isEmpty();
    }

    @Test
    void testDeleteSnapshot() throws Exception {
        String producerId = "test-manager-delete";
        Map<TableBucket, Long> offsets = createTestOffsets();

        manager.registerSnapshot(producerId, offsets, null);
        assertThat(manager.getOffsetsMetadata(producerId)).isPresent();

        manager.deleteSnapshot(producerId);

        assertThat(manager.getOffsetsMetadata(producerId)).isEmpty();
        assertThat(manager.readOffsets(producerId)).isEmpty();
    }

    @Test
    void testDeleteNonExistentSnapshot() throws Exception {
        // Should not throw
        manager.deleteSnapshot("non-existent-producer");
    }

    // ------------------------------------------------------------------------
    //  Cleanup Tests
    // ------------------------------------------------------------------------

    @Test
    void testCleanupExpiredSnapshots() throws Exception {
        String expiredProducerId = "test-cleanup-expired";
        String validProducerId = "test-cleanup-valid";

        // Create an expired snapshot
        Map<TableBucket, Long> offsets = createTestOffsets();
        store.tryStoreOffsets(expiredProducerId, offsets, System.currentTimeMillis() - 1000);

        // Create a valid snapshot
        manager.registerSnapshot(validProducerId, offsets, null);

        // Run cleanup
        int cleanedCount = manager.cleanupExpiredSnapshots();

        assertThat(cleanedCount).isEqualTo(1);
        assertThat(manager.getOffsetsMetadata(expiredProducerId)).isEmpty();
        assertThat(manager.getOffsetsMetadata(validProducerId)).isPresent();
    }

    @Test
    void testGetDefaultTtlMs() {
        assertThat(manager.getDefaultTtlMs()).isEqualTo(DEFAULT_TTL_MS);
    }

    // ------------------------------------------------------------------------
    //  Producer ID Validation Tests
    // ------------------------------------------------------------------------

    @Test
    void testRegisterSnapshotWithInvalidProducerId() {
        Map<TableBucket, Long> offsets = createTestOffsets();

        // Null producer ID
        assertThatThrownBy(() -> manager.registerSnapshot(null, offsets, null))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID")
                .hasMessageContaining("null string is not allowed");

        // Empty producer ID
        assertThatThrownBy(() -> manager.registerSnapshot("", offsets, null))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID")
                .hasMessageContaining("empty string is not allowed");

        // Producer ID with invalid characters
        assertThatThrownBy(() -> manager.registerSnapshot("invalid/producer", offsets, null))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID")
                .hasMessageContaining("contains one or more characters");

        // Producer ID as "."
        assertThatThrownBy(() -> manager.registerSnapshot(".", offsets, null))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID")
                .hasMessageContaining("'.' is not allowed");

        // Producer ID as ".."
        assertThatThrownBy(() -> manager.registerSnapshot("..", offsets, null))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID")
                .hasMessageContaining("'..' is not allowed");
    }

    @Test
    void testGetOffsetsMetadataWithInvalidProducerId() {
        // Null producer ID
        assertThatThrownBy(() -> manager.getOffsetsMetadata(null))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID");

        // Invalid characters
        assertThatThrownBy(() -> manager.getOffsetsMetadata("invalid@producer"))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID");
    }

    @Test
    void testReadOffsetsWithInvalidProducerId() {
        // Null producer ID
        assertThatThrownBy(() -> manager.readOffsets(null))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID");

        // Invalid characters
        assertThatThrownBy(() -> manager.readOffsets("invalid producer"))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID");
    }

    @Test
    void testDeleteSnapshotWithInvalidProducerId() {
        // Null producer ID
        assertThatThrownBy(() -> manager.deleteSnapshot(null))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID");

        // Invalid characters
        assertThatThrownBy(() -> manager.deleteSnapshot("invalid:producer"))
                .isInstanceOf(InvalidProducerIdException.class)
                .hasMessageContaining("Invalid producer ID");
    }

    // ------------------------------------------------------------------------
    //  Concurrent Registration Tests
    // ------------------------------------------------------------------------

    @Test
    void testConcurrentRegistrationSameProducer() throws Exception {
        String producerId = "test-concurrent-same";
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger createdCount = new AtomicInteger(0);
        AtomicInteger existsCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            Map<TableBucket, Long> offsets = new HashMap<>();
                            offsets.put(new TableBucket(1L, threadId), (long) threadId);

                            boolean created = manager.registerSnapshot(producerId, offsets, null);
                            if (created) {
                                createdCount.incrementAndGet();
                            } else {
                                existsCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // Exactly one should succeed with CREATED
        assertThat(createdCount.get()).isEqualTo(1);
        assertThat(existsCount.get()).isEqualTo(numThreads - 1);

        // Verify snapshot exists
        assertThat(manager.getOffsetsMetadata(producerId)).isPresent();
    }

    @Test
    void testConcurrentRegistrationDifferentProducers() throws Exception {
        int numProducers = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numProducers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numProducers);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numProducers; i++) {
            final int producerNum = i;
            executor.submit(
                    () -> {
                        try {
                            startLatch.await();
                            String producerId = "test-concurrent-diff-" + producerNum;
                            Map<TableBucket, Long> offsets = new HashMap<>();
                            offsets.put(new TableBucket(1L, producerNum), (long) producerNum);

                            boolean created = manager.registerSnapshot(producerId, offsets, null);
                            if (created) {
                                successCount.incrementAndGet();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            doneLatch.countDown();
                        }
                    });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // All should succeed since they're different producers
        assertThat(successCount.get()).isEqualTo(numProducers);

        // Verify all snapshots exist
        for (int i = 0; i < numProducers; i++) {
            assertThat(manager.getOffsetsMetadata("test-concurrent-diff-" + i)).isPresent();
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
}

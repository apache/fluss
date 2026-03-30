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
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.producer.ProducerOffsets;
import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ProducerOffsetsStore}. */
class ProducerOffsetsStoreTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setClusterConf(new Configuration())
                    .build();

    @TempDir Path tempDir;

    private ProducerOffsetsStore store;
    private ZooKeeperClient zkClient;

    @BeforeEach
    void setUp() {
        zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        store = new ProducerOffsetsStore(zkClient, tempDir.toString());
    }

    @Test
    void testTryStoreOffsetsSuccess() throws Exception {
        String producerId = "test-producer-store";
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);
        offsets.put(new TableBucket(1L, 1), 200L);
        offsets.put(new TableBucket(2L, 0), 300L);

        long expirationTime = System.currentTimeMillis() + 3600000;

        // First store should succeed
        boolean created = store.tryStoreOffsets(producerId, offsets, expirationTime);
        assertThat(created).isTrue();

        // Verify metadata was stored
        Optional<ProducerOffsets> snapshot = store.getOffsetsMetadata(producerId);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getExpirationTime()).isEqualTo(expirationTime);
        assertThat(snapshot.get().getTableOffsets()).hasSize(2); // 2 tables
    }

    @Test
    void testTryStoreOffsetsAlreadyExists() throws Exception {
        String producerId = "test-producer-exists";
        Map<TableBucket, Long> offsets1 = new HashMap<>();
        offsets1.put(new TableBucket(1L, 0), 100L);

        Map<TableBucket, Long> offsets2 = new HashMap<>();
        offsets2.put(new TableBucket(1L, 0), 999L);

        long expirationTime = System.currentTimeMillis() + 3600000;

        // First store should succeed
        assertThat(store.tryStoreOffsets(producerId, offsets1, expirationTime)).isTrue();

        // Second store should return false (already exists)
        assertThat(store.tryStoreOffsets(producerId, offsets2, expirationTime)).isFalse();

        // Original offsets should be preserved
        Map<TableBucket, Long> retrieved = store.readOffsets(producerId);
        assertThat(retrieved).isEqualTo(offsets1);
    }

    @Test
    void testReadOffsets() throws Exception {
        String producerId = "test-producer-read";
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);
        offsets.put(new TableBucket(1L, 1), 200L);
        offsets.put(new TableBucket(2L, 0), 300L);
        offsets.put(new TableBucket(3L, 100L, 0), 400L); // partitioned table

        long expirationTime = System.currentTimeMillis() + 3600000;
        store.tryStoreOffsets(producerId, offsets, expirationTime);

        // Read offsets back
        Map<TableBucket, Long> retrieved = store.readOffsets(producerId);
        assertThat(retrieved).isEqualTo(offsets);
    }

    @Test
    void testReadOffsetsNonExistent() throws Exception {
        Map<TableBucket, Long> offsets = store.readOffsets("non-existent-producer");
        assertThat(offsets).isEmpty();
    }

    @Test
    void testDeleteSnapshot() throws Exception {
        String producerId = "test-producer-delete";
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);

        long expirationTime = System.currentTimeMillis() + 3600000;
        store.tryStoreOffsets(producerId, offsets, expirationTime);

        // Verify it exists
        assertThat(store.getOffsetsMetadata(producerId)).isPresent();

        // Delete
        store.deleteSnapshot(producerId);

        // Verify it's gone
        assertThat(store.getOffsetsMetadata(producerId)).isEmpty();
        assertThat(store.readOffsets(producerId)).isEmpty();
    }

    @Test
    void testDeleteNonExistentSnapshot() throws Exception {
        // Should not throw
        store.deleteSnapshot("non-existent-producer");
    }

    @Test
    void testListProducerIds() throws Exception {
        // Use unique prefix to avoid conflicts with other tests
        String prefix = "list-test-" + System.currentTimeMillis() + "-";

        // Create multiple snapshots
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);
        long expirationTime = System.currentTimeMillis() + 3600000;

        store.tryStoreOffsets(prefix + "producer-1", offsets, expirationTime);
        store.tryStoreOffsets(prefix + "producer-2", offsets, expirationTime);
        store.tryStoreOffsets(prefix + "producer-3", offsets, expirationTime);

        List<String> producerIds = store.listProducerIds();
        assertThat(producerIds)
                .contains(prefix + "producer-1", prefix + "producer-2", prefix + "producer-3");
    }

    @Test
    void testGetProducersDirectory() {
        FsPath producersDir = store.getProducersDirectory();
        assertThat(producersDir.toString()).isEqualTo(tempDir.toString() + "/producers");
    }

    @Test
    void testSnapshotWithPartitionedTable() throws Exception {
        String producerId = "test-producer-partitioned";
        Map<TableBucket, Long> offsets = new HashMap<>();
        // Non-partitioned table
        offsets.put(new TableBucket(1L, 0), 100L);
        // Partitioned table with different partitions
        offsets.put(new TableBucket(2L, 10L, 0), 200L);
        offsets.put(new TableBucket(2L, 10L, 1), 201L);
        offsets.put(new TableBucket(2L, 20L, 0), 300L);

        long expirationTime = System.currentTimeMillis() + 3600000;
        store.tryStoreOffsets(producerId, offsets, expirationTime);

        Map<TableBucket, Long> retrieved = store.readOffsets(producerId);
        assertThat(retrieved).isEqualTo(offsets);
    }

    @Test
    void testSnapshotExpiration() throws Exception {
        String producerId = "test-producer-expiration";
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);

        // Create snapshot that expires in the past
        long pastExpirationTime = System.currentTimeMillis() - 1000;
        store.tryStoreOffsets(producerId, offsets, pastExpirationTime);

        Optional<ProducerOffsets> snapshot = store.getOffsetsMetadata(producerId);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().isExpired(System.currentTimeMillis())).isTrue();
    }

    @Test
    void testEmptyOffsets() throws Exception {
        String producerId = "test-producer-empty";
        Map<TableBucket, Long> offsets = new HashMap<>();

        long expirationTime = System.currentTimeMillis() + 3600000;
        store.tryStoreOffsets(producerId, offsets, expirationTime);

        Optional<ProducerOffsets> snapshot = store.getOffsetsMetadata(producerId);
        assertThat(snapshot).isPresent();
        assertThat(snapshot.get().getTableOffsets()).isEmpty();

        Map<TableBucket, Long> retrieved = store.readOffsets(producerId);
        assertThat(retrieved).isEmpty();
    }

    // ------------------------------------------------------------------------
    //  Version Control Tests
    // ------------------------------------------------------------------------

    @Test
    void testGetOffsetsMetadataWithVersion() throws Exception {
        String producerId = "test-producer-version";
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);

        long expirationTime = System.currentTimeMillis() + 3600000;
        store.tryStoreOffsets(producerId, offsets, expirationTime);

        // Get with version
        Optional<Tuple2<ProducerOffsets, Integer>> result =
                store.getOffsetsMetadataWithVersion(producerId);

        assertThat(result).isPresent();
        assertThat(result.get().f0.getExpirationTime()).isEqualTo(expirationTime);
        assertThat(result.get().f1).isGreaterThanOrEqualTo(0); // ZK version starts at 0
    }

    @Test
    void testGetOffsetsMetadataWithVersionNonExistent() throws Exception {
        Optional<Tuple2<ProducerOffsets, Integer>> result =
                store.getOffsetsMetadataWithVersion("non-existent-producer");

        assertThat(result).isEmpty();
    }

    @Test
    void testDeleteSnapshotIfVersionSuccess() throws Exception {
        String producerId = "test-producer-delete-version";
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);

        long expirationTime = System.currentTimeMillis() + 3600000;
        store.tryStoreOffsets(producerId, offsets, expirationTime);

        // Get current version
        Optional<Tuple2<ProducerOffsets, Integer>> result =
                store.getOffsetsMetadataWithVersion(producerId);
        assertThat(result).isPresent();
        int version = result.get().f1;
        ProducerOffsets snapshot = result.get().f0;

        // Delete with correct version should succeed
        boolean deleted = store.deleteSnapshotIfVersion(producerId, snapshot, version);
        assertThat(deleted).isTrue();

        // Verify deleted
        assertThat(store.getOffsetsMetadata(producerId)).isEmpty();
    }

    @Test
    void testDeleteSnapshotIfVersionMismatch() throws Exception {
        String producerId = "test-producer-version-mismatch";
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);

        long expirationTime = System.currentTimeMillis() + 3600000;
        store.tryStoreOffsets(producerId, offsets, expirationTime);

        // Get current version
        Optional<Tuple2<ProducerOffsets, Integer>> result =
                store.getOffsetsMetadataWithVersion(producerId);
        assertThat(result).isPresent();
        ProducerOffsets snapshot = result.get().f0;

        // Delete with wrong version should fail
        int wrongVersion = 999;
        boolean deleted = store.deleteSnapshotIfVersion(producerId, snapshot, wrongVersion);
        assertThat(deleted).isFalse();

        // Snapshot should still exist
        assertThat(store.getOffsetsMetadata(producerId)).isPresent();
    }

    @Test
    void testDeleteSnapshotIfVersionAlreadyDeleted() throws Exception {
        String producerId = "test-producer-already-deleted";
        Map<TableBucket, Long> offsets = new HashMap<>();
        offsets.put(new TableBucket(1L, 0), 100L);

        long expirationTime = System.currentTimeMillis() + 3600000;
        store.tryStoreOffsets(producerId, offsets, expirationTime);

        // Get snapshot info
        Optional<Tuple2<ProducerOffsets, Integer>> result =
                store.getOffsetsMetadataWithVersion(producerId);
        assertThat(result).isPresent();
        ProducerOffsets snapshot = result.get().f0;

        // Delete first
        store.deleteSnapshot(producerId);

        // Delete again with version should return true (already deleted is success)
        boolean deleted = store.deleteSnapshotIfVersion(producerId, snapshot, 0);
        assertThat(deleted).isTrue();
    }
}

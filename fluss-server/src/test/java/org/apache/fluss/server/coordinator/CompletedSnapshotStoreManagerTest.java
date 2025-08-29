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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandleStore;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import org.apache.fluss.server.testutils.KvTestUtils;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Test for {@link CompletedSnapshotStoreManager}. */
class CompletedSnapshotStoreManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;

    private static ZooKeeperCompletedSnapshotHandleStore completedSnapshotHandleStore;

    private @TempDir Path tempDir;

    private static ExecutorService ioExecutor;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        completedSnapshotHandleStore = new ZooKeeperCompletedSnapshotHandleStore(zookeeperClient);
        ioExecutor = Executors.newFixedThreadPool(1);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @AfterAll
    static void afterAll() {
        ioExecutor.shutdownNow();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testCompletedSnapshotStoreManage(int maxNumberOfSnapshotsToRetain) throws Exception {
        // create a manager for completed snapshot store
        CompletedSnapshotStoreManager completedSnapshotStoreManager =
                createCompletedSnapshotStoreManager(maxNumberOfSnapshotsToRetain);

        // add snapshots for a series of buckets
        Set<TableBucket> tableBuckets = createTableBuckets(2, 3);
        int snapshotNum = 3;

        Map<TableBucket, CompletedSnapshot> tableBucketLatestCompletedSnapshots = new HashMap<>();
        for (TableBucket tableBucket : tableBuckets) {
            // add some snapshots
            for (int snapshot = 0; snapshot < snapshotNum; snapshot++) {
                CompletedSnapshot completedSnapshot =
                        KvTestUtils.mockCompletedSnapshot(tempDir, tableBucket, snapshot);
                addCompletedSnapshot(completedSnapshotStoreManager, completedSnapshot);

                // check gotten completed snapshot
                assertThat(getCompletedSnapshot(tableBucket, snapshot))
                        .isEqualTo(completedSnapshot);
                tableBucketLatestCompletedSnapshots.put(tableBucket, completedSnapshot);
            }
            // check has retain number of snapshots
            assertThat(
                            completedSnapshotStoreManager
                                    .getOrCreateCompletedSnapshotStore(tableBucket)
                                    .getAllSnapshots())
                    .hasSize(maxNumberOfSnapshotsToRetain);
        }

        // we create another table bucket snapshot manager
        completedSnapshotStoreManager =
                createCompletedSnapshotStoreManager(maxNumberOfSnapshotsToRetain);

        for (TableBucket tableBucket : tableBucketLatestCompletedSnapshots.keySet()) {
            // get latest snapshot
            CompletedSnapshot completedSnapshot =
                    getLatestCompletedSnapshot(completedSnapshotStoreManager, tableBucket);
            // check snapshot
            assertThat(completedSnapshot)
                    .isEqualTo(tableBucketLatestCompletedSnapshots.get(tableBucket));

            // add a new snapshot
            long snapshotId = completedSnapshot.getSnapshotID() + 1;
            completedSnapshot = KvTestUtils.mockCompletedSnapshot(tempDir, tableBucket, snapshotId);
            addCompletedSnapshot(completedSnapshotStoreManager, completedSnapshot);
            // check gotten completed snapshot
            assertThat(getCompletedSnapshot(tableBucket, snapshotId)).isEqualTo(completedSnapshot);

            // check has retain number of snapshots
            assertThat(
                            completedSnapshotStoreManager
                                    .getOrCreateCompletedSnapshotStore(tableBucket)
                                    .getAllSnapshots())
                    .hasSize(maxNumberOfSnapshotsToRetain);
        }

        // for other unknown buckets, snapshots should be empty
        TableBucket nonExistBucket = new TableBucket(10, 100);
        assertThat(
                        completedSnapshotStoreManager
                                .getOrCreateCompletedSnapshotStore(nonExistBucket)
                                .getAllSnapshots())
                .hasSize(0);
    }

    @Test
    void testRemoveCompletedSnapshotStoreFromManager() throws Exception {
        CompletedSnapshotStoreManager completedSnapshotStoreManager =
                createCompletedSnapshotStoreManager(10);
        Set<TableBucket> tableBuckets = createTableBuckets(1, 2);
        int snapshotNum = 3;
        for (TableBucket tableBucket : tableBuckets) {
            // add some snapshots
            for (int snapshot = 0; snapshot < snapshotNum; snapshot++) {
                CompletedSnapshot completedSnapshot =
                        KvTestUtils.mockCompletedSnapshot(tempDir, tableBucket, snapshot);
                addCompletedSnapshot(completedSnapshotStoreManager, completedSnapshot);
            }
        }
        // before remove CompletedSnapshotStore
        assertThat(completedSnapshotStoreManager.getBucketCompletedSnapshotStores().size())
                .isEqualTo(2);
        // after remove CompletedSnapshotStore
        completedSnapshotStoreManager.removeCompletedSnapshotStoreByTableBuckets(tableBuckets);
        assertThat(completedSnapshotStoreManager.getBucketCompletedSnapshotStores()).isEmpty();
    }

    @Test
    void testMetadataInconsistencyWithMetadataNotExistsException() throws Exception {
        // setup test data with mixed valid and invalid snapshots
        TableBucket tableBucket = new TableBucket(1, 1);
        CompletedSnapshot validSnapshot =
                KvTestUtils.mockCompletedSnapshot(tempDir, tableBucket, 1L);
        CompletedSnapshotHandle validSnapshotHandle = mock(CompletedSnapshotHandle.class);
        when(validSnapshotHandle.getSnapshotId()).thenReturn(validSnapshot.getSnapshotID());
        when(validSnapshotHandle.getMetadataFilePath())
                .thenReturn(validSnapshot.getMetadataFilePath());
        when(validSnapshotHandle.retrieveCompleteSnapshot()).thenReturn(validSnapshot);

        CompletedSnapshot invalidSnapshot =
                KvTestUtils.mockCompletedSnapshot(tempDir, tableBucket, 2L);
        CompletedSnapshotHandle invalidSnapshotHandle = mock(CompletedSnapshotHandle.class);
        when(invalidSnapshotHandle.getSnapshotId()).thenReturn(invalidSnapshot.getSnapshotID());
        when(invalidSnapshotHandle.getMetadataFilePath())
                .thenReturn(invalidSnapshot.getMetadataFilePath());
        when(invalidSnapshotHandle.retrieveCompleteSnapshot())
                .thenThrow(
                        new FileNotFoundException(
                                CompletedSnapshot.SNAPSHOT_DATA_NOT_EXISTS_ERROR_MESSAGE));

        // mock CompletedSnapshotHandleStore
        CompletedSnapshotHandleStore mockCompletedSnapshotStore =
                new InMemoryCompletedSnapshotHandleStore();
        mockCompletedSnapshotStore.add(tableBucket, 1L, validSnapshotHandle);
        mockCompletedSnapshotStore.add(tableBucket, 2L, invalidSnapshotHandle);

        // CompletedSnapshotStoreManager
        CompletedSnapshotStoreManager completedSnapshotStoreManager =
                new CompletedSnapshotStoreManager(
                        10,
                        ioExecutor,
                        zookeeperClient,
                        zooKeeperClient -> mockCompletedSnapshotStore);

        // Verify that only the valid snapshot remains
        CompletedSnapshotStore completedSnapshotStore =
                completedSnapshotStoreManager.getOrCreateCompletedSnapshotStore(tableBucket);
        assertThat(completedSnapshotStore.getAllSnapshots()).hasSize(1);
        assertThat(completedSnapshotStore.getAllSnapshots().get(0).getSnapshotID()).isEqualTo(1L);
    }

    private CompletedSnapshotStoreManager createCompletedSnapshotStoreManager(
            int maxNumberOfSnapshotsToRetain) {
        return new CompletedSnapshotStoreManager(
                maxNumberOfSnapshotsToRetain, ioExecutor, zookeeperClient);
    }

    private CompletedSnapshot getLatestCompletedSnapshot(
            CompletedSnapshotStoreManager completedSnapshotStoreManager, TableBucket tableBucket) {
        CompletedSnapshotStore completedSnapshotStore =
                completedSnapshotStoreManager.getOrCreateCompletedSnapshotStore(tableBucket);
        return completedSnapshotStore.getLatestSnapshot().get();
    }

    private void addCompletedSnapshot(
            CompletedSnapshotStoreManager completedSnapshotStoreManager,
            CompletedSnapshot completedSnapshot)
            throws Exception {
        TableBucket tableBucket = completedSnapshot.getTableBucket();
        CompletedSnapshotStore completedSnapshotStore =
                completedSnapshotStoreManager.getOrCreateCompletedSnapshotStore(tableBucket);
        completedSnapshotStore.add(completedSnapshot);
    }

    private CompletedSnapshot getCompletedSnapshot(TableBucket tableBucket, long snapshotId)
            throws Exception {
        return completedSnapshotHandleStore
                .get(tableBucket, snapshotId)
                .map(
                        t -> {
                            try {
                                return t.retrieveCompleteSnapshot();
                            } catch (IOException e) {
                                throw new RuntimeException(
                                        "Fail to retrieve completed snapshot.", e);
                            }
                        })
                .orElse(null);
    }

    private Set<TableBucket> createTableBuckets(int tableNum, int bucketNum) {
        Set<TableBucket> tableBuckets = new HashSet<>();
        for (int tableId = 0; tableId < tableNum; tableId++) {
            for (int bucketId = 0; bucketId < bucketNum; bucketId++) {
                tableBuckets.add(new TableBucket(tableId, bucketId));
            }
        }
        return tableBuckets;
    }

    private static class InMemoryCompletedSnapshotHandleStore
            implements CompletedSnapshotHandleStore {
        private final Map<TableBucket, Map<Long, CompletedSnapshotHandle>> snapshotHandleMap =
                new HashMap<>();

        @Override
        public void add(
                TableBucket tableBucket,
                long snapshotId,
                CompletedSnapshotHandle completedSnapshotHandle)
                throws Exception {
            snapshotHandleMap
                    .computeIfAbsent(tableBucket, k -> new HashMap<>())
                    .put(snapshotId, completedSnapshotHandle);
        }

        @Override
        public void remove(TableBucket tableBucket, long snapshotId) throws Exception {
            snapshotHandleMap.computeIfAbsent(tableBucket, k -> new HashMap<>()).remove(snapshotId);
        }

        @Override
        public Optional<CompletedSnapshotHandle> get(TableBucket tableBucket, long snapshotId)
                throws Exception {
            return Optional.ofNullable(snapshotHandleMap.get(tableBucket))
                    .map(map -> map.get(snapshotId));
        }

        @Override
        public List<CompletedSnapshotHandle> getAllCompletedSnapshotHandles(TableBucket tableBucket)
                throws Exception {
            return new ArrayList<>(snapshotHandleMap.get(tableBucket).values());
        }

        @Override
        public Optional<CompletedSnapshotHandle> getLatestCompletedSnapshotHandle(
                TableBucket tableBucket) throws Exception {
            return new ArrayList<>(snapshotHandleMap.get(tableBucket).values())
                    .stream().max(Comparator.comparingLong(CompletedSnapshotHandle::getSnapshotId));
        }
    }
}

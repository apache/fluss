/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotStore;
import com.alibaba.fluss.server.kv.snapshot.ZooKeeperCompletedSnapshotHandleStore;
import com.alibaba.fluss.server.testutils.KvTestUtils;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

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
}

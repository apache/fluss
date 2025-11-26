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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.SequenceIDCounter;
import org.apache.fluss.server.kv.rocksdb.RocksDBExtension;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.testutils.KvTestUtils;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.fluss.server.zk.ZooKeeperUtils.startZookeeperClient;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import static org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory.builder;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link KvTabletSnapshotTarget}. */
class KvTabletSnapshotTargetTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @RegisterExtension public RocksDBExtension rocksDBExtension = new RocksDBExtension();

    private static final TableBucket tableBucket = new TableBucket(1, 1);
    private static final long periodicMaterializeDelay = 10_000L;
    private ManuallyTriggeredScheduledExecutorService scheduledExecutorService;
    private PeriodicSnapshotManager periodicSnapshotManager;
    private final CloseableRegistry closeableRegistry = new CloseableRegistry();

    private AtomicLong snapshotIdGenerator;
    private AtomicLong logOffsetGenerator;
    private AtomicLong updateMinRetainOffsetConsumer;

    static ZooKeeperClient zooKeeperClient;

    @BeforeAll
    static void beforeAll() {
        final Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        zooKeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void beforeEach() {
        // init snapshot id and log offset generator
        snapshotIdGenerator = new AtomicLong(1);
        logOffsetGenerator = new AtomicLong(1);
        updateMinRetainOffsetConsumer = new AtomicLong(Long.MAX_VALUE);
        scheduledExecutorService = new ManuallyTriggeredScheduledExecutorService();
    }

    @AfterEach
    void afterEach() throws Exception {
        closeableRegistry.close();
        if (periodicSnapshotManager != null) {
            periodicSnapshotManager.close();
        }
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    @Test
    void testSnapshot(@TempDir Path kvTabletDir, @TempDir Path temoRebuildPath) throws Exception {
        // create a snapshot target use zk as the handle store
        CompletedSnapshotHandleStore completedSnapshotHandleStore =
                new ZooKeeperCompletedSnapshotHandleStore(zooKeeperClient);

        FsPath remoteKvTabletDir = FsPath.fromLocalFile(kvTabletDir.toFile());
        KvTabletSnapshotTarget kvTabletSnapshotTarget =
                createSnapshotTarget(remoteKvTabletDir, completedSnapshotHandleStore);

        periodicSnapshotManager = createSnapshotManager(kvTabletSnapshotTarget);
        periodicSnapshotManager.start();

        RocksDB rocksDB = rocksDBExtension.getRocksDb();
        rocksDB.put("key1".getBytes(), "val1".getBytes());
        periodicSnapshotManager.triggerSnapshot();

        long snapshotId1 = 1;
        TestRocksIncrementalSnapshot rocksIncrementalSnapshot =
                (TestRocksIncrementalSnapshot) kvTabletSnapshotTarget.getRocksIncrementalSnapshot();
        // retry util the snapshot finish
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(rocksIncrementalSnapshot.completedSnapshots)
                                .contains(snapshotId1));

        // now retrieve the snapshot
        CompletedSnapshotHandle completedSnapshotHandle =
                completedSnapshotHandleStore.get(tableBucket, snapshotId1).get();
        CompletedSnapshot snapshot = completedSnapshotHandle.retrieveCompleteSnapshot();
        assertThat(snapshot.getSnapshotID()).isEqualTo(snapshotId1);
        // verify the metadata file path
        assertThat(snapshot.getMetadataFilePath())
                .isEqualTo(CompletedSnapshot.getMetadataFilePath(snapshot.getSnapshotLocation()));
        // rebuild from snapshot, and the check the rebuilt rocksdb
        try (RocksDBKv rocksDBKv =
                KvTestUtils.buildFromSnapshotHandle(
                        snapshot.getKvSnapshotHandle(), temoRebuildPath.resolve("restore1"))) {
            assertThat(rocksDBKv.get("key1".getBytes())).isEqualTo("val1".getBytes());
        }

        rocksDB.put("key2".getBytes(), "val2".getBytes());
        long snapshotId2 = 2;
        // update log offset to do snapshot
        logOffsetGenerator.set(5);
        periodicSnapshotManager.triggerSnapshot();
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(rocksIncrementalSnapshot.completedSnapshots)
                                .contains(snapshotId2));

        completedSnapshotHandle = completedSnapshotHandleStore.get(tableBucket, snapshotId2).get();
        snapshot = completedSnapshotHandle.retrieveCompleteSnapshot();
        // rebuild from snapshot, and the check the rebuilt rocksdb
        // verify the metadata file path
        assertThat(snapshot.getMetadataFilePath())
                .isEqualTo(CompletedSnapshot.getMetadataFilePath(snapshot.getSnapshotLocation()));
        assertThat(snapshot.getSnapshotID()).isEqualTo(snapshotId2);
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(5);
        try (RocksDBKv rocksDBKv =
                KvTestUtils.buildFromSnapshotHandle(
                        snapshot.getKvSnapshotHandle(), temoRebuildPath.resolve("restore2"))) {
            assertThat(rocksDBKv.get("key1".getBytes())).isEqualTo("val1".getBytes());
            assertThat(rocksDBKv.get("key2".getBytes())).isEqualTo("val2".getBytes());
        }
    }

    @Test
    void testSnapshotFailure(@TempDir Path kvTabletDir) throws Exception {
        CompletedSnapshotHandleStore completedSnapshotHandleStore =
                TestCompletedSnapshotHandleStore.newBuilder().build();
        FsPath remoteKvTabletDir = FsPath.fromLocalFile(kvTabletDir.toFile());
        // test fail in the sync phase of snapshot
        KvTabletSnapshotTarget kvTabletSnapshotTarget =
                createSnapshotTarget(
                        remoteKvTabletDir,
                        completedSnapshotHandleStore,
                        SnapshotFailType.SYNC_PHASE);
        long snapshotId1 = 1;

        periodicSnapshotManager = createSnapshotManager(kvTabletSnapshotTarget);
        periodicSnapshotManager.start();
        periodicSnapshotManager.triggerSnapshot();

        // the snapshot dir should be discarded
        FsPath snapshotPath1 = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId1);
        assertThat(snapshotPath1.getFileSystem().exists(snapshotPath1)).isFalse();

        // test fail in the async phase of snapshot
        kvTabletSnapshotTarget =
                createSnapshotTarget(
                        remoteKvTabletDir,
                        completedSnapshotHandleStore,
                        SnapshotFailType.ASYNC_PHASE);
        periodicSnapshotManager = createSnapshotManager(kvTabletSnapshotTarget);
        periodicSnapshotManager.start();
        periodicSnapshotManager.triggerSnapshot();
        final long snapshotId2 = 2;

        TestRocksIncrementalSnapshot rocksIncrementalSnapshot =
                (TestRocksIncrementalSnapshot) kvTabletSnapshotTarget.getRocksIncrementalSnapshot();
        FsPath snapshotPath2 = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId2);
        // the snapshot1 will fail
        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(rocksIncrementalSnapshot.abortedSnapshots).contains(snapshotId2);
                    // the snapshot dir should be discarded, the snapshotPath is disposed after
                    // notify abort, so we have to assert the path doesn't exist in the retry
                    assertThat(snapshotPath2.getFileSystem().exists(snapshotPath2)).isFalse();
                });
        // minRetainOffset shouldn't updated
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void testAddToSnapshotToStoreFail(@TempDir Path kvTabletDir) throws Exception {
        final String errMsg = "Add to snapshot handle failed.";
        AtomicBoolean shouldFail = new AtomicBoolean(true);
        // we use a store will fail when the variable shouldFail is true
        // addColumn the snapshot to store will fail
        CompletedSnapshotHandleStore completedSnapshotHandleStore =
                TestCompletedSnapshotHandleStore.newBuilder()
                        .setAddFunction(
                                (snapshot) -> {
                                    if (shouldFail.get()) {
                                        throw new FlussException(errMsg);
                                    }
                                    return null;
                                })
                        .build();
        FsPath remoteKvTabletDir = FsPath.fromLocalFile(kvTabletDir.toFile());
        KvTabletSnapshotTarget kvTabletSnapshotTarget =
                createSnapshotTarget(remoteKvTabletDir, completedSnapshotHandleStore);

        periodicSnapshotManager = createSnapshotManager(kvTabletSnapshotTarget);
        periodicSnapshotManager.start();

        RocksDB rocksDB = rocksDBExtension.getRocksDb();
        rocksDB.put("key".getBytes(), "val".getBytes());
        periodicSnapshotManager.triggerSnapshot();
        long snapshotId1 = 1;
        FsPath snapshotPath1 = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId1);

        retry(
                Duration.ofMinutes(1),
                () -> assertThat(snapshotPath1.getFileSystem().exists(snapshotPath1)).isTrue());

        TestRocksIncrementalSnapshot rocksIncrementalSnapshot =
                (TestRocksIncrementalSnapshot) kvTabletSnapshotTarget.getRocksIncrementalSnapshot();
        // the snapshot1 will fail
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(rocksIncrementalSnapshot.abortedSnapshots).contains(snapshotId1));
        // the snapshot dir should be discarded
        assertThat(snapshotPath1.getFileSystem().exists(snapshotPath1)).isFalse();
        // minRetainOffset shouldn't be updated when the snapshot failed
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(Long.MAX_VALUE);

        // set it to false
        shouldFail.set(false);
        long snapshotId2 = 2;
        // trigger a snapshot again, it'll be success
        periodicSnapshotManager.triggerSnapshot();
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(rocksIncrementalSnapshot.completedSnapshots)
                                .contains(snapshotId2));

        FsPath snapshotPath2 = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId2);
        // the snapshot dir should exist again
        assertThat(snapshotPath2.getFileSystem().exists(snapshotPath2)).isTrue();
        // minRetainOffset should be updated because snapshot success
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(1L);
    }

    @Test
    void testIdempotentCheckWhenSnapshotExistsInZK(@TempDir Path kvTabletDir) throws Exception {
        // Test case: coordinator commits to ZK successfully but response is lost due to failover
        // The tablet server should detect the snapshot exists in ZK and skip cleanup
        CompletedSnapshotHandleStore completedSnapshotHandleStore =
                new ZooKeeperCompletedSnapshotHandleStore(zooKeeperClient);
        FsPath remoteKvTabletDir = FsPath.fromLocalFile(kvTabletDir.toFile());

        // Create a committer that commits to ZK first, then throws exception
        // This simulates coordinator failover after ZK commit but before response
        CompletedKvSnapshotCommitter failingAfterZkCommitCommitter =
                (snapshot, coordinatorEpoch, bucketLeaderEpoch) -> {
                    // Always commit to ZK first
                    CompletedSnapshotHandle handle =
                            new CompletedSnapshotHandle(
                                    snapshot.getSnapshotID(),
                                    snapshot.getSnapshotLocation(),
                                    snapshot.getLogOffset());
                    completedSnapshotHandleStore.add(
                            snapshot.getTableBucket(), snapshot.getSnapshotID(), handle);

                    // Then throw exception - simulating coordinator failover after ZK commit
                    throw new FlussException("Coordinator failover after ZK commit");
                };

        KvTabletSnapshotTarget kvTabletSnapshotTarget =
                createSnapshotTargetWithCustomCommitter(
                        remoteKvTabletDir, failingAfterZkCommitCommitter);

        periodicSnapshotManager = createSnapshotManager(kvTabletSnapshotTarget);
        periodicSnapshotManager.start();

        RocksDB rocksDB = rocksDBExtension.getRocksDb();
        rocksDB.put("key1".getBytes(), "val1".getBytes());

        // Trigger snapshot - will commit to ZK but throw exception
        periodicSnapshotManager.triggerSnapshot();
        long snapshotId1 = 1;

        TestRocksIncrementalSnapshot rocksIncrementalSnapshot =
                (TestRocksIncrementalSnapshot) kvTabletSnapshotTarget.getRocksIncrementalSnapshot();

        // The snapshot should be treated as successful due to idempotent check
        // Even though commit threw exception, idempotent check should find it in ZK
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(rocksIncrementalSnapshot.completedSnapshots)
                                .contains(snapshotId1));

        // Verify snapshot was not cleaned up and state was updated correctly
        FsPath snapshotPath1 = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId1);
        assertThat(snapshotPath1.getFileSystem().exists(snapshotPath1)).isTrue();
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(1L);
    }

    @Test
    void testIdempotentCheckWhenSnapshotNotExistsInZK(@TempDir Path kvTabletDir) throws Exception {
        // Test case: genuine commit failure - snapshot should not exist in ZK and cleanup should
        // occur
        FsPath remoteKvTabletDir = FsPath.fromLocalFile(kvTabletDir.toFile());

        // Create a committer that always fails - simulating genuine coordinator failure
        CompletedKvSnapshotCommitter alwaysFailingCommitter =
                (snapshot, coordinatorEpoch, bucketLeaderEpoch) -> {
                    throw new FlussException(
                            "Genuine coordinator failure - snapshot not committed to ZK");
                };

        KvTabletSnapshotTarget kvTabletSnapshotTarget =
                createSnapshotTargetWithCustomCommitter(remoteKvTabletDir, alwaysFailingCommitter);

        periodicSnapshotManager = createSnapshotManager(kvTabletSnapshotTarget);
        periodicSnapshotManager.start();

        RocksDB rocksDB = rocksDBExtension.getRocksDb();
        rocksDB.put("key1".getBytes(), "val1".getBytes());
        periodicSnapshotManager.triggerSnapshot();

        long snapshotId1 = 1;
        TestRocksIncrementalSnapshot rocksIncrementalSnapshot =
                (TestRocksIncrementalSnapshot) kvTabletSnapshotTarget.getRocksIncrementalSnapshot();

        // The snapshot should be aborted since it genuinely failed
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(rocksIncrementalSnapshot.abortedSnapshots).contains(snapshotId1));

        // Verify cleanup occurred
        FsPath snapshotPath1 = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId1);
        assertThat(snapshotPath1.getFileSystem().exists(snapshotPath1)).isFalse();
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void testIdempotentCheckWhenZKQueryFails(@TempDir Path kvTabletDir) throws Exception {
        // Test case: ZK query fails - should keep snapshot in current state to avoid data loss
        FsPath remoteKvTabletDir = FsPath.fromLocalFile(kvTabletDir.toFile());

        // Create a failing ZK client that throws exception to simulate ZK query failure
        ZooKeeperClient failingZkClient = createFailingZooKeeperClient();

        CompletedKvSnapshotCommitter failingCommitter =
                (snapshot, coordinatorEpoch, bucketLeaderEpoch) -> {
                    throw new FlussException("Commit failed");
                };

        KvTabletSnapshotTarget kvTabletSnapshotTarget =
                createSnapshotTargetWithCustomZkAndCommitter(
                        remoteKvTabletDir, failingZkClient, failingCommitter);

        periodicSnapshotManager = createSnapshotManager(kvTabletSnapshotTarget);
        periodicSnapshotManager.start();

        RocksDB rocksDB = rocksDBExtension.getRocksDb();
        rocksDB.put("key1".getBytes(), "val1".getBytes());
        periodicSnapshotManager.triggerSnapshot();

        long snapshotId1 = 1;
        TestRocksIncrementalSnapshot rocksIncrementalSnapshot =
                (TestRocksIncrementalSnapshot) kvTabletSnapshotTarget.getRocksIncrementalSnapshot();

        // Wait for snapshot processing to complete
        // The snapshot should be created but commit will fail, then ZK query will fail
        // In this case, the new logic should preserve the snapshot files (no cleanup)
        retry(
                Duration.ofMinutes(1),
                () -> {
                    // Verify that snapshot creation happened but neither completion nor abortion
                    // occurred
                    // Since both commit and ZK query failed, snapshot should remain in limbo state
                    FsPath snapshotPath1 =
                            FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId1);
                    assertThat(snapshotPath1.getFileSystem().exists(snapshotPath1)).isTrue();
                    assertThat(rocksIncrementalSnapshot.abortedSnapshots)
                            .doesNotContain(snapshotId1);
                    assertThat(rocksIncrementalSnapshot.completedSnapshots)
                            .doesNotContain(snapshotId1);
                });

        // Verify local state was not updated (remain unchanged)
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(Long.MAX_VALUE);
    }

    private PeriodicSnapshotManager createSnapshotManager(
            PeriodicSnapshotManager.SnapshotTarget target) {
        return new PeriodicSnapshotManager(
                tableBucket,
                target,
                periodicMaterializeDelay,
                java.util.concurrent.Executors.newFixedThreadPool(1),
                scheduledExecutorService,
                TestingMetricGroups.BUCKET_METRICS);
    }

    private KvTabletSnapshotTarget createSnapshotTarget(
            FsPath remoteKvTabletDir, CompletedSnapshotHandleStore snapshotHandleStore)
            throws IOException {
        return createSnapshotTarget(remoteKvTabletDir, snapshotHandleStore, SnapshotFailType.NONE);
    }

    private KvTabletSnapshotTarget createSnapshotTarget(
            FsPath remoteKvTabletDir,
            CompletedSnapshotHandleStore snapshotHandleStore,
            SnapshotFailType snapshotFailType)
            throws IOException {
        TableBucket tableBucket = new TableBucket(1, 1);
        SharedKvFileRegistry sharedKvFileRegistry = new SharedKvFileRegistry();
        Executor executor = Executors.directExecutor();

        CompletedSnapshotStore completedSnapshotStore =
                new CompletedSnapshotStore(
                        1,
                        sharedKvFileRegistry,
                        Collections.emptyList(),
                        snapshotHandleStore,
                        executor);

        RocksIncrementalSnapshot rocksIncrementalSnapshot =
                createIncrementalSnapshot(snapshotFailType);

        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

        TestingSnapshotIDCounter testingSnapshotIdCounter = new TestingSnapshotIDCounter();
        Supplier<Integer> bucketLeaderEpochSupplier = () -> 0;
        Supplier<Integer> coordinatorEpochSupplier = () -> 0;

        return new KvTabletSnapshotTarget(
                tableBucket,
                new TestingStoreCompletedKvSnapshotCommitter(completedSnapshotStore),
                zooKeeperClient,
                rocksIncrementalSnapshot,
                remoteKvTabletDir,
                executor,
                cancelStreamRegistry,
                testingSnapshotIdCounter,
                logOffsetGenerator::get,
                updateMinRetainOffsetConsumer::set,
                bucketLeaderEpochSupplier,
                coordinatorEpochSupplier,
                0,
                0L);
    }

    private KvTabletSnapshotTarget createSnapshotTargetWithCustomCommitter(
            FsPath remoteKvTabletDir, CompletedKvSnapshotCommitter customCommitter)
            throws IOException {
        return createSnapshotTargetWithCustomZkAndCommitter(
                remoteKvTabletDir, zooKeeperClient, customCommitter);
    }

    private KvTabletSnapshotTarget createSnapshotTargetWithCustomZkAndCommitter(
            FsPath remoteKvTabletDir,
            ZooKeeperClient zkClient,
            CompletedKvSnapshotCommitter customCommitter)
            throws IOException {
        TableBucket tableBucket = new TableBucket(1, 1);
        Executor executor = Executors.directExecutor();
        RocksIncrementalSnapshot rocksIncrementalSnapshot =
                createIncrementalSnapshot(SnapshotFailType.NONE);
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        TestingSnapshotIDCounter testingSnapshotIdCounter = new TestingSnapshotIDCounter();
        Supplier<Integer> bucketLeaderEpochSupplier = () -> 0;
        Supplier<Integer> coordinatorEpochSupplier = () -> 0;

        return new KvTabletSnapshotTarget(
                tableBucket,
                customCommitter,
                zkClient,
                rocksIncrementalSnapshot,
                remoteKvTabletDir,
                executor,
                cancelStreamRegistry,
                testingSnapshotIdCounter,
                logOffsetGenerator::get,
                updateMinRetainOffsetConsumer::set,
                bucketLeaderEpochSupplier,
                coordinatorEpochSupplier,
                0,
                0L);
    }

    private RocksIncrementalSnapshot createIncrementalSnapshot(SnapshotFailType snapshotFailType)
            throws IOException {
        long lastCompletedSnapshotId = -1L;
        Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles = new HashMap<>();
        ResourceGuard rocksDBResourceGuard = new ResourceGuard();

        RocksDB rocksDB = rocksDBExtension.getRocksDb();

        ExecutorService downLoaderExecutor =
                java.util.concurrent.Executors.newSingleThreadExecutor();

        KvSnapshotDataUploader snapshotDataUploader =
                new KvSnapshotDataUploader(downLoaderExecutor);
        closeableRegistry.registerCloseable(downLoaderExecutor::shutdownNow);
        return new TestRocksIncrementalSnapshot(
                uploadedSstFiles,
                rocksDB,
                rocksDBResourceGuard,
                snapshotDataUploader,
                rocksDBExtension.getRockDbDir(),
                lastCompletedSnapshotId,
                snapshotFailType);
    }

    private ZooKeeperClient createFailingZooKeeperClient() {
        // Create a ZooKeeperClient that throws exception on getTableBucketSnapshot
        return new FailingZooKeeperClient();
    }

    private static class FailingZooKeeperClient extends ZooKeeperClient {

        public FailingZooKeeperClient() {
            // Create a new ZooKeeperClient using ZooKeeperUtils.startZookeeperClient
            super(createCuratorFrameworkWrapper(), new Configuration());
        }

        private static CuratorFrameworkWithUnhandledErrorListener createCuratorFrameworkWrapper() {
            Builder builder =
                    builder()
                            .connectString(
                                    ZOO_KEEPER_EXTENSION_WRAPPER
                                            .getCustomExtension()
                                            .getConnectString())
                            .retryPolicy(new ExponentialBackoffRetry(1000, 3));

            return startZookeeperClient(builder, NOPErrorHandler.INSTANCE);
        }

        @Override
        public Optional<BucketSnapshot> getTableBucketSnapshot(
                TableBucket tableBucket, long snapshotId) throws Exception {
            throw new Exception("ZK query failed");
        }
    }

    private static final class TestRocksIncrementalSnapshot extends RocksIncrementalSnapshot {

        private final Set<Long> abortedSnapshots = ConcurrentHashMap.newKeySet();
        private final Set<Long> completedSnapshots = ConcurrentHashMap.newKeySet();
        private final SnapshotFailType snapshotFailType;

        public TestRocksIncrementalSnapshot(
                Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles,
                @Nonnull RocksDB db,
                ResourceGuard rocksDBResourceGuard,
                KvSnapshotDataUploader kvSnapshotDataUploader,
                @Nonnull File instanceBasePath,
                long lastCompletedSnapshotId,
                SnapshotFailType snapshotFailType) {
            super(
                    uploadedSstFiles,
                    db,
                    rocksDBResourceGuard,
                    kvSnapshotDataUploader,
                    instanceBasePath,
                    lastCompletedSnapshotId);
            this.snapshotFailType = snapshotFailType;
        }

        public SnapshotResultSupplier asyncSnapshot(
                NativeRocksDBSnapshotResources snapshotResources,
                long snapshotId,
                long logOffset,
                @Nonnull SnapshotLocation snapshotLocation) {
            if (snapshotFailType == SnapshotFailType.SYNC_PHASE) {
                throw new FlussRuntimeException("Fail in snapshot sync phase.");
            } else if (snapshotFailType == SnapshotFailType.ASYNC_PHASE) {
                return snapshotCloseableRegistry -> {
                    throw new FlussRuntimeException("Fail in snapshot async phase.");
                };
            } else {
                return super.asyncSnapshot(
                        snapshotResources, snapshotId, logOffset, snapshotLocation);
            }
        }

        public void notifySnapshotComplete(long completedSnapshotId) {
            super.notifySnapshotComplete(completedSnapshotId);
            completedSnapshots.add(completedSnapshotId);
        }

        public void notifySnapshotAbort(long abortedSnapshotId) {
            super.notifySnapshotAbort(abortedSnapshotId);
            abortedSnapshots.add(abortedSnapshotId);
        }
    }

    private class TestingSnapshotIDCounter implements SequenceIDCounter {

        @Override
        public long getAndIncrement() {
            return snapshotIdGenerator.getAndIncrement();
        }
    }

    private enum SnapshotFailType {
        NONE, // don't fail
        SYNC_PHASE, // fail in the sync phase
        ASYNC_PHASE // fail in the async phase
    }

    /**
     * A {@link CompletedKvSnapshotCommitter} which will store the completed snapshot using {@link
     * CompletedSnapshotStore} when reporting a completed snapshot.
     */
    private static class TestingStoreCompletedKvSnapshotCommitter
            implements CompletedKvSnapshotCommitter {

        private final CompletedSnapshotStore completedSnapshotStore;

        public TestingStoreCompletedKvSnapshotCommitter(
                CompletedSnapshotStore completedSnapshotStore) {
            this.completedSnapshotStore = completedSnapshotStore;
        }

        @Override
        public void commitKvSnapshot(
                CompletedSnapshot snapshot, int coordinatorEpoch, int bucketLeaderEpoch)
                throws Exception {
            completedSnapshotStore.add(snapshot);
        }
    }
}

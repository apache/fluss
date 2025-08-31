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

package com.alibaba.fluss.server.kv.snapshot;

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussException;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.server.SequenceIDCounter;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBExtension;
import com.alibaba.fluss.server.kv.rocksdb.RocksDBKv;
import com.alibaba.fluss.server.metrics.group.TestingMetricGroups;
import com.alibaba.fluss.server.testutils.KvTestUtils;
import com.alibaba.fluss.server.utils.ResourceGuard;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import com.alibaba.fluss.utils.CloseableRegistry;
import com.alibaba.fluss.utils.FlussPaths;
import com.alibaba.fluss.utils.concurrent.Executors;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.alibaba.fluss.testutils.common.CommonTestUtils.retry;
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
        // add the snapshot to store will fail
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

    private static final class TestRocksIncrementalSnapshot extends RocksIncrementalSnapshot {

        private final Set<Long> abortedSnapshots = new HashSet<>();
        private final Set<Long> completedSnapshots = new HashSet<>();
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

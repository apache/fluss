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
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.InvalidRequestException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.server.SequenceIDCounter;
import org.apache.fluss.server.kv.rocksdb.RocksDBExtension;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.testutils.KvTestUtils;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.server.zk.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.FlussPaths;

import io.github.fluss_contrib.rocksdb.RocksDB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.apache.fluss.server.testutils.KvTestUtils.registerRecoverySnapshot;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.utils.concurrent.Executors.directExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvTabletSnapshotTarget}. */
class KvTabletSnapshotTargetTest {
    @Test
    void testLostCommitResponseAndFailedQueryAllowNextSnapshot(@TempDir Path directory)
            throws Throwable {
        registerRecoverySnapshot(
                zooKeeperClient, tableBucket, new BucketSnapshot(0, 0, "file:/initial"));
        CompletedSnapshotStore store =
                new CompletedSnapshotStore(
                        10,
                        new SharedKvFileRegistry(),
                        Collections.emptyList(),
                        new ZooKeeperCompletedSnapshotHandleStore(zooKeeperClient),
                        directExecutor(),
                        snapshot -> false);
        CompletedKvSnapshotCommitter committer =
                (snapshot, coordinatorEpoch, leaderEpoch, previousRecoverySnapshotId) -> {
                    store.add(snapshot, previousRecoverySnapshotId, leaderEpoch);
                    throw new IOException("Commit response lost.");
                };
        AtomicBoolean failQuery = new AtomicBoolean(true);
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, directory.toUri().toString());
        ZooKeeperClient reader =
                new ZooKeeperClient(
                        new CuratorFrameworkWithUnhandledErrorListener(
                                zooKeeperClient.getCuratorClient(), (message, error) -> {}),
                        configuration) {
                    @Override
                    public Optional<BucketSnapshot> getTableBucketLatestSnapshot(TableBucket bucket)
                            throws Exception {
                        if (failQuery.getAndSet(false)) {
                            throw new IOException("Snapshot query failed.");
                        }
                        return super.getTableBucketLatestSnapshot(bucket);
                    }
                };
        KvTabletSnapshotTarget original =
                createSnapshotTargetWithCustomZkAndCommitter(
                        FsPath.fromLocalFile(directory.resolve("original").toFile()),
                        reader,
                        committer);
        original.getRocksIncrementalSnapshot().notifySnapshotComplete(0L);
        rocksDBExtension.getRocksDb().put("key".getBytes(), "value".getBytes());
        PeriodicSnapshotManager.SnapshotRunnable task = original.initSnapshot().get();
        task.getSnapshotRunnable().run();
        SnapshotResult result = task.getSnapshotRunnable().get();
        assertThatThrownBy(
                        () ->
                                original.handleSnapshotResult(
                                        task.getSnapshotId(),
                                        task.getCoordinatorEpoch(),
                                        task.getBucketLeaderEpoch(),
                                        task.getSnapshotLocation(),
                                        result))
                .isInstanceOf(IOException.class);
        assertThat(updateMinRetainOffsetConsumer).hasValue(Long.MAX_VALUE);
        logOffsetGenerator.set(2L);
        PeriodicSnapshotManager.SnapshotRunnable next = original.initSnapshot().get();
        next.getSnapshotRunnable().run();
        SnapshotResult nextResult = next.getSnapshotRunnable().get();
        original.handleSnapshotResult(
                next.getSnapshotId(),
                next.getCoordinatorEpoch(),
                next.getBucketLeaderEpoch(),
                next.getSnapshotLocation(),
                nextResult);
        assertThat(zooKeeperClient.getRecoveryBucketSnapshot(tableBucket).get().getSnapshotId())
                .isEqualTo(2L);
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(2L);
    }

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @RegisterExtension public RocksDBExtension rocksDBExtension = new RocksDBExtension();

    private static final TableBucket tableBucket = new TableBucket(1, 1);
    private static final long periodicMaterializeDelay = 10_000L;
    private ManuallyTriggeredScheduledExecutorService scheduledExecutorService;
    private PeriodicSnapshotManager periodicSnapshotManager;
    private final CloseableRegistry closeableRegistry = new CloseableRegistry();
    private final Counter remoteKvCopyBytes = new ThreadSafeSimpleCounter();

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
    void beforeEach() throws Exception {
        zooKeeperClient
                .getCuratorClient()
                .create()
                .creatingParentsIfNeeded()
                .forPath(
                        ZkData.LeaderAndIsrZNode.path(tableBucket),
                        ZkData.LeaderAndIsrZNode.encode(new LeaderAndIsr(0, 0)));
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

        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(1L);
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
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(5L);
        assertThat(zooKeeperClient.getRecoveryBucketSnapshot(tableBucket).get().getSnapshotId())
                .isEqualTo(snapshotId2);
        try (RocksDBKv rocksDBKv =
                KvTestUtils.buildFromSnapshotHandle(
                        snapshot.getKvSnapshotHandle(), temoRebuildPath.resolve("restore2"))) {
            assertThat(rocksDBKv.get("key1".getBytes())).isEqualTo("val1".getBytes());
            assertThat(rocksDBKv.get("key2".getBytes())).isEqualTo("val2".getBytes());
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SnapshotFailType.class,
            names = {"SYNC_PHASE", "ASYNC_PHASE", "DIRECTORY_INITIALIZATION"})
    void testSnapshotFailure(SnapshotFailType snapshotFailType, @TempDir Path kvTabletDir)
            throws Exception {
        AtomicInteger committedSnapshots = new AtomicInteger();
        CompletedSnapshotHandleStore completedSnapshotHandleStore =
                TestCompletedSnapshotHandleStore.newBuilder()
                        .setAddFunction(
                                snapshot -> {
                                    committedSnapshots.incrementAndGet();
                                    return null;
                                })
                        .build();
        AtomicInteger mkdirsCalls = new AtomicInteger();
        AtomicInteger deleteCalls = new AtomicInteger();
        FileSystem remoteFileSystem =
                new LocalFileSystem() {
                    @Override
                    public boolean mkdirs(FsPath path) throws IOException {
                        mkdirsCalls.incrementAndGet();
                        boolean created = super.mkdirs(path);
                        if (snapshotFailType == SnapshotFailType.DIRECTORY_INITIALIZATION) {
                            throw new IOException("Directory initialization failed after creation");
                        }
                        return created;
                    }

                    @Override
                    public boolean delete(FsPath path, boolean recursive) throws IOException {
                        deleteCalls.incrementAndGet();
                        return super.delete(path, recursive);
                    }
                };
        FsPath remoteKvTabletDir =
                new FsPath(kvTabletDir.toUri()) {
                    @Override
                    public FileSystem getFileSystem() {
                        return remoteFileSystem;
                    }
                };
        KvTabletSnapshotTarget kvTabletSnapshotTarget =
                createSnapshotTarget(
                        remoteKvTabletDir, completedSnapshotHandleStore, snapshotFailType);
        long snapshotId = 1;
        TestRocksIncrementalSnapshot rocksIncrementalSnapshot =
                (TestRocksIncrementalSnapshot) kvTabletSnapshotTarget.getRocksIncrementalSnapshot();

        periodicSnapshotManager = createSnapshotManager(kvTabletSnapshotTarget);
        periodicSnapshotManager.start();
        periodicSnapshotManager.triggerSnapshot();

        if (snapshotFailType == SnapshotFailType.SYNC_PHASE) {
            // Native preparation failure must not create or delete remote directories.
            assertThat(mkdirsCalls).hasValue(0);
            assertThat(deleteCalls).hasValue(0);
            assertThat(rocksIncrementalSnapshot.abortedSnapshots).isEmpty();
        } else {
            FsPath snapshotPath = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId);
            retry(
                    Duration.ofMinutes(1),
                    () -> {
                        assertThat(rocksIncrementalSnapshot.abortedSnapshots).contains(snapshotId);
                        // Remote cleanup follows the abort notification.
                        assertThat(remoteFileSystem.exists(snapshotPath)).isFalse();
                    });
            assertThat(mkdirsCalls).hasValue(1);
            assertThat(deleteCalls).hasValue(1);
        }
        assertThat(new File(rocksDBExtension.getRockDbDir(), "snap-" + snapshotId)).doesNotExist();
        assertThat(committedSnapshots).hasValue(0);
        assertThat(updateMinRetainOffsetConsumer).hasValue(Long.MAX_VALUE);
    }

    @Test
    void testAddToSnapshotToStoreFail(@TempDir Path kvTabletDir) throws Throwable {
        final String errMsg = "Add to snapshot handle failed.";
        AtomicLong uploadedBytes = new AtomicLong();
        AtomicBoolean shouldFail = new AtomicBoolean(true);
        // we use a store will fail when the variable shouldFail is true
        // add the snapshot to store will fail
        CompletedSnapshotHandleStore completedSnapshotHandleStore =
                TestCompletedSnapshotHandleStore.newBuilder()
                        .setAddFunction(
                                (snapshot) -> {
                                    uploadedBytes.addAndGet(
                                            snapshot.retrieveCompleteSnapshot()
                                                    .getKvSnapshotHandle()
                                                    .getIncrementalSize());
                                    if (shouldFail.get()) {
                                        throw new InvalidRequestException(errMsg);
                                    }
                                    return null;
                                })
                        .build();
        FsPath remoteKvTabletDir = FsPath.fromLocalFile(kvTabletDir.toFile());
        KvTabletSnapshotTarget kvTabletSnapshotTarget =
                createSnapshotTarget(remoteKvTabletDir, completedSnapshotHandleStore);

        RocksDB rocksDB = rocksDBExtension.getRocksDb();
        rocksDB.put("key".getBytes(), "val".getBytes());
        PeriodicSnapshotManager.SnapshotRunnable first =
                kvTabletSnapshotTarget.initSnapshot().get();
        first.getSnapshotRunnable().run();
        SnapshotResult firstResult = first.getSnapshotRunnable().get();
        assertThatThrownBy(
                        () ->
                                kvTabletSnapshotTarget.handleSnapshotResult(
                                        first.getSnapshotId(),
                                        first.getCoordinatorEpoch(),
                                        first.getBucketLeaderEpoch(),
                                        first.getSnapshotLocation(),
                                        firstResult))
                .isInstanceOf(InvalidRequestException.class);
        FsPath snapshotPath1 = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, 1L);
        assertThat(snapshotPath1.getFileSystem().exists(snapshotPath1)).isFalse();
        for (KvFileHandleAndLocalPath file :
                firstResult.getKvSnapshotHandle().getSharedKvFileHandles()) {
            FsPath path = new FsPath(file.getKvFileHandle().getFilePath());
            assertThat(path.getFileSystem().exists(path)).isFalse();
        }
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(Long.MAX_VALUE);

        assertThat(uploadedBytes.get()).isPositive();
        assertThat(remoteKvCopyBytes.getCount()).isEqualTo(uploadedBytes.get());
        long bytesAfterFailedCommit = remoteKvCopyBytes.getCount();

        shouldFail.set(false);
        long snapshotId2 = 2;
        PeriodicSnapshotManager.SnapshotRunnable second =
                kvTabletSnapshotTarget.initSnapshot().get();
        second.getSnapshotRunnable().run();
        kvTabletSnapshotTarget.handleSnapshotResult(
                second.getSnapshotId(),
                second.getCoordinatorEpoch(),
                second.getBucketLeaderEpoch(),
                second.getSnapshotLocation(),
                second.getSnapshotRunnable().get());

        FsPath snapshotPath2 = FlussPaths.remoteKvSnapshotDir(remoteKvTabletDir, snapshotId2);
        // the snapshot dir should exist again
        assertThat(snapshotPath2.getFileSystem().exists(snapshotPath2)).isTrue();
        // minRetainOffset should be updated because snapshot success
        assertThat(updateMinRetainOffsetConsumer.get()).isEqualTo(1L);
        assertThat(remoteKvCopyBytes.getCount())
                .isEqualTo(uploadedBytes.get())
                .isGreaterThan(bytesAfterFailedCommit);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFailedCommitPreservesReferencedOrUncertainFiles(
            boolean failQuery, @TempDir Path directory) throws Throwable {
        registerRecoverySnapshot(
                zooKeeperClient, tableBucket, new BucketSnapshot(0, 0, "file:/initial"));
        Configuration configuration = new Configuration();
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, directory.toUri().toString());
        ZooKeeperClient reader =
                new ZooKeeperClient(
                        new CuratorFrameworkWithUnhandledErrorListener(
                                zooKeeperClient.getCuratorClient(), (message, error) -> {}),
                        configuration) {
                    @Override
                    public Optional<BucketSnapshot> getTableBucketSnapshot(
                            TableBucket bucket, long snapshotId) throws Exception {
                        if (failQuery) {
                            throw new IOException("Snapshot reference query failed.");
                        }
                        return super.getTableBucketSnapshot(bucket, snapshotId);
                    }
                };
        KvTabletSnapshotTarget target =
                createSnapshotTargetWithCustomZkAndCommitter(
                        new FsPath(directory.toUri()),
                        reader,
                        (snapshot, coordinatorEpoch, leaderEpoch, previousSnapshotId) -> {
                            zooKeeperClient.registerTableBucketSnapshot(
                                    tableBucket,
                                    new BucketSnapshot(
                                            snapshot.getSnapshotID(),
                                            snapshot.getLogOffset(),
                                            snapshot.getMetadataFilePath().toString()));
                            throw new IOException("Commit failed.");
                        });
        rocksDBExtension.getRocksDb().put("key".getBytes(), "value".getBytes());
        PeriodicSnapshotManager.SnapshotRunnable task = target.initSnapshot().get();
        task.getSnapshotRunnable().run();
        SnapshotResult result = task.getSnapshotRunnable().get();
        assertThatThrownBy(
                        () ->
                                target.handleSnapshotResult(
                                        task.getSnapshotId(),
                                        task.getCoordinatorEpoch(),
                                        task.getBucketLeaderEpoch(),
                                        task.getSnapshotLocation(),
                                        result))
                .isInstanceOf(IOException.class);
        for (KvFileHandleAndLocalPath file :
                result.getKvSnapshotHandle().getSharedKvFileHandles()) {
            FsPath path = new FsPath(file.getKvFileHandle().getFilePath());
            assertThat(path.getFileSystem().exists(path)).isTrue();
        }
        assertThat(result.getSnapshotPath().getFileSystem().exists(result.getSnapshotPath()))
                .isTrue();
        assertThat(updateMinRetainOffsetConsumer).hasValue(Long.MAX_VALUE);
    }

    private PeriodicSnapshotManager createSnapshotManager(
            PeriodicSnapshotManager.SnapshotTarget target) {
        return new PeriodicSnapshotManager(
                tableBucket,
                target,
                periodicMaterializeDelay,
                Executors.newFixedThreadPool(1),
                scheduledExecutorService);
    }

    private KvTabletSnapshotTarget createSnapshotTarget(
            FsPath remoteKvTabletDir, CompletedSnapshotHandleStore snapshotHandleStore)
            throws Exception {
        return createSnapshotTarget(remoteKvTabletDir, snapshotHandleStore, SnapshotFailType.NONE);
    }

    private KvTabletSnapshotTarget createSnapshotTarget(
            FsPath remoteKvTabletDir,
            CompletedSnapshotHandleStore snapshotHandleStore,
            SnapshotFailType snapshotFailType)
            throws Exception {
        TableBucket tableBucket = new TableBucket(1, 1);
        SharedKvFileRegistry sharedKvFileRegistry = new SharedKvFileRegistry();
        Executor executor = directExecutor();

        CompletedSnapshotStore completedSnapshotStore =
                new CompletedSnapshotStore(
                        1,
                        sharedKvFileRegistry,
                        Collections.emptyList(),
                        snapshotHandleStore,
                        executor,
                        (consumeKvSnapshotForBucket) -> false); //  only retain the latest snapshot.

        RocksIncrementalSnapshot rocksIncrementalSnapshot =
                createIncrementalSnapshot(snapshotFailType);

        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();

        TestingSnapshotIDCounter testingSnapshotIdCounter = new TestingSnapshotIDCounter();
        Supplier<Integer> coordinatorEpochSupplier = () -> 0;

        return new KvTabletSnapshotTarget(
                tableBucket,
                new TestingStoreCompletedKvSnapshotCommitter(completedSnapshotStore),
                zooKeeperClient,
                rocksIncrementalSnapshot,
                remoteKvTabletDir,
                directExecutor(),
                cancelStreamRegistry,
                testingSnapshotIdCounter,
                this::getCurrentTabletState,
                updateMinRetainOffsetConsumer::set,
                0,
                coordinatorEpochSupplier,
                0,
                0L);
    }

    private KvTabletSnapshotTarget createSnapshotTargetWithCustomZkAndCommitter(
            FsPath remoteKvTabletDir,
            ZooKeeperClient zkClient,
            CompletedKvSnapshotCommitter customCommitter)
            throws IOException {
        TableBucket tableBucket = new TableBucket(1, 1);
        RocksIncrementalSnapshot rocksIncrementalSnapshot =
                createIncrementalSnapshot(SnapshotFailType.NONE);
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        TestingSnapshotIDCounter testingSnapshotIdCounter = new TestingSnapshotIDCounter();
        Supplier<Integer> coordinatorEpochSupplier = () -> 0;

        return new KvTabletSnapshotTarget(
                tableBucket,
                customCommitter,
                zkClient,
                rocksIncrementalSnapshot,
                remoteKvTabletDir,
                directExecutor(),
                cancelStreamRegistry,
                testingSnapshotIdCounter,
                this::getCurrentTabletState,
                updateMinRetainOffsetConsumer::set,
                0,
                coordinatorEpochSupplier,
                0,
                0L);
    }

    private TabletState getCurrentTabletState() {
        return new TabletState(logOffsetGenerator.get(), null, null);
    }

    private RocksIncrementalSnapshot createIncrementalSnapshot(SnapshotFailType snapshotFailType)
            throws IOException {
        long lastCompletedSnapshotId = -1L;
        Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles = new HashMap<>();
        ResourceGuard rocksDBResourceGuard = new ResourceGuard();

        RocksDB rocksDB = rocksDBExtension.getRocksDb();

        ExecutorService downLoaderExecutor = Executors.newSingleThreadExecutor();

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
                snapshotFailType,
                remoteKvCopyBytes);
    }

    private static final class TestRocksIncrementalSnapshot extends RocksIncrementalSnapshot {

        private final Set<Long> abortedSnapshots = ConcurrentHashMap.newKeySet();
        private final Set<Long> completedSnapshots = ConcurrentHashMap.newKeySet();
        private final SnapshotFailType snapshotFailType;
        private final File instanceBasePath;

        public TestRocksIncrementalSnapshot(
                Map<Long, Collection<KvFileHandleAndLocalPath>> uploadedSstFiles,
                @Nonnull RocksDB db,
                ResourceGuard rocksDBResourceGuard,
                KvSnapshotDataUploader kvSnapshotDataUploader,
                @Nonnull File instanceBasePath,
                long lastCompletedSnapshotId,
                SnapshotFailType snapshotFailType,
                Counter remoteKvCopyBytes) {
            super(
                    uploadedSstFiles,
                    db,
                    rocksDBResourceGuard,
                    kvSnapshotDataUploader,
                    instanceBasePath,
                    lastCompletedSnapshotId,
                    remoteKvCopyBytes);
            this.snapshotFailType = snapshotFailType;
            this.instanceBasePath = instanceBasePath;
        }

        @Override
        public NativeRocksDBSnapshotResources syncPrepareResources(long snapshotId)
                throws Exception {
            if (snapshotFailType == SnapshotFailType.SYNC_PHASE) {
                // RocksDB rejects an existing checkpoint directory. Exercise native preparation
                // and its local cleanup, before the asynchronous supplier is constructed.
                Files.createDirectory(new File(instanceBasePath, "snap-" + snapshotId).toPath());
            }
            return super.syncPrepareResources(snapshotId);
        }

        @Override
        public SnapshotResultSupplier asyncSnapshot(
                NativeRocksDBSnapshotResources snapshotResources,
                long snapshotId,
                TabletState tabletState,
                @Nonnull SnapshotLocation snapshotLocation) {
            if (snapshotFailType == SnapshotFailType.ASYNC_PHASE) {
                return snapshotCloseableRegistry -> {
                    throw new FlussRuntimeException("Fail in snapshot async phase.");
                };
            } else {
                return super.asyncSnapshot(
                        snapshotResources, snapshotId, tabletState, snapshotLocation);
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
        public long getCurrent() {
            return snapshotIdGenerator.get();
        }

        @Override
        public long getAndIncrement() {
            return snapshotIdGenerator.getAndIncrement();
        }

        @Override
        public long getAndAdd(Long delta) {
            return snapshotIdGenerator.getAndAdd(delta);
        }
    }

    private enum SnapshotFailType {
        NONE, // don't fail
        SYNC_PHASE, // fail in the sync phase
        ASYNC_PHASE, // fail in the async phase
        DIRECTORY_INITIALIZATION // fail after creating the remote directory
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
                CompletedSnapshot snapshot,
                int coordinatorEpoch,
                int bucketLeaderEpoch,
                Long previousRecoverySnapshotId)
                throws Exception {
            completedSnapshotStore.add(snapshot, previousRecoverySnapshotId, bucketLeaderEpoch);
        }
    }
}

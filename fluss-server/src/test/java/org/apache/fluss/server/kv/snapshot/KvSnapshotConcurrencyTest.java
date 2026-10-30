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
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.local.LocalFileSystem;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.autoinc.TestingSequenceIDCounter;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.testutils.KvTestUtils;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.CloseableRegistry;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.server.kv.KvTabletTestUtils.flushAndWait;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests snapshot I/O against concurrent operations on real KV tablets. */
class KvSnapshotConcurrencyTest {

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("value", DataTypes.STRING())
                    .column("sequence", DataTypes.BIGINT())
                    .primaryKey("id")
                    .enableAutoIncrement("sequence")
                    .build();

    @TempDir private Path directory;

    private final ExecutorService asyncExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService uploadExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService operations = Executors.newCachedThreadPool();
    private ScheduledExecutorService scheduler = new ManuallyTriggeredScheduledExecutorService();
    private final List<TabletFixture> tablets = new ArrayList<>();
    private final List<PeriodicSnapshotManager> managers = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (TabletFixture fixture : tablets) {
            fixture.remoteFs.release.countDown();
        }
        for (PeriodicSnapshotManager manager : managers) {
            manager.close();
        }
        for (TabletFixture fixture : tablets) {
            fixture.cancelRegistry.close();
        }
        scheduler.shutdownNow();
        asyncExecutor.shutdownNow();
        operations.shutdownNow();
        assertThat(asyncExecutor.awaitTermination(10, SECONDS)).isTrue();
        assertThat(operations.awaitTermination(10, SECONDS)).isTrue();
        for (TabletFixture fixture : tablets) {
            fixture.tablet.close();
            fixture.log.close();
            fixture.allocator.close();
        }
        uploadExecutor.shutdownNow();
        assertThat(uploadExecutor.awaitTermination(10, SECONDS)).isTrue();
    }

    @Test
    void testDirectoryInitializationDoesNotBlockTabletOperations() throws Exception {
        TabletFixture fixture = createTablet("a");
        fixture.putAndFlush(1);
        byte[] firstValue = fixture.tablet.getRocksDBKv().get(key(1));
        byte[] firstLookup = fixture.lookup(1);
        List<AutoIncIDRange> capturedRanges = fixture.state().getAutoIncIDRanges();
        assertThat(capturedRanges).hasSize(1);
        assertThat(capturedRanges.get(0).isEmpty()).isFalse();
        fixture.remoteFs.blockMkdirs = true;
        startManager(fixture);

        Future<?> trigger = operations.submit(this::triggerScheduledSnapshot);
        assertThat(fixture.remoteFs.entered.await(10, SECONDS)).isTrue();
        // The gate must intercept explicit initialization, before per-file uploads create parents.
        assertThat(fixture.remoteFs.mkdirsCalls)
                .containsExactly(FlussPaths.remoteKvSnapshotDir(fixture.remotePath, 1));
        Future<?> write =
                operations.submit(
                        () -> {
                            fixture.put(2);
                            assertThat(fixture.lookup(1)).isEqualTo(firstLookup);
                            fixture.flush();
                            return null;
                        });
        try {
            write.get(10, SECONDS);
            assertThat(fixture.tablet.getFlushedLogOffset()).isEqualTo(2);
            assertThat(fixture.tablet.getRowCount()).isEqualTo(2);
            assertThat(fixture.state().getAutoIncIDRanges()).isNotEqualTo(capturedRanges);
        } finally {
            fixture.remoteFs.release.countDown();
        }
        trigger.get(10, SECONDS);
        CompletedSnapshot snapshot = fixture.awaitCommit();
        assertThat(snapshot.getLogOffset()).isEqualTo(1);
        assertThat(snapshot.getRowCount()).isEqualTo(1);
        assertThat(snapshot.getAutoIncIDRanges()).isEqualTo(capturedRanges);
        CompletedSnapshot deserialized =
                CompletedSnapshotJsonSerde.fromJson(CompletedSnapshotJsonSerde.toJson(snapshot));
        assertThat(deserialized.getAutoIncIDRanges()).isEqualTo(capturedRanges);
        try (RocksDBKv restored =
                KvTestUtils.buildFromSnapshotHandle(
                        deserialized.getKvSnapshotHandle(), directory.resolve("restored"))) {
            assertThat(restored.get(key(1))).isEqualTo(firstValue);
            assertThat(restored.get(key(2))).isNull();
        }
    }

    @Test
    void testSlowDirectoryDoesNotBlockOtherBucketsSnapshotPreparation() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        TabletFixture first = createTablet("a");
        TabletFixture second = createTablet("b");
        assertThat(first.log.getTableBucket()).isNotEqualTo(second.log.getTableBucket());
        first.putAndFlush(1);
        second.putAndFlush(1);
        first.remoteFs.blockMkdirs = true;
        startManager(first);
        assertThat(first.remoteFs.entered.await(10, SECONDS)).isTrue();

        startManager(second);
        // initSnapshot returns only after the real native checkpoint has been captured.
        assertThat(second.prepared.poll(10, SECONDS)).isNotNull();
        assertThat(second.localSnapshot(1)).exists();
        operations
                .submit(
                        () -> {
                            second.putAndFlush(2);
                            return null;
                        })
                .get(10, SECONDS);
        assertThat(second.committed).isEmpty();
        assertThat(second.remoteFs.mkdirsCalls).isEmpty();

        first.remoteFs.release.countDown();
        assertThat(first.awaitCommit().getLogOffset()).isEqualTo(1);
        assertThat(second.awaitCommit().getLogOffset()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDirectoryFailureCleanupAndRetry(boolean deleteFails) throws Exception {
        TabletFixture fixture = createTablet("a");
        fixture.putAndFlush(1);
        startManager(fixture);
        triggerScheduledSnapshot();
        CompletedSnapshot previous = fixture.awaitCommit();
        byte[] firstValue = fixture.tablet.getRocksDBKv().get(key(1));
        fixture.putAndFlush(2);
        FsPath failedDirectory = FlussPaths.remoteKvSnapshotDir(fixture.remotePath, 2);
        fixture.remoteFs.failMkdirsAt = failedDirectory;
        fixture.remoteFs.blockDelete = true;
        fixture.remoteFs.failDelete = deleteFails;

        triggerScheduledSnapshot();
        assertThat(fixture.remoteFs.entered.await(10, SECONDS)).isTrue();
        assertThat(fixture.localSnapshot(2)).doesNotExist();
        assertThat(fixture.remoteFs.exists(failedDirectory)).isTrue();
        operations
                .submit(
                        () -> {
                            fixture.put(3);
                            return null;
                        })
                .get(10, SECONDS);
        fixture.remoteFs.release.countDown();
        awaitAsyncCompletion();

        assertThat(fixture.committed).isEmpty();
        assertThat(fixture.failed).containsExactly(2L);
        assertThat(fixture.minRetainOffset).hasValue(1);
        assertThat(fixture.remoteFs.deleteCalls).containsExactly(failedDirectory);
        assertThat(fixture.remoteFs.exists(failedDirectory)).isEqualTo(deleteFails);
        assertThat(manualScheduler().getActiveScheduledTasks()).hasSize(1);
        try (RocksDBKv restored =
                KvTestUtils.buildFromSnapshotHandle(
                        previous.getKvSnapshotHandle(), directory.resolve("previous"))) {
            assertThat(restored.get(key(1))).isEqualTo(firstValue);
            assertThat(restored.get(key(2))).isNull();
        }

        // Retry at the same flushed offset: a failed snapshot must not advance the latest offset.
        assertThat(fixture.tablet.getFlushedLogOffset()).isEqualTo(2);
        triggerScheduledSnapshot();
        CompletedSnapshot retried = fixture.awaitCommit();
        assertThat(retried.getSnapshotID()).isEqualTo(3);
        assertThat(retried.getLogOffset()).isEqualTo(2);
        assertThat(fixture.minRetainOffset).hasValue(2);
        assertThat(fixture.localSnapshot(3)).doesNotExist();

        int mkdirsBeforeSkippedSnapshot = fixture.remoteFs.mkdirsCalls.size();
        triggerScheduledSnapshot();
        awaitAsyncCompletion();
        assertThat(fixture.remoteFs.mkdirsCalls).hasSize(mkdirsBeforeSkippedSnapshot);
        assertThat(fixture.committed).isEmpty();
        assertThat(manualScheduler().getActiveScheduledTasks()).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCancellationBeforeDirectoryInitialization(boolean closeRegistry) throws Exception {
        TabletFixture fixture = createTablet("a");
        fixture.putAndFlush(1);
        CountDownLatch releaseWorker = new CountDownLatch(1);
        asyncExecutor.submit(
                () -> {
                    releaseWorker.await(30, SECONDS);
                    return null;
                });
        try {
            PeriodicSnapshotManager manager = startManager(fixture);
            triggerScheduledSnapshot();
            RunnableFuture<SnapshotResult> snapshot = fixture.awaitPrepared();
            assertThat(fixture.localSnapshot(1)).exists();
            cancel(fixture, snapshot, closeRegistry);
            assertThat(snapshot.isCancelled()).isTrue();
            assertThat(fixture.localSnapshot(1)).doesNotExist();
            assertThat(fixture.remoteFs.mkdirsCalls).isEmpty();
            manager.close();
        } finally {
            releaseWorker.countDown();
        }
        awaitAsyncCompletion();
        assertThat(fixture.committed).isEmpty();
        assertThat(fixture.remoteFs.mkdirsCalls).isEmpty();
        assertThat(fixture.failed).containsExactly(1L);
        assertThat(manualScheduler().getActiveScheduledTasks()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCancellationDuringDirectoryInitialization(boolean closeRegistry) throws Exception {
        TabletFixture fixture = createTablet("a");
        fixture.putAndFlush(1);
        fixture.remoteFs.blockMkdirs = true;
        fixture.remoteFs.waitForReleaseAfterInterrupt = true;
        PeriodicSnapshotManager manager = startManager(fixture);
        triggerScheduledSnapshot();
        RunnableFuture<SnapshotResult> snapshot = fixture.awaitPrepared();
        assertThat(fixture.remoteFs.entered.await(10, SECONDS)).isTrue();

        cancel(fixture, snapshot, closeRegistry);
        assertThat(snapshot.isCancelled()).isTrue();
        assertThat(fixture.remoteFs.interrupted.await(10, SECONDS)).isTrue();
        // Running code owns the checkpoint until the filesystem call actually exits.
        assertThat(fixture.localSnapshot(1)).exists();
        manager.close();
        fixture.remoteFs.release.countDown();
        awaitAsyncCompletion();

        assertThat(fixture.localSnapshot(1)).doesNotExist();
        assertThat(fixture.committed).isEmpty();
        assertThat(fixture.failed).containsExactly(1L);
        assertThat(fixture.minRetainOffset).hasValue(0);
        assertThat(manualScheduler().getActiveScheduledTasks()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testManagerCloseStopsReschedulingWithoutCancellingSnapshot(boolean mkdirsFails)
            throws Exception {
        TabletFixture fixture = createTablet("a");
        fixture.putAndFlush(1);
        fixture.remoteFs.blockMkdirs = true;
        if (mkdirsFails) {
            fixture.remoteFs.failMkdirsAt = FlussPaths.remoteKvSnapshotDir(fixture.remotePath, 1);
        }
        PeriodicSnapshotManager manager = startManager(fixture);
        triggerScheduledSnapshot();
        RunnableFuture<SnapshotResult> snapshot = fixture.awaitPrepared();
        assertThat(fixture.remoteFs.entered.await(10, SECONDS)).isTrue();
        manager.close();
        assertThat(snapshot.isCancelled()).isFalse();
        fixture.remoteFs.release.countDown();
        awaitAsyncCompletion();

        assertThat(fixture.localSnapshot(1)).doesNotExist();
        assertThat(fixture.committed).hasSize(mkdirsFails ? 0 : 1);
        assertThat(fixture.failed).hasSize(mkdirsFails ? 1 : 0);
        assertThat(manualScheduler().getActiveScheduledTasks()).isEmpty();
    }

    private void cancel(
            TabletFixture fixture, RunnableFuture<SnapshotResult> snapshot, boolean closeRegistry)
            throws IOException {
        if (closeRegistry) {
            fixture.cancelRegistry.close();
        } else {
            assertThat(snapshot.cancel(true)).isTrue();
        }
    }

    private void awaitAsyncCompletion() throws Exception {
        asyncExecutor.submit(() -> {}).get(10, SECONDS);
    }

    private TabletFixture createTablet(String name) throws Exception {
        TabletFixture fixture = new TabletFixture(directory.resolve(name));
        tablets.add(fixture);
        return fixture;
    }

    private PeriodicSnapshotManager startManager(TabletFixture fixture) {
        // Use a short initial delay for the real scheduler and a long delay for later snapshots.
        AtomicLong interval = new AtomicLong(1);
        PeriodicSnapshotManager manager =
                new PeriodicSnapshotManager(
                        fixture.log.getTableBucket(),
                        fixture.target,
                        interval::get,
                        asyncExecutor,
                        scheduler,
                        fixture.tablet.getGuardedExecutor());
        interval.set(SECONDS.toMillis(3600));
        managers.add(manager);
        manager.start();
        return manager;
    }

    private void triggerScheduledSnapshot() {
        manualScheduler().triggerNonPeriodicScheduledTask();
    }

    private ManuallyTriggeredScheduledExecutorService manualScheduler() {
        return (ManuallyTriggeredScheduledExecutorService) scheduler;
    }

    private static byte[] key(int id) {
        return ("k" + id).getBytes(UTF_8);
    }

    private class TabletFixture {
        private final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        private final CloseableRegistry cancelRegistry = new CloseableRegistry();
        private final BlockingFileSystem remoteFs = new BlockingFileSystem();
        private final BlockingQueue<CompletedSnapshot> committed = new LinkedBlockingQueue<>();
        private final BlockingQueue<PeriodicSnapshotManager.SnapshotRunnable> prepared =
                new LinkedBlockingQueue<>();
        private final List<Long> failed = new CopyOnWriteArrayList<>();
        private final AtomicLong minRetainOffset = new AtomicLong();
        private final Path localPath;
        private final FsPath remotePath;
        private final LogTablet log;
        private final KvTablet tablet;
        private final KvTabletSnapshotTarget target;

        private TabletFixture(Path root) throws Exception {
            Configuration conf = new Configuration();
            TableConfig tableConf = new TableConfig(conf);
            PhysicalTablePath tablePath = PhysicalTablePath.of(TablePath.of("testDb", "t1"));
            TestingSchemaGetter schemaGetter = new TestingSchemaGetter(new SchemaInfo(SCHEMA, 1));
            File logRoot = Files.createDirectories(root.resolve("log")).toFile();
            File logDir =
                    Files.createDirectories(
                                    logRoot.toPath().resolve("testDb/t1-1/log-" + tablets.size()))
                            .toFile();
            log =
                    LogTablet.create(
                            logRoot,
                            tablePath,
                            logDir,
                            conf,
                            new AtomicBoolean(
                                    conf.get(
                                            ConfigOptions
                                                    .LOG_RETENTION_ROLL_ACTIVE_SEGMENT_ENABLED)),
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            0,
                            new FlussScheduler(1),
                            LogFormat.ARROW,
                            1,
                            true,
                            SystemClock.getInstance(),
                            true);
            localPath = root.resolve("kv");
            tablet =
                    KvTablet.create(
                            tablePath,
                            log.getTableBucket(),
                            log,
                            localPath.toFile(),
                            conf,
                            TestingMetricGroups.TABLET_SERVER_METRICS,
                            allocator,
                            new TestingMemorySegmentPool(10 * 1024),
                            KvFormat.COMPACTED,
                            RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter),
                            DEFAULT_COMPRESSION,
                            schemaGetter,
                            tableConf.getChangelogImage(),
                            KvManager.getDefaultRateLimiter(),
                            new AutoIncrementManager(
                                    schemaGetter,
                                    tablePath.getTablePath(),
                                    tableConf,
                                    new TestingSequenceGeneratorFactory()),
                            SystemClock.getInstance(),
                            tableConf);
            remotePath =
                    new FsPath(root.resolve("remote").toUri()) {
                        @Override
                        public FileSystem getFileSystem() {
                            return remoteFs;
                        }
                    };
            RocksIncrementalSnapshot incremental =
                    tablet.createIncrementalSnapshot(
                            new HashMap<>(),
                            new KvSnapshotDataUploader(uploadExecutor),
                            -1,
                            new ThreadSafeSimpleCounter());
            target =
                    new KvTabletSnapshotTarget(
                            log.getTableBucket(),
                            (snapshot, coordinatorEpoch, leaderEpoch) -> committed.add(snapshot),
                            null,
                            incremental,
                            remotePath,
                            4096,
                            uploadExecutor,
                            cancelRegistry,
                            new TestingSequenceIDCounter(new AtomicLong(1)),
                            tablet::getTabletState,
                            minRetainOffset::set,
                            () -> 1,
                            () -> 1,
                            0,
                            0) {
                        @Override
                        public Optional<PeriodicSnapshotManager.SnapshotRunnable> initSnapshot()
                                throws Exception {
                            Optional<PeriodicSnapshotManager.SnapshotRunnable> snapshot =
                                    super.initSnapshot();
                            snapshot.ifPresent(prepared::add);
                            return snapshot;
                        }

                        @Override
                        public void handleSnapshotFailure(
                                long snapshotId, SnapshotLocation location, Throwable cause) {
                            super.handleSnapshotFailure(snapshotId, location, cause);
                            failed.add(snapshotId);
                        }
                    };
        }

        private void put(int id) throws Exception {
            tablet.putAsLeader(
                    KvRecordTestUtils.KvRecordBatchFactory.of((short) 1)
                            .ofRecords(
                                    KvRecordTestUtils.KvRecordFactory.of(
                                                    SCHEMA.getRowType().project(new int[] {0, 1}))
                                            .ofRecord(key(id), new Object[] {id, "v" + id})),
                    new int[] {0, 1});
        }

        private void flush() throws Exception {
            flushAndWait(tablet, log.localLogEndOffset());
        }

        private void putAndFlush(int id) throws Exception {
            put(id);
            flush();
        }

        private byte[] lookup(int id) throws IOException {
            return tablet.multiGet(Collections.singletonList(key(id))).get(0).toByteArray();
        }

        private TabletState state() {
            AtomicReference<TabletState> state = new AtomicReference<>();
            tablet.getGuardedExecutor().execute(() -> state.set(tablet.getTabletState()));
            return state.get();
        }

        private CompletedSnapshot awaitCommit() throws Exception {
            CompletedSnapshot snapshot = committed.poll(10, SECONDS);
            assertThat(snapshot).as("committed snapshot").isNotNull();
            // The committer is called before the target updates its state and reschedules.
            awaitAsyncCompletion();
            return snapshot;
        }

        private RunnableFuture<SnapshotResult> awaitPrepared() throws InterruptedException {
            PeriodicSnapshotManager.SnapshotRunnable snapshot = prepared.poll(10, SECONDS);
            assertThat(snapshot).as("prepared snapshot").isNotNull();
            return snapshot.getSnapshotRunnable();
        }

        private Path localSnapshot(long snapshotId) {
            return localPath.resolve("snap-" + snapshotId);
        }
    }

    private static class BlockingFileSystem extends LocalFileSystem {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch interrupted = new CountDownLatch(1);
        private final List<FsPath> mkdirsCalls = new CopyOnWriteArrayList<>();
        private final List<FsPath> deleteCalls = new CopyOnWriteArrayList<>();
        private volatile boolean blockMkdirs;
        private volatile boolean blockDelete;
        private volatile boolean failDelete;
        private volatile boolean waitForReleaseAfterInterrupt;
        private volatile FsPath failMkdirsAt;

        @Override
        public boolean mkdirs(FsPath path) throws IOException {
            mkdirsCalls.add(path);
            if (blockMkdirs) {
                awaitRelease();
            }
            boolean created = super.mkdirs(path);
            if (path.equals(failMkdirsAt)) {
                throw new IOException("Directory initialization failed after creating " + path);
            }
            return created;
        }

        @Override
        public boolean delete(FsPath path, boolean recursive) throws IOException {
            deleteCalls.add(path);
            if (blockDelete) {
                awaitRelease();
            }
            if (failDelete) {
                throw new IOException("Directory cleanup failed for " + path);
            }
            return super.delete(path, recursive);
        }

        private void awaitRelease() throws IOException {
            entered.countDown();
            InterruptedException interruption = null;
            while (true) {
                try {
                    if (!release.await(30, SECONDS)) {
                        throw new IOException("Filesystem gate was not released");
                    }
                    break;
                } catch (InterruptedException e) {
                    interruption = e;
                    interrupted.countDown();
                    if (!waitForReleaseAfterInterrupt) {
                        break;
                    }
                }
            }
            if (interruption != null) {
                Thread.currentThread().interrupt();
                throw new IOException(interruption);
            }
        }
    }
}

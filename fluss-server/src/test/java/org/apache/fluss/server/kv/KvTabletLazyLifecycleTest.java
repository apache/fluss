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

package org.apache.fluss.server.kv;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestData;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link KvTabletLazyLifecycle}. */
final class KvTabletLazyLifecycleTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final short SCHEMA_ID = 1;
    private static final RowType BASE_ROW_TYPE = TestData.DATA1_ROW_TYPE;

    private final KvRecordTestUtils.KvRecordBatchFactory batchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
    private final KvRecordTestUtils.KvRecordFactory recordFactory =
            KvRecordTestUtils.KvRecordFactory.of(BASE_ROW_TYPE);

    private static ZooKeeperClient zkClient;

    private @TempDir File tempDir;

    private LogManager logManager;
    private KvManager kvManager;
    private Configuration conf;

    private PhysicalTablePath physicalPath;
    private TableBucket tableBucket;
    private LogTablet logTablet;

    @BeforeAll
    static void baseBeforeAll() {
        zkClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void setup() throws Exception {
        conf = new Configuration();
        conf.setString(ConfigOptions.DATA_DIR, tempDir.getAbsolutePath());
        conf.set(ConfigOptions.KV_LAZY_OPEN_ENABLED, true);

        logManager =
                LogManager.create(
                        conf,
                        zkClient,
                        new FlussScheduler(1),
                        SystemClock.getInstance(),
                        TestingMetricGroups.TABLET_SERVER_METRICS);
        kvManager =
                KvManager.create(
                        conf, zkClient, logManager, TestingMetricGroups.TABLET_SERVER_METRICS);
        kvManager.startup();

        TablePath tablePath = TablePath.of("db1", "t_lazy");
        physicalPath =
                PhysicalTablePath.of(tablePath.getDatabaseName(), tablePath.getTableName(), null);
        tableBucket = new TableBucket(20001L, 0);
        logTablet = logManager.getOrCreateLog(physicalPath, tableBucket, LogFormat.ARROW, 1, true);
    }

    @AfterEach
    void tearDown() {
        if (kvManager != null) {
            kvManager.shutdown();
        }
        if (logManager != null) {
            logManager.shutdown();
        }
    }

    // ---- Helper methods ----

    private KvTablet createSentinel() {
        return KvTablet.createLazySentinel(
                physicalPath, tableBucket, logTablet, TestingMetricGroups.TABLET_SERVER_METRICS);
    }

    private KvTabletLazyLifecycle configureLifecycle(KvTablet sentinel, ManualClock clock) {
        KvTabletLazyLifecycle lifecycle = sentinel.getLifecycle();
        lifecycle.configureLazyOpen(
                clock,
                kvManager.getOpenSemaphore(),
                kvManager.getOpenTimeoutMs(),
                kvManager.getFailedBackoffBaseMs(),
                kvManager.getFailedBackoffMaxMs(),
                KvManager.RELEASE_DRAIN_TIMEOUT_MS);
        lifecycle.setLeaderEpochSupplier(() -> 1);
        lifecycle.setBucketEpochSupplier(() -> 1);
        lifecycle.setTabletDirSupplier(() -> kvManager.getTabletDirPath(physicalPath, tableBucket));
        lifecycle.setOpenCallback(
                hasLocal ->
                        kvManager.createKvTabletUnregistered(
                                physicalPath,
                                tableBucket,
                                logTablet,
                                KvFormat.COMPACTED,
                                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)),
                                new TableConfig(new Configuration()),
                                DEFAULT_COMPRESSION));
        lifecycle.setCommitCallback(kv -> {});
        lifecycle.setReleaseCallback(kv -> {});
        lifecycle.setDropCallback(kv -> {});
        return lifecycle;
    }

    // ---- Tests ----

    @Test
    void testAcquireGuardTriggersOpen() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);

        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
            // Verify we can use the tablet inside the guard
            List<byte[]> values = sentinel.multiGet(Collections.singletonList("k1".getBytes()));
            assertThat(values).hasSize(1);
            assertThat(values.get(0)).isNull();
        }
    }

    @Test
    void testAcquireGuardFastPath() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Track open callback invocations to prove fast path skips it
        AtomicInteger openCount = new AtomicInteger(0);
        lifecycle.setOpenCallback(
                hasLocal -> {
                    openCount.incrementAndGet();
                    return kvManager.createKvTabletUnregistered(
                            physicalPath,
                            tableBucket,
                            logTablet,
                            KvFormat.COMPACTED,
                            new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)),
                            new TableConfig(new Configuration()),
                            DEFAULT_COMPRESSION);
                });

        // First open via slow path
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        }
        assertThat(openCount.get()).isEqualTo(1);

        // Second acquireGuard should take the fast path (already OPEN, no openCallback)
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
            List<byte[]> values =
                    sentinel.multiGet(Collections.singletonList("nonexistent".getBytes()));
            assertThat(values).hasSize(1);
            assertThat(values.get(0)).isNull();
        }
        // openCallback must NOT have been called again
        assertThat(openCount.get()).isEqualTo(1);
    }

    @Test
    void testFailedBackoffRejectsOpen() {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Override openCallback to throw
        lifecycle.setOpenCallback(
                hasLocal -> {
                    throw new RuntimeException("simulated open failure");
                });

        // First acquireGuard should fail
        assertThatThrownBy(sentinel::acquireGuard).isInstanceOf(RuntimeException.class);

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.FAILED);

        // Subsequent acquireGuard within cooldown should throw with cooldown message
        assertThatThrownBy(sentinel::acquireGuard)
                .isInstanceOf(KvStorageException.class)
                .hasMessageContaining("cooldown");
    }

    @Test
    void testFailedBackoffRetryAfterCooldown() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Override openCallback to throw
        lifecycle.setOpenCallback(
                hasLocal -> {
                    throw new RuntimeException("simulated open failure");
                });

        // First acquireGuard fails
        assertThatThrownBy(sentinel::acquireGuard).isInstanceOf(RuntimeException.class);

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.FAILED);

        // Advance clock past the backoff (5000 * 2^1 = 10000ms for failureCount=1)
        clock.advanceTime(11, TimeUnit.SECONDS);

        // Fix the openCallback
        lifecycle.setOpenCallback(
                hasLocal ->
                        kvManager.createKvTabletUnregistered(
                                physicalPath,
                                tableBucket,
                                logTablet,
                                KvFormat.COMPACTED,
                                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)),
                                new TableConfig(new Configuration()),
                                DEFAULT_COMPRESSION));

        // Now acquireGuard should succeed
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        }
    }

    @Test
    void testReleaseKvSuccess() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Open and write+flush data
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            KvRecord record = recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "a"});
            KvRecordBatch batch = batchFactory.ofRecords(Collections.singletonList(record));
            sentinel.putAsLeader(batch, null);
            sentinel.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        }

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);

        // Capture flushed offset before release (detach may reset it)
        long flushedOffsetBeforeRelease = sentinel.getFlushedLogOffset();

        // Release should succeed
        boolean released = sentinel.releaseKv();
        assertThat(released).isTrue();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);
        // Row count and flushed offset should be cached
        assertThat(lifecycle.getCachedRowCount()).isEqualTo(1L);
        assertThat(lifecycle.getCachedFlushedLogOffset()).isEqualTo(flushedOffsetBeforeRelease);
    }

    @Test
    void testReleaseKvRejectsUnflushedData() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Open and write data WITHOUT flush
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            KvRecord record = recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "a"});
            KvRecordBatch batch = batchFactory.ofRecords(Collections.singletonList(record));
            sentinel.putAsLeader(batch, null);
            // Intentionally NOT flushing
        }

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);

        // Release should fail because data is unflushed
        boolean released = sentinel.releaseKv();
        assertThat(released).isFalse();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
    }

    @Test
    void testReleaseAndReopenPreservesData() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Open, write+flush
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            KvRecord record = recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "a"});
            KvRecordBatch batch = batchFactory.ofRecords(Collections.singletonList(record));
            sentinel.putAsLeader(batch, null);
            sentinel.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        }

        // Release to LAZY
        boolean released = sentinel.releaseKv();
        assertThat(released).isTrue();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);

        // Reopen via acquireGuard and verify data is still readable
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
            List<byte[]> values = sentinel.multiGet(Collections.singletonList("k1".getBytes()));
            assertThat(values).hasSize(1);
            byte[] expected =
                    ValueEncoder.encodeValue(
                            SCHEMA_ID, compactedRow(BASE_ROW_TYPE, new Object[] {1, "a"}));
            assertThat(values.get(0)).isEqualTo(expected);
        }
    }

    @Test
    void testReleaseKvRejectsWriteThatCompletesDuringDrain() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        KvTablet.Guard guard = sentinel.acquireGuard();
        FutureTask<Boolean> releaseTask = new FutureTask<>(sentinel::releaseKv);
        Thread releaseThread = new Thread(releaseTask, "release-during-write");
        releaseThread.start();

        CommonTestUtils.waitUntil(
                () -> lifecycle.getLazyState() == KvTabletLazyLifecycle.LazyState.RELEASING,
                Duration.ofSeconds(5),
                "Timed out waiting for RELEASING state");
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.RELEASING);

        KvRecord record = recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "a"});
        KvRecordBatch batch = batchFactory.ofRecords(Collections.singletonList(record));
        sentinel.putAsLeader(batch, null);
        guard.close();
        assertThat(releaseTask.get(5, TimeUnit.SECONDS)).isFalse();

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        assertThat(sentinel.getKvPreWriteBuffer().getAllKvEntries()).hasSize(1);
    }

    @Test
    void testDropKvFromLazy() {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);

        sentinel.dropKvLazy();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.CLOSED);
    }

    @Test
    void testDropKvFromOpen() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Open the tablet
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        }

        File kvDir = sentinel.getKvTabletDir();
        assertThat(kvDir).isNotNull();
        assertThat(kvDir).exists();

        sentinel.dropKvLazy();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.CLOSED);
        assertThat(sentinel.getRocksDBKv()).isNull();
        assertThat(kvDir).doesNotExist();
    }

    @Test
    void testDropKvDuringReleasingRollbackStillClosesAndDeletes() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            KvRecord record = recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "a"});
            KvRecordBatch batch = batchFactory.ofRecords(Collections.singletonList(record));
            sentinel.putAsLeader(batch, null);
            sentinel.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        }

        File kvDir = sentinel.getKvTabletDir();
        assertThat(kvDir).isNotNull();
        assertThat(kvDir).exists();

        CountDownLatch releaseCallbackEntered = new CountDownLatch(1);
        CountDownLatch allowReleaseCallbackFinish = new CountDownLatch(1);
        lifecycle.setReleaseCallback(
                kv -> {
                    releaseCallbackEntered.countDown();
                    try {
                        allowReleaseCallbackFinish.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new RuntimeException("simulated release failure");
                });

        FutureTask<Boolean> releaseTask = new FutureTask<>(sentinel::releaseKv);
        Thread releaseThread = new Thread(releaseTask, "release-with-rollback");
        releaseThread.start();

        assertThat(releaseCallbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.RELEASING);

        FutureTask<Void> dropTask =
                new FutureTask<>(
                        () -> {
                            sentinel.dropKvLazy();
                            return null;
                        });
        Thread dropThread = new Thread(dropTask, "drop-during-release-rollback");
        dropThread.start();

        allowReleaseCallbackFinish.countDown();
        assertThat(releaseTask.get(5, TimeUnit.SECONDS)).isFalse();
        dropTask.get(5, TimeUnit.SECONDS);

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.CLOSED);
        assertThat(sentinel.getRocksDBKv()).isNull();
        assertThat(kvDir).doesNotExist();
    }

    @Test
    void testCommitCallbackFailureDoesNotFailOpen() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        lifecycle.setCommitCallback(
                kv -> {
                    throw new RuntimeException("simulated commit callback failure");
                });

        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
            assertThat(sentinel.getRocksDBKv()).isNotNull();
        }
    }

    @Test
    void testDropWaitsForCommitCallbackCompletion() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        CountDownLatch commitCallbackEntered = new CountDownLatch(1);
        CountDownLatch allowCommitCallbackFinish = new CountDownLatch(1);
        lifecycle.setCommitCallback(
                kv -> {
                    commitCallbackEntered.countDown();
                    try {
                        allowCommitCallbackFinish.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

        AtomicReference<Throwable> openFailure = new AtomicReference<>();
        Thread openThread =
                new Thread(
                        () -> {
                            try (KvTablet.Guard guard = sentinel.acquireGuard()) {
                                // no-op
                            } catch (Throwable t) {
                                openFailure.set(t);
                            }
                        },
                        "open-with-blocked-commit");
        openThread.start();

        assertThat(commitCallbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);

        FutureTask<Void> dropTask =
                new FutureTask<>(
                        () -> {
                            sentinel.dropKvLazy();
                            return null;
                        });
        Thread dropThread = new Thread(dropTask, "drop-during-commit");
        dropThread.start();

        // Wait for drop thread to block (commit callback holds the lock)
        CommonTestUtils.waitUntil(
                () -> {
                    Thread.State s = dropThread.getState();
                    return s == Thread.State.WAITING || s == Thread.State.TIMED_WAITING;
                },
                Duration.ofSeconds(5),
                "Timed out waiting for drop thread to block");
        assertThat(dropTask.isDone()).isFalse();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);

        allowCommitCallbackFinish.countDown();
        openThread.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(openThread.isAlive()).isFalse();
        dropTask.get(5, TimeUnit.SECONDS);

        // The open thread may or may not see a KvStorageException depending on the race
        // between guard.close() and dropKvLazy() transitioning to CLOSED. Both outcomes are valid:
        // - null: guard closed before drop detached RocksDB
        // - KvStorageException: drop completed while open thread was still in acquireGuard
        Throwable failure = openFailure.get();
        assertThat(failure == null || failure instanceof KvStorageException)
                .as(
                        "Expected null or KvStorageException but got: %s",
                        failure != null ? failure.getClass().getName() : "null")
                .isTrue();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.CLOSED);
        assertThat(sentinel.getRocksDBKv()).isNull();
    }

    @Test
    void testCloseMarksLazyTabletClosed() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        }

        sentinel.close();

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.CLOSED);
        assertThat(sentinel.getRocksDBKv()).isNull();
        assertThatThrownBy(sentinel::acquireGuard).isInstanceOf(KvStorageException.class);
    }

    @Test
    void testGuardedExecutorPinsOpenTabletUntilTaskCompletes() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        }

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch allowTaskComplete = new CountDownLatch(1);
        FutureTask<Void> guardedTask =
                new FutureTask<>(
                        () -> {
                            sentinel.getGuardedExecutor()
                                    .execute(
                                            () -> {
                                                taskStarted.countDown();
                                                try {
                                                    allowTaskComplete.await(5, TimeUnit.SECONDS);
                                                } catch (InterruptedException e) {
                                                    Thread.currentThread().interrupt();
                                                }
                                            });
                            return null;
                        });
        Thread taskThread = new Thread(guardedTask, "guarded-executor-task");
        taskThread.start();

        assertThat(taskStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(sentinel.getActivePins()).isEqualTo(1);

        FutureTask<Boolean> releaseTask = new FutureTask<>(sentinel::releaseKv);
        Thread releaseThread = new Thread(releaseTask, "release-task");
        releaseThread.start();

        CommonTestUtils.waitUntil(
                () -> lifecycle.getLazyState() == KvTabletLazyLifecycle.LazyState.RELEASING,
                Duration.ofSeconds(5),
                "Timed out waiting for RELEASING state");
        assertThat(releaseTask.isDone()).isFalse();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.RELEASING);

        allowTaskComplete.countDown();
        assertThat(releaseTask.get(5, TimeUnit.SECONDS)).isTrue();
        taskThread.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(taskThread.isAlive()).isFalse();

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);
        assertThat(sentinel.getActivePins()).isZero();
    }

    @Test
    void testGaugeUpdatesOnTransitions() throws Exception {
        TabletServerMetricGroup mg = TestingMetricGroups.TABLET_SERVER_METRICS;
        // Record baseline counts before this test
        int baseLazy = mg.kvTabletLazyCount().get();
        int baseOpen = mg.kvTabletOpenCount().get();
        int baseFailed = mg.kvTabletFailedCount().get();

        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // After configureLazyOpen: LAZY +1
        assertThat(mg.kvTabletLazyCount().get() - baseLazy).isEqualTo(1);
        assertThat(mg.kvTabletOpenCount().get() - baseOpen).isEqualTo(0);
        assertThat(mg.kvTabletFailedCount().get() - baseFailed).isEqualTo(0);

        // Open: LAZY -1, OPEN +1
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            KvRecord record = recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "a"});
            KvRecordBatch batch = batchFactory.ofRecords(Collections.singletonList(record));
            sentinel.putAsLeader(batch, null);
            sentinel.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        }
        assertThat(mg.kvTabletLazyCount().get() - baseLazy).isEqualTo(0);
        assertThat(mg.kvTabletOpenCount().get() - baseOpen).isEqualTo(1);

        // Release: OPEN -1, LAZY +1
        boolean released = sentinel.releaseKv();
        assertThat(released).isTrue();
        assertThat(mg.kvTabletLazyCount().get() - baseLazy).isEqualTo(1);
        assertThat(mg.kvTabletOpenCount().get() - baseOpen).isEqualTo(0);

        // Open again: LAZY -1, OPEN +1
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        }
        assertThat(mg.kvTabletLazyCount().get() - baseLazy).isEqualTo(0);
        assertThat(mg.kvTabletOpenCount().get() - baseOpen).isEqualTo(1);

        // Drop: OPEN -1
        sentinel.dropKvLazy();
        assertThat(mg.kvTabletOpenCount().get() - baseOpen).isEqualTo(0);
        assertThat(mg.kvTabletLazyCount().get() - baseLazy).isEqualTo(0);
        assertThat(mg.kvTabletFailedCount().get() - baseFailed).isEqualTo(0);
    }

    @Test
    void testEpochFencingRejectsStaleOpenOnLeaderEpochChange() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        AtomicInteger leaderEpoch = new AtomicInteger(1);
        lifecycle.setLeaderEpochSupplier(leaderEpoch::get);

        CountDownLatch openStarted = new CountDownLatch(1);
        CountDownLatch allowOpenFinish = new CountDownLatch(1);

        lifecycle.setOpenCallback(
                hasLocal -> {
                    openStarted.countDown();
                    try {
                        allowOpenFinish.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return kvManager.createKvTabletUnregistered(
                            physicalPath,
                            tableBucket,
                            logTablet,
                            KvFormat.COMPACTED,
                            new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)),
                            new TableConfig(new Configuration()),
                            DEFAULT_COMPRESSION);
                });

        AtomicReference<Throwable> openFailure = new AtomicReference<>();
        Thread openThread =
                new Thread(
                        () -> {
                            try (KvTablet.Guard guard = sentinel.acquireGuard()) {
                                // no-op
                            } catch (Throwable t) {
                                openFailure.set(t);
                            }
                        },
                        "epoch-fence-leader");
        openThread.start();

        assertThat(openStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // Change leader epoch while open is in progress
        leaderEpoch.set(2);
        allowOpenFinish.countDown();

        openThread.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(openThread.isAlive()).isFalse();

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.FAILED);
        assertThat(openFailure.get()).isNotNull();
    }

    @Test
    void testEpochFencingRejectsStaleOpenOnBucketEpochChange() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        AtomicInteger bucketEpoch = new AtomicInteger(1);
        lifecycle.setBucketEpochSupplier(bucketEpoch::get);

        CountDownLatch openStarted = new CountDownLatch(1);
        CountDownLatch allowOpenFinish = new CountDownLatch(1);

        lifecycle.setOpenCallback(
                hasLocal -> {
                    openStarted.countDown();
                    try {
                        allowOpenFinish.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return kvManager.createKvTabletUnregistered(
                            physicalPath,
                            tableBucket,
                            logTablet,
                            KvFormat.COMPACTED,
                            new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)),
                            new TableConfig(new Configuration()),
                            DEFAULT_COMPRESSION);
                });

        AtomicReference<Throwable> openFailure = new AtomicReference<>();
        Thread openThread =
                new Thread(
                        () -> {
                            try (KvTablet.Guard guard = sentinel.acquireGuard()) {
                                // no-op
                            } catch (Throwable t) {
                                openFailure.set(t);
                            }
                        },
                        "epoch-fence-bucket");
        openThread.start();

        assertThat(openStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // Change bucket epoch while open is in progress
        bucketEpoch.set(2);
        allowOpenFinish.countDown();

        openThread.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(openThread.isAlive()).isFalse();

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.FAILED);
        assertThat(openFailure.get()).isNotNull();
    }

    @Test
    void testConcurrentAcquireGuardOnlyOpensOnce() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        AtomicInteger openCount = new AtomicInteger(0);
        CountDownLatch openStarted = new CountDownLatch(1);
        CountDownLatch allowOpenFinish = new CountDownLatch(1);

        lifecycle.setOpenCallback(
                hasLocal -> {
                    openCount.incrementAndGet();
                    openStarted.countDown();
                    try {
                        allowOpenFinish.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return kvManager.createKvTabletUnregistered(
                            physicalPath,
                            tableBucket,
                            logTablet,
                            KvFormat.COMPACTED,
                            new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)),
                            new TableConfig(new Configuration()),
                            DEFAULT_COMPRESSION);
                });

        int threadCount = 5;
        CountDownLatch barrier = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            threads[i] =
                    new Thread(
                            () -> {
                                try {
                                    barrier.await(5, TimeUnit.SECONDS);
                                    try (KvTablet.Guard guard = sentinel.acquireGuard()) {
                                        successCount.incrementAndGet();
                                    }
                                } catch (Exception e) {
                                    failureCount.incrementAndGet();
                                }
                            },
                            "concurrent-acquire-" + i);
            threads[i].start();
        }

        // Release all threads at once
        barrier.countDown();

        // Wait for the opener thread to enter doSlowOpen
        assertThat(openStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPENING);

        // Let the open complete
        allowOpenFinish.countDown();

        // Wait for all threads to finish
        for (Thread t : threads) {
            t.join(TimeUnit.SECONDS.toMillis(10));
            assertThat(t.isAlive()).isFalse();
        }

        assertThat(openCount.get()).isEqualTo(1);
        assertThat(successCount.get()).isEqualTo(threadCount);
        assertThat(failureCount.get()).isZero();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
    }

    @Test
    void testDropKvDuringOpeningFencesAndCloses() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        CountDownLatch openStarted = new CountDownLatch(1);
        CountDownLatch allowOpenFinish = new CountDownLatch(1);

        lifecycle.setOpenCallback(
                hasLocal -> {
                    openStarted.countDown();
                    try {
                        allowOpenFinish.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return kvManager.createKvTabletUnregistered(
                            physicalPath,
                            tableBucket,
                            logTablet,
                            KvFormat.COMPACTED,
                            new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)),
                            new TableConfig(new Configuration()),
                            DEFAULT_COMPRESSION);
                });

        AtomicReference<Throwable> openFailure = new AtomicReference<>();
        Thread openThread =
                new Thread(
                        () -> {
                            try (KvTablet.Guard guard = sentinel.acquireGuard()) {
                                // no-op
                            } catch (Throwable t) {
                                openFailure.set(t);
                            }
                        },
                        "open-for-drop-fence");
        openThread.start();

        assertThat(openStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPENING);

        FutureTask<Void> dropTask =
                new FutureTask<>(
                        () -> {
                            sentinel.dropKvLazy();
                            return null;
                        });
        Thread dropThread = new Thread(dropTask, "drop-during-opening");
        dropThread.start();

        // Let the open finish — commitOpenResult should see generation mismatch
        allowOpenFinish.countDown();

        openThread.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(openThread.isAlive()).isFalse();
        dropTask.get(5, TimeUnit.SECONDS);

        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.CLOSED);
        assertThat(sentinel.getRocksDBKv()).isNull();
        assertThat(openFailure.get()).isNotNull();
    }

    @Test
    void testReleaseDrainTimeoutRollsBackToOpen() throws Exception {
        KvTablet sentinel = createSentinel();

        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTabletLazyLifecycle lifecycle = sentinel.getLifecycle();
        lifecycle.configureLazyOpen(
                clock,
                kvManager.getOpenSemaphore(),
                kvManager.getOpenTimeoutMs(),
                kvManager.getFailedBackoffBaseMs(),
                kvManager.getFailedBackoffMaxMs(),
                100L); // 100ms drain timeout
        lifecycle.setLeaderEpochSupplier(() -> 1);
        lifecycle.setBucketEpochSupplier(() -> 1);
        lifecycle.setTabletDirSupplier(() -> kvManager.getTabletDirPath(physicalPath, tableBucket));
        lifecycle.setOpenCallback(
                hasLocal ->
                        kvManager.createKvTabletUnregistered(
                                physicalPath,
                                tableBucket,
                                logTablet,
                                KvFormat.COMPACTED,
                                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)),
                                new TableConfig(new Configuration()),
                                DEFAULT_COMPRESSION));
        lifecycle.setCommitCallback(kv -> {});
        lifecycle.setReleaseCallback(kv -> {});
        lifecycle.setDropCallback(kv -> {});

        // Acquire a guard and hold it to prevent drain from completing
        KvTablet.Guard guard = sentinel.acquireGuard();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);

        // Start releaseKv in a background thread — it will block waiting for drain
        FutureTask<Boolean> releaseTask = new FutureTask<>(sentinel::releaseKv);
        Thread releaseThread = new Thread(releaseTask, "release-drain-timeout");
        releaseThread.start();

        // Wait for the release thread to enter RELEASING state
        CommonTestUtils.waitUntil(
                () -> lifecycle.getLazyState() == KvTabletLazyLifecycle.LazyState.RELEASING,
                Duration.ofSeconds(5),
                "Timed out waiting for RELEASING state");

        // Advance clock past drain timeout to trigger rollback deterministically
        clock.advanceTime(200, TimeUnit.MILLISECONDS);

        // The release should timeout and roll back to OPEN
        boolean released = releaseTask.get(5, TimeUnit.SECONDS);
        assertThat(released).isFalse();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);

        // Close the guard and verify pins are drained
        guard.close();
        assertThat(sentinel.getActivePins()).isZero();
    }

    @Test
    void testReleaseCallbackFailureRollsBackToOpenAndTabletRemainsFunctional() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Open and write+flush data
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            KvRecord record = recordFactory.ofRecord("k1".getBytes(), new Object[] {1, "a"});
            KvRecordBatch batch = batchFactory.ofRecords(Collections.singletonList(record));
            sentinel.putAsLeader(batch, null);
            sentinel.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        }

        // Set release callback to throw
        lifecycle.setReleaseCallback(
                kv -> {
                    throw new RuntimeException("simulated release callback failure");
                });

        // releaseKv should return false due to callback failure
        boolean released = sentinel.releaseKv();
        assertThat(released).isFalse();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);

        // Verify tablet is still functional — acquireGuard and multiGet should work
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            List<byte[]> values = sentinel.multiGet(Collections.singletonList("k1".getBytes()));
            assertThat(values).hasSize(1);
            byte[] expected =
                    ValueEncoder.encodeValue(
                            SCHEMA_ID, compactedRow(BASE_ROW_TYPE, new Object[] {1, "a"}));
            assertThat(values.get(0)).isEqualTo(expected);
        }

        // Fix the callback and verify subsequent release succeeds
        lifecycle.setReleaseCallback(kv -> {});
        released = sentinel.releaseKv();
        assertThat(released).isTrue();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);
    }

    @Test
    void testDropKvFromFailed() {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Override openCallback to throw
        lifecycle.setOpenCallback(
                hasLocal -> {
                    throw new RuntimeException("simulated open failure");
                });

        // First acquireGuard should fail
        assertThatThrownBy(sentinel::acquireGuard).isInstanceOf(RuntimeException.class);
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.FAILED);

        // Drop from FAILED state
        sentinel.dropKvLazy();
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.CLOSED);
    }

    @Test
    void testExponentialBackoffEscalatesAcrossMultipleFailures() throws Exception {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // Override openCallback to throw
        lifecycle.setOpenCallback(
                hasLocal -> {
                    throw new RuntimeException("simulated open failure");
                });

        // Failure 1: failureCount becomes 1, cooldown = 5000 * 2^1 = 10000ms
        assertThatThrownBy(sentinel::acquireGuard).isInstanceOf(RuntimeException.class);
        assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.FAILED);

        // Advance 9s — still within 10s cooldown
        clock.advanceTime(9, TimeUnit.SECONDS);
        assertThatThrownBy(sentinel::acquireGuard)
                .isInstanceOf(KvStorageException.class)
                .hasMessageContaining("cooldown");

        // Advance 2s more (total 11s, past 10s cooldown) — retry and fail again
        clock.advanceTime(2, TimeUnit.SECONDS);
        assertThatThrownBy(sentinel::acquireGuard).isInstanceOf(RuntimeException.class);

        // Failure 2: failureCount becomes 2, cooldown = 5000 * 2^2 = 20000ms
        // Advance 19s — still within 20s cooldown
        clock.advanceTime(19, TimeUnit.SECONDS);
        assertThatThrownBy(sentinel::acquireGuard)
                .isInstanceOf(KvStorageException.class)
                .hasMessageContaining("cooldown");

        // Advance 2s more (total 21s, past 20s cooldown) — retry and fail again
        clock.advanceTime(2, TimeUnit.SECONDS);
        assertThatThrownBy(sentinel::acquireGuard).isInstanceOf(RuntimeException.class);

        // Failure 3: failureCount becomes 3, cooldown = 5000 * 2^3 = 40000ms
        // Advance 39s — still within 40s cooldown
        clock.advanceTime(39, TimeUnit.SECONDS);
        assertThatThrownBy(sentinel::acquireGuard)
                .isInstanceOf(KvStorageException.class)
                .hasMessageContaining("cooldown");

        // Advance 2s more (total 41s, past 40s cooldown) — fix callback and verify recovery
        clock.advanceTime(2, TimeUnit.SECONDS);
        lifecycle.setOpenCallback(
                hasLocal ->
                        kvManager.createKvTabletUnregistered(
                                physicalPath,
                                tableBucket,
                                logTablet,
                                KvFormat.COMPACTED,
                                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID)),
                                new TableConfig(new Configuration()),
                                DEFAULT_COMPRESSION));
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(lifecycle.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        }
    }

    @Test
    void testGuardDoubleCloseDoesNotDecrementPinsBelowZero() {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        configureLifecycle(sentinel, clock);

        KvTablet.Guard guard = sentinel.acquireGuard();
        assertThat(sentinel.getActivePins()).isEqualTo(1);

        guard.close();
        assertThat(sentinel.getActivePins()).isZero();

        // Second close should be idempotent — pins must not go negative
        guard.close();
        assertThat(sentinel.getActivePins()).isZero();
    }

    @Test
    void testAcquireGuardOnClosedTabletThrows() {
        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        configureLifecycle(sentinel, clock);

        // Open the tablet
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            assertThat(sentinel.getLifecycle().getLazyState())
                    .isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        }

        // Drop transitions to CLOSED
        sentinel.dropKvLazy();
        assertThat(sentinel.getLifecycle().getLazyState())
                .isEqualTo(KvTabletLazyLifecycle.LazyState.CLOSED);

        // acquireGuard on a CLOSED tablet must throw
        assertThatThrownBy(sentinel::acquireGuard)
                .isInstanceOf(KvStorageException.class)
                .hasMessageContaining("closed");
    }

    @Test
    void testGaugeUpdatesOnFailedTransition() {
        TabletServerMetricGroup mg = TestingMetricGroups.TABLET_SERVER_METRICS;
        int baseLazy = mg.kvTabletLazyCount().get();
        int baseOpen = mg.kvTabletOpenCount().get();
        int baseFailed = mg.kvTabletFailedCount().get();

        ManualClock clock = new ManualClock(System.currentTimeMillis());
        KvTablet sentinel = createSentinel();
        KvTabletLazyLifecycle lifecycle = configureLifecycle(sentinel, clock);

        // After configureLazyOpen: LAZY +1
        assertThat(mg.kvTabletLazyCount().get() - baseLazy).isEqualTo(1);

        // Override openCallback to throw
        lifecycle.setOpenCallback(
                hasLocal -> {
                    throw new RuntimeException("simulated open failure");
                });

        // acquireGuard should fail and transition to FAILED
        assertThatThrownBy(sentinel::acquireGuard).isInstanceOf(RuntimeException.class);

        // LAZY back to baseline, FAILED +1, OPEN unchanged
        assertThat(mg.kvTabletLazyCount().get() - baseLazy).isEqualTo(0);
        assertThat(mg.kvTabletFailedCount().get() - baseFailed).isEqualTo(1);
        assertThat(mg.kvTabletOpenCount().get() - baseOpen).isEqualTo(0);

        // Drop from FAILED state
        sentinel.dropKvLazy();

        // All gauges back to baseline
        assertThat(mg.kvTabletLazyCount().get() - baseLazy).isEqualTo(0);
        assertThat(mg.kvTabletOpenCount().get() - baseOpen).isEqualTo(0);
        assertThat(mg.kvTabletFailedCount().get() - baseFailed).isEqualTo(0);
    }
}

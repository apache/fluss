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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    private final ExecutorService operations = Executors.newSingleThreadExecutor();
    private final ManuallyTriggeredScheduledExecutorService scheduler =
            new ManuallyTriggeredScheduledExecutorService();
    private TabletFixture fixture;
    private PeriodicSnapshotManager manager;

    @AfterEach
    void close() throws Exception {
        if (fixture != null) {
            fixture.remoteFs.release.countDown();
        }
        if (manager != null) {
            manager.close();
        }
        if (fixture != null) {
            fixture.cancelRegistry.close();
        }
        scheduler.shutdownNow();
        asyncExecutor.shutdownNow();
        operations.shutdownNow();
        assertThat(asyncExecutor.awaitTermination(10, SECONDS)).isTrue();
        assertThat(operations.awaitTermination(10, SECONDS)).isTrue();
        if (fixture != null) {
            fixture.tablet.close();
            fixture.log.close();
            fixture.allocator.close();
        }
        uploadExecutor.shutdownNow();
        assertThat(uploadExecutor.awaitTermination(10, SECONDS)).isTrue();
    }

    @Test
    void testDirectoryInitializationDoesNotBlockTabletOperations() throws Exception {
        fixture = new TabletFixture(directory);
        fixture.putAndFlush(1);
        byte[] firstValue = fixture.tablet.getRocksDBKv().get(key(1));
        byte[] firstLookup = fixture.lookup(1);
        List<AutoIncIDRange> capturedRanges = fixture.state().getAutoIncIDRanges();
        assertThat(capturedRanges).hasSize(1);
        assertThat(capturedRanges.get(0).isEmpty()).isFalse();
        manager =
                new PeriodicSnapshotManager(
                        fixture.log.getTableBucket(),
                        fixture.target,
                        () -> 10_000L,
                        asyncExecutor,
                        scheduler,
                        fixture.tablet.getGuardedExecutor());
        manager.start();

        Future<?> trigger = operations.submit(scheduler::triggerNonPeriodicScheduledTask);
        try {
            // Intercept explicit initialization before uploads can create parent directories.
            assertThat(fixture.remoteFs.mkdirsPath.get(10, SECONDS))
                    .isEqualTo(FlussPaths.remoteKvSnapshotDir(fixture.remotePath, 1));
            trigger.get(10, SECONDS);
            operations
                    .submit(
                            () -> {
                                fixture.put(2);
                                assertThat(fixture.lookup(1)).isEqualTo(firstLookup);
                                fixture.flush();
                                return null;
                            })
                    .get(10, SECONDS);
            assertThat(fixture.tablet.getFlushedLogOffset()).isEqualTo(2);
            assertThat(fixture.tablet.getRowCount()).isEqualTo(2);
            assertThat(fixture.state().getAutoIncIDRanges()).isNotEqualTo(capturedRanges);
        } finally {
            fixture.remoteFs.release.countDown();
        }
        CompletedSnapshot snapshot = fixture.committed.get(10, SECONDS);
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

    private static byte[] key(int id) {
        return ("k" + id).getBytes(UTF_8);
    }

    private class TabletFixture {
        private final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        private final CloseableRegistry cancelRegistry = new CloseableRegistry();
        private final BlockingFileSystem remoteFs = new BlockingFileSystem();
        private final CompletableFuture<CompletedSnapshot> committed = new CompletableFuture<>();
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
                    Files.createDirectories(logRoot.toPath().resolve("testDb/t1-1/log-0")).toFile();
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
            Path localPath = root.resolve("kv");
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
                            (snapshot, coordinatorEpoch, leaderEpoch) ->
                                    committed.complete(snapshot),
                            null,
                            incremental,
                            remotePath,
                            4096,
                            uploadExecutor,
                            cancelRegistry,
                            new TestingSequenceIDCounter(new AtomicLong(1)),
                            tablet::getTabletState,
                            null,
                            ignored -> {},
                            () -> 1,
                            () -> 1,
                            0,
                            null,
                            null,
                            0);
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
    }

    private static class BlockingFileSystem extends LocalFileSystem {
        private final CompletableFuture<FsPath> mkdirsPath = new CompletableFuture<>();
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public boolean mkdirs(FsPath path) throws IOException {
            mkdirsPath.complete(path);
            try {
                if (!release.await(30, SECONDS)) {
                    throw new IOException("Filesystem gate was not released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return super.mkdirs(path);
        }
    }
}

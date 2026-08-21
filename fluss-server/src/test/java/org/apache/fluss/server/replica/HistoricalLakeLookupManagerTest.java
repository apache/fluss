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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.HistoricalPartitionThrottledException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.entity.LookupDataForBucket;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.TestData.PARTITION_TABLE_ID;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HistoricalLakeLookupManager}. */
class HistoricalLakeLookupManagerTest {

    private static final long DATA_DIR_VOLUME_BYTES = MemorySize.parse("800gb").getBytes();
    private static final TableBucket HISTORICAL_BUCKET = new TableBucket(PARTITION_TABLE_ID, 1L, 0);
    private static final LakeTableLookuper.LookupMetricRecorder NO_OP_LOOKUP_METRIC_RECORDER =
            (lookupTimeNanos, lookupFileDownloaded) -> {};
    private static final Runnable NO_OP_DISK_WRITE_GUARD = () -> {};
    private static final org.apache.fluss.utils.concurrent.Scheduler NO_OP_SCHEDULER =
            new NoOpScheduler();

    @TempDir private File ioTmpDir;

    @Test
    void testHistoricalLookupThrottledWhenPermitsExhausted() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalLakeLookupManager manager = createManager(1, executor);
        assertThat(manager.numInflightRequests()).isZero();

        CompletableFuture<LookupResultForBucket> first =
                manager.lookup(
                        lookupData(HISTORICAL_BUCKET),
                        PARTITION_TABLE_INFO,
                        PARTITION_TABLE_INFO.getSchemaInfo(),
                        NO_OP_LOOKUP_METRIC_RECORDER);
        assertThat(first).isNotDone();
        assertThat(executor.numQueuedTasks()).isEqualTo(1);
        assertThat(manager.numInflightRequests()).isOne();

        TableBucket secondBucket = new TableBucket(PARTITION_TABLE_ID, 2L, 0);
        LookupResultForBucket second =
                manager.lookup(
                                lookupData(secondBucket),
                                PARTITION_TABLE_INFO,
                                PARTITION_TABLE_INFO.getSchemaInfo(),
                                NO_OP_LOOKUP_METRIC_RECORDER)
                        .get(1, TimeUnit.SECONDS);

        assertThat(second.failed()).isTrue();
        assertThat(second.getError().error()).isEqualTo(Errors.HISTORICAL_PARTITION_THROTTLED);
        assertThat(second.getError().exception())
                .isInstanceOf(HistoricalPartitionThrottledException.class);
        assertThat(executor.numQueuedTasks()).isEqualTo(1);
        assertThat(manager.numInflightRequests()).isOne();
    }

    @Test
    void testHistoricalLookupReleasesPermitOnFailure() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalLakeLookupManager manager = createManager(1, executor);

        CompletableFuture<LookupResultForBucket> first =
                manager.lookup(
                        lookupData(HISTORICAL_BUCKET),
                        PARTITION_TABLE_INFO,
                        PARTITION_TABLE_INFO.getSchemaInfo(),
                        NO_OP_LOOKUP_METRIC_RECORDER);
        executor.runNext();
        LookupResultForBucket firstResult = first.get(1, TimeUnit.SECONDS);
        assertThat(firstResult.failed()).isTrue();
        assertThat(firstResult.getError().error())
                .isNotEqualTo(Errors.HISTORICAL_PARTITION_THROTTLED);
        assertThat(manager.numInflightRequests()).isZero();

        CompletableFuture<LookupResultForBucket> second =
                manager.lookup(
                        lookupData(HISTORICAL_BUCKET),
                        PARTITION_TABLE_INFO,
                        PARTITION_TABLE_INFO.getSchemaInfo(),
                        NO_OP_LOOKUP_METRIC_RECORDER);
        assertThat(second).isNotDone();
        assertThat(executor.numQueuedTasks()).isEqualTo(1);
    }

    @Test
    void testHistoricalLookupMaxQueuedRequestsUsesExplicitConfig() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalLakeLookupManager manager = createManager(2, executor);

        CompletableFuture<LookupResultForBucket> first =
                manager.lookup(
                        lookupData(new TableBucket(PARTITION_TABLE_ID, 1L, 0)),
                        PARTITION_TABLE_INFO,
                        PARTITION_TABLE_INFO.getSchemaInfo(),
                        NO_OP_LOOKUP_METRIC_RECORDER);
        CompletableFuture<LookupResultForBucket> second =
                manager.lookup(
                        lookupData(new TableBucket(PARTITION_TABLE_ID, 2L, 0)),
                        PARTITION_TABLE_INFO,
                        PARTITION_TABLE_INFO.getSchemaInfo(),
                        NO_OP_LOOKUP_METRIC_RECORDER);
        LookupResultForBucket third =
                manager.lookup(
                                lookupData(new TableBucket(PARTITION_TABLE_ID, 3L, 0)),
                                PARTITION_TABLE_INFO,
                                PARTITION_TABLE_INFO.getSchemaInfo(),
                                NO_OP_LOOKUP_METRIC_RECORDER)
                        .get(1, TimeUnit.SECONDS);

        assertThat(first).isNotDone();
        assertThat(second).isNotDone();
        assertThat(executor.numQueuedTasks()).isEqualTo(2);
        assertThat(third.getError().error()).isEqualTo(Errors.HISTORICAL_PARTITION_THROTTLED);
    }

    @Test
    void testRejectNonPositiveHistoricalLookupMaxQueuedRequests() {
        Configuration conf = conf(0);
        ManualExecutor executor = new ManualExecutor();

        assertThatThrownBy(
                        () ->
                                new HistoricalLakeLookupManager(
                                        conf,
                                        null,
                                        executor,
                                        ioTmpDir,
                                        DATA_DIR_VOLUME_BYTES,
                                        Ticker.systemTicker(),
                                        Scheduler.disabledScheduler(),
                                        NO_OP_DISK_WRITE_GUARD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS.key());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testRejectNonPositiveHistoricalPartitionThreadPoolMaxSize(int maxThreadPoolSize) {
        Configuration conf = conf(1);
        conf.set(ConfigOptions.SERVER_HISTORICAL_PARTITION_THREAD_POOL_MAX_SIZE, maxThreadPoolSize);

        assertThatThrownBy(
                        () ->
                                new HistoricalLakeLookupManager(
                                        conf,
                                        null,
                                        null,
                                        ioTmpDir,
                                        DATA_DIR_VOLUME_BYTES,
                                        Ticker.systemTicker(),
                                        Scheduler.disabledScheduler(),
                                        NO_OP_DISK_WRITE_GUARD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        ConfigOptions.SERVER_HISTORICAL_PARTITION_THREAD_POOL_MAX_SIZE.key());
    }

    @Test
    void testCleansAndCreatesLookupCacheDirectoryOnStartup() throws Exception {
        File serverLookupDir = FlussPaths.historicalLookupRootDir(ioTmpDir);
        assertThat(serverLookupDir.mkdirs()).isTrue();
        File staleLookupFile = new File(serverLookupDir, "stale-lookup-file");
        assertThat(staleLookupFile.createNewFile()).isTrue();

        ManualExecutor executor = new ManualExecutor();
        TestingHistoricalLakeLookupManager manager =
                new TestingHistoricalLakeLookupManager(conf(1), executor);

        assertThat(staleLookupFile).exists();
        manager.startup(NO_OP_SCHEDULER);
        assertThat(staleLookupFile).doesNotExist();
        assertThat(serverLookupDir).isDirectory();
        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        assertThat(manager.createdIoTmpDirs.get(0)).startsWith(serverLookupDir.getAbsolutePath());

        File liveLookupFile = new File(serverLookupDir, "live-lookup-file");
        assertThat(liveLookupFile.createNewFile()).isTrue();
        manager.startup(NO_OP_SCHEDULER);
        assertThat(liveLookupFile).exists();
    }

    @Test
    void testCreatesLookuperWithTableKvConfig() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        TestingHistoricalLakeLookupManager manager = createTestingManager(executor);
        TableDescriptor indexedDescriptor =
                TableDescriptor.builder(PARTITION_TABLE_INFO.toTableDescriptor())
                        .kvFormat(KvFormat.INDEXED)
                        .property(
                                ConfigOptions.TABLE_KV_FORMAT_VERSION,
                                ConfigOptions.KV_FORMAT_VERSION_2)
                        .build();
        TableInfo indexedTableInfo =
                TableInfo.of(
                        PARTITION_TABLE_INFO.getTablePath(),
                        PARTITION_TABLE_INFO.getTableId(),
                        PARTITION_TABLE_INFO.getSchemaId(),
                        indexedDescriptor,
                        PARTITION_TABLE_INFO.getRemoteDataDir(),
                        PARTITION_TABLE_INFO.getCreatedTime(),
                        PARTITION_TABLE_INFO.getModifiedTime());

        lookupAndRun(manager, executor, indexedTableInfo);

        assertThat(manager.createdTableConfigs).hasSize(1);
        TableConfig createdTableConfig = manager.createdTableConfigs.get(0);
        assertThat(createdTableConfig.getKvFormat()).isEqualTo(KvFormat.INDEXED);
        assertThat(createdTableConfig.getKvFormatVersion())
                .contains(ConfigOptions.KV_FORMAT_VERSION_2);
    }

    @Test
    void testDoesNotReuseLookuperForRecreatedTable() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        TestingHistoricalLakeLookupManager manager = createTestingManager(executor);

        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        TableInfo recreatedTableInfo =
                tableInfo(PARTITION_TABLE_ID + 1, PARTITION_TABLE_INFO.getSchemaId());
        lookupAndRun(manager, executor, recreatedTableInfo);

        assertThat(manager.createdLookupers).hasSize(2);
    }

    @Test
    void testInvalidatesLookuperOnSchemaAndLifecycleChanges() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        TestingHistoricalLakeLookupManager manager = createTestingManager(executor);

        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        TestingLakeTableLookuper initialLookuper = manager.createdLookupers.get(0);

        Schema evolvedSchema =
                Schema.newBuilder()
                        .fromSchema(PARTITION_TABLE_INFO.getSchema())
                        .column("new_col", DataTypes.STRING())
                        .build();
        SchemaInfo evolvedSchemaInfo =
                new SchemaInfo(evolvedSchema, PARTITION_TABLE_INFO.getSchemaId() + 1);
        lookupAndRun(manager, executor, PARTITION_TABLE_INFO, evolvedSchemaInfo);
        assertThat(initialLookuper.closed).isTrue();
        assertThat(manager.createdLookupers).hasSize(2);

        TestingLakeTableLookuper evolvedLookuper = manager.createdLookupers.get(1);
        assertThat(evolvedLookuper.lookupContexts).hasSize(1);
        assertThat(evolvedLookuper.lookupContexts.get(0).schemaId())
                .isEqualTo((short) evolvedSchemaInfo.getSchemaId());
        assertThat(evolvedLookuper.lookupContexts.get(0).valueRowType())
                .isEqualTo(evolvedSchema.getRowType());
        manager.invalidateTableLookuper(PARTITION_TABLE_ID);
        assertThat(evolvedLookuper.closed).isTrue();

        lookupAndRun(manager, executor, PARTITION_TABLE_INFO, evolvedSchemaInfo);
        assertThat(manager.createdLookupers).hasSize(3);
    }

    @Test
    void testDoesNotReplaceLookuperForUnrelatedTableConfigChange() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        TestingHistoricalLakeLookupManager manager = createTestingManager(executor);

        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        TestingLakeTableLookuper initialLookuper = manager.createdLookupers.get(0);

        TableDescriptor changedDescriptor =
                TableDescriptor.builder(PARTITION_TABLE_INFO.toTableDescriptor())
                        .property(
                                ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS,
                                ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.defaultValue() + 1)
                        .build();
        TableInfo changedTableInfo =
                TableInfo.of(
                        PARTITION_TABLE_INFO.getTablePath(),
                        PARTITION_TABLE_INFO.getTableId(),
                        PARTITION_TABLE_INFO.getSchemaId(),
                        changedDescriptor,
                        PARTITION_TABLE_INFO.getRemoteDataDir(),
                        PARTITION_TABLE_INFO.getCreatedTime(),
                        PARTITION_TABLE_INFO.getModifiedTime());
        lookupAndRun(manager, executor, changedTableInfo);

        assertThat(manager.createdLookupers).hasSize(1);
        assertThat(initialLookuper.closed).isFalse();
    }

    @Test
    void testDynamicallyUpdatesExpirationAndExpiresIdleLookuper() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        AtomicLong tickerNanos = new AtomicLong();
        AtomicReference<FutureTask<Void>> expirationTask = new AtomicReference<>();
        Scheduler cacheScheduler =
                (cacheExecutor, command, delay, timeUnit) -> {
                    FutureTask<Void> task =
                            new FutureTask<>(
                                    () -> {
                                        cacheExecutor.execute(command);
                                        return null;
                                    });
                    expirationTask.set(task);
                    return task;
                };
        TestingHistoricalLakeLookupManager manager =
                new TestingHistoricalLakeLookupManager(
                        confWithExpiration(Duration.ofHours(1)),
                        executor,
                        tickerNanos::get,
                        cacheScheduler);
        manager.startup(NO_OP_SCHEDULER);

        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        TestingLakeTableLookuper expiredLookuper = manager.createdLookupers.get(0);

        manager.reconfigure(confWithExpiration(Duration.ofMinutes(30)));
        tickerNanos.addAndGet(Duration.ofMinutes(31).toNanos());
        assertThat(expirationTask.get()).isNotNull();
        expirationTask.get().run();

        assertThat(expiredLookuper.closed).isTrue();
        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        assertThat(manager.createdLookupers).hasSize(2);
    }

    @Test
    void testEvictsLookuperWhenCachedTableLimitIsExceeded() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        Configuration conf = conf(1);
        conf.set(ConfigOptions.SERVER_HISTORICAL_PARTITION_LOOKUP_CACHE_MAX_DISK_RATIO, 0.20);
        TestingHistoricalLakeLookupManager manager =
                new TestingHistoricalLakeLookupManager(
                        conf,
                        executor,
                        Ticker.systemTicker(),
                        Scheduler.disabledScheduler(),
                        100,
                        0);
        manager.startup(NO_OP_SCHEDULER);

        for (int i = 0; i < 11; i++) {
            lookupAndRun(
                    manager,
                    executor,
                    tableInfo(PARTITION_TABLE_ID + i, PARTITION_TABLE_INFO.getSchemaId()));
        }

        assertThat(manager.createdLookupers).hasSize(11);
        assertThat(manager.createdLookupers).filteredOn(lookuper -> lookuper.closed).hasSize(1);
        assertThat(manager.createdCacheSizes).containsOnly(2L);
        assertThat(manager.cachedTableCount()).isEqualTo(10);
        assertThat(manager.capacityEvictions().getCount()).isEqualTo(1);
    }

    @Test
    void testReconfiguresLakePropertiesAndInvalidatesLookuper() throws Exception {
        Configuration initialConf = conf(1);
        initialConf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        initialConf.setString("datalake.paimon.warehouse", "old-warehouse");
        ManualExecutor executor = new ManualExecutor();
        TestingHistoricalLakeLookupManager manager =
                new TestingHistoricalLakeLookupManager(initialConf, executor);
        manager.startup(NO_OP_SCHEDULER);

        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        TestingLakeTableLookuper initialLookuper = manager.createdLookupers.get(0);

        Configuration newConf = new Configuration(initialConf);
        newConf.setString("datalake.paimon.warehouse", "new-warehouse");
        manager.reconfigure(newConf);

        assertThat(initialLookuper.closed).isTrue();
        assertThat(manager.cachedTableCount()).isZero();
        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        assertThat(manager.createdLookupers).hasSize(2);
        assertThat(manager.createdClusterConfigs.get(1).toMap())
                .containsEntry("datalake.paimon.warehouse", "new-warehouse");
    }

    private HistoricalLakeLookupManager createManager(
            int maxQueuedHistoricalRequests, ManualExecutor executor) {
        HistoricalLakeLookupManager manager =
                new HistoricalLakeLookupManager(
                        conf(maxQueuedHistoricalRequests),
                        null,
                        executor,
                        ioTmpDir,
                        DATA_DIR_VOLUME_BYTES,
                        Ticker.systemTicker(),
                        Scheduler.disabledScheduler(),
                        NO_OP_DISK_WRITE_GUARD);
        manager.startup(NO_OP_SCHEDULER);
        return manager;
    }

    private TestingHistoricalLakeLookupManager createTestingManager(ManualExecutor executor) {
        TestingHistoricalLakeLookupManager manager =
                new TestingHistoricalLakeLookupManager(conf(1), executor);
        manager.startup(NO_OP_SCHEDULER);
        return manager;
    }

    private Configuration conf(int maxQueuedHistoricalRequests) {
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS,
                maxQueuedHistoricalRequests);
        conf.set(ConfigOptions.DATA_DIR, ioTmpDir.getAbsolutePath());
        return conf;
    }

    private Configuration confWithExpiration(Duration expiration) {
        Configuration conf = conf(1);
        conf.set(
                ConfigOptions.SERVER_HISTORICAL_PARTITION_LOOKUPER_CACHE_EXPIRE_AFTER_ACCESS,
                expiration);
        return conf;
    }

    private static LookupDataForBucket lookupData(TableBucket tableBucket) {
        return new LookupDataForBucket(
                tableBucket, Collections.singletonList(new byte[] {1}), "2024");
    }

    private static CompletableFuture<LookupResultForBucket> lookup(
            HistoricalLakeLookupManager manager, TableInfo tableInfo) {
        return manager.lookup(
                lookupData(new TableBucket(tableInfo.getTableId(), 1L, 0)),
                tableInfo,
                tableInfo.getSchemaInfo(),
                NO_OP_LOOKUP_METRIC_RECORDER);
    }

    private static TableInfo tableInfo(long tableId, int schemaId) {
        return TableInfo.of(
                PARTITION_TABLE_INFO.getTablePath(),
                tableId,
                schemaId,
                PARTITION_TABLE_INFO.toTableDescriptor(),
                PARTITION_TABLE_INFO.getRemoteDataDir(),
                PARTITION_TABLE_INFO.getCreatedTime(),
                PARTITION_TABLE_INFO.getModifiedTime());
    }

    private static void lookupAndRun(
            HistoricalLakeLookupManager manager, ManualExecutor executor, TableInfo tableInfo)
            throws Exception {
        lookupAndRun(manager, executor, tableInfo, tableInfo.getSchemaInfo());
    }

    private static void lookupAndRun(
            HistoricalLakeLookupManager manager,
            ManualExecutor executor,
            TableInfo tableInfo,
            SchemaInfo schemaInfo)
            throws Exception {
        LookupResultForBucket result = lookupResultAndRun(manager, executor, tableInfo, schemaInfo);
        assertThat(result.failed()).isFalse();
        assertThat(result.originalPartitionName()).isEqualTo("2024");
    }

    private static LookupResultForBucket lookupResultAndRun(
            HistoricalLakeLookupManager manager, ManualExecutor executor, TableInfo tableInfo)
            throws Exception {
        return lookupResultAndRun(manager, executor, tableInfo, tableInfo.getSchemaInfo());
    }

    private static LookupResultForBucket lookupResultAndRun(
            HistoricalLakeLookupManager manager,
            ManualExecutor executor,
            TableInfo tableInfo,
            SchemaInfo schemaInfo)
            throws Exception {
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 1L, 0);
        CompletableFuture<LookupResultForBucket> future =
                manager.lookup(
                        lookupData(tableBucket),
                        tableInfo,
                        schemaInfo,
                        NO_OP_LOOKUP_METRIC_RECORDER);
        executor.runNext();
        return future.get(1, TimeUnit.SECONDS);
    }

    private static final class TestingHistoricalLakeLookupManager
            extends HistoricalLakeLookupManager {
        private final List<TestingLakeTableLookuper> createdLookupers = new ArrayList<>();
        private final List<String> createdIoTmpDirs = new ArrayList<>();
        private final List<TableConfig> createdTableConfigs = new ArrayList<>();
        private final List<Long> createdCacheSizes = new ArrayList<>();
        private final List<Configuration> createdClusterConfigs = new ArrayList<>();
        private final long lookupCacheFileBytes;

        private TestingHistoricalLakeLookupManager(Configuration conf, ManualExecutor executor) {
            super(
                    conf,
                    null,
                    executor,
                    new File(conf.get(ConfigOptions.DATA_DIR)),
                    DATA_DIR_VOLUME_BYTES,
                    Ticker.systemTicker(),
                    Scheduler.disabledScheduler(),
                    NO_OP_DISK_WRITE_GUARD);
            this.lookupCacheFileBytes = 0L;
        }

        private TestingHistoricalLakeLookupManager(
                Configuration conf,
                ManualExecutor executor,
                Ticker ticker,
                Scheduler cacheScheduler) {
            super(
                    conf,
                    null,
                    executor,
                    new File(conf.get(ConfigOptions.DATA_DIR)),
                    DATA_DIR_VOLUME_BYTES,
                    ticker,
                    cacheScheduler,
                    NO_OP_DISK_WRITE_GUARD);
            this.lookupCacheFileBytes = 0L;
        }

        private TestingHistoricalLakeLookupManager(
                Configuration conf,
                ManualExecutor executor,
                Ticker ticker,
                Scheduler cacheScheduler,
                long dataDirVolumeBytes,
                long lookupCacheFileBytes) {
            super(
                    conf,
                    null,
                    executor,
                    new File(conf.get(ConfigOptions.DATA_DIR)),
                    dataDirVolumeBytes,
                    ticker,
                    cacheScheduler,
                    NO_OP_DISK_WRITE_GUARD);
            this.lookupCacheFileBytes = lookupCacheFileBytes;
        }

        @Override
        LakeTableLookuper createLakeTableLookuper(
                TablePath tablePath,
                String ioTmpDir,
                TableConfig tableConfig,
                long cacheSizeBytes,
                Configuration clusterConf) {
            TestingLakeTableLookuper lookuper =
                    new TestingLakeTableLookuper(new File(ioTmpDir), lookupCacheFileBytes);
            createdLookupers.add(lookuper);
            createdIoTmpDirs.add(ioTmpDir);
            createdTableConfigs.add(tableConfig);
            createdCacheSizes.add(cacheSizeBytes);
            createdClusterConfigs.add(clusterConf);
            return lookuper;
        }
    }

    private static final class TestingLakeTableLookuper implements LakeTableLookuper {
        private final File cacheFile;
        private final long cacheFileBytes;
        private boolean closed;
        private boolean cacheFileDownloaded;
        private final List<LookupContext> lookupContexts = new ArrayList<>();

        private TestingLakeTableLookuper(File lookupDir, long cacheFileBytes) {
            this.cacheFile = new File(lookupDir, "cache-file");
            this.cacheFileBytes = cacheFileBytes;
        }

        @Override
        public byte[] lookup(byte[] key, LookupContext context) throws Exception {
            if (closed) {
                throw new IllegalStateException("Lookuper is already closed.");
            }
            lookupContexts.add(context);
            boolean downloaded = false;
            if (!cacheFileDownloaded && cacheFileBytes > 0) {
                java.nio.file.Files.createDirectories(cacheFile.getParentFile().toPath());
                try (RandomAccessFile file = new RandomAccessFile(cacheFile, "rw")) {
                    file.setLength(cacheFileBytes);
                }
                cacheFileDownloaded = true;
                downloaded = true;
            }
            context.lookupMetricRecorder().recordLookup(1L, downloaded);
            return key;
        }

        @Override
        public void close() throws Exception {
            closed = true;
            java.nio.file.Files.deleteIfExists(cacheFile.toPath());
        }
    }

    private static final class NoOpScheduler
            implements org.apache.fluss.utils.concurrent.Scheduler {

        @Override
        public void startup() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        @Override
        public ScheduledFuture<?> schedule(
                String name, Runnable task, long delayMs, long periodMs) {
            return null;
        }
    }

    private static final class ManualExecutor extends AbstractExecutorService {
        private final BlockingQueue<Runnable> tasks = new LinkedBlockingQueue<>();
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            List<Runnable> remainingTasks = new ArrayList<>();
            tasks.drainTo(remainingTasks);
            return remainingTasks;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown && tasks.isEmpty();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return isTerminated();
        }

        @Override
        public void execute(Runnable command) {
            if (shutdown) {
                throw new RejectedExecutionException();
            }
            tasks.add(command);
        }

        private void runNext() throws Exception {
            Runnable task = tasks.poll(1, TimeUnit.SECONDS);
            assertThat(task).isNotNull();
            task.run();
        }

        private int numQueuedTasks() {
            return tasks.size();
        }
    }
}

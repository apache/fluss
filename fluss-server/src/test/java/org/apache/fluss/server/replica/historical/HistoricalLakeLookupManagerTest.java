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

package org.apache.fluss.server.replica.historical;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.entity.LookupDataForBucket;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FlussPaths;

import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.TestData.PARTITION_TABLE_ID;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_INFO;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HistoricalLakeLookupManager}. */
class HistoricalLakeLookupManagerTest {

    private static final long DATA_DIR_VOLUME_BYTES = MemorySize.parse("800gb").getBytes();
    private static final LakeTableLookuper.LookupMetricRecorder NO_OP_LOOKUP_METRIC_RECORDER =
            (lookupTimeNanos, lookupFileDownloaded) -> {};
    private static final Runnable NO_OP_DISK_WRITE_GUARD = () -> {};
    private static final org.apache.fluss.utils.concurrent.Scheduler NO_OP_SCHEDULER =
            new NoOpScheduler();

    @TempDir private File ioTmpDir;

    @Test
    void testCleansAndCreatesLookupCacheDirectoryOnStartup() throws Exception {
        File serverLookupDir = FlussPaths.historicalLookupRootDir(ioTmpDir);
        assertThat(serverLookupDir.mkdirs()).isTrue();
        File staleLookupFile = new File(serverLookupDir, "stale-lookup-file");
        assertThat(staleLookupFile.createNewFile()).isTrue();

        TestingHistoricalLakeLookupManager manager = new TestingHistoricalLakeLookupManager(conf());

        assertThat(staleLookupFile).exists();
        manager.startup(NO_OP_SCHEDULER);
        assertThat(staleLookupFile).doesNotExist();
        assertThat(serverLookupDir).isDirectory();
        lookup(manager, PARTITION_TABLE_INFO);
        assertThat(manager.createdIoTmpDirs.get(0)).startsWith(serverLookupDir.getAbsolutePath());

        File liveLookupFile = new File(serverLookupDir, "live-lookup-file");
        assertThat(liveLookupFile.createNewFile()).isTrue();
        manager.startup(NO_OP_SCHEDULER);
        assertThat(liveLookupFile).exists();
    }

    @Test
    void testCreatesLookuperWithTableKvConfig() throws Exception {
        TestingHistoricalLakeLookupManager manager = createTestingManager();
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

        lookup(manager, indexedTableInfo);

        assertThat(manager.createdTableConfigs).hasSize(1);
        TableConfig createdTableConfig = manager.createdTableConfigs.get(0);
        assertThat(createdTableConfig.getKvFormat()).isEqualTo(KvFormat.INDEXED);
        assertThat(createdTableConfig.getKvFormatVersion())
                .contains(ConfigOptions.KV_FORMAT_VERSION_2);
    }

    @Test
    void testCreatesLookuperWithMappedLakeTablePath() throws Exception {
        TestingHistoricalLakeLookupManager manager = createTestingManager();
        TablePath lakeTablePath = TablePath.of("lake_db", "lake_table");
        TableDescriptor mappedDescriptor =
                TableDescriptor.builder(PARTITION_TABLE_INFO.toTableDescriptor())
                        .property(
                                ConfigOptions.TABLE_DATALAKE_DATABASE_NAME,
                                lakeTablePath.getDatabaseName())
                        .property(
                                ConfigOptions.TABLE_DATALAKE_TABLE_NAME,
                                lakeTablePath.getTableName())
                        .build();
        TableInfo mappedTableInfo =
                TableInfo.of(
                        PARTITION_TABLE_INFO.getTablePath(),
                        PARTITION_TABLE_INFO.getTableId(),
                        PARTITION_TABLE_INFO.getSchemaId(),
                        mappedDescriptor,
                        PARTITION_TABLE_INFO.getRemoteDataDir(),
                        PARTITION_TABLE_INFO.getCreatedTime(),
                        PARTITION_TABLE_INFO.getModifiedTime());

        lookup(manager, mappedTableInfo);

        assertThat(manager.createdTablePaths).containsExactly(lakeTablePath);
    }

    @Test
    void testDoesNotReuseLookuperForRecreatedTable() throws Exception {
        TestingHistoricalLakeLookupManager manager = createTestingManager();

        lookup(manager, PARTITION_TABLE_INFO);
        TableInfo recreatedTableInfo =
                tableInfo(PARTITION_TABLE_ID + 1, PARTITION_TABLE_INFO.getSchemaId());
        lookup(manager, recreatedTableInfo);

        assertThat(manager.createdLookupers).hasSize(2);
    }

    @Test
    void testInvalidatesLookuperOnSchemaAndLifecycleChanges() throws Exception {
        TestingHistoricalLakeLookupManager manager = createTestingManager();

        lookup(manager, PARTITION_TABLE_INFO);
        TestingLakeTableLookuper initialLookuper = manager.createdLookupers.get(0);

        Schema evolvedSchema =
                Schema.newBuilder()
                        .fromSchema(PARTITION_TABLE_INFO.getSchema())
                        .column("new_col", DataTypes.STRING())
                        .build();
        SchemaInfo evolvedSchemaInfo =
                new SchemaInfo(evolvedSchema, PARTITION_TABLE_INFO.getSchemaId() + 1);
        lookup(manager, PARTITION_TABLE_INFO, evolvedSchemaInfo);
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

        lookup(manager, PARTITION_TABLE_INFO, evolvedSchemaInfo);
        assertThat(manager.createdLookupers).hasSize(3);
    }

    @Test
    void testRefreshesLookuperWhenLakeSnapshotChanges() throws Exception {
        TestingHistoricalLakeLookupManager manager = createTestingManager();

        lookup(manager, PARTITION_TABLE_INFO);
        TestingLakeTableLookuper initialLookuper = manager.createdLookupers.get(0);

        manager.requireLakeSnapshot(PARTITION_TABLE_ID, 10L);
        lookup(manager, PARTITION_TABLE_INFO);
        assertThat(initialLookuper.closed).isTrue();
        assertThat(manager.createdLookupers).hasSize(2);

        TestingLakeTableLookuper snapshotTenLookuper = manager.createdLookupers.get(1);
        // Snapshot IDs are opaque; a numerically smaller ID may identify a newer snapshot.
        manager.requireLakeSnapshot(PARTITION_TABLE_ID, 9L);
        lookup(manager, PARTITION_TABLE_INFO);
        assertThat(snapshotTenLookuper.closed).isTrue();
        assertThat(manager.createdLookupers).hasSize(3);

        TestingLakeTableLookuper snapshotNineLookuper = manager.createdLookupers.get(2);
        manager.requireLakeSnapshot(PARTITION_TABLE_ID, 9L);
        lookup(manager, PARTITION_TABLE_INFO);
        assertThat(snapshotNineLookuper.closed).isFalse();
        assertThat(manager.createdLookupers).hasSize(3);
    }

    @Test
    void testDoesNotReplaceLookuperForUnrelatedTableConfigChange() throws Exception {
        TestingHistoricalLakeLookupManager manager = createTestingManager();

        lookup(manager, PARTITION_TABLE_INFO);
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
        lookup(manager, changedTableInfo);

        assertThat(manager.createdLookupers).hasSize(1);
        assertThat(initialLookuper.closed).isFalse();
    }

    @Test
    void testDynamicallyUpdatesExpirationAndExpiresIdleLookuper() throws Exception {
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
                        confWithExpiration(Duration.ofHours(1)), tickerNanos::get, cacheScheduler);
        manager.startup(NO_OP_SCHEDULER);

        lookup(manager, PARTITION_TABLE_INFO);
        TestingLakeTableLookuper expiredLookuper = manager.createdLookupers.get(0);

        manager.reconfigure(confWithExpiration(Duration.ofMinutes(30)));
        tickerNanos.addAndGet(Duration.ofMinutes(31).toNanos());
        assertThat(expirationTask.get()).isNotNull();
        expirationTask.get().run();

        assertThat(expiredLookuper.closed).isTrue();
        lookup(manager, PARTITION_TABLE_INFO);
        assertThat(manager.createdLookupers).hasSize(2);
    }

    @Test
    void testEvictsLookuperWhenCachedTableLimitIsExceeded() throws Exception {
        Configuration conf = conf();
        conf.set(ConfigOptions.SERVER_HISTORICAL_PARTITION_LOOKUP_CACHE_MAX_DISK_RATIO, 0.20);
        TestingHistoricalLakeLookupManager manager =
                new TestingHistoricalLakeLookupManager(
                        conf, Ticker.systemTicker(), Scheduler.disabledScheduler(), 100, 0);
        manager.startup(NO_OP_SCHEDULER);

        for (int i = 0; i < 11; i++) {
            lookup(manager, tableInfo(PARTITION_TABLE_ID + i, PARTITION_TABLE_INFO.getSchemaId()));
        }

        assertThat(manager.createdLookupers).hasSize(11);
        assertThat(manager.createdLookupers).filteredOn(lookuper -> lookuper.closed).hasSize(1);
        assertThat(manager.createdCacheSizes).containsOnly(2L);
        assertThat(manager.cachedTableCount()).isEqualTo(10);
        assertThat(manager.capacityEvictions().getCount()).isEqualTo(1);
    }

    @Test
    void testReconfiguresLakePropertiesAndInvalidatesLookuper() throws Exception {
        Configuration initialConf = conf();
        initialConf.set(ConfigOptions.DATALAKE_FORMAT, DataLakeFormat.PAIMON);
        initialConf.setString("datalake.paimon.warehouse", "old-warehouse");
        TestingHistoricalLakeLookupManager manager =
                new TestingHistoricalLakeLookupManager(initialConf);
        manager.startup(NO_OP_SCHEDULER);

        lookup(manager, PARTITION_TABLE_INFO);
        TestingLakeTableLookuper initialLookuper = manager.createdLookupers.get(0);

        Configuration newConf = new Configuration(initialConf);
        newConf.setString("datalake.paimon.warehouse", "new-warehouse");
        manager.reconfigure(newConf);

        assertThat(initialLookuper.closed).isTrue();
        assertThat(manager.cachedTableCount()).isZero();
        lookup(manager, PARTITION_TABLE_INFO);
        assertThat(manager.createdLookupers).hasSize(2);
        assertThat(manager.createdClusterConfigs.get(1).toMap())
                .containsEntry("datalake.paimon.warehouse", "new-warehouse");
    }

    private TestingHistoricalLakeLookupManager createTestingManager() {
        TestingHistoricalLakeLookupManager manager = new TestingHistoricalLakeLookupManager(conf());
        manager.startup(NO_OP_SCHEDULER);
        return manager;
    }

    private Configuration conf() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.DATA_DIR, ioTmpDir.getAbsolutePath());
        return conf;
    }

    private Configuration confWithExpiration(Duration expiration) {
        Configuration conf = conf();
        conf.set(
                ConfigOptions.SERVER_HISTORICAL_PARTITION_LOOKUPER_CACHE_EXPIRE_AFTER_ACCESS,
                expiration);
        return conf;
    }

    private static LookupDataForBucket lookupData(TableBucket tableBucket) {
        return new LookupDataForBucket(
                tableBucket, Collections.singletonList(new byte[] {1}), "2024");
    }

    private static void lookup(HistoricalLakeLookupManager manager, TableInfo tableInfo)
            throws Exception {
        lookup(manager, tableInfo, tableInfo.getSchemaInfo());
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

    private static void lookup(
            HistoricalLakeLookupManager manager, TableInfo tableInfo, SchemaInfo schemaInfo)
            throws Exception {
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 1L, 0);
        LookupDataForBucket lookupData = lookupData(tableBucket);
        manager.lookup(
                lookupData,
                tableInfo,
                schemaInfo,
                ResolvedPartitionSpec.fromPartitionName(
                        tableInfo.getPartitionKeys(), lookupData.originalPartitionName()),
                NO_OP_LOOKUP_METRIC_RECORDER);
    }

    private static final class TestingHistoricalLakeLookupManager
            extends HistoricalLakeLookupManager {
        private final List<TestingLakeTableLookuper> createdLookupers = new ArrayList<>();
        private final List<TablePath> createdTablePaths = new ArrayList<>();
        private final List<String> createdIoTmpDirs = new ArrayList<>();
        private final List<TableConfig> createdTableConfigs = new ArrayList<>();
        private final List<Long> createdCacheSizes = new ArrayList<>();
        private final List<Configuration> createdClusterConfigs = new ArrayList<>();
        private final long lookupCacheFileBytes;

        private TestingHistoricalLakeLookupManager(Configuration conf) {
            super(
                    conf,
                    null,
                    new File(conf.get(ConfigOptions.DATA_DIR)),
                    DATA_DIR_VOLUME_BYTES,
                    Ticker.systemTicker(),
                    Scheduler.disabledScheduler(),
                    NO_OP_DISK_WRITE_GUARD);
            this.lookupCacheFileBytes = 0L;
        }

        private TestingHistoricalLakeLookupManager(
                Configuration conf, Ticker ticker, Scheduler cacheScheduler) {
            super(
                    conf,
                    null,
                    new File(conf.get(ConfigOptions.DATA_DIR)),
                    DATA_DIR_VOLUME_BYTES,
                    ticker,
                    cacheScheduler,
                    NO_OP_DISK_WRITE_GUARD);
            this.lookupCacheFileBytes = 0L;
        }

        private TestingHistoricalLakeLookupManager(
                Configuration conf,
                Ticker ticker,
                Scheduler cacheScheduler,
                long dataDirVolumeBytes,
                long lookupCacheFileBytes) {
            super(
                    conf,
                    null,
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
            createdTablePaths.add(tablePath);
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
}

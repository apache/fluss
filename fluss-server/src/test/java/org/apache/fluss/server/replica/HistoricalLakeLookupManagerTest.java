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
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.HistoricalLookupThrottledException;
import org.apache.fluss.lake.lakestorage.LakeTableLookuper;
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

import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.TestData.PARTITION_TABLE_ID;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HistoricalLakeLookupManager}. */
class HistoricalLakeLookupManagerTest {

    private static final int SERVER_ID = 1;
    private static final TableBucket HISTORICAL_BUCKET = new TableBucket(PARTITION_TABLE_ID, 1L, 0);

    @TempDir private File ioTmpDir;

    @Test
    void testHistoricalLookupThrottledWhenPermitsExhausted() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalLakeLookupManager manager = createManager(1, executor);

        CompletableFuture<LookupResultForBucket> first =
                manager.lookup(
                        lookupData(HISTORICAL_BUCKET),
                        PARTITION_TABLE_INFO,
                        PARTITION_TABLE_INFO.getSchemaInfo());
        assertThat(first).isNotDone();
        assertThat(executor.numQueuedTasks()).isEqualTo(1);

        TableBucket secondBucket = new TableBucket(PARTITION_TABLE_ID, 2L, 0);
        LookupResultForBucket second =
                manager.lookup(
                                lookupData(secondBucket),
                                PARTITION_TABLE_INFO,
                                PARTITION_TABLE_INFO.getSchemaInfo())
                        .get(1, TimeUnit.SECONDS);

        assertThat(second.failed()).isTrue();
        assertThat(second.getError().error()).isEqualTo(Errors.HISTORICAL_LOOKUP_THROTTLED);
        assertThat(second.getError().exception())
                .isInstanceOf(HistoricalLookupThrottledException.class);
        assertThat(executor.numQueuedTasks()).isEqualTo(1);
    }

    @Test
    void testHistoricalLookupReleasesPermitOnFailure() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        HistoricalLakeLookupManager manager = createManager(1, executor);

        CompletableFuture<LookupResultForBucket> first =
                manager.lookup(
                        lookupData(HISTORICAL_BUCKET),
                        PARTITION_TABLE_INFO,
                        PARTITION_TABLE_INFO.getSchemaInfo());
        executor.runNext();
        LookupResultForBucket firstResult = first.get(1, TimeUnit.SECONDS);
        assertThat(firstResult.failed()).isTrue();
        assertThat(firstResult.getError().error()).isNotEqualTo(Errors.HISTORICAL_LOOKUP_THROTTLED);

        CompletableFuture<LookupResultForBucket> second =
                manager.lookup(
                        lookupData(HISTORICAL_BUCKET),
                        PARTITION_TABLE_INFO,
                        PARTITION_TABLE_INFO.getSchemaInfo());
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
                        PARTITION_TABLE_INFO.getSchemaInfo());
        CompletableFuture<LookupResultForBucket> second =
                manager.lookup(
                        lookupData(new TableBucket(PARTITION_TABLE_ID, 2L, 0)),
                        PARTITION_TABLE_INFO,
                        PARTITION_TABLE_INFO.getSchemaInfo());
        LookupResultForBucket third =
                manager.lookup(
                                lookupData(new TableBucket(PARTITION_TABLE_ID, 3L, 0)),
                                PARTITION_TABLE_INFO,
                                PARTITION_TABLE_INFO.getSchemaInfo())
                        .get(1, TimeUnit.SECONDS);

        assertThat(first).isNotDone();
        assertThat(second).isNotDone();
        assertThat(executor.numQueuedTasks()).isEqualTo(2);
        assertThat(third.getError().error()).isEqualTo(Errors.HISTORICAL_LOOKUP_THROTTLED);
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
                                        SERVER_ID,
                                        Ticker.systemTicker(),
                                        Scheduler.disabledScheduler()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS.key());
    }

    @Test
    void testLazilyCleansPaimonLookupTempDirectory() throws Exception {
        File serverLookupDir =
                new File(new File(ioTmpDir, "paimon-lookup"), String.valueOf(SERVER_ID));
        assertThat(serverLookupDir.mkdirs()).isTrue();
        File staleLookupFile = new File(serverLookupDir, "stale-lookup-file");
        assertThat(staleLookupFile.createNewFile()).isTrue();

        ManualExecutor executor = new ManualExecutor();
        TestingHistoricalLakeLookupManager manager = createTestingManager(executor);

        assertThat(staleLookupFile).exists();
        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        assertThat(staleLookupFile).doesNotExist();
        assertThat(serverLookupDir).isDirectory();
        assertThat(manager.createdIoTmpDirs).containsExactly(serverLookupDir.getAbsolutePath());
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
    void testExpiresIdleLookuperWithoutAnotherLookup() throws Exception {
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
                        conf(1), executor, tickerNanos::get, cacheScheduler);

        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        TestingLakeTableLookuper expiredLookuper = manager.createdLookupers.get(0);

        tickerNanos.addAndGet(Duration.ofHours(4).toNanos());
        assertThat(expirationTask.get()).isNotNull();
        expirationTask.get().run();

        assertThat(expiredLookuper.closed).isTrue();
        lookupAndRun(manager, executor, PARTITION_TABLE_INFO);
        assertThat(manager.createdLookupers).hasSize(2);
    }

    @Test
    void testLimitsCachedLookupersToTen() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        TestingHistoricalLakeLookupManager manager = createTestingManager(executor);

        for (int i = 0; i < 11; i++) {
            lookupAndRun(
                    manager,
                    executor,
                    tableInfo(PARTITION_TABLE_ID + i, PARTITION_TABLE_INFO.getSchemaId()));
        }

        assertThat(manager.createdLookupers).hasSize(11);
        assertThat(manager.createdLookupers).filteredOn(lookuper -> lookuper.closed).hasSize(1);
    }

    private HistoricalLakeLookupManager createManager(
            int maxQueuedHistoricalRequests, ManualExecutor executor) {
        return new HistoricalLakeLookupManager(
                conf(maxQueuedHistoricalRequests),
                null,
                executor,
                SERVER_ID,
                Ticker.systemTicker(),
                Scheduler.disabledScheduler());
    }

    private TestingHistoricalLakeLookupManager createTestingManager(ManualExecutor executor) {
        return new TestingHistoricalLakeLookupManager(conf(1), executor);
    }

    private Configuration conf(int maxQueuedHistoricalRequests) {
        Configuration conf = new Configuration();
        conf.set(
                ConfigOptions.NETTY_SERVER_MAX_QUEUED_HISTORICAL_REQUESTS,
                maxQueuedHistoricalRequests);
        conf.set(ConfigOptions.IO_TMP_DIR, ioTmpDir.getAbsolutePath());
        return conf;
    }

    private static LookupDataForBucket lookupData(TableBucket tableBucket) {
        return new LookupDataForBucket(
                tableBucket, Collections.singletonList(new byte[] {1}), "2024");
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
        TableBucket tableBucket = new TableBucket(tableInfo.getTableId(), 1L, 0);
        CompletableFuture<LookupResultForBucket> future =
                manager.lookup(lookupData(tableBucket), tableInfo, schemaInfo);
        executor.runNext();
        assertThat(future.get(1, TimeUnit.SECONDS).failed()).isFalse();
    }

    private static final class TestingHistoricalLakeLookupManager
            extends HistoricalLakeLookupManager {
        private final List<TestingLakeTableLookuper> createdLookupers = new ArrayList<>();
        private final List<String> createdIoTmpDirs = new ArrayList<>();
        private final List<TableConfig> createdTableConfigs = new ArrayList<>();

        private TestingHistoricalLakeLookupManager(Configuration conf, ManualExecutor executor) {
            super(
                    conf,
                    null,
                    executor,
                    SERVER_ID,
                    Ticker.systemTicker(),
                    Scheduler.disabledScheduler());
        }

        private TestingHistoricalLakeLookupManager(
                Configuration conf,
                ManualExecutor executor,
                Ticker ticker,
                Scheduler cacheScheduler) {
            super(conf, null, executor, SERVER_ID, ticker, cacheScheduler);
        }

        @Override
        LakeTableLookuper createLakeTableLookuper(
                TablePath tablePath, String ioTmpDir, TableConfig tableConfig) {
            TestingLakeTableLookuper lookuper = new TestingLakeTableLookuper();
            createdLookupers.add(lookuper);
            createdIoTmpDirs.add(ioTmpDir);
            createdTableConfigs.add(tableConfig);
            return lookuper;
        }
    }

    private static final class TestingLakeTableLookuper implements LakeTableLookuper {
        private boolean closed;
        private final List<LookupContext> lookupContexts = new ArrayList<>();

        @Override
        public byte[] lookup(byte[] key, LookupContext context) {
            if (closed) {
                throw new IllegalStateException("Lookuper is already closed.");
            }
            lookupContexts.add(context);
            return key;
        }

        @Override
        public void close() {
            closed = true;
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

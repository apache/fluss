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

package org.apache.fluss.server.kv.scan;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.TooManyScannersException;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordTestUtils;
import org.apache.fluss.record.TestData;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.TestingSequenceGeneratorFactory;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ScannerManager}. */
class ScannerManagerTest {

    private static final short SCHEMA_ID = 1;

    @TempDir File tempLogDir;
    @TempDir File tmpKvDir;

    private final Configuration conf = new Configuration();
    private final KvRecordTestUtils.KvRecordBatchFactory kvRecordBatchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
    private final KvRecordTestUtils.KvRecordFactory kvRecordFactory =
            KvRecordTestUtils.KvRecordFactory.of(TestData.DATA1_ROW_TYPE);

    private ManualClock clock;
    private FlussScheduler scheduler;
    private LogTablet logTablet;
    private KvTablet kvTablet;

    @BeforeEach
    void setUp() throws Exception {
        clock = new ManualClock(0);
        scheduler = new FlussScheduler(1);
        scheduler.startup();

        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(TablePath.of("testDb", "t1"));
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(DATA1_SCHEMA_PK, SCHEMA_ID));

        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir,
                        physicalTablePath.getDatabaseName(),
                        0L,
                        physicalTablePath.getTableName());
        logTablet =
                LogTablet.create(
                        tempLogDir,
                        physicalTablePath,
                        logTabletDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        0,
                        new FlussScheduler(1),
                        LogFormat.ARROW,
                        1,
                        true,
                        org.apache.fluss.utils.clock.SystemClock.getInstance(),
                        true);

        TableBucket tableBucket = logTablet.getTableBucket();
        TableConfig tableConf = new TableConfig(new Configuration());
        RowMerger rowMerger = RowMerger.create(tableConf, KvFormat.COMPACTED, schemaGetter);
        AutoIncrementManager autoIncrementManager =
                new AutoIncrementManager(
                        schemaGetter,
                        physicalTablePath.getTablePath(),
                        tableConf,
                        new TestingSequenceGeneratorFactory());

        kvTablet =
                KvTablet.create(
                        physicalTablePath,
                        tableBucket,
                        logTablet,
                        tmpKvDir,
                        conf,
                        TestingMetricGroups.TABLET_SERVER_METRICS,
                        new RootAllocator(Long.MAX_VALUE),
                        new TestingMemorySegmentPool(10 * 1024),
                        KvFormat.COMPACTED,
                        rowMerger,
                        DEFAULT_COMPRESSION,
                        schemaGetter,
                        tableConf.getChangelogImage(),
                        KvManager.getDefaultRateLimiter(),
                        autoIncrementManager);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (kvTablet != null) {
            kvTablet.close();
        }
        if (logTablet != null) {
            logTablet.close();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    private ScannerManager createManager() {
        Configuration c = new Configuration();
        c.set(ConfigOptions.KV_SCANNER_TTL, Duration.ofHours(1));
        c.set(ConfigOptions.KV_SCANNER_EXPIRATION_INTERVAL, Duration.ofHours(1));
        c.set(ConfigOptions.KV_SCANNER_MAX_PER_BUCKET, 8);
        c.set(ConfigOptions.KV_SCANNER_MAX_PER_SERVER, 200);
        return new ScannerManager(c, scheduler, clock);
    }

    private ScannerManager createManager(int maxPerBucket, int maxPerServer) {
        Configuration c = new Configuration();
        c.set(ConfigOptions.KV_SCANNER_TTL, Duration.ofHours(1));
        c.set(ConfigOptions.KV_SCANNER_EXPIRATION_INTERVAL, Duration.ofHours(1));
        c.set(ConfigOptions.KV_SCANNER_MAX_PER_BUCKET, maxPerBucket);
        c.set(ConfigOptions.KV_SCANNER_MAX_PER_SERVER, maxPerServer);
        return new ScannerManager(c, scheduler, clock);
    }

    private ScannerManager createManagerWithShortTtl(long ttlMs, long expirationIntervalMs) {
        Configuration c = new Configuration();
        c.set(ConfigOptions.KV_SCANNER_TTL, Duration.ofMillis(ttlMs));
        c.set(
                ConfigOptions.KV_SCANNER_EXPIRATION_INTERVAL,
                Duration.ofMillis(expirationIntervalMs));
        c.set(ConfigOptions.KV_SCANNER_MAX_PER_BUCKET, 8);
        c.set(ConfigOptions.KV_SCANNER_MAX_PER_SERVER, 200);
        return new ScannerManager(c, scheduler, clock);
    }

    /** Opens a scanner directly via KvTablet and registers it; mirrors Replica#openScan. */
    private ScannerContext openAndRegister(ScannerManager manager) throws Exception {
        ScannerContext ctx =
                kvTablet.openScan(java.util.UUID.randomUUID().toString(), -1L, clock.milliseconds())
                        .getContext();
        if (ctx == null) {
            return null;
        }
        try {
            manager.register(ctx);
        } catch (RuntimeException e) {
            ctx.close();
            throw e;
        }
        return ctx;
    }

    private void putAndFlush(int count) throws Exception {
        List<KvRecord> rows = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            rows.add(
                    kvRecordFactory.ofRecord(
                            String.valueOf(i).getBytes(), new Object[] {i, "v" + i}));
        }
        kvTablet.putAsLeader(kvRecordBatchFactory.ofRecords(rows), null);
        kvTablet.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
    }

    @Test
    void testCreateScanner_emptyBucket_returnsNull() throws Exception {
        try (ScannerManager manager = createManager()) {
            ScannerContext context = openAndRegister(manager);
            assertThat(context).isNull();
            assertThat(manager.activeScannerCount()).isEqualTo(0);
        }
    }

    @Test
    void testCreateAndRemoveScanner() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            TableBucket tableBucket = kvTablet.getTableBucket();

            ScannerContext context = openAndRegister(manager);
            assertThat(context).isNotNull();
            assertThat(manager.activeScannerCount()).isEqualTo(1);
            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(1);

            manager.removeScanner(context);
            assertThat(manager.activeScannerCount()).isEqualTo(0);
            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(0);
        }
    }

    @Test
    void testGetScanner_doesNotRefreshLastAccessTime() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            ScannerContext context = openAndRegister(manager);
            assertThat(context).isNotNull();
            byte[] scannerId = context.getScannerId();

            clock.advanceTime(5000, TimeUnit.MILLISECONDS);
            ScannerContext fetched = manager.getScanner(scannerId);
            assertThat(fetched).isSameAs(context);

            assertThat(context.isExpired(1000L, clock.milliseconds())).isTrue();

            manager.removeScanner(context);
        }
    }

    @Test
    void testMarkAccessed_refreshesLastAccessTime() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            ScannerContext context = openAndRegister(manager);
            assertThat(context).isNotNull();

            clock.advanceTime(5000, TimeUnit.MILLISECONDS);
            manager.markAccessed(context);

            assertThat(context.isExpired(3_600_000L, clock.milliseconds())).isFalse();

            manager.removeScanner(context);
        }
    }

    @Test
    void testTryAcquireForUse_serialisesConcurrentClaims() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            ScannerContext context = openAndRegister(manager);
            assertThat(context).isNotNull();

            assertThat(context.tryAcquireForUse()).isTrue();
            assertThat(context.tryAcquireForUse()).isFalse();

            context.releaseAfterUse();

            assertThat(context.tryAcquireForUse()).isTrue();
            context.releaseAfterUse();

            manager.removeScanner(context);
        }
    }

    @Test
    void testTryAcquireForUse_exactlyOneWinnerUnderContention() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            ScannerContext context = openAndRegister(manager);
            assertThat(context).isNotNull();

            int threadCount = 16;
            java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.atomic.AtomicInteger winners =
                    new java.util.concurrent.atomic.AtomicInteger(0);
            java.util.concurrent.ExecutorService pool =
                    java.util.concurrent.Executors.newFixedThreadPool(threadCount);
            try {
                for (int i = 0; i < threadCount; i++) {
                    pool.submit(
                            () -> {
                                try {
                                    start.await();
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                                if (context.tryAcquireForUse()) {
                                    winners.incrementAndGet();
                                }
                            });
                }
                start.countDown();
                pool.shutdown();
                assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            } finally {
                pool.shutdownNow();
            }

            assertThat(winners.get()).isEqualTo(1);
            context.releaseAfterUse();
            manager.removeScanner(context);
        }
    }

    @Test
    void testCheckIteratorStatus_healthyIteratorIsNoOp() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            ScannerContext context = openAndRegister(manager);
            assertThat(context).isNotNull();

            while (context.isValid()) {
                context.advance();
            }

            context.checkIteratorStatus();

            manager.removeScanner(context);
        }
    }

    @Test
    void testTtlEviction() throws Exception {
        putAndFlush(3);
        ScannerManager manager = createManagerWithShortTtl(200, 200);
        try {
            ScannerContext context = openAndRegister(manager);
            assertThat(context).isNotNull();
            byte[] scannerId = context.getScannerId();

            clock.advanceTime(500, TimeUnit.MILLISECONDS);

            retry(
                    Duration.ofSeconds(10),
                    () -> assertThat(manager.activeScannerCount()).isEqualTo(0));
            assertThat(manager.getScanner(scannerId)).isNull();
            assertThat(manager.isRecentlyExpired(scannerId)).isTrue();
        } finally {
            manager.close();
        }
    }

    @Test
    void testPerBucketLimit() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager(2, 200)) {
            TableBucket tableBucket = kvTablet.getTableBucket();

            ScannerContext ctx1 = openAndRegister(manager);
            ScannerContext ctx2 = openAndRegister(manager);
            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(2);

            assertThatThrownBy(() -> openAndRegister(manager))
                    .isInstanceOf(TooManyScannersException.class);

            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(2);

            manager.removeScanner(ctx1);
            manager.removeScanner(ctx2);
        }
    }

    @Test
    void testPerServerLimit() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager(8, 2)) {
            ScannerContext ctx1 = openAndRegister(manager);
            ScannerContext ctx2 = openAndRegister(manager);
            assertThat(manager.activeScannerCount()).isEqualTo(2);

            assertThatThrownBy(() -> openAndRegister(manager))
                    .isInstanceOf(TooManyScannersException.class);

            assertThat(manager.activeScannerCount()).isEqualTo(2);

            manager.removeScanner(ctx1);
            manager.removeScanner(ctx2);
        }
    }

    @Test
    void testCloseScannersForBucket() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            TableBucket tableBucket = kvTablet.getTableBucket();

            openAndRegister(manager);
            openAndRegister(manager);
            assertThat(manager.activeScannerCount()).isEqualTo(2);

            manager.closeScannersForBucket(tableBucket);

            assertThat(manager.activeScannerCount()).isEqualTo(0);
            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(0);
        }
    }

    @Test
    void testCloseScannersForBucket_marksRecentlyExpired() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            TableBucket tableBucket = kvTablet.getTableBucket();

            ScannerContext ctx = openAndRegister(manager);
            assertThat(ctx).isNotNull();
            byte[] scannerId = ctx.getScannerId();

            manager.closeScannersForBucket(tableBucket);

            assertThat(manager.getScanner(scannerId)).isNull();
            assertThat(manager.isRecentlyExpired(scannerId)).isTrue();
        }
    }

    @Test
    void testShutdown_closesAllScanners() throws Exception {
        putAndFlush(3);
        ScannerManager manager = createManager();
        openAndRegister(manager);
        openAndRegister(manager);
        assertThat(manager.activeScannerCount()).isEqualTo(2);

        manager.close();

        assertThat(manager.activeScannerCount()).isEqualTo(0);
    }

    @Test
    void testTryAcquireForUse_rejectsAfterClose() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            ScannerContext ctx = openAndRegister(manager);
            assertThat(ctx).isNotNull();

            manager.removeScanner(ctx);

            assertThat(ctx.tryAcquireForUse()).isFalse();
        }
    }

    /** close() force-clears inUse and completes immediately even if inUse=true. */
    @Test
    void testClose_notWaitForInUse() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            ScannerContext ctx = openAndRegister(manager);
            assertThat(ctx).isNotNull();

            assertThat(ctx.tryAcquireForUse()).isTrue();

            // close() should complete immediately despite inUse=true
            ctx.close();

            // after close, tryAcquireForUse must be rejected
            assertThat(ctx.tryAcquireForUse()).isFalse();

            manager.removeScanner(ctx);
        }
    }
}

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

    // -------------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------------

    /** Creates a {@link ScannerManager} with a long TTL so the reaper never fires during tests. */
    private ScannerManager createManager() {
        Configuration c = new Configuration();
        c.set(ConfigOptions.SERVER_SCANNER_TTL, Duration.ofHours(1));
        c.set(ConfigOptions.SERVER_SCANNER_EXPIRATION_INTERVAL, Duration.ofHours(1));
        c.set(ConfigOptions.SERVER_SCANNER_MAX_PER_BUCKET, 8);
        c.set(ConfigOptions.SERVER_SCANNER_MAX_PER_SERVER, 200);
        return new ScannerManager(c, scheduler, clock);
    }

    /** Creates a {@link ScannerManager} with configurable limits and a long reaper interval. */
    private ScannerManager createManager(int maxPerBucket, int maxPerServer) {
        Configuration c = new Configuration();
        c.set(ConfigOptions.SERVER_SCANNER_TTL, Duration.ofHours(1));
        c.set(ConfigOptions.SERVER_SCANNER_EXPIRATION_INTERVAL, Duration.ofHours(1));
        c.set(ConfigOptions.SERVER_SCANNER_MAX_PER_BUCKET, maxPerBucket);
        c.set(ConfigOptions.SERVER_SCANNER_MAX_PER_SERVER, maxPerServer);
        return new ScannerManager(c, scheduler, clock);
    }

    /** Creates a {@link ScannerManager} with a short TTL and reaper interval for eviction tests. */
    private ScannerManager createManagerWithShortTtl(long ttlMs, long expirationIntervalMs) {
        Configuration c = new Configuration();
        c.set(ConfigOptions.SERVER_SCANNER_TTL, Duration.ofMillis(ttlMs));
        c.set(
                ConfigOptions.SERVER_SCANNER_EXPIRATION_INTERVAL,
                Duration.ofMillis(expirationIntervalMs));
        c.set(ConfigOptions.SERVER_SCANNER_MAX_PER_BUCKET, 8);
        c.set(ConfigOptions.SERVER_SCANNER_MAX_PER_SERVER, 200);
        return new ScannerManager(c, scheduler, clock);
    }

    /** Writes {@code count} rows into the KvTablet and flushes to RocksDB. */
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

    // -------------------------------------------------------------------------
    //  Tests
    // -------------------------------------------------------------------------

    @Test
    void testCreateScanner_emptyBucket_returnsNull() throws Exception {
        try (ScannerManager manager = createManager()) {
            TableBucket tableBucket = kvTablet.getTableBucket();
            // Bucket has no data — openScan must return null; no slot consumed.
            ScannerContext context = manager.createScanner(kvTablet, tableBucket, null);
            assertThat(context).isNull();
            assertThat(manager.activeScannerCount()).isEqualTo(0);
        }
    }

    @Test
    void testCreateAndRemoveScanner() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            TableBucket tableBucket = kvTablet.getTableBucket();

            ScannerContext context = manager.createScanner(kvTablet, tableBucket, null);
            assertThat(context).isNotNull();
            assertThat(manager.activeScannerCount()).isEqualTo(1);
            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(1);

            manager.removeScanner(context);
            assertThat(manager.activeScannerCount()).isEqualTo(0);
            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(0);
        }
    }

    @Test
    void testGetScanner_refreshesLastAccessTime() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager()) {
            TableBucket tableBucket = kvTablet.getTableBucket();

            // Create scanner at t=0.
            ScannerContext context = manager.createScanner(kvTablet, tableBucket, null);
            assertThat(context).isNotNull();
            byte[] scannerId = context.getScannerId();

            // Advance clock far past any TTL, then getScanner to refresh.
            clock.advanceTime(5000, TimeUnit.MILLISECONDS);
            ScannerContext fetched = manager.getScanner(scannerId);
            assertThat(fetched).isSameAs(context);

            // With a 1-hour TTL, isExpired must be false right after the refresh.
            assertThat(context.isExpired(3_600_000L, clock.milliseconds())).isFalse();

            manager.removeScanner(context);
        }
    }

    @Test
    void testTtlEviction() throws Exception {
        putAndFlush(3);
        // TTL = 200 ms, reaper every 200 ms — wide enough for slow CI schedulers.
        ScannerManager manager = createManagerWithShortTtl(200, 200);
        try {
            TableBucket tableBucket = kvTablet.getTableBucket();

            ScannerContext context = manager.createScanner(kvTablet, tableBucket, null);
            assertThat(context).isNotNull();
            byte[] scannerId = context.getScannerId();

            // Advance ManualClock past TTL so the reaper considers the session idle.
            clock.advanceTime(500, TimeUnit.MILLISECONDS);

            // Wait for the real scheduler to invoke the cleanup task.
            long deadline = System.currentTimeMillis() + 10_000;
            while (manager.activeScannerCount() > 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }

            assertThat(manager.activeScannerCount()).isEqualTo(0);
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

            ScannerContext ctx1 = manager.createScanner(kvTablet, tableBucket, null);
            ScannerContext ctx2 = manager.createScanner(kvTablet, tableBucket, null);
            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(2);

            assertThatThrownBy(() -> manager.createScanner(kvTablet, tableBucket, null))
                    .isInstanceOf(TooManyScannersException.class);

            // Count must not have changed after the failed attempt.
            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(2);

            manager.removeScanner(ctx1);
            manager.removeScanner(ctx2);
        }
    }

    @Test
    void testPerServerLimit() throws Exception {
        putAndFlush(3);
        try (ScannerManager manager = createManager(8, 2)) {
            TableBucket tableBucket = kvTablet.getTableBucket();

            ScannerContext ctx1 = manager.createScanner(kvTablet, tableBucket, null);
            ScannerContext ctx2 = manager.createScanner(kvTablet, tableBucket, null);
            assertThat(manager.activeScannerCount()).isEqualTo(2);

            assertThatThrownBy(() -> manager.createScanner(kvTablet, tableBucket, null))
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

            manager.createScanner(kvTablet, tableBucket, null);
            manager.createScanner(kvTablet, tableBucket, null);
            assertThat(manager.activeScannerCount()).isEqualTo(2);

            manager.closeScannersForBucket(tableBucket);

            assertThat(manager.activeScannerCount()).isEqualTo(0);
            assertThat(manager.activeScannerCountForBucket(tableBucket)).isEqualTo(0);
        }
    }

    @Test
    void testShutdown_closesAllScanners() throws Exception {
        putAndFlush(3);
        ScannerManager manager = createManager();
        TableBucket tableBucket = kvTablet.getTableBucket();

        manager.createScanner(kvTablet, tableBucket, null);
        manager.createScanner(kvTablet, tableBucket, null);
        assertThat(manager.activeScannerCount()).isEqualTo(2);

        manager.close();

        assertThat(manager.activeScannerCount()).isEqualTo(0);
    }
}

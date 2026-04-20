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
import org.apache.fluss.server.log.LogManager;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.testutils.common.AllCallbackWrapper;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Test for {@link KvIdleReleaseController}. */
final class KvIdleReleaseControllerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final short SCHEMA_ID = 1;
    private static final RowType BASE_ROW_TYPE = TestData.DATA1_ROW_TYPE;
    private static final long IDLE_INTERVAL_MS = 60_000;
    private static final long CHECK_INTERVAL_MS = 10_000;

    private final KvRecordTestUtils.KvRecordBatchFactory batchFactory =
            KvRecordTestUtils.KvRecordBatchFactory.of(SCHEMA_ID);
    private final KvRecordTestUtils.KvRecordFactory recordFactory =
            KvRecordTestUtils.KvRecordFactory.of(BASE_ROW_TYPE);

    private static ZooKeeperClient zkClient;

    private @TempDir File tempDir;

    private LogManager logManager;
    private KvManager kvManager;
    private Configuration conf;
    private ManualClock clock;
    private ManuallyTriggeredScheduledExecutorService scheduler;
    private List<KvTablet> allTablets;
    private int nextTableId = 30001;

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

        clock = new ManualClock(System.currentTimeMillis());
        scheduler = new ManuallyTriggeredScheduledExecutorService();
        allTablets = new ArrayList<>();
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

    private KvTablet createAndOpenTablet(int tableId, int bucketId) throws Exception {
        TablePath tablePath = TablePath.of("db1", "t_idle_" + tableId);
        PhysicalTablePath physicalPath =
                PhysicalTablePath.of(tablePath.getDatabaseName(), tablePath.getTableName(), null);
        TableBucket tableBucket = new TableBucket((long) tableId, bucketId);
        LogTablet logTablet =
                logManager.getOrCreateLog(physicalPath, tableBucket, LogFormat.ARROW, 1, true);

        KvTablet sentinel =
                KvTablet.createLazySentinel(
                        physicalPath,
                        tableBucket,
                        logTablet,
                        TestingMetricGroups.TABLET_SERVER_METRICS);

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

        // Open the tablet via acquireGuard
        try (KvTablet.Guard guard = sentinel.acquireGuard()) {
            // Write and flush so releaseKv() won't be rejected
            KvRecord record =
                    recordFactory.ofRecord(
                            ("k_" + tableId + "_" + bucketId).getBytes(),
                            new Object[] {tableId, "v"});
            KvRecordBatch batch = batchFactory.ofRecords(Collections.singletonList(record));
            sentinel.putAsLeader(batch, null);
            sentinel.flush(Long.MAX_VALUE, NOPErrorHandler.INSTANCE);
        }

        return sentinel;
    }

    private Supplier<Collection<KvTablet>> openTabletSupplier() {
        return () -> allTablets.stream().filter(KvTablet::isLazyOpen).collect(Collectors.toList());
    }

    private KvIdleReleaseController createController() {
        return new KvIdleReleaseController(
                scheduler, openTabletSupplier(), clock, CHECK_INTERVAL_MS, IDLE_INTERVAL_MS);
    }

    // ---- Tests ----

    @Test
    void testNoReleaseWhenAllActive() throws Exception {
        KvTablet tablet1 = createAndOpenTablet(nextTableId++, 0);
        KvTablet tablet2 = createAndOpenTablet(nextTableId++, 0);
        allTablets.add(tablet1);
        allTablets.add(tablet2);

        // Don't advance clock — tablets are recently accessed
        KvIdleReleaseController controller = createController();
        controller.checkAndRelease();

        assertThat(tablet1.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        assertThat(tablet2.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
    }

    @Test
    void testReleaseAllIdleTablets() throws Exception {
        KvTablet tablet1 = createAndOpenTablet(nextTableId++, 0);
        KvTablet tablet2 = createAndOpenTablet(nextTableId++, 0);
        allTablets.add(tablet1);
        allTablets.add(tablet2);

        // Advance clock past idle interval
        clock.advanceTime(IDLE_INTERVAL_MS + 1, TimeUnit.MILLISECONDS);

        KvIdleReleaseController controller = createController();
        controller.checkAndRelease();

        assertThat(tablet1.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);
        assertThat(tablet2.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);
    }

    @Test
    void testOnlyIdleTabletsReleased() throws Exception {
        // Create tablet1 first, then advance clock, then create tablet2
        KvTablet tablet1 = createAndOpenTablet(nextTableId++, 0);
        allTablets.add(tablet1);

        // Advance clock so tablet1 becomes idle but tablet2 (created after) won't be
        clock.advanceTime(IDLE_INTERVAL_MS + 1, TimeUnit.MILLISECONDS);

        KvTablet tablet2 = createAndOpenTablet(nextTableId++, 0);
        allTablets.add(tablet2);

        KvIdleReleaseController controller = createController();
        controller.checkAndRelease();

        // tablet1 is idle — released
        assertThat(tablet1.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);
        // tablet2 is recent — stays open
        assertThat(tablet2.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
    }

    @Test
    void testSkipTabletWithActivePins() throws Exception {
        KvTablet tablet1 = createAndOpenTablet(nextTableId++, 0);
        KvTablet tablet2 = createAndOpenTablet(nextTableId++, 0);
        allTablets.add(tablet1);
        allTablets.add(tablet2);

        // Hold a guard on tablet1 (don't close it) — this pins it
        try (KvTablet.Guard heldGuard = tablet1.acquireGuard()) {
            // Advance clock past idle
            clock.advanceTime(IDLE_INTERVAL_MS + 1, TimeUnit.MILLISECONDS);

            KvIdleReleaseController controller = createController();
            controller.checkAndRelease();

            // tablet1 is pinned — should NOT be released
            assertThat(tablet1.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
            // tablet2 is idle and unpinned — should be released
            assertThat(tablet2.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);
        }
    }

    @Test
    void testContinuesAfterReleaseFailure() throws Exception {
        KvTablet tablet1 = createAndOpenTablet(nextTableId++, 0);
        KvTablet tablet2 = createAndOpenTablet(nextTableId++, 0);
        allTablets.add(tablet1);
        allTablets.add(tablet2);

        // Make tablet1's releaseCallback throw so releaseKv() returns false
        tablet1.getLifecycle()
                .setReleaseCallback(
                        kv -> {
                            throw new RuntimeException("simulated release failure");
                        });

        // Advance clock past idle
        clock.advanceTime(IDLE_INTERVAL_MS + 1, TimeUnit.MILLISECONDS);

        KvIdleReleaseController controller = createController();
        assertThatCode(() -> controller.checkAndRelease()).doesNotThrowAnyException();

        // tablet1 release failed — should rollback to OPEN
        assertThat(tablet1.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.OPEN);
        // tablet2 should still be released despite tablet1's failure
        assertThat(tablet2.getLazyState()).isEqualTo(KvTabletLazyLifecycle.LazyState.LAZY);
    }
}

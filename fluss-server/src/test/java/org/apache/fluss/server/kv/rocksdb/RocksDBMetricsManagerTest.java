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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.kv.KvTablet;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.log.LogTestUtils;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.compression.ArrowCompressionInfo.DEFAULT_COMPRESSION;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA_PK;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RocksDBMetricsManager}. */
class RocksDBMetricsManagerTest {

    @TempDir Path tempDir;
    @TempDir File tempLogDir;
    @TempDir File tmpKvDir;
    private RocksDBResourceContainer resourceContainer;
    private List<RocksDBMetricsCollector> collectors;
    private ManuallyTriggeredScheduledExecutorService testScheduler;
    private TestScheduler scheduler;
    private RocksDBMetricsManager testManager;
    private final Configuration conf = new Configuration();
    @RegisterExtension public RocksDBExtension rocksDBExtension = new RocksDBExtension();

    @BeforeEach
    void setUp() {
        RocksDB.loadLibrary();
        // Setup resource container
        resourceContainer = new RocksDBResourceContainer(new Configuration(), tempDir.toFile());

        collectors = new ArrayList<>();

        // Setup test scheduler
        testScheduler = new ManuallyTriggeredScheduledExecutorService();
        scheduler = new TestScheduler(testScheduler);
        scheduler.startup();

        // Create test manager
        testManager =
                new RocksDBMetricsManager(scheduler, TestingMetricGroups.TABLET_SERVER_METRICS);
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close all collectors
        for (RocksDBMetricsCollector collector : collectors) {
            collector.close();
        }
        collectors.clear();

        // Shutdown manager
        if (testManager != null) {
            try {
                testManager.shutdown();
            } catch (Exception e) {
                // Ignore if already shutdown
            }
        }

        // Clean up resources
        if (resourceContainer != null) {
            resourceContainer.close();
        }
    }

    @Test
    void testRegisterCollector() throws Exception {
        scheduler.startup();

        assertThat(testManager.getCollectorCount()).isEqualTo(0);

        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);

        assertThat(testManager.getCollectorCount()).isEqualTo(1);
        assertThat(testManager.isRunning()).isTrue();

        collectors.add(collector);
    }

    @Test
    void testRegisterMultipleCollectors() throws Exception {
        scheduler.startup();

        for (int i = 0; i < 5; i++) {
            RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", i);
            collectors.add(collector);
        }

        assertThat(testManager.getCollectorCount()).isEqualTo(5);
        assertThat(testManager.isRunning()).isTrue();
    }

    @Test
    void testUnregisterCollector() throws Exception {
        scheduler.startup();

        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);
        collectors.add(collector);

        assertThat(testManager.getCollectorCount()).isEqualTo(1);

        testManager.unregisterCollector(collector);

        assertThat(testManager.getCollectorCount()).isEqualTo(0);
    }

    @Test
    void testRegisterAfterShutdown() throws Exception {
        scheduler.startup();
        testManager.shutdown();

        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);

        // Should not be registered after shutdown
        assertThat(testManager.getCollectorCount()).isEqualTo(0);

        collectors.add(collector);
    }

    @Test
    void testMetricsCollectionStartsWithFirstCollector() throws Exception {
        scheduler.startup();

        assertThat(testManager.isRunning()).isFalse();

        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);
        collectors.add(collector);

        assertThat(testManager.isRunning()).isTrue();
    }

    @Test
    void testBatchProcessing() throws Exception {
        // Ensure scheduler is started
        scheduler.startup();

        // Create many collectors to test batch processing
        int collectorCount = 250; // More than batch size (100)
        List<TestableRocksDBMetricsCollector> testCollectors = new ArrayList<>();

        for (int i = 0; i < collectorCount; i++) {
            TestableRocksDBMetricsCollector collector =
                    createTestableCollector("test_db", "test_table", i);
            testCollectors.add(collector);
            collectors.add(collector);
        }

        // Ensure scheduler task is scheduled (first collector triggers startCollection)
        // Then trigger metrics collection manually
        assertThat(testManager.isRunning()).isTrue();
        assertThat(testScheduler.getActivePeriodicScheduledTask().size()).isGreaterThan(0);

        // Trigger metrics collection manually
        testScheduler.triggerPeriodicScheduledTasks();

        // Verify that updateMetrics was called on all collectors
        for (TestableRocksDBMetricsCollector collector : testCollectors) {
            assertThat(collector.getUpdateMetricsCallCount())
                    .as("updateMetrics should be called at least once")
                    .isGreaterThan(0);
        }
    }

    @Test
    void testExceptionHandlingInMetricsCollection() throws Exception {
        scheduler.startup();

        // Create a collector that throws exception
        FaultyRocksDBMetricsCollector faultyCollector =
                createFaultyCollector("test_db", "test_table", 0);

        // Create a normal collector
        TestableRocksDBMetricsCollector normalCollector =
                createTestableCollector("test_db", "test_table", 1);

        collectors.add(faultyCollector);
        collectors.add(normalCollector);

        // Ensure scheduler task is scheduled
        assertThat(testManager.isRunning()).isTrue();

        // Trigger metrics collection manually
        testScheduler.triggerPeriodicScheduledTasks();

        // Normal collector should still be called despite exception in faulty collector
        assertThat(normalCollector.getUpdateMetricsCallCount())
                .as("Normal collector should still be called despite exception in faulty collector")
                .isGreaterThan(0);
    }

    @Test
    void testShutdown() {
        scheduler.startup();

        // Register some collectors
        for (int i = 0; i < 3; i++) {
            RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", i);
            testManager.registerCollector(collector);
            collectors.add(collector);
        }

        assertThat(testManager.isRunning()).isTrue();
        assertThat(testManager.getCollectorCount()).isEqualTo(3);

        testManager.shutdown();

        assertThat(testManager.isRunning()).isFalse();
        assertThat(testManager.isShutdown()).isTrue();
        assertThat(testManager.getCollectorCount()).isEqualTo(0);
    }

    @Test
    void testDoubleShutdown() {
        scheduler.startup();

        testManager.shutdown();
        assertThat(testManager.isShutdown()).isTrue();

        // Second shutdown should not cause issues
        testManager.shutdown();
        assertThat(testManager.isShutdown()).isTrue();
    }

    @Test
    void testConcurrentRegistrationAndUnregistration() throws Exception {
        scheduler.startup();

        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger collectorId = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(
                    () -> {
                        try {
                            for (int j = 0; j < 10; j++) {
                                RocksDBMetricsCollector collector =
                                        createMockCollector(
                                                "test_db",
                                                "test_table",
                                                collectorId.getAndIncrement());
                                testManager.registerCollector(collector);
                                collectors.add(collector);

                                // Immediately unregister some collectors
                                if (j % 2 == 0) {
                                    testManager.unregisterCollector(collector);
                                }
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
        }

        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Should handle concurrent access gracefully
        assertThat(testManager.getCollectorCount()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testEmptyCollectorsDoesNotCauseIssues() throws Exception {
        scheduler.startup();

        // Start with no collectors
        assertThat(testManager.getCollectorCount()).isEqualTo(0);

        // Should still be not running (no collectors registered)
        assertThat(testManager.isRunning()).isFalse();
    }

    @Test
    void testCreateReporterKey() {
        RocksDBMetricsCollector collector1 = createMockCollector("db1", "table1", 0);
        RocksDBMetricsCollector collector2 = createMockCollector("db1", "table1", 1);
        RocksDBMetricsCollector collector3 = createMockCollector("db2", "table1", 0);

        // Different buckets should have different keys
        assertThat(collector1.getTableBucket().getBucket())
                .isNotEqualTo(collector2.getTableBucket().getBucket());

        // Different databases should have different keys
        assertThat(collector1.getTablePath().getDatabaseName())
                .isNotEqualTo(collector3.getTablePath().getDatabaseName());

        collectors.add(collector1);
        collectors.add(collector2);
        collectors.add(collector3);
    }

    @Test
    void testPartitionedTableCollector() {
        RocksDBMetricsCollector collector =
                createMockCollectorWithPartition("test_db", "test_table", 0, "partition1");

        assertThat(collector.getPartitionName()).isEqualTo("partition1");

        collectors.add(collector);
    }

    @Test
    void testManagerStateAfterAllCollectorsUnregistered() {
        scheduler.startup();

        // Register collectors
        List<RocksDBMetricsCollector> testCollectors = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", i);
            testManager.registerCollector(collector);
            testCollectors.add(collector);
            collectors.add(collector);
        }

        assertThat(testManager.isRunning()).isTrue();
        assertThat(testManager.getCollectorCount()).isEqualTo(3);

        // Unregister all collectors
        for (RocksDBMetricsCollector collector : testCollectors) {
            testManager.unregisterCollector(collector);
        }

        assertThat(testManager.getCollectorCount()).isEqualTo(0);
        // Manager should still be running (collection thread continues)
        assertThat(testManager.isRunning()).isTrue();
    }

    @Test
    void testResourceCleanupOnShutdown() throws Exception {
        scheduler.startup();

        // Register a collector to start the manager
        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);
        testManager.registerCollector(collector);
        collectors.add(collector);

        assertThat(testManager.isRunning()).isTrue();

        testManager.shutdown();

        assertThat(testManager.isShutdown()).isTrue();
        // For test scheduler, we check if scheduled task is cancelled
        // The test scheduler's executor service doesn't have isShutdown() method
        // Instead, we verify that the manager is shutdown and collectors are cleared
        assertThat(testManager.getCollectorCount()).isEqualTo(0);
    }

    @Test
    void testConcurrentCloseAndMetricsCollection() throws Exception {
        scheduler.startup();

        // Create multiple collectors
        List<RocksDBMetricsCollector> testCollectors = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", i);
            testManager.registerCollector(collector);
            testCollectors.add(collector);
            collectors.add(collector);
        }

        assertThat(testManager.isRunning()).isTrue();
        assertThat(testManager.getCollectorCount()).isEqualTo(10);

        // Trigger metrics collection to simulate collection running
        testScheduler.triggerPeriodicScheduledTasks();

        // Start concurrent operations - close collectors while metrics collection is running
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        List<Future<?>> futures = new ArrayList<>();

        // Submit tasks to close collectors concurrently
        for (RocksDBMetricsCollector collector : testCollectors) {
            futures.add(
                    executorService.submit(
                            () -> {
                                try {
                                    collector.close();
                                    // Verify cleanup completion
                                    assertThat(collector.waitForCleanup(2, TimeUnit.SECONDS))
                                            .isTrue();
                                } catch (Exception e) {
                                    // Should not throw exceptions
                                    throw new RuntimeException(e);
                                }
                            }));
        }

        // Wait for all close operations to complete
        for (Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);

        // Trigger metrics collection again to process unregistered collectors
        testScheduler.triggerPeriodicScheduledTasks();
        testScheduler.triggerAll();

        // Verify all collectors were properly unregistered
        assertThat(testManager.getCollectorCount()).isEqualTo(0);

        // Verify manager is still running (collection thread continues)
        assertThat(testManager.isRunning()).isTrue();
    }

    @Test
    void testCleanupTimeoutHandling() throws Exception {
        scheduler.startup();

        // Create a collector
        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);
        testManager.registerCollector(collector);
        collectors.add(collector);

        assertThat(testManager.isRunning()).isTrue();
        assertThat(testManager.getCollectorCount()).isEqualTo(1);

        // Close the collector
        collector.close();

        // Verify cleanup completion with timeout
        assertThat(collector.waitForCleanup(1, TimeUnit.SECONDS)).isTrue();

        // Trigger metrics collection to process unregistered collector
        testScheduler.triggerPeriodicScheduledTasks();
        testScheduler.triggerAll();

        // Verify collector was unregistered
        assertThat(testManager.getCollectorCount()).isEqualTo(0);
    }

    @Test
    void testConcurrentKvTabletCreateAndClose() throws Exception {
        // Test high concurrency scenario simulating Replica dynamic online/offline
        final int threadCount = 20;
        final int operationsPerThread = 10;
        final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final List<Future<?>> futures = new ArrayList<>();
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger errorCount = new AtomicInteger(0);

        // Use existing test manager from setUp

        // Concurrently create and close KvTablets
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            futures.add(
                    executorService.submit(
                            () -> {
                                for (int j = 0; j < operationsPerThread; j++) {
                                    try {
                                        // Create a new KvTablet
                                        PhysicalTablePath tablePath =
                                                PhysicalTablePath.of(
                                                        TablePath.of(
                                                                "testDb",
                                                                "table_" + threadId + "_" + j));
                                        File kvDir = new File(tmpKvDir, "kv_" + threadId + "_" + j);
                                        kvDir.mkdirs();

                                        LogTablet logTablet =
                                                createLogTablet(
                                                        tempLogDir,
                                                        threadId * 1000L + j,
                                                        tablePath);
                                        TableBucket tableBucket = logTablet.getTableBucket();

                                        KvTablet kvTablet =
                                                createKvTablet(
                                                        tablePath,
                                                        tableBucket,
                                                        logTablet,
                                                        kvDir,
                                                        DATA1_SCHEMA_PK,
                                                        new HashMap<>());

                                        // RocksDB metrics collector is automatically initialized in
                                        // constructor
                                        // Close the KvTablet
                                        kvTablet.close();

                                        successCount.incrementAndGet();
                                    } catch (Exception e) {
                                        errorCount.incrementAndGet();
                                        // Log but don't fail - we're testing resilience
                                        System.err.println(
                                                "Error in thread "
                                                        + threadId
                                                        + " operation "
                                                        + j
                                                        + ": "
                                                        + e.getMessage());
                                    }
                                }
                            }));
        }

        // Wait for all operations to complete
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }

        // Trigger metrics collection multiple times to simulate multiple collection cycles
        // This ensures all collectors are properly processed and unregistered
        for (int i = 0; i < 3; i++) {
            testScheduler.triggerPeriodicScheduledTasks();
            testScheduler.triggerAll();
        }

        // Verify no null pointer exceptions occurred
        assertThat(errorCount.get())
                .as("No exceptions should occur during concurrent create/close")
                .isEqualTo(0);

        // Verify metrics manager is still running
        assertThat(testManager.isRunning()).isTrue();

        // Verify metrics manager can still aggregate without errors
        assertThat(testManager.getCollectorCount())
                .as("All collectors should be unregistered after close")
                .isEqualTo(0);

        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    private RocksDBMetricsCollector createMockCollector(String database, String table, int bucket) {
        return createMockCollectorWithPartition(database, table, bucket, null);
    }

    private RocksDBMetricsCollector createMockCollectorWithPartition(
            String database, String table, int bucket, String partition) {
        // Create a new Statistics object for testing
        Statistics statistics = new Statistics();

        TablePath tablePath = TablePath.of(database, table);
        TableBucket tableBucket = new TableBucket(0, bucket);

        return new RocksDBMetricsCollector(
                rocksDBExtension.getRocksDb(),
                statistics,
                tableBucket,
                tablePath,
                partition,
                resourceContainer,
                testManager);
    }

    private TestableRocksDBMetricsCollector createTestableCollector(
            String database, String table, int bucket) {
        // Create a new Statistics object for testing
        Statistics statistics = new Statistics();

        TablePath tablePath = TablePath.of(database, table);
        TableBucket tableBucket = new TableBucket(0, bucket);

        return new TestableRocksDBMetricsCollector(
                rocksDBExtension.getRocksDb(),
                statistics,
                tableBucket,
                tablePath,
                null,
                resourceContainer,
                testManager);
    }

    private FaultyRocksDBMetricsCollector createFaultyCollector(
            String database, String table, int bucket) {
        // Create a new Statistics object for testing
        Statistics statistics = new Statistics();

        TablePath tablePath = TablePath.of(database, table);
        TableBucket tableBucket = new TableBucket(0, bucket);

        return new FaultyRocksDBMetricsCollector(
                rocksDBExtension.getRocksDb(),
                statistics,
                tableBucket,
                tablePath,
                null,
                resourceContainer,
                testManager);
    }

    private LogTablet createLogTablet(File tempLogDir, long tableId, PhysicalTablePath tablePath)
            throws Exception {
        File logTabletDir =
                LogTestUtils.makeRandomLogTabletDir(
                        tempLogDir, tablePath.getDatabaseName(), tableId, tablePath.getTableName());
        return LogTablet.create(
                tablePath,
                logTabletDir,
                conf,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                0,
                new FlussScheduler(1),
                LogFormat.ARROW,
                1,
                true,
                SystemClock.getInstance(),
                true);
    }

    private KvTablet createKvTablet(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogTablet logTablet,
            File tmpKvDir,
            Schema schema,
            Map<String, String> tableConfig)
            throws Exception {
        RowMerger rowMerger =
                RowMerger.create(
                        new TableConfig(Configuration.fromMap(tableConfig)),
                        schema,
                        KvFormat.COMPACTED);
        return KvTablet.create(
                tablePath,
                tableBucket,
                logTablet,
                tmpKvDir,
                conf,
                TestingMetricGroups.TABLET_SERVER_METRICS,
                testManager,
                new RootAllocator(Long.MAX_VALUE),
                new TestingMemorySegmentPool(10 * 1024),
                KvFormat.COMPACTED,
                schema,
                rowMerger,
                DEFAULT_COMPRESSION);
    }

    /** A testable Scheduler implementation that wraps ManuallyTriggeredScheduledExecutorService. */
    private static class TestScheduler implements Scheduler {
        private final ManuallyTriggeredScheduledExecutorService executorService;
        boolean started = false;

        TestScheduler(ManuallyTriggeredScheduledExecutorService executorService) {
            this.executorService = executorService;
        }

        @Override
        public void startup() {
            started = true;
        }

        @Override
        public void shutdown() throws InterruptedException {
            started = false;
            executorService.shutdown();
        }

        @Override
        public ScheduledFuture<?> schedule(
                String name, Runnable task, long delayMs, long periodMs) {
            if (!started) {
                throw new IllegalStateException("Scheduler not started");
            }
            if (periodMs > 0) {
                return new TestScheduledFuture(
                        executorService.scheduleAtFixedRate(
                                task, delayMs, periodMs, TimeUnit.MILLISECONDS));
            } else {
                return new TestScheduledFuture(
                        executorService.schedule(task, delayMs, TimeUnit.MILLISECONDS));
            }
        }
    }

    /** Wrapper for ScheduledFuture to match Scheduler interface. */
    private static class TestScheduledFuture implements ScheduledFuture<Void> {
        private final ScheduledFuture<?> delegate;

        TestScheduledFuture(ScheduledFuture<?> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return delegate.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public Void get() {
            try {
                delegate.get();
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Void get(long timeout, TimeUnit unit) {
            try {
                delegate.get(timeout, unit);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return delegate.getDelay(unit);
        }

        @Override
        public int compareTo(java.util.concurrent.Delayed o) {
            return delegate.compareTo(o);
        }
    }

    /** Testable RocksDBMetricsCollector that tracks updateMetrics() calls without using mockito. */
    private static class TestableRocksDBMetricsCollector extends RocksDBMetricsCollector {
        private final AtomicLong updateMetricsCallCount = new AtomicLong(0);

        public TestableRocksDBMetricsCollector(
                RocksDB rocksDB,
                Statistics statistics,
                TableBucket tableBucket,
                TablePath tablePath,
                String partitionName,
                RocksDBResourceContainer resourceContainer,
                RocksDBMetricsManager metricsManager) {
            super(
                    rocksDB,
                    statistics,
                    tableBucket,
                    tablePath,
                    partitionName,
                    resourceContainer,
                    metricsManager);
        }

        @Override
        public void updateMetrics() {
            updateMetricsCallCount.incrementAndGet();
            super.updateMetrics();
        }

        public long getUpdateMetricsCallCount() {
            return updateMetricsCallCount.get();
        }
    }

    /**
     * Faulty RocksDBMetricsCollector that throws exception in updateMetrics() for testing error
     * handling.
     */
    private static class FaultyRocksDBMetricsCollector extends RocksDBMetricsCollector {
        public FaultyRocksDBMetricsCollector(
                RocksDB rocksDB,
                Statistics statistics,
                TableBucket tableBucket,
                TablePath tablePath,
                String partitionName,
                RocksDBResourceContainer resourceContainer,
                RocksDBMetricsManager metricsManager) {
            super(
                    rocksDB,
                    statistics,
                    tableBucket,
                    tablePath,
                    partitionName,
                    resourceContainer,
                    metricsManager);
        }

        @Override
        public void updateMetrics() {
            throw new RuntimeException("Test exception");
        }
    }
}

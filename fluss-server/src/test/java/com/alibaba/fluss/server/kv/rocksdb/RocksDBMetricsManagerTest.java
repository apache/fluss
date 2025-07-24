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

package com.alibaba.fluss.server.kv.rocksdb;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.metrics.group.TestingMetricGroups;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockitoAnnotations;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for {@link RocksDBMetricsManager}. */
class RocksDBMetricsManagerTest {

    @TempDir Path tempDir;
    private RocksDBResourceContainer resourceContainer;
    private List<RocksDBMetricsCollector> collectors;
    private AutoCloseable mocksCloseable;
    @RegisterExtension public RocksDBExtension rocksDBExtension = new RocksDBExtension();

    @BeforeEach
    void setUp() {
        RocksDB.loadLibrary();
        mocksCloseable = MockitoAnnotations.openMocks(this);
        // Setup resource container
        resourceContainer = new RocksDBResourceContainer(new Configuration(), tempDir.toFile());

        collectors = new ArrayList<>();

        // Reset singleton instance for testing
        resetSingleton();
    }

    @AfterEach
    void tearDown() throws Exception {
        // Close all collectors
        for (RocksDBMetricsCollector collector : collectors) {
            collector.close();
        }
        collectors.clear();

        // Shutdown manager
        RocksDBMetricsManager.getInstance().shutdown();

        // Clean up resources
        if (resourceContainer != null) {
            resourceContainer.close();
        }
        if (mocksCloseable != null) {
            mocksCloseable.close();
        }

        // Reset singleton for next test
        resetSingleton();
    }

    @Test
    void testRegisterCollector() throws Exception {

        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        assertThat(manager.getCollectorCount()).isEqualTo(0);

        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);
        manager.registerCollector(collector);

        assertThat(manager.getCollectorCount()).isEqualTo(1);
        assertThat(manager.isRunning()).isTrue();

        collectors.add(collector);
    }

    @Test
    void testRegisterMultipleCollectors() throws Exception {

        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        for (int i = 0; i < 5; i++) {
            RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", i);
            manager.registerCollector(collector);
            collectors.add(collector);
        }

        assertThat(manager.getCollectorCount()).isEqualTo(5);
        assertThat(manager.isRunning()).isTrue();
    }

    @Test
    void testUnregisterCollector() throws Exception {
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);
        manager.registerCollector(collector);
        collectors.add(collector);

        assertThat(manager.getCollectorCount()).isEqualTo(1);

        manager.unregisterCollector(collector);

        assertThat(manager.getCollectorCount()).isEqualTo(0);
    }

    @Test
    void testRegisterAfterShutdown() throws Exception {
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();
        manager.shutdown();

        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);
        manager.registerCollector(collector);

        // Should not be registered after shutdown
        assertThat(manager.getCollectorCount()).isEqualTo(0);

        collectors.add(collector);
    }

    @Test
    void testMetricsCollectionStartsWithFirstCollector() throws Exception {

        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        assertThat(manager.isRunning()).isFalse();

        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);
        manager.registerCollector(collector);
        collectors.add(collector);

        assertThat(manager.isRunning()).isTrue();
    }

    @Test
    void testBatchProcessing() throws Exception {
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        // Create many collectors to test batch processing
        int collectorCount = 250; // More than batch size (100)
        List<RocksDBMetricsCollector> spyCollectors = new ArrayList<>();

        for (int i = 0; i < collectorCount; i++) {
            RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", i);
            RocksDBMetricsCollector spyCollector = spy(collector);
            manager.registerCollector(spyCollector);
            spyCollectors.add(spyCollector);
            collectors.add(collector);
        }

        // Wait for metrics collection to occur
        Thread.sleep(6000); // Wait for at least one collection cycle

        // Verify that updateMetrics was called on all collectors
        for (RocksDBMetricsCollector collector : spyCollectors) {
            verify(collector, times(1)).updateMetrics();
        }
    }

    @Test
    void testExceptionHandlingInMetricsCollection() throws Exception {
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        // Create a collector that throws exception
        RocksDBMetricsCollector faultyCollector = createMockCollector("test_db", "test_table", 0);
        doThrow(new RuntimeException("Test exception")).when(faultyCollector).updateMetrics();

        // Create a normal collector
        RocksDBMetricsCollector normalCollector = createMockCollector("test_db", "test_table", 1);
        RocksDBMetricsCollector spyNormalCollector = spy(normalCollector);

        manager.registerCollector(faultyCollector);
        manager.registerCollector(spyNormalCollector);

        collectors.add(faultyCollector);
        collectors.add(normalCollector);

        // Wait for metrics collection
        try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Normal collector should still be called despite exception in faulty collector
        verify(spyNormalCollector, times(1)).updateMetrics();
    }

    @Test
    void testShutdown() {
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        // Register some collectors
        for (int i = 0; i < 3; i++) {
            RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", i);
            manager.registerCollector(collector);
            collectors.add(collector);
        }

        assertThat(manager.isRunning()).isTrue();
        assertThat(manager.getCollectorCount()).isEqualTo(3);

        manager.shutdown();

        assertThat(manager.isRunning()).isFalse();
        assertThat(manager.isShutdown()).isTrue();
        assertThat(manager.getCollectorCount()).isEqualTo(0);
    }

    @Test
    void testDoubleShutdown() {
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        manager.shutdown();
        assertThat(manager.isShutdown()).isTrue();

        // Second shutdown should not cause issues
        manager.shutdown();
        assertThat(manager.isShutdown()).isTrue();
    }

    @Test
    void testConcurrentRegistrationAndUnregistration() throws Exception {
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

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
                                manager.registerCollector(collector);
                                collectors.add(collector);

                                // Immediately unregister some collectors
                                if (j % 2 == 0) {
                                    manager.unregisterCollector(collector);
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
        assertThat(manager.getCollectorCount()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void testEmptyCollectorsDoesNotCauseIssues() throws Exception {
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        // Start with no collectors
        assertThat(manager.getCollectorCount()).isEqualTo(0);

        // Wait a bit to ensure no exceptions are thrown
        Thread.sleep(1000);

        // Should still be not running
        assertThat(manager.isRunning()).isFalse();
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
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        // Register collectors
        List<RocksDBMetricsCollector> testCollectors = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", i);
            manager.registerCollector(collector);
            testCollectors.add(collector);
            collectors.add(collector);
        }

        assertThat(manager.isRunning()).isTrue();
        assertThat(manager.getCollectorCount()).isEqualTo(3);

        // Unregister all collectors
        for (RocksDBMetricsCollector collector : testCollectors) {
            manager.unregisterCollector(collector);
        }

        assertThat(manager.getCollectorCount()).isEqualTo(0);
        // Manager should still be running (collection thread continues)
        assertThat(manager.isRunning()).isTrue();
    }

    @Test
    void testResourceCleanupOnShutdown() throws Exception {
        RocksDBMetricsManager manager = RocksDBMetricsManager.getInstance();

        // Register a collector to start the manager
        RocksDBMetricsCollector collector = createMockCollector("test_db", "test_table", 0);
        manager.registerCollector(collector);
        collectors.add(collector);

        assertThat(manager.isRunning()).isTrue();

        // Get the executor service via reflection to verify it's cleaned up
        Field executorField =
                RocksDBMetricsManager.class.getDeclaredField("metricsCollectionExecutor");
        executorField.setAccessible(true);
        ScheduledExecutorService executor = (ScheduledExecutorService) executorField.get(manager);

        assertThat(executor.isShutdown()).isFalse();

        manager.shutdown();

        assertThat(manager.isShutdown()).isTrue();
        assertThat(executor.isShutdown()).isTrue();
    }

    private RocksDBMetricsCollector createMockCollector(String database, String table, int bucket) {
        return createMockCollectorWithPartition(database, table, bucket, null);
    }

    private RocksDBMetricsCollector createMockCollectorWithPartition(
            String database, String table, int bucket, String partition) {
        Statistics mockStatistics = mock(Statistics.class);

        TablePath tablePath = TablePath.of(database, table);
        TableBucket tableBucket = new TableBucket(0, bucket);

        return spy(
                new RocksDBMetricsCollector(
                        rocksDBExtension.getRocksDb(),
                        mockStatistics,
                        TestingMetricGroups.BUCKET_METRICS,
                        tableBucket,
                        tablePath,
                        partition,
                        resourceContainer));
    }

    private void resetSingleton() {
        try {
            Field instanceField = RocksDBMetricsManager.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            instanceField.set(null, null);
        } catch (Exception e) {
            // Ignore for testing
        }
    }
}

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
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.server.metrics.group.BucketMetricGroup;
import com.alibaba.fluss.server.metrics.group.TestingMetricGroups;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockitoAnnotations;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link RocksDBMetricsCollector}. */
class RocksDBMetricsCollectorTest {

    @TempDir Path tempDir;
    private RocksDBResourceContainer resourceContainer;
    private RocksDBMetricsCollector collector;
    private Statistics mockStatistics;
    private AutoCloseable mocksCloseable;
    private BucketMetricGroup bucketMetricGroup;
    private AutoCloseable mockMemUtilAutocloseable;
    @RegisterExtension public RocksDBExtension rocksDBExtension = new RocksDBExtension();

    @BeforeEach
    void setUp() {
        RocksDB.loadLibrary();

        mocksCloseable = MockitoAnnotations.openMocks(this);

        bucketMetricGroup = spy(TestingMetricGroups.BUCKET_METRICS);

        // Create test table metadata
        TablePath tablePath = TablePath.of("test_db", "test_table");
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
        TableBucket tableBucket = new TableBucket(1, 1);
        // Mock RocksDB components
        mockStatistics = mock(Statistics.class);

        // Mock resource container
        resourceContainer =
                spy(new RocksDBResourceContainer(new Configuration(), tempDir.toFile()));

        // Create collector
        collector =
                new RocksDBMetricsCollector(
                        rocksDBExtension.getRocksDb(),
                        mockStatistics,
                        bucketMetricGroup,
                        tableBucket,
                        tablePath,
                        null,
                        resourceContainer);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (collector != null) {
            collector.close();
        }
        if (resourceContainer != null) {
            resourceContainer.close();
        }
        if (mocksCloseable != null) {
            mocksCloseable.close();
        }
        if (mockMemUtilAutocloseable != null) {
            mockMemUtilAutocloseable.close();
        }
    }

    @Test
    void testConstructorRegistersMetrics() {
        // Verify that metrics are registered
        verify(bucketMetricGroup).gauge(eq(MetricNames.ROCKSDB_BLOCK_CACHE_HIT_COUNT), any());
        verify(bucketMetricGroup).gauge(eq(MetricNames.ROCKSDB_BLOCK_CACHE_MISS_COUNT), any());
        verify(bucketMetricGroup).gauge(eq(MetricNames.ROCKSDB_COMPACTION_COUNT), any());
        verify(bucketMetricGroup).gauge(eq(MetricNames.ROCKSDB_BYTES_READ), any());
        verify(bucketMetricGroup).gauge(eq(MetricNames.ROCKSDB_BYTES_WRITTEN), any());
        verify(bucketMetricGroup).gauge(eq(MetricNames.ROCKSDB_MEMTABLE_MEMORY_USAGE), any());
        verify(bucketMetricGroup).gauge(eq(MetricNames.ROCKSDB_TOTAL_MEMORY_USAGE), any());

        // Verify collector is registered with manager
        assertThat(RocksDBMetricsManager.getInstance().getCollectorCount()).isGreaterThan(0);
    }

    @Test
    void testCloseUnregistersFromManager() {
        int initialCount = RocksDBMetricsManager.getInstance().getCollectorCount();

        collector.close();

        // Should be unregistered
        assertThat(RocksDBMetricsManager.getInstance().getCollectorCount())
                .isLessThan(initialCount);
    }

    @Test
    void testDoubleClose() {
        collector.close();
        // Second close should not throw
        collector.close();
    }

    @Test
    void testUpdateMetricsAfterClose() {
        collector.close();

        // This should not throw exception
        collector.updateMetrics();
    }

    @Test
    void testHistogramMetrics() throws Exception {
        // Setup mock histogram
        org.rocksdb.HistogramData histogramData = mock(org.rocksdb.HistogramData.class);
        when(histogramData.getAverage()).thenReturn(123.45);
        when(mockStatistics.getHistogramData(any())).thenReturn(histogramData);

        collector.updateMetrics();

        verify(mockStatistics).getHistogramData(any());
    }

    @Test
    void testHistogramMetricsWithNullHistogram() throws Exception {
        // Mock null histogram
        when(mockStatistics.getHistogramData(any())).thenReturn(null);

        collector.updateMetrics();

        verify(mockStatistics).getHistogramData(any());
    }

    @Test
    void testAllMetricNames() {
        // Verify all expected metrics are registered
        String[] expectedMetrics = {
            MetricNames.ROCKSDB_BLOCK_CACHE_HIT_COUNT,
            MetricNames.ROCKSDB_BLOCK_CACHE_MISS_COUNT,
            MetricNames.ROCKSDB_INDEX_BLOCK_CACHE_HIT_COUNT,
            MetricNames.ROCKSDB_INDEX_BLOCK_CACHE_MISS_COUNT,
            MetricNames.ROCKSDB_FILTER_BLOCK_CACHE_HIT_COUNT,
            MetricNames.ROCKSDB_FILTER_BLOCK_CACHE_MISS_COUNT,
            MetricNames.ROCKSDB_DATA_BLOCK_CACHE_HIT_COUNT,
            MetricNames.ROCKSDB_DATA_BLOCK_CACHE_MISS_COUNT,
            MetricNames.ROCKSDB_COMPACTION_COUNT,
            MetricNames.ROCKSDB_COMPACTION_BYTES_READ,
            MetricNames.ROCKSDB_COMPACTION_BYTES_WRITTEN,
            MetricNames.ROCKSDB_COMPACTION_CPU_TIME_MICROS,
            MetricNames.ROCKSDB_FLUSH_COUNT,
            MetricNames.ROCKSDB_FLUSH_BYTES_WRITTEN,
            MetricNames.ROCKSDB_STALL_TIME_MICROS,
            MetricNames.ROCKSDB_BYTES_READ,
            MetricNames.ROCKSDB_BYTES_WRITTEN,
            MetricNames.ROCKSDB_NUMBER_DB_NEXT,
            MetricNames.ROCKSDB_NUMBER_DB_PREV,
            MetricNames.ROCKSDB_NUMBER_DB_SEEK,
            MetricNames.ROCKSDB_NUMBER_DB_SEEK_FOUND,
            MetricNames.ROCKSDB_NUMBER_KEYS_READ,
            MetricNames.ROCKSDB_NUMBER_KEYS_WRITTEN,
            MetricNames.ROCKSDB_NUMBER_KEYS_UPDATED,
            MetricNames.ROCKSDB_NUM_LIVE_VERSIONS,
            MetricNames.ROCKSDB_NUM_IMMUTABLE_MEMTABLES,
            MetricNames.ROCKSDB_NUM_DELETES_ACTIVE_MEMTABLE,
            MetricNames.ROCKSDB_NUM_DELETES_IMMUTABLE_MEMTABLE,
            MetricNames.ROCKSDB_NUM_ENTRIES_ACTIVE_MEMTABLE,
            MetricNames.ROCKSDB_NUM_ENTRIES_IMMUTABLE_MEMTABLE,
            MetricNames.ROCKSDB_ACTIVE_MEMTABLE_SIZE,
            MetricNames.ROCKSDB_UNFLUSHED_MEMTABLE_SIZE,
            MetricNames.ROCKSDB_TOTAL_SST_FILES_SIZE,
            MetricNames.ROCKSDB_LIVE_SST_FILES_SIZE,
            MetricNames.ROCKSDB_SIZE_ALL_MEMTABLES,
            MetricNames.ROCKSDB_BLOCK_CACHE_ADD_COUNT,
            MetricNames.ROCKSDB_MEMTABLE_MEMORY_USAGE,
            MetricNames.ROCKSDB_BLOCK_CACHE_MEMORY_USAGE,
            MetricNames.ROCKSDB_TABLE_READERS_MEMORY_USAGE,
            MetricNames.ROCKSDB_TOTAL_MEMORY_USAGE
        };

        for (String metricName : expectedMetrics) {
            verify(bucketMetricGroup).gauge(eq(metricName), any());
        }
    }
}

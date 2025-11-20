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

package org.apache.fluss.server.metrics.group;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.CharacterFilter;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.DescriptiveStatisticsHistogram;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.utils.MapUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** The metric group for tablet server. */
public class TabletServerMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "tabletserver";
    private static final int WINDOW_SIZE = 1024;

    private final Map<TablePath, TableMetricGroup> metricGroupByTable =
            MapUtils.newConcurrentHashMap();

    protected final String clusterId;
    protected final String rack;
    protected final String hostname;
    protected final int serverId;

    // ---- metrics ----
    private final Counter replicationBytesIn;
    private final Counter replicationBytesOut;
    private final Counter delayedWriteExpireCount;
    private final Counter delayedFetchFromFollowerExpireCount;
    private final Counter delayedFetchFromClientExpireCount;

    // aggregated metrics
    private final Counter messagesIn;
    private final Counter bytesIn;
    private final Counter bytesOut;

    // aggregated log metrics
    private final Counter logFlushCount;
    private final Histogram logFlushLatencyHistogram;

    // aggregated kv metrics
    private final Counter kvFlushCount;
    private final Histogram kvFlushLatencyHistogram;
    private final Counter kvTruncateAsDuplicatedCount;
    private final Counter kvTruncateAsErrorCount;

    // aggregated replica metrics
    private final Counter isrShrinks;
    private final Counter isrExpands;
    private final Counter failedIsrUpdates;

    // aggregated RocksDB metrics - Counter type (SUM aggregation)
    private final Counter rocksdbBlockCacheMissCount;
    private final Counter rocksdbBlockCacheHitCount;
    private final Counter rocksdbBlockCacheAddCount;
    private final Counter rocksdbCompactionBytesRead;
    private final Counter rocksdbCompactionBytesWritten;
    private final Counter rocksdbFlushBytesWritten;
    private final Counter rocksdbBytesRead;
    private final Counter rocksdbBytesWritten;

    // aggregated RocksDB metrics - Gauge type SUM (resource usage)
    private final AtomicLong rocksdbBlockCacheUsage = new AtomicLong(0);
    private final AtomicLong rocksdbBlockCachePinnedUsage = new AtomicLong(0);
    private final AtomicLong rocksdbMemtableMemoryUsage = new AtomicLong(0);
    private final AtomicLong rocksdbBlockCacheMemoryUsage = new AtomicLong(0);
    private final AtomicLong rocksdbTableReadersMemoryUsage = new AtomicLong(0);
    private final AtomicLong rocksdbTotalMemoryUsage = new AtomicLong(0);
    private final AtomicLong rocksdbTotalSstFilesSize = new AtomicLong(0);

    // aggregated RocksDB metrics - Gauge type AVG (count metrics)
    private final AtomicLong rocksdbCompactionPendingSum = new AtomicLong(0);
    private final AtomicLong rocksdbFlushPendingSum = new AtomicLong(0);
    private final AtomicLong rocksdbNumFilesAtLevel0Sum = new AtomicLong(0);
    private final AtomicLong rocksdbCollectorCount = new AtomicLong(0);

    // aggregated RocksDB metrics - Gauge type MAX (time metrics)
    private final AtomicLong rocksdbWriteStallMicros = new AtomicLong(0);

    // aggregated RocksDB metrics - Histogram type AVG (latency metrics)
    private final Histogram rocksdbDbGetLatencyHistogram;
    private final Histogram rocksdbDbWriteLatencyHistogram;
    private final Histogram rocksdbCompactionTimeHistogram;
    private final AtomicLong rocksdbLatencyCollectorCount = new AtomicLong(0);

    public TabletServerMetricGroup(
            MetricRegistry registry, String clusterId, String rack, String hostname, int serverId) {
        super(registry, new String[] {clusterId, hostname, NAME}, null);
        this.clusterId = clusterId;
        this.rack = rack;
        this.hostname = hostname;
        this.serverId = serverId;

        replicationBytesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.REPLICATION_IN_RATE, new MeterView(replicationBytesIn));
        replicationBytesOut = new ThreadSafeSimpleCounter();
        meter(MetricNames.REPLICATION_OUT_RATE, new MeterView(replicationBytesOut));

        delayedWriteExpireCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.DELAYED_WRITE_EXPIRES_RATE, new MeterView(delayedWriteExpireCount));
        delayedFetchFromFollowerExpireCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.DELAYED_FETCH_FROM_FOLLOWER_EXPIRES_RATE,
                new MeterView(delayedFetchFromFollowerExpireCount));
        delayedFetchFromClientExpireCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.DELAYED_FETCH_FROM_CLIENT_EXPIRES_RATE,
                new MeterView(delayedFetchFromClientExpireCount));

        messagesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.MESSAGES_IN_RATE, new MeterView(messagesIn));
        bytesIn = new ThreadSafeSimpleCounter();
        meter(MetricNames.BYTES_IN_RATE, new MeterView(bytesIn));
        bytesOut = new ThreadSafeSimpleCounter();
        meter(MetricNames.BYTES_OUT_RATE, new MeterView(bytesOut));

        // about flush
        logFlushCount = new SimpleCounter();
        meter(MetricNames.LOG_FLUSH_RATE, new MeterView(logFlushCount));
        logFlushLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.LOG_FLUSH_LATENCY_MS, logFlushLatencyHistogram);

        // about pre-write buffer.
        kvFlushCount = new SimpleCounter();
        meter(MetricNames.KV_FLUSH_RATE, new MeterView(kvFlushCount));
        kvFlushLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.KV_FLUSH_LATENCY_MS, kvFlushLatencyHistogram);
        kvTruncateAsDuplicatedCount = new SimpleCounter();
        meter(
                MetricNames.KV_PRE_WRITE_BUFFER_TRUNCATE_AS_DUPLICATED_RATE,
                new MeterView(kvTruncateAsDuplicatedCount));
        kvTruncateAsErrorCount = new SimpleCounter();
        meter(
                MetricNames.KV_PRE_WRITE_BUFFER_TRUNCATE_AS_ERROR_RATE,
                new MeterView(kvTruncateAsErrorCount));

        // replica metrics
        isrExpands = new SimpleCounter();
        meter(MetricNames.ISR_EXPANDS_RATE, new MeterView(isrExpands));
        isrShrinks = new SimpleCounter();
        meter(MetricNames.ISR_SHRINKS_RATE, new MeterView(isrShrinks));
        failedIsrUpdates = new SimpleCounter();
        meter(MetricNames.FAILED_ISR_UPDATES_RATE, new MeterView(failedIsrUpdates));

        // RocksDB metrics - Counter type (SUM aggregation)
        rocksdbBlockCacheMissCount = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.ROCKSDB_BLOCK_CACHE_MISS_COUNT,
                new MeterView(rocksdbBlockCacheMissCount));
        rocksdbBlockCacheHitCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.ROCKSDB_BLOCK_CACHE_HIT_COUNT, new MeterView(rocksdbBlockCacheHitCount));
        rocksdbBlockCacheAddCount = new ThreadSafeSimpleCounter();
        meter(MetricNames.ROCKSDB_BLOCK_CACHE_ADD_COUNT, new MeterView(rocksdbBlockCacheAddCount));
        rocksdbCompactionBytesRead = new ThreadSafeSimpleCounter();
        meter(MetricNames.ROCKSDB_COMPACTION_BYTES_READ, new MeterView(rocksdbCompactionBytesRead));
        rocksdbCompactionBytesWritten = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.ROCKSDB_COMPACTION_BYTES_WRITTEN,
                new MeterView(rocksdbCompactionBytesWritten));
        rocksdbFlushBytesWritten = new ThreadSafeSimpleCounter();
        meter(MetricNames.ROCKSDB_FLUSH_BYTES_WRITTEN, new MeterView(rocksdbFlushBytesWritten));
        rocksdbBytesRead = new ThreadSafeSimpleCounter();
        meter(MetricNames.ROCKSDB_BYTES_READ, new MeterView(rocksdbBytesRead));
        rocksdbBytesWritten = new ThreadSafeSimpleCounter();
        meter(MetricNames.ROCKSDB_BYTES_WRITTEN, new MeterView(rocksdbBytesWritten));

        // RocksDB metrics - Gauge type SUM (resource usage)
        gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_USAGE,
                (Gauge<Long>) () -> rocksdbBlockCacheUsage.get());
        gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_PINNED_USAGE,
                (Gauge<Long>) () -> rocksdbBlockCachePinnedUsage.get());
        gauge(
                MetricNames.ROCKSDB_MEMTABLE_MEMORY_USAGE,
                (Gauge<Long>) () -> rocksdbMemtableMemoryUsage.get());
        gauge(
                MetricNames.ROCKSDB_BLOCK_CACHE_MEMORY_USAGE,
                (Gauge<Long>) () -> rocksdbBlockCacheMemoryUsage.get());
        gauge(
                MetricNames.ROCKSDB_TABLE_READERS_MEMORY_USAGE,
                (Gauge<Long>) () -> rocksdbTableReadersMemoryUsage.get());
        gauge(
                MetricNames.ROCKSDB_TOTAL_MEMORY_USAGE,
                (Gauge<Long>) () -> rocksdbTotalMemoryUsage.get());
        gauge(
                MetricNames.ROCKSDB_TOTAL_SST_FILES_SIZE,
                (Gauge<Long>) () -> rocksdbTotalSstFilesSize.get());

        // RocksDB metrics - Gauge type AVG (count metrics)
        gauge(
                MetricNames.ROCKSDB_COMPACTION_PENDING,
                (Gauge<Long>)
                        () -> {
                            long count = rocksdbCollectorCount.get();
                            return count > 0 ? rocksdbCompactionPendingSum.get() / count : 0;
                        });
        gauge(
                MetricNames.ROCKSDB_FLUSH_PENDING,
                (Gauge<Long>)
                        () -> {
                            long count = rocksdbCollectorCount.get();
                            return count > 0 ? rocksdbFlushPendingSum.get() / count : 0;
                        });
        gauge(
                MetricNames.ROCKSDB_NUM_FILES_AT_LEVEL_0,
                (Gauge<Long>)
                        () -> {
                            long count = rocksdbCollectorCount.get();
                            return count > 0 ? rocksdbNumFilesAtLevel0Sum.get() / count : 0;
                        });

        // RocksDB metrics - Gauge type MAX (time metrics)
        gauge(
                MetricNames.ROCKSDB_WRITE_STALL_MICROS,
                (Gauge<Long>) () -> rocksdbWriteStallMicros.get());

        // RocksDB metrics - Histogram type AVG (latency metrics)
        rocksdbDbGetLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.ROCKSDB_DB_GET_LATENCY_MICROS, rocksdbDbGetLatencyHistogram);
        rocksdbDbWriteLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.ROCKSDB_DB_WRITE_LATENCY_MICROS, rocksdbDbWriteLatencyHistogram);
        rocksdbCompactionTimeHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(MetricNames.ROCKSDB_COMPACTION_TIME_MICROS, rocksdbCompactionTimeHistogram);
    }

    @Override
    protected final void putVariables(Map<String, String> variables) {
        variables.put("cluster_id", clusterId);
        if (rack != null) {
            variables.put("rack", rack);
        } else {
            // The value of an empty string indicates no rack
            variables.put("rack", "");
        }
        variables.put("host", hostname);
        variables.put("server_id", String.valueOf(serverId));
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return NAME;
    }

    public Counter replicationBytesIn() {
        return replicationBytesIn;
    }

    public Counter replicationBytesOut() {
        return replicationBytesOut;
    }

    public Counter delayedWriteExpireCount() {
        return delayedWriteExpireCount;
    }

    public Counter delayedFetchFromFollowerExpireCount() {
        return delayedFetchFromFollowerExpireCount;
    }

    public Counter delayedFetchFromClientExpireCount() {
        return delayedFetchFromClientExpireCount;
    }

    public Counter messageIn() {
        return messagesIn;
    }

    public Counter bytesIn() {
        return bytesIn;
    }

    public Counter bytesOut() {
        return bytesOut;
    }

    public Counter logFlushCount() {
        return logFlushCount;
    }

    public Histogram logFlushLatencyHistogram() {
        return logFlushLatencyHistogram;
    }

    public Counter kvFlushCount() {
        return kvFlushCount;
    }

    public Histogram kvFlushLatencyHistogram() {
        return kvFlushLatencyHistogram;
    }

    public Counter kvTruncateAsDuplicatedCount() {
        return kvTruncateAsDuplicatedCount;
    }

    public Counter kvTruncateAsErrorCount() {
        return kvTruncateAsErrorCount;
    }

    public Counter isrShrinks() {
        return isrShrinks;
    }

    public Counter isrExpands() {
        return isrExpands;
    }

    public Counter failedIsrUpdates() {
        return failedIsrUpdates;
    }

    // ------------------------------------------------------------------------
    //  RocksDB aggregated metrics getters
    // ------------------------------------------------------------------------

    // Counter type (SUM aggregation)
    public Counter rocksdbBlockCacheMissCount() {
        return rocksdbBlockCacheMissCount;
    }

    public Counter rocksdbBlockCacheHitCount() {
        return rocksdbBlockCacheHitCount;
    }

    public Counter rocksdbBlockCacheAddCount() {
        return rocksdbBlockCacheAddCount;
    }

    public Counter rocksdbCompactionBytesRead() {
        return rocksdbCompactionBytesRead;
    }

    public Counter rocksdbCompactionBytesWritten() {
        return rocksdbCompactionBytesWritten;
    }

    public Counter rocksdbFlushBytesWritten() {
        return rocksdbFlushBytesWritten;
    }

    public Counter rocksdbBytesRead() {
        return rocksdbBytesRead;
    }

    public Counter rocksdbBytesWritten() {
        return rocksdbBytesWritten;
    }

    // Gauge type SUM (resource usage)
    public AtomicLong rocksdbBlockCacheUsage() {
        return rocksdbBlockCacheUsage;
    }

    public AtomicLong rocksdbBlockCachePinnedUsage() {
        return rocksdbBlockCachePinnedUsage;
    }

    public AtomicLong rocksdbMemtableMemoryUsage() {
        return rocksdbMemtableMemoryUsage;
    }

    public AtomicLong rocksdbBlockCacheMemoryUsage() {
        return rocksdbBlockCacheMemoryUsage;
    }

    public AtomicLong rocksdbTableReadersMemoryUsage() {
        return rocksdbTableReadersMemoryUsage;
    }

    public AtomicLong rocksdbTotalMemoryUsage() {
        return rocksdbTotalMemoryUsage;
    }

    public AtomicLong rocksdbTotalSstFilesSize() {
        return rocksdbTotalSstFilesSize;
    }

    // Gauge type AVG (count metrics)
    public AtomicLong rocksdbCompactionPendingSum() {
        return rocksdbCompactionPendingSum;
    }

    public AtomicLong rocksdbFlushPendingSum() {
        return rocksdbFlushPendingSum;
    }

    public AtomicLong rocksdbNumFilesAtLevel0Sum() {
        return rocksdbNumFilesAtLevel0Sum;
    }

    public AtomicLong rocksdbCollectorCount() {
        return rocksdbCollectorCount;
    }

    // Gauge type MAX (time metrics)
    public AtomicLong rocksdbWriteStallMicros() {
        return rocksdbWriteStallMicros;
    }

    // Histogram type AVG (latency metrics)
    public Histogram rocksdbDbGetLatencyHistogram() {
        return rocksdbDbGetLatencyHistogram;
    }

    public Histogram rocksdbDbWriteLatencyHistogram() {
        return rocksdbDbWriteLatencyHistogram;
    }

    public Histogram rocksdbCompactionTimeHistogram() {
        return rocksdbCompactionTimeHistogram;
    }

    public AtomicLong rocksdbLatencyCollectorCount() {
        return rocksdbLatencyCollectorCount;
    }

    // ------------------------------------------------------------------------
    //  table buckets groups
    // ------------------------------------------------------------------------
    public BucketMetricGroup addTableBucketMetricGroup(
            PhysicalTablePath physicalTablePath, TableBucket bucket, boolean isKvTable) {
        TablePath tablePath = physicalTablePath.getTablePath();
        TableMetricGroup tableMetricGroup =
                metricGroupByTable.computeIfAbsent(
                        tablePath,
                        table -> new TableMetricGroup(registry, tablePath, isKvTable, this));
        return tableMetricGroup.addBucketMetricGroup(physicalTablePath.getPartitionName(), bucket);
    }

    public void removeTableBucketMetricGroup(TablePath tablePath, TableBucket bucket) {
        // get the metric group of the table
        TableMetricGroup tableMetricGroup = metricGroupByTable.get(tablePath);
        // if get the table metric group
        if (tableMetricGroup != null) {
            // remove the bucket metric group
            tableMetricGroup.removeBucketMetricGroup(bucket);
            // if no any bucket groups remain in the physical table metrics group,
            // close and remove the physical table metric group
            if (tableMetricGroup.bucketGroupsCount() == 0) {
                tableMetricGroup.close();
                metricGroupByTable.remove(tablePath);
            }
        }
    }
}

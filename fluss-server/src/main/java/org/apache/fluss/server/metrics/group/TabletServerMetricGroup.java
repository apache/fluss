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
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.metrics.SimpleCounter;
import org.apache.fluss.metrics.ThreadSafeSimpleCounter;
import org.apache.fluss.metrics.groups.AbstractMetricGroup;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.server.kv.rocksdb.RocksDBStatistics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** The metric group for tablet server. */
public class TabletServerMetricGroup extends AbstractMetricGroup {

    private static final String NAME = "tabletserver";
    private static final int WINDOW_SIZE = 1024;

    private final Map<TablePath, TableMetricGroup> metricGroupByTable = new ConcurrentHashMap<>();

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

    /** Suppliers for shared block cache usage, set by KvManager when shared cache is enabled. */
    private volatile LongSupplier sharedBlockCacheUsageSupplier = () -> 0L;

    private volatile LongSupplier sharedBlockCachePinnedUsageSupplier = () -> 0L;
    private volatile long sharedBlockCacheCapacity;
    /** Supplier for shared write buffer usage, set by KvManager. */
    private volatile LongSupplier sharedWriteBufferUsageSupplier = () -> 0L;

    private volatile long sharedWriteBufferCapacity;
    // aggregated index replication metrics
    private final Counter indexReplicationSourceBytes;
    private final Counter indexReplicationCompletedBytes;
    private final Counter indexReplicationRetries;
    private final Counter indexReplicationFailures;
    private final Counter indexPushStaleProgressBatches;
    private final Counter indexPushTombstoneNoOpBatches;
    private final Histogram indexReplicationRequestLatencyHistogram;
    private final AtomicReference<IndexReplicationGaugeSource> indexReplicationGaugeSource =
            new AtomicReference<>(IndexReplicationGaugeSource.EMPTY);

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

        // index replication metrics
        indexReplicationSourceBytes = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.INDEX_REPLICATION_SOURCE_BYTES_RATE,
                new MeterView(indexReplicationSourceBytes));
        indexReplicationCompletedBytes = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.INDEX_REPLICATION_COMPLETED_BYTES_RATE,
                new MeterView(indexReplicationCompletedBytes));
        indexReplicationRetries = new ThreadSafeSimpleCounter();
        meter(MetricNames.INDEX_REPLICATION_RETRIES_RATE, new MeterView(indexReplicationRetries));
        indexReplicationFailures = new ThreadSafeSimpleCounter();
        meter(MetricNames.INDEX_REPLICATION_FAILURES_RATE, new MeterView(indexReplicationFailures));
        indexPushStaleProgressBatches = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.INDEX_PUSH_STALE_PROGRESS_BATCHES_RATE,
                new MeterView(indexPushStaleProgressBatches));
        indexPushTombstoneNoOpBatches = new ThreadSafeSimpleCounter();
        meter(
                MetricNames.INDEX_PUSH_TOMBSTONE_NO_OP_BATCHES_RATE,
                new MeterView(indexPushTombstoneNoOpBatches));
        indexReplicationRequestLatencyHistogram = new DescriptiveStatisticsHistogram(WINDOW_SIZE);
        histogram(
                MetricNames.INDEX_REPLICATION_REQUEST_LATENCY_MS,
                indexReplicationRequestLatencyHistogram);
        gauge(
                MetricNames.INDEX_REPLICATION_PENDING_BYTES,
                () -> indexReplicationGaugeSource.get().pendingBytesSupplier.getAsLong());
        gauge(
                MetricNames.INDEX_REPLICATION_MAX_NO_PROGRESS_TIME_MS,
                () -> indexReplicationGaugeSource.get().maxNoProgressTimeMsSupplier.getAsLong());
        gauge(
                MetricNames.INDEX_REPLICATION_FAILED_SOURCE_BUCKET_COUNT,
                () ->
                        indexReplicationGaugeSource
                                .get()
                                .failedSourceBucketCountSupplier
                                .getAsLong());

        // Register server-level RocksDB aggregated metrics
        registerServerRocksDBMetrics();
    }

    /**
     * Register server-level RocksDB aggregated metrics. These metrics aggregate memory usage from
     * all tables.
     */
    private void registerServerRocksDBMetrics() {
        // Total memory usage across all RocksDB instances in this server.
        // When shared block cache is enabled, per-tablet stats exclude block cache,
        // so we add the shared block cache usage once separately.
        gauge(
                MetricNames.ROCKSDB_MEMORY_USAGE_TOTAL,
                () ->
                        metricGroupByTable.values().stream()
                                        .flatMap(TableMetricGroup::allRocksDBStatistics)
                                        .mapToLong(RocksDBStatistics::getTotalMemoryUsage)
                                        .sum()
                                + sharedBlockCacheUsageSupplier.getAsLong());
        gauge(
                MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_USAGE,
                () -> sharedBlockCacheUsageSupplier.getAsLong());
        gauge(MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_CAPACITY, () -> sharedBlockCacheCapacity);
        gauge(
                MetricNames.ROCKSDB_SHARED_BLOCK_CACHE_PINNED_USAGE,
                () -> sharedBlockCachePinnedUsageSupplier.getAsLong());
        gauge(
                MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_USAGE,
                () -> sharedWriteBufferUsageSupplier.getAsLong());
        gauge(MetricNames.ROCKSDB_SHARED_WRITE_BUFFER_CAPACITY, () -> sharedWriteBufferCapacity);
    }

    /**
     * Sets the shared block cache metrics. Called by KvManager when shared block cache is enabled.
     *
     * @param usageSupplier supplier for current cache usage
     * @param pinnedUsageSupplier supplier for current pinned cache usage
     * @param capacity configured cache capacity in bytes
     */
    public void setSharedBlockCacheMetrics(
            LongSupplier usageSupplier, LongSupplier pinnedUsageSupplier, long capacity) {
        LongSupplier checkedUsageSupplier =
                checkNotNull(usageSupplier, "usageSupplier must not be null");
        LongSupplier checkedPinnedUsageSupplier =
                checkNotNull(pinnedUsageSupplier, "pinnedUsageSupplier must not be null");
        this.sharedBlockCacheUsageSupplier = checkedUsageSupplier;
        this.sharedBlockCachePinnedUsageSupplier = checkedPinnedUsageSupplier;
        this.sharedBlockCacheCapacity = capacity;
    }

    /**
     * Sets the shared write buffer metrics.
     *
     * <p>The usage is RocksDB's approximate logical charge for memtable allocations. It is not a
     * process RSS measurement or a hard memory bound.
     *
     * @param usageSupplier supplier for current shared write buffer usage
     * @param capacity configured soft capacity in bytes, or 0 when disabled
     */
    public void setSharedWriteBufferMetrics(LongSupplier usageSupplier, long capacity) {
        this.sharedWriteBufferUsageSupplier =
                checkNotNull(usageSupplier, "usageSupplier must not be null");
        this.sharedWriteBufferCapacity = capacity;
    }

    /**
     * Registers gauges for the server-wide WAL memory pool used by primary key tables. Called once
     * by KvManager when creating the server buffer pool.
     *
     * @param usageSupplier supplier for the bytes currently allocated from the pool
     * @param capacity the total pool capacity in bytes
     */
    public void registerKvWalMemoryPoolMetrics(LongSupplier usageSupplier, long capacity) {
        gauge(MetricNames.KV_WAL_MEMORY_POOL_USAGE, usageSupplier::getAsLong);
        gauge(MetricNames.KV_WAL_MEMORY_POOL_CAPACITY, () -> capacity);
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

    public Counter indexReplicationSourceBytes() {
        return indexReplicationSourceBytes;
    }

    public Counter indexReplicationCompletedBytes() {
        return indexReplicationCompletedBytes;
    }

    public Counter indexReplicationRetries() {
        return indexReplicationRetries;
    }

    public Counter indexReplicationFailures() {
        return indexReplicationFailures;
    }

    public Counter indexPushStaleProgressBatches() {
        return indexPushStaleProgressBatches;
    }

    public Counter indexPushTombstoneNoOpBatches() {
        return indexPushTombstoneNoOpBatches;
    }

    public Histogram indexReplicationRequestLatencyHistogram() {
        return indexReplicationRequestLatencyHistogram;
    }

    public GaugeRegistration registerIndexReplicationGauges(
            LongSupplier pendingBytesSupplier,
            LongSupplier maxNoProgressTimeMsSupplier,
            LongSupplier failedSourceBucketCountSupplier) {
        IndexReplicationGaugeSource source =
                new IndexReplicationGaugeSource(
                        pendingBytesSupplier,
                        maxNoProgressTimeMsSupplier,
                        failedSourceBucketCountSupplier);
        indexReplicationGaugeSource.set(source);
        return () ->
                indexReplicationGaugeSource.compareAndSet(
                        source, IndexReplicationGaugeSource.EMPTY);
    }

    /** Scoped ownership of replaceable server-level gauge suppliers. */
    @FunctionalInterface
    public interface GaugeRegistration extends AutoCloseable {
        @Override
        void close();
    }

    private static final class IndexReplicationGaugeSource {
        private static final IndexReplicationGaugeSource EMPTY =
                new IndexReplicationGaugeSource(() -> 0L, () -> 0L, () -> 0L);

        private final LongSupplier pendingBytesSupplier;
        private final LongSupplier maxNoProgressTimeMsSupplier;
        private final LongSupplier failedSourceBucketCountSupplier;

        private IndexReplicationGaugeSource(
                LongSupplier pendingBytesSupplier,
                LongSupplier maxNoProgressTimeMsSupplier,
                LongSupplier failedSourceBucketCountSupplier) {
            this.pendingBytesSupplier = pendingBytesSupplier;
            this.maxNoProgressTimeMsSupplier = maxNoProgressTimeMsSupplier;
            this.failedSourceBucketCountSupplier = failedSourceBucketCountSupplier;
        }
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

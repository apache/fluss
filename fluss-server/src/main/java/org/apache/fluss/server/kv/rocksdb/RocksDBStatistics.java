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

import org.apache.fluss.server.utils.ResourceGuard;

import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.MemoryUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Collects and provides access to RocksDB statistics for a single KvTablet.
 *
 * <p>This class encapsulates low-level RocksDB statistics collection, providing semantic methods to
 * access various RocksDB statistics and properties. It does NOT register Fluss metrics directly;
 * instead, upper layers (e.g., TableMetricGroup) consume these statistics to compute and register
 * actual Fluss Metrics.
 *
 * <p>Thread-safety: This class uses RocksDB's ResourceGuard to ensure safe concurrent access. All
 * statistics read operations acquire the resource guard to prevent accessing closed RocksDB
 * instances.
 */
public class RocksDBStatistics implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStatistics.class);

    private final RocksDB db;
    @Nullable private final Statistics statistics;
    private final ResourceGuard resourceGuard;
    private final ColumnFamilyHandle defaultColumnFamilyHandle;
    @Nullable private final Cache blockCache;

    public RocksDBStatistics(
            RocksDB db,
            @Nullable Statistics statistics,
            ResourceGuard resourceGuard,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            @Nullable Cache blockCache) {
        this.db = db;
        this.statistics = statistics;
        this.resourceGuard = resourceGuard;
        this.defaultColumnFamilyHandle = defaultColumnFamilyHandle;
        this.blockCache = blockCache;
    }

    // ==================== Ticker-based Metrics ====================

    /**
     * Get write stall duration in microseconds.
     *
     * @return write stall duration, or 0 if not available
     */
    public long getWriteStallMicros() {
        return getTickerValue(TickerType.STALL_MICROS);
    }

    /**
     * Get total bytes read.
     *
     * @return bytes read, or 0 if not available
     */
    public long getBytesRead() {
        return getTickerValue(TickerType.BYTES_READ);
    }

    /**
     * Get total bytes written.
     *
     * @return bytes written, or 0 if not available
     */
    public long getBytesWritten() {
        return getTickerValue(TickerType.BYTES_WRITTEN);
    }

    /**
     * Get flush bytes written.
     *
     * @return flush bytes written, or 0 if not available
     */
    public long getFlushBytesWritten() {
        return getTickerValue(TickerType.FLUSH_WRITE_BYTES);
    }

    /**
     * Get compaction bytes read.
     *
     * @return compaction bytes read, or 0 if not available
     */
    public long getCompactionBytesRead() {
        return getTickerValue(TickerType.COMPACT_READ_BYTES);
    }

    /**
     * Get compaction bytes written.
     *
     * @return compaction bytes written, or 0 if not available
     */
    public long getCompactionBytesWritten() {
        return getTickerValue(TickerType.COMPACT_WRITE_BYTES);
    }

    // ==================== Property-based Metrics ====================

    /**
     * Get get operation latency in microseconds (P99).
     *
     * <p>This uses RocksDB Statistics histogram data to get the P99 latency of get operations. P99
     * is used instead of average because it better reflects tail latency issues, which are more
     * critical for monitoring database performance.
     *
     * @return P99 get latency in microseconds, or 0 if not available
     */
    public long getGetLatencyMicros() {
        return getHistogramValue(HistogramType.DB_GET);
    }

    /**
     * Get write operation latency in microseconds (P99).
     *
     * <p>This uses RocksDB Statistics histogram data to get the P99 latency of write operations.
     * P99 is used instead of average because it better reflects tail latency issues, which are more
     * critical for monitoring database performance.
     *
     * @return P99 write latency in microseconds, or 0 if not available
     */
    public long getWriteLatencyMicros() {
        return getHistogramValue(HistogramType.DB_WRITE);
    }

    /**
     * Get number of files at level 0.
     *
     * <p>This property is column family specific and must be accessed through the column family
     * handle.
     *
     * @return number of L0 files, or 0 if not available
     */
    public long getNumFilesAtLevel0() {
        return getPropertyValue(defaultColumnFamilyHandle, "rocksdb.num-files-at-level0");
    }

    /**
     * Get whether a memtable flush is pending.
     *
     * @return 1 if flush is pending, 0 otherwise
     */
    public long getFlushPending() {
        return getPropertyValue("rocksdb.mem-table-flush-pending");
    }

    /**
     * Get whether a compaction is pending.
     *
     * @return 1 if compaction is pending, 0 otherwise
     */
    public long getCompactionPending() {
        return getPropertyValue("rocksdb.compaction-pending");
    }

    /**
     * Get compaction time in microseconds (P99).
     *
     * <p>This uses RocksDB Statistics histogram data to get the P99 compaction time. P99 is used
     * instead of average because it better reflects tail latency issues in compaction operations.
     *
     * @return P99 compaction time in microseconds, or 0 if not available
     */
    public long getCompactionTimeMicros() {
        return getHistogramValue(HistogramType.COMPACTION_TIME);
    }

    /**
     * Get total memory usage across all RocksDB components including block cache, memtables,
     * indexes, filters, etc.
     *
     * <p>This uses RocksDB MemoryUtil to get approximate memory usage by type and sums all types.
     * This includes:
     *
     * <ul>
     *   <li>Block cache usage (if explicit cache is provided)
     *   <li>All memtables (active and immutable)
     *   <li>Table readers (indexes and bloom filters)
     *   <li>Pinned blocks
     * </ul>
     *
     * <p>Note: To get accurate block cache memory usage, an explicit Cache object must be provided
     * during construction. If no cache is provided (null), the block cache memory usage may not be
     * fully accounted for.
     *
     * @return total memory usage in bytes, or 0 if not available
     */
    public long getTotalMemoryUsage() {
        try (ResourceGuard.Lease lease = resourceGuard.acquireResource()) {
            if (db == null) {
                return 0L;
            }

            // Create cache set for memory usage calculation.
            // If blockCache is null, pass null to MemoryUtil (will only count memtables, etc.)
            Set<Cache> caches = null;
            if (blockCache != null) {
                caches = new HashSet<>();
                caches.add(blockCache);
            }

            Map<MemoryUsageType, Long> memoryUsage =
                    MemoryUtil.getApproximateMemoryUsageByType(
                            Collections.singletonList(db), caches);
            return memoryUsage.values().stream().mapToLong(Long::longValue).sum();
        } catch (Exception e) {
            LOG.debug(
                    "Failed to get total memory usage from RocksDB (possibly closed or unavailable)",
                    e);
            return 0L;
        }
    }

    // ==================== Internal Helper Methods ====================

    /**
     * Get ticker value from RocksDB Statistics with resource guard protection.
     *
     * @param tickerType the ticker type to query
     * @return the ticker value, or 0 if not available or RocksDB is closed
     */
    private long getTickerValue(TickerType tickerType) {
        try (ResourceGuard.Lease lease = resourceGuard.acquireResource()) {
            if (statistics != null) {
                return statistics.getTickerCount(tickerType);
            }
        } catch (Exception e) {
            LOG.debug(
                    "Failed to get ticker {} from RocksDB (possibly closed or unavailable)",
                    tickerType,
                    e);
        }
        return 0L;
    }

    /**
     * Get histogram P99 value from RocksDB Statistics with resource guard protection.
     *
     * <p>Histograms are used for latency metrics and provide average, median, percentile values.
     * This method returns the P99 value (99th percentile) instead of average, which better reflects
     * tail latency and is more useful for performance monitoring. For microsecond-level latencies,
     * we round to the nearest long value to avoid precision loss where it matters.
     *
     * <p>Why P99 instead of average:
     *
     * <ul>
     *   <li>P99 captures tail latency issues that average would hide
     *   <li>More aligned with industry best practices for latency monitoring
     *   <li>Better indicator of user-facing performance problems
     * </ul>
     *
     * @param histogramType the histogram type to query
     * @return the P99 histogram value (rounded to nearest long), or 0 if not available or RocksDB
     *     is closed
     */
    private long getHistogramValue(HistogramType histogramType) {
        try (ResourceGuard.Lease lease = resourceGuard.acquireResource()) {
            if (statistics != null) {
                HistogramData histogramData = statistics.getHistogramData(histogramType);
                if (histogramData != null) {
                    // Use P99 instead of average for better tail latency monitoring
                    // Round to nearest long to preserve precision for microsecond-level values
                    return Math.round(histogramData.getPercentile99());
                }
            }
        } catch (Exception e) {
            LOG.debug(
                    "Failed to get histogram {} from RocksDB Statistics (possibly closed or unavailable)",
                    histogramType,
                    e);
        }
        return 0L;
    }

    /**
     * Get property value from RocksDB with resource guard protection.
     *
     * @param propertyName the property name to query
     * @return the property value as long, or 0 if not available or RocksDB is closed
     */
    private long getPropertyValue(String propertyName) {
        try (ResourceGuard.Lease lease = resourceGuard.acquireResource()) {
            String value = db.getProperty(propertyName);
            if (value != null && !value.isEmpty()) {
                return Long.parseLong(value);
            }
        } catch (RocksDBException e) {
            LOG.debug(
                    "Failed to get property {} from RocksDB (possibly closed or unavailable)",
                    propertyName,
                    e);
        } catch (NumberFormatException e) {
            LOG.debug("Failed to parse property {} value as long", propertyName, e);
        } catch (Exception e) {
            // ResourceGuard may throw exception if RocksDB is closed
            LOG.debug(
                    "Failed to access RocksDB for property {} (possibly closed)", propertyName, e);
        }
        return 0L;
    }

    /**
     * Get property value from RocksDB for a specific column family with resource guard protection.
     *
     * <p>Some RocksDB properties are column family specific and must be accessed through the column
     * family handle.
     *
     * @param columnFamilyHandle the column family handle
     * @param propertyName the property name to query
     * @return the property value as long, or 0 if not available or RocksDB is closed
     */
    private long getPropertyValue(ColumnFamilyHandle columnFamilyHandle, String propertyName) {
        try (ResourceGuard.Lease lease = resourceGuard.acquireResource()) {
            if (columnFamilyHandle == null) {
                return 0L;
            }
            String value = db.getProperty(columnFamilyHandle, propertyName);
            if (value != null && !value.isEmpty()) {
                return Long.parseLong(value);
            }
        } catch (RocksDBException e) {
            LOG.debug(
                    "Failed to get property {} from RocksDB column family (possibly closed or unavailable)",
                    propertyName,
                    e);
        } catch (NumberFormatException e) {
            LOG.debug("Failed to parse property {} value as long", propertyName, e);
        } catch (Exception e) {
            // ResourceGuard may throw exception if RocksDB is closed
            LOG.debug(
                    "Failed to access RocksDB for property {} (possibly closed)", propertyName, e);
        }
        return 0L;
    }

    @Override
    public void close() {
        // No resources to clean up, statistics are managed by TableMetricGroup
        LOG.debug("RocksDB statistics accessor closed");
    }
}

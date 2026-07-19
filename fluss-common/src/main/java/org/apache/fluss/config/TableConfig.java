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

package org.apache.fluss.config;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.utils.AutoPartitionStrategy;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Helper class to get table configs (prefixed with "table.*" properties).
 *
 * @since 0.6
 */
@PublicEvolving
public class TableConfig {

    // the table properties configuration
    private final Configuration config;

    /**
     * Creates a new table config.
     *
     * @param config the table properties configuration
     */
    public TableConfig(Configuration config) {
        this.config = config;
    }

    /** Returns whether the table config contains the given option. */
    public boolean contains(ConfigOption<?> option) {
        return config.contains(option);
    }

    /** Gets the log segment file size of the table. */
    public MemorySize getLogSegmentSize() {
        return config.get(ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE);
    }

    /** Gets the log index file size of the table. */
    public MemorySize getLogIndexFileSize() {
        return config.get(ConfigOptions.TABLE_LOG_INDEX_FILE_SIZE);
    }

    /** Gets the RocksDB write batch size of the table. */
    public MemorySize getKvWriteBatchSize() {
        return config.get(ConfigOptions.TABLE_KV_WRITE_BATCH_SIZE);
    }

    /** Gets the RocksDB background thread count of the table. */
    public int getKvMaxBackgroundThreads() {
        return config.get(ConfigOptions.TABLE_KV_MAX_BACKGROUND_THREADS);
    }

    /** Gets the RocksDB write buffer size of the table. */
    public MemorySize getKvWriteBufferSize() {
        return config.get(ConfigOptions.TABLE_KV_WRITE_BUFFER_SIZE);
    }

    /** Gets the RocksDB write buffer count of the table. */
    public int getKvMaxWriteBufferNumber() {
        return config.get(ConfigOptions.TABLE_KV_MAX_WRITE_BUFFER_NUMBER);
    }

    /** Gets the RocksDB maximum number of open files of the table. */
    public int getKvMaxOpenFiles() {
        return config.get(ConfigOptions.TABLE_KV_MAX_OPEN_FILES);
    }

    /** Gets the RocksDB information log maximum file size of the table. */
    public MemorySize getKvLogMaxFileSize() {
        return config.get(ConfigOptions.TABLE_KV_LOG_MAX_FILE_SIZE);
    }

    /** Gets the RocksDB information log file count of the table. */
    public int getKvLogFileNum() {
        return config.get(ConfigOptions.TABLE_KV_LOG_FILE_NUM);
    }

    /** Gets the RocksDB information log directory of the table. */
    public String getKvLogDir() {
        return config.get(ConfigOptions.TABLE_KV_LOG_DIR);
    }

    /** Gets the RocksDB information log level of the table. */
    public ConfigOptions.InfoLogLevel getKvLogLevel() {
        return config.get(ConfigOptions.TABLE_KV_LOG_LEVEL);
    }

    /** Gets the RocksDB compaction style of the table. */
    public ConfigOptions.CompactionStyle getKvCompactionStyle() {
        return config.get(ConfigOptions.TABLE_KV_COMPACTION_STYLE);
    }

    /** Returns whether RocksDB dynamic level sizing is enabled for the table. */
    public boolean isKvDynamicLevelSizeEnabled() {
        return config.get(ConfigOptions.TABLE_KV_USE_DYNAMIC_LEVEL_SIZE);
    }

    /** Gets the RocksDB compression types per level of the table. */
    public List<ConfigOptions.KvCompressionType> getKvCompressionPerLevel() {
        return config.get(ConfigOptions.TABLE_KV_COMPRESSION_PER_LEVEL);
    }

    /** Gets the RocksDB target file size base of the table. */
    public MemorySize getKvTargetFileSizeBase() {
        return config.get(ConfigOptions.TABLE_KV_TARGET_FILE_SIZE_BASE);
    }

    /** Gets the RocksDB maximum size of the base level of the table. */
    public MemorySize getKvMaxSizeLevelBase() {
        return config.get(ConfigOptions.TABLE_KV_MAX_SIZE_LEVEL_BASE);
    }

    /** Gets the minimum number of RocksDB write buffers to merge for the table. */
    public int getKvMinWriteBufferNumberToMerge() {
        return config.get(ConfigOptions.TABLE_KV_MIN_WRITE_BUFFER_NUMBER_TO_MERGE);
    }

    /** Gets the RocksDB data block size of the table. */
    public MemorySize getKvBlockSize() {
        return config.get(ConfigOptions.TABLE_KV_BLOCK_SIZE);
    }

    /** Gets the RocksDB metadata block size of the table. */
    public MemorySize getKvMetadataBlockSize() {
        return config.get(ConfigOptions.TABLE_KV_METADATA_BLOCK_SIZE);
    }

    /** Gets the RocksDB block cache size of the table. */
    public MemorySize getKvBlockCacheSize() {
        return config.get(ConfigOptions.TABLE_KV_BLOCK_CACHE_SIZE);
    }

    /** Returns whether RocksDB Bloom filters are enabled for the table. */
    public boolean isKvBloomFilterEnabled() {
        return config.get(ConfigOptions.TABLE_KV_USE_BLOOM_FILTER);
    }

    /** Gets the number of RocksDB Bloom filter bits per key of the table. */
    public double getKvBloomFilterBitsPerKey() {
        return config.get(ConfigOptions.TABLE_KV_BLOOM_FILTER_BITS_PER_KEY);
    }

    /** Returns whether RocksDB uses block-based Bloom filters for the table. */
    public boolean isKvBloomFilterBlockBasedMode() {
        return config.get(ConfigOptions.TABLE_KV_BLOOM_FILTER_BLOCK_BASED_MODE);
    }

    /** Returns whether RocksDB caches index and filter blocks for the table. */
    public boolean isKvCacheIndexAndFilterBlocks() {
        return config.get(ConfigOptions.TABLE_KV_CACHE_INDEX_AND_FILTER_BLOCKS);
    }

    /** Returns whether RocksDB caches index and filter blocks with high priority for the table. */
    public boolean isKvCacheIndexAndFilterBlocksWithHighPriority() {
        return config.get(ConfigOptions.TABLE_KV_CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY);
    }

    /** Returns whether RocksDB pins L0 filter and index blocks for the table. */
    public boolean isKvPinL0FilterAndIndexBlocksInCache() {
        return config.get(ConfigOptions.TABLE_KV_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE);
    }

    /** Returns whether RocksDB pins the top-level index and filter for the table. */
    public boolean isKvPinTopLevelIndexAndFilter() {
        return config.get(ConfigOptions.TABLE_KV_PIN_TOP_LEVEL_INDEX_AND_FILTER);
    }

    /** Gets the replication factor of the table. */
    public int getReplicationFactor() {
        return config.get(ConfigOptions.TABLE_REPLICATION_FACTOR);
    }

    /** Gets the log format of the table. */
    public LogFormat getLogFormat() {
        return config.get(ConfigOptions.TABLE_LOG_FORMAT);
    }

    /** Gets the kv format of the table. */
    public KvFormat getKvFormat() {
        return config.get(ConfigOptions.TABLE_KV_FORMAT);
    }

    /**
     * Gets the kv format version of the table. This is used for backward compatibility when
     * encoding strategy changes. Returns empty if the table was created before the version was
     * introduced (old tables).
     */
    public Optional<Integer> getKvFormatVersion() {
        return config.getOptional(ConfigOptions.TABLE_KV_FORMAT_VERSION);
    }

    /**
     * Whether standby replicas are enabled for this primary key table. Returns false for legacy
     * tables that were created before this option was introduced.
     */
    public boolean isStandbyReplicaEnabled() {
        return config.getOptional(ConfigOptions.TABLE_KV_STANDBY_REPLICA_ENABLED).orElse(false);
    }

    /** Gets the log TTL of the table. */
    public long getLogTTLMs() {
        return config.get(ConfigOptions.TABLE_LOG_TTL).toMillis();
    }

    /** Gets the local segments to retain for tiered log of the table. */
    public int getTieredLogLocalSegments() {
        return config.get(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS);
    }

    /** Whether the data lake is enabled. */
    public boolean isDataLakeEnabled() {
        return config.get(ConfigOptions.TABLE_DATALAKE_ENABLED);
    }

    /**
     * Return the data lake format of the table. It'll be the datalake format configured in Fluss
     * whiling creating the table. Return empty if no datalake format configured while creating.
     */
    public Optional<DataLakeFormat> getDataLakeFormat() {
        return config.getOptional(ConfigOptions.TABLE_DATALAKE_FORMAT);
    }

    /**
     * Gets the data lake freshness of the table. It defines the maximum amount of time that the
     * datalake table's content should lag behind updates to the Fluss table.
     */
    public Duration getDataLakeFreshness() {
        return config.get(ConfigOptions.TABLE_DATALAKE_FRESHNESS);
    }

    /** Whether auto compaction is enabled. */
    public boolean isDataLakeAutoCompaction() {
        return config.get(ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION);
    }

    /** Whether auto expire snapshot is enabled. */
    public boolean isDataLakeAutoExpireSnapshot() {
        return config.get(ConfigOptions.TABLE_DATALAKE_AUTO_EXPIRE_SNAPSHOT);
    }

    /** Gets the optional merge engine type of the table. */
    public Optional<MergeEngineType> getMergeEngineType() {
        return config.getOptional(ConfigOptions.TABLE_MERGE_ENGINE);
    }

    /**
     * Gets the optional {@link MergeEngineType#VERSIONED} merge engine version column of the table.
     */
    public Optional<String> getMergeEngineVersionColumn() {
        return config.getOptional(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN);
    }

    /** Gets the delete behavior of the table. */
    public Optional<DeleteBehavior> getDeleteBehavior() {
        return config.getOptional(ConfigOptions.TABLE_DELETE_BEHAVIOR);
    }

    /**
     * Gets the changelog image mode of the table. The changelog image mode defines what information
     * is included in the changelog for update operations.
     */
    public ChangelogImage getChangelogImage() {
        return config.get(ConfigOptions.TABLE_CHANGELOG_IMAGE);
    }

    /** Gets the Arrow compression type and compression level of the table. */
    public ArrowCompressionInfo getArrowCompressionInfo() {
        return ArrowCompressionInfo.fromConf(config);
    }

    /** Gets the auto partition strategy of the table. */
    public AutoPartitionStrategy getAutoPartitionStrategy() {
        return AutoPartitionStrategy.from(config);
    }

    /** Gets the number of auto-increment IDs cached per segment. */
    public long getAutoIncrementCacheSize() {
        return config.get(ConfigOptions.TABLE_AUTO_INCREMENT_CACHE_SIZE);
    }

    /** Gets whether statistics collection is enabled for the table. */
    public boolean isStatisticsEnabled() {
        return getStatisticsColumns().isEnabled();
    }

    /**
     * Gets the statistics columns configuration of the table.
     *
     * @return a {@link StatisticsColumnsConfig} representing the statistics collection mode:
     *     DISABLED if not configured, ALL if "*", or SPECIFIED with the list of column names.
     */
    public StatisticsColumnsConfig getStatisticsColumns() {
        String columnsStr = config.get(ConfigOptions.TABLE_STATISTICS_COLUMNS);
        if (columnsStr == null) {
            return StatisticsColumnsConfig.disabled();
        }
        if ("*".equals(columnsStr)) {
            return StatisticsColumnsConfig.all();
        }
        List<String> columns =
                Arrays.stream(columnsStr.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
        return StatisticsColumnsConfig.of(columns);
    }
}

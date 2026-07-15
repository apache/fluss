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

package org.apache.fluss.server.config;

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.TableConfig;

import javax.annotation.Nullable;

import java.util.List;

/** Server-side table config that falls back to server configuration for runtime options. */
public final class ResolvedTableConfig extends TableConfig {

    private final Configuration tableConfig;
    private final Configuration serverConfig;

    /**
     * Creates a resolved table config.
     *
     * @param tableConfig the table properties configuration
     * @param serverConfig the server configuration to fall back to for runtime options
     */
    public ResolvedTableConfig(Configuration tableConfig, @Nullable Configuration serverConfig) {
        super(tableConfig);
        this.tableConfig = tableConfig;
        this.serverConfig = serverConfig == null ? new Configuration() : serverConfig;
    }

    /** Gets the log segment file size resolved from table properties or server configuration. */
    @Override
    public MemorySize getLogSegmentSize() {
        return getResolved(
                ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE, ConfigOptions.LOG_SEGMENT_FILE_SIZE);
    }

    /** Gets the log index file size resolved from table properties or server configuration. */
    @Override
    public MemorySize getLogIndexFileSize() {
        return getResolved(
                ConfigOptions.TABLE_LOG_INDEX_FILE_SIZE, ConfigOptions.LOG_INDEX_FILE_SIZE);
    }

    /** Gets the RocksDB write batch size resolved from table properties or server configuration. */
    @Override
    public MemorySize getKvWriteBatchSize() {
        return getResolved(
                ConfigOptions.TABLE_KV_WRITE_BATCH_SIZE, ConfigOptions.KV_WRITE_BATCH_SIZE);
    }

    /** Gets the RocksDB background thread count resolved from table or server configuration. */
    @Override
    public int getKvMaxBackgroundThreads() {
        return getResolved(
                ConfigOptions.TABLE_KV_MAX_BACKGROUND_THREADS,
                ConfigOptions.KV_MAX_BACKGROUND_THREADS);
    }

    /**
     * Gets the RocksDB write buffer size resolved from table properties or server configuration.
     */
    @Override
    public MemorySize getKvWriteBufferSize() {
        return getResolved(
                ConfigOptions.TABLE_KV_WRITE_BUFFER_SIZE, ConfigOptions.KV_WRITE_BUFFER_SIZE);
    }

    /**
     * Gets the RocksDB write buffer count resolved from table properties or server configuration.
     */
    @Override
    public int getKvMaxWriteBufferNumber() {
        return getResolved(
                ConfigOptions.TABLE_KV_MAX_WRITE_BUFFER_NUMBER,
                ConfigOptions.KV_MAX_WRITE_BUFFER_NUMBER);
    }

    /** Gets the RocksDB maximum open files resolved from table or server configuration. */
    @Override
    public int getKvMaxOpenFiles() {
        return getResolved(ConfigOptions.TABLE_KV_MAX_OPEN_FILES, ConfigOptions.KV_MAX_OPEN_FILES);
    }

    /** Gets the RocksDB log maximum file size resolved from table or server configuration. */
    @Override
    public MemorySize getKvLogMaxFileSize() {
        return getResolved(
                ConfigOptions.TABLE_KV_LOG_MAX_FILE_SIZE, ConfigOptions.KV_LOG_MAX_FILE_SIZE);
    }

    /** Gets the RocksDB log file count resolved from table or server configuration. */
    @Override
    public int getKvLogFileNum() {
        return getResolved(ConfigOptions.TABLE_KV_LOG_FILE_NUM, ConfigOptions.KV_LOG_FILE_NUM);
    }

    /** Gets the RocksDB log directory resolved from table or server configuration. */
    @Override
    public String getKvLogDir() {
        return getResolved(ConfigOptions.TABLE_KV_LOG_DIR, ConfigOptions.KV_LOG_DIR);
    }

    /** Gets the RocksDB log level resolved from table or server configuration. */
    @Override
    public ConfigOptions.InfoLogLevel getKvLogLevel() {
        return getResolved(ConfigOptions.TABLE_KV_LOG_LEVEL, ConfigOptions.KV_LOG_LEVEL);
    }

    /** Gets the RocksDB compaction style resolved from table or server configuration. */
    @Override
    public ConfigOptions.CompactionStyle getKvCompactionStyle() {
        return getResolved(
                ConfigOptions.TABLE_KV_COMPACTION_STYLE, ConfigOptions.KV_COMPACTION_STYLE);
    }

    /** Returns whether RocksDB dynamic level sizing is resolved as enabled. */
    @Override
    public boolean isKvDynamicLevelSizeEnabled() {
        return getResolved(
                ConfigOptions.TABLE_KV_USE_DYNAMIC_LEVEL_SIZE,
                ConfigOptions.KV_USE_DYNAMIC_LEVEL_SIZE);
    }

    /** Gets RocksDB compression types per level resolved from table or server configuration. */
    @Override
    public List<ConfigOptions.KvCompressionType> getKvCompressionPerLevel() {
        return getResolved(
                ConfigOptions.TABLE_KV_COMPRESSION_PER_LEVEL,
                ConfigOptions.KV_COMPRESSION_PER_LEVEL);
    }

    /** Gets the RocksDB target file size base resolved from table or server configuration. */
    @Override
    public MemorySize getKvTargetFileSizeBase() {
        return getResolved(
                ConfigOptions.TABLE_KV_TARGET_FILE_SIZE_BASE,
                ConfigOptions.KV_TARGET_FILE_SIZE_BASE);
    }

    /** Gets the RocksDB maximum base-level size resolved from table or server configuration. */
    @Override
    public MemorySize getKvMaxSizeLevelBase() {
        return getResolved(
                ConfigOptions.TABLE_KV_MAX_SIZE_LEVEL_BASE, ConfigOptions.KV_MAX_SIZE_LEVEL_BASE);
    }

    /** Gets the minimum RocksDB write buffers to merge resolved from table or server config. */
    @Override
    public int getKvMinWriteBufferNumberToMerge() {
        return getResolved(
                ConfigOptions.TABLE_KV_MIN_WRITE_BUFFER_NUMBER_TO_MERGE,
                ConfigOptions.KV_MIN_WRITE_BUFFER_NUMBER_TO_MERGE);
    }

    /** Gets the RocksDB block size resolved from table or server configuration. */
    @Override
    public MemorySize getKvBlockSize() {
        return getResolved(ConfigOptions.TABLE_KV_BLOCK_SIZE, ConfigOptions.KV_BLOCK_SIZE);
    }

    /** Gets the RocksDB metadata block size resolved from table or server configuration. */
    @Override
    public MemorySize getKvMetadataBlockSize() {
        return getResolved(
                ConfigOptions.TABLE_KV_METADATA_BLOCK_SIZE, ConfigOptions.KV_METADATA_BLOCK_SIZE);
    }

    /** Gets the RocksDB block cache size resolved from table or server configuration. */
    @Override
    public MemorySize getKvBlockCacheSize() {
        return getResolved(
                ConfigOptions.TABLE_KV_BLOCK_CACHE_SIZE, ConfigOptions.KV_BLOCK_CACHE_SIZE);
    }

    /** Returns whether RocksDB Bloom filters are resolved as enabled. */
    @Override
    public boolean isKvBloomFilterEnabled() {
        return getResolved(
                ConfigOptions.TABLE_KV_USE_BLOOM_FILTER, ConfigOptions.KV_USE_BLOOM_FILTER);
    }

    /** Gets RocksDB Bloom-filter bits per key resolved from table or server configuration. */
    @Override
    public double getKvBloomFilterBitsPerKey() {
        return getResolved(
                ConfigOptions.TABLE_KV_BLOOM_FILTER_BITS_PER_KEY,
                ConfigOptions.KV_BLOOM_FILTER_BITS_PER_KEY);
    }

    /** Returns whether RocksDB block-based Bloom-filter mode is resolved as enabled. */
    @Override
    public boolean isKvBloomFilterBlockBasedMode() {
        return getResolved(
                ConfigOptions.TABLE_KV_BLOOM_FILTER_BLOCK_BASED_MODE,
                ConfigOptions.KV_BLOOM_FILTER_BLOCK_BASED_MODE);
    }

    /** Returns whether caching RocksDB index and filter blocks is resolved as enabled. */
    @Override
    public boolean isKvCacheIndexAndFilterBlocks() {
        return getResolved(
                ConfigOptions.TABLE_KV_CACHE_INDEX_AND_FILTER_BLOCKS,
                ConfigOptions.KV_CACHE_INDEX_AND_FILTER_BLOCKS);
    }

    /** Returns whether high-priority RocksDB index/filter caching is resolved as enabled. */
    @Override
    public boolean isKvCacheIndexAndFilterBlocksWithHighPriority() {
        return getResolved(
                ConfigOptions.TABLE_KV_CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY,
                ConfigOptions.KV_CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY);
    }

    /** Returns whether pinning RocksDB L0 filter/index blocks is resolved as enabled. */
    @Override
    public boolean isKvPinL0FilterAndIndexBlocksInCache() {
        return getResolved(
                ConfigOptions.TABLE_KV_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE,
                ConfigOptions.KV_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE);
    }

    /** Returns whether pinning RocksDB top-level index/filter is resolved as enabled. */
    @Override
    public boolean isKvPinTopLevelIndexAndFilter() {
        return getResolved(
                ConfigOptions.TABLE_KV_PIN_TOP_LEVEL_INDEX_AND_FILTER,
                ConfigOptions.KV_PIN_TOP_LEVEL_INDEX_AND_FILTER);
    }

    private <T> T getResolved(ConfigOption<T> tableOption, ConfigOption<T> serverOption) {
        if (tableConfig.contains(tableOption)) {
            return tableConfig.get(tableOption);
        }
        return serverConfig.get(serverOption);
    }
}

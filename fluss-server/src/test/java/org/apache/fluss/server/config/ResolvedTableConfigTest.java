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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ResolvedTableConfig}. */
class ResolvedTableConfigTest {

    @Test
    void testLogSegmentSizeFallsBackToServerConfig() {
        Configuration tableConf = new Configuration();
        Configuration serverConf = new Configuration();
        serverConf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("8kb"));
        serverConf.set(ConfigOptions.LOG_INDEX_FILE_SIZE, MemorySize.parse("2kb"));

        ResolvedTableConfig tableConfig = new ResolvedTableConfig(tableConf, serverConf);

        assertThat(tableConfig.getLogSegmentSize()).isEqualTo(MemorySize.parse("8kb"));
        assertThat(tableConfig.getLogIndexFileSize()).isEqualTo(MemorySize.parse("2kb"));

        tableConf.set(ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE, MemorySize.parse("4kb"));
        tableConf.set(ConfigOptions.TABLE_LOG_INDEX_FILE_SIZE, MemorySize.parse("1kb"));

        assertThat(tableConfig.getLogSegmentSize()).isEqualTo(MemorySize.parse("4kb"));
        assertThat(tableConfig.getLogIndexFileSize()).isEqualTo(MemorySize.parse("1kb"));
    }

    @Test
    void testKvOptionsFallBackToServerConfig() {
        Configuration tableConf = new Configuration();
        Configuration serverConf = new Configuration();
        serverConf.set(ConfigOptions.KV_WRITE_BATCH_SIZE, MemorySize.parse("16mb"));
        serverConf.set(ConfigOptions.KV_MAX_BACKGROUND_THREADS, 8);
        serverConf.set(ConfigOptions.KV_WRITE_BUFFER_SIZE, MemorySize.parse("256mb"));
        serverConf.set(ConfigOptions.KV_MAX_WRITE_BUFFER_NUMBER, 4);

        ResolvedTableConfig tableConfig = new ResolvedTableConfig(tableConf, serverConf);

        assertThat(tableConfig.getKvWriteBatchSize()).isEqualTo(MemorySize.parse("16mb"));
        assertThat(tableConfig.getKvMaxBackgroundThreads()).isEqualTo(8);
        assertThat(tableConfig.getKvWriteBufferSize()).isEqualTo(MemorySize.parse("256mb"));
        assertThat(tableConfig.getKvMaxWriteBufferNumber()).isEqualTo(4);

        tableConf.set(ConfigOptions.TABLE_KV_WRITE_BATCH_SIZE, MemorySize.parse("8mb"));
        tableConf.set(ConfigOptions.TABLE_KV_MAX_BACKGROUND_THREADS, 3);
        tableConf.set(ConfigOptions.TABLE_KV_WRITE_BUFFER_SIZE, MemorySize.parse("128mb"));
        tableConf.set(ConfigOptions.TABLE_KV_MAX_WRITE_BUFFER_NUMBER, 2);

        assertThat(tableConfig.getKvWriteBatchSize()).isEqualTo(MemorySize.parse("8mb"));
        assertThat(tableConfig.getKvMaxBackgroundThreads()).isEqualTo(3);
        assertThat(tableConfig.getKvWriteBufferSize()).isEqualTo(MemorySize.parse("128mb"));
        assertThat(tableConfig.getKvMaxWriteBufferNumber()).isEqualTo(2);
    }

    @Test
    void testRocksDbOptionsFallBackToServerConfig() {
        Configuration tableConf = new Configuration();
        Configuration serverConf = new Configuration();
        serverConf.set(ConfigOptions.KV_MAX_OPEN_FILES, 10);
        serverConf.set(ConfigOptions.KV_LOG_MAX_FILE_SIZE, MemorySize.parse("10mb"));
        serverConf.set(ConfigOptions.KV_LOG_FILE_NUM, 3);
        serverConf.set(ConfigOptions.KV_LOG_DIR, "/server-logs");
        serverConf.set(ConfigOptions.KV_LOG_LEVEL, ConfigOptions.InfoLogLevel.WARN_LEVEL);
        serverConf.set(ConfigOptions.KV_COMPACTION_STYLE, ConfigOptions.CompactionStyle.UNIVERSAL);
        serverConf.set(ConfigOptions.KV_USE_DYNAMIC_LEVEL_SIZE, true);
        serverConf.set(
                ConfigOptions.KV_COMPRESSION_PER_LEVEL,
                Arrays.asList(
                        ConfigOptions.KvCompressionType.NO, ConfigOptions.KvCompressionType.LZ4));
        serverConf.set(ConfigOptions.KV_TARGET_FILE_SIZE_BASE, MemorySize.parse("32mb"));
        serverConf.set(ConfigOptions.KV_MAX_SIZE_LEVEL_BASE, MemorySize.parse("128mb"));
        serverConf.set(ConfigOptions.KV_MIN_WRITE_BUFFER_NUMBER_TO_MERGE, 2);
        serverConf.set(ConfigOptions.KV_BLOCK_SIZE, MemorySize.parse("8kb"));
        serverConf.set(ConfigOptions.KV_METADATA_BLOCK_SIZE, MemorySize.parse("16kb"));
        serverConf.set(ConfigOptions.KV_BLOCK_CACHE_SIZE, MemorySize.parse("64mb"));
        serverConf.set(ConfigOptions.KV_USE_BLOOM_FILTER, false);
        serverConf.set(ConfigOptions.KV_BLOOM_FILTER_BITS_PER_KEY, 8.0);
        serverConf.set(ConfigOptions.KV_BLOOM_FILTER_BLOCK_BASED_MODE, true);
        serverConf.set(ConfigOptions.KV_CACHE_INDEX_AND_FILTER_BLOCKS, true);
        serverConf.set(ConfigOptions.KV_CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY, true);
        serverConf.set(ConfigOptions.KV_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE, true);
        serverConf.set(ConfigOptions.KV_PIN_TOP_LEVEL_INDEX_AND_FILTER, true);

        ResolvedTableConfig tableConfig = new ResolvedTableConfig(tableConf, serverConf);

        assertThat(tableConfig.getKvMaxOpenFiles()).isEqualTo(10);
        assertThat(tableConfig.getKvLogMaxFileSize()).isEqualTo(MemorySize.parse("10mb"));
        assertThat(tableConfig.getKvLogFileNum()).isEqualTo(3);
        assertThat(tableConfig.getKvLogDir()).isEqualTo("/server-logs");
        assertThat(tableConfig.getKvLogLevel()).isEqualTo(ConfigOptions.InfoLogLevel.WARN_LEVEL);
        assertThat(tableConfig.getKvCompactionStyle())
                .isEqualTo(ConfigOptions.CompactionStyle.UNIVERSAL);
        assertThat(tableConfig.isKvDynamicLevelSizeEnabled()).isTrue();
        assertThat(tableConfig.getKvCompressionPerLevel())
                .containsExactly(
                        ConfigOptions.KvCompressionType.NO, ConfigOptions.KvCompressionType.LZ4);
        assertThat(tableConfig.getKvTargetFileSizeBase()).isEqualTo(MemorySize.parse("32mb"));
        assertThat(tableConfig.getKvMaxSizeLevelBase()).isEqualTo(MemorySize.parse("128mb"));
        assertThat(tableConfig.getKvMinWriteBufferNumberToMerge()).isEqualTo(2);
        assertThat(tableConfig.getKvBlockSize()).isEqualTo(MemorySize.parse("8kb"));
        assertThat(tableConfig.getKvMetadataBlockSize()).isEqualTo(MemorySize.parse("16kb"));
        assertThat(tableConfig.getKvBlockCacheSize()).isEqualTo(MemorySize.parse("64mb"));
        assertThat(tableConfig.isKvBloomFilterEnabled()).isFalse();
        assertThat(tableConfig.getKvBloomFilterBitsPerKey()).isEqualTo(8.0);
        assertThat(tableConfig.isKvBloomFilterBlockBasedMode()).isTrue();
        assertThat(tableConfig.isKvCacheIndexAndFilterBlocks()).isTrue();
        assertThat(tableConfig.isKvCacheIndexAndFilterBlocksWithHighPriority()).isTrue();
        assertThat(tableConfig.isKvPinL0FilterAndIndexBlocksInCache()).isTrue();
        assertThat(tableConfig.isKvPinTopLevelIndexAndFilter()).isTrue();

        tableConf.set(ConfigOptions.TABLE_KV_MAX_OPEN_FILES, 20);
        tableConf.set(ConfigOptions.TABLE_KV_LOG_MAX_FILE_SIZE, MemorySize.parse("20mb"));
        tableConf.set(ConfigOptions.TABLE_KV_LOG_FILE_NUM, 5);
        tableConf.set(ConfigOptions.TABLE_KV_LOG_DIR, "/table-logs");
        tableConf.set(ConfigOptions.TABLE_KV_LOG_LEVEL, ConfigOptions.InfoLogLevel.ERROR_LEVEL);
        tableConf.set(ConfigOptions.TABLE_KV_COMPACTION_STYLE, ConfigOptions.CompactionStyle.FIFO);
        tableConf.set(ConfigOptions.TABLE_KV_USE_DYNAMIC_LEVEL_SIZE, false);
        tableConf.set(
                ConfigOptions.TABLE_KV_COMPRESSION_PER_LEVEL,
                Arrays.asList(
                        ConfigOptions.KvCompressionType.SNAPPY,
                        ConfigOptions.KvCompressionType.ZSTD));
        tableConf.set(ConfigOptions.TABLE_KV_TARGET_FILE_SIZE_BASE, MemorySize.parse("64mb"));
        tableConf.set(ConfigOptions.TABLE_KV_MAX_SIZE_LEVEL_BASE, MemorySize.parse("256mb"));
        tableConf.set(ConfigOptions.TABLE_KV_MIN_WRITE_BUFFER_NUMBER_TO_MERGE, 3);
        tableConf.set(ConfigOptions.TABLE_KV_BLOCK_SIZE, MemorySize.parse("16kb"));
        tableConf.set(ConfigOptions.TABLE_KV_METADATA_BLOCK_SIZE, MemorySize.parse("32kb"));
        tableConf.set(ConfigOptions.TABLE_KV_BLOCK_CACHE_SIZE, MemorySize.parse("128mb"));
        tableConf.set(ConfigOptions.TABLE_KV_USE_BLOOM_FILTER, true);
        tableConf.set(ConfigOptions.TABLE_KV_BLOOM_FILTER_BITS_PER_KEY, 12.0);
        tableConf.set(ConfigOptions.TABLE_KV_BLOOM_FILTER_BLOCK_BASED_MODE, false);
        tableConf.set(ConfigOptions.TABLE_KV_CACHE_INDEX_AND_FILTER_BLOCKS, false);
        tableConf.set(
                ConfigOptions.TABLE_KV_CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY, false);
        tableConf.set(ConfigOptions.TABLE_KV_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE, false);
        tableConf.set(ConfigOptions.TABLE_KV_PIN_TOP_LEVEL_INDEX_AND_FILTER, false);

        assertThat(tableConfig.getKvMaxOpenFiles()).isEqualTo(20);
        assertThat(tableConfig.getKvLogMaxFileSize()).isEqualTo(MemorySize.parse("20mb"));
        assertThat(tableConfig.getKvLogFileNum()).isEqualTo(5);
        assertThat(tableConfig.getKvLogDir()).isEqualTo("/table-logs");
        assertThat(tableConfig.getKvLogLevel()).isEqualTo(ConfigOptions.InfoLogLevel.ERROR_LEVEL);
        assertThat(tableConfig.getKvCompactionStyle())
                .isEqualTo(ConfigOptions.CompactionStyle.FIFO);
        assertThat(tableConfig.isKvDynamicLevelSizeEnabled()).isFalse();
        assertThat(tableConfig.getKvCompressionPerLevel())
                .containsExactly(
                        ConfigOptions.KvCompressionType.SNAPPY,
                        ConfigOptions.KvCompressionType.ZSTD);
        assertThat(tableConfig.getKvTargetFileSizeBase()).isEqualTo(MemorySize.parse("64mb"));
        assertThat(tableConfig.getKvMaxSizeLevelBase()).isEqualTo(MemorySize.parse("256mb"));
        assertThat(tableConfig.getKvMinWriteBufferNumberToMerge()).isEqualTo(3);
        assertThat(tableConfig.getKvBlockSize()).isEqualTo(MemorySize.parse("16kb"));
        assertThat(tableConfig.getKvMetadataBlockSize()).isEqualTo(MemorySize.parse("32kb"));
        assertThat(tableConfig.getKvBlockCacheSize()).isEqualTo(MemorySize.parse("128mb"));
        assertThat(tableConfig.isKvBloomFilterEnabled()).isTrue();
        assertThat(tableConfig.getKvBloomFilterBitsPerKey()).isEqualTo(12.0);
        assertThat(tableConfig.isKvBloomFilterBlockBasedMode()).isFalse();
        assertThat(tableConfig.isKvCacheIndexAndFilterBlocks()).isFalse();
        assertThat(tableConfig.isKvCacheIndexAndFilterBlocksWithHighPriority()).isFalse();
        assertThat(tableConfig.isKvPinL0FilterAndIndexBlocksInCache()).isFalse();
        assertThat(tableConfig.isKvPinTopLevelIndexAndFilter()).isFalse();
    }
}

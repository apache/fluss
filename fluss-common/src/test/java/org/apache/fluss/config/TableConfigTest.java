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

import org.apache.fluss.metadata.DeleteBehavior;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableConfig}. */
class TableConfigTest {

    @Test
    void testDeleteBehavior() {
        Configuration conf = new Configuration();
        TableConfig tableConfig = new TableConfig(conf);

        // Test default value (empty optional since not set)
        assertThat(tableConfig.getDeleteBehavior()).isEmpty();

        // Test configured value
        conf.set(ConfigOptions.TABLE_DELETE_BEHAVIOR, DeleteBehavior.ALLOW);
        TableConfig tableConfig2 = new TableConfig(conf);
        assertThat(tableConfig2.getDeleteBehavior()).hasValue(DeleteBehavior.ALLOW);

        // Test IGNORE
        conf.set(ConfigOptions.TABLE_DELETE_BEHAVIOR, DeleteBehavior.IGNORE);
        TableConfig tableConfig3 = new TableConfig(conf);
        assertThat(tableConfig3.getDeleteBehavior()).hasValue(DeleteBehavior.IGNORE);
    }

    @Test
    void testTableLevelRuntimeConfigGetters() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE, MemorySize.parse("4kb"));
        conf.set(ConfigOptions.TABLE_LOG_INDEX_FILE_SIZE, MemorySize.parse("1kb"));
        conf.set(ConfigOptions.TABLE_KV_WRITE_BATCH_SIZE, MemorySize.parse("8mb"));
        conf.set(ConfigOptions.TABLE_KV_MAX_BACKGROUND_THREADS, 3);
        conf.set(ConfigOptions.TABLE_KV_WRITE_BUFFER_SIZE, MemorySize.parse("128mb"));
        conf.set(ConfigOptions.TABLE_KV_MAX_WRITE_BUFFER_NUMBER, 2);
        conf.set(ConfigOptions.TABLE_KV_MAX_OPEN_FILES, 20);
        conf.set(ConfigOptions.TABLE_KV_LOG_MAX_FILE_SIZE, MemorySize.parse("20mb"));
        conf.set(ConfigOptions.TABLE_KV_LOG_FILE_NUM, 5);
        conf.set(ConfigOptions.TABLE_KV_LOG_DIR, "/table-logs");
        conf.set(ConfigOptions.TABLE_KV_LOG_LEVEL, ConfigOptions.InfoLogLevel.ERROR_LEVEL);
        conf.set(ConfigOptions.TABLE_KV_COMPACTION_STYLE, ConfigOptions.CompactionStyle.FIFO);
        conf.set(ConfigOptions.TABLE_KV_USE_DYNAMIC_LEVEL_SIZE, false);
        conf.set(
                ConfigOptions.TABLE_KV_COMPRESSION_PER_LEVEL,
                Arrays.asList(
                        ConfigOptions.KvCompressionType.SNAPPY,
                        ConfigOptions.KvCompressionType.ZSTD));
        conf.set(ConfigOptions.TABLE_KV_TARGET_FILE_SIZE_BASE, MemorySize.parse("64mb"));
        conf.set(ConfigOptions.TABLE_KV_MAX_SIZE_LEVEL_BASE, MemorySize.parse("256mb"));
        conf.set(ConfigOptions.TABLE_KV_MIN_WRITE_BUFFER_NUMBER_TO_MERGE, 3);
        conf.set(ConfigOptions.TABLE_KV_BLOCK_SIZE, MemorySize.parse("16kb"));
        conf.set(ConfigOptions.TABLE_KV_METADATA_BLOCK_SIZE, MemorySize.parse("32kb"));
        conf.set(ConfigOptions.TABLE_KV_BLOCK_CACHE_SIZE, MemorySize.parse("128mb"));
        conf.set(ConfigOptions.TABLE_KV_USE_BLOOM_FILTER, true);
        conf.set(ConfigOptions.TABLE_KV_BLOOM_FILTER_BITS_PER_KEY, 12.0);
        conf.set(ConfigOptions.TABLE_KV_BLOOM_FILTER_BLOCK_BASED_MODE, false);
        conf.set(ConfigOptions.TABLE_KV_CACHE_INDEX_AND_FILTER_BLOCKS, false);
        conf.set(ConfigOptions.TABLE_KV_CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY, false);
        conf.set(ConfigOptions.TABLE_KV_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE, false);
        conf.set(ConfigOptions.TABLE_KV_PIN_TOP_LEVEL_INDEX_AND_FILTER, false);

        TableConfig tableConfig = new TableConfig(conf);

        assertThat(tableConfig.contains(ConfigOptions.TABLE_LOG_SEGMENT_FILE_SIZE)).isTrue();
        assertThat(tableConfig.getLogSegmentSize()).isEqualTo(MemorySize.parse("4kb"));
        assertThat(tableConfig.getLogIndexFileSize()).isEqualTo(MemorySize.parse("1kb"));
        assertThat(tableConfig.getKvWriteBatchSize()).isEqualTo(MemorySize.parse("8mb"));
        assertThat(tableConfig.getKvMaxBackgroundThreads()).isEqualTo(3);
        assertThat(tableConfig.getKvWriteBufferSize()).isEqualTo(MemorySize.parse("128mb"));
        assertThat(tableConfig.getKvMaxWriteBufferNumber()).isEqualTo(2);
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

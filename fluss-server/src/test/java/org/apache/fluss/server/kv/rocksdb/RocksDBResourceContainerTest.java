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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.kv.KvManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.WriteBufferManager;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests to guard {@link org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer}. */
class RocksDBResourceContainerTest {

    @Test
    void testFreeDBOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        DBOptions dbOptions = container.getDbOptions();
        assertThat(dbOptions.isOwningHandle()).isTrue();
        container.close();
        assertThat(dbOptions.isOwningHandle()).isFalse();
    }

    @Test
    void testFreeMultipleDBOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        final int optionNumber = 20;
        ArrayList<DBOptions> dbOptions = new ArrayList<>(optionNumber);
        for (int i = 0; i < optionNumber; i++) {
            dbOptions.add(container.getDbOptions());
        }
        container.close();
        for (DBOptions dbOption : dbOptions) {
            assertThat(dbOption.isOwningHandle()).isFalse();
        }
    }

    @Test
    void testFreeColumnOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        ColumnFamilyOptions columnFamilyOptions = container.getColumnOptions();
        assertThat(columnFamilyOptions.isOwningHandle()).isTrue();
        container.close();
        assertThat(columnFamilyOptions.isOwningHandle()).isFalse();
    }

    @Test
    void testFreeMultipleColumnOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        final int optionNumber = 20;
        ArrayList<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(optionNumber);
        for (int i = 0; i < optionNumber; i++) {
            columnFamilyOptions.add(container.getColumnOptions());
        }
        container.close();
        for (ColumnFamilyOptions columnFamilyOption : columnFamilyOptions) {
            assertThat(columnFamilyOption.isOwningHandle()).isFalse();
        }
    }

    @Test
    void testFreeWriteReadOptionsAfterClose() throws Exception {
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        WriteOptions writeOptions = container.getWriteOptions();
        ReadOptions readOptions = container.getReadOptions();
        assertThat(writeOptions.isOwningHandle()).isTrue();
        assertThat(readOptions.isOwningHandle()).isTrue();
        container.close();
        assertThat(writeOptions.isOwningHandle()).isFalse();
        assertThat(readOptions.isOwningHandle()).isFalse();
    }

    @Test
    void testDefaultDbLogDir(@TempDir Path tempFolder) throws Exception {
        final File logFile = File.createTempFile(getClass().getSimpleName() + "-", ".log");
        // set the environment variable 'log.file' with the Flink log file location
        System.setProperty("log.file", logFile.getPath());
        try (RocksDBResourceContainer container = new RocksDBResourceContainer()) {
            assertThat(container.getDbOptions().infoLogLevel()).isEqualTo(InfoLogLevel.INFO_LEVEL);
            File rocksDbLogDirectory = new File(logFile.getParent(), "rocksdb");
            assertThat(container.getDbOptions().dbLogDir())
                    .isEqualTo(rocksDbLogDirectory.getAbsolutePath());
        } finally {
            logFile.delete();
        }

        // test the case that when the instance path is too long, we'll disable the log
        StringBuilder longInstanceBasePath =
                new StringBuilder(tempFolder.toFile().getAbsolutePath());
        while (longInstanceBasePath.length() < 255) {
            longInstanceBasePath.append("/append-for-long-path");
        }
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        new Configuration(), new File(longInstanceBasePath.toString()))) {
            // the db log dir should be empty since we disable the log for the instance path is
            // too long
            assertThat(container.getDbOptions().dbLogDir()).isEmpty();
        } finally {
            logFile.delete();
        }
    }

    @Test
    void testConfigurationOptionsFromConfig() throws Exception {
        Configuration configuration = new Configuration();

        configuration.setString(ConfigOptions.KV_LOG_LEVEL.key(), "DEBUG_LEVEL");
        configuration.setString(ConfigOptions.KV_LOG_DIR.key(), "/tmp/rocksdb-logs/");
        configuration.setString(ConfigOptions.KV_LOG_FILE_NUM.key(), "10");
        configuration.setString(ConfigOptions.KV_LOG_MAX_FILE_SIZE.key(), "2MB");
        configuration.setString(ConfigOptions.KV_COMPACTION_STYLE.key(), "level");
        configuration.setString(ConfigOptions.KV_USE_DYNAMIC_LEVEL_SIZE.key(), "TRUE");
        configuration.setString(ConfigOptions.KV_TARGET_FILE_SIZE_BASE.key(), "8 mb");
        configuration.setString(ConfigOptions.KV_MAX_SIZE_LEVEL_BASE.key(), "128MB");
        configuration.setString(ConfigOptions.KV_MAX_BACKGROUND_THREADS.key(), "4");
        configuration.setString(ConfigOptions.KV_MAX_WRITE_BUFFER_NUMBER.key(), "4");
        configuration.setString(ConfigOptions.KV_MIN_WRITE_BUFFER_NUMBER_TO_MERGE.key(), "2");
        configuration.setString(ConfigOptions.KV_WRITE_BUFFER_SIZE.key(), "64 MB");
        configuration.setString(ConfigOptions.KV_BLOCK_SIZE.key(), "4 kb");
        configuration.setString(ConfigOptions.KV_METADATA_BLOCK_SIZE.key(), "8 kb");
        configuration.setString(ConfigOptions.KV_BLOCK_CACHE_SIZE.key(), "512 mb");
        configuration.setString(ConfigOptions.KV_USE_BLOOM_FILTER.key(), "TRUE");
        configuration.set(
                ConfigOptions.KV_COMPRESSION_PER_LEVEL,
                Arrays.asList(
                        ConfigOptions.KvCompressionType.NO,
                        ConfigOptions.KvCompressionType.LZ4,
                        ConfigOptions.KvCompressionType.ZSTD));

        try (RocksDBResourceContainer optionsContainer =
                new RocksDBResourceContainer(configuration, null, true)) {

            DBOptions dbOptions = optionsContainer.getDbOptions();
            assertThat(dbOptions.maxOpenFiles()).isEqualTo(-1);
            assertThat(dbOptions.infoLogLevel()).isEqualTo(InfoLogLevel.DEBUG_LEVEL);
            assertThat(dbOptions.dbLogDir()).isEqualTo("/tmp/rocksdb-logs/");
            assertThat(dbOptions.keepLogFileNum()).isEqualTo(10);
            assertThat(dbOptions.maxLogFileSize()).isEqualTo(2 * SizeUnit.MB);
            assertThat(dbOptions.statistics()).isNotNull();
            assertThat(dbOptions.avoidFlushDuringShutdown()).isFalse();

            ColumnFamilyOptions columnOptions = optionsContainer.getColumnOptions();
            assertThat(columnOptions.compactionStyle()).isEqualTo(CompactionStyle.LEVEL);
            assertThat(columnOptions.levelCompactionDynamicLevelBytes()).isTrue();
            assertThat(columnOptions.targetFileSizeBase()).isEqualTo(8 * SizeUnit.MB);
            assertThat(columnOptions.maxBytesForLevelBase()).isEqualTo(128 * SizeUnit.MB);
            assertThat(columnOptions.maxWriteBufferNumber()).isEqualTo(4);
            assertThat(columnOptions.minWriteBufferNumberToMerge()).isEqualTo(2);
            assertThat(columnOptions.writeBufferSize()).isEqualTo(64 * SizeUnit.MB);
            assertThat(columnOptions.compressionPerLevel())
                    .isEqualTo(
                            Arrays.asList(
                                    CompressionType.NO_COMPRESSION,
                                    CompressionType.LZ4_COMPRESSION,
                                    CompressionType.ZSTD_COMPRESSION));

            BlockBasedTableConfig tableConfig =
                    (BlockBasedTableConfig) columnOptions.tableFormatConfig();
            assertThat(tableConfig.blockSize()).isEqualTo(4 * SizeUnit.KB);
            assertThat(tableConfig.metadataBlockSize()).isEqualTo(8 * SizeUnit.KB);
            // Verify block cache was created with explicit LRUCache for memory tracking
            assertThat(optionsContainer.getBlockCache()).isNotNull();
            assertThat(tableConfig.filterPolicy() instanceof BloomFilter).isTrue();
        }
    }

    @Test
    void testCacheIndexAndFilterBlocksConfig() throws Exception {
        // Test with default values (all false, following RocksDB defaults)
        Configuration defaultConfig = new Configuration();
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(defaultConfig, null)) {
            ColumnFamilyOptions columnOptions = container.getColumnOptions();
            BlockBasedTableConfig tableConfig =
                    (BlockBasedTableConfig) columnOptions.tableFormatConfig();

            // All default values should be false (RocksDB defaults)
            assertThat(tableConfig.cacheIndexAndFilterBlocks()).isFalse();
            assertThat(tableConfig.cacheIndexAndFilterBlocksWithHighPriority()).isFalse();
            assertThat(tableConfig.pinL0FilterAndIndexBlocksInCache()).isFalse();
            assertThat(tableConfig.pinTopLevelIndexAndFilter()).isFalse();
        }

        // Test with custom values (all true)
        Configuration customConfig = new Configuration();
        customConfig.setString(ConfigOptions.KV_CACHE_INDEX_AND_FILTER_BLOCKS.key(), "true");
        customConfig.setString(
                ConfigOptions.KV_CACHE_INDEX_AND_FILTER_BLOCKS_WITH_HIGH_PRIORITY.key(), "true");
        customConfig.setString(
                ConfigOptions.KV_PIN_L0_FILTER_AND_INDEX_BLOCKS_IN_CACHE.key(), "true");
        customConfig.setString(ConfigOptions.KV_PIN_TOP_LEVEL_INDEX_AND_FILTER.key(), "true");

        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(customConfig, null)) {
            ColumnFamilyOptions columnOptions = container.getColumnOptions();
            BlockBasedTableConfig tableConfig =
                    (BlockBasedTableConfig) columnOptions.tableFormatConfig();

            assertThat(tableConfig.cacheIndexAndFilterBlocks()).isTrue();
            assertThat(tableConfig.cacheIndexAndFilterBlocksWithHighPriority()).isTrue();
            assertThat(tableConfig.pinL0FilterAndIndexBlocksInCache()).isTrue();
            assertThat(tableConfig.pinTopLevelIndexAndFilter()).isTrue();
        }
    }

    @Test
    void testTwoRocksDBInstancesShareCacheUntilOwnerClosesIt(@TempDir Path tempDir)
            throws Exception {
        byte[] firstKey = new byte[] {1};
        byte[] firstValue = new byte[4096];
        byte[] secondKey = new byte[] {2};
        byte[] secondValue = new byte[4096];

        try (Cache sharedCache = new LRUCache(64 * SizeUnit.MB);
                RocksDBKv firstKv =
                        buildRocksDBKvWithSharedCache(
                                tempDir.resolve("first").toFile(), sharedCache);
                RocksDBKv secondKv =
                        buildRocksDBKvWithSharedCache(
                                tempDir.resolve("second").toFile(), sharedCache)) {
            assertThat(firstKv.getBlockCache()).isSameAs(sharedCache);
            assertThat(secondKv.getBlockCache()).isSameAs(sharedCache);

            firstKv.put(firstKey, firstValue);
            secondKv.put(secondKey, secondValue);
            try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
                firstKv.getDb().flush(flushOptions);
                secondKv.getDb().flush(flushOptions);
            }

            assertThat(firstKv.get(firstKey)).isEqualTo(firstValue);
            assertThat(secondKv.get(secondKey)).isEqualTo(secondValue);
            assertThat(sharedCache.getUsage()).isPositive();

            firstKv.close();
            assertThat(sharedCache.isOwningHandle()).isTrue();
            assertThat(secondKv.get(secondKey)).isEqualTo(secondValue);
        }
    }

    @Test
    void testTwoRocksDBInstancesShareWriteBufferManager(@TempDir Path tempDir) throws Exception {
        byte[] value = new byte[(int) (512 * SizeUnit.KB)];
        RateLimiter rateLimiter = KvManager.getDefaultRateLimiter();

        try (Cache accountingCache = new LRUCache(4 * SizeUnit.MB);
                WriteBufferManager writeBufferManager =
                        new WriteBufferManager(4 * SizeUnit.MB, accountingCache);
                RocksDBKv firstKv =
                        buildRocksDBKvWithSharedWriteBufferManager(
                                tempDir.resolve("first").toFile(),
                                rateLimiter,
                                writeBufferManager);
                RocksDBKv secondKv =
                        buildRocksDBKvWithSharedWriteBufferManager(
                                tempDir.resolve("second").toFile(),
                                rateLimiter,
                                writeBufferManager)) {
            firstKv.put(new byte[] {1}, value);
            secondKv.put(new byte[] {2}, value);
            assertThat(accountingCache.getUsage()).isPositive();

            firstKv.close();
            assertThat(writeBufferManager.isOwningHandle()).isTrue();
            secondKv.put(new byte[] {3}, value);
            assertThat(secondKv.get(new byte[] {2})).isEqualTo(value);
        }
    }

    @Test
    void testIndependentCacheClosedOnContainerClose() throws Exception {
        // When no shared cache, the independent cache should be closed with the container
        RocksDBResourceContainer container = new RocksDBResourceContainer();
        container.getColumnOptions();
        Cache blockCache = container.getBlockCache();
        assertThat(blockCache).isNotNull();
        assertThat(blockCache.isOwningHandle()).isTrue();
        container.close();
        assertThat(blockCache.isOwningHandle()).isFalse();
    }

    private RocksDBKv buildRocksDBKvWithSharedCache(File directory, Cache sharedCache)
            throws Exception {
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        new Configuration(),
                        directory,
                        false,
                        KvManager.getDefaultRateLimiter(),
                        sharedCache);
        return new RocksDBKvBuilder(directory, container, container.getColumnOptions()).build();
    }

    private RocksDBKv buildRocksDBKvWithSharedWriteBufferManager(
            File directory, RateLimiter rateLimiter, WriteBufferManager writeBufferManager)
            throws Exception {
        RocksDBResourceContainer container =
                new RocksDBResourceContainer(
                        new Configuration(),
                        directory,
                        false,
                        rateLimiter,
                        null,
                        writeBufferManager);
        return new RocksDBKvBuilder(directory, container, container.getColumnOptions()).build();
    }
}

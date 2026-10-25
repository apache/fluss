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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.server.kv.KvManager;

import io.github.fluss_contrib.rocksdb.BlockBasedTableConfig;
import io.github.fluss_contrib.rocksdb.BloomFilter;
import io.github.fluss_contrib.rocksdb.Cache;
import io.github.fluss_contrib.rocksdb.Checkpoint;
import io.github.fluss_contrib.rocksdb.ColumnFamilyOptions;
import io.github.fluss_contrib.rocksdb.CompactionStyle;
import io.github.fluss_contrib.rocksdb.CompressionType;
import io.github.fluss_contrib.rocksdb.DBOptions;
import io.github.fluss_contrib.rocksdb.FlushOptions;
import io.github.fluss_contrib.rocksdb.HyperClockCache;
import io.github.fluss_contrib.rocksdb.IndexType;
import io.github.fluss_contrib.rocksdb.InfoLogLevel;
import io.github.fluss_contrib.rocksdb.LRUCache;
import io.github.fluss_contrib.rocksdb.Options;
import io.github.fluss_contrib.rocksdb.RateLimiter;
import io.github.fluss_contrib.rocksdb.ReadOptions;
import io.github.fluss_contrib.rocksdb.RocksDB;
import io.github.fluss_contrib.rocksdb.RocksIterator;
import io.github.fluss_contrib.rocksdb.WriteBufferManager;
import io.github.fluss_contrib.rocksdb.WriteOptions;
import io.github.fluss_contrib.rocksdb.util.SizeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests to guard {@link org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer}. */
class RocksDBResourceContainerTest {

    @Test
    void testPartitioningDefaultsAndOverrides() throws Exception {
        for (boolean caching : new boolean[] {false, true}) {
            for (Boolean partitioning : new Boolean[] {null, false, true}) {
                Configuration conf = new Configuration();
                conf.set(ConfigOptions.KV_CACHE_INDEX_AND_FILTER_BLOCKS, caching);
                conf.set(ConfigOptions.KV_USE_BLOOM_FILTER, true);
                if (partitioning != null) {
                    conf.set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, partitioning);
                }
                try (RocksDBResourceContainer container =
                        new RocksDBResourceContainer(conf, null)) {
                    BlockBasedTableConfig table =
                            (BlockBasedTableConfig)
                                    container.getColumnOptions().tableFormatConfig();
                    boolean enabled = Boolean.TRUE.equals(partitioning);
                    assertThat(table.indexType())
                            .isEqualTo(
                                    enabled
                                            ? IndexType.kTwoLevelIndexSearch
                                            : IndexType.kBinarySearch);
                    assertThat(table.partitionFilters()).isEqualTo(enabled);
                    assertThat(table.cacheIndexAndFilterBlocksWithHighPriority()).isFalse();
                    assertThat(table.pinTopLevelIndexAndFilter()).isFalse();
                    assertThat(table.pinL0FilterAndIndexBlocksInCache()).isFalse();
                }
            }
        }
    }

    @Test
    void testInvalidMetadataOptions() {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, true);
        conf.set(ConfigOptions.KV_USE_BLOOM_FILTER, true);
        conf.set(ConfigOptions.KV_BLOOM_FILTER_BLOCK_BASED_MODE, true);
        assertThatThrownBy(
                        () -> {
                            try (RocksDBResourceContainer ignored =
                                    new RocksDBResourceContainer(conf, null)) {
                                // The constructor must reject invalid options.
                            }
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "kv.rocksdb.bloom-filter.block-based-mode must be false to use partitioned Bloom filters.");
        conf.set(ConfigOptions.KV_BLOOM_FILTER_BLOCK_BASED_MODE, false);
        conf.set(ConfigOptions.KV_METADATA_BLOCK_SIZE, MemorySize.ZERO);
        assertThatThrownBy(
                        () -> {
                            try (RocksDBResourceContainer ignored =
                                    new RocksDBResourceContainer(conf, null)) {
                                // The constructor must reject invalid options.
                            }
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void testPartitionedIndexWithoutBloomFilter() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, true);
        conf.set(ConfigOptions.KV_USE_BLOOM_FILTER, false);
        conf.set(ConfigOptions.KV_BLOOM_FILTER_BLOCK_BASED_MODE, true);
        try (RocksDBResourceContainer container = new RocksDBResourceContainer(conf, null)) {
            BlockBasedTableConfig table =
                    (BlockBasedTableConfig) container.getColumnOptions().tableFormatConfig();
            assertThat(table.indexType()).isEqualTo(IndexType.kTwoLevelIndexSearch);
            assertThat(table.partitionFilters()).isFalse();
            assertThat(table.filterPolicy()).isNull();
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, true"})
    void testMixedSstIteratorsAfterPartitioningToggleAndCheckpoint(
            boolean shared, boolean cacheMetadata, @TempDir Path directory) throws Exception {
        String dbPath = directory.resolve("db").toString();
        String checkpointPath = directory.resolve("checkpoint").toString();
        try (Cache sharedCache =
                shared ? new HyperClockCache(64 * SizeUnit.MB, 0, -1, false) : null) {
            for (int phase = 0; phase < 3; phase++) {
                Configuration conf = new Configuration();
                conf.set(ConfigOptions.KV_PARTITIONED_INDEX_FILTERS, phase == 1);
                conf.set(ConfigOptions.KV_CACHE_INDEX_AND_FILTER_BLOCKS, cacheMetadata);
                conf.set(ConfigOptions.KV_USE_BLOOM_FILTER, true);
                conf.set(ConfigOptions.KV_BLOCK_SIZE, MemorySize.parse("256b"));
                conf.set(ConfigOptions.KV_METADATA_BLOCK_SIZE, MemorySize.parse("256b"));
                try (RocksDBResourceContainer container =
                                new RocksDBResourceContainer(
                                        conf,
                                        null,
                                        false,
                                        KvManager.getDefaultRateLimiter(),
                                        sharedCache);
                        Options options =
                                new Options(container.getDbOptions(), container.getColumnOptions())
                                        .setDisableAutoCompactions(true);
                        RocksDB db = RocksDB.open(options, phase == 2 ? checkpointPath : dbPath)) {
                    Set<String> existingSstFiles =
                            new HashSet<>(db.getPropertiesOfAllTables().keySet());
                    for (int i = phase * 1000; i < (phase + 1) * 1000; i++) {
                        byte[] key = intKey(i);
                        db.put(key, key);
                    }
                    try (FlushOptions flush = new FlushOptions().setWaitForFlush(true)) {
                        db.flush(flush);
                    }
                    boolean partitioned = phase == 1;
                    assertThat(db.getPropertiesOfAllTables().entrySet())
                            .filteredOn(entry -> !existingSstFiles.contains(entry.getKey()))
                            .singleElement()
                            .satisfies(
                                    entry -> {
                                        if (partitioned) {
                                            assertThat(entry.getValue().getIndexPartitions())
                                                    .isGreaterThan(1);
                                        } else {
                                            assertThat(entry.getValue().getIndexPartitions())
                                                    .isZero();
                                        }
                                    });
                    assertThat(db.getPropertiesOfAllTables()).hasSize(phase + 1);
                    int count = (phase + 1) * 1000;
                    for (int i = 0; i < count; i++) {
                        assertThat(db.get(intKey(i))).isEqualTo(intKey(i));
                    }
                    try (RocksIterator iterator = db.newIterator()) {
                        iterator.seekToFirst();
                        for (int i = 0; i < count; i++) {
                            assertThat(iterator.isValid()).isTrue();
                            assertThat(iterator.key()).isEqualTo(intKey(i));
                            assertThat(iterator.value()).isEqualTo(intKey(i));
                            iterator.next();
                        }
                        assertThat(iterator.isValid()).isFalse();
                        iterator.status();
                        iterator.seek(intKey(750));
                        assertThat(iterator.key()).isEqualTo(intKey(750));
                        iterator.prev();
                        assertThat(iterator.key()).isEqualTo(intKey(749));
                        if (phase > 0) {
                            iterator.seek(intKey(1750));
                            assertThat(iterator.isValid()).isTrue();
                            assertThat(iterator.key()).isEqualTo(intKey(1750));
                            iterator.prev();
                            assertThat(iterator.isValid()).isTrue();
                            assertThat(iterator.key()).isEqualTo(intKey(1749));
                            iterator.seek(intKey(1000));
                            assertThat(iterator.isValid()).isTrue();
                            assertThat(iterator.key()).isEqualTo(intKey(1000));
                            iterator.prev();
                            assertThat(iterator.isValid()).isTrue();
                            assertThat(iterator.key()).isEqualTo(intKey(999));
                        }
                        iterator.status();
                    }
                    if (phase == 1) {
                        try (Checkpoint checkpoint = Checkpoint.create(db)) {
                            checkpoint.createCheckpoint(checkpointPath);
                        }
                    }
                }
            }
        }
    }

    private static byte[] intKey(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

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
        configuration.set(ConfigOptions.KV_USE_DIRECT_READS, true);
        configuration.set(ConfigOptions.KV_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION, true);
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
            assertThat(dbOptions.useDirectReads()).isTrue();
            assertThat(dbOptions.useDirectIoForFlushAndCompaction()).isTrue();
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
            assertThat(optionsContainer.getBlockCache()).isInstanceOf(HyperClockCache.class);
            assertThat(tableConfig.filterPolicy() instanceof BloomFilter).isTrue();
        }
    }

    @Test
    void testCacheIndexAndFilterBlocksConfig() throws Exception {
        // Test the Fluss defaults (all false).
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

        try (Cache sharedCache = new HyperClockCache(64 * SizeUnit.MB, 0, -1, false);
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

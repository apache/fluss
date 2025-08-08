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

import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.ReadOptions;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests to guard {@link com.alibaba.fluss.server.kv.rocksdb.RocksDBResourceContainer}. */
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
            assertThat(container.getDbOptions().dbLogDir()).isEqualTo(logFile.getParent());
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
                new RocksDBResourceContainer(configuration, null)) {

            DBOptions dbOptions = optionsContainer.getDbOptions();
            assertThat(dbOptions.maxOpenFiles()).isEqualTo(-1);
            assertThat(dbOptions.infoLogLevel()).isEqualTo(InfoLogLevel.DEBUG_LEVEL);
            assertThat(dbOptions.dbLogDir()).isEqualTo("/tmp/rocksdb-logs/");
            assertThat(dbOptions.keepLogFileNum()).isEqualTo(10);
            assertThat(dbOptions.maxLogFileSize()).isEqualTo(2 * SizeUnit.MB);
            assertThat(dbOptions.statistics()).isNotNull();

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
            assertThat(tableConfig.filterPolicy() instanceof BloomFilter).isTrue();
        }
    }

    @Test
    void testBlockCacheCreation() throws Exception {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_BLOCK_CACHE_SIZE.key(), "16mb");

        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            Cache blockCache = container.getBlockCache();
            assertThat(blockCache).isNotNull();
        }
    }

    @Test
    void testBlockCacheNotCreatedWhenSizeIsZero() throws Exception {
        Configuration config = new Configuration();
        config.setString(ConfigOptions.KV_BLOCK_CACHE_SIZE.key(), "0mb");

        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            Cache blockCache = container.getBlockCache();
            assertThat(blockCache).isNull();
        }
    }

    @Test
    void testStatisticsEnabledByDefault() throws Exception {
        Configuration config = new Configuration();
        // Statistics should be enabled by default
        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            DBOptions dbOptions = container.getDbOptions();
            assertThat(dbOptions.statistics()).isNotNull();
            assertThat(container.getStatistics()).isNotNull();
        }
    }

    @Test
    void testStatisticsDisabled() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), false);

        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            DBOptions dbOptions = container.getDbOptions();
            // Statistics should be null when disabled
            assertThat(dbOptions.statistics()).isNull();
            assertThat(container.getStatistics()).isNull();
        }
    }

    @Test
    void testStatisticsExplicitlyEnabled() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), true);

        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            DBOptions dbOptions = container.getDbOptions();
            assertThat(dbOptions.statistics()).isNotNull();
            assertThat(container.getStatistics()).isNotNull();
        }
    }

    @Test
    void testStatisticsClosedWithContainer() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), true);

        Statistics statistics;
        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            statistics = container.getStatistics();
            assertThat(statistics).isNotNull();
            assertThat(statistics.isOwningHandle()).isTrue();
        }

        // Statistics should be closed when container is closed
        assertThat(statistics.isOwningHandle()).isFalse();
    }

    @Test
    void testMultipleStatisticsObjectsClosedProperly() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), true);

        List<Statistics> statisticsList = new ArrayList<>();

        // Create multiple containers and collect statistics
        for (int i = 0; i < 5; i++) {
            RocksDBResourceContainer container = new RocksDBResourceContainer(config, null);
            Statistics stats = container.getStatistics();
            if (stats != null) {
                statisticsList.add(stats);
            }
            container.close();
        }

        // All statistics should be closed
        for (Statistics stats : statisticsList) {
            assertThat(stats.isOwningHandle()).isFalse();
        }
    }

    @Test
    void testStatisticsWithNullInstancePath() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), true);

        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            Statistics statistics = container.getStatistics();
            assertThat(statistics).isNotNull();
            assertThat(statistics.isOwningHandle()).isTrue();
        }
    }

    @Test
    void testStatisticsWithValidInstancePath(@TempDir Path tempFolder) throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), true);

        File instancePath = tempFolder.resolve("rocksdb-instance").toFile();
        try (RocksDBResourceContainer container =
                new RocksDBResourceContainer(config, instancePath)) {
            Statistics statistics = container.getStatistics();
            assertThat(statistics).isNotNull();
            assertThat(statistics.isOwningHandle()).isTrue();
        }
    }

    @Test
    void testStatisticsConfigurationConsistency() throws Exception {
        Configuration config = new Configuration();

        // Test with statistics disabled
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), false);
        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            assertThat(container.getStatistics()).isNull();
        }

        // Test with statistics enabled
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), true);
        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            assertThat(container.getStatistics()).isNotNull();
        }
    }

    @Test
    void testStatisticsMemoryManagement() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), true);

        // Create and close many containers to test memory management
        List<RocksDBResourceContainer> containers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            RocksDBResourceContainer container = new RocksDBResourceContainer(config, null);
            containers.add(container);

            // Verify statistics is created and valid
            Statistics stats = container.getStatistics();
            assertThat(stats).isNotNull();
            assertThat(stats.isOwningHandle()).isTrue();
        }

        // Close all containers
        for (RocksDBResourceContainer container : containers) {
            container.close();
        }

        // This should not cause memory leaks or crashes
        System.gc(); // Suggest garbage collection
    }

    @Test
    void testStatisticsThreadSafety() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), true);

        try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
            Statistics statistics = container.getStatistics();
            assertThat(statistics).isNotNull();

            // Test concurrent access to statistics
            int threadCount = 10;
            CountDownLatch latch = new CountDownLatch(threadCount);
            List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());

            for (int i = 0; i < threadCount; i++) {
                new Thread(
                                () -> {
                                    try {
                                        // Access statistics concurrently
                                        Statistics stats = container.getStatistics();
                                        assertThat(stats).isNotNull();
                                    } catch (Exception e) {
                                        exceptions.add(e);
                                    } finally {
                                        latch.countDown();
                                    }
                                })
                        .start();
            }

            latch.await(5, TimeUnit.SECONDS);
            assertThat(exceptions).isEmpty();
        }
    }

    @Test
    void testStatisticsWithDifferentConfigurations() throws Exception {
        // Test various configuration combinations
        Configuration[] configs = {
            new Configuration(), // Default
            createConfigWithStatistics(true),
            createConfigWithStatistics(false)
        };

        for (Configuration config : configs) {
            try (RocksDBResourceContainer container = new RocksDBResourceContainer(config, null)) {
                boolean expectedEnabled = config.get(ConfigOptions.KV_STATISTICS_ENABLED);
                Statistics statistics = container.getStatistics();

                if (expectedEnabled) {
                    assertThat(statistics).isNotNull();
                } else {
                    assertThat(statistics).isNull();
                }
            }
        }
    }

    private Configuration createConfigWithStatistics(boolean enabled) {
        Configuration config = new Configuration();
        config.setBoolean(ConfigOptions.KV_STATISTICS_ENABLED.key(), enabled);
        return config;
    }
}

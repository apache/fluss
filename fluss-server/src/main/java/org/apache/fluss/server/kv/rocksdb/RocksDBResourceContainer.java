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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.server.config.ResolvedTableConfig;
import org.apache.fluss.server.kv.KvManager;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The container for RocksDB resources, including predefined options, option factory and shared
 * resource among instances.
 *
 * <p>This should be the only entrance for {@link RocksDBKv} to get RocksDB options, and should be
 * properly (and necessarily) closed to prevent resource leak.
 */
public class RocksDBResourceContainer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBResourceContainer.class);

    // the filename length limit is 255 on most operating systems
    private static final int INSTANCE_PATH_LENGTH_LIMIT = 255 - "_LOG".length();

    @Nullable private final File instanceRocksDBPath;

    private final TableConfig tableConfig;

    private final boolean enableStatistics;

    /** The shared rate limiter for all RocksDB instances. */
    private final RateLimiter sharedRateLimiter;

    /** The statistics object for RocksDB, null if statistics is disabled. */
    @Nullable private Statistics statistics;

    /** The block cache for RocksDB, shared across column families. */
    @Nullable private Cache blockCache;

    /** The handles to be closed when the container is closed. */
    private final ArrayList<AutoCloseable> handlesToClose;

    @VisibleForTesting
    RocksDBResourceContainer() {
        this(
                null,
                false,
                KvManager.getDefaultRateLimiter(),
                new ResolvedTableConfig(new Configuration(), new Configuration()));
    }

    public RocksDBResourceContainer(TableConfig tableConfig, @Nullable File instanceBasePath) {
        this(instanceBasePath, false, KvManager.getDefaultRateLimiter(), tableConfig);
    }

    public RocksDBResourceContainer(
            TableConfig tableConfig, @Nullable File instanceBasePath, boolean enableStatistics) {
        this(instanceBasePath, enableStatistics, KvManager.getDefaultRateLimiter(), tableConfig);
    }

    public RocksDBResourceContainer(
            @Nullable File instanceBasePath,
            boolean enableStatistics,
            RateLimiter sharedRateLimiter,
            TableConfig tableConfig) {
        this.tableConfig = checkNotNull(tableConfig, "tableConfig must not be null.");

        this.instanceRocksDBPath =
                instanceBasePath != null
                        ? RocksDBKvBuilder.getInstanceRocksDBPath(instanceBasePath)
                        : null;
        this.enableStatistics = enableStatistics;
        this.sharedRateLimiter =
                checkNotNull(sharedRateLimiter, "sharedRateLimiter must not be null");

        this.handlesToClose = new ArrayList<>();
    }

    /** Gets the RocksDB {@link DBOptions} to be used for RocksDB instances. */
    public DBOptions getDbOptions() throws IOException {
        // initial options from common profile
        DBOptions opt = createBaseCommonDBOptions();
        handlesToClose.add(opt);

        // load configurable options on top of pre-defined profile
        setDBOptionsFromConfigurableOptions(opt);

        // todo: maybe we can allow user define options factory and some predefined options
        //  just like Flink

        // todo: introduce WriteBufferManager for controllable memory consume in FLUSS-54164814

        // add necessary default options
        opt = opt.setCreateIfMissing(true);

        // set shared rate limiter
        opt.setRateLimiter(sharedRateLimiter);

        if (enableStatistics) {
            statistics = new Statistics();
            opt.setStatistics(statistics);
            handlesToClose.add(statistics);
        }

        return opt;
    }

    /** Gets the Statistics object if statistics is enabled, null otherwise. */
    @Nullable
    public Statistics getStatistics() {
        return statistics;
    }

    /** Gets the block cache used by RocksDB, null if not yet initialized. */
    @Nullable
    public Cache getBlockCache() {
        return blockCache;
    }

    /** Gets the RocksDB {@link ColumnFamilyOptions} to be used for all RocksDB instances. */
    public ColumnFamilyOptions getColumnOptions() {
        // initial options from common profile
        ColumnFamilyOptions opt = createBaseCommonColumnOptions();
        handlesToClose.add(opt);

        // load configurable options on top of pre-defined profile
        setColumnFamilyOptionsFromConfigurableOptions(opt, handlesToClose);

        return opt;
    }

    /** Gets the RocksDB {@link WriteOptions} to be used for write operations. */
    public WriteOptions getWriteOptions() {
        // Disable WAL by default
        WriteOptions opt = new WriteOptions().setDisableWAL(true);
        handlesToClose.add(opt);

        return opt;
    }

    /** Gets the RocksDB {@link ReadOptions} to be used for read operations. */
    public ReadOptions getReadOptions() {
        ReadOptions opt = new ReadOptions();
        handlesToClose.add(opt);

        return opt;
    }

    @Override
    public void close() throws Exception {
        handlesToClose.forEach(IOUtils::closeQuietly);
        handlesToClose.clear();
    }

    /** Create a {@link DBOptions} for RocksDB, including some common settings. */
    DBOptions createBaseCommonDBOptions() {
        return new DBOptions().setUseFsync(false).setStatsDumpPeriodSec(0);
    }

    /** Create a {@link ColumnFamilyOptions} for RocksDB, including some common settings. */
    ColumnFamilyOptions createBaseCommonColumnOptions() {
        return new ColumnFamilyOptions();
    }

    @SuppressWarnings("ConstantConditions")
    private DBOptions setDBOptionsFromConfigurableOptions(DBOptions currentOptions)
            throws IOException {
        currentOptions.setMaxBackgroundJobs(tableConfig.getKvMaxBackgroundThreads());

        currentOptions.setMaxOpenFiles(tableConfig.getKvMaxOpenFiles());

        currentOptions.setInfoLogLevel(toRocksDbInfoLogLevel(tableConfig.getKvLogLevel()));

        String logDir = tableConfig.getKvLogDir();
        if (logDir == null || logDir.isEmpty()) {
            if (instanceRocksDBPath == null
                    || instanceRocksDBPath.getAbsolutePath().length()
                            <= INSTANCE_PATH_LENGTH_LIMIT) {
                relocateDefaultDbLogDir(currentOptions);
            } else {
                // disable log relocate when instance path length exceeds limit to prevent rocksdb
                // log file creation failure, details in FLINK-31743
                LOG.warn(
                        "RocksDB instance path length exceeds limit : {}, disable log relocate.",
                        instanceRocksDBPath);
            }
        } else {
            currentOptions.setDbLogDir(logDir);
        }

        currentOptions.setMaxLogFileSize(tableConfig.getKvLogMaxFileSize().getBytes());

        currentOptions.setKeepLogFileNum(tableConfig.getKvLogFileNum());

        return currentOptions;
    }

    @SuppressWarnings("ConstantConditions")
    private ColumnFamilyOptions setColumnFamilyOptionsFromConfigurableOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {

        currentOptions.setCompactionStyle(
                toRocksDbCompactionStyle(tableConfig.getKvCompactionStyle()));

        currentOptions.setCompressionPerLevel(
                toRocksDbCompressionTypes(tableConfig.getKvCompressionPerLevel()));

        currentOptions.setLevelCompactionDynamicLevelBytes(
                tableConfig.isKvDynamicLevelSizeEnabled());

        currentOptions.setTargetFileSizeBase(tableConfig.getKvTargetFileSizeBase().getBytes());

        currentOptions.setMaxBytesForLevelBase(tableConfig.getKvMaxSizeLevelBase().getBytes());

        currentOptions.setWriteBufferSize(tableConfig.getKvWriteBufferSize().getBytes());

        currentOptions.setMaxWriteBufferNumber(tableConfig.getKvMaxWriteBufferNumber());

        currentOptions.setMinWriteBufferNumberToMerge(
                tableConfig.getKvMinWriteBufferNumberToMerge());

        TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();

        BlockBasedTableConfig blockBasedTableConfig;
        if (tableFormatConfig == null) {
            blockBasedTableConfig = new BlockBasedTableConfig();
        } else {
            if (tableFormatConfig instanceof PlainTableConfig) {
                // if the table format config is PlainTableConfig, we just return current
                // column-family options
                return currentOptions;
            } else {
                blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
            }
        }

        blockBasedTableConfig.setBlockSize(tableConfig.getKvBlockSize().getBytes());

        blockBasedTableConfig.setMetadataBlockSize(tableConfig.getKvMetadataBlockSize().getBytes());

        // Create explicit LRUCache for accurate memory tracking
        long blockCacheSize = tableConfig.getKvBlockCacheSize().getBytes();
        blockCache = new LRUCache(blockCacheSize);
        handlesToClose.add(blockCache);
        blockBasedTableConfig.setBlockCache(blockCache);

        // Configure index and filter blocks caching
        blockBasedTableConfig.setCacheIndexAndFilterBlocks(
                tableConfig.isKvCacheIndexAndFilterBlocks());
        blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(
                tableConfig.isKvCacheIndexAndFilterBlocksWithHighPriority());
        blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(
                tableConfig.isKvPinL0FilterAndIndexBlocksInCache());
        blockBasedTableConfig.setPinTopLevelIndexAndFilter(
                tableConfig.isKvPinTopLevelIndexAndFilter());

        if (tableConfig.isKvBloomFilterEnabled()) {
            final double bitsPerKey = tableConfig.getKvBloomFilterBitsPerKey();
            final boolean blockBasedMode = tableConfig.isKvBloomFilterBlockBasedMode();
            BloomFilter bloomFilter = new BloomFilter(bitsPerKey, blockBasedMode);
            handlesToClose.add(bloomFilter);
            blockBasedTableConfig.setFilterPolicy(bloomFilter);
        }

        return currentOptions.setTableFormatConfig(blockBasedTableConfig);
    }

    /**
     * Relocates the default log directory of RocksDB with the Fluss log directory. Finds the Fluss
     * log directory using log.file Java property that is set during startup.
     *
     * @param dbOptions The RocksDB {@link DBOptions}.
     */
    private void relocateDefaultDbLogDir(DBOptions dbOptions) throws IOException {
        String logFilePath = System.getProperty("log.file");
        if (logFilePath != null) {
            File logFile = resolveFileLocation(logFilePath);
            if (logFile != null && resolveFileLocation(logFile.getParent()) != null) {
                File logFileDirectory = logFile.getParentFile();
                File rocksDbLogDirectory = FileUtils.createDirectory(logFileDirectory, "rocksdb");
                if (resolveFileLocation(rocksDbLogDirectory.getAbsolutePath()) != null) {
                    dbOptions.setDbLogDir(rocksDbLogDirectory.getAbsolutePath());
                }
            }
        }
    }

    File getInstanceRocksDBPath() {
        return instanceRocksDBPath;
    }

    private List<CompressionType> toRocksDbCompressionTypes(
            List<ConfigOptions.KvCompressionType> compressionTypes) {
        List<CompressionType> rocksdbCompressionTypes = new ArrayList<>();
        for (ConfigOptions.KvCompressionType compressionType : compressionTypes) {
            rocksdbCompressionTypes.add(toRocksDbCompressionType(compressionType));
        }
        return rocksdbCompressionTypes;
    }

    private CompressionType toRocksDbCompressionType(
            ConfigOptions.KvCompressionType compressionType) {
        switch (compressionType) {
            case NO:
                return CompressionType.NO_COMPRESSION;
            case LZ4:
                return CompressionType.LZ4_COMPRESSION;
            case SNAPPY:
                return CompressionType.SNAPPY_COMPRESSION;
            case ZSTD:
                return CompressionType.ZSTD_COMPRESSION;
            default:
                throw new IllegalArgumentException(
                        "Unsupported compression type: " + compressionType);
        }
    }

    private CompactionStyle toRocksDbCompactionStyle(
            ConfigOptions.CompactionStyle compactionStyle) {
        switch (compactionStyle) {
            case LEVEL:
                return CompactionStyle.LEVEL;
            case UNIVERSAL:
                return CompactionStyle.UNIVERSAL;
            case FIFO:
                return CompactionStyle.FIFO;
            case NONE:
                return CompactionStyle.NONE;
        }
        return CompactionStyle.NONE;
    }

    private InfoLogLevel toRocksDbInfoLogLevel(ConfigOptions.InfoLogLevel infoLogLevel) {
        switch (infoLogLevel) {
            case DEBUG_LEVEL:
                return InfoLogLevel.DEBUG_LEVEL;
            case INFO_LEVEL:
                return InfoLogLevel.INFO_LEVEL;
            case WARN_LEVEL:
                return InfoLogLevel.WARN_LEVEL;
            case ERROR_LEVEL:
                return InfoLogLevel.ERROR_LEVEL;
            case FATAL_LEVEL:
                return InfoLogLevel.FATAL_LEVEL;
            case HEADER_LEVEL:
                return InfoLogLevel.HEADER_LEVEL;
            case NUM_INFO_LOG_LEVELS:
                return InfoLogLevel.NUM_INFO_LOG_LEVELS;
        }
        return InfoLogLevel.INFO_LEVEL;
    }

    /**
     * Verify log file location.
     *
     * @param logFilePath Path to log file
     * @return File or null if not a valid log file
     */
    private File resolveFileLocation(String logFilePath) {
        File logFile = new File(logFilePath);
        return (logFile.exists() && logFile.canRead()) ? logFile : null;
    }
}

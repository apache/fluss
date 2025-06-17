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

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigOption;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.ReadableConfig;
import com.alibaba.fluss.utils.IOUtils;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.Statistics;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

    /** The configurations from file. */
    private final ReadableConfig configuration;

    private final boolean enableStatistics;

    /** The handles to be closed when the container is closed. */
    private final ArrayList<AutoCloseable> handlesToClose;

    @VisibleForTesting
    RocksDBResourceContainer() {
        this(new Configuration(), null, false);
    }

    public RocksDBResourceContainer(ReadableConfig configuration, @Nullable File instanceBasePath) {
        this(configuration, instanceBasePath, false);
    }

    public RocksDBResourceContainer(
            ReadableConfig configuration,
            @Nullable File instanceBasePath,
            boolean enableStatistics) {
        this.configuration = configuration;

        this.instanceRocksDBPath =
                instanceBasePath != null
                        ? RocksDBKvBuilder.getInstanceRocksDBPath(instanceBasePath)
                        : null;
        this.enableStatistics = enableStatistics;

        this.handlesToClose = new ArrayList<>();
    }

    /** Gets the RocksDB {@link DBOptions} to be used for RocksDB instances. */
    public DBOptions getDbOptions() {
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

        if (enableStatistics) {
            Statistics statistics = new Statistics();
            opt.setStatistics(statistics);
            handlesToClose.add(statistics);
        }

        return opt;
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

    @Nullable
    private <T> T internalGetOption(ConfigOption<T> option) {
        return configuration.get(option);
    }

    @SuppressWarnings("ConstantConditions")
    private DBOptions setDBOptionsFromConfigurableOptions(DBOptions currentOptions) {
        currentOptions.setMaxBackgroundJobs(
                internalGetOption(ConfigOptions.KV_MAX_BACKGROUND_THREADS));

        currentOptions.setMaxOpenFiles(internalGetOption(ConfigOptions.KV_MAX_OPEN_FILES));

        currentOptions.setInfoLogLevel(
                toRocksDbInfoLogLevel(internalGetOption(ConfigOptions.KV_LOG_LEVEL)));

        String logDir = internalGetOption(ConfigOptions.KV_LOG_DIR);
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

        currentOptions.setMaxLogFileSize(
                internalGetOption(ConfigOptions.KV_LOG_MAX_FILE_SIZE).getBytes());

        currentOptions.setKeepLogFileNum(internalGetOption(ConfigOptions.KV_LOG_FILE_NUM));

        return currentOptions;
    }

    @SuppressWarnings("ConstantConditions")
    private ColumnFamilyOptions setColumnFamilyOptionsFromConfigurableOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {

        currentOptions.setCompactionStyle(
                toRocksDbCompactionStyle(internalGetOption(ConfigOptions.KV_COMPACTION_STYLE)));

        currentOptions.setCompressionPerLevel(
                toRocksDbCompressionTypes(
                        internalGetOption(ConfigOptions.KV_COMPRESSION_PER_LEVEL)));

        currentOptions.setLevelCompactionDynamicLevelBytes(
                internalGetOption(ConfigOptions.KV_USE_DYNAMIC_LEVEL_SIZE));

        currentOptions.setTargetFileSizeBase(
                internalGetOption(ConfigOptions.KV_TARGET_FILE_SIZE_BASE).getBytes());

        currentOptions.setMaxBytesForLevelBase(
                internalGetOption(ConfigOptions.KV_MAX_SIZE_LEVEL_BASE).getBytes());

        currentOptions.setWriteBufferSize(
                internalGetOption(ConfigOptions.KV_WRITE_BUFFER_SIZE).getBytes());

        currentOptions.setMaxWriteBufferNumber(
                internalGetOption(ConfigOptions.KV_MAX_WRITE_BUFFER_NUMBER));

        currentOptions.setMinWriteBufferNumberToMerge(
                internalGetOption(ConfigOptions.KV_MIN_WRITE_BUFFER_NUMBER_TO_MERGE));

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

        blockBasedTableConfig.setBlockSize(
                internalGetOption(ConfigOptions.KV_BLOCK_SIZE).getBytes());

        blockBasedTableConfig.setMetadataBlockSize(
                internalGetOption(ConfigOptions.KV_METADATA_BLOCK_SIZE).getBytes());

        blockBasedTableConfig.setBlockCacheSize(
                internalGetOption(ConfigOptions.KV_BLOCK_CACHE_SIZE).getBytes());

        if (internalGetOption(ConfigOptions.KV_USE_BLOOM_FILTER)) {
            final double bitsPerKey = internalGetOption(ConfigOptions.KV_BLOOM_FILTER_BITS_PER_KEY);
            final boolean blockBasedMode =
                    internalGetOption(ConfigOptions.KV_BLOOM_FILTER_BLOCK_BASED_MODE);
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
    private void relocateDefaultDbLogDir(DBOptions dbOptions) {
        String logFilePath = System.getProperty("log.file");
        if (logFilePath != null) {
            File logFile = resolveFileLocation(logFilePath);
            if (logFile != null && resolveFileLocation(logFile.getParent()) != null) {
                dbOptions.setDbLogDir(logFile.getParent());
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

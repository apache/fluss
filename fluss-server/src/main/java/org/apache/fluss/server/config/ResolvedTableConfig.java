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
    public MemorySize getKvWriteBatchSize() {
        return getResolved(
                ConfigOptions.TABLE_KV_WRITE_BATCH_SIZE, ConfigOptions.KV_WRITE_BATCH_SIZE);
    }

    /** Gets the RocksDB background thread count resolved from table or server configuration. */
    public int getKvMaxBackgroundThreads() {
        return getResolved(
                ConfigOptions.TABLE_KV_MAX_BACKGROUND_THREADS,
                ConfigOptions.KV_MAX_BACKGROUND_THREADS);
    }

    /**
     * Gets the RocksDB write buffer size resolved from table properties or server configuration.
     */
    public MemorySize getKvWriteBufferSize() {
        return getResolved(
                ConfigOptions.TABLE_KV_WRITE_BUFFER_SIZE, ConfigOptions.KV_WRITE_BUFFER_SIZE);
    }

    /**
     * Gets the RocksDB write buffer count resolved from table properties or server configuration.
     */
    public int getKvMaxWriteBufferNumber() {
        return getResolved(
                ConfigOptions.TABLE_KV_MAX_WRITE_BUFFER_NUMBER,
                ConfigOptions.KV_MAX_WRITE_BUFFER_NUMBER);
    }

    private <T> T getResolved(ConfigOption<T> tableOption, ConfigOption<T> serverOption) {
        if (tableConfig.contains(tableOption)) {
            return tableConfig.get(tableOption);
        }
        return serverConfig.get(serverOption);
    }
}

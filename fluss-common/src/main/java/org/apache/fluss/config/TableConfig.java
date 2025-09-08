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
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.utils.AutoPartitionStrategy;

import java.time.Duration;
import java.util.Optional;

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

    /** Gets the Arrow compression type and compression level of the table. */
    public ArrowCompressionInfo getArrowCompressionInfo() {
        return ArrowCompressionInfo.fromConf(config);
    }

    /** Gets the auto partition strategy of the table. */
    public AutoPartitionStrategy getAutoPartitionStrategy() {
        return AutoPartitionStrategy.from(config);
    }
}

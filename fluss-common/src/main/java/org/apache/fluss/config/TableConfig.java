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
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.utils.AutoPartitionStrategy;

import javax.annotation.Nullable;

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

    /**
     * Gets the aggregate function for a specific field.
     *
     * @param fieldName the field name
     * @return the aggregate function name, or null if not configured
     */
    @Nullable
    public String getFieldAggregateFunction(String fieldName) {
        String key = ConfigOptions.AGGREGATE_MERGE_ENGINE_PREFIX + "." + fieldName;
        return config.toMap().get(key);
    }

    /**
     * Gets the default aggregate function for all non-primary-key fields.
     *
     * @return the default aggregate function name, or null if not configured
     */
    @Nullable
    public String getDefaultAggregateFunction() {
        return config.get(ConfigOptions.TABLE_AGGREGATE_DEFAULT_FUNCTION);
    }

    /**
     * Whether to remove record on delete for aggregate merge engine.
     *
     * @return true if record should be removed on delete, false otherwise
     */
    public boolean getAggregationRemoveRecordOnDelete() {
        return config.get(ConfigOptions.TABLE_AGG_REMOVE_RECORD_ON_DELETE);
    }

    /**
     * Whether to ignore retract for a specific field in aggregate merge engine.
     *
     * @param fieldName the field name
     * @return true if retract should be ignored, false otherwise
     */
    public boolean getFieldIgnoreRetract(String fieldName) {
        String key =
                ConfigOptions.AGGREGATE_MERGE_ENGINE_PREFIX + "." + fieldName + ".ignore-retract";
        String value = config.toMap().get(key);
        return value != null && Boolean.parseBoolean(value);
    }

    /**
     * Gets the listagg delimiter for a specific field in aggregate merge engine.
     *
     * @param fieldName the field name
     * @return the delimiter string, default to comma if not configured
     */
    public String getFieldListaggDelimiter(String fieldName) {
        String key =
                ConfigOptions.AGGREGATE_MERGE_ENGINE_PREFIX
                        + "."
                        + fieldName
                        + ".listagg-delimiter";
        String value = config.toMap().get(key);
        return value != null ? value : ",";
    }

    /** Gets the delete behavior of the table. */
    public Optional<DeleteBehavior> getDeleteBehavior() {
        return config.getOptional(ConfigOptions.TABLE_DELETE_BEHAVIOR);
    }

    /** Gets the Arrow compression type and compression level of the table. */
    public ArrowCompressionInfo getArrowCompressionInfo() {
        return ArrowCompressionInfo.fromConf(config);
    }

    /** Gets the auto partition strategy of the table. */
    public AutoPartitionStrategy getAutoPartitionStrategy() {
        return AutoPartitionStrategy.from(config);
    }

    /**
     * Gets the value for a given {@link ConfigOption}.
     *
     * @param option the config option
     * @param <T> the type of the value
     * @return the value of the config option
     */
    public <T> T get(ConfigOption<T> option) {
        return config.get(option);
    }
}

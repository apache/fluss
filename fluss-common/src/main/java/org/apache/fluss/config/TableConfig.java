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
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.utils.AutoPartitionStrategy;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

    /**
     * Gets the kv format version of the table. This is used for backward compatibility when
     * encoding strategy changes. Returns empty if the table was created before the version was
     * introduced (old tables).
     */
    public Optional<Integer> getKvFormatVersion() {
        return config.getOptional(ConfigOptions.TABLE_KV_FORMAT_VERSION);
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

    /** Whether auto expire snapshot is enabled. */
    public boolean isDataLakeAutoExpireSnapshot() {
        return config.get(ConfigOptions.TABLE_DATALAKE_AUTO_EXPIRE_SNAPSHOT);
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

    /** Gets the delete behavior of the table. */
    public Optional<DeleteBehavior> getDeleteBehavior() {
        return config.getOptional(ConfigOptions.TABLE_DELETE_BEHAVIOR);
    }

    /**
     * Gets the changelog image mode of the table. The changelog image mode defines what information
     * is included in the changelog for update operations.
     */
    public ChangelogImage getChangelogImage() {
        return config.get(ConfigOptions.TABLE_CHANGELOG_IMAGE);
    }

    /** Gets the Arrow compression type and compression level of the table. */
    public ArrowCompressionInfo getArrowCompressionInfo() {
        return ArrowCompressionInfo.fromConf(config);
    }

    /** Gets the auto partition strategy of the table. */
    public AutoPartitionStrategy getAutoPartitionStrategy() {
        return AutoPartitionStrategy.from(config);
    }

    /** Gets the number of auto-increment IDs cached per segment. */
    public long getAutoIncrementCacheSize() {
        return config.get(ConfigOptions.TABLE_AUTO_INCREMENT_CACHE_SIZE);
    }

    /** Gets whether statistics collection is enabled for the table. */
    public boolean isStatisticsEnabled() {
        return getStatisticsColumns().isEnabled();
    }

    /**
     * Gets the statistics columns configuration of the table.
     *
     * @return a {@link StatisticsColumnsConfig} representing the statistics collection mode:
     *     DISABLED if not configured, ALL if "*", or SPECIFIED with the list of column names.
     */
    public StatisticsColumnsConfig getStatisticsColumns() {
        String columnsStr = config.get(ConfigOptions.TABLE_STATISTICS_COLUMNS);
        if (columnsStr == null) {
            return StatisticsColumnsConfig.disabled();
        }
        if ("*".equals(columnsStr)) {
            return StatisticsColumnsConfig.all();
        }
        List<String> columns =
                Arrays.stream(columnsStr.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
        return StatisticsColumnsConfig.of(columns);
    }

    /**
     * Returns whether automatic Variant shredding is enabled for this table.
     *
     * <p>When enabled, the write path will collect field statistics for Variant columns and
     * automatically trigger schema evolution to extract frequently-occurring, type-consistent
     * fields into independent typed columns.
     */
    public boolean isVariantShreddingEnabled() {
        return config.get(ConfigOptions.TABLE_VARIANT_SHREDDING_ENABLED);
    }

    /**
     * Returns the minimum presence ratio threshold for Variant field shredding.
     *
     * <p>A Variant field must appear in at least this fraction of total records to be eligible for
     * shredding. Range: 0.0 to 1.0.
     */
    public float getVariantShreddingPresenceThreshold() {
        return config.get(ConfigOptions.TABLE_VARIANT_SHREDDING_PRESENCE_THRESHOLD).floatValue();
    }

    /**
     * Returns the minimum type-consistency ratio threshold for Variant field shredding.
     *
     * <p>At least this fraction of a field's occurrences must share the same type. Range: 0.0 to
     * 1.0.
     */
    public float getVariantShreddingTypeConsistencyThreshold() {
        return config.get(ConfigOptions.TABLE_VARIANT_SHREDDING_TYPE_CONSISTENCY_THRESHOLD)
                .floatValue();
    }

    /** Returns the maximum number of fields that can be shredded from a single Variant column. */
    public int getVariantShreddingMaxFields() {
        return config.get(ConfigOptions.TABLE_VARIANT_SHREDDING_MAX_FIELDS);
    }

    /**
     * Returns the minimum number of records that must be observed before the shredding inferrer
     * produces a schema.
     */
    public int getVariantShreddingMinSampleSize() {
        return config.get(ConfigOptions.TABLE_VARIANT_SHREDDING_MIN_SAMPLE_SIZE);
    }
}

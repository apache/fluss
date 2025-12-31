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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.ReadableConfig;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.exception.InvalidAlterTableException;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.exception.InvalidTableException;
import org.apache.fluss.exception.TooManyBucketsException;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.AutoPartitionStrategy;
import org.apache.fluss.utils.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.config.FlussConfigUtils.TABLE_OPTIONS;
import static org.apache.fluss.config.FlussConfigUtils.isAlterableTableOption;
import static org.apache.fluss.config.FlussConfigUtils.isTableStorageConfig;
import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.apache.fluss.utils.PartitionUtils.PARTITION_KEY_SUPPORTED_TYPES;

/** Validator of {@link TableDescriptor}. */
public class TableDescriptorValidation {

    private static final Set<String> SYSTEM_COLUMNS =
            Collections.unmodifiableSet(
                    new LinkedHashSet<>(
                            Arrays.asList(
                                    OFFSET_COLUMN_NAME,
                                    TIMESTAMP_COLUMN_NAME,
                                    BUCKET_COLUMN_NAME)));

    private static final List<DataTypeRoot> KEY_UNSUPPORTED_TYPES =
            Arrays.asList(DataTypeRoot.ARRAY, DataTypeRoot.MAP, DataTypeRoot.ROW);

    /** Validate table descriptor to create is valid and contain all necessary information. */
    public static void validateTableDescriptor(TableDescriptor tableDescriptor, int maxBucketNum) {
        Schema schema = tableDescriptor.getSchema();
        boolean hasPrimaryKey = schema.getPrimaryKey().isPresent();
        Configuration tableConf = Configuration.fromMap(tableDescriptor.getProperties());

        // check properties should only contain table.* options,
        // and this cluster know it, and value is valid
        for (String key : tableConf.keySet()) {

            if (!TABLE_OPTIONS.containsKey(key)) {
                if (isTableStorageConfig(key)) {
                    throw new InvalidConfigException(
                            String.format(
                                    "'%s' is not a recognized Fluss table property in the current cluster version. "
                                            + "You may be using an older Fluss cluster that does not support this property.",
                                    key));
                } else {
                    throw new InvalidConfigException(
                            String.format(
                                    "'%s' is not a Fluss table property. Please use '.customProperty(..)' to set custom properties.",
                                    key));
                }
            }
            ConfigOption<?> option = TABLE_OPTIONS.get(key);
            validateOptionValue(tableConf, option);
        }

        // check distribution
        checkDistribution(tableDescriptor, maxBucketNum);
        checkPrimaryKey(tableDescriptor);
        // check individual options
        checkReplicationFactor(tableConf);
        checkLogFormat(tableConf, hasPrimaryKey);
        checkArrowCompression(tableConf);
        checkMergeEngine(tableConf, hasPrimaryKey, schema);
        checkDeleteBehavior(tableConf, hasPrimaryKey);
        checkTieredLog(tableConf);
        checkPartition(tableConf, tableDescriptor.getPartitionKeys(), schema.getRowType());
        checkSystemColumns(schema.getRowType());
    }

    public static void validateAlterTableProperties(
            TableInfo currentTable, Set<String> tableKeysToChange, Set<String> customKeysToChange) {
        tableKeysToChange.forEach(
                k -> {
                    if (isTableStorageConfig(k) && !isAlterableTableOption(k)) {
                        throw new InvalidAlterTableException(
                                "The option '" + k + "' is not supported to alter yet.");
                    }
                });

        TableConfig currentConfig = currentTable.getTableConfig();
        if (currentConfig.isDataLakeEnabled() && currentConfig.getDataLakeFormat().isPresent()) {
            String format = currentConfig.getDataLakeFormat().get().toString();
            customKeysToChange.forEach(
                    k -> {
                        if (k.startsWith(format + ".")) {
                            throw new InvalidConfigException(
                                    String.format(
                                            "Property '%s' is not supported to alter which is for datalake table.",
                                            k));
                        }
                    });
        }
    }

    private static void checkSystemColumns(RowType schema) {
        List<String> fieldNames = schema.getFieldNames();
        List<String> unsupportedColumns =
                fieldNames.stream().filter(SYSTEM_COLUMNS::contains).collect(Collectors.toList());
        if (!unsupportedColumns.isEmpty()) {
            throw new InvalidTableException(
                    String.format(
                            "%s cannot be used as column names, "
                                    + "because they are reserved system columns in Fluss. "
                                    + "Please use other names for these columns. "
                                    + "The reserved system columns are: %s",
                            String.join(", ", unsupportedColumns),
                            String.join(", ", SYSTEM_COLUMNS)));
        }
    }

    private static void checkDistribution(TableDescriptor tableDescriptor, int maxBucketNum) {
        if (!tableDescriptor.getTableDistribution().isPresent()) {
            throw new InvalidTableException("Table distribution is required.");
        }
        if (!tableDescriptor.getTableDistribution().get().getBucketCount().isPresent()) {
            throw new InvalidTableException("Bucket number must be set.");
        }
        int bucketCount = tableDescriptor.getTableDistribution().get().getBucketCount().get();
        if (bucketCount > maxBucketNum) {
            throw new TooManyBucketsException(
                    String.format(
                            "Bucket count %s exceeds the maximum limit %s.",
                            bucketCount, maxBucketNum));
        }
        List<String> bucketKeys = tableDescriptor.getTableDistribution().get().getBucketKeys();
        if (!bucketKeys.isEmpty()) {
            // check bucket key type
            RowType schema = tableDescriptor.getSchema().getRowType();
            for (String bkColumn : bucketKeys) {
                int bkIndex = schema.getFieldIndex(bkColumn);
                DataType bkDataType = schema.getTypeAt(bkIndex);
                if (KEY_UNSUPPORTED_TYPES.contains(bkDataType.getTypeRoot())) {
                    throw new InvalidTableException(
                            String.format(
                                    "Bucket key column '%s' has unsupported data type %s. "
                                            + "Currently, bucket key column does not support types: %s.",
                                    bkColumn, bkDataType, KEY_UNSUPPORTED_TYPES));
                }
            }
        }
    }

    private static void checkPrimaryKey(TableDescriptor tableDescriptor) {
        if (tableDescriptor.hasPrimaryKey()) {
            // check primary key type
            RowType schema = tableDescriptor.getSchema().getRowType();
            Schema.PrimaryKey primaryKey = tableDescriptor.getSchema().getPrimaryKey().get();
            for (String pkColumn : primaryKey.getColumnNames()) {
                int pkIndex = schema.getFieldIndex(pkColumn);
                DataType pkDataType = schema.getTypeAt(pkIndex);
                if (KEY_UNSUPPORTED_TYPES.contains(pkDataType.getTypeRoot())) {
                    throw new InvalidTableException(
                            String.format(
                                    "Primary key column '%s' has unsupported data type %s. "
                                            + "Currently, primary key column does not support types: %s.",
                                    pkColumn, pkDataType, KEY_UNSUPPORTED_TYPES));
                }
            }
        }
    }

    private static void checkReplicationFactor(Configuration tableConf) {
        if (!tableConf.containsKey(ConfigOptions.TABLE_REPLICATION_FACTOR.key())) {
            throw new InvalidConfigException(
                    String.format(
                            "Missing required table property '%s'.",
                            ConfigOptions.TABLE_REPLICATION_FACTOR.key()));
        }
        if (tableConf.get(ConfigOptions.TABLE_REPLICATION_FACTOR) <= 0) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must be greater than 0.",
                            ConfigOptions.TABLE_REPLICATION_FACTOR.key()));
        }
    }

    private static void checkLogFormat(Configuration tableConf, boolean hasPrimaryKey) {
        KvFormat kvFormat = tableConf.get(ConfigOptions.TABLE_KV_FORMAT);
        LogFormat logFormat = tableConf.get(ConfigOptions.TABLE_LOG_FORMAT);

        // Allow COMPACTED and ARROW log formats when KV format is COMPACTED for primary key tables
        if (hasPrimaryKey
                && kvFormat == KvFormat.COMPACTED
                && !(logFormat == LogFormat.ARROW || logFormat == LogFormat.COMPACTED)) {
            throw new InvalidConfigException(
                    "Currently, Primary Key Table supports ARROW or COMPACTED log format when kv format is COMPACTED.");
        }
    }

    private static void checkArrowCompression(Configuration tableConf) {
        if (tableConf.containsKey(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL.key())) {
            int compressionLevel =
                    tableConf.get(ConfigOptions.TABLE_LOG_ARROW_COMPRESSION_ZSTD_LEVEL);
            if (compressionLevel < 1 || compressionLevel > 22) {
                throw new InvalidConfigException(
                        "Invalid ZSTD compression level: "
                                + compressionLevel
                                + ". Expected a value between 1 and 22.");
            }
        }
    }

    private static void checkMergeEngine(
            Configuration tableConf, boolean hasPrimaryKey, Schema schema) {
        MergeEngineType mergeEngine = tableConf.get(ConfigOptions.TABLE_MERGE_ENGINE);
        if (mergeEngine != null) {
            if (!hasPrimaryKey) {
                throw new InvalidConfigException(
                        "Merge engine is only supported in primary key table.");
            }
            if (mergeEngine == MergeEngineType.VERSIONED) {
                Optional<String> versionColumn =
                        tableConf.getOptional(ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN);
                if (!versionColumn.isPresent()) {
                    throw new InvalidConfigException(
                            String.format(
                                    "'%s' must be set for versioned merge engine.",
                                    ConfigOptions.TABLE_MERGE_ENGINE_VERSION_COLUMN.key()));
                }
                RowType rowType = schema.getRowType();
                int columnIndex = rowType.getFieldIndex(versionColumn.get());
                if (columnIndex < 0) {
                    throw new InvalidConfigException(
                            String.format(
                                    "The version column '%s' for versioned merge engine doesn't exist in schema.",
                                    versionColumn.get()));
                }
                EnumSet<DataTypeRoot> supportedTypes =
                        EnumSet.of(
                                DataTypeRoot.INTEGER,
                                DataTypeRoot.BIGINT,
                                DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                                DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
                DataType columnType = rowType.getTypeAt(columnIndex);
                if (!supportedTypes.contains(columnType.getTypeRoot())) {
                    throw new InvalidConfigException(
                            String.format(
                                    "The version column '%s' for versioned merge engine must be one type of "
                                            + "[INT, BIGINT, TIMESTAMP, TIMESTAMP_LTZ]"
                                            + ", but got %s.",
                                    versionColumn.get(), columnType));
                }
            } else if (mergeEngine == MergeEngineType.AGGREGATION) {
                // Validate aggregation function parameters for aggregation merge engine
                validateAggregationFunctionParameters(schema);
            }
        }
    }

    /**
     * Validates aggregation function parameters in the schema.
     *
     * <p>This method delegates to {@link AggFunction#validate()} to ensure all parameters are valid
     * according to the function's requirements.
     *
     * @param schema the schema to validate
     * @throws InvalidConfigException if any aggregation function has invalid parameters
     */
    private static void validateAggregationFunctionParameters(Schema schema) {
        // Get primary key columns for early exit
        List<String> primaryKeys = schema.getPrimaryKeyColumnNames();

        for (Schema.Column column : schema.getColumns()) {
            // Skip primary key columns (they don't use user-defined aggregation functions)
            if (primaryKeys.contains(column.getName())) {
                continue;
            }

            Optional<AggFunction> aggFunctionOpt = column.getAggFunction();
            if (!aggFunctionOpt.isPresent()) {
                continue;
            }

            // Validate aggregation function parameters
            try {
                aggFunctionOpt.get().validate();
            } catch (IllegalArgumentException e) {
                throw new InvalidConfigException(
                        String.format(
                                "Invalid aggregation function for column '%s': %s",
                                column.getName(), e.getMessage()));
            }
        }
    }

    private static void checkTieredLog(Configuration tableConf) {
        if (tableConf.get(ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS) <= 0) {
            throw new InvalidConfigException(
                    String.format(
                            "'%s' must be greater than 0.",
                            ConfigOptions.TABLE_TIERED_LOG_LOCAL_SEGMENTS.key()));
        }
    }

    private static void checkPartition(
            Configuration tableConf, List<String> partitionKeys, RowType rowType) {
        boolean isPartitioned = !partitionKeys.isEmpty();
        AutoPartitionStrategy autoPartition = AutoPartitionStrategy.from(tableConf);

        if (!isPartitioned && autoPartition.isAutoPartitionEnabled()) {
            throw new InvalidConfigException(
                    String.format(
                            "Currently, auto partition is only supported for partitioned table, please set table property '%s' to false.",
                            ConfigOptions.TABLE_AUTO_PARTITION_ENABLED.key()));
        }

        if (isPartitioned) {
            for (String partitionKey : partitionKeys) {
                int partitionIndex = rowType.getFieldIndex(partitionKey);
                DataType partitionDataType = rowType.getTypeAt(partitionIndex);
                if (!PARTITION_KEY_SUPPORTED_TYPES.contains(partitionDataType.getTypeRoot())) {
                    throw new InvalidTableException(
                            String.format(
                                    "Currently, partitioned table supported partition key type are %s, "
                                            + "but got partition key '%s' with data type %s.",
                                    PARTITION_KEY_SUPPORTED_TYPES,
                                    partitionKey,
                                    partitionDataType));
                }
            }

            if (autoPartition.isAutoPartitionEnabled()) {
                if (partitionKeys.size() > 1) {
                    // must specify auto partition key for auto partition table when optional keys
                    // size > 1
                    if (StringUtils.isNullOrWhitespaceOnly(autoPartition.key())) {
                        throw new InvalidTableException(
                                String.format(
                                        "Currently, auto partitioned table must set one auto partition key when it "
                                                + "has multiple partition keys. Please set table property '%s'.",
                                        ConfigOptions.TABLE_AUTO_PARTITION_KEY.key()));
                    }

                    if (!partitionKeys.contains(autoPartition.key())) {
                        throw new InvalidTableException(
                                String.format(
                                        "The specified key for auto partitioned table is not a partition key. "
                                                + "Your key '%s' is not in key list %s",
                                        autoPartition.key(), partitionKeys));
                    }

                    if (autoPartition.numPreCreate() > 0) {
                        throw new InvalidTableException(
                                "For a partitioned table with multiple partition keys, auto pre-create is unsupported and this value must be set to 0, but is "
                                        + autoPartition.numPreCreate());
                    }
                }

                if (autoPartition.timeUnit() == null) {
                    throw new InvalidTableException(
                            String.format(
                                    "Currently, auto partitioned table must set auto partition time unit when auto "
                                            + "partition is enabled, please set table property '%s'.",
                                    ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT.key()));
                }
            }
        }
    }

    private static void checkDeleteBehavior(Configuration tableConf, boolean hasPrimaryKey) {
        Optional<DeleteBehavior> deleteBehaviorOptional =
                tableConf.getOptional(ConfigOptions.TABLE_DELETE_BEHAVIOR);
        if (!hasPrimaryKey && deleteBehaviorOptional.isPresent()) {
            throw new InvalidConfigException(
                    "The 'table.delete.behavior' configuration is only supported for primary key tables.");
        }

        // For tables with merge engines, automatically set appropriate delete behavior
        MergeEngineType mergeEngine = tableConf.get(ConfigOptions.TABLE_MERGE_ENGINE);
        if (mergeEngine == MergeEngineType.FIRST_ROW
                || mergeEngine == MergeEngineType.VERSIONED
                || mergeEngine == MergeEngineType.AGGREGATION) {
            // For FIRST_ROW, VERSIONED and AGGREGATION merge engines, delete operations are not
            // supported by default
            // If user explicitly sets delete behavior to ALLOW, validate it
            if (deleteBehaviorOptional.isPresent()
                    && deleteBehaviorOptional.get() == DeleteBehavior.ALLOW) {
                // For FIRST_ROW and VERSIONED, ALLOW is not permitted
                if (mergeEngine == MergeEngineType.FIRST_ROW
                        || mergeEngine == MergeEngineType.VERSIONED) {
                    throw new InvalidConfigException(
                            String.format(
                                    "Table with '%s' merge engine does not support delete operations. "
                                            + "The 'table.delete.behavior' config must be set to 'ignore' or 'disable', but got 'allow'.",
                                    mergeEngine));
                }
                // For AGGREGATION, ALLOW is permitted (removes entire record)
            }
        }
    }

    private static void validateOptionValue(ReadableConfig options, ConfigOption<?> option) {
        try {
            options.get(option);
        } catch (Throwable t) {
            throw new InvalidConfigException(
                    String.format(
                            "Invalid value for config '%s'. Reason: %s",
                            option.key(), t.getMessage()));
        }
    }
}

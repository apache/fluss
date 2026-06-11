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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.StatisticsColumnsConfig;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeChecks;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Information of a created table metadata, includes table id (unique identifier of the table in the
 * cluster), schema, distribution, partitioning, etc.
 *
 * <p>{@link TableDescriptor} is an unresolved metadata of a table to create, which may contain
 * optional distribution and options. {@link TableInfo} is a resolved metadata of a table that has
 * been created, which contains deterministic distribution and options, and provides rich getters.
 * It is suggested to use {@link TableDescriptor} to create tables, and use {@link TableInfo} for
 * managing metadata of a created table.
 *
 * @since 0.1
 */
@PublicEvolving
public final class TableInfo {

    public static final long UNKNOWN_TABLE_ID = -1L;

    private final TablePath tablePath;
    private final long tableId;
    private final int schemaId;
    private final Schema schema;
    private final RowType rowType;
    private final List<String> primaryKeys;
    private final List<String> physicalPrimaryKeys;
    private final List<String> bucketKeys;
    private final List<String> partitionKeys;
    private final List<PartitionExpression> partitionExpressions;
    private final int numBuckets;
    private final Configuration properties;
    private final TableConfig tableConfig;
    private final Configuration customProperties;
    private final @Nullable String remoteDataDir;
    private final @Nullable String comment;

    private final long createdTime;
    private final long modifiedTime;

    private int[] cachedStatsIndexMapping = null;

    public TableInfo(
            TablePath tablePath,
            long tableId,
            int schemaId,
            Schema schema,
            List<String> bucketKeys,
            List<String> partitionKeys,
            int numBuckets,
            Configuration properties,
            Configuration customProperties,
            @Nullable String remoteDataDir,
            @Nullable String comment,
            long createdTime,
            long modifiedTime) {
        this(
                tablePath,
                tableId,
                schemaId,
                schema,
                bucketKeys,
                partitionKeys,
                Collections.emptyList(),
                numBuckets,
                properties,
                customProperties,
                remoteDataDir,
                comment,
                createdTime,
                modifiedTime);
    }

    public TableInfo(
            TablePath tablePath,
            long tableId,
            int schemaId,
            Schema schema,
            List<String> bucketKeys,
            List<String> partitionKeys,
            List<PartitionExpression> partitionExpressions,
            int numBuckets,
            Configuration properties,
            Configuration customProperties,
            @Nullable String remoteDataDir,
            @Nullable String comment,
            long createdTime,
            long modifiedTime) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.schema = schema;
        this.rowType = schema.getRowType();
        this.primaryKeys = schema.getPrimaryKeyColumnNames();
        this.bucketKeys = Collections.unmodifiableList(new ArrayList<>(bucketKeys));
        this.partitionKeys = Collections.unmodifiableList(new ArrayList<>(partitionKeys));
        this.partitionExpressions =
                Collections.unmodifiableList(new ArrayList<>(partitionExpressions));
        validatePartitionMetadata(schema, this.partitionKeys, this.partitionExpressions);
        this.physicalPrimaryKeys =
                generatePhysicalPrimaryKey(primaryKeys, getPhysicalPartitionKeys());
        this.numBuckets = numBuckets;
        this.properties = properties;
        this.tableConfig = new TableConfig(properties);
        this.customProperties = customProperties;
        this.remoteDataDir = remoteDataDir;
        this.comment = comment;
        this.createdTime = createdTime;
        this.modifiedTime = modifiedTime;
    }

    /**
     * Returns the database name and table name of the table that represented by this table path. A
     * table path is unique in a Fluss cluster and can be used to identify a table at one moment.
     */
    public TablePath getTablePath() {
        return tablePath;
    }

    /**
     * Returns the unique identifier for the table within the cluster.
     *
     * <p>Each table is assigned a globally unique table ID when it is created. This ID is
     * incremented sequentially with each new table creation, ensuring no two tables share the same
     * ID, even if they share the same name at different times.
     *
     * <p>Note that if a table with a previously used name is recreated after being dropped, it will
     * receive a new table ID. The table ID serves as a persistent and reliable way to identify and
     * reference a table within the cluster at any given time.
     *
     * @return the globally unique identifier for the table.
     */
    public long getTableId() {
        return tableId;
    }

    /**
     * Returns the schema ID of the table. The schema ID is a 0-based index that is incremented each
     * time the table's schema is modified, such as when a new column is added. This ID serves as a
     * unique identifier for the current schema version of the table and helps track changes to the
     * table's structure over time.
     *
     * @return the current schema ID of the table.
     */
    public int getSchemaId() {
        return schemaId;
    }

    /** Returns the schema of the table. The schema defines the columns and types of the table. */
    public Schema getSchema() {
        return schema;
    }

    /** Returns the schema info of the table, including schema and schema id. */
    public SchemaInfo getSchemaInfo() {
        return new SchemaInfo(schema, schemaId);
    }

    /**
     * Returns the row type of the table. The row type is the schema of the table, which defines the
     * columns and types of the table.
     */
    public RowType getRowType() {
        return rowType;
    }

    /** Check if the table has primary key or not. */
    public boolean hasPrimaryKey() {
        return !primaryKeys.isEmpty();
    }

    /**
     * Returns the logical primary keys of the table. The logical primary keys are the defined
     * PRIMARY KEY clause when creating table.
     */
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    /**
     * Returns the physical primary keys of the table. The physical primary keys are the columns
     * used to physically encoding as the unique key of a row when storing rows. As data is
     * partitioned by partition keys first, all data in a partition has the same partition column
     * values, and thus Fluss doesn't encode partition keys in the unique key to reduce cost and
     * improve performance.
     *
     * <p>The physical primary keys are the {@link #getPrimaryKeys() logical primary keys} excluded
     * {@link #getPartitionKeys() partition keys}.
     */
    public List<String> getPhysicalPrimaryKeys() {
        return physicalPrimaryKeys;
    }

    /**
     * Check if the table defines bucket key or not. If bucket key is defined, the writes to the
     * table has to be shuffled by the bucket key.
     */
    public boolean hasBucketKey() {
        return !bucketKeys.isEmpty();
    }

    /**
     * Check if the table is using a default bucket key. A default bucket key:
     *
     * <ul>
     *   <li>is the same with {@link #physicalPrimaryKeys} if the table is a primary key table.
     *   <li>is empty if the table is a log table.
     * </ul>
     */
    public boolean isDefaultBucketKey() {
        if (hasPrimaryKey()) {
            return bucketKeys.equals(physicalPrimaryKeys);
        } else {
            return bucketKeys.isEmpty();
        }
    }

    /** Get the bucket keys of the table. This will be an empty set if the table is not bucketed. */
    public List<String> getBucketKeys() {
        return bucketKeys;
    }

    /**
     * Check if the table is partitioned or not.
     *
     * @return true if the table is partitioned; otherwise, false
     */
    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    /** Check if the table is partitioned and auto partition is enabled. */
    public boolean isAutoPartitioned() {
        return isPartitioned() && tableConfig.getAutoPartitionStrategy().isAutoPartitionEnabled();
    }

    /**
     * Check if statistics collection is enabled for this table.
     *
     * <p>Statistics collection helps optimize query performance by providing data distribution
     * information about the table's columns. When enabled, the system collects and maintains
     * statistical information such as min/max values, null counts, and cardinality estimates.
     *
     * @return true if statistics collection is enabled, false otherwise
     */
    public boolean isStatisticsEnabled() {
        return tableConfig.isStatisticsEnabled();
    }

    /**
     * Returns the column index mapping for statistics collection.
     *
     * <p>This method provides a mapping array where each element at index {@code i} represents the
     * original column index in the table schema for the {@code i}-th statistics column. This
     * mapping is used to efficiently project rows when collecting statistics data.
     *
     * <p>The behavior varies based on the statistics configuration:
     *
     * <ul>
     *   <li><b>Disabled</b>: Returns an empty array when statistics collection is disabled
     *   <li><b>Specific columns</b>: When specific columns are configured, returns mapping for
     *       those columns only
     *   <li><b>All columns ("*")</b>: Returns mapping for all supported columns in the table schema
     *       (unsupported types are excluded as they are not suitable for statistical analysis)
     * </ul>
     *
     * <p>The result is cached after the first computation to avoid repeated processing.
     *
     * @return an array where {@code result[i]} is the original column index for the {@code i}-th
     *     statistics column. Returns an empty array if statistics collection is disabled.
     * @throws IllegalArgumentException if a configured statistics column is not found in the table
     *     schema
     */
    public int[] getStatsIndexMapping() {
        if (cachedStatsIndexMapping != null) {
            return cachedStatsIndexMapping;
        }

        StatisticsColumnsConfig statsConfig = tableConfig.getStatisticsColumns();
        List<String> statsColumns;

        switch (statsConfig.getMode()) {
            case DISABLED:
                cachedStatsIndexMapping = new int[0];
                return cachedStatsIndexMapping;
            case SPECIFIED:
                statsColumns = statsConfig.getColumns();
                break;
            case ALL:
                // Collect all supported columns
                statsColumns = new ArrayList<>();
                for (int rowIndex = 0; rowIndex < rowType.getFieldCount(); rowIndex++) {
                    DataType columnType = rowType.getTypeAt(rowIndex);
                    if (DataTypeChecks.isSupportedStatisticsType(columnType)) {
                        String columnName = rowType.getFields().get(rowIndex).getName();
                        statsColumns.add(columnName);
                    }
                }
                break;
            default:
                throw new IllegalStateException(
                        "Unknown statistics columns mode: " + statsConfig.getMode());
        }

        // Build mapping from stats column index to original row column index
        int[] mapping = new int[statsColumns.size()];
        for (int statsIndex = 0; statsIndex < statsColumns.size(); statsIndex++) {
            String statsColumnName = statsColumns.get(statsIndex);
            // Find the original column index for this stats column
            int originalIndex = rowType.getFieldIndex(statsColumnName);
            if (originalIndex == -1) {
                throw new IllegalArgumentException(
                        String.format(
                                "Statistics column '%s' not found in table schema",
                                statsColumnName));
            }
            mapping[statsIndex] = originalIndex;
        }

        // Cache the result
        cachedStatsIndexMapping = mapping;
        return mapping;
    }

    /**
     * Returns the ordered final partition spec keys.
     *
     * <p>The returned list may contain virtual keys produced by partition expressions. Use {@link
     * #getPhysicalPartitionKeys()} when schema-backed partition columns are required.
     */
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    /** Returns partition expressions for virtual partition keys. */
    public List<PartitionExpression> getPartitionExpressions() {
        return partitionExpressions;
    }

    /** Returns true when the table contains virtual partition expressions. */
    public boolean hasPartitionExpressions() {
        return !partitionExpressions.isEmpty();
    }

    /** Returns schema-backed physical partition keys only. */
    public List<String> getPhysicalPartitionKeys() {
        List<String> virtualPartitionKeys = getVirtualPartitionKeys();
        return partitionKeys.stream()
                .filter(partitionKey -> !virtualPartitionKeys.contains(partitionKey))
                .collect(Collectors.toList());
    }

    /** Returns virtual partition spec keys only. */
    public List<String> getVirtualPartitionKeys() {
        List<String> virtualPartitionKeys = new ArrayList<>();
        for (PartitionExpression partitionExpression : partitionExpressions) {
            if (partitionExpression.getVirtualPartitionSpecKey().isPresent()) {
                virtualPartitionKeys.add(partitionExpression.getVirtualPartitionSpecKey().get());
            }
        }
        return virtualPartitionKeys;
    }

    /** Returns physical columns referenced by partition transforms. */
    public List<String> getPartitionSourceColumns() {
        List<String> sourceColumns = new ArrayList<>();
        for (PartitionExpression partitionExpression : partitionExpressions) {
            for (String sourceColumn : partitionExpression.getTransform().getSourceColumns()) {
                if (!sourceColumns.contains(sourceColumn)) {
                    sourceColumns.add(sourceColumn);
                }
            }
        }
        return sourceColumns;
    }

    /** Returns physical columns required to compute partition specs. */
    public List<String> getPartitionInputColumns() {
        List<String> partitionInputColumns = new ArrayList<>(getPhysicalPartitionKeys());
        for (String sourceColumn : getPartitionSourceColumns()) {
            if (!partitionInputColumns.contains(sourceColumn)) {
                partitionInputColumns.add(sourceColumn);
            }
        }
        return partitionInputColumns;
    }

    /** Get the number of buckets of the table. */
    public int getNumBuckets() {
        return numBuckets;
    }

    /**
     * Returns the table properties.
     *
     * <p>Table properties are controlled by Fluss and will change the behavior of the table.
     */
    public Configuration getProperties() {
        return properties;
    }

    /**
     * Returns a {@link TableConfig} helper instance to easily get "table.*" related configs from
     * {@link #getProperties()}.
     */
    public TableConfig getTableConfig() {
        return tableConfig;
    }

    /**
     * Returns the custom properties of the table.
     *
     * <p>Custom properties are not understood by Fluss, but are stored as part of the table's
     * metadata. This provides a mechanism to persist user-defined properties with this table for
     * users.
     */
    public Configuration getCustomProperties() {
        return customProperties;
    }

    /** Returns the remote data directory of the table. */
    @Nullable
    public String getRemoteDataDir() {
        return remoteDataDir;
    }

    /** Returns the comment/description of the table. */
    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    /**
     * Returns the creation time of the table.
     *
     * @return the creation time of the table represented in milliseconds since the epoch (January
     *     1, 1970, 00:00:00 GMT).
     */
    public long getCreatedTime() {
        return createdTime;
    }

    /**
     * Returns the modified time of the table. A table is modified when the schema, distribution,
     * properties, etc. are changed.
     *
     * @return the modified time of the table represented in milliseconds since the epoch (January
     *     1, 1970, 00:00:00 GMT).
     */
    public long getModifiedTime() {
        return modifiedTime;
    }

    /**
     * Converts this table info to a {@link TableDescriptor}.
     *
     * <p>NOTE: It is not recommended to use this method to get metadata of a table, such as bucket
     * keys, bucket number, primary keys, etc. Use the getters of this class instead. {@link
     * TableDescriptor} is intended to be used for creating tables and serializing JSON bytes of a
     * table.
     */
    public TableDescriptor toTableDescriptor() {
        TableDescriptor.Builder builder = TableDescriptor.builder().schema(schema);
        if (partitionExpressions.isEmpty()) {
            builder.partitionedBy(partitionKeys);
        } else {
            builder.partitionedByKeys(toPartitionKeys(partitionKeys, partitionExpressions));
        }
        return builder.distributedBy(numBuckets, bucketKeys)
                .properties(properties.toMap())
                .customProperties(customProperties.toMap())
                .comment(comment)
                .build();
    }

    /** Utility to create a {@link TableInfo} from a {@link TableDescriptor} and other metadata. */
    public static TableInfo of(
            TablePath tablePath,
            long tableId,
            int schemaId,
            TableDescriptor tableDescriptor,
            String remoteDataDir,
            long createdTime,
            long modifiedTime) {
        Schema schema = tableDescriptor.getSchema();
        int numBuckets =
                tableDescriptor
                        .getTableDistribution()
                        .flatMap(TableDescriptor.TableDistribution::getBucketCount)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Bucket count is required for creating table info."));
        return new TableInfo(
                tablePath,
                tableId,
                schemaId,
                schema,
                tableDescriptor.getBucketKeys(),
                tableDescriptor.getPartitionKeys(),
                tableDescriptor.getPartitionExpressions(),
                numBuckets,
                Configuration.fromMap(tableDescriptor.getProperties()),
                Configuration.fromMap(tableDescriptor.getCustomProperties()),
                remoteDataDir,
                tableDescriptor.getComment().orElse(null),
                createdTime,
                modifiedTime);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableInfo that = (TableInfo) o;
        // exclude createdTime and modifiedTime from comparison
        return tableId == that.tableId
                && schemaId == that.schemaId
                && numBuckets == that.numBuckets
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(rowType, that.rowType)
                && Objects.equals(primaryKeys, that.primaryKeys)
                && Objects.equals(physicalPrimaryKeys, that.physicalPrimaryKeys)
                && Objects.equals(bucketKeys, that.bucketKeys)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(partitionExpressions, that.partitionExpressions)
                && Objects.equals(properties, that.properties)
                && Objects.equals(customProperties, that.customProperties)
                && Objects.equals(remoteDataDir, that.remoteDataDir)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        // exclude createdTime and modifiedTime from comparison
        return Objects.hash(
                tablePath,
                tableId,
                schemaId,
                rowType,
                primaryKeys,
                physicalPrimaryKeys,
                bucketKeys,
                partitionKeys,
                partitionExpressions,
                numBuckets,
                properties,
                customProperties,
                remoteDataDir,
                comment);
    }

    @Override
    public String toString() {
        return "TableInfo{"
                + "tablePath="
                + tablePath
                + ", tableId="
                + tableId
                + ", schemaId="
                + schemaId
                + ", schema="
                + schema
                + ", physicalPrimaryKeys="
                + physicalPrimaryKeys
                + ", bucketKeys="
                + bucketKeys
                + ", partitionKeys="
                + partitionKeys
                + ", partitionExpressions="
                + partitionExpressions
                + ", numBuckets="
                + numBuckets
                + ", properties="
                + properties
                + ", customProperties="
                + customProperties
                + ", remoteDataDir="
                + remoteDataDir
                + ", comment='"
                + comment
                + '\''
                + ", createdTime="
                + createdTime
                + ", modifiedTime="
                + modifiedTime
                + '}';
    }

    // --------------------------------------------------------------------------------------------

    private static List<String> generatePhysicalPrimaryKey(
            List<String> primaryKeys, List<String> partitionKeys) {
        return primaryKeys.stream()
                .filter(pk -> !partitionKeys.contains(pk))
                .collect(Collectors.toList());
    }

    private static void validatePartitionMetadata(
            Schema schema,
            List<String> partitionKeys,
            List<PartitionExpression> partitionExpressions) {
        Set<String> columnNames = new HashSet<>(schema.getColumnNames());
        Set<String> partitionKeySet = new HashSet<>(partitionKeys);
        checkArgument(
                partitionKeySet.size() == partitionKeys.size(),
                "Duplicate partition keys are not allowed: %s.",
                partitionKeys);

        Set<String> virtualPartitionKeys =
                validatePartitionExpressions(partitionKeySet, partitionKeys, partitionExpressions);
        for (String partitionKey : partitionKeys) {
            checkArgument(
                    columnNames.contains(partitionKey)
                            || virtualPartitionKeys.contains(partitionKey),
                    "Partition key '%s' does not exist in the schema or partition expressions.",
                    partitionKey);
        }
        for (String virtualPartitionKey : virtualPartitionKeys) {
            checkArgument(
                    !columnNames.contains(virtualPartitionKey),
                    "Virtual partition spec key '%s' conflicts with a physical column.",
                    virtualPartitionKey);
        }
        for (PartitionExpression partitionExpression : partitionExpressions) {
            for (String sourceColumn : partitionExpression.getTransform().getSourceColumns()) {
                int sourceColumnIndex = schema.getRowType().getFieldIndex(sourceColumn);
                checkArgument(
                        sourceColumnIndex >= 0,
                        "Partition transform source column '%s' does not exist in the schema.",
                        sourceColumn);
                checkArgument(
                        !schema.getRowType().getTypeAt(sourceColumnIndex).isNullable(),
                        "Partition transform source column '%s' must be non-nullable.",
                        sourceColumn);
            }
            PartitionTransform transform = partitionExpression.getTransform();
            if (transform instanceof DateTruncPartitionTransform) {
                DateTruncPartitionTransform dateTruncTransform =
                        (DateTruncPartitionTransform) transform;
                int sourceColumnIndex =
                        schema.getRowType().getFieldIndex(dateTruncTransform.getSourceColumn());
                if (sourceColumnIndex >= 0) {
                    DataTypeRoot sourceType =
                            schema.getRowType().getTypeAt(sourceColumnIndex).getTypeRoot();
                    if (sourceType == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                        checkArgument(
                                dateTruncTransform.getTimeZone().isPresent(),
                                "Persisted DATE_TRUNC partition transform for TIMESTAMP_LTZ source column '%s' must contain a resolved time zone.",
                                dateTruncTransform.getSourceColumn());
                    } else if (sourceType == DataTypeRoot.DATE
                            || sourceType == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                        checkArgument(
                                !dateTruncTransform.getTimeZone().isPresent(),
                                "DATE_TRUNC partition transform time zone is only supported for TIMESTAMP_LTZ source columns, but source column '%s' has type %s.",
                                dateTruncTransform.getSourceColumn(),
                                schema.getRowType().getTypeAt(sourceColumnIndex));
                    }
                }
            }
        }

        if (schema.getPrimaryKey().isPresent()) {
            List<String> pkColumns = schema.getPrimaryKey().get().getColumnNames();
            List<String> physicalPartitionKeys =
                    getPhysicalPartitionKeys(partitionKeys, virtualPartitionKeys);
            for (String partitionKey : physicalPartitionKeys) {
                checkArgument(
                        pkColumns.contains(partitionKey),
                        "Partitioned Primary Key Table requires physical partition keys %s is a subset of the primary key %s.",
                        physicalPartitionKeys,
                        pkColumns);
            }
            for (PartitionExpression partitionExpression : partitionExpressions) {
                for (String sourceColumn : partitionExpression.getTransform().getSourceColumns()) {
                    checkArgument(
                            pkColumns.contains(sourceColumn),
                            "Partitioned Primary Key Table requires transform source column '%s' is in the primary key %s.",
                            sourceColumn,
                            pkColumns);
                }
            }
        }
    }

    private static List<String> getPhysicalPartitionKeys(
            List<String> partitionKeys, Set<String> virtualPartitionKeys) {
        return partitionKeys.stream()
                .filter(partitionKey -> !virtualPartitionKeys.contains(partitionKey))
                .collect(Collectors.toList());
    }

    private static Set<String> validatePartitionExpressions(
            Set<String> partitionKeySet,
            List<String> partitionKeys,
            List<PartitionExpression> partitionExpressions) {
        Set<String> virtualPartitionKeys = new HashSet<>();
        for (PartitionExpression partitionExpression : partitionExpressions) {
            checkArgument(
                    partitionExpression.getVirtualPartitionSpecKey().isPresent(),
                    "Partition expression must contain a resolved virtual partition spec key.");
            String virtualPartitionKey = partitionExpression.getVirtualPartitionSpecKey().get();
            checkArgument(
                    partitionKeySet.contains(virtualPartitionKey),
                    "Virtual partition spec key '%s' is not present in partition keys %s.",
                    virtualPartitionKey,
                    partitionKeys);
            checkArgument(
                    virtualPartitionKeys.add(virtualPartitionKey),
                    "Duplicate virtual partition spec key '%s'.",
                    virtualPartitionKey);
        }
        return virtualPartitionKeys;
    }

    private static List<PartitionKey> toPartitionKeys(
            List<String> partitionKeys, List<PartitionExpression> partitionExpressions) {
        List<PartitionKey> orderedPartitionKeys = new ArrayList<>();
        for (String partitionKey : partitionKeys) {
            PartitionExpression matchedExpression = null;
            for (PartitionExpression partitionExpression : partitionExpressions) {
                if (partitionExpression.getVirtualPartitionSpecKey().isPresent()
                        && partitionExpression
                                .getVirtualPartitionSpecKey()
                                .get()
                                .equals(partitionKey)) {
                    matchedExpression = partitionExpression;
                    break;
                }
            }
            if (matchedExpression == null) {
                orderedPartitionKeys.add(PartitionKey.column(partitionKey));
            } else {
                orderedPartitionKeys.add(PartitionKey.expression(matchedExpression));
            }
        }
        return orderedPartitionKeys;
    }
}

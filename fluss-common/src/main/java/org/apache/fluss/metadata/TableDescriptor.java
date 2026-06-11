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
import org.apache.fluss.annotation.PublicStable;
import org.apache.fluss.config.ConfigOption;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.ConfigurationUtils;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.TableDescriptorJsonSerde;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Represents the metadata of a table in Fluss.
 *
 * <p>It contains all characteristics that can be expressed in a SQL {@code CREATE TABLE} statement,
 * such as schema, primary keys, partition keys, bucket keys, and options.
 *
 * @since 0.1
 */
@PublicEvolving
public final class TableDescriptor implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String OFFSET_COLUMN_NAME = "__offset";
    public static final String TIMESTAMP_COLUMN_NAME = "__timestamp";
    public static final String BUCKET_COLUMN_NAME = "__bucket";

    // Reserved column names for virtual table metadata ($changelog and $binlog)
    public static final String CHANGE_TYPE_COLUMN = "_change_type";
    public static final String LOG_OFFSET_COLUMN = "_log_offset";
    public static final String COMMIT_TIMESTAMP_COLUMN = "_commit_timestamp";

    // column names for $binlog virtual table nested row fields
    public static final String BEFORE_COLUMN = "before";
    public static final String AFTER_COLUMN = "after";

    private final Schema schema;
    private final @Nullable String comment;
    private final List<String> partitionKeys;
    private final List<PartitionExpression> partitionExpressions;
    private final @Nullable TableDistribution tableDistribution;
    private final Map<String, String> properties;
    private final Map<String, String> customProperties;

    private TableDescriptor(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            List<PartitionExpression> partitionExpressions,
            @Nullable TableDistribution tableDistribution,
            Map<String, String> properties,
            Map<String, String> customProperties) {
        this.schema = checkNotNull(schema, "schema must not be null.");
        this.comment = comment;
        this.partitionKeys =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                checkNotNull(partitionKeys, "partition keys must not be null.")));
        this.partitionExpressions =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                checkNotNull(
                                        partitionExpressions,
                                        "partition expressions must not be null.")));
        this.properties = unmodifiableMap(checkNotNull(properties, "options must not be null."));
        this.customProperties =
                unmodifiableMap(
                        checkNotNull(customProperties, "customProperties must not be null."));

        validatePartitionMetadata(schema, this.partitionKeys, this.partitionExpressions);
        List<String> physicalPartitionKeys =
                getPhysicalPartitionKeys(this.partitionKeys, this.partitionExpressions);

        // validate and normalize bucket keys.
        this.tableDistribution =
                normalizeDistribution(schema, physicalPartitionKeys, tableDistribution);

        Set<String> columnNames =
                schema.getColumns().stream()
                        .map(Schema.Column::getName)
                        .collect(Collectors.toSet());
        if (this.tableDistribution != null) {
            this.tableDistribution
                    .getBucketKeys()
                    .forEach(
                            f ->
                                    checkArgument(
                                            columnNames.contains(f),
                                            "Bucket key '%s' does not exist in the schema.",
                                            f));
        }

        checkArgument(
                properties.entrySet().stream()
                        .allMatch(e -> e.getKey() != null && e.getValue() != null),
                "options cannot have null keys or values.");

        // we don't check property validation here, it will be checked in server,
        // as the property may be supported in future version.
    }

    /** Creates a builder for building table descriptor. */
    public static Builder builder() {
        return new Builder();
    }

    /** Creates a builder based on an existing TableDescriptor. */
    public static Builder builder(TableDescriptor origin) {
        return new Builder(origin);
    }

    /** Returns the {@link Schema} of the table. */
    public Schema getSchema() {
        return schema;
    }

    /** Returns the bucket key of the table, empty if no bucket key is set. */
    public List<String> getBucketKeys() {
        return this.getTableDistribution()
                .map(TableDescriptor.TableDistribution::getBucketKeys)
                .orElse(Collections.emptyList());
    }

    /**
     * Check if the table is using a default bucket key. A default bucket key is:
     *
     * <ul>
     *   <li>the same as the primary keys excluding the partition keys.
     *   <li>empty if the table is not a primary key table.
     * </ul>
     */
    public boolean isDefaultBucketKey() {
        if (schema.getPrimaryKey().isPresent()) {
            return getBucketKeys()
                    .equals(defaultBucketKeyOfPrimaryKeyTable(schema, getPhysicalPartitionKeys()));
        } else {
            return getBucketKeys().isEmpty();
        }
    }

    /**
     * Check if the table is partitioned or not.
     *
     * @return true if the table is partitioned; otherwise, false
     */
    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    /** Check if the table has primary key or not. */
    public boolean hasPrimaryKey() {
        return schema.getPrimaryKey().isPresent();
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
        return getPhysicalPartitionKeys(partitionKeys, partitionExpressions);
    }

    /** Returns virtual partition spec keys only. */
    public List<String> getVirtualPartitionKeys() {
        return getVirtualPartitionKeys(partitionExpressions);
    }

    /** Returns physical columns referenced by partition transforms. */
    public List<String> getPartitionSourceColumns() {
        return getPartitionSourceColumns(partitionExpressions);
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

    /** Returns the distribution of the table if the {@code DISTRIBUTED} clause is defined. */
    public Optional<TableDistribution> getTableDistribution() {
        return Optional.ofNullable(tableDistribution);
    }

    /**
     * Returns the table properties.
     *
     * <p>Table properties are controlled by Fluss and will change the behavior of the table.
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Returns the custom properties of the table.
     *
     * <p>Custom properties are not understood by Fluss, but are stored as part of the table's
     * metadata. This provides a mechanism to persist user-defined properties with this table for
     * users.
     */
    public Map<String, String> getCustomProperties() {
        return customProperties;
    }

    /**
     * Gets the replication factor of the table.
     *
     * @throws IllegalArgumentException if the replication factor is not set
     */
    public int getReplicationFactor() {
        String factor = properties.get(ConfigOptions.TABLE_REPLICATION_FACTOR.key());
        checkArgument(
                factor != null, "%s is not set.", ConfigOptions.TABLE_REPLICATION_FACTOR.key());
        return Integer.parseInt(factor);
    }

    /**
     * Returns a new TableDescriptor instance that is a copy of this TableDescriptor with a new
     * properties.
     */
    public TableDescriptor withProperties(Map<String, String> newProperties) {
        return new TableDescriptor(
                schema,
                comment,
                partitionKeys,
                partitionExpressions,
                tableDistribution,
                newProperties,
                customProperties);
    }

    /**
     * Returns a new TableDescriptor instance that is a copy of this TableDescriptor with a new
     * properties and new customProperties.
     */
    public TableDescriptor withProperties(
            Map<String, String> newProperties, Map<String, String> newCustomProperties) {
        return new TableDescriptor(
                schema,
                comment,
                partitionKeys,
                partitionExpressions,
                tableDistribution,
                newProperties,
                newCustomProperties);
    }

    /**
     * Returns a new TableDescriptor instance that is a copy of this TableDescriptor with a new
     * replication factor property.
     */
    public TableDescriptor withReplicationFactor(int newReplicationFactor) {
        Map<String, String> newProperties = new HashMap<>(properties);
        newProperties.put(
                ConfigOptions.TABLE_REPLICATION_FACTOR.key(), String.valueOf(newReplicationFactor));
        return withProperties(newProperties);
    }

    /**
     * Returns a new TableDescriptor instance that is a copy of this TableDescriptor with a new
     * datalake format.
     */
    public TableDescriptor withDataLakeFormat(DataLakeFormat dataLakeFormat) {
        Map<String, String> newProperties = new HashMap<>(properties);
        newProperties.put(ConfigOptions.TABLE_DATALAKE_FORMAT.key(), dataLakeFormat.toString());
        return withProperties(newProperties);
    }

    /**
     * Returns a new TableDescriptor instance that is a copy of this TableDescriptor with a new
     * standby replica enabled.
     */
    public TableDescriptor withStandbyReplicaEnabled(boolean standbyReplicaEnabled) {
        Map<String, String> newProperties = new HashMap<>(properties);
        newProperties.put(
                ConfigOptions.TABLE_KV_STANDBY_REPLICA_ENABLED.key(),
                String.valueOf(standbyReplicaEnabled));
        return withProperties(newProperties);
    }

    /**
     * Returns a new TableDescriptor instance that is a copy of this TableDescriptor with a new
     * bucket count.
     */
    public TableDescriptor withBucketCount(int newBucketCount) {
        return new TableDescriptor(
                schema,
                comment,
                partitionKeys,
                partitionExpressions,
                new TableDistribution(
                        newBucketCount,
                        Optional.ofNullable(tableDistribution)
                                .map(TableDistribution::getBucketKeys)
                                .orElse(Collections.emptyList())),
                properties,
                customProperties);
    }

    /** Returns a copy whose implicit partition transforms have resolved time-zone metadata. */
    public TableDescriptor withResolvedPartitionExpressionTimeZone(ZoneId defaultTimeZone) {
        checkNotNull(defaultTimeZone, "default time zone must not be null.");
        if (partitionExpressions.isEmpty()) {
            return this;
        }
        List<PartitionExpression> resolvedPartitionExpressions = new ArrayList<>();
        for (PartitionExpression partitionExpression : partitionExpressions) {
            PartitionTransform transform = partitionExpression.getTransform();
            if (transform instanceof DateTruncPartitionTransform) {
                DateTruncPartitionTransform dateTruncTransform =
                        (DateTruncPartitionTransform) transform;
                DataTypeRoot sourceType =
                        schema.getRowType()
                                .getTypeAt(
                                        schema.getRowType()
                                                .getFieldIndex(
                                                        dateTruncTransform.getSourceColumn()))
                                .getTypeRoot();
                if (sourceType == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                        && !dateTruncTransform.getTimeZone().isPresent()) {
                    transform = dateTruncTransform.withTimeZone(defaultTimeZone);
                }
            }
            resolvedPartitionExpressions.add(partitionExpression.withTransform(transform));
        }
        return new TableDescriptor(
                schema,
                comment,
                partitionKeys,
                resolvedPartitionExpressions,
                tableDistribution,
                properties,
                customProperties);
    }

    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    /**
     * Serialize the table descriptor to a JSON byte array.
     *
     * @see TableDescriptorJsonSerde
     */
    public byte[] toJsonBytes() {
        return JsonSerdeUtils.writeValueAsBytes(this, TableDescriptorJsonSerde.INSTANCE);
    }

    /**
     * Deserialize from JSON byte array to an instance of {@link TableDescriptor}.
     *
     * @see TableDescriptorJsonSerde
     */
    public static TableDescriptor fromJsonBytes(byte[] json) {
        return JsonSerdeUtils.readValue(json, TableDescriptorJsonSerde.INSTANCE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableDescriptor table = (TableDescriptor) o;
        return Objects.equals(schema, table.schema)
                && Objects.equals(comment, table.comment)
                && Objects.equals(partitionKeys, table.partitionKeys)
                && Objects.equals(partitionExpressions, table.partitionExpressions)
                && Objects.equals(tableDistribution, table.tableDistribution)
                && Objects.equals(properties, table.properties)
                && Objects.equals(customProperties, table.customProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                comment,
                partitionKeys,
                partitionExpressions,
                tableDistribution,
                properties,
                customProperties);
    }

    @Override
    public String toString() {
        return "TableDescriptor{"
                + "schema="
                + schema
                + ", comment='"
                + comment
                + '\''
                + ", partitionKeys="
                + partitionKeys
                + (partitionExpressions.isEmpty()
                        ? ""
                        : ", partitionExpressions=" + partitionExpressions)
                + ", tableDistribution="
                + tableDistribution
                + ", properties="
                + properties
                + ", customProperties="
                + customProperties
                + '}';
    }

    // ----------------------------------------------------------------------------------------

    @Nullable
    private static TableDistribution normalizeDistribution(
            Schema schema,
            List<String> physicalPartitionKeys,
            @Nullable TableDistribution originDistribution) {
        if (originDistribution != null) {
            // we may need to check and normalize bucket key
            List<String> bucketKeys = originDistribution.getBucketKeys();
            // bucket key shouldn't include partition key
            if (bucketKeys.stream().anyMatch(physicalPartitionKeys::contains)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Bucket key %s shouldn't include any column in partition keys %s.",
                                bucketKeys, physicalPartitionKeys));
            }

            // if primary key set
            if (schema.getPrimaryKey().isPresent()) {
                // if bucket key is empty, force to set bucket keys
                if (bucketKeys.isEmpty()) {
                    return new TableDistribution(
                            originDistribution.getBucketCount().orElse(null),
                            defaultBucketKeyOfPrimaryKeyTable(schema, physicalPartitionKeys));
                } else {
                    // check the provided bucket key
                    List<String> pkColumns = schema.getPrimaryKey().get().getColumnNames();
                    if (!new HashSet<>(pkColumns).containsAll(bucketKeys)) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Bucket keys must be a subset of primary keys excluding partition "
                                                + "keys for primary-key tables. The primary keys are %s, the "
                                                + "partition keys are %s, but "
                                                + "the user-defined bucket keys are %s.",
                                        pkColumns, physicalPartitionKeys, bucketKeys));
                    }
                    return new TableDistribution(
                            originDistribution.getBucketCount().orElse(null), bucketKeys);
                }
            } else {
                return originDistribution;
            }
        } else {
            // if primary key is set, need to set the bucket keys
            // to primary key (exclude partition key if it is partitioned table)
            if (schema.getPrimaryKey().isPresent()) {
                return new TableDistribution(
                        null, defaultBucketKeyOfPrimaryKeyTable(schema, physicalPartitionKeys));
            } else {
                return originDistribution;
            }
        }
    }

    /** The default bucket key of primary key table is the primary key excluding partition keys. */
    private static List<String> defaultBucketKeyOfPrimaryKeyTable(
            Schema schema, List<String> physicalPartitionKeys) {
        checkArgument(schema.getPrimaryKey().isPresent(), "Primary key must be set.");
        List<String> bucketKeys = new ArrayList<>(schema.getPrimaryKey().get().getColumnNames());
        bucketKeys.removeAll(physicalPartitionKeys);
        if (bucketKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Primary Key constraint %s should not be same with partition fields %s.",
                            schema.getPrimaryKey().get().getColumnNames(), physicalPartitionKeys));
        }

        return bucketKeys;
    }

    private static void validatePartitionMetadata(
            Schema schema,
            List<String> partitionKeys,
            List<PartitionExpression> partitionExpressions) {
        Set<String> partitionKeySet = new HashSet<>(partitionKeys);
        checkArgument(
                partitionKeySet.size() == partitionKeys.size(),
                "Duplicate partition keys are not allowed: %s.",
                partitionKeys);

        Set<String> columnNames =
                schema.getColumns().stream()
                        .map(Schema.Column::getName)
                        .collect(Collectors.toSet());
        List<String> virtualPartitionKeys = getVirtualPartitionKeys(partitionExpressions);
        Set<String> virtualPartitionKeySet = new HashSet<>(virtualPartitionKeys);
        checkArgument(
                virtualPartitionKeySet.size() == virtualPartitionKeys.size(),
                "Duplicate virtual partition spec keys are not allowed: %s.",
                virtualPartitionKeys);

        for (String virtualPartitionKey : virtualPartitionKeys) {
            checkArgument(
                    partitionKeySet.contains(virtualPartitionKey),
                    "Virtual partition spec key '%s' is not present in partition keys %s.",
                    virtualPartitionKey,
                    partitionKeys);
            if (columnNames.contains(virtualPartitionKey)) {
                PartitionExpression expression =
                        partitionExpressions.stream()
                                .filter(
                                        candidate ->
                                                candidate
                                                        .getVirtualPartitionSpecKey()
                                                        .filter(virtualPartitionKey::equals)
                                                        .isPresent())
                                .findFirst()
                                .get();
                throw new IllegalArgumentException(
                        String.format(
                                "Virtual partition spec key '%s' for transform %s conflicts with physical column '%s'. Specify a different virtual partition spec key explicitly with PartitionExpression.of(...).",
                                virtualPartitionKey,
                                expression.getTransform(),
                                virtualPartitionKey));
            }
        }

        List<String> physicalPartitionKeys =
                getPhysicalPartitionKeys(partitionKeys, partitionExpressions);
        for (String partitionKey : partitionKeys) {
            if (!columnNames.contains(partitionKey)
                    && !virtualPartitionKeySet.contains(partitionKey)) {
                if (partitionExpressions.isEmpty()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Partition key '%s' does not exist in the schema.",
                                    partitionKey));
                }
                throw new IllegalArgumentException(
                        String.format(
                                "Partition key '%s' does not exist in the schema or partition expressions.",
                                partitionKey));
            }
        }
        for (String sourceColumn : getPartitionSourceColumns(partitionExpressions)) {
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
        for (PartitionExpression partitionExpression : partitionExpressions) {
            PartitionTransform transform = partitionExpression.getTransform();
            if (transform instanceof DateTruncPartitionTransform) {
                DateTruncPartitionTransform dateTruncTransform =
                        (DateTruncPartitionTransform) transform;
                int sourceColumnIndex =
                        schema.getRowType().getFieldIndex(dateTruncTransform.getSourceColumn());
                DataTypeRoot sourceType =
                        sourceColumnIndex < 0
                                ? null
                                : schema.getRowType().getTypeAt(sourceColumnIndex).getTypeRoot();
                if (sourceType == DataTypeRoot.DATE
                        || sourceType == DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                    checkArgument(
                            !dateTruncTransform.getTimeZone().isPresent(),
                            "DATE_TRUNC partition transform time zone is only supported for TIMESTAMP_LTZ source columns, but source column '%s' has type %s. TIMESTAMP_NTZ and DATE values are truncated directly as local calendar values.",
                            dateTruncTransform.getSourceColumn(),
                            schema.getRowType().getTypeAt(sourceColumnIndex));
                }
            }
        }

        if (schema.getPrimaryKey().isPresent()) {
            List<String> pkColumns = schema.getPrimaryKey().get().getColumnNames();
            for (String partitionKey : physicalPartitionKeys) {
                checkArgument(
                        pkColumns.contains(partitionKey),
                        "Partitioned Primary Key Table requires physical partition keys %s is a subset of the primary key %s.",
                        physicalPartitionKeys,
                        pkColumns);
            }
            for (String sourceColumn : getPartitionSourceColumns(partitionExpressions)) {
                checkArgument(
                        pkColumns.contains(sourceColumn),
                        "Partitioned Primary Key Table requires transform source column '%s' is in the primary key %s.",
                        sourceColumn,
                        pkColumns);
            }
        }
    }

    private static List<String> getPhysicalPartitionKeys(
            List<String> partitionKeys, List<PartitionExpression> partitionExpressions) {
        Set<String> virtualPartitionKeys =
                new HashSet<>(getVirtualPartitionKeys(partitionExpressions));
        return partitionKeys.stream()
                .filter(partitionKey -> !virtualPartitionKeys.contains(partitionKey))
                .collect(Collectors.toList());
    }

    private static List<String> getVirtualPartitionKeys(
            List<PartitionExpression> partitionExpressions) {
        List<String> virtualPartitionKeys = new ArrayList<>();
        for (PartitionExpression partitionExpression : partitionExpressions) {
            checkArgument(
                    partitionExpression.getVirtualPartitionSpecKey().isPresent(),
                    "Virtual partition expression must have a resolved partition spec key.");
            virtualPartitionKeys.add(partitionExpression.getVirtualPartitionSpecKey().get());
        }
        return virtualPartitionKeys;
    }

    private static List<String> getPartitionSourceColumns(
            List<PartitionExpression> partitionExpressions) {
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

    // ----------------------------------------------------------------------------------------

    /**
     * TableDistribution in a Table.
     *
     * @since 0.1
     */
    @PublicStable
    public static final class TableDistribution implements Serializable {

        private static final long serialVersionUID = 1L;

        private final @Nullable Integer bucketCount;
        private final List<String> bucketKeys;

        public TableDistribution(@Nullable Integer bucketCount, List<String> bucketKeys) {
            this.bucketCount = bucketCount;
            this.bucketKeys = bucketKeys;
        }

        public List<String> getBucketKeys() {
            return bucketKeys;
        }

        public Optional<Integer> getBucketCount() {
            return Optional.ofNullable(bucketCount);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableDistribution that = (TableDistribution) o;
            return Objects.equals(bucketCount, that.bucketCount)
                    && Objects.equals(bucketKeys, that.bucketKeys);
        }

        @Override
        public String toString() {
            return "{bucketKeys=" + bucketKeys + " bucketCount=" + bucketCount + "}";
        }

        @Override
        public int hashCode() {
            return Objects.hash(bucketCount, bucketKeys);
        }
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link TableDescriptor}. */
    @PublicEvolving
    public static class Builder {

        private @Nullable Schema schema;
        private final Map<String, String> properties;
        private final Map<String, String> customProperties;
        private final List<String> partitionKeys;
        private final List<PartitionExpression> partitionExpressions;
        private @Nullable String comment;
        private @Nullable TableDistribution tableDistribution;
        private @Nullable PartitionDeclarationMode partitionDeclarationMode;

        protected Builder() {
            this.properties = new HashMap<>();
            this.partitionKeys = new ArrayList<>();
            this.partitionExpressions = new ArrayList<>();
            this.customProperties = new HashMap<>();
        }

        protected Builder(TableDescriptor descriptor) {
            this.schema = descriptor.getSchema();
            this.properties = new HashMap<>(descriptor.getProperties());
            this.customProperties = new HashMap<>(descriptor.getCustomProperties());
            this.partitionKeys = new ArrayList<>(descriptor.getPartitionKeys());
            this.partitionExpressions = new ArrayList<>(descriptor.getPartitionExpressions());
            this.comment = descriptor.getComment().orElse(null);
            this.tableDistribution = descriptor.getTableDistribution().orElse(null);
            if (!partitionKeys.isEmpty()) {
                this.partitionDeclarationMode =
                        partitionExpressions.isEmpty()
                                ? PartitionDeclarationMode.LEGACY_PHYSICAL
                                : PartitionDeclarationMode.PARTITION_KEYS;
            }
        }

        /** Define the schema of the {@link TableDescriptor}. */
        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        /** Sets the log format of the table. */
        public Builder logFormat(LogFormat logFormat) {
            property(ConfigOptions.TABLE_LOG_FORMAT, logFormat);
            return this;
        }

        /** Sets the kv format of the table. */
        public Builder kvFormat(KvFormat kvFormat) {
            property(ConfigOptions.TABLE_KV_FORMAT, kvFormat);
            return this;
        }

        /**
         * Sets table property on the table.
         *
         * <p>Table properties are controlled by Fluss and will change the behavior of the table.
         */
        public <T> Builder property(ConfigOption<T> configOption, T value) {
            checkNotNull(configOption, "Config option must not be null.");
            checkNotNull(value, "Value must not be null.");
            properties.put(
                    configOption.key(), ConfigurationUtils.convertValue(value, String.class));
            return this;
        }

        /**
         * Sets table property on the table.
         *
         * <p>Table properties are controlled by Fluss and will change the behavior of the table.
         */
        public Builder property(String key, String value) {
            checkNotNull(key, "Key must not be null.");
            checkNotNull(value, "Value must not be null.");
            properties.put(key, value);
            return this;
        }

        /**
         * Sets table properties on the table.
         *
         * <p>Table properties are controlled by Fluss and will change the behavior of the table.
         */
        public Builder properties(Map<String, String> properties) {
            checkNotNull(properties, "properties must not be null.");
            this.properties.putAll(properties);
            return this;
        }

        /**
         * Sets custom property on the table.
         *
         * <p>Custom properties are not understood by Fluss, but are stored as part of the table's
         * metadata. This provides a mechanism to persist user-defined properties with this table
         * for users.
         */
        public Builder customProperty(String key, String value) {
            checkNotNull(key, "Key must not be null.");
            checkNotNull(value, "Value must not be null.");
            this.customProperties.put(key, value);
            return this;
        }

        /**
         * Sets custom properties on the table.
         *
         * <p>Custom properties are not understood by Fluss, but are stored as part of the table's
         * metadata. This provides a mechanism to persist user-defined properties with this table
         * for users.
         */
        public Builder customProperties(Map<String, String> customProperties) {
            checkNotNull(customProperties, "customProperties must not be null.");
            this.customProperties.putAll(customProperties);
            return this;
        }

        /** Define which columns this table is partitioned by. */
        public Builder partitionedBy(String... partitionKeys) {
            return partitionedBy(Arrays.asList(partitionKeys));
        }

        /** Define which columns this table is partitioned by. */
        public Builder partitionedBy(List<String> partitionKeys) {
            checkArgument(
                    partitionDeclarationMode == null
                            || partitionDeclarationMode == PartitionDeclarationMode.LEGACY_PHYSICAL,
                    "partitionedBy(...) and partitionedByKeys(...) cannot be mixed in the same builder.");
            partitionDeclarationMode = PartitionDeclarationMode.LEGACY_PHYSICAL;
            this.partitionKeys.clear();
            this.partitionKeys.addAll(partitionKeys);
            this.partitionExpressions.clear();
            return this;
        }

        /** Define ordered partition keys, including physical columns and virtual expressions. */
        public Builder partitionedByKeys(PartitionKey... partitionKeys) {
            return partitionedByKeys(Arrays.asList(partitionKeys));
        }

        /** Define ordered partition keys, including physical columns and virtual expressions. */
        public Builder partitionedByKeys(List<PartitionKey> partitionKeys) {
            checkArgument(
                    partitionDeclarationMode == null
                            || partitionDeclarationMode == PartitionDeclarationMode.PARTITION_KEYS,
                    "partitionedBy(...) and partitionedByKeys(...) cannot be mixed in the same builder.");
            partitionDeclarationMode = PartitionDeclarationMode.PARTITION_KEYS;
            this.partitionKeys.clear();
            this.partitionExpressions.clear();
            for (PartitionKey partitionKey : partitionKeys) {
                if (partitionKey.getKind() == PartitionKey.Kind.COLUMN) {
                    this.partitionKeys.add(partitionKey.getColumnName().get());
                } else {
                    PartitionExpression resolvedExpression =
                            resolvePartitionExpression(partitionKey.getExpression().get());
                    this.partitionKeys.add(resolvedExpression.getVirtualPartitionSpecKey().get());
                    this.partitionExpressions.add(resolvedExpression);
                }
            }
            return this;
        }

        /**
         * Define the distribution of the table. If the bucket keys are defined, it implies a hash
         * distribution on the bucket keys. Otherwise, it is a random distribution.
         *
         * <p>By default, a table with primary key is hash distributed by the primary key.
         */
        public Builder distributedBy(int bucketCount, String... bucketKeys) {
            return distributedBy(bucketCount, Arrays.asList(bucketKeys));
        }

        /**
         * Define the distribution of the table. If the bucketCount is null, it implies the bucket
         * count should be determined by the Fluss cluster. If the bucket keys are defined, it
         * implies a hash distribution on the bucket keys. Otherwise, it is a random distribution.
         *
         * <p>By default, a table with primary key is hash distributed by the primary key.
         */
        public Builder distributedBy(@Nullable Integer bucketCount, List<String> bucketKeys) {
            this.tableDistribution = new TableDistribution(bucketCount, bucketKeys);
            return this;
        }

        /** Define the comment for this table. */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /** Returns an immutable instance of {@link TableDescriptor}. */
        public TableDescriptor build() {
            return new TableDescriptor(
                    schema,
                    comment,
                    partitionKeys,
                    partitionExpressions,
                    tableDistribution,
                    properties,
                    customProperties);
        }

        private static PartitionExpression resolvePartitionExpression(
                PartitionExpression partitionExpression) {
            if (partitionExpression.getVirtualPartitionSpecKey().isPresent()) {
                return partitionExpression;
            }

            PartitionTransform transform = partitionExpression.getTransform();
            checkArgument(
                    transform instanceof DateTruncPartitionTransform,
                    "Unsupported partition transform type: %s.",
                    transform.getType());
            DateTruncPartitionTransform dateTruncTransform =
                    (DateTruncPartitionTransform) transform;
            return partitionExpression.withVirtualPartitionSpecKey(
                    dateTruncTransform.defaultPartitionSpecKey());
        }

        private enum PartitionDeclarationMode {
            LEGACY_PHYSICAL,
            PARTITION_KEYS
        }
    }
}

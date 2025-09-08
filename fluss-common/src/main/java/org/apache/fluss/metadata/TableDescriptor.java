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
import org.apache.fluss.utils.json.JsonSerdeUtils;
import org.apache.fluss.utils.json.TableDescriptorJsonSerde;

import javax.annotation.Nullable;

import java.io.Serializable;
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

    private final Schema schema;
    private final @Nullable String comment;
    private final List<String> partitionKeys;
    private final @Nullable TableDistribution tableDistribution;
    private final Map<String, String> properties;
    private final Map<String, String> customProperties;

    private TableDescriptor(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            @Nullable TableDistribution tableDistribution,
            Map<String, String> properties,
            Map<String, String> customProperties) {
        this.schema = checkNotNull(schema, "schema must not be null.");
        this.comment = comment;
        this.partitionKeys = checkNotNull(partitionKeys, "partition keys must not be null.");
        this.properties = unmodifiableMap(checkNotNull(properties, "options must not be null."));
        this.customProperties =
                unmodifiableMap(
                        checkNotNull(customProperties, "customProperties must not be null."));

        // validate and normalize bucket keys.
        this.tableDistribution = normalizeDistribution(schema, partitionKeys, tableDistribution);

        // validate partition keys and bucket keys
        Set<String> columnNames =
                schema.getColumns().stream()
                        .map(Schema.Column::getName)
                        .collect(Collectors.toSet());
        if (schema.getPrimaryKey().isPresent()) {
            List<String> pkColumns = schema.getPrimaryKey().get().getColumnNames();
            partitionKeys.forEach(
                    f ->
                            checkArgument(
                                    pkColumns.contains(f),
                                    "Partitioned Primary Key Table requires partition key %s is a subset of the primary key %s.",
                                    partitionKeys,
                                    pkColumns));
        } else {
            partitionKeys.forEach(
                    f ->
                            checkArgument(
                                    columnNames.contains(f),
                                    "Partition key '%s' does not exist in the schema.",
                                    f));
        }

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
            return getBucketKeys().equals(defaultBucketKeyOfPrimaryKeyTable(schema, partitionKeys));
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
     * Get the partition keys of the table. This will be an empty set if the table is not
     * partitioned.
     *
     * @return partition keys of the table
     */
    public List<String> getPartitionKeys() {
        return partitionKeys;
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
                schema, comment, partitionKeys, tableDistribution, newProperties, customProperties);
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
     * bucket count.
     */
    public TableDescriptor withBucketCount(int newBucketCount) {
        return new TableDescriptor(
                schema,
                comment,
                partitionKeys,
                new TableDistribution(
                        newBucketCount,
                        Optional.ofNullable(tableDistribution)
                                .map(TableDistribution::getBucketKeys)
                                .orElse(Collections.emptyList())),
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
                && Objects.equals(tableDistribution, table.tableDistribution)
                && Objects.equals(properties, table.properties)
                && Objects.equals(customProperties, table.customProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema, comment, partitionKeys, tableDistribution, properties, customProperties);
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
            List<String> partitionKeys,
            @Nullable TableDistribution originDistribution) {
        if (originDistribution != null) {
            // we may need to check and normalize bucket key
            List<String> bucketKeys = originDistribution.getBucketKeys();
            // bucket key shouldn't include partition key
            if (bucketKeys.stream().anyMatch(partitionKeys::contains)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Bucket key %s shouldn't include any column in partition keys %s.",
                                bucketKeys, partitionKeys));
            }

            // if primary key set
            if (schema.getPrimaryKey().isPresent()) {
                // if bucket key is empty, force to set bucket keys
                if (bucketKeys.isEmpty()) {
                    return new TableDistribution(
                            originDistribution.getBucketCount().orElse(null),
                            defaultBucketKeyOfPrimaryKeyTable(schema, partitionKeys));
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
                                        pkColumns, partitionKeys, bucketKeys));
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
                        null, defaultBucketKeyOfPrimaryKeyTable(schema, partitionKeys));
            } else {
                return originDistribution;
            }
        }
    }

    /** The default bucket key of primary key table is the primary key excluding partition keys. */
    private static List<String> defaultBucketKeyOfPrimaryKeyTable(
            Schema schema, List<String> partitionKeys) {
        checkArgument(schema.getPrimaryKey().isPresent(), "Primary key must be set.");
        List<String> bucketKeys = new ArrayList<>(schema.getPrimaryKey().get().getColumnNames());
        bucketKeys.removeAll(partitionKeys);
        if (bucketKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Primary Key constraint %s should not be same with partition fields %s.",
                            schema.getPrimaryKey().get().getColumnNames(), partitionKeys));
        }

        return bucketKeys;
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
        private @Nullable String comment;
        private @Nullable TableDistribution tableDistribution;

        protected Builder() {
            this.properties = new HashMap<>();
            this.partitionKeys = new ArrayList<>();
            this.customProperties = new HashMap<>();
        }

        protected Builder(TableDescriptor descriptor) {
            this.schema = descriptor.getSchema();
            this.properties = new HashMap<>(descriptor.getProperties());
            this.customProperties = new HashMap<>(descriptor.getCustomProperties());
            this.partitionKeys = new ArrayList<>(descriptor.getPartitionKeys());
            this.comment = descriptor.getComment().orElse(null);
            this.tableDistribution = descriptor.getTableDistribution().orElse(null);
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
            this.partitionKeys.clear();
            this.partitionKeys.addAll(partitionKeys);
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
                    tableDistribution,
                    properties,
                    customProperties);
        }
    }
}

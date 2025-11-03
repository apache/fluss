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

package org.apache.fluss.connector.trino.handle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Column handle for Fluss Trino connector with support for:
 * - Column metadata
 * - Partition key information
 * - Primary key information
 * - Column pruning optimization
 */
public class FlussColumnHandle implements ColumnHandle {

    private final String name;
    private final Type type;
    private final int ordinalPosition;
    private final boolean isPartitionKey;
    private final boolean isPrimaryKey;
    private final boolean isNullable;
    private final Optional<String> comment;
    private final Optional<String> defaultValue;

    @JsonCreator
    public FlussColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("isPartitionKey") boolean isPartitionKey,
            @JsonProperty("isPrimaryKey") boolean isPrimaryKey,
            @JsonProperty("isNullable") boolean isNullable,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("defaultValue") Optional<String> defaultValue) {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.ordinalPosition = ordinalPosition;
        this.isPartitionKey = isPartitionKey;
        this.isPrimaryKey = isPrimaryKey;
        this.isNullable = isNullable;
        this.comment = requireNonNull(comment, "comment is null");
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
    }

    // Convenience constructor for basic column handle
    public FlussColumnHandle(String name, Type type, int ordinalPosition) {
        this(name, type, ordinalPosition, false, false, true,   
             Optional.empty(), Optional.empty());
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    @JsonProperty
    public boolean isPartitionKey() {
        return isPartitionKey;
    }

    @JsonProperty
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    @JsonProperty
    public boolean isNullable() {
        return isNullable;
    }

    @JsonProperty
    public Optional<String> getComment() {
        return comment;
    }

    @JsonProperty
    public Optional<String> getDefaultValue() {
        return defaultValue;
    }

    /**
     * Create a new column handle with partition key flag.
     */
    public FlussColumnHandle withPartitionKey(boolean partitionKey) {
        return new FlussColumnHandle(
                name, type, ordinalPosition, partitionKey, isPrimaryKey,   
                isNullable, comment, defaultValue);
    }

    /**
     * Create a new column handle with primary key flag.
     */
    public FlussColumnHandle withPrimaryKey(boolean primaryKey) {
        return new FlussColumnHandle(
                name, type, ordinalPosition, isPartitionKey, primaryKey,   
                isNullable, comment, defaultValue);
    }

    /**
     * Create a new column handle with nullable flag.
     */
    public FlussColumnHandle withNullable(boolean nullable) {
        return new FlussColumnHandle(
                name, type, ordinalPosition, isPartitionKey, isPrimaryKey,   
                nullable, comment, defaultValue);
    }

    /**
     * Create a new column handle with comment.
     */
    public FlussColumnHandle withComment(String commentText) {
        return new FlussColumnHandle(
                name, type, ordinalPosition, isPartitionKey, isPrimaryKey,   
                isNullable, Optional.ofNullable(commentText), defaultValue);
    }

    /**
     * Create a new column handle with default value.
     */
    public FlussColumnHandle withDefaultValue(String defaultVal) {
        return new FlussColumnHandle(
                name, type, ordinalPosition, isPartitionKey, isPrimaryKey,   
                isNullable, comment, Optional.ofNullable(defaultVal));
    }

    /**
     * Check if this column can be used for partition pruning.
     */
    public boolean canPrunePartition() {
        return isPartitionKey;
    }

    /**
     * Check if this column can be used for point queries.
     */
    public boolean canPointQuery() {
        return isPrimaryKey;
    }

    /**
     * Get column metadata for Trino.
     */
    public ColumnMetadata toColumnMetadata() {
        ColumnMetadata.Builder builder =   
            ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setNullable(isNullable);
          
        comment.ifPresent(builder::setComment);
          
        return builder.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FlussColumnHandle that = (FlussColumnHandle) obj;
        return ordinalPosition == that.ordinalPosition &&
                isPartitionKey == that.isPartitionKey &&
                isPrimaryKey == that.isPrimaryKey &&
                isNullable == that.isNullable &&
                Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(comment, that.comment) &&
                Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, ordinalPosition, isPartitionKey,   
                           isPrimaryKey, isNullable, comment, defaultValue);
    }

    @Override
    public String toString() {
        return "FlussColumnHandle{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", ordinalPosition=" + ordinalPosition +
                ", isPartitionKey=" + isPartitionKey +
                ", isPrimaryKey=" + isPrimaryKey +
                ", isNullable=" + isNullable +
                ", comment=" + comment +
                ", defaultValue=" + defaultValue +
                '}';
    }
}

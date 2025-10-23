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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableDescriptor.TableDistribution;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The registration information of table in {@link ZkData.TableZNode}. It is used to store the table
 * information in zookeeper. Basically, it contains the same information with {@link TableInfo}
 * besides the {@link Schema} part and schema id. Because schema metadata is stored in a separate
 * {@code SchemaZNode}.
 *
 * @see TableRegistrationJsonSerde for json serialization and deserialization.
 */
public class TableRegistration {

    public final long tableId;
    public final @Nullable String comment;
    public final List<String> partitionKeys;
    public final List<String> bucketKeys;
    public final int bucketCount;
    public final Map<String, String> properties;
    public final Map<String, String> customProperties;
    public final long createdTime;
    public final long modifiedTime;

    public TableRegistration(
            long tableId,
            @Nullable String comment,
            List<String> partitionKeys,
            TableDistribution tableDistribution,
            Map<String, String> properties,
            Map<String, String> customProperties,
            long createdTime,
            long modifiedTime) {
        checkArgument(
                tableDistribution.getBucketCount().isPresent(),
                "Bucket count is required for table registration.");
        this.tableId = tableId;
        this.comment = comment;
        this.partitionKeys = partitionKeys;
        this.bucketCount = tableDistribution.getBucketCount().get();
        this.bucketKeys = tableDistribution.getBucketKeys();
        this.properties = properties;
        this.customProperties = customProperties;
        this.createdTime = createdTime;
        this.modifiedTime = modifiedTime;
    }

    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    public TableConfig getTableConfig() {
        return new TableConfig(Configuration.fromMap(properties));
    }

    public TableInfo toTableInfo(TablePath tablePath, SchemaInfo schemaInfo) {
        return toTableInfo(tablePath, schemaInfo, null);
    }

    public TableInfo toTableInfo(
            TablePath tablePath,
            SchemaInfo schemaInfo,
            @Nullable Map<String, String> defaultTableLakeOptions) {
        Configuration properties = Configuration.fromMap(this.properties);
        if (defaultTableLakeOptions != null) {
            if (properties.get(ConfigOptions.TABLE_DATALAKE_ENABLED)) {
                // only make the lake options visible when the datalake is enabled on the table
                defaultTableLakeOptions.forEach(properties::setString);
            }
        }
        return new TableInfo(
                tablePath,
                this.tableId,
                schemaInfo.getSchemaId(),
                schemaInfo.getSchema(),
                this.bucketKeys,
                this.partitionKeys,
                this.bucketCount,
                properties,
                Configuration.fromMap(this.customProperties),
                this.comment,
                this.createdTime,
                this.modifiedTime);
    }

    public static TableRegistration newTable(long tableId, TableDescriptor tableDescriptor) {
        checkArgument(
                tableDescriptor.getTableDistribution().isPresent(),
                "Table distribution is required for table registration.");
        final long currentMillis = System.currentTimeMillis();
        return new TableRegistration(
                tableId,
                tableDescriptor.getComment().orElse(null),
                tableDescriptor.getPartitionKeys(),
                tableDescriptor.getTableDistribution().get(),
                tableDescriptor.getProperties(),
                tableDescriptor.getCustomProperties(),
                currentMillis,
                currentMillis);
    }

    public TableRegistration newProperties(
            Map<String, String> newProperties, Map<String, String> newCustomProperties) {
        final long currentMillis = System.currentTimeMillis();
        return new TableRegistration(
                tableId,
                comment,
                partitionKeys,
                new TableDistribution(bucketCount, bucketKeys),
                newProperties,
                newCustomProperties,
                createdTime,
                currentMillis);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableRegistration that = (TableRegistration) o;
        return tableId == that.tableId
                && createdTime == that.createdTime
                && modifiedTime == that.modifiedTime
                && Objects.equals(comment, that.comment)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(bucketCount, that.bucketCount)
                && Objects.equals(bucketKeys, that.bucketKeys)
                && Objects.equals(properties, that.properties)
                && Objects.equals(customProperties, that.customProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableId,
                comment,
                partitionKeys,
                bucketCount,
                bucketKeys,
                properties,
                customProperties,
                createdTime,
                modifiedTime);
    }

    @Override
    public String toString() {
        return "TableRegistration{"
                + "tableId="
                + tableId
                + ", comment='"
                + comment
                + '\''
                + ", partitionKeys="
                + partitionKeys
                + ", bucketCount="
                + bucketCount
                + ", bucketKeys="
                + bucketKeys
                + ", properties="
                + properties
                + ", customProperties="
                + customProperties
                + ", createdTime="
                + createdTime
                + ", modifiedTime="
                + modifiedTime
                + '}';
    }
}

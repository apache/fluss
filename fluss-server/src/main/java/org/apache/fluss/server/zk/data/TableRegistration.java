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

    /**
     * The remote data directory of the table. It is null if and only if it is deserialized by
     * {@link TableRegistrationJsonSerde} from an existing node produced by an older version that
     * does not support multiple remote paths. But immediately after that, we will set it as the
     * default remote file path configured by {@link ConfigOptions#REMOTE_DATA_DIR} (see {@link
     * org.apache.fluss.server.zk.ZooKeeperClient#getTable}). This unifies subsequent usage and
     * eliminates the need to account for differences between versions.
     */
    public final @Nullable String remoteDataDir;

    public final long createdTime;
    public final long modifiedTime;

    /**
     * A table-level, monotonically increasing version for bucket.num changes. New tables start at
     * 0; a legacy JSON without the field is read as 0; every committed bucket.num change increments
     * it. It is used to decide whether legacy clients without bucket count are still allowed (epoch
     * 0) and to let TabletServers ignore older UpdateMetadata messages.
     */
    public final long bucketLayoutEpoch;

    public TableRegistration(
            long tableId,
            @Nullable String comment,
            List<String> partitionKeys,
            TableDistribution tableDistribution,
            Map<String, String> properties,
            Map<String, String> customProperties,
            @Nullable String remoteDataDir,
            long createdTime,
            long modifiedTime) {
        this(
                tableId,
                comment,
                partitionKeys,
                tableDistribution,
                properties,
                customProperties,
                remoteDataDir,
                createdTime,
                modifiedTime,
                0L);
    }

    public TableRegistration(
            long tableId,
            @Nullable String comment,
            List<String> partitionKeys,
            TableDistribution tableDistribution,
            Map<String, String> properties,
            Map<String, String> customProperties,
            @Nullable String remoteDataDir,
            long createdTime,
            long modifiedTime,
            long bucketLayoutEpoch) {
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
        this.remoteDataDir = remoteDataDir;
        this.createdTime = createdTime;
        this.modifiedTime = modifiedTime;
        this.bucketLayoutEpoch = bucketLayoutEpoch;
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
                this.remoteDataDir,
                this.comment,
                this.createdTime,
                this.modifiedTime,
                this.bucketLayoutEpoch);
    }

    public static TableRegistration newTable(
            long tableId, String remoteDataDir, TableDescriptor tableDescriptor) {
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
                remoteDataDir,
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
                remoteDataDir,
                createdTime,
                currentMillis,
                bucketLayoutEpoch);
    }

    /**
     * Replaces the table-level bucket count and increments {@code bucketLayoutEpoch} atomically.
     * For a partitioned table, the new count applies to partitions created after this ALTER;
     * existing partitions retain their actual bucket counts in their partition registrations.
     */
    public TableRegistration withBucketCount(int newBucketCount) {
        final long currentMillis = System.currentTimeMillis();
        return new TableRegistration(
                tableId,
                comment,
                partitionKeys,
                new TableDistribution(newBucketCount, bucketKeys),
                properties,
                customProperties,
                remoteDataDir,
                createdTime,
                currentMillis,
                bucketLayoutEpoch + 1);
    }

    /**
     * Returns a new registration with the given remote data directory. Should only be called by
     * {@link org.apache.fluss.server.zk.ZooKeeperClient#getTable} when deserialize an old
     * TableRegistration node without remote data dir configured.
     *
     * @param remoteDataDir the remote data directory
     * @return a new registration with the given remote data directory
     */
    public TableRegistration newRemoteDataDir(String remoteDataDir) {
        return new TableRegistration(
                tableId,
                comment,
                partitionKeys,
                new TableDistribution(bucketCount, bucketKeys),
                properties,
                customProperties,
                remoteDataDir,
                createdTime,
                modifiedTime,
                bucketLayoutEpoch);
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
                && bucketLayoutEpoch == that.bucketLayoutEpoch
                && Objects.equals(comment, that.comment)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(bucketCount, that.bucketCount)
                && Objects.equals(bucketKeys, that.bucketKeys)
                && Objects.equals(properties, that.properties)
                && Objects.equals(customProperties, that.customProperties)
                && Objects.equals(remoteDataDir, that.remoteDataDir);
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
                remoteDataDir,
                createdTime,
                modifiedTime,
                bucketLayoutEpoch);
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
                + ", remoteDataDir="
                + remoteDataDir
                + ", createdTime="
                + createdTime
                + ", modifiedTime="
                + modifiedTime
                + ", bucketLayoutEpoch="
                + bucketLayoutEpoch
                + '}';
    }
}

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

package org.apache.fluss.server.metadata;

import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/** This entity used to describe the table metadata. */
public class TableMetadata {

    /**
     * The already deleted tablePath. This tablePath will be used in UpdateMetadata request to
     * identify this tablePath already deleted, but there is an tableId residual in zookeeper. In
     * this case, tabletServers need to clear the metadata of this tableId.
     */
    public static final TablePath DELETED_TABLE_PATH = TablePath.of("__UNKNOWN__", "__delete__");

    /**
     * The already deleted table id. This table id will be used in UpdateMetadata request to
     * identify this table already deleted, and tabletServers need to clear the metadata of this
     * table.
     */
    public static final Long DELETED_TABLE_ID = -2L;

    private final long tableId;

    private final int schemaId;

    private final long createdTime;

    private final long modifiedTime;

    private final TablePath tablePath;

    /** Will only be set for {@link org.apache.fluss.rpc.messages.MetadataResponse}. */
    private final @Nullable TableInfo tableInfo;

    /**
     * For partition table, this list is always empty. The detail partition metadata is stored in
     * {@link PartitionMetadata}. By doing this, we can avoid to repeat send tableInfo when
     * create/drop partitions.
     *
     * <p>Note: If we try to update partition metadata, we must make sure we have already updated
     * the tableInfo for this partition table.
     */
    private final List<BucketMetadata> bucketMetadataList;

    public TableMetadata(TableInfo tableInfo, List<BucketMetadata> bucketMetadataList) {
        this(
                tableInfo.getTableId(),
                tableInfo.getSchemaId(),
                tableInfo.getCreatedTime(),
                tableInfo.getModifiedTime(),
                tableInfo.getTablePath(),
                bucketMetadataList,
                tableInfo);
    }

    public TableMetadata(
            long tableId,
            int schemaId,
            long createdTime,
            long modifiedTime,
            TablePath tablePath,
            List<BucketMetadata> bucketMetadataList) {
        this(tableId, schemaId, createdTime, modifiedTime, tablePath, bucketMetadataList, null);
    }

    public TableMetadata(
            long tableId,
            int schemaId,
            long createdTime,
            long modifiedTime,
            TablePath tablePath,
            List<BucketMetadata> bucketMetadataList,
            TableInfo tableInfo) {
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.createdTime = createdTime;
        this.modifiedTime = modifiedTime;
        this.tablePath = tablePath;
        this.bucketMetadataList = bucketMetadataList;
        this.tableInfo = tableInfo;
    }

    public long getTableId() {
        return tableId;
    }

    public int getSchemaId() {
        return schemaId;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    @Nullable
    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public List<BucketMetadata> getBucketMetadataList() {
        return bucketMetadataList;
    }

    @Override
    public String toString() {
        return "TableMetadata{"
                + "tableId="
                + tableId
                + ", schemaId="
                + schemaId
                + ", createdTime="
                + createdTime
                + ", modifiedTime="
                + modifiedTime
                + ", tablePath="
                + tablePath
                + ", tableInfo="
                + tableInfo
                + ", bucketMetadataList="
                + bucketMetadataList
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableMetadata that = (TableMetadata) o;
        return tableId == that.tableId
                && schemaId == that.schemaId
                && createdTime == that.createdTime
                && modifiedTime == that.modifiedTime
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(tableInfo, that.tableInfo)
                && Objects.equals(bucketMetadataList, that.bucketMetadataList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableId,
                schemaId,
                createdTime,
                modifiedTime,
                tablePath,
                tableInfo,
                bucketMetadataList);
    }
}

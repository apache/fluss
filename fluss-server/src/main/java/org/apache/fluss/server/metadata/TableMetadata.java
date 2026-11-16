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
import org.apache.fluss.rpc.protocol.UpdateMetadataRequestCompat;

import java.util.List;
import java.util.Objects;

/** This entity used to describe the table metadata. */
public class TableMetadata {

    /**
     * The already deleted tablePath. This tablePath will be used in UpdateMetadata request to
     * identify this tablePath already deleted, but there is an tableId residual in zookeeper. In
     * this case, tabletServers need to clear the metadata of this tableId.
     */
    public static final TablePath DELETED_TABLE_PATH =
            UpdateMetadataRequestCompat.DELETED_TABLE_PATH;

    /**
     * The already deleted table id. This table id will be used in UpdateMetadata request to
     * identify this table already deleted, and tabletServers need to clear the metadata of this
     * table.
     */
    public static final Long DELETED_TABLE_ID = UpdateMetadataRequestCompat.DELETED_TABLE_ID;

    private final TableInfo tableInfo;

    /**
     * For partition table, this list is always empty. The detail partition metadata is stored in
     * {@link PartitionMetadata}. By doing this, we can avoid to repeat send tableInfo when
     * create/drop partitions.
     *
     * <p>Note: If we try to update partition metadata, we must make sure we have already updated
     * the tableInfo for this partition table.
     */
    private final List<BucketMetadata> bucketMetadataList;

    private final boolean deleted;

    public TableMetadata(TableInfo tableInfo, List<BucketMetadata> bucketMetadataList) {
        this(tableInfo, bucketMetadataList, false);
    }

    /** Creates table metadata for a coordinator update, retaining the table ID on deletion. */
    public TableMetadata(
            TableInfo tableInfo, List<BucketMetadata> bucketMetadataList, boolean deleted) {
        this.tableInfo = tableInfo;
        this.bucketMetadataList = bucketMetadataList;
        this.deleted =
                deleted
                        || tableInfo.getTableId() == DELETED_TABLE_ID
                        || DELETED_TABLE_PATH.equals(tableInfo.getTablePath());
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public List<BucketMetadata> getBucketMetadataList() {
        return bucketMetadataList;
    }

    /** Returns whether this is an explicit deletion or a legacy deletion marker. */
    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public String toString() {
        return "TableMetadata{"
                + "tableInfo="
                + tableInfo
                + ", bucketMetadataList="
                + bucketMetadataList
                + ", deleted="
                + deleted
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableMetadata that = (TableMetadata) o;
        if (!tableInfo.equals(that.tableInfo)) {
            return false;
        }
        return bucketMetadataList.equals(that.bucketMetadataList) && deleted == that.deleted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableInfo, bucketMetadataList, deleted);
    }
}

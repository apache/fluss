/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.metadata;

import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;

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

    public TableMetadata(TableInfo tableInfo, List<BucketMetadata> bucketMetadataList) {
        this.tableInfo = tableInfo;
        this.bucketMetadataList = bucketMetadataList;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public List<BucketMetadata> getBucketMetadataList() {
        return bucketMetadataList;
    }

    @Override
    public String toString() {
        return "TableMetadata{"
                + "tableInfo="
                + tableInfo
                + ", bucketMetadataList="
                + bucketMetadataList
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
        return bucketMetadataList.equals(that.bucketMetadataList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableInfo, bucketMetadataList);
    }
}

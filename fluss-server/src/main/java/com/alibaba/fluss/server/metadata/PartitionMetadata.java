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

import java.util.List;

/** This entity used to describe the table's partition metadata. */
public class PartitionMetadata {

    /**
     * The already deleted partitionName. This partitionName will be used in UpdateMetadata request
     * to identify this partition already deleted, but there is an partitionId residual in
     * zookeeper. In this case, tabletServers need to clear the metadata of this partition.
     */
    public static final String DELETED_PARTITION_NAME = "__delete__";

    /**
     * The already delete partition id. This partition id will be used in UpdateMetadata request to
     * identify this partition already deleted, and tabletServers need to clear the metadata of this
     * partition.
     */
    public static final Long DELETED_PARTITION_ID = -2L;

    private final long tableId;
    private final String partitionName;
    private final long partitionId;
    private final List<BucketMetadata> bucketMetadataList;

    public PartitionMetadata(
            long tableId,
            String partitionName,
            long partitionId,
            List<BucketMetadata> bucketMetadataList) {
        this.tableId = tableId;
        this.partitionName = partitionName;
        this.partitionId = partitionId;
        this.bucketMetadataList = bucketMetadataList;
    }

    public long getTableId() {
        return tableId;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public List<BucketMetadata> getBucketMetadataList() {
        return bucketMetadataList;
    }
}

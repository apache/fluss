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

import com.alibaba.fluss.cluster.BucketLocation;

import java.util.List;

/** This entity used to describe the table's partition info. */
public class PartitionMetadataInfo {

    private final long tableId;
    private final String partitionName;
    private final long partitionId;
    private final List<BucketLocation> bucketLocations;

    public PartitionMetadataInfo(
            long tableId,
            String partitionName,
            long partitionId,
            List<BucketLocation> bucketLocations) {
        this.tableId = tableId;
        this.partitionName = partitionName;
        this.partitionId = partitionId;
        this.bucketLocations = bucketLocations;
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

    public List<BucketLocation> getBucketLocations() {
        return bucketLocations;
    }
}

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

package org.apache.fluss.lake.committer;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** The bucket offset information to be expected to be stored in Lake's snapshot property. */
public class BucketOffset implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final String FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY = "fluss-offsets";

    private final long logOffset;
    private final int bucket;
    private final @Nullable Long partitionId;
    private final @Nullable String partitionQualifiedName;

    public BucketOffset(
            long logOffset,
            int bucket,
            @Nullable Long partitionId,
            @Nullable String partitionQualifiedName) {
        this.logOffset = logOffset;
        this.bucket = bucket;
        this.partitionId = partitionId;
        this.partitionQualifiedName = partitionQualifiedName;
    }

    public long getLogOffset() {
        return logOffset;
    }

    public int getBucket() {
        return bucket;
    }

    @Nullable
    public Long getPartitionId() {
        return partitionId;
    }

    @Nullable
    public String getPartitionQualifiedName() {
        return partitionQualifiedName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BucketOffset that = (BucketOffset) o;
        return bucket == that.bucket
                && logOffset == that.logOffset
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(partitionQualifiedName, that.partitionQualifiedName);
    }
}

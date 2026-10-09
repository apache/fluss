/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.trino;

import java.util.Objects;
import java.util.Optional;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

final class FlussPhysicalBucket {
    private final FlussBucketHandle bucket;
    private final Optional<String> partitionName;

    FlussPhysicalBucket(FlussBucketHandle bucket, Optional<String> partitionName) {
        this.bucket = checkNotNull(bucket, "bucket is null");
        this.partitionName = checkNotNull(partitionName, "partitionName is null");

        checkArgument(
                bucket.isPartitioned() == partitionName.isPresent(),
                "partition ID and partition name must either both be present or both be absent");
    }

    FlussBucketHandle getBucket() {
        return bucket;
    }

    String getRequiredPartitionName() {
        return partitionName.orElseThrow(
                () -> new IllegalStateException("Bucket does not belong to a partition"));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussPhysicalBucket that = (FlussPhysicalBucket) o;
        return Objects.equals(bucket, that.bucket)
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucket, partitionName);
    }
}

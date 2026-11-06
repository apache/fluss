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

import org.apache.fluss.metadata.TableBucket;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.SizeOf.LONG_INSTANCE_SIZE;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable Fluss bucket handle. */
public final class FlussBucketHandle {
    private static final int INSTANCE_SIZE = instanceSize(FlussBucketHandle.class);
    private final long tableId;
    private final Optional<Long> partitionId;
    private final int bucketId;

    public FlussBucketHandle(long tableId, Optional<Long> partitionId, int bucketId) {
        checkArgument(tableId >= 0, "tableId must be non-negative");
        this.partitionId = checkNotNull(partitionId, "partitionId is null");
        partitionId.ifPresent(
                value -> checkArgument(value >= 0, "partitionId must be non-negative"));
        checkArgument(bucketId >= 0, "bucketId must be non-negative");

        this.tableId = tableId;
        this.bucketId = bucketId;
    }

    /** Creates a bucket handle from JSON with its table and bucket IDs required. */
    @JsonCreator
    public static FlussBucketHandle fromJson(
            @JsonProperty("tableId") Long tableId,
            @JsonProperty("partitionId") Optional<Long> partitionId,
            @JsonProperty("bucketId") Integer bucketId) {
        checkArgument(tableId != null, "tableId is required");
        checkArgument(bucketId != null, "bucketId is required");

        return new FlussBucketHandle(tableId, partitionId, bucketId);
    }

    @JsonProperty
    public long getTableId() {
        return tableId;
    }

    @JsonProperty
    public Optional<Long> getPartitionId() {
        return partitionId;
    }

    @JsonProperty
    public int getBucketId() {
        return bucketId;
    }

    boolean isPartitioned() {
        return partitionId.isPresent();
    }

    long getRequiredPartitionId() {
        return partitionId.orElseThrow(
                () -> new IllegalStateException("Bucket does not belong to a partition"));
    }

    TableBucket toTableBucket() {
        return new TableBucket(tableId, partitionId.orElse(null), bucketId);
    }

    long getRetainedSizeInBytes() {
        return INSTANCE_SIZE + sizeOf(partitionId, ignored -> LONG_INSTANCE_SIZE);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussBucketHandle that = (FlussBucketHandle) o;
        return tableId == that.tableId
                && bucketId == that.bucketId
                && Objects.equals(partitionId, that.partitionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, partitionId, bucketId);
    }

    @Override
    public String toString() {
        return "FlussBucketHandle{"
                + "tableId="
                + tableId
                + ", partitionId="
                + partitionId
                + ", bucketId="
                + bucketId
                + '}';
    }
}

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;

import static io.airlift.slice.SizeOf.instanceSize;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/** A fixed, exclusive-end log range for one Fluss bucket. */
public final class FlussSplit implements ConnectorSplit {
    private static final int INSTANCE_SIZE = instanceSize(FlussSplit.class);

    private final int bucketId;
    private final long startOffset;
    private final long stoppingOffset;

    /** Creates a bucket range; an empty range is valid. */
    @JsonCreator
    public FlussSplit(
            @JsonProperty("bucketId") int bucketId,
            @JsonProperty("startOffset") long startOffset,
            @JsonProperty("stoppingOffset") long stoppingOffset) {
        checkArgument(bucketId >= 0, "bucketId must be non-negative");
        checkArgument(startOffset >= 0, "startOffset must be non-negative");
        checkArgument(stoppingOffset >= startOffset, "stoppingOffset must not precede startOffset");
        this.bucketId = bucketId;
        this.startOffset = startOffset;
        this.stoppingOffset = stoppingOffset;
    }

    /** Returns the bucket to scan. */
    @JsonProperty
    public int getBucketId() {
        return bucketId;
    }

    /** Returns the inclusive starting offset. */
    @JsonProperty
    public long getStartOffset() {
        return startOffset;
    }

    /** Returns the exclusive stopping offset. */
    @JsonProperty
    public long getStoppingOffset() {
        return stoppingOffset;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE;
    }

    @Override
    public String toString() {
        return bucketId + ":[" + startOffset + "," + stoppingOffset + ")";
    }
}

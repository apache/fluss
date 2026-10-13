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

package org.apache.fluss.rpc.entity;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.utils.ByteArraySlice;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Result of {@link LookupRequest} for each table bucket.
 *
 * <p>Successful lookup values are already converted to the RPC value representation.
 */
public class LookupResultForBucket extends ResultForBucket {

    private final List<ByteArraySlice> values;

    /** Identifies the original partition for historical lookup; null for normal lookup. */
    private final @Nullable String originalPartitionName;

    /** Creates a successful lookup result with RPC-ready values. */
    public LookupResultForBucket(TableBucket tableBucket, List<ByteArraySlice> values) {
        this(tableBucket, values, null, ApiError.NONE);
    }

    /** Creates a failed lookup result. */
    public LookupResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, null, null, error);
    }

    /** Creates a successful historical lookup result. */
    public LookupResultForBucket(
            TableBucket tableBucket, List<ByteArraySlice> values, String originalPartitionName) {
        this(tableBucket, values, originalPartitionName, ApiError.NONE);
    }

    /** Creates a failed historical lookup result. */
    public LookupResultForBucket(
            TableBucket tableBucket, String originalPartitionName, ApiError error) {
        this(tableBucket, null, originalPartitionName, error);
    }

    private LookupResultForBucket(
            TableBucket tableBucket,
            List<ByteArraySlice> values,
            @Nullable String originalPartitionName,
            ApiError error) {
        super(tableBucket, error);
        this.values = values;
        this.originalPartitionName = originalPartitionName;
    }

    /** Returns the RPC-ready lookup values. */
    public List<ByteArraySlice> lookupValues() {
        return values;
    }

    /** Returns the original partition name for historical lookup, or null for normal lookup. */
    public @Nullable String originalPartitionName() {
        return originalPartitionName;
    }
}

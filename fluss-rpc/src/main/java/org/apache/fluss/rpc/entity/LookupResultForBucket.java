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

import javax.annotation.Nullable;

import java.util.List;

/** Result of {@link LookupRequest} for each table bucket. */
public class LookupResultForBucket extends ResultForBucket {

    private final List<byte[]> values;

    /** Identifies the original partition for historical lookup; null for normal lookup. */
    private final @Nullable String originalPartitionName;

    public LookupResultForBucket(TableBucket tableBucket, List<byte[]> values) {
        this(tableBucket, values, null, ApiError.NONE);
    }

    public LookupResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, null, null, error);
    }

    /**
     * Creates a lookup result with the original partition name used to identify a historical lookup
     * response.
     */
    public LookupResultForBucket(
            TableBucket tableBucket,
            List<byte[]> values,
            @Nullable String originalPartitionName,
            ApiError error) {
        super(tableBucket, error);
        this.values = values;
        this.originalPartitionName = originalPartitionName;
    }

    public List<byte[]> lookupValues() {
        return values;
    }

    /** Returns the original partition name for historical lookup, or null for normal lookup. */
    public @Nullable String originalPartitionName() {
        return originalPartitionName;
    }
}

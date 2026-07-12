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

package org.apache.fluss.server.entity;

import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Lookup request data for one table bucket. */
public final class LookupDataForBucket {

    private final TableBucket tableBucket;
    private final List<byte[]> keys;

    // Null for normal local KV lookup. Set to the original partition name when the target
    // TableBucket is the historical system partition.
    private final @Nullable String originalPartitionName;

    public LookupDataForBucket(
            TableBucket tableBucket, List<byte[]> keys, @Nullable String originalPartitionName) {
        this.tableBucket = checkNotNull(tableBucket, "tableBucket must not be null.");
        this.keys = checkNotNull(keys, "keys must not be null.");
        this.originalPartitionName = originalPartitionName;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public List<byte[]> keys() {
        return keys;
    }

    public @Nullable String originalPartitionName() {
        return originalPartitionName;
    }
}

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

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.ReadDirection;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Class to represent a prefix lookup operation, it contains the table id, bucketNums and related
 * CompletableFuture.
 */
@Internal
public class PrefixLookupQuery extends AbstractLookupQuery<List<byte[]>> {
    private final CompletableFuture<List<byte[]>> future;
    @Nullable private final byte[] startKey;
    @Nullable private final byte[] endKey;
    @Nullable private final ReadDirection direction;

    PrefixLookupQuery(
            TablePath tablePath, TableBucket tableBucket, byte[] prefixKey, int bucketCount) {
        this(tablePath, tableBucket, prefixKey, bucketCount, null, null, null);
    }

    PrefixLookupQuery(
            TablePath tablePath,
            TableBucket tableBucket,
            byte[] prefixKey,
            int bucketCount,
            @Nullable byte[] startKey,
            @Nullable byte[] endKey,
            @Nullable ReadDirection direction) {
        super(tablePath, tableBucket, prefixKey, null, bucketCount);
        this.future = new CompletableFuture<>();
        this.startKey = startKey;
        this.endKey = endKey;
        this.direction = direction;
    }

    PrefixLookupQuery(TablePath tablePath, TableBucket tableBucket, byte[] prefixKey) {
        this(tablePath, tableBucket, prefixKey, 0);
    }

    @Override
    public CompletableFuture<List<byte[]>> future() {
        return future;
    }

    @Override
    public LookupType lookupType() {
        return LookupType.PREFIX_LOOKUP;
    }

    /** Returns the inclusive physical start, or null for an unrestricted prefix. */
    @Nullable
    public byte[] startKey() {
        return startKey;
    }

    /** Returns the exclusive physical end, or null if unbounded. */
    @Nullable
    public byte[] endKey() {
        return endKey;
    }

    /** Returns the requested direction, or null for a legacy unordered read. */
    @Nullable
    public ReadDirection direction() {
        return direction;
    }
}

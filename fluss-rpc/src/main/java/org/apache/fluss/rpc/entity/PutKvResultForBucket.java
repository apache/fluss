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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;

import javax.annotation.Nullable;

/** Result of {@link PutKvRequest} for each table bucket. */
@Internal
public class PutKvResultForBucket extends WriteResultForBucket {

    /** Backpressure pressure value: 0=normal, (0,1)=DELAYED zone. */
    private final float pressure;

    private final @Nullable String originalPartitionName;

    public PutKvResultForBucket(TableBucket tableBucket, long changeLogEndOffset) {
        this(tableBucket, changeLogEndOffset, ApiError.NONE, 0f, null);
    }

    public PutKvResultForBucket(TableBucket tableBucket, long changeLogEndOffset, float pressure) {
        this(tableBucket, changeLogEndOffset, ApiError.NONE, pressure, null);
    }

    public PutKvResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, -1L, error, 0f, null);
    }

    public static PutKvResultForBucket historicalSuccess(
            TableBucket tableBucket,
            long changeLogEndOffset,
            @Nullable String originalPartitionName) {
        return new PutKvResultForBucket(
                tableBucket, changeLogEndOffset, ApiError.NONE, 0f, originalPartitionName);
    }

    public static PutKvResultForBucket historicalFailure(
            TableBucket tableBucket, ApiError error, @Nullable String originalPartitionName) {
        return new PutKvResultForBucket(tableBucket, -1L, error, 0f, originalPartitionName);
    }

    private PutKvResultForBucket(
            TableBucket tableBucket,
            long changeLogEndOffset,
            ApiError error,
            float pressure,
            @Nullable String originalPartitionName) {
        super(tableBucket, changeLogEndOffset, error);
        this.pressure = pressure;
        this.originalPartitionName = originalPartitionName;
    }

    public float getPressure() {
        return pressure;
    }

    /** Returns the original partition name for a historical write, or null for a normal write. */
    public @Nullable String getOriginalPartitionName() {
        return originalPartitionName;
    }

    @Override
    public <T extends WriteResultForBucket> T copy(Errors newError) {
        //noinspection unchecked
        return (T)
                new PutKvResultForBucket(
                        tableBucket, -1L, newError.toApiError(), 0f, originalPartitionName);
    }
}

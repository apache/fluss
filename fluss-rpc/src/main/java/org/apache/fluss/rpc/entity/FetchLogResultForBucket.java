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
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.remote.RemoteLogFetchInfo;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.protocol.ApiError;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Result of {@link FetchLogRequest} for each table bucket. */
@Internal
public class FetchLogResultForBucket extends ResultForBucket {
    public static final long NO_RESOLVED_EARLIEST_OFFSET = -1L;

    private final @Nullable RemoteLogFetchInfo remoteLogFetchInfo;
    private final @Nullable LogRecords records;
    private final long highWatermark;
    private final long filteredEndOffset;
    private final long minRetainOffset;
    private final long resolvedEarliestOffset;

    private FetchLogResultForBucket(
            TableBucket tableBucket,
            @Nullable RemoteLogFetchInfo remoteLogFetchInfo,
            @Nullable LogRecords records,
            long highWatermark,
            long filteredEndOffset,
            long minRetainOffset,
            long resolvedEarliestOffset,
            ApiError error) {
        super(tableBucket, error);
        this.remoteLogFetchInfo = remoteLogFetchInfo;
        this.records = records;
        this.highWatermark = highWatermark;
        this.filteredEndOffset = filteredEndOffset;
        this.minRetainOffset = minRetainOffset;
        this.resolvedEarliestOffset = resolvedEarliestOffset;
    }

    /** Creates a successful local fetch result. */
    public static FetchLogResultForBucket records(
            TableBucket tableBucket,
            LogRecords records,
            long highWatermark,
            long filteredEndOffset,
            long minRetainOffset) {
        return records(
                tableBucket,
                records,
                highWatermark,
                filteredEndOffset,
                minRetainOffset,
                NO_RESOLVED_EARLIEST_OFFSET);
    }

    /**
     * Creates a successful local fetch result with an EARLIEST offset resolution.
     *
     * <p>{@code resolvedEarliestOffset} must be non-negative when supplied, or {@link
     * #NO_RESOLVED_EARLIEST_OFFSET} when this fetch did not resolve a symbolic starting offset.
     */
    public static FetchLogResultForBucket records(
            TableBucket tableBucket,
            LogRecords records,
            long highWatermark,
            long filteredEndOffset,
            long minRetainOffset,
            long resolvedEarliestOffset) {
        checkArgument(minRetainOffset >= -1L, "Min retain offset must be at least -1.");

        checkArgument(
                resolvedEarliestOffset >= NO_RESOLVED_EARLIEST_OFFSET,
                "Resolved earliest offset must be at least %s.",
                NO_RESOLVED_EARLIEST_OFFSET);
        return new FetchLogResultForBucket(
                tableBucket,
                null,
                checkNotNull(records, "records can not be null"),
                highWatermark,
                filteredEndOffset,
                minRetainOffset,
                resolvedEarliestOffset,
                ApiError.NONE);
    }

    /** Creates a successful remote fetch result. */
    public static FetchLogResultForBucket remote(
            TableBucket tableBucket, RemoteLogFetchInfo remoteLogFetchInfo, long highWatermark) {
        return remote(tableBucket, remoteLogFetchInfo, highWatermark, NO_RESOLVED_EARLIEST_OFFSET);
    }

    /** Creates a successful remote fetch result with an EARLIEST offset resolution. */
    public static FetchLogResultForBucket remote(
            TableBucket tableBucket,
            RemoteLogFetchInfo remoteLogFetchInfo,
            long highWatermark,
            long resolvedEarliestOffset) {
        checkArgument(
                resolvedEarliestOffset >= NO_RESOLVED_EARLIEST_OFFSET,
                "Resolved earliest offset must be at least %s.",
                NO_RESOLVED_EARLIEST_OFFSET);
        return new FetchLogResultForBucket(
                tableBucket,
                checkNotNull(remoteLogFetchInfo, "remote log fetch info can not be null"),
                null,
                highWatermark,
                -1L,
                -1L,
                resolvedEarliestOffset,
                ApiError.NONE);
    }

    /** Creates a successful empty fetch result. */
    public static FetchLogResultForBucket empty(
            TableBucket tableBucket, long highWatermark, long filteredEndOffset) {
        return new FetchLogResultForBucket(
                tableBucket,
                null,
                null,
                highWatermark,
                filteredEndOffset,
                -1L,
                NO_RESOLVED_EARLIEST_OFFSET,
                ApiError.NONE);
    }

    /** Creates a failed fetch result. */
    public static FetchLogResultForBucket error(TableBucket tableBucket, ApiError error) {
        return new FetchLogResultForBucket(
                tableBucket, null, null, -1L, -1L, -1L, NO_RESOLVED_EARLIEST_OFFSET, error);
    }

    /**
     * The fetch result currently supporting only fetch from remote or fetch from local. It means
     * that if remoteLogFetchInfo is not null, the records should be null. Otherwise, the records
     * should not be null.
     *
     * @return {@code true} if the log is fetched from remote.
     */
    public boolean fetchFromRemote() {
        return remoteLogFetchInfo != null;
    }

    public @Nullable LogRecords records() {
        return records;
    }

    public LogRecords recordsOrEmpty() {
        if (records == null) {
            return MemoryLogRecords.EMPTY;
        } else {
            return records;
        }
    }

    public @Nullable RemoteLogFetchInfo remoteLogFetchInfo() {
        return remoteLogFetchInfo;
    }

    /**
     * Returns whether this response contains the physical offset resolved from an EARLIEST request.
     */
    public boolean hasResolvedEarliestOffset() {
        return resolvedEarliestOffset >= 0;
    }

    /**
     * Returns the physical offset resolved from an EARLIEST request, or {@link
     * #NO_RESOLVED_EARLIEST_OFFSET} when absent.
     */
    public long getResolvedEarliestOffset() {
        return resolvedEarliestOffset;
    }

    public long getHighWatermark() {
        return highWatermark;
    }

    /**
     * Returns whether a filtered end offset is set, indicating that server-side filtering was
     * applied and all batches were filtered out.
     */
    public boolean hasFilteredEndOffset() {
        return filteredEndOffset >= 0;
    }

    /**
     * Returns the offset up to which server-side filtering has been applied. Only meaningful when
     * {@link #hasFilteredEndOffset()} returns {@code true}.
     */
    public long getFilteredEndOffset() {
        return filteredEndOffset;
    }

    /** Returns whether a KV snapshot retention boundary is included in this fetch result. */
    public boolean hasMinRetainOffset() {
        return minRetainOffset >= 0;
    }

    /** Returns the KV snapshot retention boundary included in this fetch result. */
    public long getMinRetainOffset() {
        return minRetainOffset;
    }
}

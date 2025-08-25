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

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Result of {@link FetchLogRequest} for each table bucket. */
@Internal
public class FetchLogResultForBucket extends ResultForBucket {
    private final @Nullable RemoteLogFetchInfo remoteLogFetchInfo;
    private final @Nullable LogRecords records;
    private final long highWatermark;

    public FetchLogResultForBucket(
            TableBucket tableBucket, LogRecords records, long highWatermark) {
        this(
                tableBucket,
                null,
                checkNotNull(records, "records can not be null"),
                highWatermark,
                ApiError.NONE);
    }

    public FetchLogResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, null, null, -1L, error);
    }

    public FetchLogResultForBucket(
            TableBucket tableBucket, RemoteLogFetchInfo remoteLogFetchInfo, long highWatermark) {
        this(
                tableBucket,
                checkNotNull(remoteLogFetchInfo, "remote log fetch info can not be null"),
                null,
                highWatermark,
                ApiError.NONE);
    }

    private FetchLogResultForBucket(
            TableBucket tableBucket,
            @Nullable RemoteLogFetchInfo remoteLogFetchInfo,
            @Nullable LogRecords records,
            long highWatermark,
            ApiError error) {
        super(tableBucket, error);
        this.remoteLogFetchInfo = remoteLogFetchInfo;
        this.records = records;
        this.highWatermark = highWatermark;
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

    public long getHighWatermark() {
        return highWatermark;
    }
}

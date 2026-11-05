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

import java.util.Optional;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A storage scan for one Fluss bucket, with an exclusive-end range for log scans. */
public final class FlussSplit implements ConnectorSplit {
    private static final int INSTANCE_SIZE = instanceSize(FlussSplit.class);

    private final FlussScanType scanType;
    private final FlussBucketHandle bucket;
    private final Optional<FlussLogRange> logRange;

    @JsonCreator
    public FlussSplit(
            @JsonProperty("scanType") FlussScanType scanType,
            @JsonProperty("bucket") FlussBucketHandle bucket,
            @JsonProperty("logRange") Optional<FlussLogRange> logRange) {
        this.scanType = checkNotNull(scanType, "scanType is null");
        this.bucket = checkNotNull(bucket, "bucket is null");
        this.logRange = checkNotNull(logRange, "logRange is null");

        switch (scanType) {
            case LOG:
                checkArgument(logRange.isPresent(), "LOG split requires a log range");
                break;
            case KV:
                checkArgument(!logRange.isPresent(), "KV split cannot have a log range");
                break;
            default:
                throw new IllegalArgumentException("Unsupported scanType: " + scanType);
        }
    }

    static FlussSplit forLog(FlussBucketHandle bucket, long startOffset, long stoppingOffset) {
        return new FlussSplit(
                FlussScanType.LOG,
                bucket,
                Optional.of(new FlussLogRange(startOffset, stoppingOffset)));
    }

    static FlussSplit forKv(FlussBucketHandle bucket) {
        return new FlussSplit(FlussScanType.KV, bucket, Optional.empty());
    }

    @JsonProperty
    public FlussScanType getScanType() {
        return scanType;
    }

    @JsonProperty
    public FlussBucketHandle getBucket() {
        return bucket;
    }

    @JsonProperty
    public Optional<FlussLogRange> getLogRange() {
        return logRange;
    }

    FlussLogRange getRequiredLogRange() {
        return logRange.orElseThrow(
                () -> new IllegalStateException("Split does not contain a log range"));
    }

    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE
                + bucket.getRetainedSizeInBytes()
                + sizeOf(logRange, FlussLogRange::getRetainedSizeInBytes);
    }

    @Override
    public String toString() {
        switch (scanType) {
            case LOG:
                return "LOG " + bucket + ":" + getRequiredLogRange();
            case KV:
                return "KV " + bucket;
            default:
                throw new IllegalStateException("Unknown scan type: " + scanType);
        }
    }
}

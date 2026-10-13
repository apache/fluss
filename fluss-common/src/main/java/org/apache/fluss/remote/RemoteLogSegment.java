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

package org.apache.fluss.remote;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Describes one physical remote log object and the logical range for which the current manifest
 * references it. The physical range identifies bytes in remote storage, while the logical range may
 * be clipped when a newer segment overlaps this object.
 */
@Internal
public class RemoteLogSegment {
    private final PhysicalTablePath physicalTablePath;

    private final TableBucket tableBucket;

    /** Universally unique remote log segment id. */
    private final UUID remoteLogSegmentId;

    /** Inclusive physical start offset of this segment. */
    private final long remoteLogStartOffset;

    /** Exclusive physical end offset of this segment. */
    private final long remoteLogEndOffset;

    /** Inclusive logical start offset exposed by the current manifest. */
    private final long logicalStartOffset;

    /** Exclusive logical end offset exposed by the current manifest. */
    private final long logicalEndOffset;

    /** Max timestamp of this segment. */
    private final long maxTimestamp;

    private final int segmentSizeInBytes;

    private RemoteLogSegment(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            UUID remoteLogSegmentId,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            @Nullable Long logicalStartOffset,
            @Nullable Long logicalEndOffset,
            long maxTimestamp,
            int segmentSizeInBytes) {
        this.physicalTablePath = checkNotNull(physicalTablePath);
        this.tableBucket = checkNotNull(tableBucket);
        this.remoteLogSegmentId = checkNotNull(remoteLogSegmentId);

        if (remoteLogStartOffset < 0) {
            throw new IllegalArgumentException(
                    "Unexpected start offset: "
                            + remoteLogStartOffset
                            + ". StartOffset for a tiered segment cannot be negative");
        }
        this.remoteLogStartOffset = remoteLogStartOffset;

        if (remoteLogEndOffset <= remoteLogStartOffset) {
            throw new IllegalArgumentException(
                    "Unexpected remote log end offset: "
                            + remoteLogEndOffset
                            + ". The exclusive end offset for a remote segment must be greater "
                            + "than its start offset: "
                            + remoteLogStartOffset);
        }
        this.remoteLogEndOffset = remoteLogEndOffset;
        this.logicalStartOffset =
                logicalStartOffset == null ? remoteLogStartOffset : logicalStartOffset;
        this.logicalEndOffset = logicalEndOffset == null ? remoteLogEndOffset : logicalEndOffset;
        if (this.logicalStartOffset < remoteLogStartOffset
                || this.logicalStartOffset >= this.logicalEndOffset
                || this.logicalEndOffset > remoteLogEndOffset) {
            throw new IllegalArgumentException(
                    String.format(
                            "Logical range [%s, %s) must be a non-empty subset of physical range "
                                    + "[%s, %s)",
                            this.logicalStartOffset,
                            this.logicalEndOffset,
                            remoteLogStartOffset,
                            remoteLogEndOffset));
        }
        this.maxTimestamp = maxTimestamp;
        this.segmentSizeInBytes = segmentSizeInBytes;
    }

    public PhysicalTablePath physicalTablePath() {
        return physicalTablePath;
    }

    public TableBucket tableBucket() {
        return tableBucket;
    }

    public UUID remoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    /**
     * @return physical start offset of this segment (inclusive)
     */
    public long remoteLogStartOffset() {
        return remoteLogStartOffset;
    }

    /**
     * @return physical end offset of this segment (exclusive)
     */
    public long remoteLogEndOffset() {
        return remoteLogEndOffset;
    }

    /** Returns the inclusive logical start offset exposed by the current manifest. */
    public long logicalStartOffset() {
        return logicalStartOffset;
    }

    /** Returns the exclusive logical end offset exposed by the current manifest. */
    public long logicalEndOffset() {
        return logicalEndOffset;
    }

    /** Returns whether the logical view hides a suffix of this physical segment. */
    public boolean isEndOffsetClipped() {
        return logicalEndOffset < remoteLogEndOffset;
    }

    /** Returns metadata for the same physical object with a different logical range. */
    public RemoteLogSegment withLogicalRange(long logicalStartOffset, long logicalEndOffset) {
        return new RemoteLogSegment(
                physicalTablePath,
                tableBucket,
                remoteLogSegmentId,
                remoteLogStartOffset,
                remoteLogEndOffset,
                logicalStartOffset,
                logicalEndOffset,
                maxTimestamp,
                segmentSizeInBytes);
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public int segmentSizeInBytes() {
        return segmentSizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogSegment that = (RemoteLogSegment) o;
        return remoteLogStartOffset == that.remoteLogStartOffset
                && remoteLogEndOffset == that.remoteLogEndOffset
                && logicalStartOffset == that.logicalStartOffset
                && logicalEndOffset == that.logicalEndOffset
                && segmentSizeInBytes == that.segmentSizeInBytes
                && maxTimestamp == that.maxTimestamp
                && Objects.equals(remoteLogSegmentId, that.remoteLogSegmentId)
                && Objects.equals(physicalTablePath, that.physicalTablePath)
                && Objects.equals(tableBucket, that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalTablePath,
                tableBucket,
                remoteLogSegmentId,
                remoteLogStartOffset,
                remoteLogEndOffset,
                logicalStartOffset,
                logicalEndOffset,
                maxTimestamp,
                segmentSizeInBytes);
    }

    @Override
    public String toString() {
        return "RemoteLogSegment{"
                + "physicalTablePath="
                + physicalTablePath
                + ", table-bucket="
                + tableBucket
                + ", remoteLogSegmentId="
                + remoteLogSegmentId
                + ", remoteLogStartOffset="
                + remoteLogStartOffset
                + ", remoteLogEndOffset="
                + remoteLogEndOffset
                + ", logicalStartOffset="
                + logicalStartOffset
                + ", logicalEndOffset="
                + logicalEndOffset
                + ", maxTimestamp="
                + maxTimestamp
                + ", segmentSizeInBytes="
                + segmentSizeInBytes
                + '}';
    }

    /** Builder for {@link RemoteLogSegment}. */
    public static class Builder {
        private PhysicalTablePath physicalTablePath;
        private TableBucket tableBucket;
        private UUID remoteLogSegmentId;
        private long remoteLogStartOffset;
        private long remoteLogEndOffset;
        private @Nullable Long logicalStartOffset;
        private @Nullable Long logicalEndOffset;
        private long maxTimestamp;
        private int segmentSizeInBytes;

        public static Builder builder() {
            return new Builder();
        }

        public Builder remoteLogSegmentId(UUID remoteLogSegmentId) {
            this.remoteLogSegmentId = remoteLogSegmentId;
            return this;
        }

        public Builder remoteLogStartOffset(long startOffset) {
            this.remoteLogStartOffset = startOffset;
            return this;
        }

        public Builder remoteLogEndOffset(long endOffset) {
            this.remoteLogEndOffset = endOffset;
            return this;
        }

        public Builder logicalStartOffset(long logicalStartOffset) {
            this.logicalStartOffset = logicalStartOffset;
            return this;
        }

        public Builder logicalEndOffset(long logicalEndOffset) {
            this.logicalEndOffset = logicalEndOffset;
            return this;
        }

        public Builder maxTimestamp(long maxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            return this;
        }

        public Builder segmentSizeInBytes(int segmentSizeInBytes) {
            this.segmentSizeInBytes = segmentSizeInBytes;
            return this;
        }

        public Builder physicalTablePath(PhysicalTablePath physicalTablePath) {
            this.physicalTablePath = physicalTablePath;
            return this;
        }

        public Builder tableBucket(TableBucket tableBucket) {
            this.tableBucket = tableBucket;
            return this;
        }

        public RemoteLogSegment build() {
            return new RemoteLogSegment(
                    physicalTablePath,
                    tableBucket,
                    remoteLogSegmentId,
                    remoteLogStartOffset,
                    remoteLogEndOffset,
                    logicalStartOffset,
                    logicalEndOffset,
                    maxTimestamp,
                    segmentSizeInBytes);
        }
    }
}

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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** The data for request {@link CommitRemoteLogManifestRequest}. */
public class CommitRemoteLogManifestData {

    /** The table bucket that this snapshot belongs to. */
    private final TableBucket tableBucket;

    /** The location where the remote log manifest is stored in remote storage. */
    private final FsPath remoteLogManifestPath;

    /** The start offset of the remote log. */
    private final long remoteLogStartOffset;

    /** The end offset of the remote log. */
    private final long remoteLogEndOffset;

    /** The highest exclusive end offset successfully copied to remote storage. */
    private final long highestCopiedEndOffset;

    /** The coordinator epoch when the snapshot is triggered. */
    private final int coordinatorEpoch;

    /** The leader epoch of the bucket when the snapshot is triggered. */
    private final int bucketLeaderEpoch;

    private final @Nullable Integer manifestFormatVersion;
    private final @Nullable Integer expectedZkVersion;
    private final @Nullable Long newManifestGeneration;

    public CommitRemoteLogManifestData(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            int coordinatorEpoch,
            int bucketLeaderEpoch) {
        this(
                tableBucket,
                remoteLogManifestPath,
                remoteLogStartOffset,
                remoteLogEndOffset,
                remoteLogEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                null,
                null,
                null);
    }

    private CommitRemoteLogManifestData(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            long highestCopiedEndOffset,
            int coordinatorEpoch,
            int bucketLeaderEpoch,
            @Nullable Integer manifestFormatVersion,
            @Nullable Integer expectedZkVersion,
            @Nullable Long newManifestGeneration) {
        if (manifestFormatVersion != null) {
            checkArgument(newManifestGeneration > 0L, "New manifest generation must be positive");
            checkArgument(
                    isEmptyRemoteLogRange(remoteLogStartOffset, remoteLogEndOffset)
                            || remoteLogStartOffset < remoteLogEndOffset,
                    "V2 remote log offsets must form a half-open range or the empty range");
            checkArgument(
                    highestCopiedEndOffset >= remoteLogEndOffset,
                    "Highest copied end offset must not be before the logical remote end offset");
            if (expectedZkVersion == null) {
                checkArgument(
                        newManifestGeneration == 1L, "Initial Manifest V2 generation must be 1");
            } else {
                checkArgument(
                        expectedZkVersion >= 0,
                        "Expected ZK version must not use the wildcard value: %s",
                        expectedZkVersion);
            }
        }
        this.tableBucket = tableBucket;
        this.remoteLogManifestPath = remoteLogManifestPath;
        this.remoteLogStartOffset = remoteLogStartOffset;
        this.remoteLogEndOffset = remoteLogEndOffset;
        this.highestCopiedEndOffset = highestCopiedEndOffset;
        this.coordinatorEpoch = coordinatorEpoch;
        this.bucketLeaderEpoch = bucketLeaderEpoch;
        this.manifestFormatVersion = manifestFormatVersion;
        this.expectedZkVersion = expectedZkVersion;
        this.newManifestGeneration = newManifestGeneration;
    }

    public static CommitRemoteLogManifestData v2CreateIfAbsent(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            long highestCopiedEndOffset,
            long newManifestGeneration,
            int coordinatorEpoch,
            int bucketLeaderEpoch) {
        return new CommitRemoteLogManifestData(
                tableBucket,
                remoteLogManifestPath,
                remoteLogStartOffset,
                remoteLogEndOffset,
                highestCopiedEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                RemoteLogManifest.VERSION_2,
                null,
                newManifestGeneration);
    }

    public static CommitRemoteLogManifestData v2CompareAndSet(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            long highestCopiedEndOffset,
            long newManifestGeneration,
            int expectedZkVersion,
            int coordinatorEpoch,
            int bucketLeaderEpoch) {
        return new CommitRemoteLogManifestData(
                tableBucket,
                remoteLogManifestPath,
                remoteLogStartOffset,
                remoteLogEndOffset,
                highestCopiedEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                RemoteLogManifest.VERSION_2,
                expectedZkVersion,
                newManifestGeneration);
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public FsPath getRemoteLogManifestPath() {
        return remoteLogManifestPath;
    }

    public long getRemoteLogStartOffset() {
        return remoteLogStartOffset;
    }

    public long getRemoteLogEndOffset() {
        return remoteLogEndOffset;
    }

    public long getHighestCopiedEndOffset() {
        return highestCopiedEndOffset;
    }

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public int getBucketLeaderEpoch() {
        return bucketLeaderEpoch;
    }

    public boolean isV2CasCommit() {
        return manifestFormatVersion != null;
    }

    /** Returns whether this commit publishes a Manifest V2 with no active remote range. */
    public boolean isEmptyV2Manifest() {
        return isV2CasCommit() && isEmptyRemoteLogRange(remoteLogStartOffset, remoteLogEndOffset);
    }

    public @Nullable Integer getManifestFormatVersion() {
        return manifestFormatVersion;
    }

    public @Nullable Integer getExpectedZkVersion() {
        return expectedZkVersion;
    }

    public @Nullable Long getNewManifestGeneration() {
        return newManifestGeneration;
    }

    @Override
    public String toString() {
        return "CommitRemoteLogManifestData{"
                + "tableBucket="
                + tableBucket
                + ", metadataSnapshotPath="
                + remoteLogManifestPath
                + ", remoteLogStartOffset="
                + remoteLogStartOffset
                + ", remoteLogEndOffset="
                + remoteLogEndOffset
                + ", highestCopiedEndOffset="
                + highestCopiedEndOffset
                + ", coordinatorEpoch="
                + coordinatorEpoch
                + ", bucketLeaderEpoch="
                + bucketLeaderEpoch
                + ", manifestFormatVersion="
                + manifestFormatVersion
                + ", expectedZkVersion="
                + expectedZkVersion
                + ", newManifestGeneration="
                + newManifestGeneration
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommitRemoteLogManifestData that = (CommitRemoteLogManifestData) o;
        return Objects.equals(tableBucket, that.tableBucket)
                && Objects.equals(remoteLogManifestPath, that.remoteLogManifestPath)
                && remoteLogStartOffset == that.remoteLogStartOffset
                && remoteLogEndOffset == that.remoteLogEndOffset
                && highestCopiedEndOffset == that.highestCopiedEndOffset
                && coordinatorEpoch == that.coordinatorEpoch
                && bucketLeaderEpoch == that.bucketLeaderEpoch
                && Objects.equals(manifestFormatVersion, that.manifestFormatVersion)
                && Objects.equals(expectedZkVersion, that.expectedZkVersion)
                && Objects.equals(newManifestGeneration, that.newManifestGeneration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableBucket,
                remoteLogManifestPath,
                remoteLogStartOffset,
                remoteLogEndOffset,
                highestCopiedEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                manifestFormatVersion,
                expectedZkVersion,
                newManifestGeneration);
    }

    private static boolean isEmptyRemoteLogRange(long startOffset, long endOffset) {
        return startOffset == Long.MAX_VALUE && endOffset == -1L;
    }
}

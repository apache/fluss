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

    /** The coordinator epoch when the snapshot is triggered. */
    private final int coordinatorEpoch;

    /** The leader epoch of the bucket when the snapshot is triggered. */
    private final int bucketLeaderEpoch;

    private final @Nullable Integer manifestFormatVersion;
    private final @Nullable RemoteLogManifestExpectedHandleState expectedHandleState;
    private final @Nullable FsPath expectedManifestPath;
    private final @Nullable Long expectedManifestGeneration;
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
                coordinatorEpoch,
                bucketLeaderEpoch,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private CommitRemoteLogManifestData(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            int coordinatorEpoch,
            int bucketLeaderEpoch,
            @Nullable Integer manifestFormatVersion,
            @Nullable RemoteLogManifestExpectedHandleState expectedHandleState,
            @Nullable FsPath expectedManifestPath,
            @Nullable Long expectedManifestGeneration,
            @Nullable Integer expectedZkVersion,
            @Nullable Long newManifestGeneration) {
        if (manifestFormatVersion == null) {
            checkArgument(
                    expectedHandleState == null
                            && expectedManifestPath == null
                            && expectedManifestGeneration == null
                            && expectedZkVersion == null
                            && newManifestGeneration == null,
                    "Legacy manifest commit must not contain V2 CAS fields");
        } else {
            checkArgument(manifestFormatVersion == 2, "Only Manifest V2 supports CAS publish");
            checkArgument(expectedHandleState != null, "Expected handle state is required");
            checkArgument(
                    newManifestGeneration != null && newManifestGeneration > 0L,
                    "New manifest generation must be positive");
            checkArgument(
                    remoteLogStartOffset < remoteLogEndOffset,
                    "V2 remote log offsets must form a non-empty half-open range");
            if (expectedHandleState == RemoteLogManifestExpectedHandleState.ABSENT) {
                checkArgument(
                        expectedManifestPath == null
                                && expectedManifestGeneration == null
                                && expectedZkVersion == null,
                        "ABSENT commit must not contain expected handle fields");
                checkArgument(
                        newManifestGeneration == 1L, "Initial Manifest V2 generation must be 1");
            } else {
                checkArgument(
                        expectedManifestPath != null
                                && expectedManifestGeneration != null
                                && expectedZkVersion != null,
                        "PRESENT commit requires expected path, generation, and ZK version");
                checkArgument(
                        newManifestGeneration == expectedManifestGeneration + 1L,
                        "New generation must equal expected generation + 1");
            }
        }
        this.tableBucket = tableBucket;
        this.remoteLogManifestPath = remoteLogManifestPath;
        this.remoteLogStartOffset = remoteLogStartOffset;
        this.remoteLogEndOffset = remoteLogEndOffset;
        this.coordinatorEpoch = coordinatorEpoch;
        this.bucketLeaderEpoch = bucketLeaderEpoch;
        this.manifestFormatVersion = manifestFormatVersion;
        this.expectedHandleState = expectedHandleState;
        this.expectedManifestPath = expectedManifestPath;
        this.expectedManifestGeneration = expectedManifestGeneration;
        this.expectedZkVersion = expectedZkVersion;
        this.newManifestGeneration = newManifestGeneration;
    }

    public static CommitRemoteLogManifestData v2Absent(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            long newManifestGeneration,
            int coordinatorEpoch,
            int bucketLeaderEpoch) {
        return new CommitRemoteLogManifestData(
                tableBucket,
                remoteLogManifestPath,
                remoteLogStartOffset,
                remoteLogEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                2,
                RemoteLogManifestExpectedHandleState.ABSENT,
                null,
                null,
                null,
                newManifestGeneration);
    }

    public static CommitRemoteLogManifestData v2Present(
            TableBucket tableBucket,
            FsPath remoteLogManifestPath,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            long newManifestGeneration,
            FsPath expectedManifestPath,
            long expectedManifestGeneration,
            int expectedZkVersion,
            int coordinatorEpoch,
            int bucketLeaderEpoch) {
        return new CommitRemoteLogManifestData(
                tableBucket,
                remoteLogManifestPath,
                remoteLogStartOffset,
                remoteLogEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                2,
                RemoteLogManifestExpectedHandleState.PRESENT,
                expectedManifestPath,
                expectedManifestGeneration,
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

    public int getCoordinatorEpoch() {
        return coordinatorEpoch;
    }

    public int getBucketLeaderEpoch() {
        return bucketLeaderEpoch;
    }

    public boolean isV2CasCommit() {
        return manifestFormatVersion != null;
    }

    public @Nullable Integer getManifestFormatVersion() {
        return manifestFormatVersion;
    }

    public @Nullable RemoteLogManifestExpectedHandleState getExpectedHandleState() {
        return expectedHandleState;
    }

    public @Nullable FsPath getExpectedManifestPath() {
        return expectedManifestPath;
    }

    public @Nullable Long getExpectedManifestGeneration() {
        return expectedManifestGeneration;
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
                + ", remoteLogEndOffset="
                + remoteLogEndOffset
                + ", coordinatorEpoch="
                + coordinatorEpoch
                + ", bucketLeaderEpoch="
                + bucketLeaderEpoch
                + ", manifestFormatVersion="
                + manifestFormatVersion
                + ", expectedHandleState="
                + expectedHandleState
                + ", expectedManifestPath="
                + expectedManifestPath
                + ", expectedManifestGeneration="
                + expectedManifestGeneration
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
                && remoteLogEndOffset == that.remoteLogEndOffset
                && coordinatorEpoch == that.coordinatorEpoch
                && bucketLeaderEpoch == that.bucketLeaderEpoch
                && Objects.equals(manifestFormatVersion, that.manifestFormatVersion)
                && expectedHandleState == that.expectedHandleState
                && Objects.equals(expectedManifestPath, that.expectedManifestPath)
                && Objects.equals(expectedManifestGeneration, that.expectedManifestGeneration)
                && Objects.equals(expectedZkVersion, that.expectedZkVersion)
                && Objects.equals(newManifestGeneration, that.newManifestGeneration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tableBucket,
                remoteLogManifestPath,
                remoteLogEndOffset,
                coordinatorEpoch,
                bucketLeaderEpoch,
                manifestFormatVersion,
                expectedHandleState,
                expectedManifestPath,
                expectedManifestGeneration,
                expectedZkVersion,
                newManifestGeneration);
    }
}

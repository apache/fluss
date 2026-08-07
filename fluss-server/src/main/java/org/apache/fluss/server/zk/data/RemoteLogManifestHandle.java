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

package org.apache.fluss.server.zk.data;

import org.apache.fluss.fs.FsPath;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * The remote log manifest handle of a table bucket stored in {@link ZkData.BucketRemoteLogsZNode}.
 *
 * @see RemoteLogManifestHandleJsonSerde for json serialization and deserialization.
 */
public class RemoteLogManifestHandle {
    public static final int VERSION_1 = 1;
    public static final int VERSION_2 = 2;

    private final int version;
    private final FsPath remoteLogManifestPath;
    private final @Nullable Long manifestGeneration;
    private final @Nullable Long remoteLogStartOffset;
    private final long remoteLogEndOffset;
    private final long highestCopiedEndOffset;

    public RemoteLogManifestHandle(FsPath remoteLogManifestPath, long remoteLogEndOffset) {
        this(VERSION_1, remoteLogManifestPath, null, null, remoteLogEndOffset, remoteLogEndOffset);
    }

    private RemoteLogManifestHandle(
            int version,
            FsPath remoteLogManifestPath,
            @Nullable Long manifestGeneration,
            @Nullable Long remoteLogStartOffset,
            long remoteLogEndOffset,
            long highestCopiedEndOffset) {
        if (version == VERSION_2) {
            checkArgument(
                    manifestGeneration != null && manifestGeneration > 0L,
                    "V2 manifest generation must be positive");
            if (remoteLogStartOffset == null) {
                checkArgument(
                        remoteLogEndOffset == -1L,
                        "Empty V2 manifest handle must use remote log end offset -1");
            } else {
                checkArgument(
                        remoteLogStartOffset < remoteLogEndOffset,
                        "V2 remote log offsets must form a non-empty half-open range");
            }
            checkArgument(
                    highestCopiedEndOffset >= remoteLogEndOffset,
                    "Highest copied end offset must not be before the logical remote end offset");
        }
        this.version = version;
        this.remoteLogManifestPath = remoteLogManifestPath;
        this.manifestGeneration = manifestGeneration;
        this.remoteLogStartOffset = remoteLogStartOffset;
        this.remoteLogEndOffset = remoteLogEndOffset;
        this.highestCopiedEndOffset = highestCopiedEndOffset;
    }

    public static RemoteLogManifestHandle v2(
            FsPath remoteLogManifestPath,
            long manifestGeneration,
            long remoteLogStartOffset,
            long remoteLogEndOffset) {
        return v2(
                remoteLogManifestPath,
                manifestGeneration,
                remoteLogStartOffset,
                remoteLogEndOffset,
                remoteLogEndOffset);
    }

    public static RemoteLogManifestHandle v2(
            FsPath remoteLogManifestPath,
            long manifestGeneration,
            long remoteLogStartOffset,
            long remoteLogEndOffset,
            long highestCopiedEndOffset) {
        if (remoteLogStartOffset == Long.MAX_VALUE && remoteLogEndOffset == -1L) {
            return v2Empty(remoteLogManifestPath, manifestGeneration, highestCopiedEndOffset);
        }
        return new RemoteLogManifestHandle(
                VERSION_2,
                remoteLogManifestPath,
                manifestGeneration,
                remoteLogStartOffset,
                remoteLogEndOffset,
                highestCopiedEndOffset);
    }

    /** Creates a Manifest V2 handle whose authoritative snapshot has no active remote range. */
    public static RemoteLogManifestHandle v2Empty(
            FsPath remoteLogManifestPath, long manifestGeneration) {
        return v2Empty(remoteLogManifestPath, manifestGeneration, -1L);
    }

    /** Creates an empty Manifest V2 handle while retaining its highest copied end offset. */
    public static RemoteLogManifestHandle v2Empty(
            FsPath remoteLogManifestPath, long manifestGeneration, long highestCopiedEndOffset) {
        return new RemoteLogManifestHandle(
                VERSION_2,
                remoteLogManifestPath,
                manifestGeneration,
                null,
                -1L,
                highestCopiedEndOffset);
    }

    public static FsPath fromRemoteLogManifestPath(String remoteLogManifestPath) {
        return new FsPath(remoteLogManifestPath);
    }

    public FsPath getRemoteLogManifestPath() {
        return remoteLogManifestPath;
    }

    public int getVersion() {
        return version;
    }

    public OptionalLong getManifestGeneration() {
        return manifestGeneration == null
                ? OptionalLong.empty()
                : OptionalLong.of(manifestGeneration);
    }

    public OptionalLong getRemoteLogStartOffset() {
        return remoteLogStartOffset == null
                ? OptionalLong.empty()
                : OptionalLong.of(remoteLogStartOffset);
    }

    public long getRemoteLogEndOffset() {
        return remoteLogEndOffset;
    }

    public long getHighestCopiedEndOffset() {
        return highestCopiedEndOffset;
    }

    /** Returns whether this is a Manifest V2 handle with no active remote range. */
    public boolean isEmptyV2() {
        return version == VERSION_2 && remoteLogStartOffset == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogManifestHandle that = (RemoteLogManifestHandle) o;
        return version == that.version
                && remoteLogManifestPath.equals(that.remoteLogManifestPath)
                && Objects.equals(manifestGeneration, that.manifestGeneration)
                && Objects.equals(remoteLogStartOffset, that.remoteLogStartOffset)
                && remoteLogEndOffset == that.remoteLogEndOffset
                && highestCopiedEndOffset == that.highestCopiedEndOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                version,
                remoteLogManifestPath,
                manifestGeneration,
                remoteLogStartOffset,
                remoteLogEndOffset,
                highestCopiedEndOffset);
    }

    @Override
    public String toString() {
        return "RemoteLogManifestHandle{"
                + "remoteLogManifestPath="
                + remoteLogManifestPath
                + ", version="
                + version
                + ", manifestGeneration="
                + manifestGeneration
                + ", remoteLogStartOffset="
                + remoteLogStartOffset
                + ", remoteLogEndOffset="
                + remoteLogEndOffset
                + ", highestCopiedEndOffset="
                + highestCopiedEndOffset
                + '}';
    }
}

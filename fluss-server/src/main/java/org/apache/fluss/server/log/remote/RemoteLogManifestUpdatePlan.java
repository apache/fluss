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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogManifestReplacementPlanner.PlanType;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.zk.data.VersionedRemoteLogManifestHandle;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Immutable overlap-aware Manifest V2 update and its authoritative CAS base. */
@Internal
public final class RemoteLogManifestUpdatePlan {
    private final @Nullable VersionedRemoteLogManifestHandle baseHandle;
    private final RemoteLogManifest resultManifest;
    private final List<RemoteLogSegment> segmentsToCopy;
    private final List<RemoteLogSegment> segmentsToUnreference;
    private final List<PlanType> planTypes;
    private final long nextCopyOffset;

    public RemoteLogManifestUpdatePlan(
            @Nullable VersionedRemoteLogManifestHandle baseHandle,
            RemoteLogManifest resultManifest,
            List<RemoteLogSegment> segmentsToCopy,
            List<RemoteLogSegment> segmentsToUnreference,
            List<PlanType> planTypes,
            long nextCopyOffset) {
        this.resultManifest = checkNotNull(resultManifest);
        checkArgument(
                resultManifest.getVersion() == RemoteLogManifest.VERSION_2,
                "Update plan requires Manifest V2");
        long expectedGeneration =
                baseHandle == null
                        ? 1L
                        : baseHandle.handle().getManifestGeneration().orElse(0L) + 1L;
        checkArgument(
                resultManifest.getGeneration() == expectedGeneration,
                "Result generation %s does not follow authoritative base generation %s",
                resultManifest.getGeneration(),
                expectedGeneration - 1L);
        this.baseHandle = baseHandle;
        this.segmentsToCopy = immutableCopy(segmentsToCopy);
        this.segmentsToUnreference = immutableCopy(segmentsToUnreference);
        this.planTypes = Collections.unmodifiableList(new ArrayList<>(checkNotNull(planTypes)));
        this.nextCopyOffset = nextCopyOffset;
    }

    public @Nullable VersionedRemoteLogManifestHandle baseHandle() {
        return baseHandle;
    }

    public RemoteLogManifest resultManifest() {
        return resultManifest;
    }

    public List<RemoteLogSegment> segmentsToCopy() {
        return segmentsToCopy;
    }

    public List<RemoteLogSegment> segmentsToUnreference() {
        return segmentsToUnreference;
    }

    public List<PlanType> planTypes() {
        return planTypes;
    }

    public long nextCopyOffset() {
        return nextCopyOffset;
    }

    /** Builds the commit request after the immutable result snapshot has been written. */
    public CommitRemoteLogManifestData toCommitData(
            FsPath manifestPath, int coordinatorEpoch, int bucketLeaderEpoch) {
        if (baseHandle == null) {
            return CommitRemoteLogManifestData.v2Absent(
                    resultManifest.getTableBucket(),
                    manifestPath,
                    resultManifest.getRemoteLogStartOffset(),
                    resultManifest.getRemoteLogEndOffset(),
                    resultManifest.getGeneration(),
                    coordinatorEpoch,
                    bucketLeaderEpoch);
        }
        return CommitRemoteLogManifestData.v2Present(
                resultManifest.getTableBucket(),
                manifestPath,
                resultManifest.getRemoteLogStartOffset(),
                resultManifest.getRemoteLogEndOffset(),
                resultManifest.getGeneration(),
                baseHandle.handle().getRemoteLogManifestPath(),
                baseHandle.handle().getManifestGeneration().orElse(0L),
                baseHandle.zkVersion(),
                coordinatorEpoch,
                bucketLeaderEpoch);
    }

    private static List<RemoteLogSegment> immutableCopy(List<RemoteLogSegment> segments) {
        return Collections.unmodifiableList(new ArrayList<>(checkNotNull(segments)));
    }
}

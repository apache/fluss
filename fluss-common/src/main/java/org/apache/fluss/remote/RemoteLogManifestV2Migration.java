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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static org.apache.fluss.remote.UnreferencedRemoteLogSegment.GC_INELIGIBLE_TIMESTAMP;

/** Converts an authoritative V1 manifest into a canonical V2 manifest. */
@Internal
public final class RemoteLogManifestV2Migration {

    /** A canonical V2 base together with whether it requires rebuilding from a local segment. */
    public static final class Result {
        private final boolean requiresRebuild;
        private final RemoteLogManifest resultManifest;
        private final long gapStartOffset;
        private final long gapEndOffset;

        private Result(
                boolean requiresRebuild,
                RemoteLogManifest resultManifest,
                long gapStartOffset,
                long gapEndOffset) {
            this.requiresRebuild = requiresRebuild;
            this.resultManifest = resultManifest;
            this.gapStartOffset = gapStartOffset;
            this.gapEndOffset = gapEndOffset;
        }

        public RemoteLogManifest resultManifest() {
            return resultManifest;
        }

        public long gapStartOffset() {
            return gapStartOffset;
        }

        public long gapEndOffset() {
            return gapEndOffset;
        }

        public boolean requiresRebuild() {
            return requiresRebuild;
        }
    }

    public static Result migrate(RemoteLogManifest v1Manifest, long newGeneration) {
        List<RemoteLogSegment> sortedSegments =
                new ArrayList<>(v1Manifest.getRemoteLogSegmentList());
        sortedSegments.sort(
                Comparator.comparingLong(RemoteLogSegment::remoteLogStartOffset)
                        .thenComparingLong(RemoteLogSegment::remoteLogEndOffset));

        List<RemoteLogSegment> sameStartWinners = new ArrayList<>();
        List<UnreferencedRemoteLogSegment> unreferencedSegments = new ArrayList<>();
        int index = 0;
        while (index < sortedSegments.size()) {
            int groupEnd = index + 1;
            long startOffset = sortedSegments.get(index).remoteLogStartOffset();
            while (groupEnd < sortedSegments.size()
                    && sortedSegments.get(groupEnd).remoteLogStartOffset() == startOffset) {
                groupEnd++;
            }

            // The sort is stable, so equal ranges retain V1 manifest order and the later entry
            // wins, matching the V1 reader's tie-breaker.
            RemoteLogSegment winner = sortedSegments.get(groupEnd - 1);
            sameStartWinners.add(winner);
            for (int i = index; i < groupEnd - 1; i++) {
                unreferencedSegments.add(
                        replaced(sortedSegments.get(i), winner.remoteLogSegmentId()));
            }
            index = groupEnd;
        }

        List<RemoteLogSegment> activeSegments = new ArrayList<>();
        for (RemoteLogSegment segment : sameStartWinners) {
            if (activeSegments.isEmpty()) {
                activeSegments.add(segment);
                continue;
            }

            RemoteLogSegment previous = activeSegments.get(activeSegments.size() - 1);
            if (segment.remoteLogStartOffset() > previous.remoteLogEndOffset()) {
                return gappedResult(
                        v1Manifest,
                        sortedSegments,
                        newGeneration,
                        previous.remoteLogEndOffset(),
                        segment.remoteLogStartOffset());
            }
            if (segment.remoteLogEndOffset() <= previous.remoteLogEndOffset()) {
                unreferencedSegments.add(replaced(segment, previous.remoteLogSegmentId()));
            } else {
                activeSegments.add(segment);
            }
        }

        Long remoteStartOffset =
                activeSegments.isEmpty() ? null : activeSegments.get(0).remoteLogStartOffset();
        return new Result(
                false,
                RemoteLogManifest.createV2(
                        newGeneration,
                        v1Manifest.getPhysicalTablePath(),
                        v1Manifest.getTableBucket(),
                        activeSegments,
                        remoteStartOffset,
                        v1Manifest.getRemoteLogEndOffset(),
                        unreferencedSegments),
                -1L,
                -1L);
    }

    private static Result gappedResult(
            RemoteLogManifest v1Manifest,
            List<RemoteLogSegment> sortedSegments,
            long newGeneration,
            long gapStartOffset,
            long gapEndOffset) {
        List<UnreferencedRemoteLogSegment> unreferencedSegments = new ArrayList<>();
        for (RemoteLogSegment segment : sortedSegments) {
            unreferencedSegments.add(
                    new UnreferencedRemoteLogSegment(
                            segment,
                            GC_INELIGIBLE_TIMESTAMP,
                            UnreferencedRemoteLogSegment.Reason.REPLACED,
                            null));
        }
        RemoteLogManifest recoveryBase =
                RemoteLogManifest.createV2(
                        newGeneration,
                        v1Manifest.getPhysicalTablePath(),
                        v1Manifest.getTableBucket(),
                        new ArrayList<>(),
                        null,
                        v1Manifest.getRemoteLogEndOffset(),
                        unreferencedSegments);
        return new Result(true, recoveryBase, gapStartOffset, gapEndOffset);
    }

    private static UnreferencedRemoteLogSegment replaced(
            RemoteLogSegment segment, UUID replacementSegmentId) {
        return new UnreferencedRemoteLogSegment(
                segment,
                UnreferencedRemoteLogSegment.GC_INELIGIBLE_TIMESTAMP,
                UnreferencedRemoteLogSegment.Reason.REPLACED,
                replacementSegmentId);
    }

    private RemoteLogManifestV2Migration() {}
}

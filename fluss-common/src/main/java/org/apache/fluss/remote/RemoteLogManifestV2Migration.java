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

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Converts an authoritative V1 manifest into a canonical V2 manifest. */
@Internal
public final class RemoteLogManifestV2Migration {

    public static RemoteLogManifest migrate(
            RemoteLogManifest v1Manifest, long newGeneration, long unreferencedAtMs) {
        checkArgument(
                v1Manifest.getVersion() == RemoteLogManifest.VERSION_1,
                "Only V1 manifests can be migrated");

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

            RemoteLogSegment winner = sortedSegments.get(groupEnd - 1);
            if (groupEnd - index > 1
                    && sortedSegments.get(groupEnd - 2).remoteLogEndOffset()
                            == winner.remoteLogEndOffset()) {
                throw new IllegalArgumentException(
                        "Cannot deterministically migrate V1 segments with identical start and end "
                                + "offsets at "
                                + startOffset);
            }
            sameStartWinners.add(winner);
            for (int i = index; i < groupEnd - 1; i++) {
                unreferencedSegments.add(
                        replaced(
                                sortedSegments.get(i),
                                winner.remoteLogSegmentId(),
                                unreferencedAtMs));
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
                throw new IllegalArgumentException(
                        "Cannot migrate V1 manifest with gap ["
                                + previous.remoteLogEndOffset()
                                + ", "
                                + segment.remoteLogStartOffset()
                                + ")");
            }
            if (segment.remoteLogEndOffset() <= previous.remoteLogEndOffset()) {
                unreferencedSegments.add(
                        replaced(segment, previous.remoteLogSegmentId(), unreferencedAtMs));
            } else {
                activeSegments.add(segment);
            }
        }

        Long remoteStartOffset =
                activeSegments.isEmpty() ? null : activeSegments.get(0).remoteLogStartOffset();
        return RemoteLogManifest.createV2(
                newGeneration,
                v1Manifest.getPhysicalTablePath(),
                v1Manifest.getTableBucket(),
                activeSegments,
                remoteStartOffset,
                unreferencedSegments);
    }

    private static UnreferencedRemoteLogSegment replaced(
            RemoteLogSegment segment, UUID replacementSegmentId, long unreferencedAtMs) {
        return new UnreferencedRemoteLogSegment(
                segment,
                unreferencedAtMs,
                UnreferencedRemoteLogSegment.Reason.REPLACED,
                replacementSegmentId);
    }

    private RemoteLogManifestV2Migration() {}
}

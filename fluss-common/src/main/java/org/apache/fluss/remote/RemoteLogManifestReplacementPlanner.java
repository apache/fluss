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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Builds an immutable V2 manifest update for one completely copied candidate segment.
 *
 * <p>Replacement correctness requires every candidate start offset used as a logical boundary to be
 * a record-batch boundary in every overlapping segment. Clean ISR replication preserves the encoded
 * record batches, and local log rolling only creates segment boundaries between complete batches.
 * The planner validates offset ranges but does not read remote log objects to revalidate their
 * batch layout.
 */
@Internal
public final class RemoteLogManifestReplacementPlanner {

    /** Classification of a candidate physical segment against the current logical remote range. */
    public enum PlanType {
        INITIAL_COPY,
        ALREADY_COVERED,
        GAP,
        APPEND,
        REPLACE_AND_CLIP,
        REPLACE_AND_CLIP_START
    }

    /** The result of classifying and, when needed, applying one candidate segment. */
    public static final class Result {
        private final PlanType planType;
        private final RemoteLogManifest resultManifest;
        private final List<RemoteLogSegment> segmentsToUnreference;

        private Result(
                PlanType planType,
                RemoteLogManifest resultManifest,
                List<RemoteLogSegment> segmentsToUnreference) {
            this.planType = checkNotNull(planType);
            this.resultManifest = checkNotNull(resultManifest);
            this.segmentsToUnreference =
                    Collections.unmodifiableList(new ArrayList<>(segmentsToUnreference));
        }

        public PlanType planType() {
            return planType;
        }

        public RemoteLogManifest resultManifest() {
            return resultManifest;
        }

        public List<RemoteLogSegment> segmentsToUnreference() {
            return segmentsToUnreference;
        }

        public boolean requiresManifestCommit() {
            return planType != PlanType.ALREADY_COVERED && planType != PlanType.GAP;
        }
    }

    public static Result plan(
            RemoteLogManifest manifest, RemoteLogSegment candidate, long unreferencedAtMs) {
        checkArgument(
                manifest.getVersion() == RemoteLogManifest.VERSION_2,
                "Replacement planning requires a V2 manifest");
        checkArgument(
                candidate.physicalTablePath().equals(manifest.getPhysicalTablePath()),
                "Candidate physical table path does not match manifest");
        checkArgument(
                candidate.tableBucket().equals(manifest.getTableBucket()),
                "Candidate table bucket does not match manifest");
        ensureNewSegmentId(manifest, candidate.remoteLogSegmentId());

        if (manifest.getRemoteLogSegmentList().isEmpty()) {
            RemoteLogManifest resultManifest =
                    RemoteLogManifest.createV2(
                            manifest.getGeneration() + 1,
                            manifest.getPhysicalTablePath(),
                            manifest.getTableBucket(),
                            Collections.singletonList(candidate),
                            candidate.remoteLogStartOffset(),
                            manifest.getUnreferencedRemoteLogSegments());
            return new Result(PlanType.INITIAL_COPY, resultManifest, Collections.emptyList());
        }

        long remoteStartOffset = manifest.getRemoteLogStartOffset();
        long remoteEndOffset = manifest.getRemoteLogEndOffset();
        long candidateStartOffset = candidate.remoteLogStartOffset();
        long candidateEndOffset = candidate.remoteLogEndOffset();

        if (candidateEndOffset <= remoteEndOffset) {
            return new Result(PlanType.ALREADY_COVERED, manifest, Collections.emptyList());
        }
        if (candidateStartOffset > remoteEndOffset) {
            return new Result(PlanType.GAP, manifest, Collections.emptyList());
        }

        PlanType planType;
        if (candidateStartOffset == remoteEndOffset) {
            planType = PlanType.APPEND;
        } else if (candidateStartOffset < remoteStartOffset) {
            planType = PlanType.REPLACE_AND_CLIP_START;
        } else {
            planType = PlanType.REPLACE_AND_CLIP;
        }

        long insertionOffset = Math.max(candidateStartOffset, remoteStartOffset);
        List<RemoteLogSegment> activeSegments = new ArrayList<>();
        List<RemoteLogSegment> segmentsToUnreference = new ArrayList<>();
        for (RemoteLogSegmentReference reference : manifest.getRemoteLogSegmentReferences()) {
            if (reference.logicalStartOffset() < insertionOffset) {
                activeSegments.add(reference.remoteLogSegment());
            } else {
                segmentsToUnreference.add(reference.remoteLogSegment());
            }
        }

        if (insertionOffset > remoteStartOffset) {
            checkArgument(!activeSegments.isEmpty(), "Replacement is missing a prefix segment");
            RemoteLogSegment prefix = activeSegments.get(activeSegments.size() - 1);
            checkArgument(
                    prefix.remoteLogEndOffset() >= insertionOffset,
                    "Replacement creates a logical gap before offset %s",
                    insertionOffset);
        }
        activeSegments.add(candidate);

        List<UnreferencedRemoteLogSegment> unreferencedSegments =
                new ArrayList<>(manifest.getUnreferencedRemoteLogSegments());
        for (RemoteLogSegment segment : segmentsToUnreference) {
            unreferencedSegments.add(
                    new UnreferencedRemoteLogSegment(
                            segment,
                            unreferencedAtMs,
                            UnreferencedRemoteLogSegment.Reason.REPLACED,
                            candidate.remoteLogSegmentId()));
        }

        RemoteLogManifest resultManifest =
                RemoteLogManifest.createV2(
                        manifest.getGeneration() + 1,
                        manifest.getPhysicalTablePath(),
                        manifest.getTableBucket(),
                        activeSegments,
                        remoteStartOffset,
                        unreferencedSegments);
        return new Result(planType, resultManifest, segmentsToUnreference);
    }

    /**
     * Moves a continuous expired logical prefix to unreferenced metadata without deleting objects.
     *
     * <p>The last active reference is retained because the current V2 handle format represents only
     * non-empty ranges. This is conservative: expiration may be delayed, but readable data is never
     * removed early.
     */
    public static RemoteLogManifest expireContinuousPrefix(
            RemoteLogManifest manifest,
            long currentTimeMs,
            long ttlMs,
            Long lakeLogEndOffset,
            long unreferencedAtMs) {
        checkArgument(
                manifest.getVersion() == RemoteLogManifest.VERSION_2,
                "Expiration planning requires a V2 manifest");
        if (ttlMs <= 0L || manifest.getRemoteLogSegmentList().size() <= 1) {
            return manifest;
        }

        List<RemoteLogSegmentReference> references = manifest.getRemoteLogSegmentReferences();
        int expireCount = 0;
        while (expireCount < references.size() - 1) {
            RemoteLogSegment segment = references.get(expireCount).remoteLogSegment();
            boolean expired = currentTimeMs - segment.maxTimestamp() > ttlMs;
            boolean tieredToLake =
                    lakeLogEndOffset == null
                            || references.get(expireCount).logicalEndOffset() <= lakeLogEndOffset;
            if (!expired || !tieredToLake) {
                break;
            }
            expireCount++;
        }
        if (expireCount == 0) {
            return manifest;
        }

        List<RemoteLogSegment> activeSegments =
                new ArrayList<>(
                        manifest.getRemoteLogSegmentList().subList(expireCount, references.size()));
        List<UnreferencedRemoteLogSegment> unreferencedSegments =
                new ArrayList<>(manifest.getUnreferencedRemoteLogSegments());
        for (int index = 0; index < expireCount; index++) {
            unreferencedSegments.add(
                    new UnreferencedRemoteLogSegment(
                            references.get(index).remoteLogSegment(),
                            unreferencedAtMs,
                            UnreferencedRemoteLogSegment.Reason.EXPIRED,
                            null));
        }
        return RemoteLogManifest.createV2(
                manifest.getGeneration(),
                manifest.getPhysicalTablePath(),
                manifest.getTableBucket(),
                activeSegments,
                references.get(expireCount).logicalStartOffset(),
                unreferencedSegments);
    }

    private static void ensureNewSegmentId(RemoteLogManifest manifest, UUID candidateId) {
        for (RemoteLogSegment segment : manifest.getRemoteLogSegmentList()) {
            checkArgument(
                    !segment.remoteLogSegmentId().equals(candidateId),
                    "Candidate segment id already exists in active segments: %s",
                    candidateId);
        }
        for (UnreferencedRemoteLogSegment segment : manifest.getUnreferencedRemoteLogSegments()) {
            checkArgument(
                    !segment.remoteLogSegment().remoteLogSegmentId().equals(candidateId),
                    "Candidate segment id already exists in unreferenced segments: %s",
                    candidateId);
        }
    }

    private RemoteLogManifestReplacementPlanner() {}
}

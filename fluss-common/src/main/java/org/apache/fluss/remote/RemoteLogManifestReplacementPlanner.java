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

import static org.apache.fluss.remote.UnreferencedRemoteLogSegment.GC_INELIGIBLE_TIMESTAMP;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Builds an immutable V2 manifest update for one completely copied candidate segment.
 *
 * <p>Replacement correctness requires every candidate start offset used as a logical boundary to be
 * a record-batch boundary in every overlapping segment. Clean ISR replication preserves the encoded
 * record batches, and local log rolling only creates segment boundaries between complete batches.
 * The planner validates offset ranges but does not read remote log objects to revalidate their
 * batch layout. Planning preserves the input manifest generation; the caller owns generation
 * assignment for the complete publish batch.
 */
@Internal
public final class RemoteLogManifestReplacementPlanner {

    /** Classification of a candidate physical segment against the current logical remote range. */
    public enum PlanType {
        INITIAL_COPY,
        ALREADY_COVERED,
        GAP,
        RESTART_AFTER_GAP,
        APPEND,
        REPLACE_AND_CLIP,
        REPLACE_AND_CLIP_START
    }

    /** The result of classifying and, when needed, applying one candidate segment. */
    public static final class Result {
        private final PlanType planType;
        private final RemoteLogManifest resultManifest;

        private Result(PlanType planType, RemoteLogManifest resultManifest) {
            this.planType = planType;
            this.resultManifest = resultManifest;
        }

        public PlanType planType() {
            return planType;
        }

        public RemoteLogManifest resultManifest() {
            return resultManifest;
        }
    }

    public static Result plan(RemoteLogManifest manifest, RemoteLogSegment candidate) {
        if (manifest.getRemoteLogSegmentList().isEmpty()) {
            return initialCopy(manifest, candidate, candidate.remoteLogStartOffset());
        }

        long remoteStartOffset = manifest.getRemoteLogStartOffset();
        long remoteEndOffset = manifest.getRemoteLogEndOffset();
        long candidateStartOffset = candidate.remoteLogStartOffset();
        long candidateEndOffset = candidate.remoteLogEndOffset();

        if (candidateEndOffset <= remoteEndOffset) {
            return new Result(PlanType.ALREADY_COVERED, manifest);
        }
        if (candidateStartOffset > remoteEndOffset) {
            return new Result(PlanType.GAP, manifest);
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

        activeSegments.add(candidate);

        List<UnreferencedRemoteLogSegment> unreferencedSegments =
                new ArrayList<>(manifest.getUnreferencedRemoteLogSegments());
        for (RemoteLogSegment segment : segmentsToUnreference) {
            unreferencedSegments.add(
                    new UnreferencedRemoteLogSegment(
                            segment,
                            GC_INELIGIBLE_TIMESTAMP,
                            UnreferencedRemoteLogSegment.Reason.REPLACED,
                            candidate.remoteLogSegmentId()));
        }

        RemoteLogManifest resultManifest =
                RemoteLogManifest.createV2(
                        manifest.getGeneration(),
                        manifest.getPhysicalTablePath(),
                        manifest.getTableBucket(),
                        activeSegments,
                        remoteStartOffset,
                        Math.max(
                                manifest.getHighestCopiedEndOffset(),
                                candidate.remoteLogEndOffset()),
                        unreferencedSegments);
        return new Result(planType, resultManifest);
    }

    /**
     * Starts an empty logical remote range from a complete physical candidate segment.
     *
     * <p>{@code logicalStartOffset} may clip records below the current local retention boundary.
     */
    public static Result initialCopy(
            RemoteLogManifest manifest, RemoteLogSegment candidate, long logicalStartOffset) {
        checkArgument(
                manifest.getRemoteLogSegmentList().isEmpty(),
                "Initial-copy planning requires an empty manifest");
        checkArgument(
                candidate.remoteLogStartOffset() <= logicalStartOffset
                        && logicalStartOffset < candidate.remoteLogEndOffset(),
                "Logical start offset %s must be covered by candidate [%s, %s)",
                logicalStartOffset,
                candidate.remoteLogStartOffset(),
                candidate.remoteLogEndOffset());

        RemoteLogManifest resultManifest =
                RemoteLogManifest.createV2(
                        manifest.getGeneration(),
                        manifest.getPhysicalTablePath(),
                        manifest.getTableBucket(),
                        Collections.singletonList(candidate),
                        logicalStartOffset,
                        Math.max(
                                manifest.getHighestCopiedEndOffset(),
                                candidate.remoteLogEndOffset()),
                        manifest.getUnreferencedRemoteLogSegments());
        return new Result(PlanType.INITIAL_COPY, resultManifest);
    }

    /**
     * Restarts the logical remote range after local retention has made a detected gap impossible to
     * fill.
     *
     * <p>The candidate is kept as a complete physical object, while {@code newRemoteStartOffset}
     * clips its logical start. All previously active segments become GC-ineligible unreferenced
     * metadata until the new manifest has been observed as authoritative.
     */
    public static Result restartAfterGap(
            RemoteLogManifest manifest, RemoteLogSegment candidate, long newRemoteStartOffset) {
        checkArgument(
                !manifest.getRemoteLogSegmentList().isEmpty(),
                "Gap recovery requires a non-empty manifest");
        checkArgument(
                candidate.remoteLogStartOffset() > manifest.getRemoteLogEndOffset(),
                "Gap recovery requires a candidate after the current remote range");
        checkArgument(
                candidate.remoteLogStartOffset() <= newRemoteStartOffset
                        && newRemoteStartOffset < candidate.remoteLogEndOffset(),
                "New remote start offset %s must be covered by candidate [%s, %s)",
                newRemoteStartOffset,
                candidate.remoteLogStartOffset(),
                candidate.remoteLogEndOffset());

        List<RemoteLogSegment> segmentsToUnreference =
                new ArrayList<>(manifest.getRemoteLogSegmentList());
        List<UnreferencedRemoteLogSegment> unreferencedSegments =
                new ArrayList<>(manifest.getUnreferencedRemoteLogSegments());
        for (RemoteLogSegment segment : segmentsToUnreference) {
            unreferencedSegments.add(
                    new UnreferencedRemoteLogSegment(
                            segment,
                            GC_INELIGIBLE_TIMESTAMP,
                            UnreferencedRemoteLogSegment.Reason.REPLACED,
                            candidate.remoteLogSegmentId()));
        }

        RemoteLogManifest resultManifest =
                RemoteLogManifest.createV2(
                        manifest.getGeneration(),
                        manifest.getPhysicalTablePath(),
                        manifest.getTableBucket(),
                        Collections.singletonList(candidate),
                        newRemoteStartOffset,
                        Math.max(
                                manifest.getHighestCopiedEndOffset(),
                                candidate.remoteLogEndOffset()),
                        unreferencedSegments);
        return new Result(PlanType.RESTART_AFTER_GAP, resultManifest);
    }

    /**
     * Moves a continuous expired logical prefix to unreferenced metadata without deleting objects.
     */
    public static RemoteLogManifest expireContinuousPrefix(
            RemoteLogManifest manifest, long currentTimeMs, long ttlMs, Long lakeLogEndOffset) {
        if (ttlMs <= 0L || manifest.getRemoteLogSegmentList().isEmpty()) {
            return manifest;
        }

        List<RemoteLogSegmentReference> references = manifest.getRemoteLogSegmentReferences();
        int expireCount = 0;
        while (expireCount < references.size()) {
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
                            GC_INELIGIBLE_TIMESTAMP,
                            UnreferencedRemoteLogSegment.Reason.EXPIRED,
                            null));
        }
        return RemoteLogManifest.createV2(
                manifest.getGeneration(),
                manifest.getPhysicalTablePath(),
                manifest.getTableBucket(),
                activeSegments,
                activeSegments.isEmpty() ? null : references.get(expireCount).logicalStartOffset(),
                manifest.getHighestCopiedEndOffset(),
                unreferencedSegments);
    }

    /**
     * Starts the GC grace-period clock after an unreferenced transition has been observed in the
     * authoritative manifest.
     */
    public static RemoteLogManifest markUnreferencedSegmentsGcEligible(
            RemoteLogManifest manifest, long eligibleAtMs) {
        checkArgument(
                eligibleAtMs >= 0L && eligibleAtMs != GC_INELIGIBLE_TIMESTAMP,
                "GC eligibility timestamp is invalid: %s",
                eligibleAtMs);

        boolean changed = false;
        List<UnreferencedRemoteLogSegment> eligibleSegments = new ArrayList<>();
        for (UnreferencedRemoteLogSegment segment : manifest.getUnreferencedRemoteLogSegments()) {
            if (segment.isGcEligible()) {
                eligibleSegments.add(segment);
                continue;
            }
            changed = true;
            eligibleSegments.add(
                    new UnreferencedRemoteLogSegment(
                            segment.remoteLogSegment(),
                            eligibleAtMs,
                            segment.reason(),
                            segment.replacementSegmentId()));
        }
        if (!changed) {
            return manifest;
        }
        return RemoteLogManifest.createV2(
                manifest.getGeneration(),
                manifest.getPhysicalTablePath(),
                manifest.getTableBucket(),
                manifest.getRemoteLogSegmentList(),
                manifest.getRemoteLogSegmentList().isEmpty()
                        ? null
                        : manifest.getRemoteLogStartOffset(),
                manifest.getHighestCopiedEndOffset(),
                eligibleSegments);
    }

    private RemoteLogManifestReplacementPlanner() {}
}

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

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.fluss.remote.RemoteLogManifestReplacementPlanner.PlanType.ALREADY_COVERED;
import static org.apache.fluss.remote.RemoteLogManifestReplacementPlanner.PlanType.APPEND;
import static org.apache.fluss.remote.RemoteLogManifestReplacementPlanner.PlanType.GAP;
import static org.apache.fluss.remote.RemoteLogManifestReplacementPlanner.PlanType.INITIAL_COPY;
import static org.apache.fluss.remote.RemoteLogManifestReplacementPlanner.PlanType.REPLACE_AND_CLIP;
import static org.apache.fluss.remote.RemoteLogManifestReplacementPlanner.PlanType.REPLACE_AND_CLIP_START;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the Manifest V2 model, migration, and replacement planner. */
class RemoteLogManifestV2Test {
    private static final PhysicalTablePath PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(TablePath.of("database", "table"));
    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);
    private static final long UNREFERENCED_AT_MS = 1000L;

    @Test
    void testDeriveLogicalReferences() {
        RemoteLogSegment segmentA = segment(0, 10);
        RemoteLogSegment segmentB = segment(5, 20);
        RemoteLogManifest manifest = v2(8L, 0L, segmentA, segmentB);

        assertThat(manifest.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(manifest.getRemoteLogEndOffset()).isEqualTo(20L);
        assertThat(manifest.getRemoteLogSegmentReferences())
                .containsExactly(
                        new RemoteLogSegmentReference(segmentA, 0L, 5L),
                        new RemoteLogSegmentReference(segmentB, 5L, 20L));
        assertThat(manifest.getRemoteLogSegmentReferences())
                .isSameAs(manifest.getRemoteLogSegmentReferences());
    }

    @Test
    void testCachedLogicalViewUsesDefensiveSegmentCopy() {
        RemoteLogSegment segment = segment(0, 10);
        List<RemoteLogSegment> inputSegments = new ArrayList<>();
        inputSegments.add(segment);
        RemoteLogManifest manifest =
                new RemoteLogManifest(PHYSICAL_TABLE_PATH, TABLE_BUCKET, inputSegments);
        List<RemoteLogSegmentReference> references = manifest.getRemoteLogSegmentReferences();

        inputSegments.clear();

        assertThat(manifest.getRemoteLogSegmentList()).containsExactly(segment);
        assertThat(manifest.getRemoteLogSegmentReferences()).isSameAs(references);
        assertThat(references).containsExactly(new RemoteLogSegmentReference(segment, 0L, 10L));
        assertThat(manifest.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(manifest.getRemoteLogEndOffset()).isEqualTo(10L);
    }

    @Test
    void testV1CachedBoundsPreserveMinMaxCompatibility() {
        RemoteLogManifest manifest =
                new RemoteLogManifest(
                        PHYSICAL_TABLE_PATH,
                        TABLE_BUCKET,
                        Arrays.asList(segment(10, 30), segment(0, 20)));

        assertThat(manifest.getRemoteLogStartOffset()).isEqualTo(0L);
        assertThat(manifest.getRemoteLogEndOffset()).isEqualTo(30L);
    }

    @Test
    void testDeriveStartClippedReference() {
        RemoteLogSegment segment = segment(50, 250);
        RemoteLogManifest manifest = v2(2L, 100L, segment);

        assertThat(manifest.getRemoteLogSegmentReferences())
                .containsExactly(new RemoteLogSegmentReference(segment, 100L, 250L));
    }

    @Test
    void testRejectInvalidV2Manifests() {
        RemoteLogSegment segmentA = segment(0, 10);
        RemoteLogSegment segmentWithGap = segment(11, 20);
        RemoteLogSegment segmentWithCoveredEnd = segment(5, 8);

        assertThatThrownBy(
                        () ->
                                RemoteLogManifest.createV2(
                                        1L,
                                        PHYSICAL_TABLE_PATH,
                                        TABLE_BUCKET,
                                        Collections.singletonList(segmentA),
                                        null,
                                        Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must define remote log start offset");
        assertThatThrownBy(() -> v2(1L, 0L, segmentA, segmentWithGap))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gap");
        assertThatThrownBy(() -> v2(1L, 0L, segmentA, segmentWithCoveredEnd))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("end offsets must be strictly increasing");

        UnreferencedRemoteLogSegment alsoUnreferenced =
                new UnreferencedRemoteLogSegment(
                        segmentA,
                        UNREFERENCED_AT_MS,
                        UnreferencedRemoteLogSegment.Reason.REPLACED,
                        null);
        assertThatThrownBy(
                        () ->
                                RemoteLogManifest.createV2(
                                        1L,
                                        PHYSICAL_TABLE_PATH,
                                        TABLE_BUCKET,
                                        Collections.singletonList(segmentA),
                                        0L,
                                        Collections.singletonList(alsoUnreferenced)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both active and unreferenced");
    }

    @Test
    void testMigrateV1ManifestWithSameStartAndCoveredSegments() {
        RemoteLogSegment shortSameStart = segment(0, 10);
        RemoteLogSegment winner = segment(0, 20);
        RemoteLogSegment fullyCovered = segment(5, 15);
        RemoteLogSegment extending = segment(10, 30);
        RemoteLogManifest v1 =
                new RemoteLogManifest(
                        PHYSICAL_TABLE_PATH,
                        TABLE_BUCKET,
                        Arrays.asList(fullyCovered, extending, shortSameStart, winner));

        RemoteLogManifest migrated =
                RemoteLogManifestV2Migration.migrate(v1, 1L, UNREFERENCED_AT_MS);

        assertThat(migrated.getVersion()).isEqualTo(RemoteLogManifest.VERSION_2);
        assertThat(migrated.getGeneration()).isEqualTo(1L);
        assertThat(migrated.getRemoteLogSegmentList()).containsExactly(winner, extending);
        assertThat(migrated.getUnreferencedRemoteLogSegments())
                .extracting(UnreferencedRemoteLogSegment::remoteLogSegment)
                .containsExactlyInAnyOrder(shortSameStart, fullyCovered);
    }

    @Test
    void testRejectAmbiguousOrGappedV1Migration() {
        RemoteLogSegment sameRangeA = segment(0, 10);
        RemoteLogSegment sameRangeB = segment(0, 10);
        RemoteLogManifest ambiguous =
                new RemoteLogManifest(
                        PHYSICAL_TABLE_PATH, TABLE_BUCKET, Arrays.asList(sameRangeA, sameRangeB));

        assertThatThrownBy(
                        () ->
                                RemoteLogManifestV2Migration.migrate(
                                        ambiguous, 1L, UNREFERENCED_AT_MS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot deterministically migrate");

        RemoteLogManifest gapped =
                new RemoteLogManifest(
                        PHYSICAL_TABLE_PATH,
                        TABLE_BUCKET,
                        Arrays.asList(segment(0, 10), segment(11, 20)));
        assertThatThrownBy(
                        () -> RemoteLogManifestV2Migration.migrate(gapped, 1L, UNREFERENCED_AT_MS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gap");
    }

    @Test
    void testPlanInitialAppendAlreadyCoveredAndGap() {
        RemoteLogManifest empty =
                RemoteLogManifest.createV2(
                        1L,
                        PHYSICAL_TABLE_PATH,
                        TABLE_BUCKET,
                        Collections.emptyList(),
                        null,
                        Collections.emptyList());
        RemoteLogSegment first = segment(0, 10);
        RemoteLogManifestReplacementPlanner.Result initial = plan(empty, first);
        assertThat(initial.planType()).isEqualTo(INITIAL_COPY);
        assertThat(initial.resultManifest().getGeneration()).isEqualTo(2L);

        RemoteLogSegment append = segment(10, 20);
        RemoteLogManifestReplacementPlanner.Result appended =
                plan(initial.resultManifest(), append);
        assertThat(appended.planType()).isEqualTo(APPEND);
        assertThat(appended.resultManifest().getRemoteLogSegmentList())
                .containsExactly(first, append);

        RemoteLogManifestReplacementPlanner.Result covered =
                plan(appended.resultManifest(), segment(5, 15));
        assertThat(covered.planType()).isEqualTo(ALREADY_COVERED);
        assertThat(covered.requiresManifestCommit()).isFalse();

        RemoteLogManifestReplacementPlanner.Result gap =
                plan(appended.resultManifest(), segment(21, 30));
        assertThat(gap.planType()).isEqualTo(GAP);
        assertThat(gap.requiresManifestCommit()).isFalse();
    }

    @Test
    void testPlanPartialAndMultiSegmentReplacement() {
        RemoteLogSegment segmentA = segment(0, 10);
        RemoteLogSegment segmentC = segment(10, 15);
        RemoteLogManifest manifest = v2(1L, 0L, segmentA, segmentC);
        RemoteLogSegment replacement = segment(5, 20);

        RemoteLogManifestReplacementPlanner.Result result = plan(manifest, replacement);

        assertThat(result.planType()).isEqualTo(REPLACE_AND_CLIP);
        assertThat(result.resultManifest().getRemoteLogSegmentList())
                .containsExactly(segmentA, replacement);
        assertThat(result.resultManifest().getRemoteLogSegmentReferences())
                .containsExactly(
                        new RemoteLogSegmentReference(segmentA, 0L, 5L),
                        new RemoteLogSegmentReference(replacement, 5L, 20L));
        assertThat(result.segmentsToUnreference()).containsExactly(segmentC);
    }

    @Test
    void testPlanSameStartAndStartClippedReplacement() {
        RemoteLogSegment old = segment(0, 10);
        RemoteLogSegment sameStart = segment(0, 20);
        RemoteLogManifestReplacementPlanner.Result sameStartResult =
                plan(v2(1L, 0L, old), sameStart);

        assertThat(sameStartResult.planType()).isEqualTo(REPLACE_AND_CLIP);
        assertThat(sameStartResult.resultManifest().getRemoteLogSegmentList())
                .containsExactly(sameStart);
        assertThat(sameStartResult.segmentsToUnreference()).containsExactly(old);

        RemoteLogSegment startClipped = segment(50, 250);
        RemoteLogManifestReplacementPlanner.Result startClippedResult =
                plan(v2(1L, 100L, segment(100, 200)), startClipped);
        assertThat(startClippedResult.planType()).isEqualTo(REPLACE_AND_CLIP_START);
        assertThat(startClippedResult.resultManifest().getRemoteLogStartOffset()).isEqualTo(100L);
        assertThat(startClippedResult.resultManifest().getRemoteLogSegmentReferences())
                .containsExactly(new RemoteLogSegmentReference(startClipped, 100L, 250L));
    }

    @Test
    void testExpireOnlyContinuousLogicalPrefixWithoutDeletingObjects() {
        RemoteLogSegment first = segment(0, 10);
        RemoteLogSegment overlapping = segment(5, 20);
        RemoteLogSegment newest = segment(20, 30);
        RemoteLogManifest manifest = v2(3L, 0L, first, overlapping, newest);

        RemoteLogManifest expired =
                RemoteLogManifestReplacementPlanner.expireContinuousPrefix(
                        manifest, 100L, 85L, null, UNREFERENCED_AT_MS);

        assertThat(expired.getGeneration()).isEqualTo(3L);
        assertThat(expired.getRemoteLogStartOffset()).isEqualTo(5L);
        assertThat(expired.getRemoteLogSegmentList()).containsExactly(overlapping, newest);
        assertThat(expired.getUnreferencedRemoteLogSegments())
                .extracting(UnreferencedRemoteLogSegment::remoteLogSegment)
                .containsExactly(first);
        assertThat(expired.getUnreferencedRemoteLogSegments())
                .extracting(UnreferencedRemoteLogSegment::reason)
                .containsExactly(UnreferencedRemoteLogSegment.Reason.EXPIRED);
    }

    @Test
    void testExpirationStopsAtFirstIneligibleReferenceAndRetainsLastReference() {
        RemoteLogSegment first = segment(0, 10);
        RemoteLogSegment second = segment(10, 20);
        RemoteLogManifest twoSegments = v2(1L, 0L, first, second);

        assertThat(
                        RemoteLogManifestReplacementPlanner.expireContinuousPrefix(
                                twoSegments, 100L, 0L, 5L, UNREFERENCED_AT_MS))
                .isSameAs(twoSegments);

        RemoteLogManifest onlyOne = v2(1L, 0L, first);
        assertThat(
                        RemoteLogManifestReplacementPlanner.expireContinuousPrefix(
                                onlyOne, 100L, 0L, null, UNREFERENCED_AT_MS))
                .isSameAs(onlyOne);
    }

    private RemoteLogManifestReplacementPlanner.Result plan(
            RemoteLogManifest manifest, RemoteLogSegment segment) {
        return RemoteLogManifestReplacementPlanner.plan(manifest, segment, UNREFERENCED_AT_MS);
    }

    private RemoteLogManifest v2(
            long generation, Long remoteStartOffset, RemoteLogSegment... segments) {
        return RemoteLogManifest.createV2(
                generation,
                PHYSICAL_TABLE_PATH,
                TABLE_BUCKET,
                Arrays.asList(segments),
                remoteStartOffset,
                Collections.emptyList());
    }

    private RemoteLogSegment segment(long startOffset, long endOffset) {
        return RemoteLogSegment.Builder.builder()
                .physicalTablePath(PHYSICAL_TABLE_PATH)
                .tableBucket(TABLE_BUCKET)
                .remoteLogSegmentId(UUID.randomUUID())
                .remoteLogStartOffset(startOffset)
                .remoteLogEndOffset(endOffset)
                .maxTimestamp(endOffset)
                .segmentSizeInBytes(1)
                .build();
    }
}

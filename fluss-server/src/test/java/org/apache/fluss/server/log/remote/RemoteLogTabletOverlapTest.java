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

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteLogTabletOverlapTest {
    private static final PhysicalTablePath TABLE_PATH =
            PhysicalTablePath.of(TablePath.of("db", "table"));
    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLoadLegacyManifestWithSameStartOffset(boolean shorterFirst) {
        RemoteLogSegment longer = segment(10L, 30L);
        RemoteLogSegment shorter = segment(10L, 20L);
        RemoteLogManifest manifest =
                legacyManifest(shorterFirst ? shorter : longer, shorterFirst ? longer : shorter);
        RemoteLogTablet tablet = new RemoteLogTablet(TABLE_PATH, TABLE_BUCKET);
        tablet.loadRemoteLogManifest(manifest);

        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(20L)).containsExactly(longer);
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(29L)).containsExactly(longer);
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(30L)).isEmpty();
        assertThat(tablet.getRemoteLogEndOffset()).hasValue(30L);
        assertThat(tablet.currentManifest().getRemoteLogSegmentList()).containsExactly(longer);
        assertThat(tablet.getIdToRemoteLogSegmentMap())
                .containsOnlyKeys(longer.remoteLogSegmentId());
        assertThat(tablet.getRemoteSizeInBytes()).isEqualTo(longer.segmentSizeInBytes());
        assertThat(tablet.findSegmentsByTimestamp(1L)).containsExactly(longer);
        assertThat(tablet.expiredRemoteLogSegments(100L, null, 1L)).containsExactly(longer);
        assertThat(RemoteLogManifest.fromJsonBytes(manifest.toJsonBytes())).isEqualTo(manifest);
        assertThat(manifest.getRemoteLogSegmentList()).hasSize(2);
    }

    @Test
    void testLoadLegacyManifestWithNestedSegment() {
        RemoteLogSegment longer = segment(10L, 40L);
        RemoteLogTablet tablet = new RemoteLogTablet(TABLE_PATH, TABLE_BUCKET);
        tablet.loadRemoteLogManifest(legacyManifest(longer, segment(20L, 30L)));

        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(35L)).containsExactly(longer);
    }

    @Test
    void testLogicalLookupUsesClippedRanges() {
        RemoteLogSegment first = segment(0L, 10L).withLogicalRange(0L, 5L);
        RemoteLogSegment second = segment(5L, 20L);
        RemoteLogTablet tablet = new RemoteLogTablet(TABLE_PATH, TABLE_BUCKET);
        tablet.loadRemoteLogManifest(
                new RemoteLogManifest(TABLE_PATH, TABLE_BUCKET, Arrays.asList(first, second)));

        assertThat(tablet.relevantRemoteLogSegments(4L)).containsExactly(first, second);
        assertThat(tablet.relevantRemoteLogSegments(5L)).containsExactly(second);
        assertThat(tablet.relevantRemoteLogSegments(19L)).containsExactly(second);
        assertThat(tablet.relevantRemoteLogSegments(20L)).isEmpty();
    }

    @Test
    void testFetchV0StopsBeforePhysicalOverlap() {
        RemoteLogSegment first = segment(0L, 10L).withLogicalRange(0L, 5L);
        RemoteLogSegment second = segment(5L, 20L);
        RemoteLogTablet tablet = new RemoteLogTablet(TABLE_PATH, TABLE_BUCKET);
        tablet.loadRemoteLogManifest(
                new RemoteLogManifest(TABLE_PATH, TABLE_BUCKET, Arrays.asList(first, second)));

        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(0L)).containsExactly(first);
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(10L)).containsExactly(second);
    }

    @Test
    void testFetchV0ReturnsContiguousSegmentsTogether() {
        List<RemoteLogSegment> segments =
                Arrays.asList(segment(0L, 10L), segment(10L, 20L), segment(20L, 30L));
        RemoteLogTablet tablet = new RemoteLogTablet(TABLE_PATH, TABLE_BUCKET);
        tablet.loadRemoteLogManifest(new RemoteLogManifest(TABLE_PATH, TABLE_BUCKET, segments));

        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(5L))
                .containsExactlyElementsOf(segments);
    }

    @Test
    void testEmptyManifestKeepsCopyProgress() {
        RemoteLogTablet tablet = new RemoteLogTablet(TABLE_PATH, TABLE_BUCKET);
        tablet.loadRemoteLogManifest(
                new RemoteLogManifest(TABLE_PATH, TABLE_BUCKET, Collections.emptyList(), 20L));

        assertThat(tablet.allRemoteLogSegments()).isEmpty();
        assertThat(tablet.getRemoteLogEndOffset()).isEmpty();
        assertThat(tablet.getHighestCopiedEndOffset()).isEqualTo(20L);
    }

    @Test
    void testLegacyManifestMergeAndReloadKeepsLogicalView() {
        RemoteLogSegment first = segment(0L, 20L);
        RemoteLogSegment second = segment(10L, 30L);
        RemoteLogSegment third = segment(25L, 40L);
        RemoteLogTablet tablet = new RemoteLogTablet(TABLE_PATH, TABLE_BUCKET);
        tablet.loadRemoteLogManifest(legacyManifest(third, first, second));

        RemoteLogSegment extension = segment(35L, 50L);
        RemoteLogManifest merged =
                tablet.currentManifest()
                        .trimAndMerge(
                                Collections.emptyList(), Collections.singletonList(extension));
        RemoteLogManifest restored = RemoteLogManifest.fromJsonBytes(merged.toJsonBytes());
        tablet.loadRemoteLogManifest(restored);

        assertThat(tablet.currentManifest()).isEqualTo(restored);
        assertThat(tablet.allRemoteLogSegments())
                .containsExactly(
                        first.withLogicalRange(0L, 10L),
                        second.withLogicalRange(10L, 25L),
                        third.withLogicalRange(25L, 35L),
                        extension);
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(0L))
                .containsExactly(first.withLogicalRange(0L, 10L));
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(20L))
                .containsExactly(second.withLogicalRange(10L, 25L));
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(30L))
                .containsExactly(third.withLogicalRange(25L, 35L));
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(40L)).containsExactly(extension);
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(50L)).isEmpty();
        assertThat(tablet.getHighestCopiedEndOffset()).isEqualTo(50L);
    }

    @Test
    void testLegacyManifestKeepsGapsUnreadable() {
        RemoteLogSegment first = segment(10L, 20L);
        RemoteLogSegment second = segment(30L, 40L);
        RemoteLogTablet tablet = new RemoteLogTablet(TABLE_PATH, TABLE_BUCKET);
        tablet.loadRemoteLogManifest(legacyManifest(second, first));

        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(9L)).isEmpty();
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(19L)).containsExactly(first);
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(20L)).isEmpty();
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(29L)).isEmpty();
        assertThat(tablet.relevantRemoteLogSegmentsForFetchV0(30L)).containsExactly(second);
    }

    @Test
    void testLegacyTimestampLookupContinuesAfterNormalizedClippedEnd() {
        RemoteLogSegment first = segment(0L, 20L, 30L);
        RemoteLogSegment second = segment(10L, 30L, 40L);
        RemoteLogTablet tablet = new RemoteLogTablet(TABLE_PATH, TABLE_BUCKET);
        tablet.loadRemoteLogManifest(legacyManifest(first, second));

        assertThat(tablet.findSegmentsByTimestamp(25L))
                .containsExactly(first.withLogicalRange(0L, 10L), second);
        assertThat(tablet.findSegmentsByTimestamp(35L)).containsExactly(second);
    }

    private static RemoteLogManifest legacyManifest(RemoteLogSegment... segments) {
        byte[] json =
                new RemoteLogManifest(TABLE_PATH, TABLE_BUCKET, Arrays.asList(segments))
                        .toJsonBytes();
        assertThat(new String(json, StandardCharsets.UTF_8))
                .contains("\"version\":1")
                .doesNotContain("logical_start_offset", "logical_end_offset");
        return RemoteLogManifest.fromJsonBytes(json);
    }

    private static RemoteLogSegment segment(long startOffset, long endOffset) {
        return segment(startOffset, endOffset, 1L);
    }

    private static RemoteLogSegment segment(long startOffset, long endOffset, long timestamp) {
        return RemoteLogSegment.Builder.builder()
                .physicalTablePath(TABLE_PATH)
                .tableBucket(TABLE_BUCKET)
                .remoteLogSegmentId(UUID.randomUUID())
                .remoteLogStartOffset(startOffset)
                .remoteLogEndOffset(endOffset)
                .maxTimestamp(timestamp)
                .segmentSizeInBytes(10)
                .build();
    }
}

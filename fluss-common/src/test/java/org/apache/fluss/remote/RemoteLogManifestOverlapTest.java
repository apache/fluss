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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the backward-compatible logical range and overlap merge model. */
class RemoteLogManifestOverlapTest {
    private static final PhysicalTablePath TABLE_PATH =
            PhysicalTablePath.of(TablePath.of("db", "table"));
    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);

    @Test
    void testOldManifestDefaultsLogicalRangeToPhysicalRange() {
        String oldJson =
                "{\"version\":1,\"database\":\"db\",\"table\":\"table\","
                        + "\"table_id\":1,\"bucket_id\":0,\"remote_log_segments\":[{"
                        + "\"segment_id\":\"00000000-0000-0000-0000-000000000001\","
                        + "\"start_offset\":0,\"end_offset\":10,\"max_timestamp\":1,"
                        + "\"size_in_bytes\":10}]}";

        RemoteLogManifest manifest =
                RemoteLogManifest.fromJsonBytes(oldJson.getBytes(StandardCharsets.UTF_8));

        RemoteLogSegment segment = manifest.getRemoteLogSegmentList().get(0);
        assertThat(segment.logicalStartOffset()).isEqualTo(0L);
        assertThat(segment.logicalEndOffset()).isEqualTo(10L);
        assertThat(manifest.getHighestCopiedEndOffset()).isEqualTo(10L);
    }

    @Test
    void testCompatibleManifestOmitsDefaultedFields() {
        String json = new String(manifest(segment(0L, 10L)).toJsonBytes(), StandardCharsets.UTF_8);

        assertThat(json)
                .doesNotContain("logical_start_offset")
                .doesNotContain("logical_end_offset")
                .doesNotContain("highest_copied_end_offset");
    }

    @Test
    void testClippedManifestPersistsLogicalRange() {
        RemoteLogSegment clipped = segment(0L, 10L).withLogicalRange(0L, 5L);

        RemoteLogManifest restored =
                RemoteLogManifest.fromJsonBytes(manifest(clipped).toJsonBytes());

        assertThat(restored.getRemoteLogSegmentList()).containsExactly(clipped);
    }

    @Test
    void testMergeOverlappingSegmentClipsLogicalSuffix() {
        RemoteLogSegment first = segment(0L, 10L);
        RemoteLogSegment replacement = segment(5L, 20L);
        RemoteLogManifest result =
                manifest(first)
                        .trimAndMerge(
                                Collections.emptyList(), Collections.singletonList(replacement));

        assertThat(result.getRemoteLogSegmentList())
                .extracting(
                        segment ->
                                Arrays.asList(
                                        segment.remoteLogStartOffset(),
                                        segment.remoteLogEndOffset(),
                                        segment.logicalStartOffset(),
                                        segment.logicalEndOffset()))
                .containsExactly(Arrays.asList(0L, 10L, 0L, 5L), Arrays.asList(5L, 20L, 5L, 20L));
        assertThat(result.getRemoteLogEndOffset()).isEqualTo(20L);
        assertThat(result.getHighestCopiedEndOffset()).isEqualTo(20L);
    }

    @Test
    void testMergeMultipleCandidatesUsesPreviousResultAsBase() {
        RemoteLogManifest result =
                manifest(segment(0L, 10L))
                        .trimAndMerge(
                                Collections.emptyList(),
                                Arrays.asList(segment(5L, 20L), segment(15L, 30L)));

        assertThat(result.getRemoteLogSegmentList())
                .extracting(RemoteLogSegment::logicalStartOffset)
                .containsExactly(0L, 5L, 15L);
        assertThat(result.getRemoteLogSegmentList())
                .extracting(RemoteLogSegment::logicalEndOffset)
                .containsExactly(5L, 15L, 30L);
    }

    @Test
    void testMergeOverlappingSegmentTrimsExistingSuffix() {
        RemoteLogSegment first = segment(0L, 10L);
        RemoteLogSegment second = segment(10L, 20L);
        RemoteLogSegment third = segment(20L, 30L);
        RemoteLogSegment replacement = segment(15L, 40L);

        RemoteLogManifest result =
                manifest(first, second, third)
                        .trimAndMerge(
                                Collections.emptyList(), Collections.singletonList(replacement));

        assertThat(result.getRemoteLogSegmentList())
                .containsExactly(first, second.withLogicalRange(10L, 15L), replacement);
    }

    @Test
    void testFullReplacementKeepsOnlyNewPhysicalObject() {
        RemoteLogSegment oldSegment = segment(0L, 10L);
        RemoteLogSegment replacement = segment(0L, 20L);

        RemoteLogManifest result =
                manifest(oldSegment)
                        .trimAndMerge(
                                Collections.emptyList(), Collections.singletonList(replacement));

        assertThat(result.getRemoteLogSegmentList()).containsExactly(replacement);
    }

    @Test
    void testReplacementStartingBeforeRemoteStartDoesNotRestoreExpiredPrefix() {
        RemoteLogSegment replacement = segment(5L, 25L);

        RemoteLogManifest result =
                manifest(segment(10L, 20L))
                        .trimAndMerge(
                                Collections.emptyList(), Collections.singletonList(replacement));

        assertThat(result.getRemoteLogSegmentList())
                .containsExactly(replacement.withLogicalRange(10L, 25L));
        assertThat(result.getRemoteLogStartOffset()).isEqualTo(10L);
        assertThat(result.getRemoteLogEndOffset()).isEqualTo(25L);
    }

    @Test
    void testRemoteLogOffsetsUseLogicalRange() {
        RemoteLogManifest result = manifest(segment(5L, 25L).withLogicalRange(10L, 20L));

        assertThat(result.getRemoteLogStartOffset()).isEqualTo(10L);
        assertThat(result.getRemoteLogEndOffset()).isEqualTo(20L);
    }

    @Test
    void testAlreadyCoveredCandidateIsUnused() {
        RemoteLogSegment covered = segment(2L, 8L);

        RemoteLogManifest result =
                manifest(segment(0L, 10L))
                        .trimAndMerge(Collections.emptyList(), Collections.singletonList(covered));

        assertThat(result.getRemoteLogSegmentList()).hasSize(1);
    }

    @Test
    void testGapIsRejected() {
        assertThatThrownBy(
                        () ->
                                manifest(segment(0L, 10L))
                                        .trimAndMerge(
                                                Collections.emptyList(),
                                                Collections.singletonList(segment(11L, 20L))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("introduces a gap");
    }

    @Test
    void testEmptyManifestPersistsHighestCopiedEndOffset() {
        RemoteLogManifest empty =
                new RemoteLogManifest(TABLE_PATH, TABLE_BUCKET, Collections.emptyList(), 20L);

        RemoteLogManifest restored = RemoteLogManifest.fromJsonBytes(empty.toJsonBytes());

        assertThat(restored.getRemoteLogSegmentList()).isEmpty();
        assertThat(restored.getRemoteLogEndOffset()).isEqualTo(-1L);
        assertThat(restored.getHighestCopiedEndOffset()).isEqualTo(20L);
    }

    private static RemoteLogManifest manifest(RemoteLogSegment... segments) {
        return new RemoteLogManifest(TABLE_PATH, TABLE_BUCKET, Arrays.asList(segments));
    }

    private static RemoteLogSegment segment(long startOffset, long endOffset) {
        return RemoteLogSegment.Builder.builder()
                .physicalTablePath(TABLE_PATH)
                .tableBucket(TABLE_BUCKET)
                .remoteLogSegmentId(UUID.randomUUID())
                .remoteLogStartOffset(startOffset)
                .remoteLogEndOffset(endOffset)
                .maxTimestamp(1L)
                .segmentSizeInBytes(10)
                .build();
    }
}

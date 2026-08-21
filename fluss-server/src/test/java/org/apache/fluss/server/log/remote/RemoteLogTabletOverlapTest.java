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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteLogTabletOverlapTest {
    private static final PhysicalTablePath TABLE_PATH =
            PhysicalTablePath.of(TablePath.of("db", "table"));
    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);

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

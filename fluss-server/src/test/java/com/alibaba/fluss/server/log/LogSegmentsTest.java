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

package com.alibaba.fluss.server.log;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LogSegments}. */
final class LogSegmentsTest extends LogTestBase {

    private TableBucket tableBucket;
    private @TempDir File tempDir;

    @BeforeEach
    public void setup() throws IOException {
        super.before();
        tableBucket = new TableBucket(1001, 1);
    }

    @Test
    void testBasicOperations() throws IOException {
        LogSegments segments = new LogSegments(tableBucket);
        assertThat(segments.isEmpty()).isTrue();

        long offset1 = 40;
        LogSegment segment1 = createSegment(offset1);
        long offset2 = 80;
        LogSegment segment2 = createSegment(offset2);
        LogSegment segment3 = createSegment(offset1);

        // add segment1.
        segments.add(segment1);
        assertThat(segments.isEmpty()).isFalse();
        assertThat(segments.numberOfSegments()).isEqualTo(1);
        assertThat(segments.contains(offset1)).isTrue();
        assertThat(segments.get(offset1)).isEqualTo(Optional.of(segment1));

        // add segment2.
        segments.add(segment2);
        assertThat(segments.isEmpty()).isFalse();
        assertThat(segments.numberOfSegments()).isEqualTo(2);
        assertThat(segments.contains(offset2)).isTrue();
        assertThat(segments.get(offset2)).isEqualTo(Optional.of(segment2));

        // replace segment1 with segment3
        segments.add(segment3);
        assertThat(segments.isEmpty()).isFalse();
        assertThat(segments.numberOfSegments()).isEqualTo(2);
        assertThat(segments.contains(offset1)).isTrue();
        assertThat(segments.get(offset1)).isEqualTo(Optional.of(segment3));

        // remove segment2
        segments.remove(offset2);
        assertThat(segments.isEmpty()).isFalse();
        assertThat(segments.numberOfSegments()).isEqualTo(1);
        assertThat(segments.contains(offset2)).isFalse();

        // clear all segments
        segments.clear();
        assertThat(segments.isEmpty()).isTrue();
        assertThat(segments.numberOfSegments()).isEqualTo(0);
        assertThat(segments.contains(offset1)).isFalse();

        segments.close();
    }

    @Test
    void testSegmentAccess() throws IOException {
        LogSegments segments = new LogSegments(tableBucket);

        long offset1 = 1L;
        LogSegment segment1 = createSegment(offset1);
        long offset2 = 2L;
        LogSegment segment2 = createSegment(offset2);
        long offset3 = 3L;
        LogSegment segment3 = createSegment(offset3);
        long offset4 = 4L;
        LogSegment segment4 = createSegment(offset4);
        List<LogSegment> segmentList = Arrays.asList(segment1, segment2, segment3, segment4);

        // Test firstEntry, lastEntry
        for (LogSegment segment : segmentList) {
            segments.add(segment);
            assertEntry(segment1, segments.firstEntry().get());
            assertThat(segments.firstSegment()).isEqualTo(Optional.of(segment1));
            assertEntry(segment, segments.lastEntry().get());
            assertThat(segments.lastSegment()).isEqualTo(Optional.of(segment));
        }

        // Test baseOffsets
        assertThat(segments.baseOffsets())
                .isEqualTo(Arrays.asList(offset1, offset2, offset3, offset4));

        // Test values
        assertThat(segments.values()).isEqualTo(segmentList);

        // Test values(to, from)
        assertThatThrownBy(() -> segments.values(2, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(segments.values(1, 1)).isEqualTo(Collections.emptyList());
        assertThat(segments.values(1, 2)).isEqualTo(Collections.singletonList(segment1));
        assertThat(segments.values(1, 3)).isEqualTo(Arrays.asList(segment1, segment2));
        assertThat(segments.values(1, 4)).isEqualTo(Arrays.asList(segment1, segment2, segment3));
        assertThat(segments.values(2, 4)).isEqualTo(Arrays.asList(segment2, segment3));
        assertThat(segments.values(4, 5)).isEqualTo(Collections.singletonList(segment4));
    }

    @Test
    void testClosesMatchOperations() throws IOException {
        LogSegments segments = new LogSegments(tableBucket);
        LogSegment segment1 = createSegment(1L);
        LogSegment segment2 = createSegment(3L);
        LogSegment segment3 = createSegment(5L);
        LogSegment segment4 = createSegment(7L);
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment3);
        segments.add(segment4);

        // Test floorSegment
        assertThat(segments.floorSegment(2L)).isEqualTo(Optional.of(segment1));
        assertThat(segments.floorSegment(3L)).isEqualTo(Optional.of(segment2));

        // Test lowerSegment
        assertThat(segments.lowerSegment(3L)).isEqualTo(Optional.of(segment1));
        assertThat(segments.floorSegment(4L)).isEqualTo(Optional.of(segment2));

        // Test higherSegment, higherEntry
        assertThat(segments.higherSegment(4L)).isEqualTo(Optional.of(segment3));
        assertEntry(segment3, segments.higherEntry(4L).get());
        assertThat(segments.higherSegment(5L)).isEqualTo(Optional.of(segment4));
        assertEntry(segment4, segments.higherEntry(5L).get());

        segments.close();
    }

    @Test
    void testHigherSegments() throws IOException {
        LogSegments segments = new LogSegments(tableBucket);
        LogSegment segment1 = createSegment(1L);
        LogSegment segment2 = createSegment(3L);
        LogSegment segment3 = createSegment(5L);
        LogSegment segment4 = createSegment(7L);
        LogSegment segment5 = createSegment(9L);
        segments.add(segment1);
        segments.add(segment2);
        segments.add(segment3);
        segments.add(segment4);
        segments.add(segment5);

        // higherSegments(0) should return all segments in order
        assertThat(segments.higherSegments(0L))
                .isEqualTo(Arrays.asList(segment1, segment2, segment3, segment4, segment5));

        // higherSegments(1) should return all segments in order except segment1
        assertThat(segments.higherSegments(1L))
                .isEqualTo(Arrays.asList(segment2, segment3, segment4, segment5));

        // higherSegments(8) should return only segment5
        assertThat(segments.higherSegments(8L)).isEqualTo(Collections.singletonList(segment5));

        // higherSegments(9) should return no segments
        assertThat(segments.higherSegments(9L)).isEqualTo(Collections.emptyList());
    }

    private void assertEntry(LogSegment segment, Map.Entry<Long, LogSegment> tested) {
        assertThat(segment.getBaseOffset()).isEqualTo(tested.getKey());
        assertThat(segment).isEqualTo(tested.getValue());
    }

    private LogSegment createSegment(long baseOffset) throws IOException {
        return LogTestUtils.createSegment(baseOffset, tempDir, 10);
    }
}

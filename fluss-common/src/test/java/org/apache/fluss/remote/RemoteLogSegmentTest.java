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

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RemoteLogSegment}. */
class RemoteLogSegmentTest {
    private static final PhysicalTablePath PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(TablePath.of("database", "table"));
    private static final TableBucket TABLE_BUCKET = new TableBucket(1L, 0);

    @Test
    void testHalfOpenOffsetRange() {
        RemoteLogSegment segment = createRemoteLogSegment(10L, 20L);

        assertThat(segment.remoteLogStartOffset()).isEqualTo(10L);
        assertThat(segment.remoteLogEndOffset()).isEqualTo(20L);
    }

    @Test
    void testRejectEmptyOffsetRange() {
        assertThatThrownBy(() -> createRemoteLogSegment(10L, 10L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exclusive end offset");
    }

    @Test
    void testRejectReversedOffsetRange() {
        assertThatThrownBy(() -> createRemoteLogSegment(20L, 10L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exclusive end offset");
    }

    private RemoteLogSegment createRemoteLogSegment(long startOffset, long endOffset) {
        return RemoteLogSegment.Builder.builder()
                .physicalTablePath(PHYSICAL_TABLE_PATH)
                .tableBucket(TABLE_BUCKET)
                .remoteLogSegmentId(UUID.randomUUID())
                .remoteLogStartOffset(startOffset)
                .remoteLogEndOffset(endOffset)
                .maxTimestamp(1L)
                .segmentSizeInBytes(1)
                .build();
    }
}

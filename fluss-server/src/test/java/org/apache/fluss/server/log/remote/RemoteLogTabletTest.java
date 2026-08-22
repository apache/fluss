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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.remote.RemoteLogManifest;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.LogTablet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RemoteLogTablet}. */
class RemoteLogTabletTest extends RemoteLogTestBase {

    @BeforeEach
    public void setup() throws Exception {
        super.setup();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTakeAndLoadSnapshot(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        List<RemoteLogSegment> remoteLogSegmentList = createRemoteLogSegmentList(logTablet);
        loadRemoteLogSegments(remoteLogTablet, logTablet, remoteLogSegmentList);
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap())
                .hasSize(remoteLogSegmentList.size());

        RemoteLogTablet newLogManifest = buildRemoteLogTablet(logTablet);
        newLogManifest.loadRemoteLogManifest(remoteLogTablet.currentManifest());
        assertThat(newLogManifest.getIdToRemoteLogSegmentMap())
                .isEqualTo(remoteLogTablet.getIdToRemoteLogSegmentMap());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRelevantRemoteLogSegments(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        List<RemoteLogSegment> remoteLogSegmentList = createRemoteLogSegmentList(logTablet);
        loadRemoteLogSegments(remoteLogTablet, logTablet, remoteLogSegmentList);

        // An offset before the first segment should return empty (OutOfRange).
        List<RemoteLogSegment> result = remoteLogTablet.relevantRemoteLogSegments(-1L);
        assertThat(result).isEmpty();

        // Get offset from 0.
        result = remoteLogTablet.relevantRemoteLogSegments(0L);
        assertThat(result.size()).isEqualTo(5);
        assertThat(result).containsExactlyInAnyOrderElementsOf(remoteLogSegmentList);

        // Get offset from 10, the remote log start offset of the second segment is 10, so the first
        // segment will not be included.
        result = remoteLogTablet.relevantRemoteLogSegments(10L);
        assertThat(result.size()).isEqualTo(4);

        result = remoteLogTablet.relevantRemoteLogSegments(11L);
        assertThat(result.size()).isEqualTo(4);

        result = remoteLogTablet.relevantRemoteLogSegments(49L);
        assertThat(result.size()).isEqualTo(1);

        // Get offset from 50, the remote start offset of the last segment is 40, the remote log end
        // offset is 50, no segment will be included.
        result = remoteLogTablet.relevantRemoteLogSegments(50L);
        assertThat(result.size()).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFindRemoteLogSegmentByTimestamp(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        loadRemoteLogSegments(
                remoteLogTablet,
                logTablet,
                Arrays.asList(
                        createLogSegmentWithMaxTimestamp(logTablet, 10, 0, 10),
                        createLogSegmentWithMaxTimestamp(logTablet, 20, 10, 20),
                        createLogSegmentWithMaxTimestamp(logTablet, 30, 20, 30),
                        createLogSegmentWithMaxTimestamp(logTablet, 40, 30, 40),
                        createLogSegmentWithMaxTimestamp(logTablet, 50, 40, 50)));

        assertThat(remoteLogTablet.findSegmentsByTimestamp(0L).get(0).remoteLogStartOffset())
                .isEqualTo(0L);
        assertThat(remoteLogTablet.findSegmentsByTimestamp(1L).get(0).remoteLogStartOffset())
                .isEqualTo(0L);
        assertThat(remoteLogTablet.findSegmentsByTimestamp(10L).get(0).remoteLogStartOffset())
                .isEqualTo(0L);
        assertThat(remoteLogTablet.findSegmentsByTimestamp(40L).get(0).remoteLogStartOffset())
                .isEqualTo(30L);
        assertThat(remoteLogTablet.findSegmentsByTimestamp(50L).get(0).remoteLogStartOffset())
                .isEqualTo(40L);
        assertThat(remoteLogTablet.findSegmentsByTimestamp(51L)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFindRemoteLogSegmentsByTimestampContinuesAfterClippedEnd(boolean partitionTable)
            throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        RemoteLogSegment clippedSegment =
                createLogSegmentWithMaxTimestamp(logTablet, 30, 0, 20).withLogicalRange(0, 10);
        RemoteLogSegment nextSegment = createLogSegmentWithMaxTimestamp(logTablet, 40, 10, 30);
        loadRemoteLogSegments(
                remoteLogTablet, logTablet, Arrays.asList(clippedSegment, nextSegment));

        assertThat(remoteLogTablet.findSegmentsByTimestamp(25L))
                .extracting(RemoteLogSegment::logicalStartOffset)
                .containsExactly(0L, 10L);
        assertThat(remoteLogTablet.findSegmentsByTimestamp(35L))
                .extracting(RemoteLogSegment::logicalStartOffset)
                .containsExactly(10L);
    }

    private void loadRemoteLogSegments(
            RemoteLogTablet remoteLogTablet,
            LogTablet logTablet,
            List<RemoteLogSegment> remoteLogSegments) {
        remoteLogTablet.loadRemoteLogManifest(
                new RemoteLogManifest(
                        logTablet.getPhysicalTablePath(),
                        logTablet.getTableBucket(),
                        remoteLogSegments));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testExpireSegmentsWithCurrentTtl(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);

        // The current table TTL is supplied by the tiering task for each expiration pass.
        long defaultTtlMs = conf.get(ConfigOptions.TABLE_LOG_TTL).toMillis();

        // add 1 segment with maxTimestamp = 0
        RemoteLogSegment segment = createLogSegmentWithMaxTimestamp(logTablet, 0L, 0L, 10L);
        loadRemoteLogSegments(remoteLogTablet, logTablet, Collections.singletonList(segment));

        // currentTime = 1 hour. (1h - 0) < 7d, so the segment is NOT expired.
        long oneHourMs = java.time.Duration.ofHours(1).toMillis();
        assertThat(remoteLogTablet.expiredRemoteLogSegments(oneHourMs, null, defaultTtlMs))
                .isEmpty();

        // With a 1 ms TTL, the same segment is expired.
        assertThat(remoteLogTablet.expiredRemoteLogSegments(oneHourMs, null, 1L))
                .containsExactly(segment);

        // A non-positive current TTL disables expiration.
        assertThat(remoteLogTablet.expiredRemoteLogSegments(oneHourMs, null, -1L)).isEmpty();
    }

    RemoteLogSegment createLogSegmentWithMaxTimestamp(
            LogTablet logTablet,
            long timestamp,
            long remoteLogStartOffset,
            long remoteLogEndOffset) {
        return RemoteLogSegment.Builder.builder()
                .remoteLogSegmentId(UUID.randomUUID())
                .remoteLogStartOffset(remoteLogStartOffset)
                .remoteLogEndOffset(remoteLogEndOffset)
                .maxTimestamp(timestamp)
                .segmentSizeInBytes(1000)
                .tableBucket(logTablet.getTableBucket())
                .physicalTablePath(logTablet.getPhysicalTablePath())
                .build();
    }
}

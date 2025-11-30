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

import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.server.log.LogTablet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
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
    void testAppend(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        List<RemoteLogSegment> remoteLogSegmentList = createRemoteLogSegmentList(logTablet);
        remoteLogTablet.addAndDeleteLogSegments(remoteLogSegmentList, Collections.emptyList());
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap())
                .hasSize(remoteLogSegmentList.size());
        assertThat(remoteLogTablet.allRemoteLogSegments())
                .containsExactlyInAnyOrderElementsOf(remoteLogSegmentList);
        assertThat(remoteLogTablet.relevantRemoteLogSegments(0L))
                .containsExactlyInAnyOrderElementsOf(remoteLogSegmentList);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDelete(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        List<RemoteLogSegment> remoteLogSegmentList = createRemoteLogSegmentList(logTablet);
        remoteLogTablet.addAndDeleteLogSegments(remoteLogSegmentList, Collections.emptyList());
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap()).hasSize(5);
        assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(0);

        // try to delete two segments.
        RemoteLogSegment firstSegment = remoteLogSegmentList.get(0);
        RemoteLogSegment secondSegment = remoteLogSegmentList.get(1);
        remoteLogTablet.addAndDeleteLogSegments(
                Collections.emptyList(), Arrays.asList(firstSegment, secondSegment));
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap()).hasSize(3);
        assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(20);
        assertThat(remoteLogTablet.getRemoteLogEndOffset()).isEqualTo(OptionalLong.of(50));

        // delete all.
        remoteLogTablet.addAndDeleteLogSegments(Collections.emptyList(), remoteLogSegmentList);
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap()).isEmpty();
        assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(Long.MAX_VALUE);
        assertThat(remoteLogTablet.getRemoteLogEndOffset()).isEqualTo(OptionalLong.empty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAppendAndDelete(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        List<RemoteLogSegment> remoteLogSegmentList = createRemoteLogSegmentList(logTablet);

        remoteLogTablet.addAndDeleteLogSegments(
                remoteLogSegmentList.subList(0, 3), Collections.emptyList());
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap()).hasSize(3);
        assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(0);
        assertThat(remoteLogTablet.getRemoteLogEndOffset()).isEqualTo(OptionalLong.of(30));

        // delete first remote log segment and add another one remote log segments.
        remoteLogTablet.addAndDeleteLogSegments(
                Collections.singletonList(remoteLogSegmentList.get(3)),
                Collections.singletonList(remoteLogSegmentList.get(0)));
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap()).hasSize(3);
        assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(10);
        assertThat(remoteLogTablet.getRemoteLogEndOffset()).isEqualTo(OptionalLong.of(40));

        // delete all exist and append one. we will first add then delete.
        remoteLogTablet.addAndDeleteLogSegments(
                Collections.singletonList(remoteLogSegmentList.get(4)), remoteLogSegmentList);
        assertThat(remoteLogTablet.getIdToRemoteLogSegmentMap()).hasSize(0);
        assertThat(remoteLogTablet.getRemoteLogStartOffset()).isEqualTo(Long.MAX_VALUE);
        assertThat(remoteLogTablet.getRemoteLogEndOffset()).isEqualTo(OptionalLong.empty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTakeAndLoadSnapshot(boolean partitionTable) throws Exception {
        LogTablet logTablet = makeLogTabletAndAddSegments(partitionTable);
        RemoteLogTablet remoteLogTablet = buildRemoteLogTablet(logTablet);
        List<RemoteLogSegment> remoteLogSegmentList = createRemoteLogSegmentList(logTablet);
        remoteLogTablet.addAndDeleteLogSegments(remoteLogSegmentList, Collections.emptyList());
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
        remoteLogTablet.addAndDeleteLogSegments(remoteLogSegmentList, Collections.emptyList());

        // Get offset from 0.
        List<RemoteLogSegment> result = remoteLogTablet.relevantRemoteLogSegments(0L);
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
        remoteLogTablet.addAndDeleteLogSegments(
                Arrays.asList(
                        createLogSegmentWithMaxTimestamp(logTablet, 10, 0, 10),
                        createLogSegmentWithMaxTimestamp(logTablet, 20, 10, 20),
                        createLogSegmentWithMaxTimestamp(logTablet, 30, 20, 30),
                        createLogSegmentWithMaxTimestamp(logTablet, 40, 30, 40),
                        createLogSegmentWithMaxTimestamp(logTablet, 50, 40, 50)),
                Collections.emptyList());

        assertThat(remoteLogTablet.findSegmentByTimestamp(0L).remoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLogTablet.findSegmentByTimestamp(1L).remoteLogStartOffset()).isEqualTo(0L);
        assertThat(remoteLogTablet.findSegmentByTimestamp(10L).remoteLogStartOffset())
                .isEqualTo(0L);
        assertThat(remoteLogTablet.findSegmentByTimestamp(40L).remoteLogStartOffset())
                .isEqualTo(30L);
        assertThat(remoteLogTablet.findSegmentByTimestamp(50L).remoteLogStartOffset())
                .isEqualTo(40L);
        assertThat(remoteLogTablet.findSegmentByTimestamp(51L)).isNull();
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

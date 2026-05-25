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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.remote.RemoteLogSegment;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteLogManifest}. */
class RemoteLogManifestTest {

    private static final PhysicalTablePath PHYSICAL_TABLE_PATH =
            PhysicalTablePath.of(TablePath.of("db", "test_table"));
    private static final TableBucket TABLE_BUCKET = new TableBucket(1001, 0);
    private static final FsPath NEW_REMOTE_LOG_DIR = new FsPath("/new_remote_data_dir/log");

    @Test
    void testNewManifestSetsRemoteLogDirOnAllSegments() {
        UUID segmentId1 = UUID.randomUUID();
        UUID segmentId2 = UUID.randomUUID();

        List<RemoteLogSegment> segments =
                Arrays.asList(
                        RemoteLogSegment.Builder.builder()
                                .physicalTablePath(PHYSICAL_TABLE_PATH)
                                .tableBucket(TABLE_BUCKET)
                                .remoteLogSegmentId(segmentId1)
                                .remoteLogStartOffset(0)
                                .remoteLogEndOffset(9)
                                .maxTimestamp(1000L)
                                .segmentSizeInBytes(2850)
                                .remoteLogDir(null)
                                .build(),
                        RemoteLogSegment.Builder.builder()
                                .physicalTablePath(PHYSICAL_TABLE_PATH)
                                .tableBucket(TABLE_BUCKET)
                                .remoteLogSegmentId(segmentId2)
                                .remoteLogStartOffset(10)
                                .remoteLogEndOffset(19)
                                .maxTimestamp(2000L)
                                .segmentSizeInBytes(3200)
                                .remoteLogDir(null)
                                .build());

        RemoteLogManifest oldManifest =
                new RemoteLogManifest(PHYSICAL_TABLE_PATH, TABLE_BUCKET, segments, null);
        assertThat(oldManifest.getRemoteLogDir()).isNull();

        RemoteLogManifest newManifest = oldManifest.newManifest(NEW_REMOTE_LOG_DIR);

        assertThat(newManifest.getRemoteLogDir()).isEqualTo(NEW_REMOTE_LOG_DIR);
        assertThat(newManifest.getPhysicalTablePath()).isEqualTo(PHYSICAL_TABLE_PATH);
        assertThat(newManifest.getTableBucket()).isEqualTo(TABLE_BUCKET);

        List<RemoteLogSegment> newSegments = newManifest.getRemoteLogSegmentList();
        assertThat(newSegments).hasSize(2);

        RemoteLogSegment seg1 = newSegments.get(0);
        assertThat(seg1.remoteLogDir()).isEqualTo(NEW_REMOTE_LOG_DIR);
        assertThat(seg1.physicalTablePath()).isEqualTo(PHYSICAL_TABLE_PATH);
        assertThat(seg1.tableBucket()).isEqualTo(TABLE_BUCKET);
        assertThat(seg1.remoteLogSegmentId()).isEqualTo(segmentId1);
        assertThat(seg1.remoteLogStartOffset()).isEqualTo(0);
        assertThat(seg1.remoteLogEndOffset()).isEqualTo(9);
        assertThat(seg1.maxTimestamp()).isEqualTo(1000L);
        assertThat(seg1.segmentSizeInBytes()).isEqualTo(2850);

        RemoteLogSegment seg2 = newSegments.get(1);
        assertThat(seg2.remoteLogDir()).isEqualTo(NEW_REMOTE_LOG_DIR);
        assertThat(seg2.remoteLogSegmentId()).isEqualTo(segmentId2);
        assertThat(seg2.remoteLogStartOffset()).isEqualTo(10);
        assertThat(seg2.remoteLogEndOffset()).isEqualTo(19);
        assertThat(seg2.maxTimestamp()).isEqualTo(2000L);
        assertThat(seg2.segmentSizeInBytes()).isEqualTo(3200);
    }
}

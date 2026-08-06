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

package org.apache.fluss.server.utils;

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.remote.RemoteLogFetchInfoV2;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.remote.RemoteLogSegmentReference;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.PbFetchLogRespForBucket;
import org.apache.fluss.rpc.messages.PbRemoteLogFetchInfo;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.getFetchLogResultForBucket;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getCommitRemoteLogManifestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeCommitRemoteLogManifestRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for server RPC message conversions. */
class ServerRpcMessageUtilsTest {

    @Test
    void testRemoteLogManifestCasRequestRoundTrip() {
        TableBucket tableBucket = new TableBucket(1L, 0);
        CommitRemoteLogManifestData absent =
                CommitRemoteLogManifestData.v2Absent(
                        tableBucket, new FsPath("file:///remote/m1"), 0L, 10L, 1L, 3, 4);
        CommitRemoteLogManifestRequest absentRequest = makeCommitRemoteLogManifestRequest(absent);
        assertThat(getCommitRemoteLogManifestData(absentRequest)).isEqualTo(absent);
        assertThat(absentRequest.hasExpectedManifestPath()).isFalse();
        assertThat(absentRequest.hasExpectedManifestGeneration()).isFalse();
        assertThat(absentRequest.hasExpectedZkVersion()).isFalse();

        CommitRemoteLogManifestData present =
                CommitRemoteLogManifestData.v2Present(
                        tableBucket,
                        new FsPath("file:///remote/m2"),
                        0L,
                        20L,
                        2L,
                        new FsPath("file:///remote/m1"),
                        1L,
                        7,
                        3,
                        4);
        assertThat(getCommitRemoteLogManifestData(makeCommitRemoteLogManifestRequest(present)))
                .isEqualTo(present);

        assertThatThrownBy(
                        () -> getCommitRemoteLogManifestData(absentRequest.setExpectedZkVersion(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ABSENT");
    }

    @Test
    void testRemoteLogFetchInfoV2RoundTrip() {
        TablePath tablePath = TablePath.of("db", "table");
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath);
        TableBucket tableBucket = new TableBucket(1L, 0);
        RemoteLogSegment segment =
                RemoteLogSegment.Builder.builder()
                        .physicalTablePath(physicalTablePath)
                        .tableBucket(tableBucket)
                        .remoteLogSegmentId(UUID.randomUUID())
                        .remoteLogStartOffset(0L)
                        .remoteLogEndOffset(20L)
                        .maxTimestamp(1L)
                        .segmentSizeInBytes(100)
                        .build();
        RemoteLogSegmentReference reference = new RemoteLogSegmentReference(segment, 5L, 20L);
        FetchLogResultForBucket result =
                new FetchLogResultForBucket(
                        tableBucket,
                        new RemoteLogFetchInfoV2(
                                "file:///remote/tablet",
                                null,
                                Collections.singletonList(reference)),
                        20L);

        FetchLogResponse response =
                makeFetchLogResponse(Collections.singletonMap(tableBucket, result));
        PbFetchLogRespForBucket pbBucket = response.getTablesRespAt(0).getBucketsRespAt(0);

        assertThat(pbBucket.hasRemoteLogFetchInfo()).isFalse();
        assertThat(pbBucket.hasRemoteLogFetchInfoV2()).isTrue();

        FetchLogResultForBucket decoded =
                getFetchLogResultForBucket(tableBucket, tablePath, pbBucket);
        assertThat(decoded.remoteLogFetchInfo()).isNull();
        assertThat(decoded.remoteLogFetchInfoV2().activeReferences()).containsExactly(reference);

        PbFetchLogRespForBucket ambiguousBucket =
                pbBucket.setRemoteLogFetchInfo(
                        new PbRemoteLogFetchInfo().setRemoteLogTabletDir("file:///legacy"));
        assertThatThrownBy(
                        () -> getFetchLogResultForBucket(tableBucket, tablePath, ambiguousBucket))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both v0 and v1");
    }
}

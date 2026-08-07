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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import org.apache.fluss.server.entity.CommitRemoteLogManifestData;
import org.apache.fluss.server.entity.NotifyRemoteLogOffsetsData;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getCommitRemoteLogManifestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getNotifyRemoteLogOffsetsData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeCommitRemoteLogManifestRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyRemoteLogOffsetsRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for server RPC message conversions. */
class ServerRpcMessageUtilsTest {

    @Test
    void testRemoteLogManifestCasRequestRoundTrip() {
        TableBucket tableBucket = new TableBucket(1L, 0);
        CommitRemoteLogManifestData absent =
                CommitRemoteLogManifestData.v2CreateIfAbsent(
                        tableBucket, new FsPath("file:///remote/m1"), 0L, 10L, 10L, 1L, 3, 4);
        CommitRemoteLogManifestRequest absentRequest = makeCommitRemoteLogManifestRequest(absent);
        assertThat(getCommitRemoteLogManifestData(absentRequest)).isEqualTo(absent);
        assertThat(absentRequest.hasExpectedZkVersion()).isFalse();

        CommitRemoteLogManifestData present =
                CommitRemoteLogManifestData.v2CompareAndSet(
                        tableBucket, new FsPath("file:///remote/m2"), 0L, 20L, 20L, 2L, 7, 3, 4);
        assertThat(getCommitRemoteLogManifestData(makeCommitRemoteLogManifestRequest(present)))
                .isEqualTo(present);

        assertThat(
                        getCommitRemoteLogManifestData(absentRequest.setExpectedZkVersion(1))
                                .getExpectedZkVersion())
                .isEqualTo(1);
        assertThatThrownBy(
                        () ->
                                getCommitRemoteLogManifestData(
                                        absentRequest.setExpectedZkVersion(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wildcard");
    }

    @Test
    void testRemoteLogOffsetNotificationCarriesTieredFrontier() {
        TableBucket tableBucket = new TableBucket(1L, 0);
        NotifyRemoteLogOffsetsRequest v2Request =
                makeNotifyRemoteLogOffsetsRequest(tableBucket, Long.MAX_VALUE, -1L, 10L)
                        .setCoordinatorEpoch(3);
        NotifyRemoteLogOffsetsData v2Data = getNotifyRemoteLogOffsetsData(v2Request);
        assertThat(v2Data.getRemoteLogEndOffset()).isEqualTo(-1L);
        assertThat(v2Data.getHighestCopiedEndOffset()).isEqualTo(10L);

        v2Request.clearHighestCopiedEndOffset().setRemoteEndOffset(8L);
        assertThat(getNotifyRemoteLogOffsetsData(v2Request).getHighestCopiedEndOffset())
                .isEqualTo(8L);
    }
}

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

class RemoteLogMessageUtilsTest {

    @Test
    void testOldCommitRequestDefaultsCopyProgressToRemoteEnd() {
        CommitRemoteLogManifestRequest request =
                new CommitRemoteLogManifestRequest()
                        .setTableId(1L)
                        .setBucketId(0)
                        .setRemoteLogManifestPath("file:///manifest")
                        .setRemoteLogStartOffset(0L)
                        .setRemoteLogEndOffset(10L)
                        .setCoordinatorEpoch(1)
                        .setBucketLeaderEpoch(2);

        assertThat(getCommitRemoteLogManifestData(request).getHighestCopiedEndOffset())
                .isEqualTo(10L);
    }

    @Test
    void testCommitRequestCarriesIndependentCopyProgress() {
        CommitRemoteLogManifestData data =
                new CommitRemoteLogManifestData(
                        new TableBucket(1L, 0),
                        new FsPath("file:///manifest"),
                        Long.MAX_VALUE,
                        -1L,
                        20L,
                        1,
                        2);

        CommitRemoteLogManifestData restored =
                getCommitRemoteLogManifestData(makeCommitRemoteLogManifestRequest(data));

        assertThat(restored).isEqualTo(data);
    }

    @Test
    void testOldNotifyRequestDefaultsCopyProgressToRemoteEnd() {
        NotifyRemoteLogOffsetsRequest request =
                new NotifyRemoteLogOffsetsRequest()
                        .setTableId(1L)
                        .setBucketId(0)
                        .setRemoteStartOffset(0L)
                        .setRemoteEndOffset(10L)
                        .setCoordinatorEpoch(1);

        assertThat(getNotifyRemoteLogOffsetsData(request).getHighestCopiedEndOffset())
                .isEqualTo(10L);
    }

    @Test
    void testNotifyRequestCarriesIndependentCopyProgress() {
        NotifyRemoteLogOffsetsRequest request =
                makeNotifyRemoteLogOffsetsRequest(new TableBucket(1L, 0), Long.MAX_VALUE, -1L, 20L);
        request.setCoordinatorEpoch(1);
        NotifyRemoteLogOffsetsData restored = getNotifyRemoteLogOffsetsData(request);

        assertThat(restored.getHighestCopiedEndOffset()).isEqualTo(20L);
    }
}

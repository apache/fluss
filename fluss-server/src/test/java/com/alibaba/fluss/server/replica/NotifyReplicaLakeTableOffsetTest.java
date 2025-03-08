/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.replica;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import com.alibaba.fluss.server.entity.LakeBucketOffset;
import com.alibaba.fluss.server.entity.NotifyLakeTableOffsetData;

import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.record.TestData.DATA1_TABLE_ID;

/** Test for notify replica lakehouse data info. */
class NotifyReplicaLakeTableOffsetTest extends ReplicaTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testNotifyWithOutRemoteLog(boolean partitionedTable) throws Exception {
        TableBucket tb = makeTableBucket(partitionedTable);
        // make leader
        makeLogTableAsLeader(tb, partitionedTable);
        Replica replica = replicaManager.getReplicaOrException(tb);

        // now, notify lake table offset
        notifyAndVerify(tb, replica, 1, 0L, 20L);
        // notify again
        notifyAndVerify(tb, replica, 2, 20L, 30L);
    }

    private void notifyAndVerify(
            TableBucket tb, Replica replica, long snapshotId, long startOffset, long endOffset)
            throws Exception {
        NotifyLakeTableOffsetData notifyLakeTableOffsetData =
                getNotifyLakeTableOffset(tb, snapshotId, startOffset, endOffset);
        CompletableFuture<NotifyLakeTableOffsetResponse> future = new CompletableFuture<>();
        replicaManager.notifyLakeTableOffset(notifyLakeTableOffsetData, future::complete);
        future.get();
        verifyLakeTableOffset(replica, snapshotId, startOffset, endOffset);
    }

    private void verifyLakeTableOffset(
            Replica replica, long snapshotId, long startOffset, long endOffset) {
        AssertionsForClassTypes.assertThat(replica.getLogTablet().getLakeTableSnapshotId())
                .isEqualTo(snapshotId);
        AssertionsForClassTypes.assertThat(replica.getLogTablet().getLakeLogStartOffset())
                .isEqualTo(startOffset);
        AssertionsForClassTypes.assertThat(replica.getLogTablet().getLakeLogEndOffset())
                .isEqualTo(endOffset);
    }

    private TableBucket makeTableBucket(boolean partitionTable) {
        return makeTableBucket(DATA1_TABLE_ID, partitionTable);
    }

    private TableBucket makeTableBucket(long tableId, boolean partitionTable) {
        if (partitionTable) {
            return new TableBucket(tableId, 0L, 0);
        } else {
            return new TableBucket(tableId, 0);
        }
    }

    private NotifyLakeTableOffsetData getNotifyLakeTableOffset(
            TableBucket tableBucket, long snapshotId, long startOffset, long endOffset) {
        return new NotifyLakeTableOffsetData(
                1,
                Collections.singletonMap(
                        tableBucket, new LakeBucketOffset(snapshotId, startOffset, endOffset)));
    }
}

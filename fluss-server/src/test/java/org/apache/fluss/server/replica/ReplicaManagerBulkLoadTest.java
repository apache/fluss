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

package org.apache.fluss.server.replica;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.entity.PutKvResultForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import org.apache.fluss.server.tablet.bulkload.BulkLoadTargetMetadata;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadDataState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_ID_PK;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH_PK;
import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.zk.data.LeaderAndIsr.INITIAL_LEADER_EPOCH;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.assertj.core.api.Assertions.assertThat;

/** Bulk-load related tests for {@link ReplicaManager}. */
class ReplicaManagerBulkLoadTest extends ReplicaTestBase {

    private static final short PUT_KV_VERSION = 1;

    @Test
    void testNotifyLeaderAndIsrRejectsNonEmptyLoadingLeaderPerBucket() throws Exception {
        TableBucket filled = new TableBucket(DATA1_TABLE_ID_PK, 0);
        TableBucket empty = new TableBucket(DATA1_TABLE_ID_PK, 1);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, filled.getBucket());
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, empty.getBucket());

        writeKv(filled);

        // The same leader epoch keeps the pre-existing KV tablet in place, which is the only state
        // where local content can be observed at all.
        Map<TableBucket, NotifyLeaderAndIsrResultForBucket> results = new HashMap<>();
        for (NotifyLeaderAndIsrResultForBucket result :
                notifyReplicas(
                        fenceLoadingLeader(filled, INITIAL_LEADER_EPOCH, true),
                        fenceLoadingLeader(empty, INITIAL_LEADER_EPOCH, true))) {
            results.put(result.getTableBucket(), result);
        }
        assertThat(results.get(filled).getError().error())
                .isEqualTo(Errors.INVALID_BULK_LOAD_REQUEST);
        assertThat(results.get(filled).getErrorMessage()).contains("contains local data");
        // Installing a fence never enters ordinary LOADING recovery, so an empty sibling bucket
        // succeeds outright while its neighbour is rejected.
        assertThat(results.get(empty).failed())
                .as("the emptiness proof must be decided per bucket")
                .isFalse();
        assertThat(replicaManager.getReplicaOrException(empty).getBulkLoadTargetMetadata())
                .isNotNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNotifyLeaderAndIsrRejectsFollowerWithLocalLog(boolean requireEmptyTarget)
            throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID, 2);
        makeLogTableAsLeader(tb.getBucket());
        replicaManager
                .getReplicaOrException(tb)
                .appendRecordsToLeader(genMemoryLogRecordsByObject(DATA1), 1);
        assertThat(replicaManager.getReplicaOrException(tb).getLocalLogEndOffset()).isPositive();

        NotifyLeaderAndIsrResultForBucket result =
                notifyReplicas(
                                fenceLoadingFollower(
                                        tb, INITIAL_LEADER_EPOCH + 1, requireEmptyTarget))
                        .get(0);
        assertThat(result.getError().error()).isEqualTo(Errors.INVALID_BULK_LOAD_REQUEST);
        assertThat(result.getErrorMessage()).contains("contains local data");
    }

    @Test
    void testUnprovenLoadingLeaderPreservesLocalKvData() throws Exception {
        TableBucket tb = new TableBucket(DATA1_TABLE_ID_PK, 0);
        makeKvTableAsLeader(DATA1_TABLE_ID_PK, DATA1_TABLE_PATH_PK, tb.getBucket());

        writeKv(tb);

        // Ordinary role notifications can arrive before every holder is fenced. They must
        // preserve pre-existing data until the coordinator has decided whether Begin is allowed.
        assertThat(
                        notifyReplicas(fenceLoadingLeader(tb, INITIAL_LEADER_EPOCH, false))
                                .get(0)
                                .getErrorCode())
                .isEqualTo(Errors.INVALID_BULK_LOAD_REQUEST.code());
        assertThat(replicaManager.getReplicaOrException(tb).getLocalLogEndOffset()).isPositive();
    }

    private void writeKv(TableBucket tableBucket) throws Exception {
        CompletableFuture<List<PutKvResultForBucket>> put = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                20000,
                1,
                Collections.singletonMap(tableBucket, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)),
                null,
                MergeMode.DEFAULT,
                PUT_KV_VERSION,
                put::complete);
        assertThat(put.get()).hasSize(1);
    }

    private List<NotifyLeaderAndIsrResultForBucket> notifyReplicas(NotifyLeaderAndIsrData... data)
            throws Exception {
        CompletableFuture<List<NotifyLeaderAndIsrResultForBucket>> result =
                new CompletableFuture<>();
        replicaManager.becomeLeaderOrFollower(
                INITIAL_COORDINATOR_EPOCH, Arrays.asList(data), result::complete);
        return result.get();
    }

    private NotifyLeaderAndIsrData fenceLoadingLeader(
            TableBucket tableBucket, int leaderEpoch, boolean requireEmptyTarget) {
        return loadingNotify(
                PhysicalTablePath.of(DATA1_TABLE_PATH_PK),
                tableBucket,
                new LeaderAndIsr(
                        TABLET_SERVER_ID,
                        leaderEpoch,
                        Collections.singletonList(TABLET_SERVER_ID),
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        leaderEpoch),
                requireEmptyTarget);
    }

    private NotifyLeaderAndIsrData fenceLoadingFollower(
            TableBucket tableBucket, int leaderEpoch, boolean requireEmptyTarget) {
        return loadingNotify(
                PhysicalTablePath.of(DATA1_TABLE_PATH),
                tableBucket,
                new LeaderAndIsr(
                        TABLET_SERVER_ID + 1,
                        leaderEpoch,
                        Arrays.asList(TABLET_SERVER_ID, TABLET_SERVER_ID + 1),
                        Collections.emptyList(),
                        INITIAL_COORDINATOR_EPOCH,
                        leaderEpoch),
                requireEmptyTarget);
    }

    private NotifyLeaderAndIsrData loadingNotify(
            PhysicalTablePath physicalTablePath,
            TableBucket tableBucket,
            LeaderAndIsr leaderAndIsr,
            boolean requireEmptyTarget) {
        return new NotifyLeaderAndIsrData(
                physicalTablePath,
                tableBucket,
                leaderAndIsr.isr(),
                leaderAndIsr,
                new BulkLoadTargetMetadata(tableBucket, 1, BulkLoadDataState.LOADING),
                requireEmptyTarget);
    }
}

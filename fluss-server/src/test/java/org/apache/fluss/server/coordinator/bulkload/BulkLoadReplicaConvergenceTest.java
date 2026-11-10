/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.bulkload;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.metadata.BulkLoadHandle;
import org.apache.fluss.metadata.BulkLoadState;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.coordinator.TestCoordinatorChannelManager;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadMetadataStore.Versioned;
import org.apache.fluss.server.coordinator.bulkload.BulkLoadReplicaConvergence.ReplicaConvergenceContext;
import org.apache.fluss.server.metadata.TabletServerResource;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.ServerProtocolVersion;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTarget;
import org.apache.fluss.server.zk.data.bulkload.BulkLoadTransaction;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Current-leader restoration requests and complete round ownership. */
class BulkLoadReplicaConvergenceTest {
    @ParameterizedTest
    @CsvSource({"true,1,2", "false,1,2", "false,0,2", "false,2,2", "false,1,0", "false,1,1"})
    void testSharedRoundWaitsForEverySendAndChecksServerSessions(
            boolean notReady, long firstSessionId, long secondSessionId) throws Exception {
        TablePath table = TablePath.of("db", "shared");
        BulkLoadTransaction transaction =
                new BulkLoadTransaction(
                        new BulkLoadHandle(table, 41L, "550e8400-e29b-41d4-a716-446655440000"),
                        BulkLoadState.COMMITTING,
                        "alice",
                        "USER",
                        Arrays.asList(
                                new BulkLoadTarget(
                                        PhysicalTablePath.of(table, "p2"),
                                        72L,
                                        "file:///warehouse",
                                        1,
                                        1,
                                        new long[] {8L}),
                                new BulkLoadTarget(
                                        PhysicalTablePath.of(table, "p1"),
                                        71L,
                                        "file:///warehouse",
                                        1,
                                        1,
                                        new long[] {7L})),
                        Long.MAX_VALUE,
                        "a",
                        null,
                        null);
        Map<TableBucket, Versioned<LeaderAndIsr>> leaders = new HashMap<>();
        Map<Integer, Versioned<TabletServerRegistration>> holders = new HashMap<>();
        java.util.List<Versioned<? extends TableAssignment>> assignments =
                new java.util.ArrayList<>();
        for (int id = 1; id <= 2; id++) {
            TableBucket bucket = new TableBucket(41L, 70L + id, 0);
            leaders.put(
                    bucket,
                    new Versioned<>(
                            new LeaderAndIsr(id, 3), ZkData.LeaderAndIsrZNode.path(bucket), 5, 0));
            assignments.add(
                    new Versioned<>(
                            new PartitionAssignment(
                                    41L, Collections.singletonMap(0, BucketAssignment.of(id))),
                            ZkData.PartitionIdZNode.path(70L + id),
                            2,
                            0));
            holders.put(
                    id,
                    new Versioned<>(
                            new TabletServerRegistration(
                                    "rack",
                                    Collections.singletonList(
                                            new Endpoint("localhost", 20000 + id, "INTERNAL")),
                                    1L,
                                    TabletServerResource.unknown(),
                                    ServerProtocolVersion.CURRENT),
                            ZkData.ServerIdZNode.path(id),
                            0,
                            id));
        }
        Map<Integer, NotifyLeaderAndIsrRequest> sent = new HashMap<>();
        Map<Integer, BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable>> callbacks =
                new HashMap<>();
        TestCoordinatorChannelManager channels =
                new TestCoordinatorChannelManager() {
                    @Override
                    public void sendBucketLeaderAndIsrRequest(
                            int server,
                            NotifyLeaderAndIsrRequest request,
                            BiConsumer<NotifyLeaderAndIsrResponse, ? super Throwable> callback) {
                        sent.put(server, request);
                        callbacks.put(server, callback);
                    }
                };
        try {
            CompletableFuture<Void> result =
                    new BulkLoadReplicaConvergence(channels)
                            .apply(
                                    transaction,
                                    new ReplicaConvergenceContext(
                                            assignments, leaders, holders, 0L),
                                    3);
            assertThat(sent).hasSize(2);
            assertThat(sent.get(1).getNotifyBucketsLeaderReqsList().get(0).getRestoreSnapshotId())
                    .isEqualTo(7L);
            assertThat(sent.get(2).getNotifyBucketsLeaderReqsList().get(0).getRestoreSnapshotId())
                    .isEqualTo(8L);
            NotifyLeaderAndIsrResponse pending = new NotifyLeaderAndIsrResponse();
            if (firstSessionId != 0) {
                pending.setServerSessionId(firstSessionId);
            }
            PbNotifyLeaderAndIsrRespForBucket first =
                    new PbNotifyLeaderAndIsrRespForBucket()
                            .setTableBucket(
                                    sent.get(1)
                                            .getNotifyBucketsLeaderReqsList()
                                            .get(0)
                                            .getTableBucket());
            if (notReady) {
                first.setErrorCode(Errors.REQUEST_TIME_OUT.code());
            }
            pending.addAllNotifyBucketsLeaderResps(Collections.singletonList(first));
            callbacks.get(1).accept(pending, null);
            assertThat(result).isNotDone();
            NotifyLeaderAndIsrResponse ready = new NotifyLeaderAndIsrResponse();
            if (secondSessionId != 0) {
                ready.setServerSessionId(secondSessionId);
            }
            ready.addAllNotifyBucketsLeaderResps(
                    Collections.singletonList(
                            new PbNotifyLeaderAndIsrRespForBucket()
                                    .setTableBucket(
                                            sent.get(2)
                                                    .getNotifyBucketsLeaderReqsList()
                                                    .get(0)
                                                    .getTableBucket())));
            callbacks.get(2).accept(ready, null);
            if (!notReady && firstSessionId == 1 && secondSessionId == 2) {
                assertThat(result).isCompleted();
            } else {
                assertThat(result).isCompletedExceptionally();
            }
        } finally {
            channels.close();
        }
    }
}

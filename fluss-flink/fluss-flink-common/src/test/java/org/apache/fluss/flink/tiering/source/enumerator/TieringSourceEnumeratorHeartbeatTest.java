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

package org.apache.fluss.flink.tiering.source.enumerator;

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.client.tiering.TieringSplitGenerator;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.NetworkException;
import org.apache.fluss.flink.sink.testutils.TestAdminAdapter;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.rpc.messages.PbHeartbeatReqForTable;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.utils.clock.ManualClock;
import org.apache.fluss.utils.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests how {@link TieringSourceEnumerator} handles failed coordinator heartbeats. */
class TieringSourceEnumeratorHeartbeatTest {

    private static final long TABLE_ID = 7L;

    private final ManualClock clock = new ManualClock();
    private boolean splitGenerationFails = true;

    private final Deque<CompletableFuture<LakeTieringHeartbeatResponse>> responses =
            new ArrayDeque<>();
    private final List<LakeTieringHeartbeatRequest> requests = new ArrayList<>();

    @Test
    void testRetriableFailureKeepsUnacknowledgedReports() throws Exception {
        try (FlussMockSplitEnumeratorContext<FlinkTieringSplit> context =
                new FlussMockSplitEnumeratorContext<>(1)) {
            TieringSourceEnumerator enumerator = startEnumerator(context, Duration.ofMinutes(1));

            // Split generation fails, which leaves a report to deliver.
            responses.add(CompletableFuture.completedFuture(grant(1)));
            enumerator.handleSplitRequest(0, null);

            responses.add(networkFailure());
            enumerator.handleSplitRequest(0, null);

            // A new coordinator acknowledges the retried report and then grants a table.
            LakeTieringHeartbeatResponse acknowledged = response(2);
            acknowledged.addFailedTableResp().setTableId(TABLE_ID);
            responses.add(CompletableFuture.completedFuture(acknowledged));
            enumerator.handleSplitRequest(0, null);
            responses.add(CompletableFuture.completedFuture(grant(2)));
            enumerator.handleSplitRequest(0, null);
            responses.add(CompletableFuture.completedFuture(response(2)));
            enumerator.handleSplitRequest(0, null);

            assertThat(requests.get(2).getFailedTablesList()).hasSize(1);
            assertThat(requests.get(3).getFailedTablesList()).hasSize(1);
            assertThat(requests.get(4).getFailedTablesList()).isEmpty();
            assertThat(requests.get(5).getFailedTableAt(0).getCoordinatorEpoch()).isEqualTo(2);
        }
    }

    @Test
    void testFailsOnFirstRetriableFailureByDefault() throws Exception {
        assertHeartbeatFails(Duration.ZERO, networkFailure());
    }

    @Test
    void testNonRetriableFailureIsNotTolerated() throws Exception {
        assertHeartbeatFails(
                Duration.ofMinutes(1),
                FutureUtils.completedExceptionally(new IllegalStateException("broken")));
    }

    @Test
    void testFailsOnceToleranceElapses() throws Exception {
        try (FlussMockSplitEnumeratorContext<FlinkTieringSplit> context =
                new FlussMockSplitEnumeratorContext<>(1)) {
            TieringSourceEnumerator enumerator = startEnumerator(context, Duration.ofMillis(1));
            responses.add(networkFailure());
            enumerator.handleSplitRequest(0, null);

            clock.advanceTime(Duration.ofMillis(1));
            responses.add(networkFailure());
            assertThatThrownBy(() -> enumerator.handleSplitRequest(0, null))
                    .hasRootCauseInstanceOf(NetworkException.class);
        }
    }

    @Test
    void testNoTableIsTrackedUnderThePreviousCoordinatorEpoch() throws Exception {
        try (FlussMockSplitEnumeratorContext<FlinkTieringSplit> context =
                new FlussMockSplitEnumeratorContext<>(1)) {
            TieringSourceEnumerator enumerator = startEnumerator(context, Duration.ofMinutes(1));
            splitGenerationFails = false;
            responses.add(CompletableFuture.completedFuture(grant(1)));
            enumerator.handleSplitRequest(0, null);

            // The coordinator changes while that round is still running.
            responses.add(CompletableFuture.completedFuture(grant(2, TABLE_ID + 1)));
            enumerator.handleSplitRequest(0, null);
            responses.add(CompletableFuture.completedFuture(response(2)));
            enumerator.handleSplitRequest(0, null);
            enumerator.handleSourceEvent(0, new FailedTieringEvent(TABLE_ID, "fenced"));
            LakeTieringHeartbeatResponse drained = response(2);
            drained.addFailedTableResp().setTableId(TABLE_ID);
            responses.add(CompletableFuture.completedFuture(drained));
            enumerator.handleSplitRequest(0, null);
            responses.add(CompletableFuture.completedFuture(response(2)));
            enumerator.handleSplitRequest(0, null);

            assertThat(requests.get(3).hasRequestTable()).isFalse();
            assertThat(requests.get(3).getTieringTablesList())
                    .extracting(PbHeartbeatReqForTable::getTableId)
                    .containsExactly(TABLE_ID);
            assertThat(requests.get(5).isRequestTable()).isTrue();
        }
    }

    private void assertHeartbeatFails(
            Duration tolerance, CompletableFuture<LakeTieringHeartbeatResponse> failure)
            throws Exception {
        try (FlussMockSplitEnumeratorContext<FlinkTieringSplit> context =
                new FlussMockSplitEnumeratorContext<>(1)) {
            TieringSourceEnumerator enumerator = startEnumerator(context, tolerance);
            responses.add(failure);
            assertThatThrownBy(() -> enumerator.handleSplitRequest(0, null))
                    .hasMessageContaining("Failed to wait heartbeat response");
        }
    }

    private TieringSourceEnumerator startEnumerator(
            FlussMockSplitEnumeratorContext<FlinkTieringSplit> context, Duration tolerance) {
        Configuration lakeTieringConfig = new Configuration();
        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_HEARTBEAT_FAILURE_TOLERANCE, tolerance);
        TieringSourceEnumerator enumerator =
                new TieringSourceEnumerator(
                        new Configuration(), lakeTieringConfig, context, null, 60_000L);
        responses.add(CompletableFuture.completedFuture(response(1)));
        enumerator.setClock(clock);
        enumerator.start(scriptedGateway(), admin(), splitGenerator());
        context.registerSourceReader(0, 0, "localhost");
        return enumerator;
    }

    private static Admin admin() {
        return new TestAdminAdapter() {
            @Override
            public CompletableFuture<TableInfo> getTableInfo(TablePath tablePath) {
                return CompletableFuture.completedFuture(DATA1_TABLE_INFO);
            }
        };
    }

    private TieringSplitGenerator splitGenerator() {
        return new TieringSplitGenerator(null) {
            @Override
            public List<TieringSplit> generateTableSplits(TableInfo tableInfo) {
                if (splitGenerationFails) {
                    throw new IllegalStateException("split generation failed");
                }
                return Collections.singletonList(
                        new TieringLogSplit(
                                tableInfo.getTablePath(),
                                new TableBucket(TABLE_ID, 0),
                                null,
                                0L,
                                1L));
            }
        };
    }

    private CoordinatorGateway scriptedGateway() {
        return new TestCoordinatorGateway() {
            @Override
            public CompletableFuture<LakeTieringHeartbeatResponse> lakeTieringHeartbeat(
                    LakeTieringHeartbeatRequest request) {
                requests.add(request);
                return responses.remove();
            }
        };
    }

    private static LakeTieringHeartbeatResponse response(int coordinatorEpoch) {
        return new LakeTieringHeartbeatResponse().setCoordinatorEpoch(coordinatorEpoch);
    }

    private static LakeTieringHeartbeatResponse grant(int coordinatorEpoch) {
        return grant(coordinatorEpoch, TABLE_ID);
    }

    private static LakeTieringHeartbeatResponse grant(int coordinatorEpoch, long tableId) {
        LakeTieringHeartbeatResponse response = response(coordinatorEpoch);
        response.setTieringTable()
                .setTableId(tableId)
                .setTieringEpoch(3L)
                .setTablePath()
                .setDatabaseName("db")
                .setTableName("t");
        return response;
    }

    private static CompletableFuture<LakeTieringHeartbeatResponse> networkFailure() {
        return FutureUtils.completedExceptionally(new NetworkException("coordinator disconnected"));
    }
}

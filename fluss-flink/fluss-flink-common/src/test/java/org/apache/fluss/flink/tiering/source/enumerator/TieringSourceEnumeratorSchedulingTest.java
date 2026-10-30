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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSplit;
import org.apache.fluss.client.tiering.TieringSplitGenerator;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.source.TieringSourceOptions;
import org.apache.fluss.flink.tiering.source.metrics.TieringMetrics;
import org.apache.fluss.flink.tiering.source.split.FlinkTieringSplit;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatRequest;
import org.apache.fluss.rpc.messages.LakeTieringHeartbeatResponse;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSplitEnumeratorMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for split assignment and table scheduling in {@link TieringSourceEnumerator}. */
class TieringSourceEnumeratorSchedulingTest {

    @ParameterizedTest
    @ValueSource(strings = {"default", "large"})
    void testTieringGroupIncludedInEveryHeartbeat(String group) throws Throwable {
        Configuration configuration = new Configuration();
        if (!group.equals(TieringSourceOptions.TIERING_GROUP.defaultValue())) {
            configuration.set(TieringSourceOptions.TIERING_GROUP, group);
        }
        try (Fixture fixture = new Fixture(1, new MetricListener(), configuration)) {
            configuration.set(TieringSourceOptions.TIERING_GROUP, "changed-after-start");
            fixture.addTable(1, false);
            fixture.requestTable();
            fixture.context.runPeriodicCallable(0);
            assertThat(fixture.requests.get(2).getTieringTablesList())
                    .singleElement()
                    .satisfies(table -> assertThat(table.getTableId()).isEqualTo(1));

            fixture.enumerator.handleSourceEvent(0, new FinishedTieringEvent(1));
            fixture.context.runNextOneTimeCallable();
            assertThat(fixture.requests.get(3).getFinishedTablesList())
                    .singleElement()
                    .satisfies(table -> assertThat(table.getTableId()).isEqualTo(1));

            fixture.addTable(2, false);
            fixture.requestTable();
            fixture.enumerator.handleSourceEvent(0, new FailedTieringEvent(2, "test failure"));
            fixture.context.runNextOneTimeCallable();
            assertThat(fixture.requests.get(5).getFailedTablesList())
                    .singleElement()
                    .satisfies(table -> assertThat(table.getTableId()).isEqualTo(2));

            fixture.addTable(3, false);
            fixture.requestTable();
            fixture.closeEnumerator();
            assertThat(fixture.requests.get(7).getFailedTablesList())
                    .singleElement()
                    .satisfies(table -> assertThat(table.getTableId()).isEqualTo(3));

            assertThat(fixture.requests).hasSize(8);
            assertThat(fixture.claims()).hasSize(3);
            assertThat(fixture.requests)
                    .extracting(LakeTieringHeartbeatRequest::getTieringGroup)
                    .containsOnly(group);
        }
    }

    @Test
    void testPendingSplitsAssignedInOrder() throws Exception {
        try (Fixture fixture = new Fixture()) {
            assertThat(fixture.readersAwaitingSplitRatio()).isEqualTo(1.0);
            List<FlinkTieringSplit> splits = new ArrayList<>();
            for (int bucket = 0; bucket < 4; bucket++) {
                splits.add(
                        new FlinkTieringSplit(
                                new TieringLogSplit(
                                        TablePath.of("db", "table"),
                                        new TableBucket(1, bucket),
                                        null,
                                        0,
                                        1)));
            }
            fixture.enumerator.addSplitsBack(splits.subList(0, 2), 0);
            // Returned splits join the tail of the remaining queue.
            fixture.enumerator.addSplitsBack(splits.subList(2, splits.size()), 0);
            for (int i = 2; i < splits.size(); i++) {
                fixture.requestTable();
            }

            List<FlinkTieringSplit> assignedSplits =
                    fixture.context.getSplitsAssignmentSequence().stream()
                            .flatMap(assignment -> assignment.assignment().get(0).stream())
                            .collect(Collectors.toList());
            assertThat(assignedSplits).containsExactlyElementsOf(splits);
            assertThat(fixture.claims()).isEmpty();
            assertThat(fixture.readersAwaitingSplitRatio()).isZero();

            // Once drained, another request polls for a new table without reassigning old splits.
            fixture.requestTable();
            assertThat(fixture.claims()).hasSize(1);
            assertThat(fixture.context.getSplitsAssignmentSequence()).hasSize(splits.size());
            assertThat(fixture.readersAwaitingSplitRatio()).isEqualTo(1.0);
        }
    }

    @Test
    void testEmptyTablesShareContinuationAndAdvance() throws Throwable {
        try (Fixture fixture = new Fixture()) {
            fixture.addTable(1, true);
            fixture.addTable(2, true);
            fixture.addTable(3, true);
            fixture.addTable(4, false);

            fixture.context.runPeriodicCallable(0);
            assertThat(fixture.timer.getActiveNonPeriodicScheduledTask()).hasSize(1);
            ScheduledFuture<?> continuation =
                    fixture.timer.getActiveNonPeriodicScheduledTask().iterator().next();
            assertThat(continuation.getDelay(TimeUnit.MILLISECONDS)).isEqualTo(1000L);

            fixture.requestTable();
            assertThat(fixture.timer.getActiveNonPeriodicScheduledTask())
                    .containsExactly(continuation);
            assertThat(fixture.claims()).hasSize(2);
            assertThat(fixture.context.getOneTimeCallables()).isEmpty();

            fixture.runContinuation();
            fixture.runContinuation();

            List<LakeTieringHeartbeatRequest> claims = fixture.claims();
            assertThat(claims).hasSize(4);
            for (int tableId = 1; tableId <= 3; tableId++) {
                assertThat(claims.get(tableId).getFinishedTablesList())
                        .singleElement()
                        .extracting(table -> table.getTableId(), table -> table.getTieringEpoch())
                        .containsExactly((long) tableId, tableId + 100L);
            }
            assertThat(fixture.context.getSplitsAssignmentSequence())
                    .singleElement()
                    .satisfies(
                            assignment ->
                                    assertThat(assignment.assignment().get(0))
                                            .extracting(
                                                    split ->
                                                            split.unwrap()
                                                                    .getTableBucket()
                                                                    .getTableId())
                                            .containsExactly(4L));
            assertThat(fixture.context.getOneTimeCallables()).isEmpty();
        }
    }

    @Test
    void testLastEmptyTableStopsContinuationButPeriodicStillPolls() throws Throwable {
        try (Fixture fixture = new Fixture()) {
            fixture.addTable(1, true);
            fixture.requestTable();
            fixture.runContinuation();

            assertThat(fixture.claims()).hasSize(2);
            assertThat(fixture.claims().get(1).getFinishedTablesList())
                    .singleElement()
                    .satisfies(table -> assertThat(table.getTableId()).isEqualTo(1));
            assertThat(fixture.timer.getActiveNonPeriodicScheduledTask()).isEmpty();
            assertThat(fixture.context.getOneTimeCallables()).isEmpty();

            fixture.addTable(2, false);
            fixture.context.runPeriodicCallable(0);
            assertThat(fixture.claims()).hasSize(3);
            assertThat(fixture.context.getSplitsAssignmentSequence())
                    .singleElement()
                    .satisfies(
                            assignment ->
                                    assertThat(assignment.assignment().get(0))
                                            .extracting(
                                                    split ->
                                                            split.unwrap()
                                                                    .getTableBucket()
                                                                    .getTableId())
                                            .containsExactly(2L));
        }
    }

    @Test
    void testGenerationFailureDoesNotScheduleContinuation() throws Throwable {
        try (Fixture fixture = new Fixture()) {
            TableInfo table = fixture.addTable(1, true);
            when(fixture.generator.generateTableSplits(table))
                    .thenThrow(new IOException("metadata unavailable"));

            fixture.requestTable();
            assertThat(fixture.readersAwaitingSplitRatio()).isEqualTo(1.0);

            // Failures must not drive immediate retries of a table re-queued by the coordinator.
            assertThat(fixture.timer.getActiveNonPeriodicScheduledTask()).isEmpty();
            assertThat(fixture.context.getOneTimeCallables()).isEmpty();

            fixture.context.runPeriodicCallable(0);
            LakeTieringHeartbeatRequest heartbeat = fixture.claims().get(1);
            assertThat(heartbeat.getFinishedTablesList()).isEmpty();
            assertThat(heartbeat.getFailedTablesList())
                    .singleElement()
                    .satisfies(reported -> assertThat(reported.getTableId()).isEqualTo(1));
        }
    }

    @Test
    void testFailOverPreventsContinuationClaim() throws Throwable {
        try (Fixture fixture = new Fixture(2)) {
            assertThat(fixture.readersAwaitingSplitRatio()).isEqualTo(0.5);
            fixture.registerReader(1, 0);
            assertThat(fixture.readersAwaitingSplitRatio()).isEqualTo(1.0);
            fixture.addTable(1, true);
            fixture.requestTable();

            // Reader 0 restarts while reader 1 is still on attempt 0, so failover is in progress.
            fixture.registerReader(0, 1);
            assertThat(fixture.readersAwaitingSplitRatio()).isEqualTo(1.0);

            // The failover guard must leave the next table to the periodic poll.
            fixture.fireContinuation();
            assertThat(fixture.claims()).hasSize(1);
            assertThat(fixture.timer.getActiveNonPeriodicScheduledTask()).isEmpty();
            assertThat(fixture.context.getOneTimeCallables()).isEmpty();

            // Once all readers reach the same attempt, the periodic poll claims the next table.
            fixture.addTable(2, true);
            fixture.registerReader(1, 1);
            fixture.context.runPeriodicCallable(0);

            assertThat(fixture.claims()).hasSize(2);
            assertThat(fixture.timer.getActiveNonPeriodicScheduledTask()).hasSize(1);
        }
    }

    @Test
    void testClosePreventsScheduledContinuation() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.addTable(1, true);
            fixture.requestTable();
            assertThat(fixture.timer.getActiveNonPeriodicScheduledTask()).hasSize(1);

            fixture.closeEnumerator();
            // Even if the timer still fires, the closed guard prevents any further claim.
            fixture.fireContinuation();

            assertThat(fixture.context.getOneTimeCallables()).isEmpty();
            assertThat(fixture.claims()).hasSize(1);
        }
    }

    @ParameterizedTest(name = "withInFlightTable={0}")
    @ValueSource(booleans = {false, true})
    void testCloseReportsPendingTables(boolean withInFlightTable) throws Exception {
        try (Fixture fixture = new Fixture()) {
            if (withInFlightTable) {
                // Table 1 produces a split, so it is still in flight when the enumerator closes.
                fixture.addTable(1, false);
                fixture.requestTable();

                // Table 2 produces no split. It is marked finished while the heartbeat that
                // requested it only carried a snapshot taken beforehand, so it is still pending at
                // close time: both maps have to be reported together in this single heartbeat.
                fixture.addTable(2, true);
                fixture.requestTable();
            } else {
                // The empty table is marked finished as soon as it produces no splits, but that
                // completion is only carried back by the delayed continuation poll, which never
                // runs once the enumerator is closed. Closing has to flush it explicitly,
                // otherwise the coordinator never learns about it and the table remains in the
                // Tiering state until the tiering service timeout expires.
                fixture.addTable(1, true);
                fixture.requestTable();
            }

            fixture.closeEnumerator();

            long expectedFinishedTableId = withInFlightTable ? 2L : 1L;
            long expectedTieringEpoch = expectedFinishedTableId + 100;
            assertThat(fixture.finishedTableReports())
                    .singleElement()
                    .satisfies(
                            request -> {
                                assertThat(request.getFinishedTablesList())
                                        .singleElement()
                                        .extracting(
                                                table -> table.getTableId(),
                                                table -> table.getTieringEpoch())
                                        .containsExactly(
                                                expectedFinishedTableId, expectedTieringEpoch);
                                if (withInFlightTable) {
                                    assertThat(request.getFailedTablesList())
                                            .singleElement()
                                            .extracting(
                                                    table -> table.getTableId(),
                                                    table -> table.getTieringEpoch())
                                            .containsExactly(1L, 101L);
                                } else {
                                    assertThat(request.getFailedTablesList()).isEmpty();
                                }
                            });

            // A repeated close is a no-op: it must not send a second heartbeat.
            int heartbeatCount = fixture.heartbeatCount();
            fixture.closeEnumerator();
            assertThat(fixture.heartbeatCount()).isEqualTo(heartbeatCount);
        }
    }

    @ParameterizedTest(name = "heartbeatFails={0}")
    @ValueSource(booleans = {false, true})
    void testCloseReleasesResources(boolean heartbeatFails) throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.addTable(1, true);
            fixture.requestTable();
            assertThat(fixture.readersAwaitingSplitRatio()).isEqualTo(1.0);
            RpcClient rpcClient = mock(RpcClient.class);
            Connection connection = mock(Connection.class);
            Field rpcClientField = TieringSourceEnumerator.class.getDeclaredField("rpcClient");
            rpcClientField.setAccessible(true);
            rpcClientField.set(fixture.enumerator, rpcClient);
            Field connectionField = TieringSourceEnumerator.class.getDeclaredField("connection");
            connectionField.setAccessible(true);
            connectionField.set(fixture.enumerator, connection);
            if (heartbeatFails) {
                IOException failure = new IOException("coordinator unavailable");
                CompletableFuture<LakeTieringHeartbeatResponse> response =
                        new CompletableFuture<>();
                response.completeExceptionally(failure);
                fixture.respondWith(response);

                assertThatThrownBy(fixture::closeEnumerator)
                        .isInstanceOf(FlinkRuntimeException.class)
                        .hasRootCause(failure);
            } else {
                fixture.closeEnumerator();
            }

            // A repeated close is a no-op: no further heartbeat, and the resources are released
            // once (the verifications below use the default times(1)).
            int heartbeatCount = fixture.heartbeatCount();
            fixture.closeEnumerator();
            assertThat(fixture.heartbeatCount()).isEqualTo(heartbeatCount);
            verify(rpcClient).close();
            verify(fixture.admin).close();
            verify(connection).close();

            // Recovery reuses the parent metric group, but must report the new enumerator's state.
            try (Fixture restored = new Fixture(1, fixture.context.metricListener)) {
                restored.addTable(2, false);
                restored.requestTable();
                assertThat(restored.readersAwaitingSplitRatio()).isZero();
            }
        }
    }

    private static TieringSplit splitFor(TableInfo table) {
        return new TieringLogSplit(
                table.getTablePath(), new TableBucket(table.getTableId(), 0), null, 0, 1);
    }

    private static class Fixture implements AutoCloseable {
        private final TestingContext context;
        private final ManuallyTriggeredScheduledExecutorService timer =
                new ManuallyTriggeredScheduledExecutorService();
        private final Admin admin = mock(Admin.class);
        private final CoordinatorGateway gateway = mock(CoordinatorGateway.class);
        private final TieringSplitGenerator generator = mock(TieringSplitGenerator.class);
        private final Queue<LakeTieringHeartbeatResponse> tables = new ArrayDeque<>();
        private final List<LakeTieringHeartbeatRequest> requests = new ArrayList<>();
        private final TieringSourceEnumerator enumerator;

        private Fixture() {
            this(1);
        }

        private Fixture(int parallelism) {
            this(parallelism, new MetricListener());
        }

        private Fixture(int parallelism, MetricListener metricListener) {
            this(parallelism, metricListener, new Configuration());
        }

        private Fixture(
                int parallelism, MetricListener metricListener, Configuration configuration) {
            this.context = new TestingContext(parallelism, metricListener);
            when(gateway.lakeTieringHeartbeat(any()))
                    .thenAnswer(invocation -> respondToHeartbeat(invocation.getArgument(0)));
            enumerator =
                    new TieringSourceEnumerator(
                            configuration,
                            new Configuration(),
                            context,
                            new TestingLakeTieringFactory(),
                            30_000L,
                            timer);
            enumerator.start(gateway, admin, generator);
            registerReader(0, 0);
        }

        private void registerReader(int subtaskId, int attemptNumber) {
            context.registerSourceReader(subtaskId, attemptNumber, "localhost-" + subtaskId);
            enumerator.addReader(subtaskId);
        }

        private double readersAwaitingSplitRatio() {
            return context.metricListener
                    .<Double>getGauge(
                            "enumerator",
                            TieringMetrics.FLUSS_METRIC_GROUP,
                            TieringMetrics.TIERING_SERVICE_GROUP,
                            MetricNames.TIERING_SERVICE_READERS_AWAITING_SPLIT_RATIO)
                    .get()
                    .getValue();
        }

        private CompletableFuture<LakeTieringHeartbeatResponse> respondToHeartbeat(
                LakeTieringHeartbeatRequest request) {
            requests.add(request);
            LakeTieringHeartbeatResponse response =
                    request.hasRequestTable() && request.isRequestTable() ? tables.poll() : null;
            return CompletableFuture.completedFuture(
                    response == null
                            ? new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1)
                            : response);
        }

        private void requestTable() {
            enumerator.handleSplitRequest(0, "localhost");
        }

        private TableInfo addTable(long id, boolean empty) throws Exception {
            TablePath path = TablePath.of("db", "table_" + id);
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                            .distributedBy(1)
                            .build();
            TableInfo info = TableInfo.of(path, id, 0, descriptor, null, 0, 0);
            when(admin.getTableInfo(path)).thenReturn(CompletableFuture.completedFuture(info));
            when(generator.generateTableSplits(info))
                    .thenReturn(
                            empty
                                    ? Collections.emptyList()
                                    : Collections.singletonList(splitFor(info)));
            LakeTieringHeartbeatResponse response =
                    new LakeTieringHeartbeatResponse().setCoordinatorEpoch(1);
            response.setTieringTable()
                    .setTableId(id)
                    .setTieringEpoch(id + 100)
                    .setTablePath()
                    .setDatabaseName(path.getDatabaseName())
                    .setTableName(path.getTableName());
            tables.add(response);
            return info;
        }

        private List<LakeTieringHeartbeatRequest> claims() {
            return requests.stream()
                    .filter(request -> request.hasRequestTable() && request.isRequestTable())
                    .collect(Collectors.toList());
        }

        private List<LakeTieringHeartbeatRequest> finishedTableReports() {
            return requests.stream()
                    .filter(request -> !request.getFinishedTablesList().isEmpty())
                    .collect(Collectors.toList());
        }

        private void fireContinuation() {
            assertThat(timer.getActiveNonPeriodicScheduledTask()).hasSize(1);
            assertThat(context.getOneTimeCallables()).isEmpty();
            timer.triggerNonPeriodicScheduledTasks();
            // The timer only hops back to the coordinator thread; it issues no RPC itself.
            assertThat(context.getOneTimeCallables()).isEmpty();
            context.runCoordinatorCalls();
        }

        private void runContinuation() throws Throwable {
            fireContinuation();
            assertThat(context.getOneTimeCallables()).hasSize(1);
            context.runNextOneTimeCallable();
        }

        private void respondWith(CompletableFuture<LakeTieringHeartbeatResponse> response) {
            doAnswer(
                            invocation -> {
                                requests.add(invocation.getArgument(0));
                                return response;
                            })
                    .when(gateway)
                    .lakeTieringHeartbeat(any());
        }

        private int heartbeatCount() {
            return requests.size();
        }

        private void closeEnumerator() throws IOException {
            enumerator.close();
        }

        @Override
        public void close() throws Exception {
            // close() is idempotent, so calling it again for tests that already closed the
            // enumerator reports nothing and releases nothing.
            enumerator.close();
            context.close();
        }
    }

    private static class TestingContext extends FlussMockSplitEnumeratorContext<FlinkTieringSplit> {
        private final Queue<Runnable> coordinatorCalls = new ArrayDeque<>();
        private final MetricListener metricListener;

        private TestingContext(int parallelism, MetricListener metricListener) {
            super(parallelism);
            this.metricListener = metricListener;
        }

        @Override
        public SplitEnumeratorMetricGroup metricGroup() {
            return new InternalSplitEnumeratorMetricGroup(metricListener.getMetricGroup());
        }

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            coordinatorCalls.add(runnable);
        }

        private void runCoordinatorCalls() {
            while (!coordinatorCalls.isEmpty()) {
                coordinatorCalls.remove().run();
            }
        }
    }
}

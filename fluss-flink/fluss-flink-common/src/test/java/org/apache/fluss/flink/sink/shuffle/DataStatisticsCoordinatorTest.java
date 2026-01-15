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

package org.apache.fluss.flink.sink.shuffle;

import org.apache.fluss.flink.sink.serializer.RowDataSerializationSchema;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.flink.sink.shuffle.StatisticsEvent.createStatisticsEvent;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataStatisticsCoordinator}. */
public class DataStatisticsCoordinatorTest {
    private static final DataStatisticsSerializer DATA_STATISTICS_SERIALIZER =
            new DataStatisticsSerializer();

    @Test
    void testStatisticsEvent() throws Exception {
        try (DataStatisticsCoordinator coordinator = getDataStatisticsCoordinator()) {
            coordinator.start();
            DataStatisticsCoordinator.SubtaskGateways subtaskGateways =
                    coordinator.getSubtaskGateways();
            coordinator.executionAttemptReady(0, 1, new MockGateway(0, 1));
            coordinator.executionAttemptReady(1, 1, new MockGateway(1, 1));
            coordinator.handleEventFromOperator(
                    0,
                    1,
                    createStatisticsEvent(
                            0,
                            new DataStatistics(Collections.singletonMap("partition 1", 10000L)),
                            DATA_STATISTICS_SERIALIZER));
            coordinator.handleEventFromOperator(
                    1,
                    1,
                    createStatisticsEvent(
                            1,
                            new DataStatistics(Collections.singletonMap("partition 1", 20000L)),
                            DATA_STATISTICS_SERIALIZER));
            waitUntilAllPendingTasksFinished(coordinator);
            assertThat(((MockGateway) subtaskGateways.getSubtaskGateway(0)).events).isEmpty();
            assertThat(((MockGateway) subtaskGateways.getSubtaskGateway(1)).events).isEmpty();

            Map<String, Long> partitionFrequencies = new LinkedHashMap<>();
            partitionFrequencies.put("partition 1", 10000L);
            partitionFrequencies.put("partition 2", 20000L);
            coordinator.handleEventFromOperator(
                    0,
                    1,
                    createStatisticsEvent(
                            1,
                            new DataStatistics(partitionFrequencies),
                            DATA_STATISTICS_SERIALIZER));

            Map<String, Long> expectedPartitionFrequencies = new LinkedHashMap<>();
            expectedPartitionFrequencies.put("partition 1", 30000L);
            expectedPartitionFrequencies.put("partition 2", 20000L);
            StatisticsEvent expectedStatisticsEvent =
                    createStatisticsEvent(
                            1,
                            new DataStatistics(expectedPartitionFrequencies),
                            DATA_STATISTICS_SERIALIZER);

            waitUntilAllPendingTasksFinished(coordinator);

            assertThat(((MockGateway) subtaskGateways.getSubtaskGateway(0)).events).hasSize(1);
            assertStatisticsEventSame(
                    (StatisticsEvent)
                            ((MockGateway) subtaskGateways.getSubtaskGateway(0)).events.get(0),
                    expectedStatisticsEvent);

            assertThat(((MockGateway) subtaskGateways.getSubtaskGateway(1)).events).hasSize(1);
            assertStatisticsEventSame(
                    (StatisticsEvent)
                            ((MockGateway) subtaskGateways.getSubtaskGateway(1)).events.get(0),
                    expectedStatisticsEvent);
        }
    }

    private void assertStatisticsEventSame(StatisticsEvent actual, StatisticsEvent expected) {
        assertThat(actual.getCheckpointId()).isEqualTo(expected.getCheckpointId());
        assertThat(
                        StatisticsUtil.deserializeDataStatistics(
                                actual.getStatisticsBytes(), DATA_STATISTICS_SERIALIZER))
                .isEqualTo(
                        StatisticsUtil.deserializeDataStatistics(
                                expected.getStatisticsBytes(), DATA_STATISTICS_SERIALIZER));
    }

    private DataStatisticsCoordinator getDataStatisticsCoordinator() throws Exception {
        DataStatisticsOperatorFactory<RowData> factory =
                new DataStatisticsOperatorFactory<RowData>(
                        DATA1_ROW_TYPE,
                        Collections.singletonList("b"),
                        new RowDataSerializationSchema(false, false));
        OperatorID operatorID = new OperatorID();
        DataStatisticsCoordinatorProvider provider =
                (DataStatisticsCoordinatorProvider)
                        factory.getCoordinatorProvider(
                                "test-data-statistic-coordinator", operatorID);
        return (DataStatisticsCoordinator)
                provider.getCoordinator(new MockOperatorCoordinatorContext(operatorID, 2));
    }

    private void waitUntilAllPendingTasksFinished(DataStatisticsCoordinator coordinator)
            throws ExecutionException, InterruptedException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        coordinator.runInCoordinatorThread(
                () -> future.complete(null),
                "wait until all pending tasks in coordinator are finished ");
        future.get();
    }

    private static class MockGateway implements OperatorCoordinator.SubtaskGateway {
        protected final List<OperatorEvent> events;
        private final int subtaskIndex;
        private final int attemptNumber;

        public MockGateway(int subtaskIndex, int attemptNumber) {
            this.subtaskIndex = subtaskIndex;
            this.attemptNumber = attemptNumber;
            this.events = new ArrayList<>();
        }

        @Override
        public CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt) {
            events.add(evt);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        @Override
        public ExecutionAttemptID getExecution() {
            return new ExecutionAttemptID(
                    new ExecutionGraphID(),
                    new ExecutionVertexID(new JobVertexID(), subtaskIndex),
                    attemptNumber);
        }

        @Override
        public int getSubtask() {
            return subtaskIndex;
        }
    }
}

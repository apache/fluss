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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.sink.state.WriterStateSerializer;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link UndoRecoveryOperator}. */
class UndoRecoveryOperatorTest {

    private static final int MAX_PARALLELISM = 4;

    private static final TableBucket BUCKET_0 = new TableBucket(1L, null, 0);

    @Test
    void testNoOpNonZeroSubtaskLifecycleDoesNotUseRecoveryClient() throws Exception {
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(RecoveryAction.NO_OP, 2, 1)) {
            harness.open();

            UndoRecoveryOperator<Integer> operator = operator(harness);
            operator.reportOffset(BUCKET_0, 7L);
            harness.processElement(new StreamRecord<>(42));
            harness.snapshot(1L, 1L);
            harness.notifyOfCompletedCheckpoint(1L);
            harness.endInput();

            assertThat(harness.extractOutputValues()).containsExactly(42);
        }
    }

    @Test
    void testNoOpDefaultProducerDoesNotUseRecoveryClient() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), "127.0.0.1:1");
        configuration.set(ConfigOptions.CLIENT_REQUEST_TIMEOUT, Duration.ofMillis(100));

        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(RecoveryAction.NO_OP, 1, 0, configuration, null)) {
            harness.open();
            harness.processElement(new StreamRecord<>(42));

            assertThat(harness.extractOutputValues()).containsExactly(42);
        }
    }

    @Test
    void testNoOpReplacesLegacyUndoStateWithActionMarker() throws Exception {
        Map<TableBucket, Long> legacyOffsets = Collections.singletonMap(BUCKET_0, 7L);
        OperatorSubtaskState legacyCheckpoint =
                createCheckpoint(legacyOffsets, Collections.emptyList());
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), "127.0.0.1:1");

        OperatorSubtaskState noOpCheckpoint;
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                createHarness(RecoveryAction.NO_OP, 1, 0, configuration, null)) {
            harness.initializeState(legacyCheckpoint);
            harness.open();

            UndoRecoveryOperator<Integer> operator = operator(harness);
            assertThat(operator.getBucketOffsets()).isEmpty();
            operator.reportOffset(BUCKET_0, 8L);
            assertThat(operator.getBucketOffsets()).isEmpty();
            noOpCheckpoint = harness.snapshot(2L, 2L);
        }

        StateProbeOperator probe = new StateProbeOperator();
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                new OneInputStreamOperatorTestHarness<>(probe, MAX_PARALLELISM, 1, 0)) {
            harness.initializeState(noOpCheckpoint);
            harness.open();

            assertThat(probe.writerStates).isEmpty();
            assertThat(probe.recoveryActionMarkers).containsExactly(RecoveryAction.NO_OP.name());
        }
    }

    @Test
    void testNoOpCheckpointCannotRestoreWithUndo() throws Exception {
        OperatorSubtaskState noOpCheckpoint =
                createCheckpoint(
                        Collections.emptyMap(),
                        Collections.singletonList(RecoveryAction.NO_OP.name()));
        try (OneInputStreamOperatorTestHarness<Integer, Integer> undo =
                createHarness(RecoveryAction.UNDO, 1, 0)) {
            assertThatThrownBy(() -> undo.initializeState(noOpCheckpoint))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("NO_OP")
                    .hasMessageContaining("UNDO");
        }
    }

    private static OperatorSubtaskState createCheckpoint(
            Map<TableBucket, Long> offsets, List<String> recoveryActionMarkers) throws Exception {
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new CheckpointStateOperator(offsets, recoveryActionMarkers),
                        MAX_PARALLELISM,
                        1,
                        0)) {
            harness.open();
            return harness.snapshot(1L, 1L);
        }
    }

    private static OneInputStreamOperatorTestHarness<Integer, Integer> createHarness(
            RecoveryAction action, int parallelism, int subtaskIndex) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), "127.0.0.1:1");
        return createHarness(action, parallelism, subtaskIndex, configuration);
    }

    private static OneInputStreamOperatorTestHarness<Integer, Integer> createHarness(
            RecoveryAction action, int parallelism, int subtaskIndex, Configuration configuration)
            throws Exception {
        return createHarness(action, parallelism, subtaskIndex, configuration, "test-producer");
    }

    private static OneInputStreamOperatorTestHarness<Integer, Integer> createHarness(
            RecoveryAction action,
            int parallelism,
            int subtaskIndex,
            Configuration configuration,
            String producerId)
            throws Exception {
        UndoRecoveryOperatorFactory<Integer> factory =
                new UndoRecoveryOperatorFactory<>(
                        TablePath.of("db", "table"),
                        configuration,
                        DATA1_ROW_TYPE,
                        null,
                        2,
                        false,
                        producerId,
                        action,
                        1L,
                        1L);
        return new OneInputStreamOperatorTestHarness<>(
                factory, MAX_PARALLELISM, parallelism, subtaskIndex);
    }

    @SuppressWarnings("unchecked")
    private static UndoRecoveryOperator<Integer> operator(
            OneInputStreamOperatorTestHarness<Integer, Integer> harness) {
        return (UndoRecoveryOperator<Integer>) harness.getOperator();
    }

    private static final class CheckpointStateOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private static final long serialVersionUID = 1L;

        private final Map<TableBucket, Long> offsets;
        private final List<String> recoveryActionMarkers;

        private CheckpointStateOperator(
                Map<TableBucket, Long> offsets, List<String> recoveryActionMarkers) {
            this.offsets = offsets;
            this.recoveryActionMarkers = recoveryActionMarkers;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            ListState<WriterState> undoState =
                    context.getOperatorStateStore()
                            .getUnionListState(
                                    new ListStateDescriptor<>(
                                            "undo_recovery_state", new WriterStateSerializer()));
            if (!offsets.isEmpty()) {
                undoState.add(new WriterState(offsets));
            }

            ListState<String> recoveryActionState =
                    context.getOperatorStateStore()
                            .getUnionListState(
                                    new ListStateDescriptor<>(
                                            "recovery_action_state", StringSerializer.INSTANCE));
            for (String recoveryActionMarker : recoveryActionMarkers) {
                recoveryActionState.add(recoveryActionMarker);
            }
        }

        @Override
        public void processElement(StreamRecord<Integer> element) {
            output.collect(element);
        }
    }

    private static final class StateProbeOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private static final long serialVersionUID = 1L;

        private final List<WriterState> writerStates = new ArrayList<>();
        private final List<String> recoveryActionMarkers = new ArrayList<>();

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            ListState<WriterState> undoState =
                    context.getOperatorStateStore()
                            .getUnionListState(
                                    new ListStateDescriptor<>(
                                            "undo_recovery_state", new WriterStateSerializer()));
            for (WriterState writerState : undoState.get()) {
                writerStates.add(writerState);
            }

            ListState<String> recoveryActionState =
                    context.getOperatorStateStore()
                            .getUnionListState(
                                    new ListStateDescriptor<>(
                                            "recovery_action_state", StringSerializer.INSTANCE));
            for (String recoveryActionMarker : recoveryActionState.get()) {
                recoveryActionMarkers.add(recoveryActionMarker);
            }
        }

        @Override
        public void processElement(StreamRecord<Integer> element) {
            output.collect(element);
        }
    }
}

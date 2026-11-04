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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.adapter.StreamOperatorParametersAdapter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.InstantiationUtils;

import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link UndoRecoveryOperatorFactory}'s offset reporter routing. */
class UndoRecoveryOperatorFactoryTest {

    @Test
    void testReportingBeforeDelegateRegistrationFails() {
        UndoRecoveryOperatorFactory<String> factory = createFactory();

        assertThatThrownBy(
                        () -> createReporter(factory, 0).reportOffset(new TableBucket(1L, 0), 1L))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No delegate")
                .hasMessageContaining("offset report");
    }

    @Test
    void testOperatorRejectsReportsBeforeStateInitialization() throws Exception {
        UndoRecoveryOperatorFactory<String> factory = createFactory();
        UndoRecoveryOperator<String> operator =
                factory.createStreamOperator(createOperatorParameters());
        ProducerOffsetReporter reporter = createReporter(factory, 0);
        TableBucket bucket = new TableBucket(1L, 0);

        try {
            assertThatThrownBy(() -> reporter.reportOffset(bucket, 1L))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("bucketOffsets")
                    .hasMessageContaining("initialized");
        } finally {
            operator.close();
        }
    }

    @Test
    void testIndependentlySerializedFactoriesRouteToMatchingSubtasks() throws Exception {
        UndoRecoveryOperatorFactory<String> originalFactory = createFactory();
        byte[] serializedFactory = InstantiationUtils.serializeObject(originalFactory);
        UndoRecoveryOperatorFactory<String> firstFactory = deserializeFactory(serializedFactory);
        UndoRecoveryOperatorFactory<String> secondFactory = deserializeFactory(serializedFactory);
        ProducerOffsetReporter firstReporter = createReporter(firstFactory, 0);
        ProducerOffsetReporter secondReporter = createReporter(secondFactory, 1);
        UndoRecoveryOperator<String> firstOperator =
                firstFactory.createStreamOperator(createOperatorParameters(2, 0));
        UndoRecoveryOperator<String> secondOperator =
                secondFactory.createStreamOperator(createOperatorParameters(2, 1));
        TableBucket firstBucket = new TableBucket(1L, 0);
        TableBucket secondBucket = new TableBucket(1L, 1);

        firstOperator.initializeBucketOffsets(new HashMap<>());
        secondOperator.initializeBucketOffsets(new HashMap<>());

        try {
            firstReporter.reportOffset(firstBucket, 10L);
            secondReporter.reportOffset(secondBucket, 20L);

            SoftAssertions.assertSoftly(
                    softly -> {
                        softly.assertThat(firstOperator.getBucketOffsets())
                                .as("first subtask reporter must update only its own operator")
                                .containsOnlyKeys(firstBucket)
                                .containsEntry(firstBucket, 10L);
                        softly.assertThat(secondOperator.getBucketOffsets())
                                .as("second subtask reporter must update only its own operator")
                                .containsOnlyKeys(secondBucket)
                                .containsEntry(secondBucket, 20L);
                    });
        } finally {
            firstOperator.close();
            secondOperator.close();
        }
    }

    @Test
    void testClosingOneSubtaskDoesNotUnregisterAnother() throws Exception {
        UndoRecoveryOperatorFactory<String> originalFactory = createFactory();
        byte[] serializedFactory = InstantiationUtils.serializeObject(originalFactory);
        UndoRecoveryOperatorFactory<String> firstFactory = deserializeFactory(serializedFactory);
        UndoRecoveryOperatorFactory<String> secondFactory = deserializeFactory(serializedFactory);
        ProducerOffsetReporter secondReporter = createReporter(secondFactory, 1);
        UndoRecoveryOperator<String> firstOperator =
                firstFactory.createStreamOperator(createOperatorParameters(2, 0));
        UndoRecoveryOperator<String> secondOperator =
                secondFactory.createStreamOperator(createOperatorParameters(2, 1));
        TableBucket secondBucket = new TableBucket(1L, 1);

        secondOperator.initializeBucketOffsets(new HashMap<>());

        try {
            firstOperator.close();
            secondReporter.reportOffset(secondBucket, 20L);

            assertThat(secondOperator.getBucketOffsets())
                    .as("remaining operator must still receive its reporter offset")
                    .containsOnlyKeys(secondBucket)
                    .containsEntry(secondBucket, 20L);
        } finally {
            secondOperator.close();
        }
    }

    @Test
    void testDelayedCloseDoesNotUnregisterReplacementForSameSubtask() throws Exception {
        UndoRecoveryOperatorFactory<String> originalFactory = createFactory();
        byte[] serializedFactory = InstantiationUtils.serializeObject(originalFactory);
        TableBucket bucket = new TableBucket(1L, 0);

        UndoRecoveryOperatorFactory<String> firstFactory = deserializeFactory(serializedFactory);
        UndoRecoveryOperator<String> firstOperator =
                firstFactory.createStreamOperator(createOperatorParameters(2, 0));

        UndoRecoveryOperatorFactory<String> recoveredFactory =
                deserializeFactory(serializedFactory);
        ProducerOffsetReporter recoveredReporter = createReporter(recoveredFactory, 0);
        UndoRecoveryOperator<String> recoveredOperator =
                recoveredFactory.createStreamOperator(createOperatorParameters(2, 0));
        recoveredOperator.initializeBucketOffsets(new HashMap<>());

        try {
            firstOperator.close();
            recoveredReporter.reportOffset(bucket, 10L);

            assertThat(recoveredOperator.getBucketOffsets())
                    .containsOnlyKeys(bucket)
                    .containsEntry(bucket, 10L);
        } finally {
            recoveredOperator.close();
        }
    }

    private static UndoRecoveryOperatorFactory<String> createFactory() {
        return new UndoRecoveryOperatorFactory<>(
                TablePath.of("test_db", "test_table"),
                new Configuration(),
                RowType.of(DataTypes.INT()),
                null,
                1,
                false,
                "test-producer");
    }

    private static UndoRecoveryOperatorFactory<String> deserializeFactory(byte[] serializedFactory)
            throws Exception {
        return InstantiationUtils.deserializeObject(
                serializedFactory, UndoRecoveryOperatorFactoryTest.class.getClassLoader());
    }

    private static ProducerOffsetReporter createReporter(
            UndoRecoveryOperatorFactory<String> factory, int subtaskIndex) {
        return factory.createProducerOffsetReporter(subtaskIndex);
    }

    private static StreamOperatorParameters<String> createOperatorParameters() throws Exception {
        return createOperatorParameters(1, 0);
    }

    private static StreamOperatorParameters<String> createOperatorParameters(
            int parallelism, int subtaskIndex) throws Exception {
        return StreamOperatorParametersAdapter.create(
                new SourceOperatorStreamTask<String>(
                        new DummyEnvironment("test-task", parallelism, subtaskIndex)),
                new MockStreamConfig(new org.apache.flink.configuration.Configuration(), 1),
                new MockOutput<>(new ArrayList<>()),
                null,
                null,
                null);
    }
}

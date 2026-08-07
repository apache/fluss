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

package org.apache.fluss.flink.sink;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.flink.sink.serializer.FlussSerializationSchema;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.flink.sink.undo.RecoveryAction;
import org.apache.fluss.flink.sink.undo.UndoRecoveryOperatorFactory;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the pre-write topology built by {@link FlinkSink.UpsertSinkWriterBuilder}. */
class FlinkSinkTopologyTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "table");
    private static final RowType ROW_TYPE =
            (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
    private static final FlussSerializationSchema<Integer> SERIALIZATION_SCHEMA =
            new FlussSerializationSchema<Integer>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void open(InitializationContext context) {}

                @Override
                public RowWithOp serialize(Integer value) {
                    return null;
                }
            };

    @Test
    void testNoOpKeepsUndoRecoveryTransformIdentity() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Integer> input = env.fromElements(1);

        FlinkSink.UpsertSinkWriterBuilder<Integer> builder =
                createBuilder(true, RecoveryAction.NO_OP);
        DataStream<Integer> transformed = builder.addPreWriteTopology(input);

        @SuppressWarnings("unchecked")
        OneInputTransformation<Integer, Integer> transformation =
                (OneInputTransformation<Integer, Integer>) transformed.getTransformation();
        assertThat(transformation.getName()).isEqualTo("UndoRecovery(db.table)");
        assertThat(transformation.getParallelism()).isEqualTo(input.getParallelism());
        assertThat(transformation.getOperatorFactory())
                .isInstanceOf(UndoRecoveryOperatorFactory.class);

        UndoRecoveryOperatorFactory<?> factory =
                (UndoRecoveryOperatorFactory<?>) transformation.getOperatorFactory();
        assertThat(factory.getRecoveryAction()).isEqualTo(RecoveryAction.NO_OP);
        assertThat(factory.getChainingStrategy()).isEqualTo(ChainingStrategy.ALWAYS);
        assertThat(builder).extracting("offsetReporter").isNull();
    }

    @Test
    void testUndoInstallsOffsetReporter() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> input = env.fromElements(1);

        FlinkSink.UpsertSinkWriterBuilder<Integer> builder =
                createBuilder(true, RecoveryAction.UNDO);
        DataStream<Integer> transformed = builder.addPreWriteTopology(input);
        OneInputTransformation<?, ?> transformation =
                (OneInputTransformation<?, ?>) transformed.getTransformation();
        UndoRecoveryOperatorFactory<?> factory =
                (UndoRecoveryOperatorFactory<?>) transformation.getOperatorFactory();
        assertThat(factory.getRecoveryAction()).isEqualTo(RecoveryAction.UNDO);
        assertThat(builder).extracting("offsetReporter").isNotNull();
    }

    private static FlinkSink.UpsertSinkWriterBuilder<Integer> createBuilder(
            boolean enableUndoRecovery, RecoveryAction action) {
        return new FlinkSink.UpsertSinkWriterBuilder<>(
                TABLE_PATH,
                new Configuration(),
                ROW_TYPE,
                null,
                1,
                Collections.singletonList("id"),
                Collections.emptyList(),
                null,
                DistributionMode.NONE,
                SERIALIZATION_SCHEMA,
                enableUndoRecovery,
                action,
                null);
    }
}

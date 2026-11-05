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

package org.apache.fluss.flink.sink.serializer;

import org.apache.fluss.flink.row.OperationType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.fluss.flink.row.OperationType.APPEND;
import static org.apache.fluss.flink.row.OperationType.DELETE;
import static org.apache.fluss.flink.row.OperationType.IGNORE;
import static org.apache.fluss.flink.row.OperationType.RETRACT;
import static org.apache.fluss.flink.row.OperationType.UPSERT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataSerializationSchema}. */
class RowDataSerializationSchemaTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("operationMappings")
    void testOperationMapping(
            String modeName, RowDataSerializationSchema schema, OperationType[] expected)
            throws Exception {
        schema.open(new TestInitializationContext());
        GenericRowData row = GenericRowData.of(1, 2);
        RowKind[] rowKinds = {
            RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER, RowKind.DELETE
        };
        for (int i = 0; i < rowKinds.length; i++) {
            row.setRowKind(rowKinds[i]);
            assertThat(schema.serialize(row).getOperationType())
                    .as("%s with %s", modeName, rowKinds[i])
                    .isEqualTo(expected[i]);
        }
    }

    private static Stream<Arguments> operationMappings() {
        return Stream.of(
                Arguments.of(
                        "append",
                        new RowDataSerializationSchema(true, false),
                        new OperationType[] {APPEND, APPEND, APPEND, APPEND}),
                Arguments.of(
                        "append with ignore-delete",
                        new RowDataSerializationSchema(true, true),
                        new OperationType[] {APPEND, IGNORE, APPEND, IGNORE}),
                Arguments.of(
                        "upsert",
                        new RowDataSerializationSchema(false, false),
                        new OperationType[] {UPSERT, DELETE, UPSERT, DELETE}),
                Arguments.of(
                        "upsert with ignore-delete",
                        new RowDataSerializationSchema(false, true),
                        new OperationType[] {UPSERT, IGNORE, UPSERT, IGNORE}),
                Arguments.of(
                        "aggregation retract",
                        new RowDataSerializationSchema(SinkOperationMode.aggregation(false, true)),
                        new OperationType[] {UPSERT, RETRACT, UPSERT, RETRACT}),
                Arguments.of(
                        "aggregation retract with ignore-delete",
                        new RowDataSerializationSchema(SinkOperationMode.aggregation(true, true)),
                        new OperationType[] {UPSERT, IGNORE, UPSERT, IGNORE}),
                Arguments.of(
                        "aggregation retract with whole-row delete",
                        new RowDataSerializationSchema(
                                SinkOperationMode.aggregation(false, true, true)),
                        new OperationType[] {UPSERT, RETRACT, UPSERT, DELETE}));
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"UPDATE_BEFORE", "DELETE"})
    void testUnsupportedAggregationRetractFails(RowKind rowKind) throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(SinkOperationMode.aggregation(false, false));
        schema.open(new TestInitializationContext());
        GenericRowData row = GenericRowData.of(1, 2);
        row.setRowKind(rowKind);

        assertThatThrownBy(() -> schema.serialize(row))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("support retract");
    }

    /**
     * Minimal InitializationContext for unit tests that don't need schema evolution or statistics.
     */
    static class TestInitializationContext
            implements FlussSerializationSchema.InitializationContext {

        private static final RowType FLUSS_ROW_TYPE =
                RowType.of(
                        new org.apache.fluss.types.DataType[] {new IntType(), new IntType()},
                        new String[] {"a", "b"});

        private static final org.apache.flink.table.types.logical.RowType FLINK_ROW_TYPE =
                org.apache.flink.table.types.logical.RowType.of(
                        new org.apache.flink.table.types.logical.IntType(),
                        new org.apache.flink.table.types.logical.IntType());

        @Override
        public RowType getRowSchema() {
            return FLUSS_ROW_TYPE;
        }

        @Override
        public org.apache.flink.table.types.logical.RowType getInputRowSchema() {
            return FLINK_ROW_TYPE;
        }

        @Override
        public boolean isStatisticEnabled() {
            return false;
        }
    }
}

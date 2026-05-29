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
import org.apache.fluss.flink.row.RowWithOp;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataSerializationSchema}. */
class RowDataSerializationSchemaTest {

    @Test
    void testUpdateBeforeMapsToRetractWhenRetractCompatible() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(SinkOperationMode.aggregation(false, true));
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.UPDATE_BEFORE);

        RowWithOp result = schema.serialize(rowData);
        assertThat(result.getOperationType()).isEqualTo(OperationType.RETRACT);
    }

    @Test
    void testDeleteStillMapsToDeleteWhenRetractCompatible() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(SinkOperationMode.aggregation(false, true));
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.DELETE);

        RowWithOp result = schema.serialize(rowData);
        assertThat(result.getOperationType()).isEqualTo(OperationType.DELETE);
    }

    @Test
    void testUpdateBeforeMapsToDeleteWhenNotRetractCompatible() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(SinkOperationMode.upsert(false));
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.UPDATE_BEFORE);

        RowWithOp result = schema.serialize(rowData);
        assertThat(result.getOperationType()).isEqualTo(OperationType.DELETE);
    }

    @Test
    void testUpdateBeforeFailsFastForAggregationTableWithoutRetract() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(SinkOperationMode.aggregation(false, false));
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.UPDATE_BEFORE);

        assertThatThrownBy(() -> schema.serialize(rowData))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support retract");
    }

    @Test
    void testNonAggregationTableKeepsCurrentBehavior() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(SinkOperationMode.upsert(false));
        schema.open(new TestInitializationContext());

        // INSERT -> UPSERT
        GenericRowData insert = new GenericRowData(2);
        insert.setField(0, 1);
        insert.setField(1, 2);
        insert.setRowKind(RowKind.INSERT);
        assertThat(schema.serialize(insert).getOperationType()).isEqualTo(OperationType.UPSERT);

        // UPDATE_AFTER -> UPSERT
        GenericRowData updateAfter = new GenericRowData(2);
        updateAfter.setField(0, 1);
        updateAfter.setField(1, 2);
        updateAfter.setRowKind(RowKind.UPDATE_AFTER);
        assertThat(schema.serialize(updateAfter).getOperationType())
                .isEqualTo(OperationType.UPSERT);

        // DELETE -> DELETE
        GenericRowData delete = new GenericRowData(2);
        delete.setField(0, 1);
        delete.setField(1, 2);
        delete.setRowKind(RowKind.DELETE);
        assertThat(schema.serialize(delete).getOperationType()).isEqualTo(OperationType.DELETE);

        // UPDATE_BEFORE -> DELETE (not RETRACT)
        GenericRowData updateBefore = new GenericRowData(2);
        updateBefore.setField(0, 1);
        updateBefore.setField(1, 2);
        updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
        assertThat(schema.serialize(updateBefore).getOperationType())
                .isEqualTo(OperationType.DELETE);
    }

    @Test
    void testInsertAndUpdateAfterStillMapToUpsertWhenAggregationTable() throws Exception {
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(SinkOperationMode.aggregation(false, true));
        schema.open(new TestInitializationContext());

        // INSERT -> UPSERT even when isAggregationTable=true
        GenericRowData insert = new GenericRowData(2);
        insert.setField(0, 1);
        insert.setField(1, 2);
        insert.setRowKind(RowKind.INSERT);
        assertThat(schema.serialize(insert).getOperationType()).isEqualTo(OperationType.UPSERT);

        // UPDATE_AFTER -> UPSERT even when isAggregationTable=true
        GenericRowData updateAfter = new GenericRowData(2);
        updateAfter.setField(0, 1);
        updateAfter.setField(1, 2);
        updateAfter.setRowKind(RowKind.UPDATE_AFTER);
        assertThat(schema.serialize(updateAfter).getOperationType())
                .isEqualTo(OperationType.UPSERT);
    }

    @Test
    void testIgnoreDeleteOverridesAggregationTable() throws Exception {
        // When ignoreDelete=true, UPDATE_BEFORE should be IGNORE even if aggregation table
        RowDataSerializationSchema schema =
                new RowDataSerializationSchema(SinkOperationMode.aggregation(true, true));
        schema.open(new TestInitializationContext());

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, 2);
        rowData.setRowKind(RowKind.UPDATE_BEFORE);

        RowWithOp result = schema.serialize(rowData);
        assertThat(result.getOperationType()).isEqualTo(OperationType.IGNORE);
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

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

package org.apache.fluss.flink.source.testutils;

import org.apache.fluss.flink.source.deserializer.FlussDeserializationSchema;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;
import java.util.List;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkRowType;

/**
 * Utility class for generating mock data and test infrastructure for Fluss-Flink integration tests.
 *
 * <p>The mock data and utilities in this class are used across various Fluss source connector tests
 * to verify functionality like schema handling, record deserialization, and data type conversion.
 */
public class MockDataUtils {
    public static final List<Order> ORDERS =
            Arrays.asList(
                    new Order(600, 20, 600, "addr1"),
                    new Order(700, 22, 601, "addr2"),
                    new Order(800, 23, 602, "addr3"),
                    new Order(900, 24, 603, "addr4"),
                    new Order(1000, 25, 604, "addr5"));

    public static Schema getOrdersSchemaPK() {
        return Schema.newBuilder()
                .column("orderId", DataTypes.BIGINT())
                .column("itemId", DataTypes.BIGINT())
                .column("amount", DataTypes.INT())
                .column("address", DataTypes.STRING())
                .primaryKey("orderId")
                .build();
    }

    public static Schema getOrdersSchemaLog() {
        return Schema.newBuilder()
                .column("orderId", DataTypes.BIGINT())
                .column("itemId", DataTypes.BIGINT())
                .column("amount", DataTypes.INT())
                .column("address", DataTypes.STRING())
                .build();
    }

    /**
     * Utility method to create a readable copy of a BinaryRowData. Creates a GenericRowData with
     * the same content as the BinaryRowData.
     *
     * @param binaryRow The BinaryRowData to copy
     * @param flussRowType The RowType describing the row structure
     * @return A GenericRowData with the same content
     */
    public static RowData binaryRowToGenericRow(RowData binaryRow, RowType flussRowType) {
        // Convert Fluss RowType to Flink RowType
        org.apache.flink.table.types.logical.RowType flinkRowType = toFlinkRowType(flussRowType);

        int fieldCount = binaryRow.getArity();
        GenericRowData genericRow = new GenericRowData(fieldCount);
        genericRow.setRowKind(binaryRow.getRowKind());

        for (int i = 0; i < fieldCount; i++) {
            if (binaryRow.isNullAt(i)) {
                genericRow.setField(i, null);
                continue;
            }

            LogicalType fieldType = flinkRowType.getTypeAt(i);
            switch (fieldType.getTypeRoot()) {
                case BIGINT:
                    genericRow.setField(i, binaryRow.getLong(i));
                    break;
                case INTEGER:
                    genericRow.setField(i, binaryRow.getInt(i));
                    break;
                case DOUBLE:
                    genericRow.setField(i, binaryRow.getDouble(i));
                    break;
                case FLOAT:
                    genericRow.setField(i, binaryRow.getFloat(i));
                    break;
                case VARCHAR:
                case CHAR:
                    genericRow.setField(i, binaryRow.getString(i));
                    break;
                case BOOLEAN:
                    genericRow.setField(i, binaryRow.getBoolean(i));
                    break;
                default:
                    genericRow.setField(i, binaryRow.getString(i));
            }
        }

        return genericRow;
    }

    /**
     * A deserialization schema for converting Fluss {@link LogRecord} objects to {@link Order}
     * POJOs.
     */
    public static class OrderDeserializationSchema implements FlussDeserializationSchema<Order> {

        @Override
        public void open(InitializationContext context) throws Exception {}

        @Override
        public Order deserialize(LogRecord record) throws Exception {
            InternalRow row = record.getRow();
            long orderId = row.getLong(0);
            long itemId = row.getLong(1);
            int amount = row.getInt(2);
            String address = String.valueOf(row.getString(3));
            return new Order(orderId, itemId, amount, address);
        }

        @Override
        public TypeInformation<Order> getProducedType(RowType rowSchema) {
            return TypeInformation.of(Order.class);
        }
    }
}

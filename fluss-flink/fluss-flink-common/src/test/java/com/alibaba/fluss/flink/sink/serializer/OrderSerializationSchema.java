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

package com.alibaba.fluss.flink.sink.serializer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.flink.row.OperationType;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.flink.source.testutils.Order;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.table.data.RowData;

/**
 * A serialization schema that converts {@link Order} objects to {@link RowData} for writing to
 * Fluss.
 *
 * <p>This implementation demonstrates how to implement a custom serialization schema for a specific
 * class. It directly maps fields from the Order object to the appropriate Flink internal data
 * structures without using reflection, providing better performance than a generic solution.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * FlussSink<Order> sink = FlussSink.<Order>builder()
 *     .setBootstrapServers("localhost:9092")
 *     .setDatabase("fluss")
 *     .setTable("orders")
 *     .setSerializer(new OrderSerializationSchema())
 *     .build();
 * }</pre>
 *
 * @since 0.7
 */
@PublicEvolving
public class OrderSerializationSchema implements FlussSerializationSchema<Order> {
    private static final long serialVersionUID = 1L;

    private RowType rowType;

    @Override
    public void open(InitializationContext context) throws Exception {
        this.rowType = context.getRowSchema();

        // Validate schema compatibility with Order class
        if (rowType.getFieldCount() < 4) {
            throw new IllegalStateException(
                    "Schema must have at least 4 fields to serialize Order objects");
        }
    }

    @Override
    public RowWithOp serialize(Order order) throws Exception {
        // Create a new row with the same number of fields as the schema
        GenericRow row = new GenericRow(rowType.getFieldCount());

        // Set order fields directly, knowing their exact position and type
        row.setField(0, order.getOrderId());
        row.setField(1, order.getItemId());
        row.setField(2, order.getAmount());

        // Convert String to StringData for Flink internal representation
        String address = order.getAddress();
        if (address != null) {
            row.setField(3, BinaryString.fromString(address));
        } else {
            row.setField(3, null);
        }

        // If the schema has more fields than the Order class, set them to null
        for (int i = 4; i < rowType.getFieldCount(); i++) {
            row.setField(i, null);
        }

        return new RowWithOp(row, OperationType.APPEND);
    }
}

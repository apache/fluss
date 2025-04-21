/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.sink.serializer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.flink.common.Order;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

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
    public RowData serialize(Order order) throws Exception {
        if (order == null) {
            return null;
        }

        // Create a new row with the same number of fields as the schema
        GenericRowData rowData = new GenericRowData(rowType.getFieldCount());

        // Set order fields directly, knowing their exact position and type
        rowData.setField(0, order.getOrderId());
        rowData.setField(1, order.getItemId());
        rowData.setField(2, order.getAmount());

        // Convert String to StringData for Flink internal representation
        String address = order.getAddress();
        if (address != null) {
            rowData.setField(3, StringData.fromString(address));
        } else {
            rowData.setField(3, null);
        }

        // If the schema has more fields than the Order class, set them to null
        for (int i = 4; i < rowType.getFieldCount(); i++) {
            rowData.setField(i, null);
        }

        return rowData;
    }
}

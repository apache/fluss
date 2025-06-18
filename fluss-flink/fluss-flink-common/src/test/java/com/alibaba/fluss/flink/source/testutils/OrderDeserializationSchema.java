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

package com.alibaba.fluss.flink.source.testutils;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * A deserialization schema that converts {@link LogRecord} objects to {@link Order} objects.
 *
 * <p>This implementation extracts fields from the Fluss {@link InternalRow} and maps them to the
 * {@link Order} domain model. The schema assumes a specific structure for the input rows:
 *
 * <ul>
 *   <li>Field 0: orderId (BIGINT)
 *   <li>Field 1: itemId (BIGINT)
 *   <li>Field 2: amount (INT)
 *   <li>Field 3: address (STRING)
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * OrderDeserializationSchema schema = new OrderDeserializationSchema();
 * FlussSource<Order> source = FlussSource.builder()
 *     .setDeserializationSchema(schema)
 *     .build();
 * }</pre>
 *
 * @since 0.7
 */
@PublicEvolving
public class OrderDeserializationSchema implements FlussDeserializationSchema<Order> {
    private static final long serialVersionUID = 1L;

    /**
     * Initializes the deserialization schema.
     *
     * <p>This implementation doesn't require any initialization.
     *
     * @param context Contextual information for initialization
     * @throws Exception if initialization fails
     */
    @Override
    public void open(InitializationContext context) throws Exception {}

    /**
     * Deserializes a {@link LogRecord} into an {@link Order} object.
     *
     * <p>The method extracts fields from the record's internal row and constructs an Order object
     * with the retrieved values.
     *
     * @param record The Fluss LogRecord to deserialize
     * @return The deserialized Order object
     * @throws Exception If deserialization fails or if field types don't match expectations
     */
    @Override
    public Order deserialize(LogRecord record) throws Exception {
        InternalRow row = record.getRow();

        long orderId = row.getLong(0);
        long itemId = row.getLong(1);
        int amount = row.getInt(2);

        String address = String.valueOf(row.getString(3));

        return new Order(orderId, itemId, amount, address);
    }

    /**
     * Returns the TypeInformation for the produced {@link Order} type.
     *
     * @return TypeInformation for Order class
     */
    @Override
    public TypeInformation<Order> getProducedType(RowType rowSchema) {
        return TypeInformation.of(Order.class);
    }
}

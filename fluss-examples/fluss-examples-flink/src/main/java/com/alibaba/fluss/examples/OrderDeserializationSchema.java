/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.examples;

import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Custom deserialization schema for converting Fluss log records into Order objects. This class
 * implements FlussDeserializationSchema to handle the conversion.
 */
public class OrderDeserializationSchema implements FlussDeserializationSchema<Order> {
    @Override
    public void open(InitializationContext context) throws Exception {
        // Initialization code if needed
    }

    @Override
    public Order deserialize(LogRecord record) throws Exception {
        InternalRow row = record.getRow();

        // Extract fields from the row
        long orderId = row.getLong(0);
        long itemId = row.getLong(1);
        int amount = row.getInt(2);
        String address = row.getString(3).toString();

        // Create and return your custom object
        return new Order(orderId, itemId, address, amount);
    }

    @Override
    public TypeInformation<Order> getProducedType(RowType rowSchema) {
        return TypeInformation.of(Order.class);
    }
}

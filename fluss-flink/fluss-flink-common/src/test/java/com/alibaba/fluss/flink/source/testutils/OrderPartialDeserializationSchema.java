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

package com.alibaba.fluss.flink.source.testutils;

import com.alibaba.fluss.flink.source.deserializer.FlussDeserializationSchema;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * A deserialization schema for partially deserializing Order records to {@link OrderPartial}
 * objects.
 */
public class OrderPartialDeserializationSchema implements FlussDeserializationSchema<OrderPartial> {

    @Override
    public void open(InitializationContext context) throws Exception {
        RowType rowSchema = context.getRowSchema();
        checkArgument(rowSchema.getFieldCount() == 2);
        checkArgument(rowSchema.getFields().get(0).getType().getTypeRoot() == DataTypeRoot.BIGINT);
        checkArgument(rowSchema.getFields().get(1).getType().getTypeRoot() == DataTypeRoot.INTEGER);
    }

    @Override
    public OrderPartial deserialize(LogRecord record) throws Exception {
        InternalRow row = record.getRow();

        long orderId = row.getLong(0);
        int amount = row.getInt(1);

        return new OrderPartial(orderId, amount);
    }

    @Override
    public TypeInformation<OrderPartial> getProducedType(RowType rowSchema) {
        return TypeInformation.of(OrderPartial.class);
    }
}

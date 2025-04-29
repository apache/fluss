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

package com.alibaba.fluss.flink.source.deserializer;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.flink.source.testutils.Order;
import com.alibaba.fluss.flink.source.testutils.OrderDeserializationSchema;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.GenericRow;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class for the {@link FlussDeserializationSchema} implementations that validates the
 * conversion from Fluss records to various target formats.
 */
public class FlussDeserializationSchemaTest {
    @Test
    public void testDeserialize() throws Exception {
        // Create GenericRow with proper types
        GenericRow row = new GenericRow(4);
        row.setField(0, 1001L);
        row.setField(1, 5001L);
        row.setField(2, 3);
        row.setField(3, BinaryString.fromString("123 Main St"));

        ScanRecord scanRecord = new ScanRecord(row);
        OrderDeserializationSchema deserializer = new OrderDeserializationSchema();

        Order result = deserializer.deserialize(scanRecord);

        assertThat(result.getOrderId()).isEqualTo(1001L);
        assertThat(result.getItemId()).isEqualTo(5001L);
        assertThat(result.getAmount()).isEqualTo(3);
        assertThat(result.getAddress()).isEqualTo("123 Main St");
    }

    @Test
    public void testDeserializeWithNumericConversion() throws Exception {
        GenericRow row = new GenericRow(4);
        row.setField(0, 1002L);
        row.setField(1, 5002L);
        row.setField(2, 4);
        row.setField(3, BinaryString.fromString("456 Oak Ave"));

        ScanRecord scanRecord = new ScanRecord(row);
        OrderDeserializationSchema schema = new OrderDeserializationSchema();

        Order result = schema.deserialize(scanRecord);

        assertThat(result.getOrderId()).isEqualTo(1002L);
        assertThat(result.getItemId()).isEqualTo(5002L);
        assertThat(result.getAmount()).isEqualTo(4);
        assertThat(result.getAddress()).isEqualTo("456 Oak Ave");
    }

    @Test
    public void testDeserializeWithNullValues() throws Exception {
        GenericRow row = new GenericRow(4);
        row.setField(0, 1003L);
        row.setField(1, 5003L);
        row.setField(2, 5);
        row.setField(3, null);

        ScanRecord scanRecord = new ScanRecord(row);
        OrderDeserializationSchema schema = new OrderDeserializationSchema();

        Order result = schema.deserialize(scanRecord);

        assertThat(result.getOrderId()).isEqualTo(1003L);
        assertThat(result.getItemId()).isEqualTo(5003L);
        assertThat(result.getAmount()).isEqualTo(5);
        assertThat(result.getAddress()).isEqualTo("null");
    }

    @Test
    public void testDeserializeWithNullRecord() {
        OrderDeserializationSchema schema = new OrderDeserializationSchema();

        assertThatThrownBy(() -> schema.deserialize(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testGetProducedType() {
        OrderDeserializationSchema schema = new OrderDeserializationSchema();
        TypeInformation<Order> typeInfo = schema.getProducedType(null);

        assertThat(typeInfo).isNotNull();
        assertThat(typeInfo).isInstanceOf(PojoTypeInfo.class);
        assertThat(typeInfo.getTypeClass()).isEqualTo(Order.class);
    }

    @Test
    public void testJsonStringDeserialize() throws Exception {
        // Create test data
        GenericRow row = new GenericRow(2);
        row.setField(0, 42L);
        row.setField(1, BinaryString.fromString("test value"));
        ScanRecord scanRecord = new ScanRecord(row);

        // Create deserializer
        JsonStringDeserializationSchema deserializer = new JsonStringDeserializationSchema();
        // Test deserialization
        deserializer.open(null);
        String result = deserializer.deserialize(scanRecord);

        // Verify result
        assertThat(result).isNotNull();
        assertThat(result)
                .isEqualTo(
                        "{\"offset\":-1,\"timestamp\":-1,\"change_type\":\"INSERT\",\"row\":\"(42,test value)\"}");

        // Verify with offset and timestamp
        ScanRecord scanRecord2 = new ScanRecord(1001, 1743261788400L, ChangeType.DELETE, row);
        String result2 = deserializer.deserialize(scanRecord2);
        assertThat(result2).isNotNull();
        assertThat(result2)
                .isEqualTo(
                        "{\"offset\":1001,\"timestamp\":1743261788400,\"change_type\":\"DELETE\",\"row\":\"(42,test value)\"}");
    }

    @Test
    public void testStringGetProducedType() {
        // Create deserializer
        JsonStringDeserializationSchema deserializer = new JsonStringDeserializationSchema();

        // Get type information
        TypeInformation<String> typeInfo = deserializer.getProducedType(null);

        // Verify type information
        assertThat(typeInfo).isNotNull();
        assertThat(typeInfo.getTypeClass()).isEqualTo(String.class);
    }
}

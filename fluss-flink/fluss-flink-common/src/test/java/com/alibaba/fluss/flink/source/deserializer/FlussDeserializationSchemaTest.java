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

package com.alibaba.fluss.flink.source.deserializer;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.flink.source.testutils.Order;
import com.alibaba.fluss.flink.source.testutils.OrderDeserializationSchema;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

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
        List<DataField> fields =
                Arrays.asList(
                        new DataField("char", DataTypes.CHAR(64)),
                        new DataField("string", DataTypes.STRING()),
                        new DataField("boolean", DataTypes.BOOLEAN()),
                        new DataField("binary", DataTypes.BINARY(64)),
                        new DataField("bytes", DataTypes.BYTES()),
                        new DataField("decimal", DataTypes.DECIMAL(6, 3)),
                        new DataField("tinyInt", DataTypes.TINYINT()),
                        new DataField("smallInt", DataTypes.SMALLINT()),
                        new DataField("integer", DataTypes.INT()),
                        new DataField("bigInt", DataTypes.BIGINT()),
                        new DataField("float", DataTypes.FLOAT()),
                        new DataField("double", DataTypes.DOUBLE()),
                        new DataField("date", DataTypes.DATE()),
                        new DataField("timeWithoutTimeZone", DataTypes.TIME()),
                        new DataField("timestampWithoutTimeZone", DataTypes.TIMESTAMP()),
                        new DataField("timestampWithLocalTimeZone", DataTypes.TIMESTAMP_LTZ()),
                        new DataField("nullVal", DataTypes.STRING()));

        RowType rowType = new RowType(fields);

        // Mon Apr 21 2025 10:00:00 GMT+0000
        long testTimestampInSeconds = 1745229600;

        // Create test data
        GenericRow row = new GenericRow(17);
        row.setField(0, BinaryString.fromString("a"));
        row.setField(1, BinaryString.fromString("test value"));
        row.setField(2, false);
        row.setField(3, "test binary".getBytes(StandardCharsets.UTF_8));
        row.setField(4, "test bytes".getBytes(StandardCharsets.UTF_8));
        row.setField(5, Decimal.fromBigDecimal(BigDecimal.valueOf(123), 6, 3));
        row.setField(6, (byte) 1);
        row.setField(7, (short) 64);
        row.setField(8, 1024);
        row.setField(9, 2048L);
        row.setField(10, 1.1f);
        row.setField(11, 3.14d);
        row.setField(12, (int) LocalDate.of(2025, 4, 21).toEpochDay());
        row.setField(13, 36000000);
        row.setField(14, TimestampNtz.fromMillis(testTimestampInSeconds * 1000));
        row.setField(15, TimestampLtz.fromEpochMillis(testTimestampInSeconds * 1000));
        row.setField(16, null);
        ScanRecord scanRecord = new ScanRecord(row);

        // Create deserializer
        JsonStringDeserializationSchema deserializer = new JsonStringDeserializationSchema();
        // Test deserialization
        deserializer.open(new DeserializerInitContextImpl(null, null, rowType));
        String result = deserializer.deserialize(scanRecord);

        String rowJson =
                "{"
                        + "\"char\":\"a\","
                        + "\"string\":\"test value\","
                        + "\"boolean\":false,"
                        + "\"binary\":\"dGVzdCBiaW5hcnk=\","
                        + "\"bytes\":\"dGVzdCBieXRlcw==\","
                        + "\"decimal\":123,"
                        + "\"tinyInt\":1,"
                        + "\"smallInt\":64,"
                        + "\"integer\":1024,"
                        + "\"bigInt\":2048,"
                        + "\"float\":1.1,"
                        + "\"double\":3.14,"
                        + "\"date\":\"2025-04-21\","
                        + "\"timeWithoutTimeZone\":\"10:00:00\","
                        + "\"timestampWithoutTimeZone\":\"2025-04-21T10:00:00\","
                        + "\"timestampWithLocalTimeZone\":\"2025-04-21T10:00:00Z\","
                        + "\"nullVal\":null"
                        + "}";
        // Verify result
        assertThat(result).isNotNull();
        assertThat(result)
                .isEqualTo(
                        "{\"offset\":-1,\"timestamp\":-1,\"change_type\":\"INSERT\",\"row\":"
                                + rowJson
                                + "}");

        // Verify with offset and timestamp
        ScanRecord scanRecord2 = new ScanRecord(1001, 1743261788400L, ChangeType.DELETE, row);
        String result2 = deserializer.deserialize(scanRecord2);
        assertThat(result2).isNotNull();
        assertThat(result2)
                .isEqualTo(
                        "{\"offset\":1001,\"timestamp\":1743261788400,\"change_type\":\"DELETE\",\"row\":"
                                + rowJson
                                + "}");

        // change several value to test reuse node
        row.setField(0, BinaryString.fromString("b"));
        row.setField(2, true);
        row.setField(8, 512);
        row.setField(13, 72000000);
        ScanRecord changedRecord = new ScanRecord(row);
        String changedResult = deserializer.deserialize(changedRecord);
        String changedRowJson =
                "{"
                        + "\"char\":\"b\","
                        + "\"string\":\"test value\","
                        + "\"boolean\":true,"
                        + "\"binary\":\"dGVzdCBiaW5hcnk=\","
                        + "\"bytes\":\"dGVzdCBieXRlcw==\","
                        + "\"decimal\":123,"
                        + "\"tinyInt\":1,"
                        + "\"smallInt\":64,"
                        + "\"integer\":512,"
                        + "\"bigInt\":2048,"
                        + "\"float\":1.1,"
                        + "\"double\":3.14,"
                        + "\"date\":\"2025-04-21\","
                        + "\"timeWithoutTimeZone\":\"20:00:00\","
                        + "\"timestampWithoutTimeZone\":\"2025-04-21T10:00:00\","
                        + "\"timestampWithLocalTimeZone\":\"2025-04-21T10:00:00Z\","
                        + "\"nullVal\":null"
                        + "}";
        assertThat(changedResult).isNotNull();
        assertThat(changedResult)
                .isEqualTo(
                        "{\"offset\":-1,\"timestamp\":-1,\"change_type\":\"INSERT\",\"row\":"
                                + changedRowJson
                                + "}");
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

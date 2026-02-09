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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IcebergRecordAsFlussRow}. */
class IcebergRecordAsFlussRowTest {

    private IcebergRecordAsFlussRow icebergRecordAsFlussRow;
    private Record record;

    @BeforeEach
    void setUp() {
        icebergRecordAsFlussRow = new IcebergRecordAsFlussRow();

        // Create a schema with various data types
        Schema schema =
                new Schema(
                        required(1, "id", Types.LongType.get()),
                        optional(2, "name", Types.StringType.get()),
                        optional(3, "age", Types.IntegerType.get()),
                        optional(4, "salary", Types.DoubleType.get()),
                        optional(5, "is_active", Types.BooleanType.get()),
                        optional(6, "tiny_int", Types.IntegerType.get()),
                        optional(7, "small_int", Types.IntegerType.get()),
                        optional(8, "float_val", Types.FloatType.get()),
                        optional(9, "decimal_val", Types.DecimalType.of(10, 2)),
                        optional(10, "timestamp_ntz", Types.TimestampType.withoutZone()),
                        optional(11, "timestamp_ltz", Types.TimestampType.withZone()),
                        optional(12, "binary_data", Types.BinaryType.get()),
                        optional(13, "char_data", Types.StringType.get()),
                        optional(
                                14,
                                "nested_row",
                                Types.StructType.of(
                                        required(15, "city", Types.StringType.get()),
                                        required(
                                                16,
                                                "address",
                                                Types.StructType.of(
                                                        required(
                                                                17,
                                                                "subfield1",
                                                                Types.StringType.get()),
                                                        required(
                                                                18,
                                                                "subfield2",
                                                                Types.IntegerType.get()))))),
                        optional(
                                19,
                                "map_field",
                                Types.MapType.ofOptional(
                                        20, 21, Types.StringType.get(), Types.IntegerType.get())),
                        optional(25, "date_field", Types.DateType.get()),
                        optional(26, "time_field", Types.TimeType.get()),
                        optional(
                                27,
                                "date_array",
                                Types.ListType.ofOptional(28, Types.DateType.get())),
                        optional(
                                29,
                                "time_array",
                                Types.ListType.ofOptional(30, Types.TimeType.get())),
                        required(22, "__bucket", Types.IntegerType.get()),
                        required(23, "__offset", Types.LongType.get()),
                        required(24, "__timestamp", Types.TimestampType.withZone()));

        record = GenericRecord.create(schema);
    }

    @Test
    void testGetFieldCount() {
        // Set up record with data
        record.setField("id", 1L);
        record.setField("name", "John");
        record.setField("age", 30);
        record.setField("__bucket", 1);
        record.setField("__offset", 100L);
        record.setField("__timestamp", OffsetDateTime.now(ZoneOffset.UTC));

        icebergRecordAsFlussRow.replaceIcebergRecord(record);

        assertThat(icebergRecordAsFlussRow.getFieldCount()).isEqualTo(19);
    }

    @Test
    void testIsNullAt() {
        record.setField("id", 1L);
        record.setField("name", null); // null value
        record.setField("age", 30);

        icebergRecordAsFlussRow.replaceIcebergRecord(record);

        assertThat(icebergRecordAsFlussRow.isNullAt(0)).isFalse(); // id
        assertThat(icebergRecordAsFlussRow.isNullAt(1)).isTrue(); // name
        assertThat(icebergRecordAsFlussRow.isNullAt(2)).isFalse(); // age
    }

    @Test
    void testAllDataTypes() {
        // Set up all data types with test values
        record.setField("id", 12345L);
        record.setField("name", "John Doe");
        record.setField("age", 30);
        record.setField("salary", 50000.50);
        record.setField("is_active", true);
        record.setField("tiny_int", 127);
        record.setField("small_int", 32767);
        record.setField("float_val", 3.14f);
        record.setField("decimal_val", new BigDecimal("123.45"));
        record.setField("timestamp_ntz", LocalDateTime.of(2023, 12, 25, 10, 30, 45));
        record.setField(
                "timestamp_ltz", OffsetDateTime.of(2023, 12, 25, 10, 30, 45, 0, ZoneOffset.UTC));
        record.setField("binary_data", ByteBuffer.wrap("Hello World".getBytes()));
        record.setField("char_data", "Hello");
        // System columns
        record.setField("__bucket", 1);
        record.setField("__offset", 100L);
        record.setField("__timestamp", OffsetDateTime.now(ZoneOffset.UTC));

        icebergRecordAsFlussRow.replaceIcebergRecord(record);

        // Test all data type conversions
        assertThat(icebergRecordAsFlussRow.getLong(0)).isEqualTo(12345L); // id
        assertThat(icebergRecordAsFlussRow.getString(1).toString()).isEqualTo("John Doe"); // name
        assertThat(icebergRecordAsFlussRow.getInt(2)).isEqualTo(30); // age
        assertThat(icebergRecordAsFlussRow.getDouble(3)).isEqualTo(50000.50); // salary
        assertThat(icebergRecordAsFlussRow.getBoolean(4)).isTrue(); // is_active
        assertThat(icebergRecordAsFlussRow.getByte(5)).isEqualTo((byte) 127); // tiny_int
        assertThat(icebergRecordAsFlussRow.getShort(6)).isEqualTo((short) 32767); // small_int
        assertThat(icebergRecordAsFlussRow.getFloat(7)).isEqualTo(3.14f); // float_val
        assertThat(icebergRecordAsFlussRow.getDecimal(8, 10, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("123.45")); // decimal_val
        assertThat(icebergRecordAsFlussRow.getTimestampNtz(9, 3).toLocalDateTime())
                .isEqualTo(LocalDateTime.of(2023, 12, 25, 10, 30, 45)); // timestamp_ntz
        assertThat(icebergRecordAsFlussRow.getTimestampLtz(10, 3).toInstant())
                .isEqualTo(
                        OffsetDateTime.of(2023, 12, 25, 10, 30, 45, 0, ZoneOffset.UTC)
                                .toInstant()); // timestamp_ltz
        assertThat(icebergRecordAsFlussRow.getBytes(11))
                .isEqualTo("Hello World".getBytes()); // binary_data
        assertThat(icebergRecordAsFlussRow.getChar(12, 10).toString())
                .isEqualTo("Hello"); // char_data

        assertThat(icebergRecordAsFlussRow.getFieldCount()).isEqualTo(19);
    }

    @Test
    void testGetIntWithLocalDateAndLocalTime() {
        LocalDate date = LocalDate.of(2020, 6, 15);
        LocalTime time = LocalTime.of(14, 30, 0);

        record.setField("id", 1L);
        record.setField("date_field", date);
        record.setField("time_field", time);
        record.setField("date_array", Arrays.asList(date));
        record.setField("time_array", Arrays.asList(time));
        record.setField("__bucket", 1);
        record.setField("__offset", 100L);
        record.setField("__timestamp", OffsetDateTime.now(ZoneOffset.UTC));

        icebergRecordAsFlussRow.replaceIcebergRecord(record);

        assertThat(icebergRecordAsFlussRow.getInt(15)).isEqualTo((int) date.toEpochDay());
        assertThat(icebergRecordAsFlussRow.getInt(16))
                .isEqualTo((int) time.toNanoOfDay() / 1_000_000);

        InternalArray dateArray = icebergRecordAsFlussRow.getArray(17);
        InternalArray timeArray = icebergRecordAsFlussRow.getArray(18);
        assertThat(dateArray.getInt(0)).isEqualTo((int) date.toEpochDay());
        assertThat(timeArray.getInt(0)).isEqualTo((int) time.toNanoOfDay() / 1_000_000);
    }

    @Test
    void testNestedRow() {
        String cityValue = "Seoul";
        String subfield1Value = "string value";
        Integer subfield2Value = 12345;
        Record nestedRecord =
                GenericRecord.create(record.struct().fields().get(13).type().asStructType());
        nestedRecord.setField("city", "Seoul");

        Record deepNestedRecord =
                GenericRecord.create(nestedRecord.struct().fields().get(1).type().asStructType());
        deepNestedRecord.setField("subfield1", subfield1Value);
        deepNestedRecord.setField("subfield2", subfield2Value);

        nestedRecord.setField("address", deepNestedRecord);

        record.setField("nested_row", nestedRecord);
        icebergRecordAsFlussRow.replaceIcebergRecord(record);

        InternalRow nestedRow = icebergRecordAsFlussRow.getRow(13, 2);
        assertThat(nestedRow).isNotNull();
        assertThat(nestedRow.getString(0).toString()).isEqualTo(cityValue);

        InternalRow deepNestedRow = nestedRow.getRow(1, 2);
        assertThat(deepNestedRow).isNotNull();
        assertThat(deepNestedRow.getString(0).toString()).isEqualTo(subfield1Value);
        assertThat(deepNestedRow.getInt(1)).isEqualTo(subfield2Value);
    }

    @Test
    void testMapType() {
        // Create a simple map with String keys and Integer values
        Map<String, Integer> mapData = new HashMap<>();
        mapData.put("key1", 100);
        mapData.put("key2", 200);
        mapData.put("key3", 300);

        record.setField("id", 1L);
        record.setField("map_field", mapData);
        // System columns
        record.setField("__bucket", 1);
        record.setField("__offset", 100L);
        record.setField("__timestamp", OffsetDateTime.now(ZoneOffset.UTC));

        icebergRecordAsFlussRow.replaceIcebergRecord(record);

        // Test map retrieval - map_field is at index 14
        org.apache.fluss.row.InternalMap internalMap = icebergRecordAsFlussRow.getMap(14);
        assertThat(internalMap).isNotNull();
        assertThat(internalMap.size()).isEqualTo(3);

        // Verify map contents by iterating through keys and values
        org.apache.fluss.row.InternalArray keyArray = internalMap.keyArray();
        org.apache.fluss.row.InternalArray valueArray = internalMap.valueArray();

        Map<String, Integer> resultMap = new HashMap<>();
        for (int i = 0; i < internalMap.size(); i++) {
            String key = keyArray.getString(i).toString();
            int value = valueArray.getInt(i);
            resultMap.put(key, value);
        }

        assertThat(resultMap).containsEntry("key1", 100);
        assertThat(resultMap).containsEntry("key2", 200);
        assertThat(resultMap).containsEntry("key3", 300);

        // Test null map
        record.setField("map_field", null);
        icebergRecordAsFlussRow.replaceIcebergRecord(record);
        assertThat(icebergRecordAsFlussRow.isNullAt(14)).isTrue();
    }
}

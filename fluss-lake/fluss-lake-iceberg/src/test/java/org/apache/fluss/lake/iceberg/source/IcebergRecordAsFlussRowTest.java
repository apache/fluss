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

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

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
                        // System columns
                        required(14, "__bucket", Types.IntegerType.get()),
                        required(15, "__offset", Types.LongType.get()),
                        required(16, "__timestamp", Types.TimestampType.withZone()));

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

        // Should return count excluding system columns (3 system columns)
        assertThat(icebergRecordAsFlussRow.getFieldCount()).isEqualTo(13);
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

        // Test field count (excluding system columns)
        assertThat(icebergRecordAsFlussRow.getFieldCount()).isEqualTo(13);
    }
}

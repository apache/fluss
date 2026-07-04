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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.metadata.TableDescriptor.BUCKET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.OFFSET_COLUMN_NAME;
import static org.apache.fluss.metadata.TableDescriptor.TIMESTAMP_COLUMN_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FlussRecordAsIcebergRecord}.
 *
 * <p>These tests focus on the tiering-only behavior added on top of {@link
 * org.apache.fluss.lake.iceberg.source.FlussRowAsIcebergRecord}: dispatch of the three system
 * columns ({@code __bucket}, {@code __offset}, {@code __timestamp}) by both positional and
 * name-based access, field count validation in {@code setFlussRecord}, and correct propagation of
 * complex column types (ARRAY / ROW / MAP) through the parent wrapper.
 *
 * <p>The coverage matrix mirrors {@code FlussRecordAsPaimonRowTest} so log-table tiering into
 * Iceberg has the same guardrails as tiering into Paimon.
 */
class FlussRecordAsIcebergRecordTest {

    /** Field id counter for building Iceberg struct types in tests. */
    private static int fieldId;

    private static Types.NestedField required(String name, org.apache.iceberg.types.Type type) {
        return Types.NestedField.required(++fieldId, name, type);
    }

    private static Types.NestedField optional(String name, org.apache.iceberg.types.Type type) {
        return Types.NestedField.optional(++fieldId, name, type);
    }

    private static Types.ListType listOfRequired(org.apache.iceberg.types.Type element) {
        return Types.ListType.ofRequired(++fieldId, element);
    }

    private static Types.ListType listOfOptional(org.apache.iceberg.types.Type element) {
        return Types.ListType.ofOptional(++fieldId, element);
    }

    private static Types.MapType mapOfStringToInt() {
        int keyId = ++fieldId;
        int valueId = ++fieldId;
        return Types.MapType.ofRequired(
                keyId, valueId, Types.StringType.get(), Types.IntegerType.get());
    }

    private static Types.NestedField systemBucket() {
        return Types.NestedField.required(++fieldId, BUCKET_COLUMN_NAME, Types.IntegerType.get());
    }

    private static Types.NestedField systemOffset() {
        return Types.NestedField.required(++fieldId, OFFSET_COLUMN_NAME, Types.LongType.get());
    }

    private static Types.NestedField systemTimestamp() {
        return Types.NestedField.required(
                ++fieldId, TIMESTAMP_COLUMN_NAME, Types.TimestampType.withZone());
    }

    @Test
    void testLogTableRecordAllTypes() {
        fieldId = 0;
        Types.StructType structType =
                Types.StructType.of(
                        required("f_boolean", Types.BooleanType.get()),
                        required("f_tinyint", Types.IntegerType.get()),
                        required("f_smallint", Types.IntegerType.get()),
                        required("f_int", Types.IntegerType.get()),
                        required("f_bigint", Types.LongType.get()),
                        required("f_float", Types.FloatType.get()),
                        required("f_double", Types.DoubleType.get()),
                        required("f_string", Types.StringType.get()),
                        required("f_decimal_5_2", Types.DecimalType.of(5, 2)),
                        required("f_decimal_20_0", Types.DecimalType.of(20, 0)),
                        required("f_ts_ltz", Types.TimestampType.withZone()),
                        required("f_ts_ntz", Types.TimestampType.withoutZone()),
                        required("f_binary", Types.BinaryType.get()),
                        optional("f_null_string", Types.StringType.get()),
                        systemBucket(),
                        systemOffset(),
                        systemTimestamp());

        RowType flussRowType =
                RowType.of(
                        DataTypes.BOOLEAN(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.STRING(),
                        DataTypes.DECIMAL(5, 2),
                        DataTypes.DECIMAL(20, 0),
                        DataTypes.TIMESTAMP_LTZ(6),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.BYTES(),
                        DataTypes.STRING());

        int tableBucket = 3;
        long logOffset = 100L;
        long timestamp = 1698235273182L;

        GenericRow genericRow = new GenericRow(14);
        genericRow.setField(0, true);
        genericRow.setField(1, (byte) 1);
        genericRow.setField(2, (short) 2);
        genericRow.setField(3, 3);
        genericRow.setField(4, 4L);
        genericRow.setField(5, 5.1f);
        genericRow.setField(6, 6.0d);
        genericRow.setField(7, BinaryString.fromString("string"));
        genericRow.setField(8, Decimal.fromUnscaledLong(9, 5, 2));
        genericRow.setField(9, Decimal.fromBigDecimal(new BigDecimal(10), 20, 0));
        genericRow.setField(10, TimestampLtz.fromEpochMillis(timestamp));
        genericRow.setField(11, TimestampNtz.fromMillis(timestamp));
        genericRow.setField(12, new byte[] {1, 2, 3, 4});
        genericRow.setField(13, null);

        FlussRecordAsIcebergRecord record =
                new FlussRecordAsIcebergRecord(tableBucket, structType, flussRowType);
        LogRecord logRecord =
                new GenericRecord(logOffset, timestamp, ChangeType.APPEND_ONLY, genericRow);
        record.setFlussRecord(logRecord);

        // Business columns delegate to the parent wrapper.
        assertThat(record.get(0)).isEqualTo(true);
        assertThat(record.get(1)).isEqualTo(1);
        assertThat(record.get(2)).isEqualTo(2);
        assertThat(record.get(3)).isEqualTo(3);
        assertThat(record.get(4)).isEqualTo(4L);
        assertThat(record.get(5)).isEqualTo(5.1f);
        assertThat(record.get(6)).isEqualTo(6.0d);
        assertThat(record.get(7)).isEqualTo("string");
        assertThat(record.get(8)).isEqualTo(new BigDecimal("0.09"));
        assertThat(record.get(9)).isEqualTo(new BigDecimal(10));
        assertThat(record.get(10)).isEqualTo(expectedLtz(timestamp));
        assertThat(record.get(11)).isEqualTo(TimestampNtz.fromMillis(timestamp).toLocalDateTime());
        assertThat(record.get(12)).isEqualTo(java.nio.ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
        assertThat(record.get(13)).isNull();

        // System columns are answered by the tiering wrapper via positional access.
        assertThat(record.get(14)).isEqualTo(tableBucket);
        assertThat(record.get(15)).isEqualTo(logOffset);
        assertThat(record.get(16)).isEqualTo(expectedLtz(timestamp));

        // System columns are also answered via name-based access.
        assertThat(record.getField(BUCKET_COLUMN_NAME)).isEqualTo(tableBucket);
        assertThat(record.getField(OFFSET_COLUMN_NAME)).isEqualTo(logOffset);
        assertThat(record.getField(TIMESTAMP_COLUMN_NAME)).isEqualTo(expectedLtz(timestamp));

        // The struct size includes both business and system columns.
        assertThat(record.size()).isEqualTo(17);
    }

    @Test
    void testPrimaryKeyTableRecordAllChangeTypes() {
        fieldId = 0;
        Types.StructType structType =
                Types.StructType.of(
                        required("f_int", Types.IntegerType.get()),
                        systemBucket(),
                        systemOffset(),
                        systemTimestamp());
        RowType flussRowType = RowType.of(DataTypes.INT());

        int tableBucket = 7;
        FlussRecordAsIcebergRecord record =
                new FlussRecordAsIcebergRecord(tableBucket, structType, flussRowType);

        // Reuse the same wrapper across multiple records: verify that offset / timestamp are
        // refreshed each time setFlussRecord is called, regardless of ChangeType.
        for (ChangeType type :
                new ChangeType[] {
                    ChangeType.INSERT,
                    ChangeType.UPDATE_BEFORE,
                    ChangeType.UPDATE_AFTER,
                    ChangeType.DELETE,
                    ChangeType.APPEND_ONLY
                }) {
            long offset = 42L + type.ordinal();
            long ts = 1_700_000_000_000L + type.ordinal();
            GenericRow row = new GenericRow(1);
            row.setField(0, 100 + type.ordinal());
            record.setFlussRecord(new GenericRecord(offset, ts, type, row));

            assertThat(record.get(0)).isEqualTo(100 + type.ordinal());
            assertThat(record.get(1)).isEqualTo(tableBucket);
            assertThat(record.get(2)).isEqualTo(offset);
            assertThat(record.get(3)).isEqualTo(expectedLtz(ts));
        }
    }

    @Test
    void testFieldCountMismatchThrows() {
        // struct has 2 business columns + 3 system columns = 5, but flussRowType has only 1
        // business column. The wrapper requires: fluss.fieldCount == struct.size - system.
        fieldId = 0;
        Types.StructType structType =
                Types.StructType.of(
                        required("f_int", Types.IntegerType.get()),
                        required("f_string", Types.StringType.get()),
                        systemBucket(),
                        systemOffset(),
                        systemTimestamp());
        RowType flussRowType = RowType.of(DataTypes.INT());

        FlussRecordAsIcebergRecord record =
                new FlussRecordAsIcebergRecord(0, structType, flussRowType);
        GenericRow row = new GenericRow(1);
        row.setField(0, 1);
        LogRecord logRecord = new GenericRecord(0L, 0L, ChangeType.APPEND_ONLY, row);

        assertThatThrownBy(() -> record.setFlussRecord(logRecord))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fields count");
    }

    @Test
    void testArrayTypeWithIntElements() {
        fieldId = 0;
        Types.StructType structType =
                Types.StructType.of(
                        required("f_int", Types.IntegerType.get()),
                        required("f_array", listOfRequired(Types.IntegerType.get())),
                        systemBucket(),
                        systemOffset(),
                        systemTimestamp());
        RowType flussRowType = RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()));

        int tableBucket = 0;
        long logOffset = 10L;
        long timestamp = 1_700_000_000_000L;

        GenericRow row = new GenericRow(2);
        row.setField(0, 42);
        row.setField(1, new GenericArray(new int[] {1, 2, 3, 4, 5}));

        FlussRecordAsIcebergRecord record =
                new FlussRecordAsIcebergRecord(tableBucket, structType, flussRowType);
        record.setFlussRecord(new GenericRecord(logOffset, timestamp, ChangeType.APPEND_ONLY, row));

        assertThat(record.get(0)).isEqualTo(42);
        @SuppressWarnings("unchecked")
        List<Object> array = (List<Object>) record.get(1);
        assertThat(array).containsExactly(1, 2, 3, 4, 5);

        // System columns are still accessible after complex column dispatch.
        assertThat(record.get(2)).isEqualTo(tableBucket);
        assertThat(record.get(3)).isEqualTo(logOffset);
        assertThat(record.get(4)).isEqualTo(expectedLtz(timestamp));
    }

    @Test
    void testNestedArrayType() {
        fieldId = 0;
        Types.StructType structType =
                Types.StructType.of(
                        required(
                                "f_nested_array",
                                listOfRequired(listOfRequired(Types.IntegerType.get()))),
                        systemBucket(),
                        systemOffset(),
                        systemTimestamp());
        RowType flussRowType = RowType.of(DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())));

        GenericRow row = new GenericRow(1);
        row.setField(
                0,
                new GenericArray(
                        new Object[] {
                            new GenericArray(new int[] {1, 2}),
                            new GenericArray(new int[] {3, 4, 5})
                        }));

        FlussRecordAsIcebergRecord record =
                new FlussRecordAsIcebergRecord(1, structType, flussRowType);
        record.setFlussRecord(new GenericRecord(0L, 0L, ChangeType.APPEND_ONLY, row));

        @SuppressWarnings("unchecked")
        List<Object> outer = (List<Object>) record.get(0);
        assertThat(outer).hasSize(2);
        @SuppressWarnings("unchecked")
        List<Object> outer0 = (List<Object>) outer.get(0);
        @SuppressWarnings("unchecked")
        List<Object> outer1 = (List<Object>) outer.get(1);
        assertThat(outer0).containsExactly(1, 2);
        assertThat(outer1).containsExactly(3, 4, 5);
    }

    @Test
    void testArrayWithNullableElementsAndEmptyAndNullArray() {
        fieldId = 0;
        Types.StructType structType =
                Types.StructType.of(
                        required("f_nullable_ints", listOfOptional(Types.IntegerType.get())),
                        required("f_empty_ints", listOfRequired(Types.IntegerType.get())),
                        optional("f_null_ints", listOfRequired(Types.IntegerType.get())),
                        systemBucket(),
                        systemOffset(),
                        systemTimestamp());
        RowType flussRowType =
                RowType.of(
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.ARRAY(DataTypes.INT()));

        GenericRow row = new GenericRow(3);
        row.setField(0, new GenericArray(new Object[] {1, null, 3}));
        row.setField(1, new GenericArray(new int[] {}));
        row.setField(2, null);

        FlussRecordAsIcebergRecord record =
                new FlussRecordAsIcebergRecord(0, structType, flussRowType);
        record.setFlussRecord(new GenericRecord(0L, 0L, ChangeType.APPEND_ONLY, row));

        @SuppressWarnings("unchecked")
        List<Object> nullable = (List<Object>) record.get(0);
        assertThat(nullable).containsExactly(1, null, 3);
        List<?> empty = (List<?>) record.get(1);
        assertThat(empty).isEmpty();
        assertThat(record.get(2)).isNull();
    }

    @Test
    void testMapTypeWithStringToIntAndEmptyAndNullMap() {
        fieldId = 0;
        Types.StructType structType =
                Types.StructType.of(
                        required("f_map", mapOfStringToInt()),
                        required("f_empty_map", mapOfStringToInt()),
                        Types.NestedField.optional(
                                ++fieldId,
                                "f_null_map",
                                Types.MapType.ofRequired(
                                        ++fieldId,
                                        ++fieldId,
                                        Types.StringType.get(),
                                        Types.IntegerType.get())),
                        systemBucket(),
                        systemOffset(),
                        systemTimestamp());
        RowType flussRowType =
                RowType.of(
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));

        Map<Object, Object> populated = new LinkedHashMap<>();
        populated.put(BinaryString.fromString("a"), 1);
        populated.put(BinaryString.fromString("b"), 2);

        GenericRow row = new GenericRow(3);
        row.setField(0, new GenericMap(populated));
        row.setField(1, new GenericMap(new LinkedHashMap<>()));
        row.setField(2, null);

        FlussRecordAsIcebergRecord record =
                new FlussRecordAsIcebergRecord(0, structType, flussRowType);
        record.setFlussRecord(new GenericRecord(0L, 0L, ChangeType.APPEND_ONLY, row));

        Map<?, ?> map = (Map<?, ?>) record.get(0);
        assertThat(map).hasSize(2);
        assertThat(map.get("a")).isEqualTo(1);
        assertThat(map.get("b")).isEqualTo(2);

        Map<?, ?> emptyMap = (Map<?, ?>) record.get(1);
        assertThat(emptyMap).isEmpty();

        assertThat(record.get(2)).isNull();
    }

    @Test
    void testNestedRowType() {
        fieldId = 0;
        Types.StructType nestedStruct =
                Types.StructType.of(
                        Types.NestedField.required(++fieldId, "n_int", Types.IntegerType.get()),
                        Types.NestedField.optional(++fieldId, "n_string", Types.StringType.get()));
        Types.StructType structType =
                Types.StructType.of(
                        required("f_int", Types.IntegerType.get()),
                        Types.NestedField.required(++fieldId, "f_row", nestedStruct),
                        Types.NestedField.optional(
                                ++fieldId,
                                "f_null_row",
                                Types.StructType.of(
                                        Types.NestedField.required(
                                                ++fieldId, "n_int", Types.IntegerType.get()))),
                        systemBucket(),
                        systemOffset(),
                        systemTimestamp());
        RowType flussRowType =
                RowType.of(
                        DataTypes.INT(),
                        DataTypes.ROW(
                                DataTypes.FIELD("n_int", DataTypes.INT()),
                                DataTypes.FIELD("n_string", DataTypes.STRING())),
                        DataTypes.ROW(DataTypes.FIELD("n_int", DataTypes.INT())));

        GenericRow nested = new GenericRow(2);
        nested.setField(0, 42);
        nested.setField(1, BinaryString.fromString("hello"));

        GenericRow row = new GenericRow(3);
        row.setField(0, 1);
        row.setField(1, nested);
        row.setField(2, null);

        FlussRecordAsIcebergRecord record =
                new FlussRecordAsIcebergRecord(0, structType, flussRowType);
        record.setFlussRecord(new GenericRecord(0L, 0L, ChangeType.APPEND_ONLY, row));

        assertThat(record.get(0)).isEqualTo(1);
        Record nestedRecord = (Record) record.get(1);
        assertThat(nestedRecord.get(0)).isEqualTo(42);
        assertThat(nestedRecord.get(1)).isEqualTo("hello");

        // The parent wrapper short-circuits null business rows via isNullAt(pos), so a null
        // nested ROW column surfaces as a null value rather than a wrapper with a null inner row.
        assertThat(record.get(2)).isNull();
    }

    private static OffsetDateTime expectedLtz(long millis) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}

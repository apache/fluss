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

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link FlussRowAsIcebergRecord} with array types. */
class FlussRowAsIcebergRecordTest {

    @Test
    void testArrayWithIntElements() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(0, "id", Types.IntegerType.get()),
                        Types.NestedField.required(
                                1,
                                "int_array",
                                Types.ListType.ofRequired(2, Types.IntegerType.get())));

        RowType flussRowType = RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()));

        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 42);
        genericRow.setField(1, new GenericArray(new int[] {1, 2, 3, 4, 5}));

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        assertThat(record.get(0)).isEqualTo(42);
        List<?> array = (List<?>) record.get(1);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(5);
        assertThat(array.get(0)).isEqualTo(1);
        assertThat(array.get(1)).isEqualTo(2);
        assertThat(array.get(2)).isEqualTo(3);
        assertThat(array.get(3)).isEqualTo(4);
        assertThat(array.get(4)).isEqualTo(5);
    }

    @Test
    void testArrayWithStringElements() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "string_array",
                                Types.ListType.ofRequired(1, Types.StringType.get())));

        RowType flussRowType = RowType.of(DataTypes.ARRAY(DataTypes.STRING()));

        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            BinaryString.fromString("hello"), BinaryString.fromString("world")
                        }));

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        List<?> array = (List<?>) record.get(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.get(0)).isEqualTo("hello");
        assertThat(array.get(1)).isEqualTo("world");
    }

    @Test
    void testNestedArrayType() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "nested_array",
                                Types.ListType.ofRequired(
                                        1, Types.ListType.ofRequired(2, Types.IntegerType.get()))));

        RowType flussRowType = RowType.of(DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())));

        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            new GenericArray(new int[] {1, 2}),
                            new GenericArray(new int[] {3, 4, 5})
                        }));

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        List<?> outerArray = (List<?>) record.get(0);
        assertThat(outerArray).isNotNull();
        assertThat(outerArray.size()).isEqualTo(2);

        List<?> innerArray1 = (List<?>) outerArray.get(0);
        assertThat(innerArray1.size()).isEqualTo(2);
        assertThat(innerArray1.get(0)).isEqualTo(1);
        assertThat(innerArray1.get(1)).isEqualTo(2);

        List<?> innerArray2 = (List<?>) outerArray.get(1);
        assertThat(innerArray2.size()).isEqualTo(3);
        assertThat(innerArray2.get(0)).isEqualTo(3);
        assertThat(innerArray2.get(1)).isEqualTo(4);
        assertThat(innerArray2.get(2)).isEqualTo(5);
    }

    @Test
    void testArrayWithAllPrimitiveTypes() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "bool_array",
                                Types.ListType.ofRequired(1, Types.BooleanType.get())),
                        Types.NestedField.required(
                                2,
                                "byte_array",
                                Types.ListType.ofRequired(3, Types.IntegerType.get())),
                        Types.NestedField.required(
                                4,
                                "short_array",
                                Types.ListType.ofRequired(5, Types.IntegerType.get())),
                        Types.NestedField.required(
                                6,
                                "int_array",
                                Types.ListType.ofRequired(7, Types.IntegerType.get())),
                        Types.NestedField.required(
                                8,
                                "long_array",
                                Types.ListType.ofRequired(9, Types.LongType.get())),
                        Types.NestedField.required(
                                10,
                                "float_array",
                                Types.ListType.ofRequired(11, Types.FloatType.get())),
                        Types.NestedField.required(
                                12,
                                "double_array",
                                Types.ListType.ofRequired(13, Types.DoubleType.get())));

        RowType flussRowType =
                RowType.of(
                        DataTypes.ARRAY(DataTypes.BOOLEAN()),
                        DataTypes.ARRAY(DataTypes.TINYINT()),
                        DataTypes.ARRAY(DataTypes.SMALLINT()),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.ARRAY(DataTypes.BIGINT()),
                        DataTypes.ARRAY(DataTypes.FLOAT()),
                        DataTypes.ARRAY(DataTypes.DOUBLE()));

        GenericRow genericRow = new GenericRow(7);
        genericRow.setField(0, new GenericArray(new boolean[] {true, false, true}));
        genericRow.setField(1, new GenericArray(new byte[] {1, 2, 3}));
        genericRow.setField(2, new GenericArray(new short[] {100, 200, 300}));
        genericRow.setField(3, new GenericArray(new int[] {1000, 2000, 3000}));
        genericRow.setField(4, new GenericArray(new long[] {10000L, 20000L, 30000L}));
        genericRow.setField(5, new GenericArray(new float[] {1.1f, 2.2f, 3.3f}));
        genericRow.setField(6, new GenericArray(new double[] {1.11, 2.22, 3.33}));

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        List<?> boolArray = (List<?>) record.get(0);
        assertThat(boolArray.size()).isEqualTo(3);
        assertThat(boolArray.get(0)).isEqualTo(true);
        assertThat(boolArray.get(1)).isEqualTo(false);
        assertThat(boolArray.get(2)).isEqualTo(true);

        List<?> byteArray = (List<?>) record.get(1);
        assertThat(byteArray.size()).isEqualTo(3);
        assertThat(byteArray.get(0)).isEqualTo(1);
        assertThat(byteArray.get(1)).isEqualTo(2);
        assertThat(byteArray.get(2)).isEqualTo(3);

        List<?> shortArray = (List<?>) record.get(2);
        assertThat(shortArray.size()).isEqualTo(3);
        assertThat(shortArray.get(0)).isEqualTo(100);
        assertThat(shortArray.get(1)).isEqualTo(200);
        assertThat(shortArray.get(2)).isEqualTo(300);

        List<?> intArray = (List<?>) record.get(3);
        assertThat(intArray.size()).isEqualTo(3);
        assertThat(intArray.get(0)).isEqualTo(1000);
        assertThat(intArray.get(1)).isEqualTo(2000);
        assertThat(intArray.get(2)).isEqualTo(3000);

        List<?> longArray = (List<?>) record.get(4);
        assertThat(longArray.size()).isEqualTo(3);
        assertThat(longArray.get(0)).isEqualTo(10000L);
        assertThat(longArray.get(1)).isEqualTo(20000L);
        assertThat(longArray.get(2)).isEqualTo(30000L);

        List<?> floatArray = (List<?>) record.get(5);
        assertThat(floatArray.size()).isEqualTo(3);
        assertThat(floatArray.get(0)).isEqualTo(1.1f);
        assertThat(floatArray.get(1)).isEqualTo(2.2f);
        assertThat(floatArray.get(2)).isEqualTo(3.3f);

        List<?> doubleArray = (List<?>) record.get(6);
        assertThat(doubleArray.size()).isEqualTo(3);
        assertThat(doubleArray.get(0)).isEqualTo(1.11);
        assertThat(doubleArray.get(1)).isEqualTo(2.22);
        assertThat(doubleArray.get(2)).isEqualTo(3.33);
    }

    @Test
    void testArrayWithDecimalElements() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "decimal_array",
                                Types.ListType.ofRequired(1, Types.DecimalType.of(10, 2))));

        RowType flussRowType = RowType.of(DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)));

        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2),
                            Decimal.fromBigDecimal(new BigDecimal("678.90"), 10, 2)
                        }));

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        List<?> array = (List<?>) record.get(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.get(0)).isEqualTo(new BigDecimal("123.45"));
        assertThat(array.get(1)).isEqualTo(new BigDecimal("678.90"));
    }

    @Test
    void testArrayWithTimestampElements() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "timestamp_ntz_array",
                                Types.ListType.ofRequired(1, Types.TimestampType.withoutZone())),
                        Types.NestedField.required(
                                2,
                                "timestamp_ltz_array",
                                Types.ListType.ofRequired(3, Types.TimestampType.withZone())));

        RowType flussRowType =
                RowType.of(
                        DataTypes.ARRAY(DataTypes.TIMESTAMP(6)),
                        DataTypes.ARRAY(DataTypes.TIMESTAMP_LTZ(6)));

        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            org.apache.fluss.row.TimestampNtz.fromLocalDateTime(
                                    LocalDateTime.now()),
                            org.apache.fluss.row.TimestampNtz.fromLocalDateTime(
                                    LocalDateTime.now().plusSeconds(1))
                        }));
        genericRow.setField(
                1,
                new GenericArray(
                        new Object[] {
                            org.apache.fluss.row.TimestampLtz.fromEpochMillis(
                                    System.currentTimeMillis()),
                            org.apache.fluss.row.TimestampLtz.fromEpochMillis(
                                    System.currentTimeMillis() + 1000)
                        }));

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        List<?> timestampNtzArray = (List<?>) record.get(0);
        assertThat(timestampNtzArray).isNotNull();
        assertThat(timestampNtzArray.size()).isEqualTo(2);
        assertThat(timestampNtzArray.get(0)).isInstanceOf(LocalDateTime.class);
        assertThat(timestampNtzArray.get(1)).isInstanceOf(LocalDateTime.class);

        List<?> timestampLtzArray = (List<?>) record.get(1);
        assertThat(timestampLtzArray).isNotNull();
        assertThat(timestampLtzArray.size()).isEqualTo(2);
        assertThat(timestampLtzArray.get(0)).isInstanceOf(OffsetDateTime.class);
        assertThat(timestampLtzArray.get(1)).isInstanceOf(OffsetDateTime.class);
    }

    @Test
    void testArrayWithNullElements() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "nullable_int_array",
                                Types.ListType.ofOptional(1, Types.IntegerType.get())));

        RowType flussRowType = RowType.of(DataTypes.ARRAY(DataTypes.INT()));

        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, new GenericArray(new Object[] {1, null, 3}));

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        List<?> array = (List<?>) record.get(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.get(0)).isEqualTo(1);
        assertThat(array.get(1)).isNull();
        assertThat(array.get(2)).isEqualTo(3);
    }

    @Test
    void testNullArray() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.optional(
                                0,
                                "nullable_array",
                                Types.ListType.ofRequired(1, Types.IntegerType.get())));

        RowType flussRowType = RowType.of(DataTypes.ARRAY(DataTypes.INT()));

        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, null);

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        assertThat(record.get(0)).isNull();
    }

    @Test
    void testArrayWithBinaryElements() {
        Types.StructType structType =
                Types.StructType.of(
                        Types.NestedField.required(
                                0,
                                "binary_array",
                                Types.ListType.ofRequired(1, Types.BinaryType.get())));

        RowType flussRowType = RowType.of(DataTypes.ARRAY(DataTypes.BYTES()));

        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {"hello".getBytes(), "world".getBytes(), "test".getBytes()}));

        FlussRowAsIcebergRecord record = new FlussRowAsIcebergRecord(structType, flussRowType);
        record.internalRow = genericRow;

        List<?> array = (List<?>) record.get(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.get(0)).isInstanceOf(ByteBuffer.class);
        assertThat(((ByteBuffer) array.get(0)).array()).isEqualTo("hello".getBytes());
        assertThat(((ByteBuffer) array.get(1)).array()).isEqualTo("world".getBytes());
        assertThat(((ByteBuffer) array.get(2)).array()).isEqualTo("test".getBytes());
    }
}

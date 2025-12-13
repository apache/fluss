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
 *
 *
 */

package org.apache.fluss.lake.paimon.source;

import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.apache.fluss.record.ChangeType.APPEND_ONLY;
import static org.apache.fluss.record.ChangeType.DELETE;
import static org.apache.fluss.record.ChangeType.INSERT;
import static org.apache.fluss.record.ChangeType.UPDATE_AFTER;
import static org.apache.fluss.record.ChangeType.UPDATE_BEFORE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test case for {@link FlussRowAsPaimonRow}. */
class FlussRowAsPaimonRowTest {
    @Test
    void testLogTableRecordAllTypes() {
        // Construct a FlussRowAsPaimonRow instance
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.BooleanType(),
                        new org.apache.paimon.types.TinyIntType(),
                        new org.apache.paimon.types.SmallIntType(),
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.FloatType(),
                        new org.apache.paimon.types.DoubleType(),
                        new org.apache.paimon.types.VarCharType(),
                        new org.apache.paimon.types.DecimalType(5, 2),
                        new org.apache.paimon.types.DecimalType(20, 0),
                        new org.apache.paimon.types.LocalZonedTimestampType(6),
                        new org.apache.paimon.types.TimestampType(6),
                        new org.apache.paimon.types.BinaryType(),
                        new org.apache.paimon.types.VarCharType());

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
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
        genericRow.setField(10, TimestampLtz.fromEpochMillis(1698235273182L, 5678));
        genericRow.setField(11, TimestampNtz.fromMillis(1698235273182L, 5678));
        genericRow.setField(12, new byte[] {1, 2, 3, 4});
        genericRow.setField(13, null);
        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        // verify FlussRecordAsPaimonRow normal columns
        assertThat(flussRowAsPaimonRow.getBoolean(0)).isTrue();
        assertThat(flussRowAsPaimonRow.getByte(1)).isEqualTo((byte) 1);
        assertThat(flussRowAsPaimonRow.getShort(2)).isEqualTo((short) 2);
        assertThat(flussRowAsPaimonRow.getInt(3)).isEqualTo(3);
        assertThat(flussRowAsPaimonRow.getLong(4)).isEqualTo(4L);
        assertThat(flussRowAsPaimonRow.getFloat(5)).isEqualTo(5.1f);
        assertThat(flussRowAsPaimonRow.getDouble(6)).isEqualTo(6.0d);
        assertThat(flussRowAsPaimonRow.getString(7).toString()).isEqualTo("string");
        assertThat(flussRowAsPaimonRow.getDecimal(8, 5, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("0.09"));
        assertThat(flussRowAsPaimonRow.getDecimal(9, 20, 0).toBigDecimal())
                .isEqualTo(new BigDecimal(10));
        assertThat(flussRowAsPaimonRow.getTimestamp(10, 6).getMillisecond())
                .isEqualTo(1698235273182L);
        assertThat(flussRowAsPaimonRow.getTimestamp(10, 6).getNanoOfMillisecond()).isEqualTo(5678);
        assertThat(flussRowAsPaimonRow.getTimestamp(11, 6).getMillisecond())
                .isEqualTo(1698235273182L);
        assertThat(flussRowAsPaimonRow.getTimestamp(11, 6).getNanoOfMillisecond()).isEqualTo(5678);
        assertThat(flussRowAsPaimonRow.getBinary(12)).isEqualTo(new byte[] {1, 2, 3, 4});
        assertThat(flussRowAsPaimonRow.isNullAt(13)).isTrue();
    }

    @Test
    void testPrimaryKeyTableRecord() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BooleanType());

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 10);
        genericRow.setField(1, true);

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, INSERT, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        assertThat(flussRowAsPaimonRow.getInt(0)).isEqualTo(10);
        // verify rowkind
        assertThat(flussRowAsPaimonRow.getRowKind()).isEqualTo(RowKind.INSERT);

        logRecord = new GenericRecord(logOffset, timeStamp, UPDATE_BEFORE, genericRow);
        assertThat(new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType).getRowKind())
                .isEqualTo(RowKind.INSERT);

        logRecord = new GenericRecord(logOffset, timeStamp, UPDATE_AFTER, genericRow);
        assertThat(new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType).getRowKind())
                .isEqualTo(RowKind.INSERT);

        logRecord = new GenericRecord(logOffset, timeStamp, DELETE, genericRow);
        assertThat(new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType).getRowKind())
                .isEqualTo(RowKind.INSERT);
    }

    @Test
    void testArrayTypeWithIntElements() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.IntType()));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 42);
        genericRow.setField(1, new GenericArray(new int[] {1, 2, 3, 4, 5}));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        assertThat(flussRowAsPaimonRow.getInt(0)).isEqualTo(42);
        InternalArray array = flussRowAsPaimonRow.getArray(1);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(5);
        assertThat(array.getInt(0)).isEqualTo(1);
        assertThat(array.getInt(1)).isEqualTo(2);
        assertThat(array.getInt(2)).isEqualTo(3);
        assertThat(array.getInt(3)).isEqualTo(4);
        assertThat(array.getInt(4)).isEqualTo(5);
    }

    @Test
    void testArrayTypeWithStringElements() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.VarCharType(),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.VarCharType()));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, BinaryString.fromString("name"));
        genericRow.setField(
                1,
                new GenericArray(
                        new Object[] {
                            BinaryString.fromString("a"),
                            BinaryString.fromString("b"),
                            BinaryString.fromString("c")
                        }));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        assertThat(flussRowAsPaimonRow.getString(0).toString()).isEqualTo("name");
        InternalArray array = flussRowAsPaimonRow.getArray(1);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getString(0).toString()).isEqualTo("a");
        assertThat(array.getString(1).toString()).isEqualTo("b");
        assertThat(array.getString(2).toString()).isEqualTo("c");
    }

    @Test
    void testArrayTypeWithNullableElements() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.IntType().nullable()));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, new GenericArray(new Object[] {1, null, 3}));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        InternalArray array = flussRowAsPaimonRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getInt(0)).isEqualTo(1);
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(array.getInt(2)).isEqualTo(3);
    }

    @Test
    void testNullArray() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(new org.apache.paimon.types.IntType())
                                .nullable());

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, null);

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        assertThat(flussRowAsPaimonRow.isNullAt(0)).isTrue();
    }

    @Test
    void testNestedArrayType() {
        // Test ARRAY<ARRAY<INT>>
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.ArrayType(
                                        new org.apache.paimon.types.IntType())));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            new GenericArray(new int[] {1, 2}),
                            new GenericArray(new int[] {3, 4, 5})
                        }));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        InternalArray outerArray = flussRowAsPaimonRow.getArray(0);
        assertThat(outerArray).isNotNull();
        assertThat(outerArray.size()).isEqualTo(2);

        InternalArray innerArray1 = outerArray.getArray(0);
        assertThat(innerArray1.size()).isEqualTo(2);
        assertThat(innerArray1.getInt(0)).isEqualTo(1);
        assertThat(innerArray1.getInt(1)).isEqualTo(2);

        InternalArray innerArray2 = outerArray.getArray(1);
        assertThat(innerArray2.size()).isEqualTo(3);
        assertThat(innerArray2.getInt(0)).isEqualTo(3);
        assertThat(innerArray2.getInt(1)).isEqualTo(4);
        assertThat(innerArray2.getInt(2)).isEqualTo(5);
    }

    @Test
    void testEmptyArray() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.IntType()));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, new GenericArray(new int[] {}));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        InternalArray array = flussRowAsPaimonRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(0);
    }

    @Test
    void testArrayWithAllPrimitiveTypes() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.BooleanType()),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.TinyIntType()),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.SmallIntType()),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.IntType()),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.BigIntType()),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.FloatType()),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.DoubleType()));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(7);
        genericRow.setField(0, new GenericArray(new boolean[] {true, false, true}));
        genericRow.setField(1, new GenericArray(new byte[] {1, 2, 3}));
        genericRow.setField(2, new GenericArray(new short[] {100, 200, 300}));
        genericRow.setField(3, new GenericArray(new int[] {1000, 2000, 3000}));
        genericRow.setField(4, new GenericArray(new long[] {10000L, 20000L, 30000L}));
        genericRow.setField(5, new GenericArray(new float[] {1.1f, 2.2f, 3.3f}));
        genericRow.setField(6, new GenericArray(new double[] {1.11, 2.22, 3.33}));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        // Test boolean array
        InternalArray boolArray = flussRowAsPaimonRow.getArray(0);
        assertThat(boolArray.size()).isEqualTo(3);
        assertThat(boolArray.getBoolean(0)).isTrue();
        assertThat(boolArray.getBoolean(1)).isFalse();
        assertThat(boolArray.getBoolean(2)).isTrue();
        assertThat(boolArray.toBooleanArray()).isEqualTo(new boolean[] {true, false, true});

        // Test byte array
        InternalArray byteArray = flussRowAsPaimonRow.getArray(1);
        assertThat(byteArray.size()).isEqualTo(3);
        assertThat(byteArray.getByte(0)).isEqualTo((byte) 1);
        assertThat(byteArray.getByte(1)).isEqualTo((byte) 2);
        assertThat(byteArray.getByte(2)).isEqualTo((byte) 3);
        assertThat(byteArray.toByteArray()).isEqualTo(new byte[] {1, 2, 3});

        // Test short array
        InternalArray shortArray = flussRowAsPaimonRow.getArray(2);
        assertThat(shortArray.size()).isEqualTo(3);
        assertThat(shortArray.getShort(0)).isEqualTo((short) 100);
        assertThat(shortArray.getShort(1)).isEqualTo((short) 200);
        assertThat(shortArray.getShort(2)).isEqualTo((short) 300);
        assertThat(shortArray.toShortArray()).isEqualTo(new short[] {100, 200, 300});

        // Test int array
        InternalArray intArray = flussRowAsPaimonRow.getArray(3);
        assertThat(intArray.size()).isEqualTo(3);
        assertThat(intArray.getInt(0)).isEqualTo(1000);
        assertThat(intArray.getInt(1)).isEqualTo(2000);
        assertThat(intArray.getInt(2)).isEqualTo(3000);
        assertThat(intArray.toIntArray()).isEqualTo(new int[] {1000, 2000, 3000});

        // Test long array
        InternalArray longArray = flussRowAsPaimonRow.getArray(4);
        assertThat(longArray.size()).isEqualTo(3);
        assertThat(longArray.getLong(0)).isEqualTo(10000L);
        assertThat(longArray.getLong(1)).isEqualTo(20000L);
        assertThat(longArray.getLong(2)).isEqualTo(30000L);
        assertThat(longArray.toLongArray()).isEqualTo(new long[] {10000L, 20000L, 30000L});

        // Test float array
        InternalArray floatArray = flussRowAsPaimonRow.getArray(5);
        assertThat(floatArray.size()).isEqualTo(3);
        assertThat(floatArray.getFloat(0)).isEqualTo(1.1f);
        assertThat(floatArray.getFloat(1)).isEqualTo(2.2f);
        assertThat(floatArray.getFloat(2)).isEqualTo(3.3f);
        assertThat(floatArray.toFloatArray()).isEqualTo(new float[] {1.1f, 2.2f, 3.3f});

        // Test double array
        InternalArray doubleArray = flussRowAsPaimonRow.getArray(6);
        assertThat(doubleArray.size()).isEqualTo(3);
        assertThat(doubleArray.getDouble(0)).isEqualTo(1.11);
        assertThat(doubleArray.getDouble(1)).isEqualTo(2.22);
        assertThat(doubleArray.getDouble(2)).isEqualTo(3.33);
        assertThat(doubleArray.toDoubleArray()).isEqualTo(new double[] {1.11, 2.22, 3.33});
    }

    @Test
    void testArrayWithDecimalElements() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.DecimalType(10, 2)));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2),
                            Decimal.fromBigDecimal(new BigDecimal("678.90"), 10, 2)
                        }));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        InternalArray array = flussRowAsPaimonRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getDecimal(0, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("123.45"));
        assertThat(array.getDecimal(1, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("678.90"));
    }

    @Test
    void testArrayWithTimestampElements() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.TimestampType(3)));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            TimestampNtz.fromMillis(1698235273182L),
                            TimestampNtz.fromMillis(1698235274000L)
                        }));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        InternalArray array = flussRowAsPaimonRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getTimestamp(0, 3).getMillisecond()).isEqualTo(1698235273182L);
        assertThat(array.getTimestamp(1, 3).getMillisecond()).isEqualTo(1698235274000L);
    }

    @Test
    void testArrayWithBinaryElements() {
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.VarBinaryType()));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            new byte[] {1, 2, 3},
                            new byte[] {4, 5, 6, 7}
                        }));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        InternalArray array = flussRowAsPaimonRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getBinary(0)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(array.getBinary(1)).isEqualTo(new byte[] {4, 5, 6, 7});
    }
}

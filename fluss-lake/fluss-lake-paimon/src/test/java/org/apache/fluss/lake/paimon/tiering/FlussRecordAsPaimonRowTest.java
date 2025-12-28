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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.Timestamp;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlussRecordAsPaimonRow}. */
class FlussRecordAsPaimonRowTest {

    @Test
    void testLogTableRecordAllTypes() {
        // Construct a FlussRecordAsPaimonRow instance
        int tableBucket = 0;
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
                        new org.apache.paimon.types.VarCharType(),
                        // append three system columns: __bucket, __offset,__timestamp
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
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
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        // verify FlussRecordAsPaimonRow normal columns
        assertThat(flussRecordAsPaimonRow.getBoolean(0)).isTrue();
        assertThat(flussRecordAsPaimonRow.getByte(1)).isEqualTo((byte) 1);
        assertThat(flussRecordAsPaimonRow.getShort(2)).isEqualTo((short) 2);
        assertThat(flussRecordAsPaimonRow.getInt(3)).isEqualTo(3);
        assertThat(flussRecordAsPaimonRow.getLong(4)).isEqualTo(4L);
        assertThat(flussRecordAsPaimonRow.getFloat(5)).isEqualTo(5.1f);
        assertThat(flussRecordAsPaimonRow.getDouble(6)).isEqualTo(6.0d);
        assertThat(flussRecordAsPaimonRow.getString(7).toString()).isEqualTo("string");
        assertThat(flussRecordAsPaimonRow.getDecimal(8, 5, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("0.09"));
        assertThat(flussRecordAsPaimonRow.getDecimal(9, 20, 0).toBigDecimal())
                .isEqualTo(new BigDecimal(10));
        assertThat(flussRecordAsPaimonRow.getTimestamp(10, 6).getMillisecond())
                .isEqualTo(1698235273182L);
        assertThat(flussRecordAsPaimonRow.getTimestamp(10, 6).getNanoOfMillisecond())
                .isEqualTo(5678);
        assertThat(flussRecordAsPaimonRow.getTimestamp(11, 6).getMillisecond())
                .isEqualTo(1698235273182L);
        assertThat(flussRecordAsPaimonRow.getTimestamp(11, 6).getNanoOfMillisecond())
                .isEqualTo(5678);
        assertThat(flussRecordAsPaimonRow.getBinary(12)).isEqualTo(new byte[] {1, 2, 3, 4});
        assertThat(flussRecordAsPaimonRow.isNullAt(13)).isTrue();

        // verify FlussRecordAsPaimonRow system columns (no partition fields, so indices stay same)
        assertThat(flussRecordAsPaimonRow.getInt(14)).isEqualTo(tableBucket);
        assertThat(flussRecordAsPaimonRow.getLong(15)).isEqualTo(logOffset);
        assertThat(flussRecordAsPaimonRow.getLong(16)).isEqualTo(timeStamp);
        assertThat(flussRecordAsPaimonRow.getTimestamp(16, 4))
                .isEqualTo(Timestamp.fromEpochMillis(timeStamp));
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.INSERT);

        assertThat(flussRecordAsPaimonRow.getFieldCount())
                .isEqualTo(14 + 3); // business  + system = 14 + 0 + 3 = 17
    }

    @Test
    void testPrimaryKeyTableRecord() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.BooleanType(),
                        // append three system columns: __bucket, __offset,__timestamp
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));
        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, true);

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, INSERT, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        assertThat(flussRecordAsPaimonRow.getBoolean(0)).isTrue();
        // normal columns + system columns
        assertThat(flussRecordAsPaimonRow.getFieldCount()).isEqualTo(4);
        // verify rowkind
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.INSERT);

        logRecord = new GenericRecord(logOffset, timeStamp, UPDATE_BEFORE, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);

        logRecord = new GenericRecord(logOffset, timeStamp, UPDATE_AFTER, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.UPDATE_AFTER);

        logRecord = new GenericRecord(logOffset, timeStamp, DELETE, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);
        assertThat(flussRecordAsPaimonRow.getRowKind()).isEqualTo(RowKind.DELETE);
    }

    @Test
    void testArrayTypeWithIntElements() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.IntType()),
                        // system columns: __bucket, __offset, __timestamp
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
        long logOffset = 10;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, 42);
        genericRow.setField(1, new GenericArray(new int[] {1, 2, 3, 4, 5}));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        assertThat(flussRecordAsPaimonRow.getInt(0)).isEqualTo(42);
        InternalArray array = flussRecordAsPaimonRow.getArray(1);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(5);
        assertThat(array.getInt(0)).isEqualTo(1);
        assertThat(array.getInt(1)).isEqualTo(2);
        assertThat(array.getInt(2)).isEqualTo(3);
        assertThat(array.getInt(3)).isEqualTo(4);
        assertThat(array.getInt(4)).isEqualTo(5);

        // Verify system columns are still accessible
        assertThat(flussRecordAsPaimonRow.getInt(2)).isEqualTo(tableBucket);
        assertThat(flussRecordAsPaimonRow.getLong(3)).isEqualTo(logOffset);
        assertThat(flussRecordAsPaimonRow.getLong(4)).isEqualTo(timeStamp);
    }

    @Test
    void testArrayTypeWithStringElements() {
        int tableBucket = 1;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.VarCharType()),
                        // system columns: __bucket, __offset, __timestamp
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
        long logOffset = 5;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            BinaryString.fromString("hello"), BinaryString.fromString("world")
                        }));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, INSERT, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        InternalArray array = flussRecordAsPaimonRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getString(0).toString()).isEqualTo("hello");
        assertThat(array.getString(1).toString()).isEqualTo("world");
    }

    @Test
    void testNestedArrayType() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.ArrayType(
                                        new org.apache.paimon.types.IntType())),
                        // system columns: __bucket, __offset, __timestamp
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
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
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        InternalArray outerArray = flussRecordAsPaimonRow.getArray(0);
        assertThat(outerArray).isNotNull();
        assertThat(outerArray.size()).isEqualTo(2);

        InternalArray innerArray1 = outerArray.getArray(0);
        assertThat(innerArray1.size()).isEqualTo(2);
        assertThat(innerArray1.getInt(0)).isEqualTo(1);
        assertThat(innerArray1.getInt(1)).isEqualTo(2);

        InternalArray innerArray2 = outerArray.getArray(1);
        assertThat(innerArray2.size()).isEqualTo(3);
        assertThat(innerArray2.getInt(0)).isEqualTo(3);
    }

    @Test
    void testArrayWithAllPrimitiveTypes() {
        int tableBucket = 0;
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
                                new org.apache.paimon.types.DoubleType()),
                        // system columns: __bucket, __offset, __timestamp
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
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
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        // Test boolean array
        InternalArray boolArray = flussRecordAsPaimonRow.getArray(0);
        assertThat(boolArray.size()).isEqualTo(3);
        assertThat(boolArray.getBoolean(0)).isTrue();
        assertThat(boolArray.getBoolean(1)).isFalse();
        assertThat(boolArray.getBoolean(2)).isTrue();
        assertThat(boolArray.toBooleanArray()).isEqualTo(new boolean[] {true, false, true});

        // Test byte array
        InternalArray byteArray = flussRecordAsPaimonRow.getArray(1);
        assertThat(byteArray.size()).isEqualTo(3);
        assertThat(byteArray.getByte(0)).isEqualTo((byte) 1);
        assertThat(byteArray.getByte(1)).isEqualTo((byte) 2);
        assertThat(byteArray.toByteArray()).isEqualTo(new byte[] {1, 2, 3});

        // Test short array
        InternalArray shortArray = flussRecordAsPaimonRow.getArray(2);
        assertThat(shortArray.size()).isEqualTo(3);
        assertThat(shortArray.getShort(0)).isEqualTo((short) 100);
        assertThat(shortArray.toShortArray()).isEqualTo(new short[] {100, 200, 300});

        // Test int array
        InternalArray intArray = flussRecordAsPaimonRow.getArray(3);
        assertThat(intArray.toIntArray()).isEqualTo(new int[] {1000, 2000, 3000});

        // Test long array
        InternalArray longArray = flussRecordAsPaimonRow.getArray(4);
        assertThat(longArray.getLong(0)).isEqualTo(10000L);
        assertThat(longArray.toLongArray()).isEqualTo(new long[] {10000L, 20000L, 30000L});

        // Test float array
        InternalArray floatArray = flussRecordAsPaimonRow.getArray(5);
        assertThat(floatArray.getFloat(0)).isEqualTo(1.1f);
        assertThat(floatArray.toFloatArray()).isEqualTo(new float[] {1.1f, 2.2f, 3.3f});

        // Test double array
        InternalArray doubleArray = flussRecordAsPaimonRow.getArray(6);
        assertThat(doubleArray.getDouble(0)).isEqualTo(1.11);
        assertThat(doubleArray.toDoubleArray()).isEqualTo(new double[] {1.11, 2.22, 3.33});

        // Verify system columns
        assertThat(flussRecordAsPaimonRow.getInt(7)).isEqualTo(tableBucket);
        assertThat(flussRecordAsPaimonRow.getLong(8)).isEqualTo(logOffset);
    }

    @Test
    void testArrayWithDecimalElements() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.DecimalType(10, 2)),
                        // system columns
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
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
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        InternalArray array = flussRecordAsPaimonRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getDecimal(0, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("123.45"));
        assertThat(array.getDecimal(1, 10, 2).toBigDecimal()).isEqualTo(new BigDecimal("678.90"));
    }

    @Test
    void testArrayWithTimestampElements() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.TimestampType(3)),
                        // system columns
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
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
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        InternalArray array = flussRecordAsPaimonRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getTimestamp(0, 3).getMillisecond()).isEqualTo(1698235273182L);
        assertThat(array.getTimestamp(1, 3).getMillisecond()).isEqualTo(1698235274000L);
    }

    @Test
    void testArrayWithBinaryElements() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.VarBinaryType()),
                        // system columns
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(
                0, new GenericArray(new Object[] {new byte[] {1, 2, 3}, new byte[] {4, 5, 6, 7}}));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        InternalArray array = flussRecordAsPaimonRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getBinary(0)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(array.getBinary(1)).isEqualTo(new byte[] {4, 5, 6, 7});
    }

    @Test
    void testNullArray() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(new org.apache.paimon.types.IntType())
                                .nullable(),
                        // system columns
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, null);

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        assertThat(flussRecordAsPaimonRow.isNullAt(0)).isTrue();
    }

    @Test
    void testArrayWithNullableElements() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.IntType().nullable()),
                        // system columns
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, new GenericArray(new Object[] {1, null, 3}));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        InternalArray array = flussRecordAsPaimonRow.getArray(0);
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getInt(0)).isEqualTo(1);
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(array.getInt(2)).isEqualTo(3);
    }

    @Test
    void testEmptyArray() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.IntType()),
                        // system columns
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);
        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, new GenericArray(new int[] {}));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        InternalArray array = flussRecordAsPaimonRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(0);
    }

    @Test
    void testPaimonSchemaWiderThanFlussRecord() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.BooleanType(),
                        new org.apache.paimon.types.VarCharType(),
                        // append three system columns: __bucket, __offset,__timestamp
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);

        long logOffset = 7L;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(1);
        genericRow.setField(0, true);
        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        flussRecordAsPaimonRow.setFlussRecord(logRecord);

        assertThat(flussRecordAsPaimonRow.getFieldCount()).isEqualTo(5);

        assertThat(flussRecordAsPaimonRow.getBoolean(0)).isTrue();
        assertThat(flussRecordAsPaimonRow.isNullAt(1)).isTrue();
        assertThat(flussRecordAsPaimonRow.getInt(2)).isEqualTo(tableBucket);
        assertThat(flussRecordAsPaimonRow.getLong(3)).isEqualTo(logOffset);
        assertThat(flussRecordAsPaimonRow.getTimestamp(4, 3))
                .isEqualTo(Timestamp.fromEpochMillis(timeStamp));
    }

    @Test
    void testFlussRecordWiderThanPaimonSchema() {
        int tableBucket = 0;
        RowType tableRowType =
                RowType.of(
                        new org.apache.paimon.types.BooleanType(),
                        // append three system columns: __bucket, __offset,__timestamp
                        new org.apache.paimon.types.IntType(),
                        new org.apache.paimon.types.BigIntType(),
                        new org.apache.paimon.types.LocalZonedTimestampType(3));

        FlussRecordAsPaimonRow flussRecordAsPaimonRow =
                new FlussRecordAsPaimonRow(tableBucket, tableRowType);

        long logOffset = 7L;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(2);
        genericRow.setField(0, true);
        genericRow.setField(1, BinaryString.fromString("extra"));
        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);

        // Should throw exception instead of silently truncating data
        assertThatThrownBy(() -> flussRecordAsPaimonRow.setFlussRecord(logRecord))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Fluss record has 2 fields but Paimon schema only has 1 business fields");
    }
}

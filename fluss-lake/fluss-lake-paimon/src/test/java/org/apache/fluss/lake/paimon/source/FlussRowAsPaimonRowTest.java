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
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

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
    void testArrayWithAllTypes() {
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
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.VarCharType(true, 30)),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.DecimalType(10, 2)),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.TimestampType(3)),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.LocalZonedTimestampType()),
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.VarBinaryType()),
                        // array<array<int>>
                        new org.apache.paimon.types.ArrayType(
                                new org.apache.paimon.types.ArrayType(
                                        new org.apache.paimon.types.IntType())));

        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();
        GenericRow genericRow = new GenericRow(13);
        genericRow.setField(0, new GenericArray(new boolean[] {true, false, true}));
        genericRow.setField(1, new GenericArray(new byte[] {1, 2, 3}));
        genericRow.setField(2, new GenericArray(new short[] {100, 200, 300}));
        genericRow.setField(3, new GenericArray(new Object[] {1000, 2000, 3000}));
        genericRow.setField(4, new GenericArray(new long[] {10000L, 20000L, 30000L}));
        genericRow.setField(5, new GenericArray(new float[] {1.1f, 2.2f, 3.3f}));
        genericRow.setField(6, new GenericArray(new double[] {1.11, 2.22, 3.33}));
        // String type
        genericRow.setField(
                7,
                new GenericArray(
                        new BinaryString[] {
                            BinaryString.fromString("hello"),
                            BinaryString.fromString("world"),
                            BinaryString.fromString("test")
                        }));

        // Decimal type
        genericRow.setField(
                8,
                new GenericArray(
                        new Object[] {
                            Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2),
                            Decimal.fromBigDecimal(new BigDecimal("678.90"), 10, 2),
                            Decimal.fromBigDecimal(new BigDecimal("999.99"), 10, 2)
                        }));

        // Timestamp type
        genericRow.setField(
                9,
                new GenericArray(
                        new Object[] {
                            TimestampNtz.fromMillis(1698235273182L),
                            TimestampNtz.fromMillis(1698235274000L),
                            TimestampNtz.fromMillis(1698235275000L)
                        }));

        // TimestampLTZ type
        genericRow.setField(
                10,
                new GenericArray(
                        new Object[] {
                            TimestampLtz.fromEpochMillis(1698235273182L),
                            TimestampLtz.fromEpochMillis(1698235274000L),
                            TimestampLtz.fromEpochMillis(1698235275000L)
                        }));

        // Binary type
        genericRow.setField(
                11,
                new GenericArray(
                        new Object[] {
                            new byte[] {1, 2, 3},
                            new byte[] {4, 5, 6, 7},
                            new byte[] {8, 9, 10, 11, 12}
                        }));

        // array<array<int>> type
        genericRow.setField(
                12,
                new GenericArray(
                        new Object[] {
                            new GenericArray(new int[] {1, 2}),
                            new GenericArray(new int[] {3, 4, 5})
                        }));

        LogRecord logRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, genericRow);
        FlussRowAsPaimonRow flussRowAsPaimonRow =
                new FlussRowAsPaimonRow(logRecord.getRow(), tableRowType);

        // Test boolean array
        InternalArray boolArray = flussRowAsPaimonRow.getArray(0);
        assertThat(boolArray.size()).isEqualTo(3);
        assertThat(boolArray.toBooleanArray()).isEqualTo(new boolean[] {true, false, true});

        // Test byte array
        InternalArray byteArray = flussRowAsPaimonRow.getArray(1);
        assertThat(byteArray.size()).isEqualTo(3);
        assertThat(byteArray.toByteArray()).isEqualTo(new byte[] {1, 2, 3});

        // Test short array
        InternalArray shortArray = flussRowAsPaimonRow.getArray(2);
        assertThat(shortArray.size()).isEqualTo(3);
        assertThat(shortArray.toShortArray()).isEqualTo(new short[] {100, 200, 300});

        // Test int array
        InternalArray intArray = flussRowAsPaimonRow.getArray(3);
        assertThat(intArray.size()).isEqualTo(3);
        assertThat(intArray.toIntArray()).isEqualTo(new int[] {1000, 2000, 3000});

        // Test long array
        InternalArray longArray = flussRowAsPaimonRow.getArray(4);
        assertThat(longArray.size()).isEqualTo(3);
        assertThat(longArray.toLongArray()).isEqualTo(new long[] {10000L, 20000L, 30000L});

        // Test float array
        InternalArray floatArray = flussRowAsPaimonRow.getArray(5);
        assertThat(floatArray.size()).isEqualTo(3);
        assertThat(floatArray.toFloatArray()).isEqualTo(new float[] {1.1f, 2.2f, 3.3f});

        // Test double array
        InternalArray doubleArray = flussRowAsPaimonRow.getArray(6);
        assertThat(doubleArray.size()).isEqualTo(3);
        assertThat(doubleArray.toDoubleArray()).isEqualTo(new double[] {1.11, 2.22, 3.33});

        // Test string array
        InternalArray stringArray = flussRowAsPaimonRow.getArray(7);
        assertThat(stringArray.size()).isEqualTo(3);
        assertThat(stringArray.getString(0).toString()).isEqualTo("hello");
        assertThat(stringArray.getString(1).toString()).isEqualTo("world");
        assertThat(stringArray.getString(2).toString()).isEqualTo("test");

        // Test decimal array
        InternalArray decimalArray = flussRowAsPaimonRow.getArray(8);
        assertThat(decimalArray.size()).isEqualTo(3);
        assertThat(decimalArray.getDecimal(0, 10, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("123.45"));
        assertThat(decimalArray.getDecimal(1, 10, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("678.90"));
        assertThat(decimalArray.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("999.99"));

        // Test timestamp array
        InternalArray timestampArray = flussRowAsPaimonRow.getArray(9);
        assertThat(timestampArray.size()).isEqualTo(3);
        assertThat(timestampArray.getTimestamp(0, 3).getMillisecond()).isEqualTo(1698235273182L);
        assertThat(timestampArray.getTimestamp(1, 3).getMillisecond()).isEqualTo(1698235274000L);
        assertThat(timestampArray.getTimestamp(2, 3).getMillisecond()).isEqualTo(1698235275000L);

        // test timestamp_ltz array
        timestampArray = flussRowAsPaimonRow.getArray(10);
        assertThat(timestampArray.size()).isEqualTo(3);
        assertThat(timestampArray.getTimestamp(0, 3).getMillisecond()).isEqualTo(1698235273182L);
        assertThat(timestampArray.getTimestamp(1, 3).getMillisecond()).isEqualTo(1698235274000L);
        assertThat(timestampArray.getTimestamp(2, 3).getMillisecond()).isEqualTo(1698235275000L);

        // Test binary array
        InternalArray binaryArray = flussRowAsPaimonRow.getArray(11);
        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getBinary(0)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(binaryArray.getBinary(1)).isEqualTo(new byte[] {4, 5, 6, 7});
        assertThat(binaryArray.getBinary(2)).isEqualTo(new byte[] {8, 9, 10, 11, 12});

        // Also test array<array<int>> (nested int array)
        InternalArray outerArray = flussRowAsPaimonRow.getArray(12);
        assertThat(outerArray).isNotNull();
        assertThat(outerArray.size()).isEqualTo(2);

        InternalArray innerArray1 = outerArray.getArray(0);
        assertThat(innerArray1.toIntArray()).isEqualTo(new int[] {1, 2});

        InternalArray innerArray2 = outerArray.getArray(1);
        assertThat(innerArray2.toIntArray()).isEqualTo(new int[] {3, 4, 5});
    }

    @Test
    void testMapWithAllTypes() {
        long logOffset = 0;
        long timeStamp = System.currentTimeMillis();

        // Test map with integer key and integer value
        RowType intMapRowType =
                RowType.of(
                        new org.apache.paimon.types.MapType(
                                new org.apache.paimon.types.IntType(),
                                new org.apache.paimon.types.IntType()));
        GenericRow intMapRow = new GenericRow(1);
        Map<Object, Object> intMapData = new HashMap<>();
        intMapData.put(1, 100);
        intMapData.put(2, 200);
        intMapData.put(3, 300);
        intMapRow.setField(0, new GenericMap(intMapData));
        LogRecord intMapRecord = new GenericRecord(logOffset, timeStamp, APPEND_ONLY, intMapRow);
        FlussRowAsPaimonRow intMapFlussRow =
                new FlussRowAsPaimonRow(intMapRecord.getRow(), intMapRowType);

        InternalMap intMap = intMapFlussRow.getMap(0);
        assertThat(intMap).isNotNull();
        assertThat(intMap.size()).isEqualTo(3);
        InternalArray intKeys = intMap.keyArray();
        InternalArray intValues = intMap.valueArray();
        assertThat(intKeys.size()).isEqualTo(3);
        assertThat(intValues.size()).isEqualTo(3);
        assertThat(intKeys.toIntArray()).containsExactlyInAnyOrder(1, 2, 3);
        assertThat(intValues.toIntArray()).containsExactlyInAnyOrder(100, 200, 300);

        // Test map with string key and integer value
        RowType stringMapRowType =
                RowType.of(
                        new org.apache.paimon.types.MapType(
                                new org.apache.paimon.types.VarCharType(),
                                new org.apache.paimon.types.IntType()));
        GenericRow stringMapRow = new GenericRow(1);
        Map<Object, Object> stringMapData = new HashMap<>();
        stringMapData.put(BinaryString.fromString("key1"), 100);
        stringMapData.put(BinaryString.fromString("key2"), 200);
        stringMapRow.setField(0, new GenericMap(stringMapData));
        LogRecord stringMapRecord =
                new GenericRecord(logOffset, timeStamp, APPEND_ONLY, stringMapRow);
        FlussRowAsPaimonRow stringMapFlussRow =
                new FlussRowAsPaimonRow(stringMapRecord.getRow(), stringMapRowType);

        InternalMap stringMap = stringMapFlussRow.getMap(0);
        assertThat(stringMap).isNotNull();
        assertThat(stringMap.size()).isEqualTo(2);
        InternalArray stringKeys = stringMap.keyArray();
        InternalArray stringValues = stringMap.valueArray();
        assertThat(stringKeys.size()).isEqualTo(2);
        assertThat(stringValues.size()).isEqualTo(2);
        assertThat(stringKeys.getString(0).toString()).isIn("key1", "key2");
        assertThat(stringKeys.getString(1).toString()).isIn("key1", "key2");
        assertThat(stringValues.toIntArray()).containsExactlyInAnyOrder(100, 200);

        // Test map with long key and double value
        RowType longDoubleMapRowType =
                RowType.of(
                        new org.apache.paimon.types.MapType(
                                new org.apache.paimon.types.BigIntType(),
                                new org.apache.paimon.types.DoubleType()));
        GenericRow longDoubleMapRow = new GenericRow(1);
        Map<Object, Object> longDoubleMapData = new HashMap<>();
        longDoubleMapData.put(1L, 1.1);
        longDoubleMapData.put(2L, 2.2);
        longDoubleMapRow.setField(0, new GenericMap(longDoubleMapData));
        LogRecord longDoubleMapRecord =
                new GenericRecord(logOffset, timeStamp, APPEND_ONLY, longDoubleMapRow);
        FlussRowAsPaimonRow longDoubleMapFlussRow =
                new FlussRowAsPaimonRow(longDoubleMapRecord.getRow(), longDoubleMapRowType);

        InternalMap longDoubleMap = longDoubleMapFlussRow.getMap(0);
        assertThat(longDoubleMap).isNotNull();
        assertThat(longDoubleMap.size()).isEqualTo(2);
        InternalArray longKeys = longDoubleMap.keyArray();
        InternalArray doubleValues = longDoubleMap.valueArray();
        assertThat(longKeys.toLongArray()).containsExactlyInAnyOrder(1L, 2L);
        assertThat(doubleValues.toDoubleArray()).containsExactlyInAnyOrder(1.1, 2.2);

        // Test map with decimal values
        RowType decimalMapRowType =
                RowType.of(
                        new org.apache.paimon.types.MapType(
                                new org.apache.paimon.types.IntType(),
                                new org.apache.paimon.types.DecimalType(10, 2)));
        GenericRow decimalMapRow = new GenericRow(1);
        Map<Object, Object> decimalMapData = new HashMap<>();
        decimalMapData.put(1, Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2));
        decimalMapData.put(2, Decimal.fromBigDecimal(new BigDecimal("678.90"), 10, 2));
        decimalMapRow.setField(0, new GenericMap(decimalMapData));
        LogRecord decimalMapRecord =
                new GenericRecord(logOffset, timeStamp, APPEND_ONLY, decimalMapRow);
        FlussRowAsPaimonRow decimalMapFlussRow =
                new FlussRowAsPaimonRow(decimalMapRecord.getRow(), decimalMapRowType);

        InternalMap decimalMap = decimalMapFlussRow.getMap(0);
        assertThat(decimalMap).isNotNull();
        assertThat(decimalMap.size()).isEqualTo(2);
        InternalArray decimalKeys = decimalMap.keyArray();
        InternalArray decimalValues = decimalMap.valueArray();
        assertThat(decimalKeys.toIntArray()).containsExactlyInAnyOrder(1, 2);
        assertThat(decimalValues.getDecimal(0, 10, 2).toBigDecimal())
                .isIn(new BigDecimal("123.45"), new BigDecimal("678.90"));
        assertThat(decimalValues.getDecimal(1, 10, 2).toBigDecimal())
                .isIn(new BigDecimal("123.45"), new BigDecimal("678.90"));

        // Test nested map type
        RowType nestedMapRowType =
                RowType.of(
                        new org.apache.paimon.types.MapType(
                                new org.apache.paimon.types.IntType(),
                                new org.apache.paimon.types.MapType(
                                        new org.apache.paimon.types.VarCharType(),
                                        new org.apache.paimon.types.IntType())));
        GenericRow nestedMapRow = new GenericRow(1);

        Map<Object, Object> innerMap1 = new HashMap<>();
        innerMap1.put(BinaryString.fromString("a"), 1);
        innerMap1.put(BinaryString.fromString("b"), 2);

        Map<Object, Object> innerMap2 = new HashMap<>();
        innerMap2.put(BinaryString.fromString("c"), 3);
        innerMap2.put(BinaryString.fromString("d"), 4);

        Map<Object, Object> outerMap = new HashMap<>();
        outerMap.put(1, new GenericMap(innerMap1));
        outerMap.put(2, new GenericMap(innerMap2));

        nestedMapRow.setField(0, new GenericMap(outerMap));

        LogRecord nestedMapRecord =
                new GenericRecord(logOffset, timeStamp, APPEND_ONLY, nestedMapRow);
        FlussRowAsPaimonRow nestedMapFlussRow =
                new FlussRowAsPaimonRow(nestedMapRecord.getRow(), nestedMapRowType);

        InternalMap outerMapResult = nestedMapFlussRow.getMap(0);
        assertThat(outerMapResult).isNotNull();
        assertThat(outerMapResult.size()).isEqualTo(2);

        InternalArray values = outerMapResult.valueArray();
        InternalMap innerMap1Result = values.getMap(0);
        assertThat(innerMap1Result).isNotNull();
        assertThat(innerMap1Result.size()).isEqualTo(2);

        InternalMap innerMap2Result = values.getMap(1);
        assertThat(innerMap2Result).isNotNull();
        assertThat(innerMap2Result.size()).isEqualTo(2);
    }
}

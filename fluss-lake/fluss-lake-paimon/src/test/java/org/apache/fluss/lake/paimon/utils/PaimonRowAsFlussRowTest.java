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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.row.InternalArray;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonRowAsFlussRow}. */
class PaimonRowAsFlussRowTest {

    @Test
    void testArrayTypeWithIntElements() {
        // Create a Paimon row with INT and ARRAY<INT> columns
        // Note: PaimonRowAsFlussRow expects system columns at the end, so we add 3 dummy system
        // columns
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(0, 42);
        paimonRow.setField(1, new GenericArray(new int[] {1, 2, 3, 4, 5}));
        // System columns: __bucket, __offset, __timestamp
        paimonRow.setField(2, 0);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        assertThat(flussRow.getInt(0)).isEqualTo(42);
        InternalArray array = flussRow.getArray(1);
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
        GenericRow paimonRow = new GenericRow(5);
        paimonRow.setField(0, BinaryString.fromString("name"));
        paimonRow.setField(
                1,
                new GenericArray(
                        new BinaryString[] {
                            BinaryString.fromString("a"),
                            BinaryString.fromString("b"),
                            BinaryString.fromString("c")
                        }));
        // System columns: __bucket, __offset, __timestamp
        paimonRow.setField(2, 0);
        paimonRow.setField(3, 0L);
        paimonRow.setField(4, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        assertThat(flussRow.getString(0).toString()).isEqualTo("name");
        InternalArray array = flussRow.getArray(1);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getString(0).toString()).isEqualTo("a");
        assertThat(array.getString(1).toString()).isEqualTo("b");
        assertThat(array.getString(2).toString()).isEqualTo("c");
    }

    @Test
    void testArrayTypeWithNullableElements() {
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(0, new GenericArray(new Object[] {1, null, 3}));
        // System columns: __bucket, __offset, __timestamp
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray array = flussRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getInt(0)).isEqualTo(1);
        assertThat(array.isNullAt(1)).isTrue();
        assertThat(array.getInt(2)).isEqualTo(3);
    }

    @Test
    void testNullArray() {
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(0, null);
        // System columns: __bucket, __offset, __timestamp
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        assertThat(flussRow.isNullAt(0)).isTrue();
    }

    @Test
    void testNestedArrayType() {
        // Test ARRAY<ARRAY<INT>>
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            new GenericArray(new int[] {1, 2}),
                            new GenericArray(new int[] {3, 4, 5})
                        }));
        // System columns: __bucket, __offset, __timestamp
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray outerArray = flussRow.getArray(0);
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
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(0, new GenericArray(new int[] {}));
        // System columns: __bucket, __offset, __timestamp
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray array = flussRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(0);
    }

    @Test
    void testReplaceRow() {
        GenericRow paimonRow1 = new GenericRow(4);
        paimonRow1.setField(0, new GenericArray(new int[] {1, 2, 3}));
        paimonRow1.setField(1, 0);
        paimonRow1.setField(2, 0L);
        paimonRow1.setField(3, 0L);

        GenericRow paimonRow2 = new GenericRow(4);
        paimonRow2.setField(0, new GenericArray(new int[] {4, 5}));
        paimonRow2.setField(1, 0);
        paimonRow2.setField(2, 0L);
        paimonRow2.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow1);
        assertThat(flussRow.getArray(0).size()).isEqualTo(3);

        flussRow.replaceRow(paimonRow2);
        assertThat(flussRow.getArray(0).size()).isEqualTo(2);
    }

    @Test
    void testArrayWithLongElements() {
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(0, new GenericArray(new long[] {100L, 200L, 300L}));
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray array = flussRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getLong(0)).isEqualTo(100L);
        assertThat(array.getLong(1)).isEqualTo(200L);
        assertThat(array.getLong(2)).isEqualTo(300L);
    }

    @Test
    void testArrayWithDoubleElements() {
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(0, new GenericArray(new double[] {1.1, 2.2, 3.3}));
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray array = flussRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getDouble(0)).isEqualTo(1.1);
        assertThat(array.getDouble(1)).isEqualTo(2.2);
        assertThat(array.getDouble(2)).isEqualTo(3.3);
    }

    @Test
    void testArrayWithBooleanElements() {
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(0, new GenericArray(new boolean[] {true, false, true}));
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray array = flussRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(3);
        assertThat(array.getBoolean(0)).isTrue();
        assertThat(array.getBoolean(1)).isFalse();
        assertThat(array.getBoolean(2)).isTrue();
    }

    @Test
    void testArrayWithAllPrimitiveTypes() {
        GenericRow paimonRow = new GenericRow(10);
        paimonRow.setField(0, new GenericArray(new boolean[] {true, false}));
        paimonRow.setField(1, new GenericArray(new byte[] {1, 2, 3}));
        paimonRow.setField(2, new GenericArray(new short[] {100, 200}));
        paimonRow.setField(3, new GenericArray(new int[] {1000, 2000}));
        paimonRow.setField(4, new GenericArray(new long[] {10000L, 20000L}));
        paimonRow.setField(5, new GenericArray(new float[] {1.1f, 2.2f}));
        paimonRow.setField(6, new GenericArray(new double[] {1.11, 2.22}));
        // System columns
        paimonRow.setField(7, 0);
        paimonRow.setField(8, 0L);
        paimonRow.setField(9, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        // Test boolean array with toArray
        InternalArray boolArray = flussRow.getArray(0);
        assertThat(boolArray.toBooleanArray()).isEqualTo(new boolean[] {true, false});

        // Test byte array with toArray
        InternalArray byteArray = flussRow.getArray(1);
        assertThat(byteArray.getByte(0)).isEqualTo((byte) 1);
        assertThat(byteArray.getByte(1)).isEqualTo((byte) 2);
        assertThat(byteArray.toByteArray()).isEqualTo(new byte[] {1, 2, 3});

        // Test short array with toArray
        InternalArray shortArray = flussRow.getArray(2);
        assertThat(shortArray.getShort(0)).isEqualTo((short) 100);
        assertThat(shortArray.getShort(1)).isEqualTo((short) 200);
        assertThat(shortArray.toShortArray()).isEqualTo(new short[] {100, 200});

        // Test int array with toArray
        InternalArray intArray = flussRow.getArray(3);
        assertThat(intArray.toIntArray()).isEqualTo(new int[] {1000, 2000});

        // Test long array with toArray
        InternalArray longArray = flussRow.getArray(4);
        assertThat(longArray.toLongArray()).isEqualTo(new long[] {10000L, 20000L});

        // Test float array with toArray
        InternalArray floatArray = flussRow.getArray(5);
        assertThat(floatArray.getFloat(0)).isEqualTo(1.1f);
        assertThat(floatArray.toFloatArray()).isEqualTo(new float[] {1.1f, 2.2f});

        // Test double array with toArray
        InternalArray doubleArray = flussRow.getArray(6);
        assertThat(doubleArray.toDoubleArray()).isEqualTo(new double[] {1.11, 2.22});
    }

    @Test
    void testArrayWithDecimalElements() {
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            org.apache.paimon.data.Decimal.fromBigDecimal(
                                    new java.math.BigDecimal("123.45"), 10, 2),
                            org.apache.paimon.data.Decimal.fromBigDecimal(
                                    new java.math.BigDecimal("678.90"), 10, 2)
                        }));
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray array = flussRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getDecimal(0, 10, 2).toBigDecimal())
                .isEqualTo(new java.math.BigDecimal("123.45"));
        assertThat(array.getDecimal(1, 10, 2).toBigDecimal())
                .isEqualTo(new java.math.BigDecimal("678.90"));
    }

    @Test
    void testArrayWithTimestampElements() {
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            org.apache.paimon.data.Timestamp.fromEpochMillis(1698235273182L),
                            org.apache.paimon.data.Timestamp.fromEpochMillis(1698235274000L)
                        }));
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray array = flussRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getTimestampNtz(0, 3).getMillisecond()).isEqualTo(1698235273182L);
        assertThat(array.getTimestampLtz(1, 3).getEpochMillisecond()).isEqualTo(1698235274000L);
    }

    @Test
    void testArrayWithBinaryElements() {
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(
                0,
                new GenericArray(
                        new Object[] {
                            new byte[] {1, 2, 3},
                            new byte[] {4, 5, 6, 7}
                        }));
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray array = flussRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getBinary(0, 3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(array.getBytes(1)).isEqualTo(new byte[] {4, 5, 6, 7});
    }

    @Test
    void testArrayWithCharElements() {
        GenericRow paimonRow = new GenericRow(4);
        paimonRow.setField(
                0,
                new GenericArray(
                        new BinaryString[] {
                            BinaryString.fromString("abc"), BinaryString.fromString("def")
                        }));
        paimonRow.setField(1, 0);
        paimonRow.setField(2, 0L);
        paimonRow.setField(3, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        InternalArray array = flussRow.getArray(0);
        assertThat(array.size()).isEqualTo(2);
        assertThat(array.getChar(0, 3).toString()).isEqualTo("abc");
        assertThat(array.getString(1).toString()).isEqualTo("def");
    }
}

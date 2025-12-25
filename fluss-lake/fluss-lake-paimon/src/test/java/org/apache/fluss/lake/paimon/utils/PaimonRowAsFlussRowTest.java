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
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonRowAsFlussRow}. */
class PaimonRowAsFlussRowTest {

    @Test
    void testArrayWithAllTypes() {
        GenericRow paimonRow = new GenericRow(16);
        // Primitive types
        paimonRow.setField(0, new GenericArray(new boolean[] {true, false, true}));
        paimonRow.setField(1, new GenericArray(new byte[] {1, 2, 3}));
        paimonRow.setField(2, new GenericArray(new short[] {100, 200, 300}));
        paimonRow.setField(3, new GenericArray(new int[] {1000, 2000, 3000}));
        paimonRow.setField(4, new GenericArray(new long[] {10000L, 20000L, 30000L}));
        paimonRow.setField(5, new GenericArray(new float[] {1.1f, 2.2f, 3.3f}));
        paimonRow.setField(6, new GenericArray(new double[] {1.11, 2.22, 3.33}));
        // String type
        paimonRow.setField(
                7,
                new GenericArray(
                        new BinaryString[] {
                            BinaryString.fromString("hello"),
                            BinaryString.fromString("world"),
                            BinaryString.fromString("test")
                        }));
        // Decimal type
        paimonRow.setField(
                8,
                new GenericArray(
                        new Object[] {
                            Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2),
                            Decimal.fromBigDecimal(new BigDecimal("678.90"), 10, 2),
                            Decimal.fromBigDecimal(new BigDecimal("999.99"), 10, 2)
                        }));
        // Timestamp type
        paimonRow.setField(
                9,
                new GenericArray(
                        new Object[] {
                            Timestamp.fromEpochMillis(1698235273182L),
                            Timestamp.fromEpochMillis(1698235274000L),
                            Timestamp.fromEpochMillis(1698235275000L)
                        }));
        // TimestampLTZ type
        paimonRow.setField(
                10,
                new GenericArray(
                        new Object[] {
                            Timestamp.fromEpochMillis(1698235273182L),
                            Timestamp.fromEpochMillis(1698235274000L),
                            Timestamp.fromEpochMillis(1698235275000L)
                        }));
        // Binary type
        paimonRow.setField(
                11,
                new GenericArray(
                        new Object[] {
                            new byte[] {1, 2, 3},
                            new byte[] {4, 5, 6, 7},
                            new byte[] {8, 9, 10, 11, 12}
                        }));
        // array<array<int>> type
        paimonRow.setField(
                12,
                new GenericArray(
                        new Object[] {
                            new GenericArray(new int[] {1, 2}),
                            new GenericArray(new int[] {3, 4, 5})
                        }));
        // System columns: __bucket, __offset, __timestamp
        paimonRow.setField(13, 0);
        paimonRow.setField(14, 0L);
        paimonRow.setField(15, 0L);

        PaimonRowAsFlussRow flussRow = new PaimonRowAsFlussRow(paimonRow);

        // Test boolean array
        InternalArray boolArray = flussRow.getArray(0);
        assertThat(boolArray.size()).isEqualTo(3);
        assertThat(boolArray.toBooleanArray()).isEqualTo(new boolean[] {true, false, true});

        // Test byte array
        InternalArray byteArray = flussRow.getArray(1);
        assertThat(byteArray.size()).isEqualTo(3);
        assertThat(byteArray.toByteArray()).isEqualTo(new byte[] {1, 2, 3});

        // Test short array
        InternalArray shortArray = flussRow.getArray(2);
        assertThat(shortArray.size()).isEqualTo(3);
        assertThat(shortArray.toShortArray()).isEqualTo(new short[] {100, 200, 300});

        // Test int array
        InternalArray intArray = flussRow.getArray(3);
        assertThat(intArray.size()).isEqualTo(3);
        assertThat(intArray.toIntArray()).isEqualTo(new int[] {1000, 2000, 3000});

        // Test long array
        InternalArray longArray = flussRow.getArray(4);
        assertThat(longArray.size()).isEqualTo(3);
        assertThat(longArray.toLongArray()).isEqualTo(new long[] {10000L, 20000L, 30000L});

        // Test float array
        InternalArray floatArray = flussRow.getArray(5);
        assertThat(floatArray.size()).isEqualTo(3);
        assertThat(floatArray.toFloatArray()).isEqualTo(new float[] {1.1f, 2.2f, 3.3f});

        // Test double array
        InternalArray doubleArray = flussRow.getArray(6);
        assertThat(doubleArray.size()).isEqualTo(3);
        assertThat(doubleArray.toDoubleArray()).isEqualTo(new double[] {1.11, 2.22, 3.33});

        // Test string array
        InternalArray stringArray = flussRow.getArray(7);
        assertThat(stringArray.size()).isEqualTo(3);
        assertThat(stringArray.getString(0).toString()).isEqualTo("hello");
        assertThat(stringArray.getString(1).toString()).isEqualTo("world");
        assertThat(stringArray.getString(2).toString()).isEqualTo("test");

        // Test decimal array
        InternalArray decimalArray = flussRow.getArray(8);
        assertThat(decimalArray.size()).isEqualTo(3);
        assertThat(decimalArray.getDecimal(0, 10, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("123.45"));
        assertThat(decimalArray.getDecimal(1, 10, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("678.90"));
        assertThat(decimalArray.getDecimal(2, 10, 2).toBigDecimal())
                .isEqualTo(new BigDecimal("999.99"));

        // Test timestamp array
        InternalArray timestampArray = flussRow.getArray(9);
        assertThat(timestampArray.size()).isEqualTo(3);
        assertThat(timestampArray.getTimestampNtz(0, 3).getMillisecond()).isEqualTo(1698235273182L);
        assertThat(timestampArray.getTimestampNtz(1, 3).getMillisecond()).isEqualTo(1698235274000L);
        assertThat(timestampArray.getTimestampNtz(2, 3).getMillisecond()).isEqualTo(1698235275000L);

        // Test timestamp_ltz array
        InternalArray timestampLtzArray = flussRow.getArray(10);
        assertThat(timestampLtzArray.size()).isEqualTo(3);
        assertThat(timestampLtzArray.getTimestampLtz(0, 3).getEpochMillisecond())
                .isEqualTo(1698235273182L);
        assertThat(timestampLtzArray.getTimestampLtz(1, 3).getEpochMillisecond())
                .isEqualTo(1698235274000L);
        assertThat(timestampLtzArray.getTimestampLtz(2, 3).getEpochMillisecond())
                .isEqualTo(1698235275000L);

        // Test binary array
        InternalArray binaryArray = flussRow.getArray(11);
        assertThat(binaryArray.size()).isEqualTo(3);
        assertThat(binaryArray.getBinary(0, 3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(binaryArray.getBinary(1, 4)).isEqualTo(new byte[] {4, 5, 6, 7});
        assertThat(binaryArray.getBinary(2, 5)).isEqualTo(new byte[] {8, 9, 10, 11, 12});
        // Also test getBytes method
        assertThat(binaryArray.getBytes(0)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(binaryArray.getBytes(1)).isEqualTo(new byte[] {4, 5, 6, 7});
        assertThat(binaryArray.getBytes(2)).isEqualTo(new byte[] {8, 9, 10, 11, 12});

        // Also test array<array<int>> (nested int array)
        InternalArray outerArray = flussRow.getArray(12);
        assertThat(outerArray).isNotNull();
        assertThat(outerArray.size()).isEqualTo(2);

        InternalArray innerArray1 = outerArray.getArray(0);
        assertThat(innerArray1.toIntArray()).isEqualTo(new int[] {1, 2});

        InternalArray innerArray2 = outerArray.getArray(1);
        assertThat(innerArray2.toIntArray()).isEqualTo(new int[] {3, 4, 5});
    }
}

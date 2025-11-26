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

package org.apache.fluss.lake.paimon.utils;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonArrayAsFlussArray}. */
class PaimonArrayAsFlussArrayTest {

    @Test
    void testPrimitiveArrayMethods() {
        GenericArray paimonArray =
                new GenericArray(new Object[] {true, (byte) 1, (short) 2, 3, 4L, 5.5f, 6.6d});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        assertThat(flussArray.size()).isEqualTo(7);
        assertThat(flussArray.getBoolean(0)).isTrue();
        assertThat(flussArray.getByte(1)).isEqualTo((byte) 1);
        assertThat(flussArray.getShort(2)).isEqualTo((short) 2);
        assertThat(flussArray.getInt(3)).isEqualTo(3);
        assertThat(flussArray.getLong(4)).isEqualTo(4L);
        assertThat(flussArray.getFloat(5)).isEqualTo(5.5f);
        assertThat(flussArray.getDouble(6)).isEqualTo(6.6d);
    }

    @Test
    void testStringAndCharMethods() {
        GenericArray paimonArray =
                new GenericArray(
                        new Object[] {
                            org.apache.paimon.data.BinaryString.fromString("hello"),
                            org.apache.paimon.data.BinaryString.fromString("world")
                        });
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        BinaryString str = flussArray.getString(0);
        assertThat(str.toString()).isEqualTo("hello");

        BinaryString charStr = flussArray.getChar(1, 5);
        assertThat(charStr.toString()).isEqualTo("world");
    }

    @Test
    void testBinaryAndBytesMethods() {
        GenericArray paimonArray =
                new GenericArray(new Object[] {new byte[] {1, 2, 3}, new byte[] {4, 5, 6}});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        assertThat(flussArray.getBinary(0, 3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(flussArray.getBytes(1)).isEqualTo(new byte[] {4, 5, 6});
    }

    @Test
    void testDecimalCompact() {
        org.apache.paimon.data.Decimal paimonDecimal =
                org.apache.paimon.data.Decimal.fromUnscaledLong(12345L, 10, 2);
        GenericArray paimonArray = new GenericArray(new Object[] {paimonDecimal});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        Decimal decimal = flussArray.getDecimal(0, 10, 2);
        assertThat(decimal.toUnscaledLong()).isEqualTo(12345L);
        assertThat(decimal.precision()).isEqualTo(10);
        assertThat(decimal.scale()).isEqualTo(2);
    }

    @Test
    void testDecimalNonCompact() {
        BigDecimal bigDecimalValue = new BigDecimal("12345678901234567890.12");
        org.apache.paimon.data.Decimal paimonDecimal =
                org.apache.paimon.data.Decimal.fromBigDecimal(bigDecimalValue, 25, 2);
        GenericArray paimonArray = new GenericArray(new Object[] {paimonDecimal});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        Decimal decimal = flussArray.getDecimal(0, 25, 2);
        assertThat(decimal.toBigDecimal()).isEqualTo(bigDecimalValue);
    }

    @Test
    void testTimestampNtzCompact() {
        Timestamp paimonTimestamp = Timestamp.fromEpochMillis(1672531200000L);
        GenericArray paimonArray = new GenericArray(new Object[] {paimonTimestamp});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        TimestampNtz ntz = flussArray.getTimestampNtz(0, 3);
        assertThat(ntz.getMillisecond()).isEqualTo(1672531200000L);
    }

    @Test
    void testTimestampNtzNonCompact() {
        Timestamp paimonTimestamp = Timestamp.fromEpochMillis(1672531200000L, 123456);
        GenericArray paimonArray = new GenericArray(new Object[] {paimonTimestamp});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        TimestampNtz ntz = flussArray.getTimestampNtz(0, 9);
        assertThat(ntz.getMillisecond()).isEqualTo(1672531200000L);
        assertThat(ntz.getNanoOfMillisecond()).isEqualTo(123456);
    }

    @Test
    void testTimestampLtzCompact() {
        Timestamp paimonTimestamp = Timestamp.fromEpochMillis(1672531200000L);
        GenericArray paimonArray = new GenericArray(new Object[] {paimonTimestamp});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        TimestampLtz ltz = flussArray.getTimestampLtz(0, 3);
        assertThat(ltz.getEpochMillisecond()).isEqualTo(1672531200000L);
    }

    @Test
    void testTimestampLtzNonCompact() {
        Timestamp paimonTimestamp = Timestamp.fromEpochMillis(1672531200000L, 654321);
        GenericArray paimonArray = new GenericArray(new Object[] {paimonTimestamp});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        TimestampLtz ltz = flussArray.getTimestampLtz(0, 9);
        assertThat(ltz.getEpochMillisecond()).isEqualTo(1672531200000L);
        assertThat(ltz.getNanoOfMillisecond()).isEqualTo(654321);
    }

    @Test
    void testIsNullAt() {
        GenericArray paimonArray = new GenericArray(new Object[] {null, 1, null});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        assertThat(flussArray.isNullAt(0)).isTrue();
        assertThat(flussArray.isNullAt(1)).isFalse();
        assertThat(flussArray.isNullAt(2)).isTrue();
    }

    @Test
    void testNestedArray() {
        GenericArray innerArray = new GenericArray(new Object[] {1, 2, 3});
        GenericArray outerArray = new GenericArray(new Object[] {innerArray});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(outerArray);

        InternalArray nested = flussArray.getArray(0);
        assertThat(nested).isInstanceOf(PaimonArrayAsFlussArray.class);
        assertThat(nested.size()).isEqualTo(3);
        assertThat(nested.getInt(0)).isEqualTo(1);
        assertThat(nested.getInt(1)).isEqualTo(2);
        assertThat(nested.getInt(2)).isEqualTo(3);
    }

    @Test
    void testNestedRow() {
        GenericRow innerRow = new GenericRow(2);
        innerRow.setField(0, 42);
        innerRow.setField(1, org.apache.paimon.data.BinaryString.fromString("test"));

        GenericArray paimonArray = new GenericArray(new Object[] {innerRow});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        InternalRow row = flussArray.getRow(0, 2);
        assertThat(row).isInstanceOf(PaimonRowAsFlussRow.class);
        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getString(1).toString()).isEqualTo("test");
    }

    @Test
    void testToBooleanArray() {
        GenericArray paimonArray = new GenericArray(new boolean[] {true, false, true});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        boolean[] result = flussArray.toBooleanArray();
        assertThat(result).containsExactly(true, false, true);
    }

    @Test
    void testToByteArray() {
        GenericArray paimonArray = new GenericArray(new byte[] {1, 2, 3});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        byte[] result = flussArray.toByteArray();
        assertThat(result).containsExactly((byte) 1, (byte) 2, (byte) 3);
    }

    @Test
    void testToShortArray() {
        GenericArray paimonArray = new GenericArray(new short[] {10, 20, 30});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        short[] result = flussArray.toShortArray();
        assertThat(result).containsExactly((short) 10, (short) 20, (short) 30);
    }

    @Test
    void testToIntArray() {
        GenericArray paimonArray = new GenericArray(new int[] {100, 200, 300});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        int[] result = flussArray.toIntArray();
        assertThat(result).containsExactly(100, 200, 300);
    }

    @Test
    void testToLongArray() {
        GenericArray paimonArray = new GenericArray(new long[] {1000L, 2000L, 3000L});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        long[] result = flussArray.toLongArray();
        assertThat(result).containsExactly(1000L, 2000L, 3000L);
    }

    @Test
    void testToFloatArray() {
        GenericArray paimonArray = new GenericArray(new float[] {1.1f, 2.2f, 3.3f});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        float[] result = flussArray.toFloatArray();
        assertThat(result).containsExactly(1.1f, 2.2f, 3.3f);
    }

    @Test
    void testToDoubleArray() {
        GenericArray paimonArray = new GenericArray(new double[] {1.11d, 2.22d, 3.33d});
        PaimonArrayAsFlussArray flussArray = new PaimonArrayAsFlussArray(paimonArray);

        double[] result = flussArray.toDoubleArray();
        assertThat(result).containsExactly(1.11d, 2.22d, 3.33d);
    }
}

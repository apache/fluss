/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.flink.row;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlinkAsFlussArrayTest {

    @Test
    void testPrimitiveArrayMethods() {
        ArrayData array =
                new GenericArrayData(new Object[] {true, (byte) 1, (short) 2, 3, 4L, 5.5f, 6.6d});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);

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
    void testStringAndBinaryMethods() {
        ArrayData array =
                new GenericArrayData(
                        new Object[] {StringData.fromString("hello"), new byte[] {1, 2, 3}});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);

        BinaryString str = flussArray.getString(0);
        assertThat(str.toString()).isEqualTo("hello");
        assertThat(flussArray.getBinary(1, 3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(flussArray.getBytes(1)).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    void testDecimalAndTimestampMethods() {
        ArrayData array =
                new GenericArrayData(
                        new Object[] {
                            DecimalData.fromUnscaledLong(12345L, 10, 2),
                            TimestampData.fromInstant(Instant.ofEpochMilli(1672531200000L)),
                            TimestampData.fromEpochMillis(1672531200000000L, 9)
                        });
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);

        Decimal decimal = flussArray.getDecimal(0, 10, 2);
        assertThat(decimal.toUnscaledLong()).isEqualTo(12345L);

        TimestampNtz ntz = flussArray.getTimestampNtz(1, 3);
        assertThat(ntz.getMillisecond()).isEqualTo(1672531200000L);

        TimestampLtz ltz = flussArray.getTimestampLtz(2, 9);
        assertThat(ltz.getEpochMillisecond()).isEqualTo(1672531200000000L);
        assertThat(ltz.getNanoOfMillisecond()).isEqualTo(9);
    }

    @Test
    void testIsNullAtAndGetArray() {
        ArrayData inner = new GenericArrayData(new Object[] {1, 2});
        ArrayData array = new GenericArrayData(new Object[] {null, inner});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);

        assertThat(flussArray.isNullAt(0)).isTrue();
        InternalArray arr = flussArray.getArray(1);
        assertThat(arr.size()).isEqualTo(2);
        assertThat(arr.getInt(0)).isEqualTo(1);
    }

    @Test
    void testToBooleanArray() {
        ArrayData array = new GenericArrayData(new boolean[] {true, false, true});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        assertThat(flussArray.toBooleanArray()).isEqualTo(new boolean[] {true, false, true});
    }

    @Test
    void testToByteArray() {
        ArrayData array = new GenericArrayData(new byte[] {1, 2, 3});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        assertThat(flussArray.toByteArray()).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    void testToShortArray() {
        ArrayData array = new GenericArrayData(new short[] {10, 20, 30});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        assertThat(flussArray.toShortArray()).isEqualTo(new short[] {10, 20, 30});
    }

    @Test
    void testToIntArray() {
        ArrayData array = new GenericArrayData(new int[] {100, 200, 300});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        assertThat(flussArray.toIntArray()).isEqualTo(new int[] {100, 200, 300});
    }

    @Test
    void testToLongArray() {
        ArrayData array = new GenericArrayData(new long[] {1000L, 2000L, 3000L});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        assertThat(flussArray.toLongArray()).isEqualTo(new long[] {1000L, 2000L, 3000L});
    }

    @Test
    void testToFloatArray() {
        ArrayData array = new GenericArrayData(new float[] {1.1f, 2.2f, 3.3f});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        assertThat(flussArray.toFloatArray()).isEqualTo(new float[] {1.1f, 2.2f, 3.3f});
    }

    @Test
    void testToDoubleArray() {
        ArrayData array = new GenericArrayData(new double[] {1.1d, 2.2d, 3.3d});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        assertThat(flussArray.toDoubleArray()).isEqualTo(new double[] {1.1d, 2.2d, 3.3d});
    }

    @Test
    void testGetChar() {
        ArrayData array = new GenericArrayData(new Object[] {StringData.fromString("ab")});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        BinaryString ch = flussArray.getChar(0, 2);
        assertThat(ch.toString()).isEqualTo("ab");
    }

    @Test
    void testGetMap() {
        Map<StringData, Integer> mapContent = new HashMap<>();
        mapContent.put(StringData.fromString("key"), 42);
        GenericMapData mapData = new GenericMapData(mapContent);
        ArrayData array = new GenericArrayData(new Object[] {mapData});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        InternalMap map = flussArray.getMap(0);
        assertThat(map.size()).isEqualTo(1);
    }

    @Test
    void testGetRow() {
        GenericRowData rowData = GenericRowData.of(1, StringData.fromString("hello"));
        ArrayData array = new GenericArrayData(new Object[] {rowData});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        InternalRow row = flussArray.getRow(0, 2);
        assertThat(row.getInt(0)).isEqualTo(1);
        assertThat(row.getString(1).toString()).isEqualTo("hello");
    }

    @Test
    void testGetVariantThrowsOnOlderFlink() {
        ArrayData array = new GenericArrayData(new Object[] {"dummy"});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        assertThatThrownBy(() -> flussArray.getVariant(0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Variant type requires Flink 2.1 or later");
    }

    @Test
    void testToObjectArray() {
        ArrayData array = new GenericArrayData(new Object[] {1, 2, 3});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        Object[] result = flussArray.toObjectArray(DataTypes.INT());
        assertThat(result).containsExactly(1, 2, 3);
    }

    @Test
    void testToObjectArrayWithNulls() {
        ArrayData array = new GenericArrayData(new Object[] {1, null, 3});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        Object[] result = flussArray.toObjectArray(DataTypes.INT());
        assertThat(result).containsExactly(1, null, 3);
    }
}

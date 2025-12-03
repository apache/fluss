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
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

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
    void testToObjectArray() {
        ArrayData array = new GenericArrayData(new Object[] {1, 2, 3});
        FlinkAsFlussArray flussArray = new FlinkAsFlussArray(array);
        Object[] result = flussArray.toObjectArray(DataTypes.INT());
        assertThat(result).containsExactly(1, 2, 3);
    }
}

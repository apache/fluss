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

package com.alibaba.fluss.row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link GenericRow}. */
class GenericRowTest {

    @Test
    void testConstructor() {
        GenericRow row = new GenericRow(3);
        assertThat(row.getFieldCount()).isEqualTo(3);
        assertThat(row.getField(0)).isNull();
        assertThat(row.getField(1)).isNull();
        assertThat(row.getField(2)).isNull();

        assertThatThrownBy(() -> new GenericRow(-1)).isInstanceOf(NegativeArraySizeException.class);

        // test set/get and toString().
        row.setField(0, 1);
        row.setField(1, null);
        row.setField(2, 3);
        assertThat(row).isEqualTo(GenericRow.of(1, null, 3));
        assertThat(row.getField(0)).isEqualTo(1);
        assertThat(row.getField(1)).isNull();
        assertThat(row.getField(2)).isEqualTo(3);
        assertThat(row.toString()).isEqualTo("(1,null,3)");
    }

    @Test
    void testGenericRows() {
        GenericRow genericRow = GenericRow.of(2, 0, 1, null, 4);
        assertThat(genericRow.getFieldCount()).isEqualTo(5);
        assertThat(genericRow.getInt(0)).isEqualTo(2);
        assertThat(genericRow.getInt(1)).isEqualTo(0);
        assertThat(genericRow.getInt(2)).isEqualTo(1);
        assertThat(genericRow.isNullAt(3)).isTrue();
        assertThat(genericRow.getInt(4)).isEqualTo(4);

        genericRow = GenericRow.of(0L, 1L, null, 3L);
        assertThat(genericRow.getLong(0)).isEqualTo(0L);
        assertThat(genericRow.getLong(1)).isEqualTo(1L);
        assertThat(genericRow.isNullAt(2)).isTrue();
        assertThat(genericRow.getLong(3)).isEqualTo(3L);

        genericRow = GenericRow.of((short) 5, null, (short) 7, (short) 8);
        assertThat(genericRow.getShort(0)).isEqualTo((short) 5);
        assertThat(genericRow.isNullAt(1)).isTrue();
        assertThat(genericRow.getShort(2)).isEqualTo((short) 7);
        assertThat(genericRow.getShort(3)).isEqualTo((short) 8);

        genericRow = GenericRow.of((byte) 5, (byte) 6, null, (byte) 8);
        assertThat(genericRow.getByte(0)).isEqualTo((byte) 5);
        assertThat(genericRow.getByte(1)).isEqualTo((byte) 6);
        assertThat(genericRow.isNullAt(2)).isTrue();
        assertThat(genericRow.getByte(3)).isEqualTo((byte) 8);

        genericRow = GenericRow.of(null, false, true, false);
        assertThat(genericRow.isNullAt(0)).isTrue();
        assertThat(genericRow.getBoolean(1)).isEqualTo(false);
        assertThat(genericRow.getBoolean(2)).isEqualTo(true);
        assertThat(genericRow.getBoolean(3)).isEqualTo(false);

        genericRow = GenericRow.of(0.0f, null, 0.2f, 0.3f);
        assertThat(genericRow.getFloat(0)).isEqualTo(0.0f);
        assertThat(genericRow.isNullAt(1)).isTrue();
        assertThat(genericRow.getFloat(2)).isEqualTo(0.2f);
        assertThat(genericRow.getFloat(3)).isEqualTo(0.3f);

        genericRow = GenericRow.of(null, 0.6d, 0.7d, 0.8d);
        assertThat(genericRow.isNullAt(0)).isTrue();
        assertThat(genericRow.getDouble(1)).isEqualTo(0.6d);
        assertThat(genericRow.getDouble(2)).isEqualTo(0.7d);
        assertThat(genericRow.getDouble(3)).isEqualTo(0.8d);

        genericRow =
                GenericRow.of(
                        BinaryString.fromString("0"),
                        null,
                        BinaryString.fromString("2"),
                        BinaryString.fromString("3"));
        assertThat(genericRow.getChar(0, 1).toString()).isEqualTo("0");
        assertThat(genericRow.isNullAt(1)).isTrue();
        assertThat(genericRow.getChar(2, 1).toString()).isEqualTo("2");
        assertThat(genericRow.getChar(3, 1).toString()).isEqualTo("3");
        assertThat(genericRow.getString(0).toString()).isEqualTo("0");
        assertThat(genericRow.getString(2).toString()).isEqualTo("2");
        assertThat(genericRow.getString(3).toString()).isEqualTo("3");

        genericRow =
                GenericRow.of(
                        null,
                        Decimal.fromBigDecimal(new BigDecimal("1"), 18, 0),
                        Decimal.fromBigDecimal(new BigDecimal("2"), 18, 0),
                        Decimal.fromBigDecimal(new BigDecimal("3"), 18, 0));
        assertThat(genericRow.isNullAt(0)).isTrue();
        assertThat(genericRow.getDecimal(1, 18, 0).toString()).isEqualTo("1");
        assertThat(genericRow.getDecimal(2, 18, 0).toString()).isEqualTo("2");
        assertThat(genericRow.getDecimal(3, 18, 0).toString()).isEqualTo("3");

        genericRow =
                GenericRow.of(
                        TimestampNtz.fromMillis(5L),
                        null,
                        TimestampNtz.fromMillis(7L),
                        TimestampNtz.fromMillis(8L));
        assertThat(genericRow.getTimestampNtz(0, 3)).isEqualTo(TimestampNtz.fromMillis(5));
        assertThat(genericRow.isNullAt(1)).isTrue();
        assertThat(genericRow.getTimestampNtz(2, 3)).isEqualTo(TimestampNtz.fromMillis(7));
        assertThat(genericRow.getTimestampNtz(3, 3)).isEqualTo(TimestampNtz.fromMillis(8));

        genericRow =
                GenericRow.of(
                        TimestampLtz.fromEpochMicros(5L),
                        TimestampLtz.fromEpochMicros(6L),
                        TimestampLtz.fromEpochMicros(7L),
                        null);
        assertThat(genericRow.getTimestampLtz(0, 3)).isEqualTo(TimestampLtz.fromEpochMicros(5));
        assertThat(genericRow.getTimestampLtz(1, 3)).isEqualTo(TimestampLtz.fromEpochMicros(6));
        assertThat(genericRow.getTimestampLtz(2, 3)).isEqualTo(TimestampLtz.fromEpochMicros(7));
        assertThat(genericRow.isNullAt(3)).isTrue();

        genericRow = GenericRow.of(new byte[] {5}, new byte[] {6}, null, new byte[] {8});
        assertThat(genericRow.getBytes(0)).isEqualTo(new byte[] {5});
        assertThat(genericRow.getBytes(1)).isEqualTo(new byte[] {6});
        assertThat(genericRow.isNullAt(2)).isTrue();
        assertThat(genericRow.getBytes(3)).isEqualTo(new byte[] {8});
        assertThat(genericRow.getBinary(0, 1)).isEqualTo(new byte[] {5});
        assertThat(genericRow.getBinary(1, 1)).isEqualTo(new byte[] {6});
        assertThat(genericRow.getBinary(3, 1)).isEqualTo(new byte[] {8});
    }
}

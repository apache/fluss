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

package org.apache.fluss.row;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaddingRow}. */
class PaddingRowTest {

    @Test
    void testPaddingRowIsNullAt() {
        // mock an InternalRow with only 2 columns
        InternalRow row = GenericRow.of(1, null);
        PaddingRow paddingRow = new PaddingRow(4).replaceRow(row);

        assertThat(paddingRow.getFieldCount()).isEqualTo(4);
        assertThat(paddingRow.isNullAt(0)).isFalse();
        assertThat(paddingRow.isNullAt(1)).isTrue();
        assertThat(paddingRow.isNullAt(2)).isTrue(); // exceed original row, should be null
        assertThat(paddingRow.isNullAt(3)).isTrue();
    }

    @Test
    void testPaddingRowDelegatesGetters() {
        InternalRow row =
                GenericRow.of(
                        true,
                        (byte) 2,
                        (short) 3,
                        4,
                        5L,
                        6.5f,
                        7.5,
                        new byte[] {1, 2, 3},
                        new byte[] {4, 5},
                        BinaryString.fromString("abc"),
                        TimestampLtz.fromEpochMillis(1000));
        PaddingRow paddingRow = new PaddingRow(9).replaceRow(row);

        assertThat(paddingRow.getBoolean(0)).isTrue();
        assertThat(paddingRow.getByte(1)).isEqualTo((byte) 2);
        assertThat(paddingRow.getShort(2)).isEqualTo((short) 3);
        assertThat(paddingRow.getInt(3)).isEqualTo(4);
        assertThat(paddingRow.getLong(4)).isEqualTo(5L);
        assertThat(paddingRow.getFloat(5)).isEqualTo(6.5f);
        assertThat(paddingRow.getDouble(6)).isEqualTo(7.5);
        assertThat(paddingRow.getBinary(7, 3)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(paddingRow.getBytes(8)).isEqualTo(new byte[] {4, 5});
        assertThat(paddingRow.getString(9)).isEqualTo(BinaryString.fromString("abc"));
        assertThat(paddingRow.getTimestampLtz(10, 3)).isEqualTo(TimestampLtz.fromEpochMillis(1000));
    }

    @Test
    void testReplaceRowInPlace() {
        InternalRow row1 = GenericRow.of(1);
        InternalRow row2 = GenericRow.of(2);
        PaddingRow paddingRow = new PaddingRow(1).replaceRow(row1);
        assertThat(paddingRow.getInt(0)).isEqualTo(1);
        paddingRow.replaceRow(row2);
        assertThat(paddingRow.getInt(0)).isEqualTo(2);
    }
}

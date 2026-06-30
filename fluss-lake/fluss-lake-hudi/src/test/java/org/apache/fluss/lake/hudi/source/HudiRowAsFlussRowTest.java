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

package org.apache.fluss.lake.hudi.source;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HudiRowAsFlussRow}. */
class HudiRowAsFlussRowTest {

    @Test
    void testAccessPrimitiveAndNestedValues() {
        GenericRowData nestedRow = new GenericRowData(1);
        nestedRow.setField(0, StringData.fromString("nested"));

        GenericRowData rowData = new GenericRowData(19);
        rowData.setField(0, true);
        rowData.setField(1, (byte) 1);
        rowData.setField(2, (short) 2);
        rowData.setField(3, 3);
        rowData.setField(4, 4L);
        rowData.setField(5, 1.5f);
        rowData.setField(6, 2.5d);
        rowData.setField(7, StringData.fromString("string"));
        rowData.setField(8, DecimalData.fromUnscaledLong(1234, 10, 2));
        rowData.setField(9, DecimalData.fromBigDecimal(new BigDecimal("1234567890"), 20, 0));
        rowData.setField(10, TimestampData.fromEpochMillis(1000L));
        rowData.setField(11, TimestampData.fromEpochMillis(2000L, 123456));
        rowData.setField(12, new byte[] {1, 2});
        rowData.setField(13, new GenericArrayData(new Object[] {StringData.fromString("a")}));
        rowData.setField(
                14,
                new GenericMapData(
                        Collections.singletonMap(
                                StringData.fromString("key"), StringData.fromString("value"))));
        rowData.setField(15, nestedRow);
        rowData.setField(16, 0);
        rowData.setField(17, 1L);
        rowData.setField(18, TimestampData.fromEpochMillis(3000L));

        HudiRowAsFlussRow row = new HudiRowAsFlussRow(rowData);

        assertThat(row.getFieldCount()).isEqualTo(16);
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getByte(1)).isEqualTo((byte) 1);
        assertThat(row.getShort(2)).isEqualTo((short) 2);
        assertThat(row.getInt(3)).isEqualTo(3);
        assertThat(row.getLong(4)).isEqualTo(4L);
        assertThat(row.getFloat(5)).isEqualTo(1.5f);
        assertThat(row.getDouble(6)).isEqualTo(2.5d);
        assertThat(row.getString(7)).isEqualTo(BinaryString.fromString("string"));
        assertThat(row.getDecimal(8, 10, 2)).isEqualTo(Decimal.fromUnscaledLong(1234, 10, 2));
        assertThat(row.getDecimal(9, 20, 0))
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal("1234567890"), 20, 0));
        assertThat(row.getTimestampNtz(10, 3)).isEqualTo(TimestampNtz.fromMillis(1000L));
        assertThat(row.getTimestampLtz(11, 6))
                .isEqualTo(TimestampLtz.fromEpochMillis(2000L, 123456));
        assertThat(row.getBytes(12)).containsExactly(1, 2);

        InternalArray array = row.getArray(13);
        assertThat(array.getString(0)).isEqualTo(BinaryString.fromString("a"));

        InternalMap map = row.getMap(14);
        assertThat(map.keyArray().getString(0)).isEqualTo(BinaryString.fromString("key"));
        assertThat(map.valueArray().getString(0)).isEqualTo(BinaryString.fromString("value"));

        InternalRow nested = row.getRow(15, 1);
        assertThat(nested.getFieldCount()).isEqualTo(1);
        assertThat(nested.getString(0)).isEqualTo(BinaryString.fromString("nested"));
    }
}

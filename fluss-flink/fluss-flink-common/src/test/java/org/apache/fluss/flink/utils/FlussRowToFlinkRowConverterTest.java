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

package org.apache.fluss.flink.utils;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.row.FlinkAsFlussArray;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.DateTimeUtils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.row.TestInternalRowGenerator.createAllRowType;
import static org.apache.fluss.row.TestInternalRowGenerator.createAllTypes;
import static org.apache.fluss.row.indexed.IndexedRowTest.genRecordForAllTypes;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter}. */
class FlussRowToFlinkRowConverterTest {

    @Test
    void testConverter() throws Exception {
        RowType rowType = createAllRowType();
        FlussRowToFlinkRowConverter flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(rowType);

        IndexedRow row = new IndexedRow(rowType.getChildren().toArray(new DataType[0]));
        try (IndexedRowWriter writer = genRecordForAllTypes(createAllTypes())) {
            row.pointTo(writer.segment(), 0, writer.position());

            ScanRecord scanRecord = new ScanRecord(0, 1L, ChangeType.UPDATE_BEFORE, row);

            RowData flinkRow = flussRowToFlinkRowConverter.toFlinkRowData(scanRecord);
            assertThat(flinkRow.getArity()).isEqualTo(rowType.getFieldCount());
            assertThat(flinkRow.getRowKind()).isEqualTo(RowKind.UPDATE_BEFORE);

            // now, check the field values
            assertThat(flinkRow.getBoolean(0)).isTrue();
            assertThat(flinkRow.getByte(1)).isEqualTo((byte) 2);
            assertThat(flinkRow.getShort(2)).isEqualTo((short) 10);
            assertThat(flinkRow.getInt(3)).isEqualTo(100);
            assertThat(flinkRow.getLong(4))
                    .isEqualTo(new BigInteger("12345678901234567890").longValue());
            assertThat(flinkRow.getFloat(5)).isEqualTo(13.2f);
            assertThat(flinkRow.getDouble(6)).isEqualTo(15.21);

            assertThat(DateTimeUtils.toLocalDate(flinkRow.getInt(7)))
                    .isEqualTo(LocalDate.of(2023, 10, 25));
            assertThat(DateTimeUtils.toLocalTime(flinkRow.getInt(8)))
                    .isEqualTo(LocalTime.of(9, 30, 0, 0));

            assertThat(flinkRow.getBinary(9)).isEqualTo("1234567890".getBytes());
            assertThat(flinkRow.getBinary(10)).isEqualTo("20".getBytes());

            assertThat(flinkRow.getString(11).toString()).isEqualTo("1");
            assertThat(flinkRow.getString(12).toString()).isEqualTo("hello");

            assertThat(flinkRow.getDecimal(13, 5, 2).toBigDecimal())
                    .isEqualTo(BigDecimal.valueOf(9, 2));
            assertThat(flinkRow.getDecimal(14, 20, 0).toBigDecimal()).isEqualTo(new BigDecimal(10));

            assertThat(flinkRow.getTimestamp(15, 1).toString())
                    .isEqualTo("2023-10-25T12:01:13.182");
            assertThat(flinkRow.getTimestamp(16, 5).toString())
                    .isEqualTo("2023-10-25T12:01:13.182");

            assertThat(flinkRow.getTimestamp(17, 1).toString())
                    .isEqualTo("2023-10-25T12:01:13.182");
            assertThat(flinkRow.getTimestamp(18, 5).toString())
                    .isEqualTo("2023-10-25T12:01:13.182");

            // array of int
            Integer[] array1 =
                    new FlinkAsFlussArray(flinkRow.getArray(19)).toObjectArray(DataTypes.INT());
            assertThat(array1).isEqualTo(new Integer[] {1, 2, 3, 4, 5, -11, null, 444, 102234});

            // array of float
            Float[] array2 =
                    new FlinkAsFlussArray(flinkRow.getArray(20)).toObjectArray(DataTypes.FLOAT());
            assertThat(array2)
                    .isEqualTo(
                            new Float[] {
                                0.1f, 1.1f, -0.5f, 6.6f, Float.MAX_VALUE, Float.MIN_VALUE
                            });

            // array of string
            assertThat(flinkRow.getArray(21).size()).isEqualTo(3);
            BinaryString[] stringArray1 =
                    new FlinkAsFlussArray(flinkRow.getArray(21).getArray(0))
                            .toObjectArray(DataTypes.STRING());
            assertThat(stringArray1)
                    .isEqualTo(new BinaryString[] {fromString("a"), null, fromString("c")});
            assertThat(flinkRow.getArray(21).isNullAt(1)).isTrue();
            BinaryString[] stringArray2 =
                    new FlinkAsFlussArray(flinkRow.getArray(21).getArray(2))
                            .toObjectArray(DataTypes.STRING());
            assertThat(stringArray2)
                    .isEqualTo(new BinaryString[] {fromString("hello"), fromString("world")});

            // map
            MapData mapData = flinkRow.getMap(22);
            ArrayData keyArray = mapData.keyArray();
            ArrayData valueArray = mapData.valueArray();
            assertThat(keyArray.getInt(0)).isEqualTo(0);
            assertThat(valueArray.isNullAt(0)).isTrue();
            assertThat(keyArray.getInt(1)).isEqualTo(1);
            assertThat(valueArray.getString(1).toString()).isEqualTo("1");
            assertThat(keyArray.getInt(2)).isEqualTo(2);
            assertThat(valueArray.getString(2).toString()).isEqualTo("2");

            // nested row: ROW<u1: INT, u2: ROW<v1: INT>, u3: STRING>
            assertThat(flinkRow.getRow(23, 3)).isNotNull();
            assertThat(flinkRow.getRow(23, 3).getInt(0)).isEqualTo(123);
            assertThat(flinkRow.getRow(23, 3).getRow(1, 1)).isNotNull();
            assertThat(flinkRow.getRow(23, 3).getRow(1, 1).getInt(0)).isEqualTo(22);
            assertThat(flinkRow.getRow(23, 3).getString(2).toString()).isEqualTo("Test");
        }
    }
}

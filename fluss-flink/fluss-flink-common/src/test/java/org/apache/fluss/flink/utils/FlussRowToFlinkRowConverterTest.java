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
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.DateTimeUtils;
import org.apache.fluss.utils.TypeUtils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.row.TestInternalRowGenerator.createAllRowType;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter}. */
class FlussRowToFlinkRowConverterTest {

    @Test
    void testConverter() throws Exception {
        // TODO: Exclude ARRAY type for now, will be supported in a future PR
        RowType allRowType = createAllRowType();
        // Exclude the last field (array type) as it's not yet supported in Flink connector
        RowType rowType =
                new RowType(allRowType.getFields().subList(0, allRowType.getFieldCount() - 1));
        FlussRowToFlinkRowConverter flussRowToFlinkRowConverter =
                new FlussRowToFlinkRowConverter(rowType);

        DataType[] dataTypes = rowType.getChildren().toArray(new DataType[0]);
        IndexedRow row = new IndexedRow(dataTypes);
        // Generate data for 19 fields (exclude array type at the end)
        try (IndexedRowWriter writer = genRecordForAllTypesExceptArray(dataTypes)) {
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
            assertThat(flinkRow.isNullAt(18)).isTrue();
        }
    }

    // Helper method to generate test data for all types except array
    private static IndexedRowWriter genRecordForAllTypesExceptArray(DataType[] dataTypes) {
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeBoolean(true);
        writer.writeByte((byte) 2);
        writer.writeShort(Short.parseShort("10"));
        writer.writeInt(100);
        writer.writeLong(new BigInteger("12345678901234567890").longValue());
        writer.writeFloat(Float.parseFloat("13.2"));
        writer.writeDouble(Double.parseDouble("15.21"));
        writer.writeInt((int) TypeUtils.castFromString("2023-10-25", DataTypes.DATE()));
        writer.writeInt((int) TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()));
        writer.writeBinary("1234567890".getBytes(), 20);
        writer.writeBytes("20".getBytes());
        writer.writeChar(fromString("1"), 2);
        writer.writeString(fromString("hello"));
        writer.writeDecimal(Decimal.fromUnscaledLong(9, 5, 2), 5);
        writer.writeDecimal(Decimal.fromBigDecimal(new BigDecimal(10), 20, 0), 20);
        writer.writeTimestampNtz(TimestampNtz.fromMillis(1698235273182L), 1);
        writer.writeTimestampNtz(TimestampNtz.fromMillis(1698235273182L), 5);
        writer.writeTimestampLtz(TimestampLtz.fromEpochMillis(1698235273182L), 1);
        writer.writeTimestampLtz(TimestampLtz.fromEpochMillis(1698235273182L), 5);
        writer.setNullAt(18); // Last field (index 18) is null
        return writer;
    }
}

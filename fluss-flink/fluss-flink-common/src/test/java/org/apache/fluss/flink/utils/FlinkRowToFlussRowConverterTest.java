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

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.DateTimeUtils;
import org.apache.fluss.utils.TypeUtils;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkRowType;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.row.TestInternalRowGenerator.createAllRowType;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.flink.utils.FlinkRowToFlussRowConverter}. */
public class FlinkRowToFlussRowConverterTest {

    @Test
    void testConverter() throws Exception {
        // TODO: Exclude ARRAY type for now, will be supported in a future PR
        RowType allFlussRowType = createAllRowType();
        // Exclude the last field (array type) as it's not yet supported in Flink connector
        RowType flussRowType =
                new RowType(
                        allFlussRowType
                                .getFields()
                                .subList(0, allFlussRowType.getFieldCount() - 1));

        // test indexed row converter
        try (FlinkRowToFlussRowConverter converter =
                FlinkRowToFlussRowConverter.create(toFlinkRowType(flussRowType))) {
            InternalRow internalRow = converter.toInternalRow(genRowDataForAllType());
            assertThat(internalRow.getFieldCount()).isEqualTo(19);
            assertAllTypeEqualsExceptArray(internalRow);
        }

        // test compacted row converter
        try (FlinkRowToFlussRowConverter converter =
                FlinkRowToFlussRowConverter.create(
                        toFlinkRowType(flussRowType), KvFormat.COMPACTED)) {
            InternalRow internalRow = converter.toInternalRow(genRowDataForAllType());
            assertThat(internalRow.getFieldCount()).isEqualTo(19);
            assertAllTypeEqualsExceptArray(internalRow);
        }
    }

    private static RowData genRowDataForAllType() {
        GenericRowData genericRowData = new GenericRowData(19);
        genericRowData.setField(0, true);
        genericRowData.setField(1, (byte) 2);
        genericRowData.setField(2, Short.parseShort("10"));
        genericRowData.setField(3, 100);
        genericRowData.setField(4, new BigInteger("12345678901234567890").longValue());
        genericRowData.setField(5, Float.parseFloat("13.2"));
        genericRowData.setField(6, Double.parseDouble("15.21"));
        genericRowData.setField(7, TypeUtils.castFromString("2023-10-25", DataTypes.DATE()));
        genericRowData.setField(8, TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()));
        genericRowData.setField(9, "1234567890".getBytes());
        genericRowData.setField(10, "20".getBytes());
        genericRowData.setField(11, StringData.fromBytes("1".getBytes()));
        genericRowData.setField(12, StringData.fromString("hello"));
        genericRowData.setField(13, DecimalData.fromUnscaledLong(9, 5, 2));
        genericRowData.setField(14, DecimalData.fromBigDecimal(new BigDecimal(10), 20, 0));
        genericRowData.setField(15, TimestampData.fromEpochMillis(1698235273182L, 0));
        genericRowData.setField(16, TimestampData.fromEpochMillis(1698235273182L, 0));
        genericRowData.setField(
                17,
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2023-10-25T12:01:13.182")));
        genericRowData.setField(18, null);
        return genericRowData;
    }

    // Helper method to assert all types except array (19 fields instead of 20)
    private static void assertAllTypeEqualsExceptArray(InternalRow row) {
        assertThat(row.getBoolean(0)).isEqualTo(true);
        assertThat(row.getByte(1)).isEqualTo((byte) 2);
        assertThat(row.getShort(2)).isEqualTo(Short.parseShort("10"));
        assertThat(row.getInt(3)).isEqualTo(100);
        assertThat(row.getLong(4)).isEqualTo(new BigInteger("12345678901234567890").longValue());
        assertThat(row.getFloat(5)).isEqualTo(Float.parseFloat("13.2"));
        assertThat(row.getDouble(6)).isEqualTo(Double.parseDouble("15.21"));
        assertThat(DateTimeUtils.toLocalDate(row.getInt(7))).isEqualTo(LocalDate.of(2023, 10, 25));
        assertThat(DateTimeUtils.toLocalTime(row.getInt(8))).isEqualTo(LocalTime.of(9, 30, 0, 0));
        assertThat(row.getBinary(9, 20)).isEqualTo("1234567890".getBytes());
        assertThat(row.getBytes(10)).isEqualTo("20".getBytes());
        assertThat(row.getChar(11, 2)).isEqualTo(fromString("1"));
        assertThat(row.getString(12)).isEqualTo(fromString("hello"));
        assertThat(row.getDecimal(13, 5, 2)).isEqualTo(Decimal.fromUnscaledLong(9, 5, 2));
        assertThat(row.getDecimal(14, 20, 0))
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal(10), 20, 0));
        assertThat(row.getTimestampNtz(15, 1).toString()).isEqualTo("2023-10-25T12:01:13.182");
        assertThat(row.getTimestampNtz(16, 5).toString()).isEqualTo("2023-10-25T12:01:13.182");
        assertThat(row.getTimestampLtz(17, 1).toString()).isEqualTo("2023-10-25T12:01:13.182Z");
        // Field 18 is null in the 19-field row (last TIMESTAMP_LTZ field was excluded to make room)
        assertThat(row.isNullAt(18)).isTrue();
    }
}

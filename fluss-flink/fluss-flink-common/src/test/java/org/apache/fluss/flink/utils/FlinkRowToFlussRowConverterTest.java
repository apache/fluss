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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.TypeUtils;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;

import static org.apache.flink.table.data.binary.BinaryStringData.fromString;
import static org.apache.fluss.flink.utils.FlinkConversions.toFlinkRowType;
import static org.apache.fluss.row.TestInternalRowGenerator.createAllRowType;
import static org.apache.fluss.row.indexed.IndexedRowTest.assertAllTypeEquals;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.flink.utils.FlinkRowToFlussRowConverter}. */
public class FlinkRowToFlussRowConverterTest {

    @Test
    void testConverter() throws Exception {
        RowType flussRowType = createAllRowType();

        // test indexed row converter
        try (FlinkRowToFlussRowConverter converter =
                FlinkRowToFlussRowConverter.create(toFlinkRowType(flussRowType))) {
            InternalRow internalRow = converter.toInternalRow(genRowDataForAllType());
            assertThat(internalRow.getFieldCount()).isEqualTo(22);
            assertAllTypeEquals(internalRow);
        }

        // test compacted row converter
        try (FlinkRowToFlussRowConverter converter =
                FlinkRowToFlussRowConverter.create(
                        toFlinkRowType(flussRowType), KvFormat.COMPACTED)) {
            InternalRow internalRow = converter.toInternalRow(genRowDataForAllType());
            assertThat(internalRow.getFieldCount()).isEqualTo(22);
            assertAllTypeEquals(internalRow);
        }
    }

    private static RowData genRowDataForAllType() {
        GenericRowData genericRowData = new GenericRowData(22);
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
        genericRowData.setField(
                18,
                TimestampData.fromLocalDateTime(LocalDateTime.parse("2023-10-25T12:01:13.182")));
        genericRowData.setField(
                19, new GenericArrayData(new Integer[] {1, 2, 3, 4, 5, -11, null, 444, 102234}));
        genericRowData.setField(
                20,
                new GenericArrayData(
                        new float[] {0.1f, 1.1f, -0.5f, 6.6f, Float.MAX_VALUE, Float.MIN_VALUE}));
        genericRowData.setField(
                21,
                new GenericArrayData(
                        new GenericArrayData[] {
                            new GenericArrayData(
                                    new StringData[] {fromString("a"), null, fromString("c")}),
                            null,
                            new GenericArrayData(
                                    new StringData[] {fromString("hello"), fromString("world")})
                        }));
        return genericRowData;
    }
}

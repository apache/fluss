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

package org.apache.fluss.lake.delta.utils;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.lake.delta.testutils.DeltaTestUtils.render;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlussRowToDeltaValues} and {@link DeltaColumnarBatch}. */
class DeltaColumnarBatchTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new org.apache.fluss.types.DataType[] {
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.DECIMAL(10, 2),
                        DataTypes.TIMESTAMP(6),
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.BYTES(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.ROW(
                                DataTypes.FIELD("city", DataTypes.STRING()),
                                DataTypes.FIELD("zip", DataTypes.INT()))
                    },
                    new String[] {
                        "id", "name", "amount", "ts", "ts_ltz", "payload", "tags", "attrs",
                        "address"
                    });

    @Test
    void testColumnsReadBackTheRowValues() {
        GenericRow address = GenericRow.of(BinaryString.fromString("Seoul"), 4500);
        GenericRow full =
                GenericRow.of(
                        1,
                        BinaryString.fromString("a"),
                        Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2),
                        TimestampNtz.fromMicros(1_000_001L),
                        TimestampLtz.fromEpochMillis(2_000L),
                        new byte[] {1, 2},
                        new GenericArray(
                                new Object[] {
                                    BinaryString.fromString("x"), null, BinaryString.fromString("y")
                                }),
                        new GenericMap(Collections.singletonMap(BinaryString.fromString("k"), 7)),
                        address);
        GenericRow nulls = new GenericRow(ROW_TYPE.getFieldCount());

        FlussRowToDeltaValues converter = new FlussRowToDeltaValues(ROW_TYPE);
        List<Object[]> rows = new ArrayList<>();
        rows.add(converter.convert(full));
        rows.add(converter.convert(nulls));
        StructType schema = FlussDataTypeToDeltaDataType.toDeltaSchema(ROW_TYPE);
        ColumnarBatch batch = DeltaColumnarBatch.of(schema, rows);

        assertThat(batch.getSize()).isEqualTo(2);
        assertThat(renderRow(batch, 0))
                .containsExactly(
                        "1",
                        "a",
                        "12.34",
                        "1000001",
                        "2000000",
                        "[1, 2]",
                        "[x, null, y]",
                        "{k=7}",
                        "(Seoul, 4500)");
        assertThat(renderRow(batch, 1))
                .containsExactly(
                        "null", "null", "null", "null", "null", "null", "null", "null", "null");
    }

    @Test
    void testDeleteAndAddColumns() {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        FlussRowToDeltaValues converter = new FlussRowToDeltaValues(rowType);
        ColumnarBatch batch =
                DeltaColumnarBatch.of(
                        FlussDataTypeToDeltaDataType.toDeltaSchema(rowType),
                        Collections.singletonList(
                                converter.convert(GenericRow.of(1, BinaryString.fromString("a")))));

        ColumnarBatch deleted = batch.withDeletedColumnAt(0);
        assertThat(deleted.getSchema().fieldNames()).containsExactly("f1");
        assertThat(render(deleted.getColumnVector(0), 0)).isEqualTo("a");

        ColumnarBatch added =
                deleted.withNewColumn(
                        0,
                        new StructField("f0", StringType.STRING, true),
                        batch.getColumnVector(1));
        assertThat(added.getSchema().fieldNames()).containsExactly("f0", "f1");
        assertThat(added.getSize()).isEqualTo(1);
    }

    private static List<String> renderRow(ColumnarBatch batch, int rowId) {
        String[] values = new String[batch.getSchema().length()];
        for (int i = 0; i < values.length; i++) {
            values[i] = render(batch.getColumnVector(i), rowId);
        }
        return Arrays.asList(values);
    }
}

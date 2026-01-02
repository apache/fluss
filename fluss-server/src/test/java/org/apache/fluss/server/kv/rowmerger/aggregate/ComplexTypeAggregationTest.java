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

package org.apache.fluss.server.kv.rowmerger.aggregate;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.AggregateRowMerger;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ComplexTypeAggregationTest {

    private static final short SCHEMA_ID = (short) 1;

    @Test
    void testArrayLastValue() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("arr", DataTypes.ARRAY(DataTypes.INT()), AggFunctions.LAST_VALUE())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger = createMerger(schema, tableConfig);

        BinaryArray arr1 = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        BinaryArray arr2 = BinaryArray.fromPrimitiveArray(new int[] {4, 5, 6});

        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, arr1});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, arr2});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        BinaryArray resultArr = (BinaryArray) merged.row.getArray(1);
        assertThat(resultArr).isEqualTo(arr2);
    }

    @Test
    void testArrayMinMax() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("min_arr", DataTypes.ARRAY(DataTypes.INT()), AggFunctions.MIN())
                        .column("max_arr", DataTypes.ARRAY(DataTypes.INT()), AggFunctions.MAX())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // arr1 < arr2
        BinaryArray arr1 = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3});
        BinaryArray arr2 = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 4});

        BinaryRow row1 = compactedRow(schema.getRowType(), new Object[] {1, arr1, arr1});
        BinaryRow row2 = compactedRow(schema.getRowType(), new Object[] {1, arr2, arr2});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        BinaryArray minResult = (BinaryArray) merged.row.getArray(1);
        BinaryArray maxResult = (BinaryArray) merged.row.getArray(2);

        assertThat(minResult).isEqualTo(arr1);
        assertThat(maxResult).isEqualTo(arr2);

        // Test with different sizes
        // arr3 < arr1 (size 2 vs 3, prefix match)
        BinaryArray arr3 = BinaryArray.fromPrimitiveArray(new int[] {1, 2});
        BinaryRow row3 = compactedRow(schema.getRowType(), new Object[] {1, arr3, arr3});

        merged = merger.merge(toBinaryValue(row1), toBinaryValue(row3));
        minResult = (BinaryArray) merged.row.getArray(1);
        maxResult = (BinaryArray) merged.row.getArray(2);

        assertThat(minResult).isEqualTo(arr3);
        assertThat(maxResult).isEqualTo(arr1);
    }

    @Test
    void testRowMinMax() {
        RowType nestedRowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("min_row", nestedRowType, AggFunctions.MIN())
                        .column("max_row", nestedRowType, AggFunctions.MAX())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregateRowMerger merger = createMerger(schema, tableConfig);

        // row1 < row2
        BinaryRow nestedRow1 =
                compactedRow(nestedRowType, new Object[] {1, BinaryString.fromString("a")});
        BinaryRow nestedRow2 =
                compactedRow(nestedRowType, new Object[] {1, BinaryString.fromString("b")});

        BinaryRow row1 =
                compactedRow(schema.getRowType(), new Object[] {1, nestedRow1, nestedRow1});
        BinaryRow row2 =
                compactedRow(schema.getRowType(), new Object[] {1, nestedRow2, nestedRow2});

        BinaryValue merged = merger.merge(toBinaryValue(row1), toBinaryValue(row2));

        BinaryRow minResult = (BinaryRow) merged.row.getRow(1, 2);
        BinaryRow maxResult = (BinaryRow) merged.row.getRow(2, 2);

        assertThat(minResult).isEqualTo(nestedRow1);
        assertThat(maxResult).isEqualTo(nestedRow2);
    }

    private BinaryValue toBinaryValue(BinaryRow row) {
        return new BinaryValue(SCHEMA_ID, row);
    }

    private AggregateRowMerger createMerger(Schema schema, TableConfig tableConfig) {
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(schema, (short) 1));
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);
        merger.configureTargetColumns(null, (short) 1, schema);
        return merger;
    }

    private BinaryRow compactedRow(RowType rowType, Object[] values) {
        RowEncoder encoder = RowEncoder.create(KvFormat.COMPACTED, rowType);
        encoder.startNewRow();
        for (int i = 0; i < values.length; i++) {
            encoder.encodeField(i, values[i]);
        }
        return encoder.finishRow();
    }
}

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

package org.apache.fluss.row;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.row.indexed.IndexedRowWriter;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Test of {@link org.apache.fluss.row.InternalRow}. */
public class InternalRowTest {

    @Test
    void testGetDataClass() {
        assertThat(InternalRow.getDataClass(DataTypes.CHAR(10))).isEqualTo(BinaryString.class);
        assertThat(InternalRow.getDataClass(DataTypes.STRING())).isEqualTo(BinaryString.class);
        assertThat(InternalRow.getDataClass(DataTypes.BOOLEAN())).isEqualTo(Boolean.class);
        assertThat(InternalRow.getDataClass(DataTypes.BINARY(10))).isEqualTo(byte[].class);
        assertThat(InternalRow.getDataClass(DataTypes.BYTES())).isEqualTo(byte[].class);
        assertThat(InternalRow.getDataClass(DataTypes.DECIMAL(5, 2))).isEqualTo(Decimal.class);
        assertThat(InternalRow.getDataClass(DataTypes.TINYINT())).isEqualTo(Byte.class);
        assertThat(InternalRow.getDataClass(DataTypes.SMALLINT())).isEqualTo(Short.class);
        assertThat(InternalRow.getDataClass(DataTypes.INT())).isEqualTo(Integer.class);
        assertThat(InternalRow.getDataClass(DataTypes.DATE())).isEqualTo(Integer.class);
        assertThat(InternalRow.getDataClass(DataTypes.TIME())).isEqualTo(Integer.class);
        assertThat(InternalRow.getDataClass(DataTypes.BIGINT())).isEqualTo(Long.class);
        assertThat(InternalRow.getDataClass(DataTypes.FLOAT())).isEqualTo(Float.class);
        assertThat(InternalRow.getDataClass(DataTypes.DOUBLE())).isEqualTo(Double.class);
        assertThat(InternalRow.getDataClass(DataTypes.TIMESTAMP())).isEqualTo(TimestampNtz.class);
        assertThat(InternalRow.getDataClass(DataTypes.TIMESTAMP_LTZ()))
                .isEqualTo(TimestampLtz.class);
        assertThat(InternalRow.getDataClass(DataTypes.ARRAY(DataTypes.TIMESTAMP())))
                .isEqualTo(InternalArray.class);
        assertThat(InternalRow.getDataClass(DataTypes.MAP(DataTypes.INT(), DataTypes.TIMESTAMP())))
                .isEqualTo(InternalMap.class);
        assertThat(InternalRow.getDataClass(DataTypes.ROW(new DataField("a", DataTypes.INT()))))
                .isEqualTo(InternalRow.class);
    }

    /** copyStrings=true copies STRING/CHAR (including nested) so values survive buffer release. */
    @Test
    void testDeepFieldGetterCopiesStringsAfterBufferRelease() {
        DataType[] dataTypes = {
            DataTypes.STRING(),
            DataTypes.CHAR(2),
            DataTypes.ARRAY(DataTypes.STRING()),
            DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()),
            DataTypes.ROW(new DataField("nested", DataTypes.STRING()))
        };

        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        BinaryWriter.ValueWriter[] writers = new BinaryWriter.ValueWriter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            writers[i] = BinaryWriter.createValueWriter(dataTypes[i], INDEXED);
        }
        writers[0].writeValue(writer, 0, fromString("hello"));
        writers[1].writeValue(writer, 1, fromString("ab"));
        writers[2].writeValue(writer, 2, GenericArray.of(fromString("x"), fromString("y")));
        writers[3].writeValue(writer, 3, GenericMap.of(1, fromString("one")));
        writers[4].writeValue(writer, 4, GenericRow.of(fromString("nested")));

        IndexedRow row = new IndexedRow(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());

        Object copiedString = deepGetter(dataTypes[0], 0, true).getFieldOrNull(row);
        Object copiedChar = deepGetter(dataTypes[1], 1, true).getFieldOrNull(row);
        InternalArray copiedArray =
                (InternalArray) deepGetter(dataTypes[2], 2, true).getFieldOrNull(row);
        InternalMap copiedMap = (InternalMap) deepGetter(dataTypes[3], 3, true).getFieldOrNull(row);
        InternalRow copiedRow = (InternalRow) deepGetter(dataTypes[4], 4, true).getFieldOrNull(row);

        // A copyStrings=false view stays un-materialized so it reflects later overwrites.
        Object viewString = deepGetter(dataTypes[0], 0, false).getFieldOrNull(row);

        MemorySegment segment = writer.segment();
        for (int i = 0; i < writer.position(); i++) {
            segment.put(i, (byte) 0);
        }

        assertThat(copiedString).isEqualTo(fromString("hello"));
        assertThat(copiedChar).isEqualTo(fromString("ab"));
        assertThat(copiedArray.getString(0)).isEqualTo(fromString("x"));
        assertThat(copiedArray.getString(1)).isEqualTo(fromString("y"));
        assertThat(copiedMap.valueArray().getString(0)).isEqualTo(fromString("one"));
        assertThat(copiedRow.getString(0)).isEqualTo(fromString("nested"));
        assertThat(viewString).isNotEqualTo(fromString("hello"));
    }

    private static InternalRow.FieldGetter deepGetter(
            DataType dataType, int pos, boolean copyStrings) {
        return InternalRow.createDeepFieldGetter(dataType, pos, copyStrings);
    }
}

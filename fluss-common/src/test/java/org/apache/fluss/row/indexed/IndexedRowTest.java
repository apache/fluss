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

package org.apache.fluss.row.indexed;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.utils.DateTimeUtils;
import org.apache.fluss.utils.TypeUtils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.row.TestInternalRowGenerator.createAllTypes;
import static org.apache.fluss.testutils.InternalArrayAssert.assertThatArray;
import static org.apache.fluss.testutils.InternalMapAssert.assertThatMap;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test of {@link IndexedRow} and {@link IndexedRowWriter}. */
public class IndexedRowTest {

    @Test
    void testConstructor() {
        IndexedRow indexedRow = new IndexedRow(new DataType[0]);
        assertThat(indexedRow.getFieldCount()).isEqualTo(0);
        assertThat(indexedRow.getHeaderSizeInBytes()).isEqualTo(0);

        indexedRow = new IndexedRow(new DataType[] {new IntType()});
        assertThat(indexedRow.getFieldCount()).isEqualTo(1);
        assertThat(indexedRow.getHeaderSizeInBytes()).isEqualTo(1);

        indexedRow = new IndexedRow(new DataType[] {new IntType(), new StringType()});
        assertThat(indexedRow.getFieldCount()).isEqualTo(2);
        assertThat(indexedRow.getHeaderSizeInBytes()).isEqualTo(5);
    }

    @Test
    void testWriterAndIndexedRowGetter() {
        DataType[] dataTypes = createAllTypes();
        IndexedRow row = new IndexedRow(dataTypes);
        IndexedRowWriter writer = genRecordForAllTypes(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());

        assertAllTypeEquals(row);

        assertThat(row.getFieldCount()).isEqualTo(24);
        assertThat(row.anyNull()).isFalse();
        assertThat(row.anyNull(new int[] {0, 1})).isFalse();
    }

    @Test
    void testCopy() {
        DataType[] dataTypes = {DataTypes.INT()};
        IndexedRow row = new IndexedRow(dataTypes);
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeInt(1000);

        row.pointTo(writer.segment(), 0, writer.position());

        assertThat(row.getInt(0)).isEqualTo(1000);
        IndexedRow indexedRow1 = row.copy();
        assertThat(indexedRow1.getInt(0)).isEqualTo(1000);
        IndexedRow indexedRow2 = new IndexedRow(dataTypes);
        row.copy(indexedRow2);
        assertThat(indexedRow2.getInt(0)).isEqualTo(1000);
    }

    @Test
    public void testEqualsAndHashCode() {
        DataType[] dataTypes = {DataTypes.INT()};
        IndexedRow row = new IndexedRow(dataTypes);
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeInt(1);
        row.pointTo(writer.segment(), 0, writer.position());

        byte[] buffer = new byte[row.getSizeInBytes() + 23];
        System.arraycopy(writer.buffer(), 0, buffer, 13, row.getSizeInBytes());
        IndexedRow newRow = new IndexedRow(dataTypes);
        newRow.pointTo(MemorySegment.wrap(buffer), 13, row.getSizeInBytes());

        assertThat(row).isEqualTo(row);
        assertThat(newRow).isEqualTo(row);
        assertThat(newRow.hashCode()).isEqualTo(row.hashCode());
    }

    @Test
    void testCreateFieldWriter() {
        DataType[] dataTypes = createAllTypes();
        IndexedRow row = new IndexedRow(dataTypes);
        IndexedRowWriter writer = genRecordForAllTypes(dataTypes);
        row.pointTo(writer.segment(), 0, writer.position());

        InternalRow.FieldGetter[] fieldGetter = new InternalRow.FieldGetter[dataTypes.length];
        BinaryWriter.ValueWriter[] writers = new BinaryWriter.ValueWriter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            fieldGetter[i] = InternalRow.createFieldGetter(dataTypes[i], i);
            writers[i] = BinaryWriter.createValueWriter(dataTypes[i], INDEXED);
        }

        IndexedRowWriter writer1 = new IndexedRowWriter(dataTypes);
        for (int i = 0; i < dataTypes.length; i++) {
            writers[i].writeValue(writer1, i, fieldGetter[i].getFieldOrNull(row));
        }

        IndexedRow row1 = new IndexedRow(dataTypes);
        row1.pointTo(writer1.segment(), 0, writer1.position());
        assertAllTypeEquals(row1);
    }

    @Test
    void testWriterReset() {
        DataType[] dataTypes = {DataTypes.INT()};
        IndexedRow row = new IndexedRow(dataTypes);
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeInt(1);
        row.pointTo(writer.segment(), 0, writer.position());

        writer.reset();
        assertThat(writer.position()).isEqualTo(1);
    }

    @Test
    void testProjectRow() {
        DataType[] dataTypes = {
            DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()
        };
        IndexedRow row = new IndexedRow(dataTypes);
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);
        writer.writeInt(1000);
        writer.writeInt(2000);
        writer.writeString(fromString("hello"));
        writer.writeLong(500000L);
        row.pointTo(writer.segment(), 0, writer.position());
        assertThat(row.getInt(0)).isEqualTo(1000);
        assertThat(row.getString(2)).isEqualTo(fromString("hello"));

        IndexedRow projectRow = row.projectRow(new int[] {0, 2});
        assertThat(projectRow.getInt(0)).isEqualTo(1000);
        assertThat(projectRow.getString(1)).isEqualTo(fromString("hello"));

        assertThatThrownBy(() -> row.projectRow(new int[] {0, 1, 2, 3, 4}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("project fields length is larger than row arity");
    }

    public static IndexedRowWriter genRecordForAllTypes(DataType[] dataTypes) {
        IndexedRowWriter writer = new IndexedRowWriter(dataTypes);

        BinaryWriter.ValueWriter[] writers = new BinaryWriter.ValueWriter[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            writers[i] = BinaryWriter.createValueWriter(dataTypes[i], INDEXED);
        }

        writers[0].writeValue(writer, 0, true);
        writers[1].writeValue(writer, 1, (byte) 2);
        writers[2].writeValue(writer, 2, Short.parseShort("10"));
        writers[3].writeValue(writer, 3, 100);
        writers[4].writeValue(writer, 4, new BigInteger("12345678901234567890").longValue());
        writers[5].writeValue(writer, 5, Float.parseFloat("13.2"));
        writers[6].writeValue(writer, 6, Double.parseDouble("15.21"));
        writers[7].writeValue(
                writer, 7, (int) TypeUtils.castFromString("2023-10-25", DataTypes.DATE()));
        writers[8].writeValue(
                writer, 8, (int) TypeUtils.castFromString("09:30:00.0", DataTypes.TIME()));
        writers[9].writeValue(writer, 9, "1234567890".getBytes());
        writers[10].writeValue(writer, 10, "20".getBytes());
        writers[11].writeValue(writer, 11, fromString("1"));
        writers[12].writeValue(writer, 12, fromString("hello"));
        writers[13].writeValue(writer, 13, Decimal.fromUnscaledLong(9, 5, 2));
        writers[14].writeValue(writer, 14, Decimal.fromBigDecimal(new BigDecimal(10), 20, 0));
        writers[15].writeValue(writer, 15, TimestampNtz.fromMillis(1698235273182L));
        writers[16].writeValue(writer, 16, TimestampNtz.fromMillis(1698235273182L));
        writers[17].writeValue(writer, 17, TimestampLtz.fromEpochMillis(1698235273182L));
        writers[18].writeValue(writer, 18, TimestampLtz.fromEpochMillis(1698235273182L));
        writers[19].writeValue(writer, 19, GenericArray.of(1, 2, 3, 4, 5, -11, null, 444, 102234));
        writers[20].writeValue(
                writer,
                20,
                GenericArray.of(0.1f, 1.1f, -0.5f, 6.6f, Float.MAX_VALUE, Float.MIN_VALUE));
        writers[21].writeValue(
                writer,
                21,
                GenericArray.of(
                        GenericArray.of(fromString("a"), null, fromString("c")),
                        null,
                        GenericArray.of(fromString("hello"), fromString("world"))));

        GenericMap genericMap = GenericMap.of(0, null, 1, fromString("1"), 2, fromString("2"));
        writers[22].writeValue(writer, 22, genericMap);

        GenericRow innerRow = GenericRow.of(22);
        GenericRow nestedRow = GenericRow.of(123, innerRow, fromString("Test"));
        writers[23].writeValue(writer, 23, nestedRow);
        return writer;
    }

    public static void assertAllTypeEquals(InternalRow row) {
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
        assertThat(row.getTimestampLtz(18, 5).toString()).isEqualTo("2023-10-25T12:01:13.182Z");
        assertThatArray(row.getArray(19))
                .withElementType(DataTypes.INT())
                .isEqualTo(GenericArray.of(1, 2, 3, 4, 5, -11, null, 444, 102234));
        assertThatArray(row.getArray(20))
                .withElementType(DataTypes.FLOAT().copy(false))
                .isEqualTo(
                        GenericArray.of(0.1f, 1.1f, -0.5f, 6.6f, Float.MAX_VALUE, Float.MIN_VALUE));
        assertThatArray(row.getArray(21))
                .withElementType(DataTypes.ARRAY(DataTypes.STRING()))
                .isEqualTo(
                        GenericArray.of(
                                GenericArray.of(fromString("a"), null, fromString("c")),
                                null,
                                GenericArray.of(fromString("hello"), fromString("world"))));
        assertThatMap(row.getMap(22))
                .withMapType(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                .isEqualTo(
                        GenericMap.of(
                                0, null,
                                1, fromString("1"),
                                2, fromString("2")));
        GenericRow expectedInnerRow = GenericRow.of(22);
        GenericRow expectedNestedRow = GenericRow.of(123, expectedInnerRow, fromString("Test"));
        assertThatRow(row.getRow(23, 3))
                .withSchema(
                        DataTypes.ROW(
                                DataTypes.FIELD("u1", DataTypes.INT()),
                                DataTypes.FIELD(
                                        "u2",
                                        DataTypes.ROW(DataTypes.FIELD("v1", DataTypes.INT()))),
                                DataTypes.FIELD("u3", DataTypes.STRING())))
                .isEqualTo(expectedNestedRow);
    }

    @Test
    void testIndexedArrayGetRow() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f1", DataTypes.INT()),
                        DataTypes.FIELD("f2", DataTypes.STRING()));

        DataType[] fieldTypes = new DataType[] {DataTypes.ARRAY(rowType)};
        IndexedRow outerRow = new IndexedRow(fieldTypes);
        IndexedRowWriter outerWriter = new IndexedRowWriter(fieldTypes);

        GenericRow row1 = GenericRow.of(100, fromString("first"));
        GenericRow row2 = GenericRow.of(200, fromString("second"));
        GenericArray arrayData = GenericArray.of(row1, row2);

        BinaryWriter.ValueWriter arrayWriter =
                BinaryWriter.createValueWriter(fieldTypes[0], INDEXED);
        arrayWriter.writeValue(outerWriter, 0, arrayData);

        outerRow.pointTo(outerWriter.segment(), 0, outerWriter.position());

        InternalArray array = outerRow.getArray(0);
        assertThat(array).isNotNull();
        assertThat(array.size()).isEqualTo(2);

        InternalRow nestedRow1 = array.getRow(0, 2);
        assertThat(nestedRow1.getInt(0)).isEqualTo(100);
        assertThat(nestedRow1.getString(1)).isEqualTo(fromString("first"));

        InternalRow nestedRow2 = array.getRow(1, 2);
        assertThat(nestedRow2.getInt(0)).isEqualTo(200);
        assertThat(nestedRow2.getString(1)).isEqualTo(fromString("second"));
    }

    @Test
    void testIndexedArrayGetRowWithInvalidType() {
        DataType[] fieldTypes = new DataType[] {DataTypes.ARRAY(DataTypes.INT())};
        IndexedRow outerRow = new IndexedRow(fieldTypes);
        IndexedRowWriter outerWriter = new IndexedRowWriter(fieldTypes);

        GenericArray arrayData = GenericArray.of(1, 2, 3);
        BinaryWriter.ValueWriter arrayWriter =
                BinaryWriter.createValueWriter(fieldTypes[0], INDEXED);
        arrayWriter.writeValue(outerWriter, 0, arrayData);

        outerRow.pointTo(outerWriter.segment(), 0, outerWriter.position());

        InternalArray array = outerRow.getArray(0);
        assertThatThrownBy(() -> array.getRow(0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Can not get row from Array of type");
    }

    @Test
    void testIndexedArrayGetRowWithWrongNumFields() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f1", DataTypes.INT()),
                        DataTypes.FIELD("f2", DataTypes.STRING()));

        DataType[] fieldTypes = new DataType[] {DataTypes.ARRAY(rowType)};
        IndexedRow outerRow = new IndexedRow(fieldTypes);
        IndexedRowWriter outerWriter = new IndexedRowWriter(fieldTypes);

        GenericRow row1 = GenericRow.of(100, fromString("first"));
        GenericArray arrayData = GenericArray.of(row1);

        BinaryWriter.ValueWriter arrayWriter =
                BinaryWriter.createValueWriter(fieldTypes[0], INDEXED);
        arrayWriter.writeValue(outerWriter, 0, arrayData);

        outerRow.pointTo(outerWriter.segment(), 0, outerWriter.position());

        InternalArray array = outerRow.getArray(0);
        assertThatThrownBy(() -> array.getRow(0, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected number of fields");
    }

    @Test
    void testIndexedArrayGetNestedArray() {
        DataType innerArrayType = DataTypes.ARRAY(DataTypes.INT());
        DataType outerArrayType = DataTypes.ARRAY(innerArrayType);

        DataType[] fieldTypes = new DataType[] {outerArrayType};
        IndexedRow row = new IndexedRow(fieldTypes);
        IndexedRowWriter rowWriter = new IndexedRowWriter(fieldTypes);

        GenericArray innerArray1 = GenericArray.of(1, 2, 3);
        GenericArray innerArray2 = GenericArray.of(4, 5, 6);
        GenericArray outerArrayData = GenericArray.of(innerArray1, innerArray2);

        BinaryWriter.ValueWriter arrayWriter =
                BinaryWriter.createValueWriter(fieldTypes[0], INDEXED);
        arrayWriter.writeValue(rowWriter, 0, outerArrayData);

        row.pointTo(rowWriter.segment(), 0, rowWriter.position());

        InternalArray outerArray = row.getArray(0);
        assertThat(outerArray).isNotNull();
        assertThat(outerArray.size()).isEqualTo(2);

        InternalArray nestedArray1 = outerArray.getArray(0);
        assertThat(nestedArray1).isNotNull();
        assertThat(nestedArray1.size()).isEqualTo(3);
        assertThat(nestedArray1.getInt(0)).isEqualTo(1);
        assertThat(nestedArray1.getInt(1)).isEqualTo(2);
        assertThat(nestedArray1.getInt(2)).isEqualTo(3);

        InternalArray nestedArray2 = outerArray.getArray(1);
        assertThat(nestedArray2).isNotNull();
        assertThat(nestedArray2.size()).isEqualTo(3);
        assertThat(nestedArray2.getInt(0)).isEqualTo(4);
        assertThat(nestedArray2.getInt(1)).isEqualTo(5);
        assertThat(nestedArray2.getInt(2)).isEqualTo(6);
    }
}

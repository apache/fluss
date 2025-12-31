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

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;
import org.apache.fluss.utils.DateTimeUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.INDEXED;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.fluss.row.TestInternalRowGenerator.createAllTypes;
import static org.apache.fluss.row.indexed.IndexedRowTest.assertAllTypeEquals;
import static org.apache.fluss.row.indexed.IndexedRowTest.genRecordForAllTypes;
import static org.apache.fluss.testutils.InternalArrayAssert.assertThatArray;
import static org.apache.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Test of {@link IndexedRowReader}. */
public class IndexedRowReaderTest {

    private DataType[] dataTypes;
    private IndexedRowWriter writer;
    private IndexedRowReader reader;

    @BeforeEach
    public void before() {
        dataTypes = createAllTypes();
        writer = genRecordForAllTypes(dataTypes);
        reader = new IndexedRowReader(dataTypes);
        reader.pointTo(writer.segment(), 0);
    }

    @Test
    void testWriteAndReadAllTypes() {
        assertAllTypeEqualsForReader(reader);
    }

    @Test
    void testCreateFieldReader() {
        BinaryWriter.ValueWriter[] writers = new BinaryWriter.ValueWriter[dataTypes.length];
        IndexedRowReader.FieldReader[] readers = new IndexedRowReader.FieldReader[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            readers[i] = IndexedRowReader.createFieldReader(dataTypes[i]);
            writers[i] = BinaryWriter.createValueWriter(dataTypes[i], INDEXED);
        }

        IndexedRowWriter writer1 = new IndexedRowWriter(dataTypes);
        for (int i = 0; i < dataTypes.length; i++) {
            writers[i].writeValue(writer1, i, readers[i].readField(reader, i));
        }

        IndexedRow row1 = new IndexedRow(dataTypes);
        row1.pointTo(writer1.segment(), 0, writer.position());
        assertAllTypeEquals(row1);
    }

    private void assertAllTypeEqualsForReader(IndexedRowReader reader) {
        assertThat(reader.readBoolean()).isEqualTo(true);
        assertThat(reader.readByte()).isEqualTo((byte) 2);
        assertThat(reader.readShort()).isEqualTo(Short.parseShort("10"));
        assertThat(reader.readInt()).isEqualTo(100);
        assertThat(reader.readLong()).isEqualTo(new BigInteger("12345678901234567890").longValue());
        assertThat(reader.readFloat()).isEqualTo(Float.parseFloat("13.2"));
        assertThat(reader.readDouble()).isEqualTo(Double.parseDouble("15.21"));
        assertThat(DateTimeUtils.toLocalDate(reader.readInt()))
                .isEqualTo(LocalDate.of(2023, 10, 25));
        assertThat(DateTimeUtils.toLocalTime(reader.readInt()))
                .isEqualTo(LocalTime.of(9, 30, 0, 0));
        assertThat(reader.readBinary(20)).isEqualTo("1234567890".getBytes());
        assertThat(reader.readBytes()).isEqualTo("20".getBytes());
        assertThat(reader.readChar(2)).isEqualTo(BinaryString.fromString("1"));
        assertThat(reader.readString()).isEqualTo(BinaryString.fromString("hello"));
        assertThat(reader.readDecimal(5, 2)).isEqualTo(Decimal.fromUnscaledLong(9, 5, 2));
        assertThat(reader.readDecimal(20, 0))
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal(10), 20, 0));
        assertThat(reader.readTimestampNtz(1).toString()).isEqualTo("2023-10-25T12:01:13.182");
        assertThat(reader.readTimestampNtz(5).toString()).isEqualTo("2023-10-25T12:01:13.182");
        assertThat(reader.readTimestampLtz(1).toString()).isEqualTo("2023-10-25T12:01:13.182Z");
        assertThat(reader.readTimestampLtz(5).toString()).isEqualTo("2023-10-25T12:01:13.182Z");
        assertThatArray(reader.readArray(dataTypes[19]))
                .withElementType(DataTypes.INT())
                .isEqualTo(GenericArray.of(1, 2, 3, 4, 5, -11, null, 444, 102234));
        assertThatArray(reader.readArray(dataTypes[20]))
                .withElementType(DataTypes.FLOAT())
                .isEqualTo(
                        GenericArray.of(0.1f, 1.1f, -0.5f, 6.6f, Float.MAX_VALUE, Float.MIN_VALUE));
        assertThatArray(reader.readArray(dataTypes[21]))
                .withElementType(DataTypes.ARRAY(DataTypes.STRING()))
                .isEqualTo(
                        GenericArray.of(
                                GenericArray.of(fromString("a"), null, fromString("c")),
                                null,
                                GenericArray.of(fromString("hello"), fromString("world"))));
        MapType mapType = (MapType) dataTypes[22];
        InternalMap map = reader.readMap(mapType.getKeyType(), mapType.getValueType());
        assertThat(map.size()).isEqualTo(3);

        // Assert map keys and values
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        assertThat(keys.getInt(0)).isEqualTo(0);
        assertThat(keys.getInt(1)).isEqualTo(1);
        assertThat(keys.getInt(2)).isEqualTo(2);
        assertThat(values.isNullAt(0)).isTrue();
        assertThat(values.getString(1)).isEqualTo(fromString("1"));
        assertThat(values.getString(2)).isEqualTo(fromString("2"));
        InternalRow nestedRow =
                reader.readRow(dataTypes[23].getChildren().toArray(new DataType[0]));
        GenericRow expectedInnerRow = GenericRow.of(22);
        GenericRow expectedNestedRow = GenericRow.of(123, expectedInnerRow, fromString("Test"));
        assertThatRow(nestedRow)
                .withSchema(
                        DataTypes.ROW(
                                DataTypes.FIELD("u1", DataTypes.INT()),
                                DataTypes.FIELD(
                                        "u2",
                                        DataTypes.ROW(DataTypes.FIELD("v1", DataTypes.INT()))),
                                DataTypes.FIELD("u3", DataTypes.STRING())))
                .isEqualTo(expectedNestedRow);
    }
}

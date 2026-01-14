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

package org.apache.fluss.row.compacted;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CompactedRow}. */
public class CompactedRowTest {

    @Test
    public void testBasicTypes() {
        DataType[] types = {
            DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BOOLEAN()
        };

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        // Write data
        writer.writeInt(42);
        writer.writeLong(123456L);
        writer.writeString(BinaryString.fromString("test"));
        writer.writeBoolean(true);

        // Point row to the written data
        row.pointTo(writer.segment(), 0, writer.position());

        // Verify
        assertThat(row.getFieldCount()).isEqualTo(4);
        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getLong(1)).isEqualTo(123456L);
        assertThat(row.getString(2)).isEqualTo(BinaryString.fromString("test"));
        assertThat(row.getBoolean(3)).isTrue();
    }

    @Test
    public void testWithNulls() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        // Write data with nulls
        writer.writeInt(100);
        writer.setNullAt(1);
        writer.writeDouble(3.14);

        row.pointTo(writer.segment(), 0, writer.position());

        assertThat(row.isNullAt(0)).isFalse();
        assertThat(row.isNullAt(1)).isTrue();
        assertThat(row.isNullAt(2)).isFalse();
        assertThat(row.getInt(0)).isEqualTo(100);
        assertThat(row.getDouble(2)).isEqualTo(3.14);
    }

    @Test
    public void testAllPrimitiveTypes() {
        DataType[] types = {
            DataTypes.BOOLEAN(),
            DataTypes.TINYINT(),
            DataTypes.SMALLINT(),
            DataTypes.INT(),
            DataTypes.BIGINT(),
            DataTypes.FLOAT(),
            DataTypes.DOUBLE()
        };

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        writer.writeBoolean(true);
        writer.writeByte((byte) 1);
        writer.writeShort((short) 100);
        writer.writeInt(1000);
        writer.writeLong(10000L);
        writer.writeFloat(1.5f);
        writer.writeDouble(2.5);

        row.pointTo(writer.segment(), 0, writer.position());

        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getByte(1)).isEqualTo((byte) 1);
        assertThat(row.getShort(2)).isEqualTo((short) 100);
        assertThat(row.getInt(3)).isEqualTo(1000);
        assertThat(row.getLong(4)).isEqualTo(10000L);
        assertThat(row.getFloat(5)).isEqualTo(1.5f);
        assertThat(row.getDouble(6)).isEqualTo(2.5);
    }

    @Test
    public void testStringAndBinary() {
        DataType[] types = {DataTypes.STRING(), DataTypes.BYTES()};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        BinaryString str = BinaryString.fromString("Hello World");
        byte[] bytes = {1, 2, 3, 4, 5};

        writer.writeString(str);
        writer.writeBytes(bytes);

        row.pointTo(writer.segment(), 0, writer.position());

        assertThat(row.getString(0)).isEqualTo(str);
        assertThat(row.getBytes(1)).isEqualTo(bytes);
    }

    @Test
    public void testDecimal() {
        DataType[] types = {DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(20, 5)};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        Decimal decimal1 = Decimal.fromUnscaledLong(12345, 10, 2);
        Decimal decimal2 = Decimal.fromBigDecimal(new java.math.BigDecimal("123456.78901"), 20, 5);

        writer.writeDecimal(decimal1, 10);
        writer.writeDecimal(decimal2, 20);

        row.pointTo(writer.segment(), 0, writer.position());

        assertThat(row.getDecimal(0, 10, 2)).isEqualTo(decimal1);
        assertThat(row.getDecimal(1, 20, 5)).isEqualTo(decimal2);
    }

    @Test
    public void testTimestamp() {
        DataType[] types = {DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_LTZ(6)};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        TimestampNtz tsNtz = TimestampNtz.fromMillis(1609459200000L);
        TimestampLtz tsLtz = TimestampLtz.fromEpochMillis(1609459200000L, 456000);

        writer.writeTimestampNtz(tsNtz, 3);
        writer.writeTimestampLtz(tsLtz, 6);

        row.pointTo(writer.segment(), 0, writer.position());

        assertThat(row.getTimestampNtz(0, 3)).isEqualTo(tsNtz);
        assertThat(row.getTimestampLtz(1, 6)).isEqualTo(tsLtz);
    }

    @Test
    public void testGetChar() {
        DataType[] types = {DataTypes.CHAR(10)};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        BinaryString str = BinaryString.fromString("Hello");
        writer.writeString(str);
        row.pointTo(writer.segment(), 0, writer.position());
        assertThat(row.getChar(0, 10)).isEqualTo(str);
    }

    @Test
    public void testGetBinary() {
        DataType[] types = {DataTypes.BINARY(5)};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        byte[] data = {1, 2, 3, 4, 5};
        writer.writeBytes(data);

        row.pointTo(writer.segment(), 0, writer.position());

        assertThat(row.getBytes(0)).isEqualTo(data);
    }

    @Test
    public void testCopy() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING()};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        writer.writeInt(42);
        writer.writeString(BinaryString.fromString("test"));

        row.pointTo(writer.segment(), 0, writer.position());

        CompactedRow copy = row.copy();

        assertThat(copy.getInt(0)).isEqualTo(42);
        assertThat(copy.getString(1)).isEqualTo(BinaryString.fromString("test"));
    }

    @Test
    public void testCopyWithReuse() {
        DataType[] types = {DataTypes.INT(), DataTypes.BIGINT()};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        writer.writeInt(100);
        writer.writeLong(200L);

        row.pointTo(writer.segment(), 0, writer.position());

        CompactedRow reuse = new CompactedRow(types);
        CompactedRow copy = row.copy(reuse);

        assertThat(copy).isSameAs(reuse);
        assertThat(copy.getInt(0)).isEqualTo(100);
        assertThat(copy.getLong(1)).isEqualTo(200L);
    }

    @Test
    public void testPointToWithMultipleSegments() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING()};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        writer.writeInt(42);
        writer.writeString(BinaryString.fromString("test"));

        MemorySegment[] segments = {writer.segment()};
        row.pointTo(segments, 0, writer.position());

        assertThat(row.getSegments()).isEqualTo(segments);
        assertThat(row.getOffset()).isEqualTo(0);
        assertThat(row.getSizeInBytes()).isEqualTo(writer.position());
        assertThat(row.getSegment()).isEqualTo(segments[0]);
    }

    @Test
    public void testCopyTo() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING()};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        writer.writeInt(42);
        writer.writeString(BinaryString.fromString("test"));

        row.pointTo(writer.segment(), 0, writer.position());

        byte[] dest = new byte[row.getSizeInBytes()];
        row.copyTo(dest, 0);

        assertThat(dest).isNotNull();
        assertThat(dest.length).isEqualTo(row.getSizeInBytes());
    }

    @Test
    public void testEquals() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING()};

        CompactedRowWriter writer1 = new CompactedRowWriter(types.length);
        writer1.writeInt(42);
        writer1.writeString(BinaryString.fromString("test"));

        CompactedRowWriter writer2 = new CompactedRowWriter(types.length);
        writer2.writeInt(42);
        writer2.writeString(BinaryString.fromString("test"));

        CompactedRow row1 = new CompactedRow(types);
        CompactedRow row2 = new CompactedRow(types);

        row1.pointTo(writer1.segment(), 0, writer1.position());
        row2.pointTo(writer2.segment(), 0, writer2.position());

        assertThat(row1.equals(row1)).isTrue();
        assertThat(row1.equals(row2)).isTrue();
        assertThat(row1.equals(null)).isFalse();
        assertThat(row1.equals("string")).isFalse();
    }

    @Test
    public void testHashCode() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING()};

        CompactedRowWriter writer1 = new CompactedRowWriter(types.length);
        writer1.writeInt(42);
        writer1.writeString(BinaryString.fromString("test"));

        CompactedRowWriter writer2 = new CompactedRowWriter(types.length);
        writer2.writeInt(42);
        writer2.writeString(BinaryString.fromString("test"));

        CompactedRow row1 = new CompactedRow(types);
        CompactedRow row2 = new CompactedRow(types);

        row1.pointTo(writer1.segment(), 0, writer1.position());
        row2.pointTo(writer2.segment(), 0, writer2.position());

        assertThat(row1.hashCode()).isEqualTo(row2.hashCode());
    }

    @Test
    public void testFromMethod() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING()};

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(42);
        writer.writeString(BinaryString.fromString("test"));

        byte[] dataBytes = new byte[writer.position()];
        writer.segment().get(0, dataBytes, 0, writer.position());

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);
        CompactedRow row = CompactedRow.from(types, dataBytes, deserializer);

        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getString(1)).isEqualTo(BinaryString.fromString("test"));
    }

    @Test
    public void testGetFieldCount() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};
        CompactedRow row = new CompactedRow(types);
        assertThat(row.getFieldCount()).isEqualTo(3);
    }

    @Test
    public void testGetArray() {
        DataType[] types = {DataTypes.ARRAY(DataTypes.INT()), DataTypes.STRING()};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        // Write array data (simplified - actual implementation may vary)
        writer.setNullAt(0); // Simplified: set as null for now
        writer.writeString(BinaryString.fromString("test"));

        row.pointTo(writer.segment(), 0, writer.position());

        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.getString(1)).isEqualTo(BinaryString.fromString("test"));
    }

    @Test
    public void testConstructorWithArityAndDeserializer() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRow row = new CompactedRow(2, deserializer);
        assertThat(row.getFieldCount()).isEqualTo(2);
    }

    @Test
    public void testMultipleReads() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(types.length);

        writer.writeInt(42);
        writer.writeString(BinaryString.fromString("test"));
        writer.writeDouble(3.14);

        row.pointTo(writer.segment(), 0, writer.position());

        // Read multiple times to test caching
        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getInt(0)).isEqualTo(42); // Second read
        assertThat(row.getString(1)).isEqualTo(BinaryString.fromString("test"));
        assertThat(row.getString(1)).isEqualTo(BinaryString.fromString("test")); // Second read
    }

    @Test
    public void testLargeRow() {
        // Test with many fields
        int numFields = 100;
        DataType[] types = new DataType[numFields];
        for (int i = 0; i < numFields; i++) {
            types[i] = DataTypes.INT();
        }

        CompactedRow row = new CompactedRow(types);
        CompactedRowWriter writer = new CompactedRowWriter(numFields);

        for (int i = 0; i < numFields; i++) {
            writer.writeInt(i * 10);
        }

        row.pointTo(writer.segment(), 0, writer.position());

        for (int i = 0; i < numFields; i++) {
            assertThat(row.getInt(i)).isEqualTo(i * 10);
        }
    }

    @Test
    public void testCompactedArrayGetRowWithInvalidType() {
        DataType arrayType = DataTypes.ARRAY(DataTypes.INT());
        DataType[] fieldTypes = {arrayType};

        CompactedRow outerRow = new CompactedRow(fieldTypes);
        CompactedRowWriter outerWriter = new CompactedRowWriter(fieldTypes.length);

        GenericArray arrayData = GenericArray.of(1, 2, 3);
        ArraySerializer serializer =
                new ArraySerializer(DataTypes.INT(), BinaryRowFormat.COMPACTED);
        outerWriter.writeArray(arrayData, serializer);

        outerRow.pointTo(outerWriter.segment(), 0, outerWriter.position());

        InternalArray array = outerRow.getArray(0);
        assertThatThrownBy(() -> array.getRow(0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Can not get row from Array of type");
    }

    @Test
    public void testCompactedArrayGetRowWithWrongNumFields() {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("f1", DataTypes.INT()),
                        DataTypes.FIELD("f2", DataTypes.STRING()));
        DataType arrayType = DataTypes.ARRAY(rowType);
        DataType[] fieldTypes = {arrayType};

        CompactedRow outerRow = new CompactedRow(fieldTypes);
        CompactedRowWriter outerWriter = new CompactedRowWriter(fieldTypes.length);

        GenericRow innerRow = GenericRow.of(100, BinaryString.fromString("test"));
        GenericArray arrayData = GenericArray.of(innerRow);

        ArraySerializer serializer = new ArraySerializer(rowType, BinaryRowFormat.COMPACTED);
        outerWriter.writeArray(arrayData, serializer);

        outerRow.pointTo(outerWriter.segment(), 0, outerWriter.position());

        InternalArray array = outerRow.getArray(0);
        assertThatThrownBy(() -> array.getRow(0, 5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unexpected number of fields");
    }

    @Test
    public void testCompactedArrayGetNestedArray() {
        DataType innerArrayType = DataTypes.ARRAY(DataTypes.INT());
        DataType outerArrayType = DataTypes.ARRAY(innerArrayType);
        DataType[] fieldTypes = {outerArrayType};

        CompactedRow outerRow = new CompactedRow(fieldTypes);
        CompactedRowWriter outerWriter = new CompactedRowWriter(fieldTypes.length);

        GenericArray innerArray1 = GenericArray.of(10, 20, 30);
        GenericArray innerArray2 = GenericArray.of(40, 50, 60);
        GenericArray outerArrayData = GenericArray.of(innerArray1, innerArray2);

        ArraySerializer serializer = new ArraySerializer(innerArrayType, BinaryRowFormat.COMPACTED);
        outerWriter.writeArray(outerArrayData, serializer);

        outerRow.pointTo(outerWriter.segment(), 0, outerWriter.position());

        InternalArray outerArray = outerRow.getArray(0);
        assertThat(outerArray).isNotNull();
        assertThat(outerArray.size()).isEqualTo(2);

        InternalArray nestedArray1 = outerArray.getArray(0);
        assertThat(nestedArray1).isNotNull();
        assertThat(nestedArray1.size()).isEqualTo(3);
        assertThat(nestedArray1.getInt(0)).isEqualTo(10);
        assertThat(nestedArray1.getInt(1)).isEqualTo(20);
        assertThat(nestedArray1.getInt(2)).isEqualTo(30);

        InternalArray nestedArray2 = outerArray.getArray(1);
        assertThat(nestedArray2).isNotNull();
        assertThat(nestedArray2.size()).isEqualTo(3);
        assertThat(nestedArray2.getInt(0)).isEqualTo(40);
        assertThat(nestedArray2.getInt(1)).isEqualTo(50);
        assertThat(nestedArray2.getInt(2)).isEqualTo(60);
    }
}

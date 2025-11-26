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

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryArrayWriter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.array.PrimitiveBinaryArray;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.apache.fluss.row.BinaryRow.BinaryRowFormat.COMPACTED;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactedRowDeserializer}. */
public class CompactedRowDeserializerTest {

    @Test
    public void testDeserializeBasicTypes() {
        DataType[] types = {
            DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.BOOLEAN()
        };

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        // Write data
        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(42);
        writer.writeLong(123456L);
        writer.writeString(BinaryString.fromString("test"));
        writer.writeBoolean(true);

        // Deserialize
        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(42);
        assertThat(output.getField(1)).isEqualTo(123456L);
        assertThat(output.getField(2)).isEqualTo(BinaryString.fromString("test"));
        assertThat(output.getField(3)).isEqualTo(true);
    }

    @Test
    public void testDeserializeWithNulls() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        // Write data with nulls
        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(100);
        writer.setNullAt(1);
        writer.writeDouble(3.14);

        // Deserialize
        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(100);
        assertThat(output.getField(1)).isNull();
        assertThat(output.getField(2)).isEqualTo(3.14);
    }

    @Test
    public void testDeserializeAllPrimitiveTypes() {
        DataType[] types = {
            DataTypes.BOOLEAN(),
            DataTypes.TINYINT(),
            DataTypes.SMALLINT(),
            DataTypes.INT(),
            DataTypes.BIGINT(),
            DataTypes.FLOAT(),
            DataTypes.DOUBLE()
        };

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeBoolean(true);
        writer.writeByte((byte) 1);
        writer.writeShort((short) 100);
        writer.writeInt(1000);
        writer.writeLong(10000L);
        writer.writeFloat(1.5f);
        writer.writeDouble(2.5);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(true);
        assertThat(output.getField(1)).isEqualTo((byte) 1);
        assertThat(output.getField(2)).isEqualTo((short) 100);
        assertThat(output.getField(3)).isEqualTo(1000);
        assertThat(output.getField(4)).isEqualTo(10000L);
        assertThat(output.getField(5)).isEqualTo(1.5f);
        assertThat(output.getField(6)).isEqualTo(2.5);
    }

    @Test
    public void testDeserializeStringAndBinary() {
        DataType[] types = {DataTypes.STRING(), DataTypes.CHAR(10), DataTypes.BYTES()};

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        BinaryString str1 = BinaryString.fromString("Hello World");
        BinaryString str2 = BinaryString.fromString("Test");
        byte[] bytes = {1, 2, 3, 4, 5};

        writer.writeString(str1);
        writer.writeString(str2);
        writer.writeBytes(bytes);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(str1);
        assertThat(output.getField(1)).isEqualTo(str2);
        assertThat(output.getField(2)).isEqualTo(bytes);
    }

    @Test
    public void testDeserializeDecimal() {
        DataType[] types = {DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(20, 5)};

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        Decimal decimal1 = Decimal.fromUnscaledLong(12345, 10, 2);
        Decimal decimal2 = Decimal.fromBigDecimal(new BigDecimal("123456.78901"), 20, 5);

        writer.writeDecimal(decimal1, 10);
        writer.writeDecimal(decimal2, 20);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(decimal1);
        assertThat(output.getField(1)).isEqualTo(decimal2);
    }

    @Test
    public void testDeserializeTimestamp() {
        DataType[] types = {
            DataTypes.TIMESTAMP(3),
            DataTypes.TIMESTAMP_LTZ(6),
            DataTypes.TIMESTAMP(9), // high precision
            DataTypes.TIMESTAMP_LTZ(9) // high precision
        };

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        TimestampNtz tsNtz1 = TimestampNtz.fromMillis(1609459200000L);
        TimestampLtz tsLtz1 = TimestampLtz.fromEpochMillis(1609459200000L);
        TimestampNtz tsNtz2 = TimestampNtz.fromMillis(1609459200000L, 123456);
        TimestampLtz tsLtz2 = TimestampLtz.fromEpochMillis(1609459200000L, 654321);

        writer.writeTimestampNtz(tsNtz1, 3);
        writer.writeTimestampLtz(tsLtz1, 6);
        writer.writeTimestampNtz(tsNtz2, 9);
        writer.writeTimestampLtz(tsLtz2, 9);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(tsNtz1);
        assertThat(output.getField(1)).isEqualTo(tsLtz1);
        assertThat(output.getField(2)).isEqualTo(tsNtz2);
        assertThat(output.getField(3)).isEqualTo(tsLtz2);
    }

    @Test
    public void testDeserializeDateAndTime() {
        DataType[] types = {DataTypes.DATE(), DataTypes.TIME()};

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(18628); // Date: 2021-01-01
        writer.writeInt(43200000); // Time: 12:00:00

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(18628);
        assertThat(output.getField(1)).isEqualTo(43200000);
    }

    @Test
    public void testDeserializeMultipleTimes() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING()};

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(42);
        writer.writeString(BinaryString.fromString("test"));

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        // Deserialize first time
        GenericRow output1 = new GenericRow(types.length);
        deserializer.deserialize(reader, output1);

        assertThat(output1.getField(0)).isEqualTo(42);
        assertThat(output1.getField(1)).isEqualTo(BinaryString.fromString("test"));

        // Deserialize second time (reader should be reusable)
        reader.pointTo(writer.segment(), 0, writer.position());
        GenericRow output2 = new GenericRow(types.length);
        deserializer.deserialize(reader, output2);

        assertThat(output2.getField(0)).isEqualTo(42);
        assertThat(output2.getField(1)).isEqualTo(BinaryString.fromString("test"));
    }

    @Test
    public void testDeserializeManyFields() {
        int numFields = 50;
        DataType[] types = new DataType[numFields];
        for (int i = 0; i < numFields; i++) {
            types[i] = DataTypes.INT();
        }

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(numFields);
        for (int i = 0; i < numFields; i++) {
            writer.writeInt(i * 10);
        }

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(numFields);
        deserializer.deserialize(reader, output);

        for (int i = 0; i < numFields; i++) {
            assertThat(output.getField(i)).isEqualTo(i * 10);
        }
    }

    @Test
    public void testDeserializeNullableTypes() {
        DataType[] types = {
            DataTypes.INT().copy(true), DataTypes.STRING().copy(true), DataTypes.DOUBLE().copy(true)
        };

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.setNullAt(0);
        writer.writeString(BinaryString.fromString("test"));
        writer.setNullAt(2);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isNull();
        assertThat(output.getField(1)).isEqualTo(BinaryString.fromString("test"));
        assertThat(output.getField(2)).isNull();
    }

    @Test
    public void testDeserializeMixedTypes() {
        DataType[] types = {
            DataTypes.BOOLEAN(),
            DataTypes.TINYINT(),
            DataTypes.STRING(),
            DataTypes.DECIMAL(10, 2),
            DataTypes.TIMESTAMP(3),
            DataTypes.BYTES()
        };

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeBoolean(true);
        writer.writeByte((byte) 5);
        writer.writeString(BinaryString.fromString("mixed"));
        writer.writeDecimal(Decimal.fromUnscaledLong(12345, 10, 2), 10);
        writer.writeTimestampNtz(TimestampNtz.fromMillis(1000L), 3);
        writer.writeBytes(new byte[] {1, 2, 3});

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(true);
        assertThat(output.getField(1)).isEqualTo((byte) 5);
        assertThat(output.getField(2)).isEqualTo(BinaryString.fromString("mixed"));
        assertThat(output.getField(3)).isEqualTo(Decimal.fromUnscaledLong(12345, 10, 2));
        assertThat(output.getField(4)).isEqualTo(TimestampNtz.fromMillis(1000L));
        assertThat(output.getField(5)).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    public void testDeserializeEmptyRow() {
        DataType[] types = {};

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(0);
        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(0);
        deserializer.deserialize(reader, output);

        assertThat(output.getFieldCount()).isEqualTo(0);
    }

    @Test
    public void testDeserializeWithRowType() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()},
                        new String[] {"f1", "f2", "f3"});

        DataType[] types = rowType.getChildren().toArray(new DataType[0]);
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(123);
        writer.writeString(BinaryString.fromString("rowtype"));
        writer.writeDouble(4.56);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(123);
        assertThat(output.getField(1)).isEqualTo(BinaryString.fromString("rowtype"));
        assertThat(output.getField(2)).isEqualTo(4.56);
    }

    @Test
    public void testDeserializeAllNulls() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.setNullAt(0);
        writer.setNullAt(1);
        writer.setNullAt(2);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isNull();
        assertThat(output.getField(1)).isNull();
        assertThat(output.getField(2)).isNull();
    }

    @Test
    public void testDeserializeComplexMixedTypesWithNulls() {
        DataType[] types = {
            DataTypes.STRING(),
            DataTypes.DECIMAL(15, 3),
            DataTypes.TIMESTAMP(6),
            DataTypes.BYTES(),
            DataTypes.BOOLEAN()
        };

        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.setNullAt(0);
        writer.writeDecimal(Decimal.fromUnscaledLong(123456, 15, 3), 15);
        writer.setNullAt(2);
        writer.writeBytes(new byte[] {1, 2, 3, 4, 5});
        writer.writeBoolean(true);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isNull();
        assertThat(output.getField(1)).isEqualTo(Decimal.fromUnscaledLong(123456, 15, 3));
        assertThat(output.getField(2)).isNull();
        assertThat(output.getField(3)).isEqualTo(new byte[] {1, 2, 3, 4, 5});
        assertThat(output.getField(4)).isEqualTo(true);
    }

    @Test
    public void testDeserializeVeryLongDecimal() {
        DataType[] types = {DataTypes.DECIMAL(38, 10)};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        Decimal largeDecimal =
                Decimal.fromBigDecimal(new BigDecimal("12345678901234567890.1234567890"), 38, 10);
        writer.writeDecimal(largeDecimal, 38);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(largeDecimal);
    }

    @Test
    public void testDeserializeHighPrecisionTimestamps() {
        DataType[] types = {DataTypes.TIMESTAMP(9), DataTypes.TIMESTAMP_LTZ(9)};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        TimestampNtz tsNtz = TimestampNtz.fromMillis(1609459200000L, 123456);
        TimestampLtz tsLtz = TimestampLtz.fromEpochMillis(1609459200000L, 654321);

        writer.writeTimestampNtz(tsNtz, 9);
        writer.writeTimestampLtz(tsLtz, 9);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(tsNtz);
        assertThat(output.getField(1)).isEqualTo(tsLtz);
    }

    @Test
    public void testDeserializeBooleanType() {
        DataType[] types = {DataTypes.BOOLEAN(), DataTypes.BOOLEAN()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeBoolean(true);
        writer.writeBoolean(false);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(true);
        assertThat(output.getField(1)).isEqualTo(false);
    }

    @Test
    public void testDeserializeByteType() {
        DataType[] types = {DataTypes.TINYINT(), DataTypes.TINYINT()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeByte((byte) 127);
        writer.writeByte((byte) -128);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo((byte) 127);
        assertThat(output.getField(1)).isEqualTo((byte) -128);
    }

    @Test
    public void testDeserializeShortType() {
        DataType[] types = {DataTypes.SMALLINT(), DataTypes.SMALLINT()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeShort((short) 32767);
        writer.writeShort((short) -32768);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo((short) 32767);
        assertThat(output.getField(1)).isEqualTo((short) -32768);
    }

    @Test
    public void testDeserializeBinaryType() {
        DataType[] types = {DataTypes.BYTES(), DataTypes.BYTES()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        byte[] binary1 = {1, 2, 3, 4, 5};
        byte[] binary2 = {10, 20, 30, 40, 50, 60};

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeBytes(binary1);
        writer.writeBytes(binary2);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(binary1);
        assertThat(output.getField(1)).isEqualTo(binary2);
    }

    @Test
    public void testDeserializeFloatType() {
        DataType[] types = {DataTypes.FLOAT(), DataTypes.FLOAT()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeFloat(3.14f);
        writer.writeFloat(-2.718f);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(3.14f);
        assertThat(output.getField(1)).isEqualTo(-2.718f);
    }

    @Test
    public void testDeserializeBigIntType() {
        DataType[] types = {DataTypes.BIGINT(), DataTypes.BIGINT()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeLong(9223372036854775807L);
        writer.writeLong(-9223372036854775808L);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(9223372036854775807L);
        assertThat(output.getField(1)).isEqualTo(-9223372036854775808L);
    }

    @Test
    public void testDeserializeNullableFields() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(100);
        writer.setNullAt(1);
        writer.writeDouble(3.14);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(100);
        assertThat(output.getField(1)).isNull();
        assertThat(output.getField(2)).isEqualTo(3.14);
    }

    @Test
    public void testDeserializeCharType() {
        DataType[] types = {DataTypes.CHAR(10), DataTypes.CHAR(5)};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeString(BinaryString.fromString("hello"));
        writer.writeString(BinaryString.fromString("world"));

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(output.getField(1)).isEqualTo(BinaryString.fromString("world"));
    }

    @Test
    public void testDeserializeCompactDecimal() {
        DataType[] types = {DataTypes.DECIMAL(10, 2), DataTypes.DECIMAL(5, 1)};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        Decimal d1 = Decimal.fromUnscaledLong(12345, 10, 2);
        Decimal d2 = Decimal.fromUnscaledLong(678, 5, 1);
        writer.writeDecimal(d1, 10);
        writer.writeDecimal(d2, 5);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(d1);
        assertThat(output.getField(1)).isEqualTo(d2);
    }

    @Test
    public void testDeserializeCompactTimestamps() {
        DataType[] types = {DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_LTZ(3)};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        TimestampNtz tsNtz = TimestampNtz.fromMillis(1609459200000L);
        TimestampLtz tsLtz = TimestampLtz.fromEpochMillis(1609459200000L);
        writer.writeTimestampNtz(tsNtz, 3);
        writer.writeTimestampLtz(tsLtz, 3);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(tsNtz);
        assertThat(output.getField(1)).isEqualTo(tsLtz);
    }

    @Test
    public void testDeserializeWithAllNullableTypes() {
        DataType[] types = {
            DataTypes.BOOLEAN(),
            DataTypes.TINYINT(),
            DataTypes.SMALLINT(),
            DataTypes.INT(),
            DataTypes.BIGINT(),
            DataTypes.FLOAT(),
            DataTypes.DOUBLE()
        };
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.setNullAt(0);
        writer.setNullAt(1);
        writer.setNullAt(2);
        writer.setNullAt(3);
        writer.setNullAt(4);
        writer.setNullAt(5);
        writer.setNullAt(6);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        for (int i = 0; i < types.length; i++) {
            assertThat(output.getField(i)).isNull();
        }
    }

    @Test
    public void testDeserializeIntAndString() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(12345);
        writer.writeString(BinaryString.fromString("test_string"));

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(12345);
        assertThat(output.getField(1)).isEqualTo(BinaryString.fromString("test_string"));
    }

    @Test
    public void testDeserializeDoubleType() {
        DataType[] types = {DataTypes.DOUBLE(), DataTypes.DOUBLE()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeDouble(Double.MAX_VALUE);
        writer.writeDouble(Double.MIN_VALUE);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(Double.MAX_VALUE);
        assertThat(output.getField(1)).isEqualTo(Double.MIN_VALUE);
    }

    @Test
    public void testDeserializeDateType() {
        DataType[] types = {DataTypes.DATE()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(18628);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(18628);
    }

    @Test
    public void testDeserializeTimeType() {
        DataType[] types = {DataTypes.TIME()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(43200);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(43200);
    }

    @Test
    public void testDeserializeAllPrimitiveTypesWithValues() {
        DataType[] types = {
            DataTypes.BOOLEAN(),
            DataTypes.TINYINT(),
            DataTypes.SMALLINT(),
            DataTypes.INT(),
            DataTypes.BIGINT(),
            DataTypes.FLOAT(),
            DataTypes.DOUBLE()
        };
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeBoolean(true);
        writer.writeByte((byte) 100);
        writer.writeShort((short) 20000);
        writer.writeInt(1000000);
        writer.writeLong(9000000000L);
        writer.writeFloat(3.14159f);
        writer.writeDouble(2.71828);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(true);
        assertThat(output.getField(1)).isEqualTo((byte) 100);
        assertThat(output.getField(2)).isEqualTo((short) 20000);
        assertThat(output.getField(3)).isEqualTo(1000000);
        assertThat(output.getField(4)).isEqualTo(9000000000L);
        assertThat(output.getField(5)).isEqualTo(3.14159f);
        assertThat(output.getField(6)).isEqualTo(2.71828);
    }

    @Test
    public void testDeserializeSingleField() {
        DataType[] types = {DataTypes.INT()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(42);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(42);
    }

    @Test
    public void testDeserializeMixedNullsAndValues() {
        DataType[] types = {
            DataTypes.INT(),
            DataTypes.STRING(),
            DataTypes.INT(),
            DataTypes.STRING(),
            DataTypes.DOUBLE()
        };
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(100);
        writer.setNullAt(1);
        writer.writeInt(200);
        writer.writeString(BinaryString.fromString("test"));
        writer.setNullAt(4);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(100);
        assertThat(output.getField(1)).isNull();
        assertThat(output.getField(2)).isEqualTo(200);
        assertThat(output.getField(3)).isEqualTo(BinaryString.fromString("test"));
        assertThat(output.getField(4)).isNull();
    }

    @Test
    public void testDeserializeOnlyNullFields() {
        DataType[] types = {DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE()};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.setNullAt(0);
        writer.setNullAt(1);
        writer.setNullAt(2);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isNull();
        assertThat(output.getField(1)).isNull();
        assertThat(output.getField(2)).isNull();
    }

    @Test
    public void testDeserializeMultipleStringFields() {
        DataType[] types = {
            DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
        };
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeString(BinaryString.fromString("first"));
        writer.writeString(BinaryString.fromString("second"));
        writer.writeString(BinaryString.fromString("third"));
        writer.writeString(BinaryString.fromString("fourth"));

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(BinaryString.fromString("first"));
        assertThat(output.getField(1)).isEqualTo(BinaryString.fromString("second"));
        assertThat(output.getField(2)).isEqualTo(BinaryString.fromString("third"));
        assertThat(output.getField(3)).isEqualTo(BinaryString.fromString("fourth"));
    }

    @Test
    public void testDeserializeArrayType() {
        DataType[] types = {DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT())};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        BinaryArray intArray = BinaryArray.fromPrimitiveArray(new int[] {1, 2, 3, 4, 5});
        ArraySerializer arraySerializer = new ArraySerializer(DataTypes.INT(), COMPACTED);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(100);
        writer.writeArray(intArray, arraySerializer);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(100);
        InternalArray resultArray = (InternalArray) output.getField(1);
        assertThat(resultArray).isNotNull();
        assertThat(resultArray.size()).isEqualTo(5);
        assertThat(resultArray.getInt(0)).isEqualTo(1);
        assertThat(resultArray.getInt(4)).isEqualTo(5);
    }

    @Test
    public void testDeserializeArrayOfStrings() {
        DataType[] types = {DataTypes.ARRAY(DataTypes.STRING())};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        BinaryArray strArray = new PrimitiveBinaryArray();
        BinaryArrayWriter strArrayWriter = new BinaryArrayWriter(strArray, 3, 8);
        strArrayWriter.writeString(0, BinaryString.fromString("hello"));
        strArrayWriter.writeString(1, BinaryString.fromString("world"));
        strArrayWriter.writeString(2, BinaryString.fromString("test"));
        strArrayWriter.complete();

        ArraySerializer arraySerializer = new ArraySerializer(DataTypes.STRING(), COMPACTED);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeArray(strArray, arraySerializer);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        InternalArray resultArray = (InternalArray) output.getField(0);
        assertThat(resultArray).isNotNull();
        assertThat(resultArray.size()).isEqualTo(3);
        assertThat(resultArray.getString(0)).isEqualTo(BinaryString.fromString("hello"));
        assertThat(resultArray.getString(1)).isEqualTo(BinaryString.fromString("world"));
        assertThat(resultArray.getString(2)).isEqualTo(BinaryString.fromString("test"));
    }

    @Test
    public void testDeserializeNullArray() {
        DataType[] types = {DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT())};
        CompactedRowDeserializer deserializer = new CompactedRowDeserializer(types);

        CompactedRowWriter writer = new CompactedRowWriter(types.length);
        writer.writeInt(100);
        writer.setNullAt(1);

        CompactedRowReader reader = new CompactedRowReader(types.length);
        reader.pointTo(writer.segment(), 0, writer.position());

        GenericRow output = new GenericRow(types.length);
        deserializer.deserialize(reader, output);

        assertThat(output.getField(0)).isEqualTo(100);
        assertThat(output.getField(1)).isNull();
    }
}

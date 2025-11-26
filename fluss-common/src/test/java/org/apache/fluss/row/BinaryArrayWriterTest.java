/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.row.array.PrimitiveBinaryArray;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinaryArrayWriter}. */
public class BinaryArrayWriterTest {

    @Test
    public void testWriteAndReadAllTypes() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 10, 8);

        writer.writeBoolean(0, true);
        writer.writeByte(1, (byte) 10);
        writer.writeShort(2, (short) 100);
        writer.writeInt(3, 1000);
        writer.writeLong(4, 10000L);
        writer.writeFloat(5, 1.5f);
        writer.writeDouble(6, 2.5);
        writer.writeString(7, BinaryString.fromString("test"));
        writer.writeBytes(8, new byte[] {1, 2, 3});
        writer.writeDecimal(9, Decimal.fromUnscaledLong(123, 5, 2), 5);
        writer.complete();

        assertThat(array.getBoolean(0)).isTrue();
        assertThat(array.getByte(1)).isEqualTo((byte) 10);
        assertThat(array.getShort(2)).isEqualTo((short) 100);
        assertThat(array.getInt(3)).isEqualTo(1000);
        assertThat(array.getLong(4)).isEqualTo(10000L);
        assertThat(array.getFloat(5)).isEqualTo(1.5f);
        assertThat(array.getDouble(6)).isEqualTo(2.5);
        assertThat(array.getString(7)).isEqualTo(BinaryString.fromString("test"));
        assertThat(array.getBytes(8)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(array.getDecimal(9, 5, 2)).isEqualTo(Decimal.fromUnscaledLong(123, 5, 2));
    }

    @Test
    public void testReset() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeInt(0, 10);
        writer.writeInt(1, 20);
        writer.writeInt(2, 30);
        writer.complete();

        assertThat(array.getInt(0)).isEqualTo(10);

        writer.reset();
        writer.writeInt(0, 100);
        writer.writeInt(1, 200);
        writer.writeInt(2, 300);
        writer.complete();

        assertThat(array.getInt(0)).isEqualTo(100);
        assertThat(array.getInt(1)).isEqualTo(200);
        assertThat(array.getInt(2)).isEqualTo(300);
    }

    @Test
    public void testSetNullMethods() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 8, 8);

        writer.setNullBoolean(0);
        writer.setNullByte(1);
        writer.setNullShort(2);
        writer.setNullInt(3);
        writer.setNullLong(4);
        writer.setNullFloat(5);
        writer.setNullDouble(6);
        writer.setNullAt(7);
        writer.complete();

        for (int i = 0; i < 8; i++) {
            assertThat(array.isNullAt(i)).isTrue();
        }

        assertThat(array.getBoolean(0)).isFalse();
        assertThat(array.getByte(1)).isEqualTo((byte) 0);
        assertThat(array.getShort(2)).isEqualTo((short) 0);
        assertThat(array.getInt(3)).isEqualTo(0);
        assertThat(array.getLong(4)).isEqualTo(0L);
        assertThat(array.getFloat(5)).isEqualTo(0.0f);
        assertThat(array.getDouble(6)).isEqualTo(0.0);
    }

    @Test
    public void testSetOffsetAndSize() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        writer.setOffsetAndSize(0, 100, 200);
        writer.setOffsetAndSize(1, 300, 400);
        writer.complete();

        long offsetAndSize0 = array.getLong(0);
        assertThat(offsetAndSize0 >>> 32).isEqualTo(100);
        assertThat(offsetAndSize0 & 0xFFFFFFFFL).isEqualTo(200);

        long offsetAndSize1 = array.getLong(1);
        assertThat(offsetAndSize1 >>> 32).isEqualTo(300);
        assertThat(offsetAndSize1 & 0xFFFFFFFFL).isEqualTo(400);
    }

    @Test
    public void testGetFieldOffset() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 5, 8);

        int fieldOffset0 = writer.getFieldOffset(0);
        int fieldOffset1 = writer.getFieldOffset(1);
        int fieldOffset2 = writer.getFieldOffset(2);

        assertThat(fieldOffset1 - fieldOffset0).isEqualTo(8);
        assertThat(fieldOffset2 - fieldOffset1).isEqualTo(8);
    }

    @Test
    public void testGetNumElements() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 5, 4);

        assertThat(writer.getNumElements()).isEqualTo(5);
    }

    @Test
    public void testWriteChar() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        writer.writeChar(0, BinaryString.fromString("hello"), 5);
        writer.writeChar(1, BinaryString.fromString("world"), 5);
        writer.complete();

        assertThat(array.getString(0).toString()).isEqualTo("hello");
        assertThat(array.getString(1).toString()).isEqualTo("world");
    }

    @Test
    public void testWriteBinary() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        byte[] binary1 = {1, 2, 3, 4};
        byte[] binary2 = {5, 6, 7, 8};

        writer.writeBinary(0, binary1, 4);
        writer.writeBinary(1, binary2, 4);
        writer.complete();

        assertThat(array.getBytes(0)).isEqualTo(binary1);
        assertThat(array.getBytes(1)).isEqualTo(binary2);
    }

    @Test
    public void testWriteTimestampNtz() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampNtz ts1 = TimestampNtz.fromMillis(1000L, 123456);
        TimestampNtz ts2 = TimestampNtz.fromMillis(2000L, 654321);

        writer.writeTimestampNtz(0, ts1, 3);
        writer.writeTimestampNtz(1, ts2, 9);
        writer.complete();

        assertThat(array.getTimestampNtz(0, 3).getMillisecond()).isEqualTo(1000L);
        assertThat(array.getTimestampNtz(1, 9)).isEqualTo(ts2);
    }

    @Test
    public void testWriteTimestampLtz() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        TimestampLtz ts1 = TimestampLtz.fromEpochMillis(1000L, 123456);
        TimestampLtz ts2 = TimestampLtz.fromEpochMillis(2000L, 654321);

        writer.writeTimestampLtz(0, ts1, 3);
        writer.writeTimestampLtz(1, ts2, 9);
        writer.complete();

        assertThat(array.getTimestampLtz(0, 3).getEpochMillisecond()).isEqualTo(1000L);
        assertThat(array.getTimestampLtz(1, 9)).isEqualTo(ts2);
    }

    @Test
    public void testWriteNaNFloat() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 4);

        writer.writeFloat(0, Float.NaN);
        writer.writeFloat(1, Float.POSITIVE_INFINITY);
        writer.writeFloat(2, Float.NEGATIVE_INFINITY);
        writer.complete();

        assertThat(array.getFloat(0)).isNaN();
        assertThat(array.getFloat(1)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(array.getFloat(2)).isEqualTo(Float.NEGATIVE_INFINITY);
    }

    @Test
    public void testWriteNaNDouble() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 3, 8);

        writer.writeDouble(0, Double.NaN);
        writer.writeDouble(1, Double.POSITIVE_INFINITY);
        writer.writeDouble(2, Double.NEGATIVE_INFINITY);
        writer.complete();

        assertThat(array.getDouble(0)).isNaN();
        assertThat(array.getDouble(1)).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(array.getDouble(2)).isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testAfterGrow() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 8);

        writer.writeLong(0, 100L);
        writer.writeLong(1, 200L);

        writer.afterGrow();

        assertThat(array.getSizeInBytes()).isGreaterThan(0);
    }

    @Test
    public void testCreateNullSetter() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 10, 8);

        BinaryArrayWriter.NullSetter booleanSetter =
                BinaryArrayWriter.createNullSetter(DataTypes.BOOLEAN());
        booleanSetter.setNull(writer, 0);

        BinaryArrayWriter.NullSetter tinyintSetter =
                BinaryArrayWriter.createNullSetter(DataTypes.TINYINT());
        tinyintSetter.setNull(writer, 1);

        BinaryArrayWriter.NullSetter smallintSetter =
                BinaryArrayWriter.createNullSetter(DataTypes.SMALLINT());
        smallintSetter.setNull(writer, 2);

        BinaryArrayWriter.NullSetter intSetter =
                BinaryArrayWriter.createNullSetter(DataTypes.INT());
        intSetter.setNull(writer, 3);

        BinaryArrayWriter.NullSetter floatSetter =
                BinaryArrayWriter.createNullSetter(DataTypes.FLOAT());
        floatSetter.setNull(writer, 4);

        BinaryArrayWriter.NullSetter doubleSetter =
                BinaryArrayWriter.createNullSetter(DataTypes.DOUBLE());
        doubleSetter.setNull(writer, 5);

        BinaryArrayWriter.NullSetter bigintSetter =
                BinaryArrayWriter.createNullSetter(DataTypes.BIGINT());
        bigintSetter.setNull(writer, 6);

        BinaryArrayWriter.NullSetter charSetter =
                BinaryArrayWriter.createNullSetter(DataTypes.CHAR(10));
        charSetter.setNull(writer, 7);

        BinaryArrayWriter.NullSetter arraySetter =
                BinaryArrayWriter.createNullSetter(DataTypes.ARRAY(DataTypes.INT()));
        arraySetter.setNull(writer, 8);

        BinaryArrayWriter.NullSetter timestampSetter =
                BinaryArrayWriter.createNullSetter(DataTypes.TIMESTAMP(3));
        timestampSetter.setNull(writer, 9);

        writer.complete();

        for (int i = 0; i < 10; i++) {
            assertThat(array.isNullAt(i)).isTrue();
        }
    }

    @Test
    public void testRoundNumberOfBytesToNearestWord() {
        assertThat(BinaryArrayWriter.roundNumberOfBytesToNearestWord(0)).isEqualTo(0);
        assertThat(BinaryArrayWriter.roundNumberOfBytesToNearestWord(1)).isEqualTo(8);
        assertThat(BinaryArrayWriter.roundNumberOfBytesToNearestWord(7)).isEqualTo(8);
        assertThat(BinaryArrayWriter.roundNumberOfBytesToNearestWord(8)).isEqualTo(8);
        assertThat(BinaryArrayWriter.roundNumberOfBytesToNearestWord(9)).isEqualTo(16);
        assertThat(BinaryArrayWriter.roundNumberOfBytesToNearestWord(15)).isEqualTo(16);
        assertThat(BinaryArrayWriter.roundNumberOfBytesToNearestWord(16)).isEqualTo(16);
    }

    @Test
    public void testWriteLargeString() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        String largeString = new String(new char[100]).replace('\0', 'a');
        writer.writeString(0, BinaryString.fromString(largeString));
        writer.complete();

        assertThat(array.getString(0).toString()).isEqualTo(largeString);
    }

    @Test
    public void testWriteSmallString() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        String smallString = "ab";
        writer.writeString(0, BinaryString.fromString(smallString));
        writer.complete();

        assertThat(array.getString(0).toString()).isEqualTo(smallString);
    }

    @Test
    public void testWriteLargeBinary() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        byte[] largeBinary = new byte[100];
        for (int i = 0; i < largeBinary.length; i++) {
            largeBinary[i] = (byte) i;
        }

        writer.writeBytes(0, largeBinary);
        writer.complete();

        assertThat(array.getBytes(0)).isEqualTo(largeBinary);
    }

    @Test
    public void testSetNullBit() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 5, 4);

        writer.setNullBit(0);
        writer.setNullBit(2);
        writer.setNullBit(4);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
        assertThat(array.isNullAt(1)).isFalse();
        assertThat(array.isNullAt(2)).isTrue();
        assertThat(array.isNullAt(3)).isFalse();
        assertThat(array.isNullAt(4)).isTrue();
    }

    @Test
    public void testMultipleResetCycles() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 2, 4);

        for (int cycle = 0; cycle < 5; cycle++) {
            writer.writeInt(0, cycle * 10);
            writer.writeInt(1, cycle * 10 + 1);
            writer.complete();

            assertThat(array.getInt(0)).isEqualTo(cycle * 10);
            assertThat(array.getInt(1)).isEqualTo(cycle * 10 + 1);

            if (cycle < 4) {
                writer.reset();
            }
        }
    }

    @Test
    public void testWriteNullDecimal() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        writer.writeDecimal(0, null, 20);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
    }

    @Test
    public void testWriteNullTimestampNtz() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        writer.writeTimestampNtz(0, null, 9);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
    }

    @Test
    public void testWriteNullTimestampLtz() {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, 1, 8);

        writer.writeTimestampLtz(0, null, 9);
        writer.complete();

        assertThat(array.isNullAt(0)).isTrue();
    }
}

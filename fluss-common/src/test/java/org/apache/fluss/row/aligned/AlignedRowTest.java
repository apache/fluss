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

package org.apache.fluss.row.aligned;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.BinaryWriter;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AlignedRow}. */
class AlignedRowTest {

    @Test
    public void testBasic() {
        // consider header 1 byte.
        assertThat(new AlignedRow(0).getFixedLengthPartSize()).isEqualTo(8);
        assertThat(new AlignedRow(1).getFixedLengthPartSize()).isEqualTo(16);
        assertThat(new AlignedRow(65).getFixedLengthPartSize()).isEqualTo(536);
        assertThat(new AlignedRow(128).getFixedLengthPartSize()).isEqualTo(1048);

        MemorySegment segment = MemorySegment.wrap(new byte[100]);
        AlignedRow row = new AlignedRow(2);
        row.pointTo(segment, 10, 48);
        assertThat(segment).isSameAs(row.getSegments()[0]);
        row.setInt(0, 5);
        row.setDouble(1, 5.8D);
    }

    @Test
    public void testSetAndGet() throws IOException {
        MemorySegment segment = MemorySegment.wrap(new byte[100]);
        AlignedRow row = new AlignedRow(9);
        row.pointTo(segment, 20, 80);
        row.setNullAt(0);
        row.setInt(1, 11);
        row.setLong(2, 22);
        row.setDouble(3, 33);
        row.setBoolean(4, true);
        row.setShort(5, (short) 55);
        row.setByte(6, (byte) 66);
        row.setFloat(7, 77f);

        assertThat((long) row.getDouble(3)).isEqualTo(33L);
        assertThat(row.getInt(1)).isEqualTo(11);
        assertThat(row.isNullAt(0)).isTrue();
        assertThat(row.getShort(5)).isEqualTo((short) 55);
        assertThat(row.getLong(2)).isEqualTo(22L);
        assertThat(row.getBoolean(4)).isTrue();
        assertThat(row.getByte(6)).isEqualTo((byte) 66);
        assertThat(row.getFloat(7)).isEqualTo(77f);
    }

    @Test
    public void testWriter() {

        int arity = 13;
        AlignedRow row = new AlignedRow(arity);
        AlignedRowWriter writer = new AlignedRowWriter(row, 20);

        writer.writeString(0, BinaryString.fromString("1"));
        writer.writeString(3, BinaryString.fromString("1234567"));
        writer.writeString(5, BinaryString.fromString("12345678"));
        writer.writeString(
                9, BinaryString.fromString("God in his heaven, alls right with the world"));

        writer.writeBoolean(1, true);
        writer.writeByte(2, (byte) 99);
        writer.writeDouble(6, 87.1d);
        writer.writeFloat(7, 26.1f);
        writer.writeInt(8, 88);
        writer.writeLong(10, 284);
        writer.writeShort(11, (short) 292);
        writer.setNullAt(12);

        writer.complete();

        assertTestWriterRow(row);
        assertTestWriterRow(row.copy());

        // test copy from var segments.
        int subSize = row.getFixedLengthPartSize() + 10;
        MemorySegment subMs1 = MemorySegment.wrap(new byte[subSize]);
        MemorySegment subMs2 = MemorySegment.wrap(new byte[subSize]);
        row.getSegments()[0].copyTo(0, subMs1, 0, subSize);
        row.getSegments()[0].copyTo(subSize, subMs2, 0, row.getSizeInBytes() - subSize);

        AlignedRow toCopy = new AlignedRow(arity);
        toCopy.pointTo(new MemorySegment[] {subMs1, subMs2}, 0, row.getSizeInBytes());
        assertThat(toCopy).isEqualTo(row);
        assertTestWriterRow(toCopy);
        assertTestWriterRow(toCopy.copy(new AlignedRow(arity)));
    }

    @Test
    public void testWriteString() {
        {
            // litter byte[]
            AlignedRow row = new AlignedRow(1);
            AlignedRowWriter writer = new AlignedRowWriter(row);
            char[] chars = new char[2];
            chars[0] = 0xFFFF;
            chars[1] = 0;
            writer.writeString(0, BinaryString.fromString(new String(chars)));
            writer.complete();

            String str = row.getString(0).toString();
            assertThat(str.charAt(0)).isEqualTo(chars[0]);
            assertThat(str.charAt(1)).isEqualTo(chars[1]);
        }

        {
            // big byte[]
            String str = "God in his heaven, alls right with the world";
            AlignedRow row = new AlignedRow(2);
            AlignedRowWriter writer = new AlignedRowWriter(row);
            writer.writeString(0, BinaryString.fromString(str));
            writer.writeString(1, BinaryString.fromBytes(str.getBytes(StandardCharsets.UTF_8)));
            writer.complete();

            assertThat(row.getString(0).toString()).isEqualTo(str);
            assertThat(row.getString(1).toString()).isEqualTo(str);
        }
    }

    private void assertTestWriterRow(AlignedRow row) {
        assertThat(row.getString(0).toString()).isEqualTo("1");
        assertThat(row.getInt(8)).isEqualTo(88);
        assertThat(row.getShort(11)).isEqualTo((short) 292);
        assertThat(row.getLong(10)).isEqualTo(284);
        assertThat(row.getByte(2)).isEqualTo((byte) 99);
        assertThat(row.getDouble(6)).isEqualTo(87.1d);
        assertThat(row.getFloat(7)).isEqualTo(26.1f);
        assertThat(row.getBoolean(1)).isTrue();
        assertThat(row.getString(3).toString()).isEqualTo("1234567");
        assertThat(row.getString(5).toString()).isEqualTo("12345678");
        assertThat(row.getString(9).toString())
                .isEqualTo("God in his heaven, alls right with the world");
        assertThat(row.getString(9).hashCode())
                .isEqualTo(
                        BinaryString.fromString("God in his heaven, alls right with the world")
                                .hashCode());
        assertThat(row.isNullAt(12)).isTrue();
    }

    @Test
    public void testReuseWriter() {
        AlignedRow row = new AlignedRow(2);
        AlignedRowWriter writer = new AlignedRowWriter(row);
        writer.writeString(0, BinaryString.fromString("01234567"));
        writer.writeString(1, BinaryString.fromString("012345678"));
        writer.complete();
        assertThat(row.getString(0).toString()).isEqualTo("01234567");
        assertThat(row.getString(1).toString()).isEqualTo("012345678");

        writer.reset();
        writer.writeString(0, BinaryString.fromString("1"));
        writer.writeString(1, BinaryString.fromString("0123456789"));
        writer.complete();
        assertThat(row.getString(0).toString()).isEqualTo("1");
        assertThat(row.getString(1).toString()).isEqualTo("0123456789");
    }

    @Test
    public void anyNullTest() {
        {
            AlignedRow row = new AlignedRow(3);
            AlignedRowWriter writer = new AlignedRowWriter(row);
            assertThat(row.anyNull()).isFalse();

            // test header should not compute by anyNull
            assertThat(row.anyNull()).isFalse();

            writer.setNullAt(2);
            assertThat(row.anyNull()).isTrue();

            writer.setNullAt(0);
            assertThat(row.anyNull(new int[] {0, 1, 2})).isTrue();
            assertThat(row.anyNull(new int[] {1})).isFalse();

            writer.setNullAt(1);
            assertThat(row.anyNull()).isTrue();
        }

        int numFields = 80;
        for (int i = 0; i < numFields; i++) {
            AlignedRow row = new AlignedRow(numFields);
            AlignedRowWriter writer = new AlignedRowWriter(row);
            assertThat(row.anyNull()).isFalse();
            writer.setNullAt(i);
            assertThat(row.anyNull()).isTrue();
        }
    }

    @Test
    public void testSingleSegmentBinaryRowHashCode() {
        final Random rnd = new Random(System.currentTimeMillis());
        // test hash stabilization
        AlignedRow row = new AlignedRow(13);
        AlignedRowWriter writer = new AlignedRowWriter(row);
        for (int i = 0; i < 99; i++) {
            writer.reset();
            writer.writeString(0, BinaryString.fromString("" + rnd.nextInt()));
            writer.writeString(3, BinaryString.fromString("01234567"));
            writer.writeString(5, BinaryString.fromString("012345678"));
            writer.writeString(
                    9, BinaryString.fromString("God in his heaven, alls right with the world"));
            writer.writeBoolean(1, true);
            writer.writeByte(2, (byte) 99);
            writer.writeDouble(6, 87.1d);
            writer.writeFloat(7, 26.1f);
            writer.writeInt(8, 88);
            writer.writeLong(10, 284);
            writer.writeShort(11, (short) 292);
            writer.setNullAt(12);
            writer.complete();
            AlignedRow copy = row.copy();
            assertThat(copy.hashCode()).isEqualTo(row.hashCode());
        }

        // test hash distribution
        int count = 999999;
        Set<Integer> hashCodes = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            row.setInt(8, i);
            hashCodes.add(row.hashCode());
        }
        assertThat(hashCodes).hasSize(count);
        hashCodes.clear();
        row = new AlignedRow(1);
        writer = new AlignedRowWriter(row);
        for (int i = 0; i < count; i++) {
            writer.reset();
            writer.writeString(
                    0, BinaryString.fromString("God in his heaven, alls right with the world" + i));
            writer.complete();
            hashCodes.add(row.hashCode());
        }
        assertThat(hashCodes.size()).isGreaterThan((int) (count * 0.997));
    }

    @Test
    public void testHeaderSize() {
        assertThat(AlignedRow.calculateBitSetWidthInBytes(56)).isEqualTo(8);
        assertThat(AlignedRow.calculateBitSetWidthInBytes(57)).isEqualTo(16);
        assertThat(AlignedRow.calculateBitSetWidthInBytes(120)).isEqualTo(16);
        assertThat(AlignedRow.calculateBitSetWidthInBytes(121)).isEqualTo(24);
    }

    @Test
    public void testHeader() {
        AlignedRow row = new AlignedRow(2);
        AlignedRowWriter writer = new AlignedRowWriter(row);

        writer.writeInt(0, 10);
        writer.setNullAt(1);
        writer.complete();

        AlignedRow newRow = row.copy();
        assertThat(newRow).isEqualTo(row);
    }

    @Test
    public void testDecimal() {
        // 1.compact
        {
            int precision = 4;
            int scale = 2;
            AlignedRow row = new AlignedRow(2);
            AlignedRowWriter writer = new AlignedRowWriter(row);
            writer.writeDecimal(0, Decimal.fromUnscaledLong(5, precision, scale), precision);
            writer.setNullAt(1);
            writer.complete();

            assertThat(row.getDecimal(0, precision, scale).toString()).isEqualTo("0.05");
            assertThat(row.isNullAt(1)).isTrue();
            row.setDecimal(0, Decimal.fromUnscaledLong(6, precision, scale), precision);
            assertThat(row.getDecimal(0, precision, scale).toString()).isEqualTo("0.06");
        }

        // 2.not compact
        {
            int precision = 25;
            int scale = 5;
            Decimal decimal1 = Decimal.fromBigDecimal(BigDecimal.valueOf(5.55), precision, scale);
            Decimal decimal2 = Decimal.fromBigDecimal(BigDecimal.valueOf(6.55), precision, scale);

            AlignedRow row = new AlignedRow(2);
            AlignedRowWriter writer = new AlignedRowWriter(row);
            writer.writeDecimal(0, decimal1, precision);
            writer.writeDecimal(1, null, precision);
            writer.complete();

            assertThat(row.getDecimal(0, precision, scale).toString()).isEqualTo("5.55000");
            assertThat(row.isNullAt(1)).isTrue();
            row.setDecimal(0, decimal2, precision);
            assertThat(row.getDecimal(0, precision, scale).toString()).isEqualTo("6.55000");
        }
    }

    @Test
    public void testBinary() {
        AlignedRow row = new AlignedRow(2);
        AlignedRowWriter writer = new AlignedRowWriter(row);
        byte[] bytes1 = new byte[] {1, -1, 5};
        byte[] bytes2 = new byte[] {1, -1, 5, 5, 1, 5, 1, 5};
        writer.writeBytes(0, bytes1);
        writer.writeBytes(1, bytes2);
        writer.complete();

        assertThat(row.getBinary(0, bytes1.length)).isEqualTo(bytes1);
        assertThat(row.getBinary(1, bytes2.length)).isEqualTo(bytes2);
    }

    @Test
    public void testZeroOutPaddingString() {

        Random random = new Random();
        byte[] bytes = new byte[1024];

        AlignedRow row = new AlignedRow(1);
        AlignedRowWriter writer = new AlignedRowWriter(row);

        writer.reset();
        random.nextBytes(bytes);
        writer.writeBytes(0, bytes);
        writer.reset();
        writer.writeString(0, BinaryString.fromString("wahahah"));
        writer.complete();
        int hash1 = row.hashCode();

        writer.reset();
        random.nextBytes(bytes);
        writer.writeBytes(0, bytes);
        writer.reset();
        writer.writeString(0, BinaryString.fromString("wahahah"));
        writer.complete();
        int hash2 = row.hashCode();

        assertThat(hash2).isEqualTo(hash1);
    }

    @Test
    public void testTimestamp() {
        // 1. compact
        {
            final int precision = 3;
            AlignedRow row = new AlignedRow(2);
            AlignedRowWriter writer = new AlignedRowWriter(row);
            writer.writeTimestampNtz(0, TimestampNtz.fromMillis(123L), precision);
            writer.setNullAt(1);
            writer.complete();

            assertThat(row.getTimestampNtz(0, 3).toString()).isEqualTo("1970-01-01T00:00:00.123");
            assertThat(row.isNullAt(1)).isTrue();
            row.setTimestampNtz(0, TimestampNtz.fromMillis(-123L), precision);
            assertThat(row.getTimestampNtz(0, 3).toString()).isEqualTo("1969-12-31T23:59:59.877");
        }

        // 2. not compact
        {
            final int precision = 9;
            TimestampLtz timestamp1 =
                    TimestampLtz.fromInstant(
                            LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789)
                                    .toInstant(ZoneOffset.UTC));
            TimestampLtz timestamp2 =
                    TimestampLtz.fromInstant(
                            LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123456789)
                                    .toInstant(ZoneOffset.UTC));
            AlignedRow row = new AlignedRow(2);
            AlignedRowWriter writer = new AlignedRowWriter(row);
            writer.writeTimestampLtz(0, timestamp1, precision);
            writer.writeTimestampLtz(1, null, precision);
            writer.complete();

            // the size of row should be 8 + (8 + 8) * 2
            // (8 bytes nullBits, 8 bytes fixed-length part and 8 bytes variable-length part for
            // each timestamp(9))
            assertThat(row.getSizeInBytes()).isEqualTo(40);

            assertThat(row.getTimestampLtz(0, precision).toString())
                    .isEqualTo("1969-01-01T00:00:00.123456789Z");
            assertThat(row.isNullAt(1)).isTrue();
            row.setTimestampLtz(0, timestamp2, precision);
            assertThat(row.getTimestampLtz(0, precision).toString())
                    .isEqualTo("1970-01-01T00:00:00.123456789Z");
        }
    }

    @Test
    public void testGetChar() {
        AlignedRow row = new AlignedRow(3);
        AlignedRowWriter writer = new AlignedRowWriter(row);

        String shortString = "hello";
        String longString = "This is a longer string for testing getChar method";
        String unicodeString = "测试Unicode字符串";

        writer.writeString(0, BinaryString.fromString(shortString));
        writer.writeString(1, BinaryString.fromString(longString));
        writer.writeString(2, BinaryString.fromString(unicodeString));
        writer.complete();

        // Test getChar with exact length
        assertThat(row.getChar(0, shortString.length()).toString()).isEqualTo(shortString);
        assertThat(row.getChar(1, longString.length()).toString()).isEqualTo(longString);
        assertThat(row.getChar(2, unicodeString.length()).toString()).isEqualTo(unicodeString);

        // Test getChar with different lengths (should still return the full string)
        assertThat(row.getChar(0, shortString.length() + 10).toString()).isEqualTo(shortString);
        assertThat(row.getChar(1, longString.length() - 10).toString()).isEqualTo(longString);

        // Verify getChar returns same result as getString
        assertThat(row.getChar(0, shortString.length())).isEqualTo(row.getString(0));
        assertThat(row.getChar(1, longString.length())).isEqualTo(row.getString(1));
        assertThat(row.getChar(2, unicodeString.length())).isEqualTo(row.getString(2));
    }

    @Test
    public void testGetBytes() {
        AlignedRow row = new AlignedRow(3);
        AlignedRowWriter writer = new AlignedRowWriter(row);

        byte[] smallBytes = new byte[] {1, 2, 3};
        byte[] largeBytes = new byte[] {1, -1, 5, 10, -10, 127, -128, 0, 50, -50};
        byte[] emptyBytes = new byte[0];

        writer.writeBytes(0, smallBytes);
        writer.writeBytes(1, largeBytes);
        writer.writeBytes(2, emptyBytes);
        writer.complete();

        // Test getBytes method
        assertThat(row.getBytes(0)).isEqualTo(smallBytes);
        assertThat(row.getBytes(1)).isEqualTo(largeBytes);
        assertThat(row.getBytes(2)).isEqualTo(emptyBytes);

        // Verify getBytes returns same result as getBinary with correct length
        assertThat(row.getBytes(0)).isEqualTo(row.getBinary(0, smallBytes.length));
        assertThat(row.getBytes(1)).isEqualTo(row.getBinary(1, largeBytes.length));
        assertThat(row.getBytes(2)).isEqualTo(row.getBinary(2, emptyBytes.length));

        // Test with copied row
        AlignedRow copiedRow = row.copy();
        assertThat(copiedRow.getBytes(0)).isEqualTo(smallBytes);
        assertThat(copiedRow.getBytes(1)).isEqualTo(largeBytes);
        assertThat(copiedRow.getBytes(2)).isEqualTo(emptyBytes);
    }

    @Test
    public void testMemoryGrowth() {
        // Test automatic memory growth when initial size is small
        AlignedRow row = new AlignedRow(3);
        AlignedRowWriter writer = new AlignedRowWriter(row, 10); // small initial size

        // Write data that exceeds initial capacity
        String largeString =
                "This is a very long string that should cause memory growth in the binary row writer implementation when written to the row";
        byte[] largeBytes = new byte[200];
        for (int i = 0; i < largeBytes.length; i++) {
            largeBytes[i] = (byte) (i % 127);
        }

        writer.writeString(0, BinaryString.fromString(largeString));
        writer.writeBytes(1, largeBytes);
        writer.writeInt(2, 42);
        writer.complete();

        // Verify data integrity after growth
        assertThat(row.getString(0).toString()).isEqualTo(largeString);
        assertThat(row.getBytes(1)).isEqualTo(largeBytes);
        assertThat(row.getInt(2)).isEqualTo(42);

        // Verify the segment has grown
        assertThat(row.getSizeInBytes()).isGreaterThan(10);
    }

    @Test
    public void testGetSegments() {
        AlignedRow row = new AlignedRow(2);
        AlignedRowWriter writer = new AlignedRowWriter(row, 50);

        writer.writeString(0, BinaryString.fromString("test"));
        writer.writeInt(1, 123);
        writer.complete();

        // Test getSegments method
        MemorySegment segment = writer.getSegments();
        assertThat(segment).isNotNull();
        assertThat(segment).isSameAs(row.getSegments()[0]);

        // Verify we can read data from the segment
        assertThat(row.getString(0).toString()).isEqualTo("test");
        assertThat(row.getInt(1)).isEqualTo(123);
    }

    @Test
    public void testValueWriter() {
        DataType[] fieldTypes =
                new DataType[] {
                    DataTypes.BOOLEAN(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.STRING(),
                    DataTypes.BINARY(3),
                    DataTypes.DECIMAL(3, 2)
                };
        AlignedRow row = new AlignedRow(10);
        AlignedRowWriter writer = new AlignedRowWriter(row);
        BinaryWriter.ValueWriter[] fieldSetters = new BinaryWriter.ValueWriter[10];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldSetters[i] = BinaryWriter.createValueWriter(fieldTypes[i]);
        }

        // Test static write method for different data types
        fieldSetters[0].writeValue(writer, 0, true);
        fieldSetters[1].writeValue(writer, 1, (byte) 100);
        fieldSetters[2].writeValue(writer, 2, (short) 1000);
        fieldSetters[3].writeValue(writer, 3, 100000);
        fieldSetters[4].writeValue(writer, 4, 100000000L);
        fieldSetters[5].writeValue(writer, 5, 3.14f);
        fieldSetters[6].writeValue(writer, 6, 3.14159);
        fieldSetters[7].writeValue(writer, 7, BinaryString.fromString("hello"));
        fieldSetters[8].writeValue(writer, 8, new byte[] {1, 2, 3});
        // Test decimal
        Decimal decimal = Decimal.fromUnscaledLong(314, 3, 2);
        fieldSetters[9].writeValue(writer, 9, decimal);
        writer.complete();

        // Verify all written data
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getByte(1)).isEqualTo((byte) 100);
        assertThat(row.getShort(2)).isEqualTo((short) 1000);
        assertThat(row.getInt(3)).isEqualTo(100000);
        assertThat(row.getLong(4)).isEqualTo(100000000L);
        assertThat(row.getFloat(5)).isEqualTo(3.14f);
        assertThat(row.getDouble(6)).isEqualTo(3.14159);
        assertThat(row.getString(7).toString()).isEqualTo("hello");
        assertThat(row.getBytes(8)).isEqualTo(new byte[] {1, 2, 3});
        assertThat(row.getDecimal(9, 3, 2).toString()).isEqualTo("3.14");
    }

    @Test
    public void testEdgeCases() {
        // Test with zero fields
        AlignedRow emptyRow = new AlignedRow(0);
        AlignedRowWriter emptyWriter = new AlignedRowWriter(emptyRow);
        emptyWriter.complete();
        assertThat(emptyRow.getFieldCount()).isEqualTo(0);

        // Test with single field
        AlignedRow singleRow = new AlignedRow(1);
        AlignedRowWriter singleWriter = new AlignedRowWriter(singleRow);
        singleWriter.writeInt(0, 42);
        singleWriter.complete();
        assertThat(singleRow.getInt(0)).isEqualTo(42);

        // Test with maximum fixed-length data (7 bytes)
        AlignedRow maxFixedRow = new AlignedRow(1);
        AlignedRowWriter maxFixedWriter = new AlignedRowWriter(maxFixedRow);
        byte[] maxFixedBytes = new byte[7];
        Arrays.fill(maxFixedBytes, (byte) 0xFF);
        maxFixedWriter.writeBytes(0, maxFixedBytes);
        maxFixedWriter.complete();
        assertThat(maxFixedRow.getBytes(0)).isEqualTo(maxFixedBytes);

        // Test with 8 bytes (should go to variable length part)
        AlignedRow varLenRow = new AlignedRow(1);
        AlignedRowWriter varLenWriter = new AlignedRowWriter(varLenRow);
        byte[] varLenBytes = new byte[8];
        Arrays.fill(varLenBytes, (byte) 0xAA);
        varLenWriter.writeBytes(0, varLenBytes);
        varLenWriter.complete();
        assertThat(varLenRow.getBytes(0)).isEqualTo(varLenBytes);
    }

    @Test
    public void testLargeFieldCount() {
        // Test with many fields (80 fields as used in anyNullTest)
        int fieldCount = 80;
        AlignedRow row = new AlignedRow(fieldCount);
        AlignedRowWriter writer = new AlignedRowWriter(row);

        // Write different types to different fields
        for (int i = 0; i < fieldCount; i++) {
            switch (i % 5) {
                case 0:
                    writer.writeInt(i, i);
                    break;
                case 1:
                    writer.writeString(i, BinaryString.fromString("field_" + i));
                    break;
                case 2:
                    writer.writeDouble(i, i * 1.5);
                    break;
                case 3:
                    writer.writeBoolean(i, i % 2 == 0);
                    break;
                case 4:
                    writer.writeLong(i, (long) i * 1000);
                    break;
            }
        }
        writer.complete();

        // Verify data integrity
        for (int i = 0; i < fieldCount; i++) {
            switch (i % 5) {
                case 0:
                    assertThat(row.getInt(i)).isEqualTo(i);
                    break;
                case 1:
                    assertThat(row.getString(i).toString()).isEqualTo("field_" + i);
                    break;
                case 2:
                    assertThat(row.getDouble(i)).isEqualTo(i * 1.5);
                    break;
                case 3:
                    assertThat(row.getBoolean(i)).isEqualTo(i % 2 == 0);
                    break;
                case 4:
                    assertThat(row.getLong(i)).isEqualTo((long) i * 1000);
                    break;
            }
        }
    }

    @Test
    public void testResetAndReusability() {
        AlignedRow row = new AlignedRow(3);
        AlignedRowWriter writer = new AlignedRowWriter(row);

        // First write
        writer.writeInt(0, 100);
        writer.writeString(1, BinaryString.fromString("first"));
        writer.setNullAt(2);
        writer.complete();

        assertThat(row.getInt(0)).isEqualTo(100);
        assertThat(row.getString(1).toString()).isEqualTo("first");
        assertThat(row.isNullAt(2)).isTrue();

        // Reset and write again
        writer.reset();
        writer.writeInt(0, 200);
        writer.writeString(1, BinaryString.fromString("second"));
        writer.writeDouble(2, 3.14);
        writer.complete();

        assertThat(row.getInt(0)).isEqualTo(200);
        assertThat(row.getString(1).toString()).isEqualTo("second");
        assertThat(row.getDouble(2)).isEqualTo(3.14);
        assertThat(row.isNullAt(2)).isFalse();

        // Reset multiple times
        for (int i = 0; i < 5; i++) {
            writer.reset();
            writer.writeInt(0, i);
            writer.writeString(1, BinaryString.fromString("iteration_" + i));
            writer.writeBoolean(2, i % 2 == 0);
            writer.complete();

            assertThat(row.getInt(0)).isEqualTo(i);
            assertThat(row.getString(1).toString()).isEqualTo("iteration_" + i);
            assertThat(row.getBoolean(2)).isEqualTo(i % 2 == 0);
        }
    }

    @Test
    public void testComplexDataMix() {
        // Test mixing all supported data types in one row
        AlignedRow row = new AlignedRow(12);
        AlignedRowWriter writer = new AlignedRowWriter(row);

        // Write various types including null values
        writer.writeBoolean(0, true);
        writer.writeByte(1, (byte) -128);
        writer.writeShort(2, Short.MAX_VALUE);
        writer.writeInt(3, Integer.MIN_VALUE);
        writer.writeLong(4, Long.MAX_VALUE);
        writer.writeFloat(5, Float.MIN_VALUE);
        writer.writeDouble(6, Double.MAX_VALUE);
        writer.writeString(7, BinaryString.fromString("复杂测试字符串with special chars !@#$%"));
        writer.writeBytes(8, new byte[] {-1, 0, 1, 127, -128});

        // Test compact decimal
        writer.writeDecimal(9, Decimal.fromUnscaledLong(12345, 5, 2), 5);

        // Test non-compact timestamp
        writer.writeTimestampNtz(10, TimestampNtz.fromMillis(1609459200000L, 123456), 9);

        writer.setNullAt(11);
        writer.complete();

        // Verify all data
        assertThat(row.getBoolean(0)).isTrue();
        assertThat(row.getByte(1)).isEqualTo((byte) -128);
        assertThat(row.getShort(2)).isEqualTo(Short.MAX_VALUE);
        assertThat(row.getInt(3)).isEqualTo(Integer.MIN_VALUE);
        assertThat(row.getLong(4)).isEqualTo(Long.MAX_VALUE);
        assertThat(row.getFloat(5)).isEqualTo(Float.MIN_VALUE);
        assertThat(row.getDouble(6)).isEqualTo(Double.MAX_VALUE);
        assertThat(row.getString(7).toString()).isEqualTo("复杂测试字符串with special chars !@#$%");
        assertThat(row.getBytes(8)).isEqualTo(new byte[] {-1, 0, 1, 127, -128});
        assertThat(row.getDecimal(9, 5, 2).toString()).isEqualTo("123.45");
        assertThat(row.getTimestampNtz(10, 9).toString()).contains("2021-01-01T00:00:00.000123456");
        assertThat(row.isNullAt(11)).isTrue();
    }
}

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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BinaryRowData}. */
class BinaryRowDataTest {

    @Test
    public void testBasic() {
        // consider header 1 byte.
        assertThat(new BinaryRowData(0).getFixedLengthPartSize()).isEqualTo(8);
        assertThat(new BinaryRowData(1).getFixedLengthPartSize()).isEqualTo(16);
        assertThat(new BinaryRowData(65).getFixedLengthPartSize()).isEqualTo(536);
        assertThat(new BinaryRowData(128).getFixedLengthPartSize()).isEqualTo(1048);

        MemorySegment segment = MemorySegment.wrap(new byte[100]);
        BinaryRowData row = new BinaryRowData(2);
        row.pointTo(segment, 10, 48);
        assertThat(segment).isSameAs(row.getSegments()[0]);
        row.setInt(0, 5);
        row.setDouble(1, 5.8D);
    }

    @Test
    public void testSetAndGet() throws IOException, ClassNotFoundException {
        MemorySegment segment = MemorySegment.wrap(new byte[100]);
        BinaryRowData row = new BinaryRowData(9);
        row.pointTo(segment, 20, 80);
        row.setNullAt(0);
        row.setInt(1, 11);
        row.setLong(2, 22);
        row.setDouble(3, 33);
        row.setBoolean(4, true);
        row.setShort(5, (short) 55);
        row.setByte(6, (byte) 66);
        row.setFloat(7, 77f);

        Consumer<BinaryRow> assertConsumer =
                assertRow -> {
                    assertThat((long) assertRow.getDouble(3)).isEqualTo(33L);
                    assertThat(assertRow.getInt(1)).isEqualTo(11);
                    assertThat(assertRow.isNullAt(0)).isTrue();
                    assertThat(assertRow.getShort(5)).isEqualTo((short) 55);
                    assertThat(assertRow.getLong(2)).isEqualTo(22L);
                    assertThat(assertRow.getBoolean(4)).isTrue();
                    assertThat(assertRow.getByte(6)).isEqualTo((byte) 66);
                    assertThat(assertRow.getFloat(7)).isEqualTo(77f);
                };

        assertConsumer.accept(row);
    }

    @Test
    public void testWriter() {

        int arity = 13;
        BinaryRowData row = new BinaryRowData(arity);
        BinaryRowWriter writer = new BinaryRowWriter(row, 20);

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

        BinaryRowData toCopy = new BinaryRowData(arity);
        toCopy.pointTo(new MemorySegment[] {subMs1, subMs2}, 0, row.getSizeInBytes());
        assertThat(toCopy).isEqualTo(row);
        assertTestWriterRow(toCopy);
        assertTestWriterRow(toCopy.copy(new BinaryRowData(arity)));
    }

    @Test
    public void testWriteString() {
        {
            // litter byte[]
            BinaryRowData row = new BinaryRowData(1);
            BinaryRowWriter writer = new BinaryRowWriter(row);
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
            BinaryRowData row = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            writer.writeString(0, BinaryString.fromString(str));
            writer.writeString(1, BinaryString.fromBytes(str.getBytes(StandardCharsets.UTF_8)));
            writer.complete();

            assertThat(row.getString(0).toString()).isEqualTo(str);
            assertThat(row.getString(1).toString()).isEqualTo(str);
        }
    }

    private void assertTestWriterRow(BinaryRowData row) {
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
        BinaryRowData row = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
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
            BinaryRowData row = new BinaryRowData(3);
            BinaryRowWriter writer = new BinaryRowWriter(row);
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
            BinaryRowData row = new BinaryRowData(numFields);
            BinaryRowWriter writer = new BinaryRowWriter(row);
            assertThat(row.anyNull()).isFalse();
            writer.setNullAt(i);
            assertThat(row.anyNull()).isTrue();
        }
    }

    @Test
    public void testSingleSegmentBinaryRowHashCode() {
        final Random rnd = new Random(System.currentTimeMillis());
        // test hash stabilization
        BinaryRowData row = new BinaryRowData(13);
        BinaryRowWriter writer = new BinaryRowWriter(row);
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
            BinaryRowData copy = row.copy();
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
        row = new BinaryRowData(1);
        writer = new BinaryRowWriter(row);
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
        assertThat(BinaryRowData.calculateBitSetWidthInBytes(56)).isEqualTo(8);
        assertThat(BinaryRowData.calculateBitSetWidthInBytes(57)).isEqualTo(16);
        assertThat(BinaryRowData.calculateBitSetWidthInBytes(120)).isEqualTo(16);
        assertThat(BinaryRowData.calculateBitSetWidthInBytes(121)).isEqualTo(24);
    }

    @Test
    public void testHeader() {
        BinaryRowData row = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);

        writer.writeInt(0, 10);
        writer.setNullAt(1);
        writer.complete();

        BinaryRowData newRow = row.copy();
        assertThat(newRow).isEqualTo(row);
    }

    @Test
    public void testDecimal() {
        // 1.compact
        {
            int precision = 4;
            int scale = 2;
            BinaryRowData row = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
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

            BinaryRowData row = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
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
        BinaryRowData row = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        byte[] bytes1 = new byte[] {1, -1, 5};
        byte[] bytes2 = new byte[] {1, -1, 5, 5, 1, 5, 1, 5};
        writer.writeBinary(0, bytes1);
        writer.writeBinary(1, bytes2);
        writer.complete();

        assertThat(row.getBinary(0, bytes1.length)).isEqualTo(bytes1);
        assertThat(row.getBinary(1, bytes2.length)).isEqualTo(bytes2);
    }

    @Test
    public void testZeroOutPaddingString() {

        Random random = new Random();
        byte[] bytes = new byte[1024];

        BinaryRowData row = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);

        writer.reset();
        random.nextBytes(bytes);
        writer.writeBinary(0, bytes);
        writer.reset();
        writer.writeString(0, BinaryString.fromString("wahahah"));
        writer.complete();
        int hash1 = row.hashCode();

        writer.reset();
        random.nextBytes(bytes);
        writer.writeBinary(0, bytes);
        writer.reset();
        writer.writeString(0, BinaryString.fromString("wahahah"));
        writer.complete();
        int hash2 = row.hashCode();

        assertThat(hash2).isEqualTo(hash1);
    }

    @Test
    public void testTimestampData() {
        // 1. compact
        {
            final int precision = 3;
            BinaryRowData row = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
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
                    TimestampLtz.fromLocalDateTime(
                            LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789));
            TimestampLtz timestamp2 =
                    TimestampLtz.fromLocalDateTime(
                            LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123456789));
            BinaryRowData row = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(row);
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
        BinaryRowData row = new BinaryRowData(3);
        BinaryRowWriter writer = new BinaryRowWriter(row);

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
        BinaryRowData row = new BinaryRowData(3);
        BinaryRowWriter writer = new BinaryRowWriter(row);

        byte[] smallBytes = new byte[] {1, 2, 3};
        byte[] largeBytes = new byte[] {1, -1, 5, 10, -10, 127, -128, 0, 50, -50};
        byte[] emptyBytes = new byte[0];

        writer.writeBinary(0, smallBytes);
        writer.writeBinary(1, largeBytes);
        writer.writeBinary(2, emptyBytes);
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
        BinaryRowData copiedRow = row.copy();
        assertThat(copiedRow.getBytes(0)).isEqualTo(smallBytes);
        assertThat(copiedRow.getBytes(1)).isEqualTo(largeBytes);
        assertThat(copiedRow.getBytes(2)).isEqualTo(emptyBytes);
    }
}

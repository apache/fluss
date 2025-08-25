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

package com.alibaba.fluss.row.encode.iceberg;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link IcebergBinaryRowWriter}. */
class IcebergBinaryRowWriterTest {

    @Test
    void testWriteBoolean() {
        boolean value = false;

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeBoolean(value);

        byte[] writerBytes = writer.toBytes();

        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) (value ? 1 : 0));
        byte[] expectedBytes = buffer.array();
        assertThat(writerBytes).isEqualTo(expectedBytes);
    }

    @Test
    void testWriteByte() {
        byte value = 1;

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeByte(value);

        byte[] writerBytes = writer.toBytes();

        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) (value));
        byte[] expectedBytes = buffer.array();
        assertThat(writerBytes).isEqualTo(expectedBytes);
    }

    @Test
    void testWriteShort() {
        short value = 20;

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeShort(value);

        byte[] writerBytes = writer.toBytes();

        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.put((byte) value);
        byte[] expectedBytes = buffer.array();
        assertThat(writerBytes).isEqualTo(expectedBytes);
    }

    @Test
    void testWriteInt() {
        int value = 1234;

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeInt(value);

        byte[] writerBytes = writer.toBytes();

        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(value);
        byte[] expectedBytes = buffer.array();
        assertThat(writerBytes).isEqualTo(expectedBytes);
    }

    @Test
    void testWriteIntAsLong() {
        int value = 1234;

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeIntAsLong(value);

        byte[] writerBytes = writer.toBytes();

        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value);
        byte[] expectedBytes = buffer.array();
        assertThat(writerBytes).isEqualTo(expectedBytes);
    }

    @Test
    void testWriteLong() {
        long value = 123456899122345678L;

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeLong(value);

        byte[] writerBytes = writer.toBytes();

        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(value);
        byte[] expectedBytes = buffer.array();
        assertThat(writerBytes).isEqualTo(expectedBytes);
    }

    @Test
    void testWriteFloat() {
        float value = 3.14f;

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeFloat(value);

        byte[] writerBytes = writer.toBytes();

        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putFloat(value);
        byte[] expectedBytes = buffer.array();
        assertThat(writerBytes).isEqualTo(expectedBytes);
    }

    @Test
    void testWriteDouble() {
        double value = 3.1415926;

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeDouble(value);

        byte[] writerBytes = writer.toBytes();

        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putDouble(value);
        byte[] expectedBytes = buffer.array();
        assertThat(writerBytes).isEqualTo(expectedBytes);
    }

    @Test
    void testWriteString() {
        String value = "Hello World!";

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeString(BinaryString.fromString(value));
        byte[] writerBytes = writer.toBytes();
        byte[] strBytes = value.getBytes(StandardCharsets.UTF_8);

        int length = ByteBuffer.wrap(writerBytes, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] actualContent = Arrays.copyOfRange(writerBytes, 4, 4 + length);

        assertThat(length).isEqualTo(strBytes.length);
        assertThat(actualContent).isEqualTo(strBytes);

        writer.reset();

        writer.writeString(BinaryString.fromString(value), true);
        byte[] writerBytesWithoutPrefix = writer.toBytes();
        assertThat(writerBytesWithoutPrefix).isEqualTo(strBytes);
    }

    @Test
    void testWriteBytes() {
        byte[] value = new byte[] {12, 23, 34, 45};

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeBytes(value);
        byte[] writerBytes = writer.toBytes();

        int length = ByteBuffer.wrap(writerBytes, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] actualContent = Arrays.copyOfRange(writerBytes, 4, 4 + length);

        assertThat(length).isEqualTo(value.length);
        assertThat(actualContent).isEqualTo(value);

        writer.reset();

        writer.writeBytes(value, true);
        byte[] writerBytesWithoutPrefix = writer.toBytes();
        assertThat(writerBytesWithoutPrefix).isEqualTo(value);
    }

    @Test
    void testWriteDecimal() {
        BigDecimal bigDecimal = new BigDecimal("123.45");
        Decimal value = Decimal.fromBigDecimal(bigDecimal, 10, 2);

        IcebergBinaryRowWriter writer = new IcebergBinaryRowWriter(1);
        writer.writeDecimal(value);
        byte[] writerBytes = writer.toBytes();
        byte[] expectedBytes = bigDecimal.unscaledValue().toByteArray();

        int length = ByteBuffer.wrap(writerBytes, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        byte[] actualContent = Arrays.copyOfRange(writerBytes, 4, 4 + length);

        assertThat(length).isEqualTo(expectedBytes.length);
        assertThat(actualContent).isEqualTo(expectedBytes);

        writer.reset();

        writer.writeDecimal(value, true);
        byte[] writerBytesWithoutPrefix = writer.toBytes();
        assertThat(writerBytesWithoutPrefix).isEqualTo(expectedBytes);
    }
}

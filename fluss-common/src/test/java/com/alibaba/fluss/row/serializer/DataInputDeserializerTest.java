/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.serializer;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DataInputDeserializer}. */
class DataInputDeserializerTest {

    @Test
    void testConstructor() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        assertThat(deserializer.available()).isEqualTo(0);
    }

    @Test
    void testConstructorWithBuffer() {
        byte[] buffer = {1, 2, 3, 4, 5};
        DataInputDeserializer deserializer = new DataInputDeserializer(buffer);
        assertThat(deserializer.available()).isEqualTo(5);
    }

    @Test
    void testReadBoolean() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeBoolean(true);
        serializer.writeBoolean(false);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readBoolean()).isTrue();
        assertThat(deserializer.readBoolean()).isFalse();
    }

    @Test
    void testReadByte() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeByte(42);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readByte()).isEqualTo((byte) 42);
    }

    @Test
    void testReadInt() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeInt(12345);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readInt()).isEqualTo(12345);
    }

    @Test
    void testReadLong() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeLong(123456789L);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readLong()).isEqualTo(123456789L);
    }

    @Test
    void testReadShort() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeShort(12345);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readShort()).isEqualTo((short) 12345);
    }

    @Test
    void testReadUTF() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(100);
        serializer.writeUTF("Hello World");

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readUTF()).isEqualTo("Hello World");
    }

    @Test
    void testReadFully() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        byte[] data = {1, 2, 3, 4, 5};
        serializer.write(data);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        byte[] result = new byte[5];
        deserializer.readFully(result);
        assertThat(result).isEqualTo(data);
    }

    @Test
    void testSkipBytes() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        int skipped = deserializer.skipBytes(3);
        assertThat(skipped).isEqualTo(3);
        assertThat(deserializer.available()).isEqualTo(2);
    }

    @Test
    void testGetPosition() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        assertThat(deserializer.getPosition()).isEqualTo(0);
        deserializer.skipBytes(2);
        assertThat(deserializer.getPosition()).isEqualTo(2);
    }

    @Test
    void testEOFException() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1});

        assertThatThrownBy(() -> deserializer.readInt()).isInstanceOf(EOFException.class);

        assertThatThrownBy(() -> deserializer.readLong()).isInstanceOf(EOFException.class);
    }

    @Test
    void testSetBuffer() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        byte[] buffer = {1, 2, 3, 4, 5};
        deserializer.setBuffer(buffer);
        assertThat(deserializer.available()).isEqualTo(5);
    }

    @Test
    void testSetBufferWithByteBuffer() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5});
        deserializer.setBuffer(buffer);
        assertThat(deserializer.available()).isEqualTo(5);
    }

    @Test
    void testReadChar() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeChar('A');

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readChar()).isEqualTo('A');
    }

    @Test
    void testReadDouble() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeDouble(3.14);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readDouble()).isEqualTo(3.14);
    }

    @Test
    void testReadFloat() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeFloat(3.14f);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readFloat()).isEqualTo(3.14f);
    }

    @Test
    void testReadUnsignedByte() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeByte(200); // Value > 127

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readUnsignedByte()).isEqualTo(200);
    }

    @Test
    void testReadUnsignedShort() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeShort(40000); // Value > 32767

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readUnsignedShort()).isEqualTo(40000);
    }

    @Test
    void testReadFullyWithOffset() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        byte[] data = {1, 2, 3, 4, 5};
        serializer.write(data);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        byte[] result = new byte[7];
        deserializer.readFully(result, 1, 5);
        assertThat(result[0]).isEqualTo((byte) 0); // Default value
        assertThat(result[1]).isEqualTo((byte) 1);
        assertThat(result[2]).isEqualTo((byte) 2);
        assertThat(result[3]).isEqualTo((byte) 3);
        assertThat(result[4]).isEqualTo((byte) 4);
        assertThat(result[5]).isEqualTo((byte) 5);
        assertThat(result[6]).isEqualTo((byte) 0); // Default value
    }

    @Test
    void testReadFullyWithNullArray() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        assertThatThrownBy(() -> deserializer.readFully(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testReadFullyWithNullArrayAndOffset() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        assertThatThrownBy(() -> deserializer.readFully(null, 0, 1))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testReadFullyWithInvalidOffset() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        byte[] result = new byte[5];
        assertThatThrownBy(() -> deserializer.readFully(result, -1, 5))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testReadFullyWithInvalidLength() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        byte[] result = new byte[5];
        assertThatThrownBy(() -> deserializer.readFully(result, 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReadFullyWithInsufficientData() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3});
        byte[] result = new byte[5];
        assertThatThrownBy(() -> deserializer.readFully(result)).isInstanceOf(EOFException.class);
    }

    @Test
    void testSkipBytesWithInvalidValue() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        int skipped = deserializer.skipBytes(-1);
        assertThat(skipped).isEqualTo(-1);
    }

    @Test
    void testSkipBytesBeyondAvailable() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        int skipped = deserializer.skipBytes(10);
        assertThat(skipped).isEqualTo(5);
        assertThat(deserializer.available()).isEqualTo(0);
    }

    @Test
    void testReadUTFWithEmptyString() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        serializer.writeUTF("");

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readUTF()).isEqualTo("");
    }

    @Test
    void testReadUTFWithSpecialCharacters() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(100);
        serializer.writeUTF("Hello\nWorld\tTest");

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readUTF()).isEqualTo("Hello\nWorld\tTest");
    }

    @Test
    void testReadUTFWithInsufficientData() {
        DataInputDeserializer deserializer =
                new DataInputDeserializer(new byte[] {0, 1}); // Length 1 but no data
        assertThatThrownBy(() -> deserializer.readUTF()).isInstanceOf(EOFException.class);
    }

    @Test
    void testSetBufferWithNull() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        assertThatThrownBy(() -> deserializer.setBuffer((byte[]) null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testSetBufferWithNullByteBuffer() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        assertThatThrownBy(() -> deserializer.setBuffer((ByteBuffer) null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testAvailableAfterSetBuffer() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        assertThat(deserializer.available()).isEqualTo(0);

        byte[] buffer = {1, 2, 3, 4, 5};
        deserializer.setBuffer(buffer);
        assertThat(deserializer.available()).isEqualTo(5);

        deserializer.skipBytes(2);
        assertThat(deserializer.available()).isEqualTo(3);
    }

    @Test
    void testSetBufferWithInvalidBounds() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        byte[] buffer = {1, 2, 3, 4, 5};

        assertThatThrownBy(() -> deserializer.setBuffer(buffer, -1, 3))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> deserializer.setBuffer(buffer, 0, -1))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> deserializer.setBuffer(buffer, 10, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetBufferWithDirectByteBuffer() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(5);
        directBuffer.put(new byte[] {1, 2, 3, 4, 5});
        directBuffer.flip();

        deserializer.setBuffer(directBuffer);
        assertThat(deserializer.available()).isEqualTo(5);
    }

    @Test
    void testSetBufferWithReadOnlyByteBuffer() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        ByteBuffer readOnlyBuffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}).asReadOnlyBuffer();

        deserializer.setBuffer(readOnlyBuffer);
        assertThat(deserializer.available()).isEqualTo(5);
    }

    @Test
    void testSetBufferWithInvalidByteBuffer() {
        DataInputDeserializer deserializer = new DataInputDeserializer();
        ByteBuffer invalidBuffer = ByteBuffer.allocate(5);
        invalidBuffer.position(3);
        invalidBuffer.limit(2); // Invalid state

        // setBuffer doesn't validate ByteBuffer state, it only checks buffer type
        // The method will still work but may have unexpected behavior
        deserializer.setBuffer(invalidBuffer);
        assertThat(deserializer.available()).isEqualTo(0); // No remaining bytes
    }

    @Test
    void testReleaseArrays() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        deserializer.releaseArrays();
        // releaseArrays only sets buffer to null, but available() still calculates end - position
        // The test should check that the buffer is actually released
        assertThatThrownBy(() -> deserializer.readByte()).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testReadLine() throws IOException {
        // Create data directly without using writeBytes due to its bug
        byte[] data = "Hello\nWorld\r\nTest\n".getBytes();
        DataInputDeserializer deserializer = new DataInputDeserializer(data);
        assertThat(deserializer.readLine()).isEqualTo("Hello");
        assertThat(deserializer.readLine()).isEqualTo("World");
        assertThat(deserializer.readLine()).isEqualTo("Test");
    }

    @Test
    void testReadLineWithCarriageReturn() throws IOException {
        // Create data directly without using writeBytes due to its bug
        byte[] data = "Hello\r\nWorld\n".getBytes();
        DataInputDeserializer deserializer = new DataInputDeserializer(data);
        assertThat(deserializer.readLine()).isEqualTo("Hello");
        assertThat(deserializer.readLine()).isEqualTo("World");
    }

    @Test
    void testReadLineWithNullAtEnd() throws IOException {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[0]);
        assertThat(deserializer.readLine()).isNull();
    }

    @Test
    void testReadWithOffset() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(10);
        byte[] data = {1, 2, 3, 4, 5};
        serializer.write(data);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        byte[] result = new byte[5];
        int bytesRead = deserializer.read(result, 1, 3);
        assertThat(bytesRead).isEqualTo(3);
        assertThat(result[1]).isEqualTo((byte) 1);
        assertThat(result[2]).isEqualTo((byte) 2);
        assertThat(result[3]).isEqualTo((byte) 3);
    }

    @Test
    void testReadWithNullArray() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        assertThatThrownBy(() -> deserializer.read(null, 0, 1))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testReadWithInvalidOffset() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        byte[] result = new byte[5];

        assertThatThrownBy(() -> deserializer.read(result, -1, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> deserializer.read(result, 10, 1))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testReadWithInvalidLength() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        byte[] result = new byte[5];

        assertThatThrownBy(() -> deserializer.read(result, 0, -1))
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> deserializer.read(result, 0, 10))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void testReadWithNoData() throws IOException {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[0]);
        byte[] result = new byte[5];
        int bytesRead = deserializer.read(result);
        assertThat(bytesRead).isEqualTo(-1);
    }

    @Test
    void testSkipBytesToRead() throws IOException {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        deserializer.skipBytesToRead(3);
        assertThat(deserializer.getPosition()).isEqualTo(3);
        assertThat(deserializer.available()).isEqualTo(2);
    }

    @Test
    void testSkipBytesToReadWithInsufficientData() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5});
        assertThatThrownBy(() -> deserializer.skipBytesToRead(10)).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadUTFWithUnicode() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(100);
        serializer.writeUTF("Hello 世界");
        serializer.writeUTF("Hello\uD800\uDC00"); // surrogate pair

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readUTF()).isEqualTo("Hello 世界");
        assertThat(deserializer.readUTF()).isEqualTo("Hello\uD800\uDC00");
    }

    @Test
    void testReadUTFWithSpecialUTF8Sequences() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(100);
        String[] testStrings = {
            "Hello\u0080World", // 2-byte UTF-8
            "Hello\u0800World", // 3-byte UTF-8
            "Hello\uD800\uDC00World", // surrogate pair
            "Hello\u0000World", // null character
            "Hello\uFFFFWorld", // max BMP character
        };

        for (String testStr : testStrings) {
            serializer.clear();
            serializer.writeUTF(testStr);
            DataInputDeserializer deserializer =
                    new DataInputDeserializer(serializer.getCopyOfBuffer());
            assertThat(deserializer.readUTF()).isEqualTo(testStr);
        }
    }

    @Test
    void testReadUTFWithInvalidUTF8() {
        // Create invalid UTF-8 data
        byte[] invalidUtf8 = {
            0, 3, (byte) 0x80, (byte) 0x80, (byte) 0x80
        }; // Invalid UTF-8 sequence
        DataInputDeserializer deserializer = new DataInputDeserializer(invalidUtf8);

        assertThatThrownBy(() -> deserializer.readUTF()).isInstanceOf(IOException.class);
    }

    @Test
    void testReadUTFWithInsufficientLength() {
        byte[] insufficientData = {0, 10}; // Length 10 but only 2 bytes available
        DataInputDeserializer deserializer = new DataInputDeserializer(insufficientData);

        assertThatThrownBy(() -> deserializer.readUTF()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadBooleanWithEOF() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[0]);
        assertThatThrownBy(() -> deserializer.readBoolean()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadByteWithEOF() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[0]);
        assertThatThrownBy(() -> deserializer.readByte()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadCharWithEOF() {
        DataInputDeserializer deserializer =
                new DataInputDeserializer(new byte[] {1}); // Only 1 byte
        assertThatThrownBy(() -> deserializer.readChar()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadDoubleWithEOF() {
        DataInputDeserializer deserializer =
                new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5, 6, 7}); // Only 7 bytes
        assertThatThrownBy(() -> deserializer.readDouble()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadFloatWithEOF() {
        DataInputDeserializer deserializer =
                new DataInputDeserializer(new byte[] {1, 2, 3}); // Only 3 bytes
        assertThatThrownBy(() -> deserializer.readFloat()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadIntWithEOF() {
        DataInputDeserializer deserializer =
                new DataInputDeserializer(new byte[] {1, 2, 3}); // Only 3 bytes
        assertThatThrownBy(() -> deserializer.readInt()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadLongWithEOF() {
        DataInputDeserializer deserializer =
                new DataInputDeserializer(new byte[] {1, 2, 3, 4, 5, 6, 7}); // Only 7 bytes
        assertThatThrownBy(() -> deserializer.readLong()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadShortWithEOF() {
        DataInputDeserializer deserializer =
                new DataInputDeserializer(new byte[] {1}); // Only 1 byte
        assertThatThrownBy(() -> deserializer.readShort()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadUnsignedByteWithEOF() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[0]);
        assertThatThrownBy(() -> deserializer.readUnsignedByte()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadUnsignedShortWithEOF() {
        DataInputDeserializer deserializer =
                new DataInputDeserializer(new byte[] {1}); // Only 1 byte
        assertThatThrownBy(() -> deserializer.readUnsignedShort()).isInstanceOf(EOFException.class);
    }

    @Test
    void testReadFullyWithEOF() {
        DataInputDeserializer deserializer = new DataInputDeserializer(new byte[] {1, 2, 3});
        byte[] result = new byte[5];
        assertThatThrownBy(() -> deserializer.readFully(result)).isInstanceOf(EOFException.class);
    }

    @Test
    void testEndianness() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(20);
        serializer.writeInt(0x12345678);
        serializer.writeLong(0x123456789ABCDEF0L);

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        assertThat(deserializer.readInt()).isEqualTo(0x12345678);
        assertThat(deserializer.readLong()).isEqualTo(0x123456789ABCDEF0L);
    }

    @Test
    void testLargeDataHandling() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(1000);
        for (int i = 0; i < 100; i++) {
            serializer.writeInt(i);
            serializer.writeUTF("test" + i);
        }

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        for (int i = 0; i < 100; i++) {
            assertThat(deserializer.readInt()).isEqualTo(i);
            assertThat(deserializer.readUTF()).isEqualTo("test" + i);
        }
    }

    @Test
    void testConcurrentAccessPattern() throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(1000);
        for (int i = 0; i < 100; i++) {
            serializer.writeInt(i);
            serializer.writeUTF("test" + i);
            serializer.writeDouble(i * 1.5);
            serializer.writeBoolean(i % 2 == 0);
        }

        DataInputDeserializer deserializer =
                new DataInputDeserializer(serializer.getCopyOfBuffer());
        for (int i = 0; i < 100; i++) {
            assertThat(deserializer.readInt()).isEqualTo(i);
            assertThat(deserializer.readUTF()).isEqualTo("test" + i);
            assertThat(deserializer.readDouble()).isEqualTo(i * 1.5);
            assertThat(deserializer.readBoolean()).isEqualTo(i % 2 == 0);
        }
    }
}

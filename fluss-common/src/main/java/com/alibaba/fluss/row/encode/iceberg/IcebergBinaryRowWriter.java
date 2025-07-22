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

import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.utils.UnsafeUtils;

import java.io.Serializable;
import java.util.Arrays;

import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;

/**
 * A writer to encode Fluss's {@link com.alibaba.fluss.row.InternalRow} using Iceberg's binary
 * encoding format.
 *
 * <p>The encoding logic is based on Iceberg's Conversions.toByteBuffer() implementation:
 * https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/types/Conversions.java
 *
 * <p>Key encoding principles from Iceberg's Conversions class:
 *
 * <ul>
 *   <li>All numeric types (int, long, float, double, timestamps) use LITTLE-ENDIAN byte order
 *   <li>Decimal types use BIG-ENDIAN byte order
 *   <li>Strings are encoded as UTF-8 bytes
 *   <li>Timestamps are stored as long values (microseconds since epoch)
 * </ul>
 *
 * <p>Note: This implementation uses Fluss's MemorySegment instead of ByteBuffer for performance,
 * but maintains byte-level compatibility with Iceberg's encoding.
 */
class IcebergBinaryRowWriter {

    private final int arity;
    private byte[] buffer;
    private MemorySegment segment;
    private int cursor;

    public IcebergBinaryRowWriter(int arity) {
        this.arity = arity;
        // Conservative initial size to avoid frequent resizing
        int initialSize = 8 + (arity * 8);
        setBuffer(new byte[initialSize]);
        reset();
    }

    public void reset() {
        this.cursor = 0;
        // Clear only the used portion for efficiency
        if (cursor > 0) {
            Arrays.fill(buffer, 0, Math.min(cursor, buffer.length), (byte) 0);
        }
    }

    public byte[] toBytes() {
        byte[] result = new byte[cursor];
        System.arraycopy(buffer, 0, result, 0, cursor);
        return result;
    }

    public void setNullAt(int pos) {
        // For Iceberg key encoding, null values should not occur
        // This is validated at the encoder level
        throw new UnsupportedOperationException(
                "Null values are not supported in Iceberg key encoding");
    }

    public void writeBoolean(boolean value) {
        ensureCapacity(1);
        UnsafeUtils.putBoolean(buffer, cursor, value);
        cursor += 1;
    }

    public void writeByte(byte value) {
        ensureCapacity(1);
        UnsafeUtils.putByte(buffer, cursor, value);
        cursor += 1;
    }

    public void writeShort(short value) {
        ensureCapacity(2);
        UnsafeUtils.putShort(buffer, cursor, value);
        cursor += 2;
    }

    public void writeInt(int value) {
        ensureCapacity(4);
        UnsafeUtils.putInt(buffer, cursor, value);
        cursor += 4;
    }

    public void writeLong(long value) {
        ensureCapacity(8);
        UnsafeUtils.putLong(buffer, cursor, value);
        cursor += 8;
    }

    public void writeFloat(float value) {
        ensureCapacity(4);
        UnsafeUtils.putFloat(buffer, cursor, value);
        cursor += 4;
    }

    public void writeDouble(double value) {
        ensureCapacity(8);
        UnsafeUtils.putDouble(buffer, cursor, value);
        cursor += 8;
    }

    public void writeString(BinaryString value) {
        // Convert to UTF-8 byte array
        byte[] bytes = BinaryString.encodeUTF8(value.toString());
        // Write length prefix followed by UTF-8 bytes
        writeInt(bytes.length); // 4-byte length prefix
        ensureCapacity(bytes.length); // Ensure space for actual string bytes
        segment.put(cursor, bytes, 0, bytes.length);
        cursor += bytes.length;
    }

    public void writeBytes(byte[] bytes) {
        // Write length prefix followed by binary data
        writeInt(bytes.length); // 4-byte length prefix
        ensureCapacity(bytes.length); // Ensure space for actual binary bytes
        segment.put(cursor, bytes, 0, bytes.length);
        cursor += bytes.length;
    }

    public void writeDecimal(Decimal value, int precision) {
        byte[] unscaled = value.toUnscaledBytes();
        writeBytes(unscaled); // Adds 4-byte length prefix before the actual bytes
    }

    private void ensureCapacity(int neededSize) {
        if (buffer.length < cursor + neededSize) {
            grow(cursor + neededSize);
        }
    }

    private void grow(int minCapacity) {
        int oldCapacity = buffer.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1); // 1.5x growth
        if (newCapacity < minCapacity) {
            newCapacity = minCapacity;
        }
        setBuffer(Arrays.copyOf(buffer, newCapacity));
    }

    private void setBuffer(byte[] buffer) {
        this.buffer = buffer;
        this.segment = MemorySegment.wrap(buffer);
    }

    /**
     * Creates an accessor for writing the elements of an iceberg binary row writer during runtime.
     *
     * @param fieldType the field type to write
     */
    public static FieldWriter createFieldWriter(DataType fieldType) {
        switch (fieldType.getTypeRoot()) {
            case INTEGER:
            case DATE:
                return (writer, value) -> writer.writeInt((int) value);

            case TIME_WITHOUT_TIME_ZONE:
                // Write time as microseconds long (milliseconds * 1000)
                return (writer, value) -> {
                    int millis = (int) value;
                    long micros = millis * 1000L;
                    writer.writeLong(micros);
                };

            case BIGINT:
                return (writer, value) -> writer.writeLong((long) value);
                // support for nanoseconds come check again after #1195 merge
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (writer, value) -> {
                    TimestampNtz ts = (TimestampNtz) value;
                    long micros = ts.getMillisecond() * 1000L + (ts.getNanoOfMillisecond() / 1000L);
                    writer.writeLong(micros);
                };

            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                return (writer, value) -> writer.writeDecimal((Decimal) value, decimalPrecision);

            case STRING:
            case CHAR:
                return (writer, value) -> writer.writeString((BinaryString) value);

            case BINARY:
            case BYTES:
                return (writer, value) -> writer.writeBytes((byte[]) value);

            default:
                throw new IllegalArgumentException(
                        "Unsupported type for Iceberg binary row writer: " + fieldType);
        }
    }

    /** Accessor for writing the elements of an iceberg binary row writer during runtime. */
    interface FieldWriter extends Serializable {
        void writeField(IcebergBinaryRowWriter writer, Object value);
    }
}

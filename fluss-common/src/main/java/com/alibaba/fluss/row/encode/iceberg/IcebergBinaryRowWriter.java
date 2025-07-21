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
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.utils.UnsafeUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;

/**
 * A writer to encode Fluss's {@link InternalRow} using Iceberg's binary encoding format.
 *
 * <p>This implementation follows Iceberg's binary encoding specification for partition and bucket
 * keys. Reference: https://iceberg.apache.org/spec/#partition-transforms
 *
 * <p>The encoding logic is based on Iceberg's Conversions.toByteBuffer() implementation:
 * https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/types/Conversions.java
 *
 * <p>Key encoding principles from Iceberg's Conversions class:
 *
 * <ul>
 *   <li>All numeric types (int, long, float, double, timestamps) use LITTLE-ENDIAN byte order
 *   <li>Decimal and UUID types use BIG-ENDIAN byte order
 *   <li>NO length prefix for any type - the buffer size determines the length
 *   <li>Strings are encoded as UTF-8 bytes without length prefix
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
        // Based on Iceberg's Conversions.toByteBuffer() for STRING type:
        // Strings are encoded as UTF-8 bytes without length prefix
        // Reference:https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/types/Conversions.java
        byte[] bytes = BinaryString.encodeUTF8(value.toString());
        ensureCapacity(bytes.length);
        segment.put(cursor, bytes, 0, bytes.length);
        cursor += bytes.length;
    }

    void writeBytes(byte[] bytes) {
        // Based on Iceberg's Conversions.toByteBuffer() for BINARY type:
        // Binary data is stored directly without length prefix
        ensureCapacity(bytes.length);
        segment.put(cursor, bytes, 0, bytes.length);
        cursor += bytes.length;
    }

    public void writeDecimal(Decimal value, int precision) {
        // Iceberg stores decimals as unscaled big-endian byte arrays
        // Reference:https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/types/Conversions.java
        byte[] unscaledBytes = value.toUnscaledBytes();

        // For Iceberg, decimals are stored with fixed byte lengths based on precision
        int requiredBytes = getIcebergDecimalBytes(precision);

        byte[] icebergBytes = new byte[requiredBytes];

        // Convert to big-endian format with proper padding
        if (unscaledBytes.length <= requiredBytes) {
            // Pad with sign extension
            byte paddingByte =
                    (unscaledBytes.length > 0 && (unscaledBytes[0] & 0x80) != 0)
                            ? (byte) 0xFF
                            : (byte) 0x00;

            Arrays.fill(icebergBytes, 0, requiredBytes - unscaledBytes.length, paddingByte);
            System.arraycopy(
                    unscaledBytes,
                    0,
                    icebergBytes,
                    requiredBytes - unscaledBytes.length,
                    unscaledBytes.length);
        } else {
            // Truncate if too large (should not happen with proper validation)
            System.arraycopy(unscaledBytes, 0, icebergBytes, 0, requiredBytes);
        }

        writeBytes(icebergBytes);
    }

    public void writeTimestampNtz(TimestampNtz value, int precision) {
        // Iceberg stores timestamps as microseconds since epoch
        // Reference:
        // https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/types/Conversions.java
        long micros = value.getMillisecond() * 1000L + (value.getNanoOfMillisecond() / 1000L);
        writeLong(micros);
    }

    public void writeTimestampLtz(TimestampLtz value, int precision) {
        // Iceberg stores timestamptz as microseconds since epoch in UTC
        // Reference:
        // https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/types/Conversions.java
        long epochMillis = value.getEpochMillisecond();
        int nanoOfMilli = value.getNanoOfMillisecond();
        long totalMicros = epochMillis * 1000L + (nanoOfMilli / 1000L);
        writeLong(totalMicros);
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
     * Returns the number of bytes required to store a decimal with the given precision. Based on
     * Iceberg's decimal storage specification.
     *
     * @param precision the decimal precision
     * @return number of bytes required
     */
    private static int getIcebergDecimalBytes(int precision) {
        // Reference:
        // https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/types/Conversions.java
        if (precision <= 9) {
            return 4; // Can fit in 4 bytes
        } else if (precision <= 18) {
            return 8; // Can fit in 8 bytes
        } else {
            return 16; // Requires 16 bytes
        }
    }

    public ByteBuffer toByteBuffer() {
        // Create a ByteBuffer that wraps only the valid data
        return ByteBuffer.wrap(buffer, 0, cursor).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Creates an accessor for writing the elements of an iceberg binary row writer during runtime.
     *
     * @param fieldType the field type to write
     */
    public static FieldWriter createFieldWriter(DataType fieldType) {
        final FieldWriter fieldWriter;

        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case STRING:
                fieldWriter = (writer, value) -> writer.writeString((BinaryString) value);
                break;
            case BOOLEAN:
                fieldWriter = (writer, value) -> writer.writeBoolean((boolean) value);
                break;
            case BINARY:
            case BYTES:
                fieldWriter = (writer, value) -> writer.writeBytes((byte[]) value);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, value) -> writer.writeDecimal((Decimal) value, decimalPrecision);
                break;
            case TINYINT:
                fieldWriter = (writer, value) -> writer.writeByte((byte) value);
                break;
            case SMALLINT:
                fieldWriter = (writer, value) -> writer.writeShort((short) value);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                // Fluss stores TIME as int (milliseconds), Iceberg expects long (microseconds)
                fieldWriter =
                        (writer, value) -> {
                            int millis = (int) value;
                            long micros = millis * 1000L;
                            writer.writeLong(micros);
                        };
                break;
            case BIGINT:
                fieldWriter = (writer, value) -> writer.writeLong((long) value);
                break;
            case FLOAT:
                fieldWriter = (writer, value) -> writer.writeFloat((float) value);
                break;
            case DOUBLE:
                fieldWriter = (writer, value) -> writer.writeDouble((double) value);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, value) ->
                                writer.writeTimestampNtz(
                                        (TimestampNtz) value, timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, value) ->
                                writer.writeTimestampLtz(
                                        (TimestampLtz) value, timestampLtzPrecision);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported type for Iceberg binary row writer: " + fieldType);
        }

        return fieldWriter;
    }

    /** Accessor for writing the elements of an iceberg binary row writer during runtime. */
    interface FieldWriter extends Serializable {
        void writeField(IcebergBinaryRowWriter writer, Object value);
    }
}

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
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;

/* This file is based on source code of Apache Iceberg Project (https://iceberg.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A writer to encode Fluss's {@link InternalRow} using Iceberg's binary encoding way.
 *
 * <p>The logic follows Iceberg's binary encoding format for row data using MemorySegment.
 */
class IcebergBinaryRowWriter {

    private final int arity;
    private byte[] buffer;
    private MemorySegment segment;
    private int cursor;

    public IcebergBinaryRowWriter(int arity) {
        // Verify MemorySegment uses little-endian for Iceberg compatibility
        if (!MemorySegment.LITTLE_ENDIAN) {
            throw new IllegalStateException(
                    "MemorySegment must use little-endian byte order for Iceberg compatibility");
        }

        this.arity = arity;
        // Initial buffer size estimation
        int initialSize = calculateInitialSize(arity);
        setBuffer(new byte[initialSize]);
        reset();
    }

    public void reset() {
        this.cursor = 0;
        // Clear buffer
        Arrays.fill(buffer, 0, Math.min(buffer.length, calculateInitialSize(arity)), (byte) 0);
    }

    public byte[] toBytes() {
        byte[] result = new byte[cursor];
        System.arraycopy(buffer, 0, result, 0, cursor);
        return result;
    }

    public void setNullAt(int pos) {
        // Iceberg handles nulls with a null marker byte
        ensureCapacity(1);
        segment.put(cursor, (byte) 0x00); // Null marker
        cursor += 1;
    }

    public void writeChangeType(ChangeType kind) {
        // TODO DISCUSS WITH YUXIA
        // Iceberg doesn't store ChangeType in binary format like Paimon
        // This is handled at the metadata/manifest level
        // We skip this for Iceberg binary encoding
    }

    public void writeBoolean(int pos, boolean value) {
        ensureCapacity(1);
        segment.put(cursor, value ? (byte) 1 : (byte) 0);
        cursor += 1;
    }

    public void writeByte(int pos, byte value) {
        ensureCapacity(1);
        segment.put(cursor, value);
        cursor += 1;
    }

    public void writeShort(int pos, short value) {
        ensureCapacity(2);
        segment.putShort(cursor, value);
        cursor += 2;
    }

    public void writeInt(int pos, int value) {
        ensureCapacity(4);
        segment.putInt(cursor, value);
        cursor += 4;
    }

    public void writeLong(int pos, long value) {
        ensureCapacity(8);
        segment.putLong(cursor, value);
        cursor += 8;
    }

    public void writeFloat(int pos, float value) {
        ensureCapacity(4);
        segment.putFloat(cursor, value);
        cursor += 4;
    }

    public void writeDouble(int pos, double value) {
        ensureCapacity(8);
        segment.putDouble(cursor, value);
        cursor += 8;
    }

    public void writeString(int pos, BinaryString input) {
        if (input == null) {
            setNullAt(pos);
            return;
        }

        byte[] bytes;
        if (input.getSegments() == null) {
            bytes = input.toString().getBytes(StandardCharsets.UTF_8);
        } else {
            int len = input.getSizeInBytes();
            bytes = new byte[len];
            extractFromFlussSegments(input, bytes);
        }
        writeBytes(pos, bytes);
    }

     void writeBytes(int pos, byte[] bytes) {
        if (bytes == null) {
            setNullAt(pos);
            return;
        }

        // Iceberg variable-length encoding: length prefix + data
        ensureCapacity(4 + bytes.length);
        segment.putInt(cursor, bytes.length);
        cursor += 4;
        segment.put(cursor, bytes, 0, bytes.length);
        cursor += bytes.length;
    }

    public void writeDecimal(int pos, Decimal value, int precision) {
        if (value == null) {
            setNullAt(pos);
            return;
        }

        // Iceberg stores decimals as big-endian byte arrays
        byte[] unscaledBytes = value.toUnscaledBytes();

        // Calculate required bytes based on precision (following Iceberg's rules)
        int requiredBytes = getIcebergDecimalBytes(precision);
        ensureCapacity(4 + requiredBytes); // length + data

        // Write length
        segment.putInt(cursor, requiredBytes);
        cursor += 4;

        // Pad or truncate to required size
        if (unscaledBytes.length <= requiredBytes) {
            // Pad with sign extension
            byte paddingByte = (unscaledBytes.length > 0 && (unscaledBytes[0] & 0x80) != 0) ?
                    (byte) 0xFF : (byte) 0x00;

            // Write padding
            for (int i = 0; i < requiredBytes - unscaledBytes.length; i++) {
                segment.put(cursor + i, paddingByte);
            }
            // Write actual bytes
            segment.put(cursor + (requiredBytes - unscaledBytes.length), unscaledBytes, 0, unscaledBytes.length);
        } else {
            // Truncate if too large
            segment.put(cursor, unscaledBytes, 0, requiredBytes);
        }
        cursor += requiredBytes;
    }

    public void writeTimestampNtz(int pos, TimestampNtz value, int precision) {
        if (value == null) {
            setNullAt(pos);
            return;
        }

        // Iceberg stores timestamps as microseconds since epoch
        ensureCapacity(8);
        long micros = value.getMillisecond() * 1000L + (value.getNanoOfMillisecond() / 1000L);
        segment.putLong(cursor, micros);
        cursor += 8;
    }

    public void writeTimestampLtz(int pos, TimestampLtz value, int precision) {
        if (value == null) {
            setNullAt(pos);
            return;
        }

        // Iceberg stores timestamptz as microseconds since epoch in UTC
        ensureCapacity(8);
        long epochMillis = value.getEpochMillisecond();
        int nanoOfMilli = value.getNanoOfMillisecond();
        long totalMicros = epochMillis * 1000L + (nanoOfMilli / 1000L);
        segment.putLong(cursor, totalMicros);
        cursor += 8;
    }

    private void ensureCapacity(int neededSize) {
        if (segment.size() < cursor + neededSize) {
            grow(cursor + neededSize);
        }
    }

    private void grow(int minCapacity) {
        int oldCapacity = segment.size();
        int newCapacity = Math.max(oldCapacity * 2, minCapacity);
        setBuffer(Arrays.copyOf(buffer, newCapacity));
    }

    private void setBuffer(byte[] buffer) {
        this.buffer = buffer;
        this.segment = MemorySegment.wrap(buffer);
    }

    private void extractFromFlussSegments(BinaryString input, byte[] target) {
        // Convert Fluss MemorySegments to byte array
        MemorySegment[] segments = input.getSegments();
        int offset = input.getOffset();
        int length = input.getSizeInBytes();

        if (segments.length == 1) {
            segments[0].get(offset, target, 0, length);
        } else {
            copyFromFlussSegments(segments, offset, target, 0, length);
        }
    }

    private void copyFromFlussSegments(MemorySegment[] segments, int offset, byte[] target, int targetOffset, int length) {
        int remaining = length;
        int currentOffset = offset;
        int currentTargetOffset = targetOffset;

        for (MemorySegment segment : segments) {
            if (remaining <= 0) {
                break;
            }

            int segmentSize = segment.size();
            if (currentOffset >= segmentSize) {
                currentOffset -= segmentSize;
                continue;
            }

            int availableInSegment = segmentSize - currentOffset;
            int toCopy = Math.min(remaining, availableInSegment);

            // Copy from MemorySegment to byte array
            segment.get(currentOffset, target, currentTargetOffset, toCopy);

            remaining -= toCopy;
            currentTargetOffset += toCopy;
            currentOffset = 0;
        }
    }

    private static int calculateInitialSize(int arity) {
        // Conservative estimate for Iceberg format
        return 64 + (arity * 16); // Allow for variable length fields
    }

    private static int getIcebergDecimalBytes(int precision) {
        // Iceberg's decimal storage requirements
        if (precision <= 9) return 4;
        if (precision <= 18) return 8;
        return 16;
    }

    /**
     * Creates an accessor for writing the elements of an iceberg binary row writer during runtime.
     *
     * @param fieldType the field type of the indexed row
     */
    public static FieldWriter createFieldWriter(DataType fieldType) {
        final FieldWriter fieldWriter;
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case STRING:
                fieldWriter = (writer, pos, value) -> writer.writeString(pos, (BinaryString) value);
                break;
            case BOOLEAN:
                fieldWriter = (writer, pos, value) -> writer.writeBoolean(pos, (boolean) value);
                break;
            case BINARY:
            case BYTES:
                fieldWriter = (writer, pos, value) -> writer.writeBytes(pos, (byte[]) value);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeDecimal(pos, (Decimal) value, decimalPrecision);
                break;
            case TINYINT:
                fieldWriter = (writer, pos, value) -> writer.writeByte(pos, (byte) value);
                break;
            case SMALLINT:
                fieldWriter = (writer, pos, value) -> writer.writeShort(pos, (short) value);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldWriter = (writer, pos, value) -> writer.writeInt(pos, (int) value);
                break;
            case BIGINT:
                fieldWriter = (writer, pos, value) -> writer.writeLong(pos, (long) value);
                break;
            case FLOAT:
                fieldWriter = (writer, pos, value) -> writer.writeFloat(pos, (float) value);
                break;
            case DOUBLE:
                fieldWriter = (writer, pos, value) -> writer.writeDouble(pos, (double) value);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeTimestampNtz(
                                        pos, (TimestampNtz) value, timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeTimestampLtz(
                                        pos, (TimestampLtz) value, timestampLtzPrecision);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported type for Iceberg binary row writer: " + fieldType);
        }

        if (!fieldType.isNullable()) {
            return fieldWriter;
        }

        return (writer, pos, value) -> {
            if (value == null) {
                writer.setNullAt(pos);
            } else {
                fieldWriter.writeField(writer, pos, value);
            }
        };
    }

    /** Accessor for writing the elements of an iceberg binary row writer during runtime. */
    interface FieldWriter extends Serializable {
        void writeField(IcebergBinaryRowWriter writer, int pos, Object value);
    }
}
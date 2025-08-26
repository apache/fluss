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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentWritable;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.UnsafeUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/**
 * Writer for {@link CompactedRow}.
 *
 * <p>Serializes fields in an encoded way. In order to save more space, int and long are written in
 * variable length (including length). The average size of integers is:
 *
 * <ul>
 *   <li>Unsigned Int: 1-5 bytes, average 4.87 bytes.
 *   <li>Signed Int: 1-5 bytes, average 4.93 bytes.
 *   <li>Unsigned Long: 1-10 bytes, average 8.99 bytes.
 *   <li>Signed Long: 1-10 bytes, average 9.49 bytes.
 * </ul>
 *
 * <p>A variable-length quantity (VLQ) is a universal code that uses an arbitrary number of binary
 * octets (eight-bit bytes) to represent an arbitrarily large integer. A VLQ is essentially a
 * base-128 representation of an unsigned integer with the addition of the eighth bit to mark
 * continuation of bytes. It's very efficient for small positive integers, but for negative
 * integers, always stored as five/ten bytes. Number of encoded bytes:
 *
 * <ul>
 *   <li>0 ~ 127: 1 byte.
 *   <li>128 ~ 16383: 2 bytes.
 *   <li>16384 ~ 2097151: 3 bytes.
 *   <li>2097152 ~ 268435455: 4 bytes.
 * </ul>
 *
 * <p>Compared with zigzag: Zigzag in order to optimize the negative integer, the storage space of
 * the positive integer is doubled. We assume that the probability of general integers being
 * positive is higher, so sacrifice the negative number to promote the positive number.
 */
public class CompactedRowWriter {

    private final int headerSizeInBytes;

    private static final int MAX_INT_SIZE = 5;
    private static final int MAX_LONG_SIZE = 10;
    private byte[] buffer;
    private int position;
    private MemorySegment segment;

    public CompactedRowWriter(int fieldCount) {
        this.headerSizeInBytes = CompactedRow.calculateBitSetWidthInBytes(fieldCount);
        this.position = headerSizeInBytes;
        setBuffer(new byte[Math.max(64, headerSizeInBytes)]);
    }

    // ----------------------- internal methods -------------------------------

    public void reset() {
        this.position = headerSizeInBytes;
        for (int i = 0; i < headerSizeInBytes; i++) {
            buffer[i] = 0;
        }
    }

    private void setBuffer(byte[] buffer) {
        this.buffer = buffer;
        this.segment = MemorySegment.wrap(buffer);
    }

    public MemorySegment segment() {
        return segment;
    }

    public int position() {
        return position;
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[position];
        System.arraycopy(buffer, 0, bytes, 0, position);
        return bytes;
    }

    @VisibleForTesting
    byte[] buffer() {
        return buffer;
    }

    private void write(byte[] value, int off, int len) {
        ensureCapacity(len);
        System.arraycopy(value, off, buffer, position, len);
        position += len;
    }

    public void setNullAt(int pos) {
        UnsafeUtils.bitSet(buffer, 0, pos);
    }

    public void writeByte(byte value) {
        ensureCapacity(1);
        UnsafeUtils.putByte(buffer, position++, value);
    }

    public void writeString(BinaryString value) {
        if (value.getSegments() == null) {
            writeString(value.toString());
        } else {
            writeSegments(value.getSegments(), value.getOffset(), value.getSizeInBytes());
        }
    }

    private void writeSegments(MemorySegment[] segments, int off, int len) {
        writeInt(len);
        if (len + off <= segments[0].size()) {
            write(segments[0], off, len);
        } else {
            write(segments, off, len);
        }
    }

    private void write(MemorySegment[] segments, int off, int len) {
        ensureCapacity(len);
        int toWrite = len;
        int fromOffset = off;
        int toOffset = this.position;
        for (MemorySegment sourceSegment : segments) {
            int remain = sourceSegment.size() - fromOffset;
            if (remain > 0) {
                int localToWrite = Math.min(remain, toWrite);
                sourceSegment.get(fromOffset, buffer, toOffset, localToWrite);
                toWrite -= localToWrite;
                toOffset += localToWrite;
                fromOffset = 0;
            } else {
                fromOffset -= sourceSegment.size();
            }
        }
        this.position += len;
    }

    public void writeString(String string) {
        byte[] bytes = BinaryString.encodeUTF8(string);
        writeBytes(bytes);
    }

    private void writeBoolean(boolean value) {
        ensureCapacity(1);
        UnsafeUtils.putBoolean(buffer, position++, value);
    }

    public void writeBytes(byte[] value) {
        writeInt(value.length);
        write(value, 0, value.length);
    }

    public void writeDecimal(Decimal value, int precision) {
        if (Decimal.isCompact(precision)) {
            writeLong(value.toUnscaledLong());
        } else {
            writeBytes(value.toUnscaledBytes());
        }
    }

    public void writeShort(short value) {
        ensureCapacity(2);
        UnsafeUtils.putShort(buffer, position, value);
        position += 2;
    }

    public void writeInt(int value) {
        ensureCapacity(MAX_INT_SIZE);
        // UNSAFE + Loop unrolling faster.
        if ((value & ~0x7F) == 0) {
            UnsafeUtils.putByte(buffer, position++, (byte) value);
            return;
        }
        UnsafeUtils.putByte(buffer, position++, (byte) (value | 0x80));
        value >>>= 7;
        if ((value & ~0x7F) == 0) {
            UnsafeUtils.putByte(buffer, position++, (byte) value);
            return;
        }
        UnsafeUtils.putByte(buffer, position++, (byte) (value | 0x80));
        value >>>= 7;
        if ((value & ~0x7F) == 0) {
            UnsafeUtils.putByte(buffer, position++, (byte) value);
            return;
        }
        UnsafeUtils.putByte(buffer, position++, (byte) (value | 0x80));
        value >>>= 7;
        if ((value & ~0x7F) == 0) {
            UnsafeUtils.putByte(buffer, position++, (byte) value);
            return;
        }
        UnsafeUtils.putByte(buffer, position++, (byte) (value | 0x80));
        value >>>= 7;
        UnsafeUtils.putByte(buffer, position++, (byte) value);
    }

    public void writeLong(long value) {
        ensureCapacity(MAX_LONG_SIZE);
        while (true) {
            if ((value & ~0x7FL) == 0) {
                UnsafeUtils.putByte(buffer, position++, (byte) value);
                return;
            } else {
                UnsafeUtils.putByte(buffer, position++, (byte) (((int) value & 0x7F) | 0x80));
                value >>>= 7;
            }
        }
    }

    private void writeFloat(float value) {
        ensureCapacity(4);
        UnsafeUtils.putFloat(buffer, position, value);
        position += 4;
    }

    private void writeDouble(double value) {
        ensureCapacity(8);
        UnsafeUtils.putDouble(buffer, position, value);
        position += 8;
    }

    private void writeTimestampNtz(TimestampNtz value, int precision) {
        if (TimestampNtz.isCompact(precision)) {
            writeLong(value.getMillisecond());
        } else {
            writeLong(value.getMillisecond());
            writeInt(value.getNanoOfMillisecond());
        }
    }

    private void writeTimestampLtz(TimestampLtz value, int precision) {
        if (TimestampLtz.isCompact(precision)) {
            writeLong(value.getEpochMillisecond());
        } else {
            writeLong(value.getEpochMillisecond());
            writeInt(value.getNanoOfMillisecond());
        }
    }

    private void ensureCapacity(int size) {
        if (buffer.length - position < size) {
            grow(size);
        }
    }

    private void grow(int minCapacityAdd) {
        int newLen = Math.max(this.buffer.length * 2, this.buffer.length + minCapacityAdd);
        setBuffer(Arrays.copyOf(this.buffer, newLen));
    }

    private void write(MemorySegment segment, int off, int len) {
        ensureCapacity(len);
        segment.get(off, this.buffer, this.position, len);
        this.position += len;
    }

    // ------------------------------------------------------------------------------------------

    public static void serializeCompactedRow(CompactedRow row, OutputView target)
            throws IOException {
        int sizeInBytes = row.getSizeInBytes();
        if (target instanceof MemorySegmentWritable) {
            ((MemorySegmentWritable) target).write(row.getSegment(), row.getOffset(), sizeInBytes);
        } else {
            byte[] bytes = BinarySegmentUtils.allocateReuseBytes(sizeInBytes);
            row.getSegment().get(row.getOffset(), bytes, 0, sizeInBytes);
            target.write(bytes, 0, sizeInBytes);
        }
    }

    /**
     * Creates an accessor for writing the elements of an indexed row writer during runtime.
     *
     * @param fieldType the field type of the indexed row
     */
    public static FieldWriter createFieldWriter(DataType fieldType) {
        final FieldWriter fieldWriter;
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case STRING:
                fieldWriter = (writer, pos, value) -> writer.writeString((BinaryString) value);
                break;
            case BOOLEAN:
                fieldWriter = (writer, pos, value) -> writer.writeBoolean((boolean) value);
                break;
            case BINARY:
            case BYTES:
                fieldWriter = (writer, pos, value) -> writer.writeBytes((byte[]) value);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeDecimal((Decimal) value, decimalPrecision);
                break;
            case TINYINT:
                fieldWriter = (writer, pos, value) -> writer.writeByte((byte) value);
                break;
            case SMALLINT:
                fieldWriter = (writer, pos, value) -> writer.writeShort((short) value);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldWriter = (writer, pos, value) -> writer.writeInt((int) value);
                break;
            case BIGINT:
                fieldWriter = (writer, pos, value) -> writer.writeLong((long) value);
                break;
            case FLOAT:
                fieldWriter = (writer, pos, value) -> writer.writeFloat((float) value);
                break;
            case DOUBLE:
                fieldWriter = (writer, pos, value) -> writer.writeDouble((double) value);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeTimestampNtz(
                                        (TimestampNtz) value, timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                fieldWriter =
                        (writer, pos, value) ->
                                writer.writeTimestampLtz(
                                        (TimestampLtz) value, timestampLtzPrecision);
                break;
            default:
                throw new IllegalArgumentException("Unsupported type for IndexedRow: " + fieldType);
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

    /** Accessor for writing the elements of an compacted row writer during runtime. */
    public interface FieldWriter extends Serializable {
        void writeField(CompactedRowWriter writer, int pos, Object value);
    }
}

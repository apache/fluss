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

package org.apache.fluss.row.encode.paimon;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A writer to encode Fluss's {@link InternalRow} using Paimon's BinaryRow encoding way.
 *
 * <p>The logic is almost copied from Paimon's BinaryRowWriter.
 */
class PaimonBinaryRowWriter {

    private static final int HEADER_SIZE_IN_BITS = 8;
    private static final int MAX_FIX_PART_DATA_SIZE = 7;

    private final int nullBitsSizeInBytes;
    private final int fixedSize;
    private byte[] buffer;
    private MemorySegment segment;

    private int cursor;

    public PaimonBinaryRowWriter(int arity) {
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
        this.fixedSize = getFixedLengthPartSize(nullBitsSizeInBytes, arity);
        this.cursor = fixedSize;
        setBuffer(new byte[fixedSize]);
    }

    public void reset() {
        this.cursor = fixedSize;
        for (int i = 0; i < nullBitsSizeInBytes; i += 8) {
            segment.putLong(i, 0L);
        }
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[cursor];
        System.arraycopy(buffer, 0, bytes, 0, cursor);
        return bytes;
    }

    public void setNullAt(int pos) {
        setNullBit(pos);
        segment.putLong(getFieldOffset(pos), 0L);
    }

    private void setNullBit(int pos) {
        BinarySegmentUtils.bitSet(segment, 0, pos + HEADER_SIZE_IN_BITS);
    }

    public void writeChangeType(ChangeType kind) {
        // convert Fluss changeType to Paimon rowKind byte value
        byte paimonRowKindByte;
        switch (kind) {
            case APPEND_ONLY:
            case INSERT:
                paimonRowKindByte = 0;
                break;
            case UPDATE_BEFORE:
                paimonRowKindByte = 1;
                break;
            case UPDATE_AFTER:
                paimonRowKindByte = 2;
                break;
            case DELETE:
                paimonRowKindByte = 3;
                break;
            default:
                throw new IllegalArgumentException("Unsupported change type: " + kind);
        }
        segment.put(0, paimonRowKindByte);
    }

    public void writeBoolean(int pos, boolean value) {
        segment.putBoolean(getFieldOffset(pos), value);
    }

    public void writeByte(int pos, byte value) {
        segment.put(getFieldOffset(pos), value);
    }

    public void writeShort(int pos, short value) {
        segment.putShort(getFieldOffset(pos), value);
    }

    public void writeInt(int pos, int value) {
        segment.putInt(getFieldOffset(pos), value);
    }

    public void writeLong(int pos, long value) {
        segment.putLong(getFieldOffset(pos), value);
    }

    public void writeFloat(int pos, float value) {
        segment.putFloat(getFieldOffset(pos), value);
    }

    public void writeDouble(int pos, double value) {
        segment.putDouble(getFieldOffset(pos), value);
    }

    public void writeString(int pos, BinaryString input) {
        if (input.getSegments() == null) {
            String javaObject = input.toString();
            writeBytes(pos, javaObject.getBytes(StandardCharsets.UTF_8));
        } else {
            int len = input.getSizeInBytes();
            if (len <= 7) {
                byte[] bytes = BinarySegmentUtils.allocateReuseBytes(len);
                BinarySegmentUtils.copyToBytes(
                        input.getSegments(), input.getOffset(), bytes, 0, len);
                writeBytesToFixLenPart(buffer, getFieldOffset(pos), bytes, len);
            } else {
                writeSegmentsToVarLenPart(pos, input.getSegments(), input.getOffset(), len);
            }
        }
    }

    private void writeBytes(int pos, byte[] bytes) {
        int len = bytes.length;
        if (len <= MAX_FIX_PART_DATA_SIZE) {
            writeBytesToFixLenPart(buffer, getFieldOffset(pos), bytes, len);
        } else {
            writeBytesToVarLenPart(pos, bytes, len);
        }
    }

    public void writeDecimal(int pos, Decimal value, int precision) {
        assert value == null || (value.precision() == precision);
        if (Decimal.isCompact(precision)) {
            assert value != null;
            writeLong(pos, value.toUnscaledLong());
        } else {
            // grow the global buffer before writing data.
            ensureCapacity(16);

            // zero-out the bytes
            segment.putLong(cursor, 0L);
            segment.putLong(cursor + 8, 0L);

            // Make sure Decimal object has the same scale as DecimalType.
            // Note that we may pass in null Decimal object to set null for it.
            if (value == null) {
                setNullBit(pos);
                // keep the offset for future update
                setOffsetAndSize(pos, cursor, 0);
            } else {
                final byte[] bytes = value.toUnscaledBytes();
                assert bytes.length <= 16;

                // Write the bytes to the variable length portion.
                segment.put(cursor, bytes, 0, bytes.length);
                setOffsetAndSize(pos, cursor, bytes.length);
            }

            // move the cursor forward.
            cursor += 16;
        }
    }

    public void writeTimestampNtz(int pos, TimestampNtz value, int precision) {
        if (TimestampNtz.isCompact(precision)) {
            writeLong(pos, value.getMillisecond());
        } else {
            // store the nanoOfMillisecond in fixed-length part as offset and nanoOfMillisecond
            ensureCapacity(8);
            if (value == null) {
                setNullBit(pos);
                // zero-out the bytes
                segment.putLong(cursor, 0L);
                setOffsetAndSize(pos, cursor, 0);
            } else {
                segment.putLong(cursor, value.getMillisecond());
                setOffsetAndSize(pos, cursor, value.getNanoOfMillisecond());
            }
            cursor += 8;
        }
    }

    public void writeTimestampLtz(int pos, TimestampLtz value, int precision) {
        if (TimestampLtz.isCompact(precision)) {
            writeLong(pos, value.getEpochMillisecond());
        } else {
            // store the nanoOfMillisecond in fixed-length part as offset and nanoOfMillisecond
            ensureCapacity(8);
            if (value == null) {
                setNullBit(pos);
                // zero-out the bytes
                segment.putLong(cursor, 0L);
                setOffsetAndSize(pos, cursor, 0);
            } else {
                segment.putLong(cursor, value.getEpochMillisecond());
                setOffsetAndSize(pos, cursor, value.getNanoOfMillisecond());
            }
        }
    }

    protected void zeroOutPaddingBytes(int numBytes) {
        if ((numBytes & 0x07) > 0) {
            segment.putLong(cursor + ((numBytes >> 3) << 3), 0L);
        }
    }

    protected void ensureCapacity(int neededSize) {
        final int length = cursor + neededSize;
        if (segment.size() < length) {
            grow(length);
        }
    }

    private void writeSegmentsToVarLenPart(
            int pos, MemorySegment[] segments, int offset, int size) {
        final int roundedSize = roundNumberOfBytesToNearestWord(size);

        // grow the global buffer before writing data.
        ensureCapacity(roundedSize);

        zeroOutPaddingBytes(size);

        if (segments.length == 1) {
            segments[0].copyTo(offset, segment, cursor, size);
        } else {
            writeMultiSegmentsToVarLenPart(segments, offset, size);
        }

        setOffsetAndSize(pos, cursor, size);

        // move the cursor forward.
        cursor += roundedSize;
    }

    private void writeMultiSegmentsToVarLenPart(MemorySegment[] segments, int offset, int size) {
        // Write the bytes to the variable length portion.
        int needCopy = size;
        int fromOffset = offset;
        int toOffset = cursor;
        for (MemorySegment sourceSegment : segments) {
            int remain = sourceSegment.size() - fromOffset;
            if (remain > 0) {
                int copySize = Math.min(remain, needCopy);
                sourceSegment.copyTo(fromOffset, segment, toOffset, copySize);
                needCopy -= copySize;
                toOffset += copySize;
                fromOffset = 0;
            } else {
                fromOffset -= sourceSegment.size();
            }
        }
    }

    private void writeBytesToVarLenPart(int pos, byte[] bytes, int len) {
        final int roundedSize = roundNumberOfBytesToNearestWord(len);

        // grow the global buffer before writing data.
        ensureCapacity(roundedSize);

        zeroOutPaddingBytes(len);

        // Write the bytes to the variable length portion.
        segment.put(cursor, bytes, 0, len);

        setOffsetAndSize(pos, cursor, len);

        // move the cursor forward.
        cursor += roundedSize;
    }

    private void grow(int minCapacity) {
        int oldCapacity = segment.size();
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        setBuffer(Arrays.copyOf(buffer, newCapacity));
    }

    protected static int roundNumberOfBytesToNearestWord(int numBytes) {
        int remainder = numBytes & 0x07;
        if (remainder == 0) {
            return numBytes;
        } else {
            return numBytes + (8 - remainder);
        }
    }

    private void writeBytesToFixLenPart(byte[] buffer, int fieldOffset, byte[] bytes, int len) {
        long firstByte = len | 0x80; // first bit is 1, other bits is len
        long sevenBytes = 0L; // real data
        if (MemorySegment.LITTLE_ENDIAN) {
            for (int i = 0; i < len; i++) {
                sevenBytes |= ((0x00000000000000FFL & bytes[i]) << (i * 8L));
            }
        } else {
            for (int i = 0; i < len; i++) {
                sevenBytes |= ((0x00000000000000FFL & bytes[i]) << ((6 - i) * 8L));
            }
        }

        final long offsetAndSize = (firstByte << 56) | sevenBytes;

        segment.putLong(fieldOffset, offsetAndSize);
    }

    // ----------------------- internal methods -------------------------------

    private void setBuffer(byte[] buffer) {
        this.buffer = buffer;
        this.segment = MemorySegment.wrap(buffer);
    }

    private void setOffsetAndSize(int pos, int offset, long size) {
        final long offsetAndSize = ((long) offset << 32) | size;
        segment.putLong(getFieldOffset(pos), offsetAndSize);
    }

    private int getFieldOffset(int pos) {
        return nullBitsSizeInBytes + 8 * pos;
    }

    private static int getFixedLengthPartSize(int nullBitsSizeInBytes, int arity) {
        return nullBitsSizeInBytes + 8 * arity;
    }

    private static int calculateBitSetWidthInBytes(int arity) {
        return ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8;
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
                        "Unsupported type for Paimon BinaryRow writer: " + fieldType);
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

    /** Accessor for writing the elements of an paimon binary row writer during runtime. */
    interface FieldWriter extends Serializable {
        void writeField(PaimonBinaryRowWriter writer, int pos, Object value);
    }
}

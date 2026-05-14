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

package org.apache.fluss.row.encode.hudi;

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/**
 * A writer to encode Fluss's {@link org.apache.fluss.row.InternalRow} using Hudi's binary encoding
 * format.
 */
public class HudiBinaryRowWriter {

    private final int nullBitsSizeInBytes;
    private final int fixedSize;
    private byte[] buffer;
    private MemorySegment segment;
    private int cursor;

    public HudiBinaryRowWriter(int arity) {
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
        this.fixedSize = getFixedLengthPartSize(nullBitsSizeInBytes, arity);
        this.cursor = fixedSize;
        this.buffer = new byte[fixedSize];
        this.segment = MemorySegment.wrap(buffer);
    }

    public void reset() {
        this.cursor = this.fixedSize;

        for (int i = 0; i < this.nullBitsSizeInBytes; i += 8) {
            this.segment.putLong(i, 0L);
        }
    }

    public byte[] toBytes() {
        byte[] result = new byte[cursor];
        System.arraycopy(buffer, 0, result, 0, cursor);
        return result;
    }

    public void setNullAt(int pos) {
        setNullBit(pos);
        segment.putLong(getFieldOffset(pos), 0L);
    }

    public void setNullBit(int pos) {
        BinarySegmentUtils.bitSet(segment, 0, pos + 8);
    }

    public void writeRowKind(ChangeType kind) {
        // convert Fluss changeType to Hudi rowKind byte value
        byte hudiRowKindByte;
        switch (kind) {
            case APPEND_ONLY:
            case INSERT:
                hudiRowKindByte = 0;
                break;
            case UPDATE_BEFORE:
                hudiRowKindByte = 1;
                break;
            case UPDATE_AFTER:
                hudiRowKindByte = 2;
                break;
            case DELETE:
                hudiRowKindByte = 3;
                break;
            default:
                throw new IllegalArgumentException("Unsupported change type: " + kind);
        }
        segment.put(0, hudiRowKindByte);
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
                writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
            } else {
                writeSegmentsToVarLenPart(pos, input.getSegments(), input.getOffset(), len);
            }
        }
    }

    private void writeBytes(int pos, byte[] bytes) {
        int len = bytes.length;
        if (len <= 7) {
            writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
        } else {
            writeBytesToVarLenPart(pos, bytes, len);
        }
    }

    public void writeDecimal(int pos, Decimal value, int precision) {
        assert value == null || value.precision() == precision;

        if (Decimal.isCompact(precision)) {
            assert value != null;

            this.writeLong(pos, value.toUnscaledLong());
        } else {
            this.ensureCapacity(16);
            this.segment.putLong(this.cursor, 0L);
            this.segment.putLong(this.cursor + 8, 0L);
            if (value == null) {
                this.setNullBit(pos);
                this.setOffsetAndSize(pos, this.cursor, 0L);
            } else {
                byte[] bytes = value.toUnscaledBytes();

                assert bytes.length <= 16;

                this.segment.put(this.cursor, bytes, 0, bytes.length);
                this.setOffsetAndSize(pos, this.cursor, bytes.length);
            }

            this.cursor += 16;
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

    private void writeSegmentsToVarLenPart(
            int pos, MemorySegment[] segments, int offset, int size) {
        int roundedSize = roundNumberOfBytesToNearestWord(size);
        this.ensureCapacity(roundedSize);
        this.zeroOutPaddingBytes(size);
        if (segments.length == 1) {
            segments[0].copyTo(offset, this.segment, this.cursor, size);
        } else {
            this.writeMultiSegmentsToVarLenPart(segments, offset, size);
        }

        this.setOffsetAndSize(pos, this.cursor, size);
        this.cursor += roundedSize;
    }

    private void writeMultiSegmentsToVarLenPart(MemorySegment[] segments, int offset, int size) {
        int needCopy = size;
        int fromOffset = offset;
        int toOffset = this.cursor;

        for (MemorySegment sourceSegment : segments) {
            int remain = sourceSegment.size() - fromOffset;
            if (remain > 0) {
                int copySize = Math.min(remain, needCopy);
                sourceSegment.copyTo(fromOffset, this.segment, toOffset, copySize);
                needCopy -= copySize;
                toOffset += copySize;
                fromOffset = 0;
            } else {
                fromOffset -= sourceSegment.size();
            }
        }
    }

    private void writeBytesToVarLenPart(int pos, byte[] bytes, int len) {
        int roundedSize = roundNumberOfBytesToNearestWord(len);
        this.ensureCapacity(roundedSize);
        this.zeroOutPaddingBytes(len);
        this.segment.put(this.cursor, bytes, 0, len);
        this.setOffsetAndSize(pos, this.cursor, len);
        this.cursor += roundedSize;
    }

    protected static int roundNumberOfBytesToNearestWord(int numBytes) {
        int remainder = numBytes & 7;
        return remainder == 0 ? numBytes : numBytes + (8 - remainder);
    }

    protected void zeroOutPaddingBytes(int numBytes) {
        if ((numBytes & 7) > 0) {
            this.segment.putLong(this.cursor + (numBytes >> 3 << 3), 0L);
        }
    }

    protected void ensureCapacity(int neededSize) {
        int length = this.cursor + neededSize;
        if (this.segment.size() < length) {
            this.grow(length);
        }
    }

    private void grow(int minCapacity) {
        int oldCapacity = this.segment.size();
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        this.buffer = Arrays.copyOf(this.segment.getArray(), newCapacity);
        this.segment = MemorySegment.wrap(buffer);
    }

    private static void writeBytesToFixLenPart(
            MemorySegment segment, int fieldOffset, byte[] bytes, int len) {
        long firstByte = len | 128;
        long sevenBytes = 0L;
        if (MemorySegment.LITTLE_ENDIAN) {
            for (int i = 0; i < len; ++i) {
                sevenBytes |= (255L & (long) bytes[i]) << (int) ((long) i * 8L);
            }
        } else {
            for (int i = 0; i < len; ++i) {
                sevenBytes |= (255L & (long) bytes[i]) << (int) ((long) (6 - i) * 8L);
            }
        }

        long offsetAndSize = firstByte << 56 | sevenBytes;
        segment.putLong(fieldOffset, offsetAndSize);
    }

    // ----------------------- internal methods -------------------------------

    private int getFieldOffset(int pos) {
        return nullBitsSizeInBytes + 8 * pos;
    }

    private static int getFixedLengthPartSize(int nullBitsSizeInBytes, int arity) {
        return nullBitsSizeInBytes + 8 * arity;
    }

    private static int calculateBitSetWidthInBytes(int arity) {
        return (arity + 63 + 8) / 64 * 8;
    }

    public void setOffsetAndSize(int pos, int offset, long size) {
        long offsetAndSize = (long) offset << 32 | size;
        this.segment.putLong(this.getFieldOffset(pos), offsetAndSize);
    }

    /**
     * Creates an accessor for writing the elements of an hudi binary row writer during runtime.
     *
     * @param fieldType the field type to write
     */
    public static FieldWriter createFieldWriter(DataType fieldType) {
        final FieldWriter fieldWriter;
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case STRING:
                fieldWriter = (writer, pos, value) -> writer.writeString(pos, (BinaryString) value);
                break;
            case BOOLEAN:
                fieldWriter = ((writer, pos, value) -> writer.writeBoolean(pos, (boolean) value));
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
                        "Unsupported type for Hudi BinaryRow writer: " + fieldType);
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

    /** Accessor for writing the elements of an hudi binary row writer during runtime. */
    interface FieldWriter extends Serializable {
        void writeField(HudiBinaryRowWriter writer, int pos, Object value);
    }
}

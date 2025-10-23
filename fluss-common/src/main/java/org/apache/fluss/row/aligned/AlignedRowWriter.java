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
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.LocalZonedTimestampType;
import org.apache.fluss.types.TimestampType;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.fluss.row.BinarySection.MAX_FIX_PART_DATA_SIZE;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Writer for {@link AlignedRow}.
 *
 * <p>Use the special format to write data to a {@link MemorySegment} (its capacity grows
 * automatically).
 *
 * <p>If write a format binary: 1. New a writer. 2. Write each field by writeXX or setNullAt.
 * (Variable length fields can not be written repeatedly.) 3. Invoke {@link #complete()}.
 *
 * <p>If want to reuse this writer, please invoke {@link #reset()} first.
 */
public final class AlignedRowWriter {

    private final int nullBitsSizeInBytes;
    private final AlignedRow row;
    private final int fixedSize;

    private MemorySegment segment;
    private int cursor;

    public AlignedRowWriter(AlignedRow row) {
        this(row, 0);
    }

    public AlignedRowWriter(AlignedRow row, int initialSize) {
        this.nullBitsSizeInBytes = AlignedRow.calculateBitSetWidthInBytes(row.getFieldCount());
        this.fixedSize = row.getFixedLengthPartSize();
        this.cursor = fixedSize;

        this.segment = MemorySegment.wrap(new byte[fixedSize + initialSize]);
        this.row = row;
        this.row.pointTo(segment, 0, segment.size());
    }

    /** Reset writer to prepare next write. First, reset. */
    public void reset() {
        this.cursor = fixedSize;
        for (int i = 0; i < nullBitsSizeInBytes; i += 8) {
            segment.putLong(i, 0L);
        }
    }

    /** Set null to this field. Default not null. */
    public void setNullAt(int pos) {
        setNullBit(pos);
        segment.putLong(getFieldOffset(pos), 0L);
    }

    public void setNullBit(int pos) {
        BinarySegmentUtils.bitSet(segment, 0, pos + AlignedRow.HEADER_SIZE_IN_BITS);
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

    /** See {@link BinarySegmentUtils#readBinaryString(MemorySegment[], int, int, long)}. */
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
        if (len <= MAX_FIX_PART_DATA_SIZE) {
            writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
        } else {
            writeBytesToVarLenPart(pos, bytes, len);
        }
    }

    public void writeBinary(int pos, byte[] bytes) {
        int len = bytes.length;
        if (len <= MAX_FIX_PART_DATA_SIZE) {
            writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
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

            cursor += 8;
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

    /** Finally, complete write to set real size to binary. */
    public void complete() {
        row.setTotalSize(cursor);
    }

    public int getFieldOffset(int pos) {
        return nullBitsSizeInBytes + 8 * pos;
    }

    /** Set offset and size to fix len part. */
    public void setOffsetAndSize(int pos, int offset, long size) {
        final long offsetAndSize = ((long) offset << 32) | size;
        segment.putLong(getFieldOffset(pos), offsetAndSize);
    }

    /** After grow, need point to new memory. */
    public void afterGrow() {
        row.pointTo(segment, 0, segment.size());
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

    /** Increases the capacity to ensure that it can hold at least the minimum capacity argument. */
    private void grow(int minCapacity) {
        int oldCapacity = segment.size();
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        segment = MemorySegment.wrap(Arrays.copyOf(segment.getArray(), newCapacity));
        afterGrow();
    }

    protected static int roundNumberOfBytesToNearestWord(int numBytes) {
        int remainder = numBytes & 0x07;
        if (remainder == 0) {
            return numBytes;
        } else {
            return numBytes + (8 - remainder);
        }
    }

    private static void writeBytesToFixLenPart(
            MemorySegment segment, int fieldOffset, byte[] bytes, int len) {
        long firstByte = len | 0x80; // first bit is 1, other bits is len
        long sevenBytes = 0L; // real data
        if (AlignedRow.LITTLE_ENDIAN) {
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

    public MemorySegment getSegments() {
        return segment;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * @deprecated Use {@code #createValueSetter(DataType)} for avoiding logical types during
     *     runtime.
     */
    @Deprecated
    public static void write(AlignedRowWriter writer, int pos, Object o, DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                writer.writeBoolean(pos, (boolean) o);
                break;
            case TINYINT:
                writer.writeByte(pos, (byte) o);
                break;
            case SMALLINT:
                writer.writeShort(pos, (short) o);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                writer.writeInt(pos, (int) o);
                break;
            case BIGINT:
                writer.writeLong(pos, (long) o);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                writer.writeTimestampNtz(pos, (TimestampNtz) o, timestampType.getPrecision());
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
                writer.writeTimestampLtz(pos, (TimestampLtz) o, lzTs.getPrecision());
                break;
            case FLOAT:
                writer.writeFloat(pos, (float) o);
                break;
            case DOUBLE:
                writer.writeDouble(pos, (double) o);
                break;
            case CHAR:
            case STRING:
                writer.writeString(pos, (BinaryString) o);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                writer.writeDecimal(pos, (Decimal) o, decimalType.getPrecision());
                break;
            case BINARY:
                writer.writeBinary(pos, (byte[]) o);
                break;
            default:
                throw new UnsupportedOperationException("Not support type: " + type);
        }
    }
}

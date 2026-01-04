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

import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.array.CompactedArray;
import org.apache.fluss.row.map.CompactedMap;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import java.io.Serializable;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.apache.fluss.types.DataTypeChecks.getScale;

/**
 * Deserializes a {@link InternalRow} in an decoded way. In order to save more space, int and long
 * are written in variable length (lengths of strings, binaries, etc. are also written in this way).
 *
 * <p>NOTE: read from byte[] instead of {@link MemorySegment} can be a bit more efficient.
 *
 * <p>See {@link CompactedRowWriter}.
 */
public class CompactedRowReader {
    // Including null bits.
    private final int headerSizeInBytes;

    private MemorySegment segment;
    private MemorySegment[] segments;
    private int offset;
    private int position;
    private int limit;

    public CompactedRowReader(int fieldCount) {
        this.headerSizeInBytes = CompactedRow.calculateBitSetWidthInBytes(fieldCount);
    }

    public void pointTo(MemorySegment segment, int offset, int length) {
        pointTo(segment, offset, offset + headerSizeInBytes, offset + length);
    }

    private void pointTo(MemorySegment segment, int offset, int position, int limit) {
        if (segment != this.segment) {
            this.segment = segment;
            this.segments = new MemorySegment[] {segment};
        }
        this.offset = offset;
        this.position = position;
        this.limit = limit;
    }

    public boolean isNullAt(int pos) {
        return BinarySegmentUtils.bitGet(segment, offset, pos);
    }

    public boolean readBoolean() {
        return segment.getBoolean(position++);
    }

    public byte readByte() {
        return segment.get(position++);
    }

    public short readShort() {
        short value = segment.getShort(position);
        position += 2;
        return value;
    }

    /** See {@link #readLong()}. */
    public int readInt() {
        int tempPos = position;
        int x;
        if ((x = segment.get(tempPos++)) >= 0) {
            position = tempPos;
            return x;
        } else if ((x ^= (segment.get(tempPos++) << 7)) < 0) {
            x ^= (~0 << 7);
        } else if ((x ^= (segment.get(tempPos++) << 14)) >= 0) {
            x ^= (~0 << 7) ^ (~0 << 14);
        } else if ((x ^= (segment.get(tempPos++) << 21)) < 0) {
            x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21);
        } else {
            int y = segment.get(tempPos++);
            x ^= y << 28;
            x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21) ^ (~0 << 28);
        }
        position = tempPos;
        return x;
    }

    public long readLong() {
        // Influenced by Protobuf CodedInputStream.
        // Implementation notes:
        //
        // Optimized for one-byte values, expected to be common.
        // The particular code below was selected from various candidates
        // empirically.
        //
        // Sign extension of (signed) Java bytes is usually a nuisance, but
        // we exploit it here to more easily obtain the sign of bytes read.
        // Instead of cleaning up the sign extension bits by masking eagerly,
        // we delay until we find the final (positive) byte, when we clear all
        // accumulated bits with one xor.  We depend on javac to constant fold.
        fastPath:
        {
            int tempPos = position;
            if (limit == tempPos) {
                break fastPath; // illegal, throws exception
            }
            final MemorySegment segment = this.segment;
            long x;
            int y;
            if ((y = segment.get(tempPos++)) >= 0) {
                position = tempPos;
                return y;
            } else if (limit - tempPos < 9) {
                break fastPath;
            } else if ((y ^= (segment.get(tempPos++) << 7)) < 0) {
                x = y ^ (~0 << 7);
            } else if ((y ^= (segment.get(tempPos++) << 14)) >= 0) {
                x = y ^ ((~0 << 7) ^ (~0 << 14));
            } else if ((y ^= (segment.get(tempPos++) << 21)) < 0) {
                x = y ^ ((~0 << 7) ^ (~0 << 14) ^ (~0 << 21));
            } else if ((x = y ^ ((long) segment.get(tempPos++) << 28)) >= 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
            } else if ((x ^= ((long) segment.get(tempPos++) << 35)) < 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
            } else if ((x ^= ((long) segment.get(tempPos++) << 42)) >= 0L) {
                x ^=
                        (~0L << 7)
                                ^ (~0L << 14)
                                ^ (~0L << 21)
                                ^ (~0L << 28)
                                ^ (~0L << 35)
                                ^ (~0L << 42);
            } else if ((x ^= ((long) segment.get(tempPos++) << 49)) < 0L) {
                x ^=
                        (~0L << 7)
                                ^ (~0L << 14)
                                ^ (~0L << 21)
                                ^ (~0L << 28)
                                ^ (~0L << 35)
                                ^ (~0L << 42)
                                ^ (~0L << 49);
            } else {
                x ^= ((long) segment.get(tempPos++) << 56);
                x ^=
                        (~0L << 7)
                                ^ (~0L << 14)
                                ^ (~0L << 21)
                                ^ (~0L << 28)
                                ^ (~0L << 35)
                                ^ (~0L << 42)
                                ^ (~0L << 49)
                                ^ (~0L << 56);
                if (x < 0L) {
                    if (segment.get(tempPos++) < 0L) {
                        break fastPath; // illegal, throws exception
                    }
                }
            }
            position = tempPos;
            return x;
        }
        return readLongSlowPath();
    }

    public float readFloat() {
        float value = segment.getFloat(position);
        position += 4;
        return value;
    }

    public double readDouble() {
        double value = segment.getDouble(position);
        position += 8;
        return value;
    }

    public BinaryString readString() {
        int length = readInt();
        BinaryString string = BinaryString.fromAddress(segments, position, length);
        position += length;
        return string;
    }

    public BinaryString readChar(int length) {
        BinaryString string = BinaryString.fromAddress(segments, position, length);
        position += length;
        return string;
    }

    public Decimal readDecimal(int precision, int scale) {
        return Decimal.isCompact(precision)
                ? Decimal.fromUnscaledLong(readLong(), precision, scale)
                : Decimal.fromUnscaledBytes(readBytes(), precision, scale);
    }

    public TimestampLtz readTimestampLtz(int precision) {
        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(readLong());
        }
        long milliseconds = readLong();
        int nanosOfMillisecond = readInt();
        return TimestampLtz.fromEpochMillis(milliseconds, nanosOfMillisecond);
    }

    public TimestampNtz readTimestampNtz(int precision) {
        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(readLong());
        }
        long milliseconds = readLong();
        int nanosOfMillisecond = readInt();
        return TimestampNtz.fromMillis(milliseconds, nanosOfMillisecond);
    }

    public byte[] readBytes() {
        int length = readInt();
        return readBytesInternal(length);
    }

    public byte[] readBinary(int length) {
        return readBytesInternal(length);
    }

    // ----------------------- internal methods -------------------------------
    private byte[] readBytesInternal(int length) {
        byte[] bytes = new byte[length];
        segment.get(position, bytes, 0, length);
        position += length;
        return bytes;
    }

    private long readLongSlowPath() {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            final byte b = readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new RuntimeException("Invalid input stream.");
    }

    /**
     * Creates an accessor for reading elements.
     *
     * @param fieldType the element type of the row
     */
    static FieldReader createFieldReader(DataType fieldType) {
        final FieldReader fieldReader;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
                // TODO: use readChar(length) in the future, but need to keep compatibility
            case STRING:
                fieldReader = (reader, pos) -> reader.readString();
                break;
            case BOOLEAN:
                fieldReader = (reader, pos) -> reader.readBoolean();
                break;
            case BINARY:
                // TODO: use readBinary(length) in the future, but need to keep compatibility
            case BYTES:
                fieldReader = (reader, pos) -> reader.readBytes();
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldReader = (reader, pos) -> reader.readDecimal(decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldReader = (reader, pos) -> reader.readByte();
                break;
            case SMALLINT:
                fieldReader = (reader, pos) -> reader.readShort();
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldReader = (reader, pos) -> reader.readInt();
                break;
            case BIGINT:
                fieldReader = (reader, pos) -> reader.readLong();
                break;
            case FLOAT:
                fieldReader = (reader, pos) -> reader.readFloat();
                break;
            case DOUBLE:
                fieldReader = (reader, pos) -> reader.readDouble();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(fieldType);
                fieldReader = (reader, pos) -> reader.readTimestampNtz(timestampNtzPrecision);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(fieldType);
                fieldReader = (reader, pos) -> reader.readTimestampLtz(timestampLtzPrecision);
                break;
            case ARRAY:
                DataType elementType = ((ArrayType) fieldType).getElementType();
                fieldReader = (reader, pos) -> reader.readArray(elementType);
                break;
            case MAP:
                MapType mapType = (MapType) fieldType;
                fieldReader = (reader, pos) -> reader.readMap(mapType);
                break;
            case ROW:
                DataType[] nestedFieldTypes =
                        ((RowType) fieldType).getFieldTypes().toArray(new DataType[0]);
                fieldReader = (reader, pos) -> reader.readRow(nestedFieldTypes);
                break;
            default:
                throw new IllegalArgumentException("Unsupported type for IndexedRow: " + fieldType);
        }
        if (!fieldType.isNullable()) {
            return fieldReader;
        }
        return (reader, pos) -> {
            if (reader.isNullAt(pos)) {
                return null;
            }
            return fieldReader.readField(reader, pos);
        };
    }

    public InternalArray readArray(DataType elementType) {
        int length = readInt();
        InternalArray array =
                BinarySegmentUtils.readBinaryArray(
                        segments, position, length, new CompactedArray(elementType));
        position += length;
        return array;
    }

    public InternalMap readMap(MapType mapType) {
        int length = readInt();
        CompactedMap map = new CompactedMap(mapType.getKeyType(), mapType.getValueType());
        map.pointTo(segments, position, length);
        position += length;
        return map;
    }

    public InternalRow readRow(DataType[] nestedFieldTypes) {
        int length = readInt();
        CompactedRow row = new CompactedRow(nestedFieldTypes);
        row.pointTo(segments, position, length);
        position += length;
        return row;
    }

    /**
     * Accessor for reading the field of a row during runtime.
     *
     * @see #createFieldReader(DataType)
     */
    interface FieldReader extends Serializable {
        Object readField(CompactedRowReader reader, int pos);
    }
}

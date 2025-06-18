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

package com.alibaba.fluss.row.indexed;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.row.BinarySegmentUtils;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.DataType;

import java.io.Serializable;
import java.util.Arrays;

import static com.alibaba.fluss.types.DataTypeChecks.getLength;
import static com.alibaba.fluss.types.DataTypeChecks.getPrecision;
import static com.alibaba.fluss.types.DataTypeChecks.getScale;

/**
 * Reader for {@link IndexedRow}. Deserializes a {@link IndexedRow} in a decoded way.
 *
 * <p>NOTE: read from byte[] instead of {@link MemorySegment} can be a bit more efficient.
 *
 * <p>See {@link IndexedRowWriter}.
 */
@Internal
public class IndexedRowReader {

    private final int nullBitsSizeInBytes;
    private final int variableColumnLengthListInBytes;
    // nullBitSet size + variable column length list size.
    private final int headerSizeInBytes;

    private MemorySegment segment;
    private int offset;
    private int position;
    private int variableLengthPosition;

    public IndexedRowReader(DataType[] types) {
        this.nullBitsSizeInBytes = IndexedRow.calculateBitSetWidthInBytes(types.length);
        this.variableColumnLengthListInBytes =
                IndexedRow.calculateVariableColumnLengthListSize(types);
        this.headerSizeInBytes = nullBitsSizeInBytes + variableColumnLengthListInBytes;
        // init variable length position.
        this.variableLengthPosition = nullBitsSizeInBytes;
    }

    public void pointTo(MemorySegment segment, int offset) {
        if (segment != this.segment) {
            this.segment = segment;
        }
        this.offset = offset;
        this.position = offset + headerSizeInBytes;
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

    public int readInt() {
        int value = segment.getInt(position);
        position += 4;
        return value;
    }

    public long readLong() {
        long value = segment.getLong(position);
        position += 8;
        return value;
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

    public BinaryString readChar(int length) {
        byte[] bytes = new byte[length];
        segment.get(position, bytes, 0, length);

        int newLen = 0;
        for (int i = length - 1; i >= 0; i--) {
            if (bytes[i] != (byte) 0) {
                newLen = i + 1;
                break;
            }
        }

        position += length;
        return BinaryString.fromString(BinaryString.decodeUTF8(bytes, 0, newLen));
    }

    public BinaryString readString() {
        int length = readVarLengthFromVarLengthList();
        return readStringInternal(length);
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

    public byte[] readBinary(int length) {
        return readBytesInternal(length);
    }

    public byte[] readBytes() {
        int length = readVarLengthFromVarLengthList();
        return readBytesInternal(length);
    }

    private int readVarLengthFromVarLengthList() {
        if (variableLengthPosition - nullBitsSizeInBytes + 4 > variableColumnLengthListInBytes) {
            throw new IllegalArgumentException();
        }

        int value = segment.getInt(variableLengthPosition);
        variableLengthPosition += 4;
        return value;
    }

    private BinaryString readStringInternal(int length) {
        BinaryString string =
                BinaryString.fromAddress(new MemorySegment[] {segment}, position, length);
        position += length;
        return string;
    }

    private byte[] readBytesInternal(int length) {
        byte[] bytes = new byte[length];
        segment.get(position, bytes, 0, length);

        int newLen = 0;
        for (int i = length - 1; i >= 0; i--) {
            if (bytes[i] != (byte) 0) {
                newLen = i + 1;
                break;
            }
        }

        position += length;
        return Arrays.copyOfRange(bytes, 0, newLen);
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
                final int charLength = getLength(fieldType);
                fieldReader = (reader, pos) -> reader.readChar(charLength);
                break;
            case STRING:
                fieldReader = (reader, pos) -> reader.readString();
                break;
            case BOOLEAN:
                fieldReader = (reader, pos) -> reader.readBoolean();
                break;
            case BINARY:
                final int binaryLength = getLength(fieldType);
                fieldReader = (reader, pos) -> reader.readBinary(binaryLength);
                break;
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

    /**
     * Accessor for reading the field of a row during runtime.
     *
     * @see #createFieldReader(DataType)
     */
    interface FieldReader extends Serializable {
        Object readField(IndexedRowReader reader, int pos);
    }
}

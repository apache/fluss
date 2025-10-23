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

package org.apache.fluss.row.indexed;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.MemorySegmentWritable;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.UnsafeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;

import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/** Writer for {@link IndexedRow}. */
@Internal
public class IndexedRowWriter extends OutputStream implements MemorySegmentWritable {

    private final int nullBitsSizeInBytes;
    private final int variableColumnLengthListInBytes;
    // nullBitSet size + variable column length list size.
    private final int headerSizeInBytes;

    private byte[] buffer;
    private MemorySegment segment;
    private int position;
    private int variableLengthPosition;

    public IndexedRowWriter(RowType rowType) {
        this(rowType.getChildren().toArray(new DataType[0]));
    }

    public IndexedRowWriter(DataType[] types) {
        this.nullBitsSizeInBytes = IndexedRow.calculateBitSetWidthInBytes(types.length);
        this.variableColumnLengthListInBytes =
                IndexedRow.calculateVariableColumnLengthListSize(types);
        this.headerSizeInBytes = nullBitsSizeInBytes + variableColumnLengthListInBytes;
        this.position = headerSizeInBytes;
        // begin from nullBitsSizeInBytes.
        this.variableLengthPosition = nullBitsSizeInBytes;

        setBuffer(new byte[Math.max(64, headerSizeInBytes)]);
    }

    public void reset() {
        this.position = headerSizeInBytes;
        this.variableLengthPosition = nullBitsSizeInBytes;
        for (int i = 0; i < headerSizeInBytes; i++) {
            buffer[i] = 0;
        }
    }

    /** Default not null. */
    public void setNullAt(int pos) {
        UnsafeUtils.bitSet(buffer, 0, pos);
    }

    public void writeBoolean(boolean value) {
        ensureCapacity(1);
        UnsafeUtils.putBoolean(buffer, position++, value);
    }

    public void writeByte(byte value) {
        ensureCapacity(1);
        UnsafeUtils.putByte(buffer, position++, value);
    }

    public void writeShort(short value) {
        ensureCapacity(2);
        UnsafeUtils.putShort(buffer, position, value);
        position += 2;
    }

    public void writeInt(int value) {
        ensureCapacity(4);
        UnsafeUtils.putInt(buffer, position, value);
        position += 4;
    }

    public void writeLong(long value) {
        ensureCapacity(8);
        UnsafeUtils.putLong(buffer, position, value);
        position += 8;
    }

    public void writeFloat(float value) {
        ensureCapacity(4);
        UnsafeUtils.putFloat(buffer, position, value);
        position += 4;
    }

    public void writeDouble(double value) {
        ensureCapacity(8);
        UnsafeUtils.putDouble(buffer, position, value);
        position += 8;
    }

    public void writeChar(BinaryString value, int length) {
        writeChar(value.toString(), length);
    }

    public void writeString(BinaryString value) {
        int length = value.getSizeInBytes();
        // write var length in variable column length list.
        writeVarLengthToVarLengthList(length);
        if (value.getSegments() == null) {
            writeChar(value, length);
        } else {
            int offset = value.getOffset();
            MemorySegment segment0 = value.getSegments()[0];
            if (offset + length <= segment0.size()) {
                write(segment0, offset, length);
            } else {
                byte[] bytes = BinarySegmentUtils.allocateReuseBytes(length);
                BinarySegmentUtils.copyToBytes(value.getSegments(), offset, bytes, 0, length);
                write(bytes, 0, length);
            }
        }
    }

    private void writeChar(String string, int length) {
        byte[] bytes = new byte[length];
        BinaryString.encodeUTF8(string, bytes);
        write(bytes, 0, length);
    }

    public void writeBinary(byte[] value, int length) {
        if (value.length > length) {
            throw new IllegalArgumentException();
        }
        byte[] newByte = new byte[length];
        System.arraycopy(value, 0, newByte, 0, value.length);
        write(newByte, 0, length);
    }

    public void writeBytes(byte[] value) {
        writeVarLengthToVarLengthList(value.length);
        write(value, 0, value.length);
    }

    public void writeDecimal(Decimal value, int precision) {
        if (Decimal.isCompact(precision)) {
            writeLong(value.toUnscaledLong());
        } else {
            writeBytes(value.toUnscaledBytes());
        }
    }

    public void writeTimestampNtz(TimestampNtz value, int precision) {
        if (TimestampNtz.isCompact(precision)) {
            writeLong(value.getMillisecond());
        } else {
            writeLong(value.getMillisecond());
            writeInt(value.getNanoOfMillisecond());
        }
    }

    public void writeTimestampLtz(TimestampLtz value, int precision) {
        if (TimestampLtz.isCompact(precision)) {
            writeLong(value.getEpochMillisecond());
        } else {
            writeLong(value.getEpochMillisecond());
            writeInt(value.getNanoOfMillisecond());
        }
    }

    @Override
    public void write(int b) {
        writeByte((byte) b);
    }

    @Override
    public void write(byte[] value, int off, int len) {
        ensureCapacity(len);
        System.arraycopy(value, off, buffer, position, len);
        position += len;
    }

    @Override
    public void write(MemorySegment segment, int off, int len) {
        ensureCapacity(len);
        segment.get(off, this.buffer, this.position, len);
        this.position += len;
    }

    public byte[] buffer() {
        return buffer;
    }

    public MemorySegment segment() {
        return segment;
    }

    public int position() {
        return position;
    }

    // ----------------------- internal methods -------------------------------

    private void setBuffer(byte[] buffer) {
        this.buffer = buffer;
        this.segment = MemorySegment.wrap(buffer);
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

    private void writeVarLengthToVarLengthList(int length) {
        if (variableLengthPosition - nullBitsSizeInBytes + 4 > variableColumnLengthListInBytes) {
            throw new IllegalArgumentException();
        }
        UnsafeUtils.putInt(buffer, variableLengthPosition, length);
        variableLengthPosition += 4;
    }

    // ------------------------------------------------------------------------------------------

    public static void serializeIndexedRow(IndexedRow row, OutputView target) throws IOException {
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
                final int charLength = getLength(fieldType);
                fieldWriter =
                        (writer, pos, value) -> writer.writeChar((BinaryString) value, charLength);
                break;
            case STRING:
                fieldWriter = (writer, pos, value) -> writer.writeString((BinaryString) value);
                break;
            case BOOLEAN:
                fieldWriter = (writer, pos, value) -> writer.writeBoolean((boolean) value);
                break;
            case BINARY:
                final int binaryLength = getLength(fieldType);
                fieldWriter =
                        (writer, pos, value) -> writer.writeBinary((byte[]) value, binaryLength);
                break;
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

    /** Accessor for writing the elements of an indexed row writer during runtime. */
    public interface FieldWriter extends Serializable {
        void writeField(IndexedRowWriter writer, int pos, Object value);
    }
}

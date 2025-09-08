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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.NullAwareGetters;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.BinaryType;
import org.apache.fluss.types.CharType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.IntType;
import org.apache.fluss.types.StringType;
import org.apache.fluss.utils.MurmurHashUtils;

import java.util.Arrays;

import static org.apache.fluss.types.DataTypeChecks.getPrecision;

/**
 * An implementation of {@link InternalRow} which is backed by {@link MemorySegment} instead of
 * Object. Data are stored in this row with a binary format, and it does not have the ability of
 * random access.
 *
 * <p>A Row has Three part: nullBitSet + variable column length list + value.
 *
 * <p>For nullBitSet, to null values, only bit will be set on the head, and there is no content in
 * the data part. For example:
 *
 * <ul>
 *   <li>IndexedRow(1, 2) will stored as 3 bytes: NULL_BIT_SET + 1 + 2.
 *   <li>IndexedRow(null, 2) will stored as 2 bytes: NULL_BIT_SET(First bit is 1) + 2.
 * </ul>
 *
 * <p>Variable column length list is designed to provide indexing capability for the value part. It
 * only records the length of the variable column, such as {@link StringType} and {@link CharType}.
 * For fixed length columns, like {@link IntType}, they can be indexed without storage column
 * length. Each variable column length is storage as int32 (4B).
 *
 * <p>Tradeoff: Sacrifice cpu for space. As long as there is a read, all fields are accessed.
 */
@Internal
public class IndexedRow implements BinaryRow, NullAwareGetters {
    public static final int VARIABLE_COLUMN_LENGTH_SIZE = Integer.BYTES;

    private final int arity;
    private final int nullBitsSizeInBytes;
    // nullBitSet size + variable column length list size.
    private final int headerSizeInBytes;
    private final DataType[] fieldTypes;

    private MemorySegment segment;
    private MemorySegment[] segments;
    private int offset;
    private int sizeInBytes;
    private int[] columnLengths;

    public IndexedRow(DataType[] fieldTypes) {
        this.fieldTypes = fieldTypes;
        this.arity = fieldTypes.length;
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
        this.headerSizeInBytes =
                nullBitsSizeInBytes + calculateVariableColumnLengthListSize(fieldTypes);
    }

    public static IndexedRow from(DataType[] fieldType, byte[] dataBytes) {
        MemorySegment memorySegment = MemorySegment.wrap(dataBytes);
        IndexedRow indexedRow = new IndexedRow(fieldType);
        indexedRow.pointTo(memorySegment, 0, memorySegment.size());
        return indexedRow;
    }

    /**
     * Copies the bytes of the row to the destination memory, beginning at the given offset.
     *
     * @param dst The memory into which the bytes will be copied.
     * @param dstOffset The copying offset in the destination memory.
     * @throws IndexOutOfBoundsException Thrown, if too large that the bytes of the row exceed the
     *     amount of memory between the dstOffset and the dst array's end.
     */
    @Override
    public void copyTo(byte[] dst, int dstOffset) {
        segment.get(offset, dst, dstOffset, sizeInBytes);
    }

    public void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
        this.segment = segment;
        this.segments = new MemorySegment[] {segment};
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        this.columnLengths = calculateColumnLengths();
    }

    public int getHeaderSizeInBytes() {
        return headerSizeInBytes;
    }

    private int[] calculateColumnLengths() {
        int variableLengthPosition = offset + nullBitsSizeInBytes;
        int[] columnLengths = new int[arity];
        for (int i = 0; i < arity; i++) {
            if (isNullAt(i)) {
                columnLengths[i] = 0;
                continue;
            }

            DataType type = fieldTypes[i];
            if (!isFixedLength(type)) {
                columnLengths[i] = getLengthFromVariableColumnLengthList(variableLengthPosition);
                variableLengthPosition += 4;
            } else {
                columnLengths[i] = getLength(type);
            }
        }

        return columnLengths;
    }

    private int getLengthFromVariableColumnLengthList(int variableLengthPosition) {
        return segment.getInt(variableLengthPosition);
    }

    public static int calculateVariableColumnLengthListSize(DataType[] types) {
        int size = 0;
        for (DataType dataType : types) {
            if (!isFixedLength(dataType)) {
                size += VARIABLE_COLUMN_LENGTH_SIZE;
            }
        }

        return size;
    }

    public MemorySegment getSegment() {
        return segment;
    }

    @Override
    public MemorySegment[] getSegments() {
        return segments;
    }

    @Override
    public int getOffset() {
        return offset;
    }

    @Override
    public int getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public int getFieldCount() {
        return arity;
    }

    /** The bit is 1 when the field is null. Default is 0. */
    @Override
    public boolean anyNull() {
        for (int i = 1; i < nullBitsSizeInBytes; i++) {
            if (segment.get(i) != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean anyNull(int[] fields) {
        for (int field : fields) {
            if (isNullAt(field)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets an IndexedRow with only the specified fields after projecting.
     *
     * <p>NOTE: ONLY TESTING PURPOSE.
     *
     * @param fields the fields index to project.
     * @return the projected row.
     */
    @VisibleForTesting
    public IndexedRow projectRow(int[] fields) {
        if (fields.length > arity) {
            throw new IllegalArgumentException("project fields length is larger than row arity");
        }

        DataType[] newType = new DataType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            newType[i] = fieldTypes[fields[i]];
        }

        InternalRow.FieldGetter[] fieldGetter = new InternalRow.FieldGetter[newType.length];
        IndexedRowWriter.FieldWriter[] writers = new IndexedRowWriter.FieldWriter[newType.length];
        for (int i = 0; i < newType.length; i++) {
            fieldGetter[i] = InternalRow.createFieldGetter(newType[i], fields[i]);
            writers[i] = IndexedRowWriter.createFieldWriter(newType[i]);
        }

        IndexedRow projectRow = new IndexedRow(newType);
        IndexedRowWriter writer = new IndexedRowWriter(newType);
        for (int i = 0; i < newType.length; i++) {
            writers[i].writeField(writer, i, fieldGetter[i].getFieldOrNull(this));
        }

        projectRow.pointTo(writer.segment(), 0, writer.position());

        return projectRow;
    }

    public static int calculateBitSetWidthInBytes(int arity) {
        return (arity + 7) / 8;
    }

    public static boolean isFixedLength(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case CHAR:
            case BINARY:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            case STRING:
            case BYTES:
                return false;
            case DECIMAL:
                return Decimal.isCompact(((DecimalType) dataType).getPrecision());
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Currently, Data type '%s' is not supported in indexedRow",
                                dataType));
        }
    }

    private static int getLength(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
            case FLOAT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return 4;
            case BIGINT:
            case DOUBLE:
            case DECIMAL:
                return 8;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampNtzPrecision = getPrecision(dataType);
                if (TimestampNtz.isCompact(timestampNtzPrecision)) {
                    return 8;
                } else {
                    return 12;
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampLtzPrecision = getPrecision(dataType);
                if (TimestampLtz.isCompact(timestampLtzPrecision)) {
                    return 8;
                } else {
                    return 12;
                }
            case CHAR:
                return ((CharType) dataType).getLength();
            case BINARY:
                return ((BinaryType) dataType).getLength();
            default:
                throw new IllegalArgumentException(" Data type '%s' is not fixed length type!");
        }
    }

    @Override
    public boolean isNullAt(int pos) {
        return BinarySegmentUtils.bitGet(segment, offset, pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return segment.getBoolean(getFieldOffset(pos));
    }

    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return segment.get(getFieldOffset(pos));
    }

    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return segment.getShort(getFieldOffset(pos));
    }

    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return segment.getInt(getFieldOffset(pos));
    }

    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return segment.getLong(getFieldOffset(pos));
    }

    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return segment.getFloat(getFieldOffset(pos));
    }

    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return segment.getDouble(getFieldOffset(pos));
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        assertIndexIsValid(pos);
        byte[] bytes = new byte[length];
        segment.get(getFieldOffset(pos), bytes, 0, length);

        int newLen = 0;
        for (int i = length - 1; i >= 0; i--) {
            if (bytes[i] != (byte) 0) {
                newLen = i + 1;
                break;
            }
        }

        return BinaryString.fromString(BinaryString.decodeUTF8(bytes, 0, newLen));
    }

    @Override
    public BinaryString getString(int pos) {
        assertIndexIsValid(pos);
        return BinaryString.fromAddress(segments, getFieldOffset(pos), columnLengths[pos]);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);
        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(segment.getLong(getFieldOffset(pos)), precision, scale);
        }

        return Decimal.fromUnscaledBytes(getBytes(pos), precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        int fieldOffset = getFieldOffset(pos);
        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(segment.getLong(fieldOffset));
        }

        long milliseconds = segment.getLong(fieldOffset);
        int nanosOfMillisecond = segment.getInt(fieldOffset + 8);
        return TimestampNtz.fromMillis(milliseconds, nanosOfMillisecond);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        int fieldOffset = getFieldOffset(pos);
        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(segment.getLong(fieldOffset));
        }

        long milliseconds = segment.getLong(fieldOffset);
        int nanosOfMillisecond = segment.getInt(fieldOffset + 8);
        return TimestampLtz.fromEpochMillis(milliseconds, nanosOfMillisecond);
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        byte[] bytes = new byte[length];
        segment.get(getFieldOffset(pos), bytes, 0, length);

        int newLen = 0;
        for (int i = length - 1; i >= 0; i--) {
            if (bytes[i] != (byte) 0) {
                newLen = i + 1;
                break;
            }
        }
        return Arrays.copyOfRange(bytes, 0, newLen);
    }

    @Override
    public byte[] getBytes(int pos) {
        int length = columnLengths[pos];
        byte[] bytes = new byte[length];
        segment.get(getFieldOffset(pos), bytes, 0, length);
        return bytes;
    }

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < arity : "index (" + index + ") should < " + arity;
    }

    private int getFieldOffset(int pos) {
        int baseOffset = offset + headerSizeInBytes;

        if (pos == 0) {
            return baseOffset;
        }

        for (int i = 0; i < pos; i++) {
            baseOffset += columnLengths[i];
        }

        return baseOffset;
    }

    public IndexedRow copy() {
        return copy(new IndexedRow(fieldTypes));
    }

    public IndexedRow copy(IndexedRow from) {
        byte[] newBuffer = new byte[sizeInBytes];
        segment.get(offset, newBuffer, 0, sizeInBytes);
        from.pointTo(MemorySegment.wrap(newBuffer), 0, sizeInBytes);
        return from;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexedRow)) {
            return false;
        }
        IndexedRow that = (IndexedRow) o;
        return sizeInBytes == that.sizeInBytes
                && segment.equalTo(that.segment, offset, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return MurmurHashUtils.hashBytes(segment, offset, sizeInBytes);
    }
}

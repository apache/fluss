/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.row.array.PrimitiveBinaryArray;
import org.apache.fluss.types.DataType;

import java.lang.reflect.Array;

import static org.apache.fluss.memory.MemoryUtils.UNSAFE;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * A binary implementation of {@link InternalArray} which is backed by {@link MemorySegment}s.
 *
 * <p>For fields that hold fixed-length primitive types, such as long, double or int, they are
 * stored compacted in bytes, just like the original java array.
 *
 * <p>The binary layout of {@link BinaryArray}:
 *
 * <pre>
 * [size(int)] + [null bits(4-byte word boundaries)] + [values or offset&length] + [variable length part].
 * </pre>
 *
 * <p>Note: different from {@link BinaryRow}, we only have one implementation of {@link
 * BinaryArray}. Even for {@link org.apache.fluss.row.compacted.CompactedRow}, we still use {@link
 * BinaryArray} to represent its array field instead of introducing a compacted array (use
 * var-length encoding for numerics). This is because the var-length encoding will lose memory-copy
 * from binary to in-memory primitive arrays (see {@link #fromPrimitiveArray(float[])}), this is
 * very important for performance of vector embedding encodings.
 *
 * @since 0.9
 */
@PublicEvolving
public abstract class BinaryArray extends BinarySection
        implements InternalArray, MemoryAwareGetters, DataSetters {

    private static final long serialVersionUID = 1L;

    /** Offset for Arrays. */
    private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static final int BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
    private static final int SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
    private static final int INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
    private static final int LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    private static final int FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
    private static final int DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);

    public static int calculateHeaderInBytes(int numFields) {
        return 4 + ((numFields + 31) / 32) * 4;
    }

    /**
     * It store real value when type is primitive. It store the length and offset of variable-length
     * part when type is string, map, etc.
     */
    public static int calculateFixLengthPartSize(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case CHAR:
            case STRING:
            case BINARY:
            case BYTES:
            case DECIMAL:
            case BIGINT:
            case DOUBLE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case ARRAY:
            case MAP:
            case ROW:
                // long and double are 8 bytes;
                // otherwise it stores the length and offset of the variable-length part for types
                // such as is string, map, etc.
                return 8;
            case SMALLINT:
                return 2;
            case INTEGER:
            case FLOAT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return 4;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    // The number of elements in this array
    private transient int size;

    /** The position to start storing array elements. */
    private transient int elementOffset;

    protected void assertIndexIsValid(int ordinal) {
        assert ordinal >= 0 : "ordinal (" + ordinal + ") should >= 0";
        assert ordinal < size : "ordinal (" + ordinal + ") should < " + size;
    }

    private int getElementOffset(int ordinal, int elementSize) {
        return elementOffset + ordinal * elementSize;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        checkArgument(
                segments.length == 1, "Currently, segments.length must be 1 for AlignedArray");
        // Read the number of elements from the first 4 bytes.
        final int size = BinarySegmentUtils.getInt(segments, offset);
        assert size >= 0 : "size (" + size + ") should >= 0";

        this.size = size;
        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        this.elementOffset = offset + calculateHeaderInBytes(this.size);
    }

    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.bitGet(segments, offset + 4, pos);
    }

    @Override
    public void setNullAt(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
    }

    public void setNotNullAt(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitUnSet(segments, offset + 4, pos);
    }

    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getLong(segments, getElementOffset(pos, 8));
    }

    @Override
    public void setLong(int pos, long value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setLong(segments, getElementOffset(pos, 8), value);
    }

    public void setNullLong(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setLong(segments, getElementOffset(pos, 8), 0L);
    }

    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getInt(segments, getElementOffset(pos, 4));
    }

    @Override
    public void setInt(int pos, int value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setInt(segments, getElementOffset(pos, 4), value);
    }

    public void setNullInt(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setInt(segments, getElementOffset(pos, 4), 0);
    }

    @Override
    public BinaryString getString(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readBinaryString(segments, offset, fieldOffset, offsetAndSize);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);
        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(
                    BinarySegmentUtils.getLong(segments, getElementOffset(pos, 8)),
                    precision,
                    scale);
        }

        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readDecimal(segments, offset, offsetAndSize, precision, scale);
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        assertIndexIsValid(pos);
        return getBytes(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndSize);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        assertIndexIsValid(pos);

        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(
                    BinarySegmentUtils.getLong(segments, getElementOffset(pos, 8)));
        }

        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndNanoOfMilli = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readTimestampNtz(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        assertIndexIsValid(pos);

        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(
                    BinarySegmentUtils.getLong(segments, getElementOffset(pos, 8)));
        }

        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndNanoOfMilli = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readTimestampLtz(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public InternalArray getArray(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.readBinaryArray(
                segments, offset, getLong(pos), createNestedArrayInstance());
    }

    /** Creates a nested {@link BinaryArray} with the nested data type information. */
    protected abstract BinaryArray createNestedArrayInstance();

    /** Creates a nested {@link BinaryMap} with the nested data type information. */
    protected abstract BinaryMap createNestedMapInstance();

    @Override
    public InternalMap getMap(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.readBinaryMap(
                segments, offset, getLong(pos), createNestedMapInstance());
    }

    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getBoolean(segments, getElementOffset(pos, 1));
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setBoolean(segments, getElementOffset(pos, 1), value);
    }

    public void setNullBoolean(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setBoolean(segments, getElementOffset(pos, 1), false);
    }

    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getByte(segments, getElementOffset(pos, 1));
    }

    @Override
    public void setByte(int pos, byte value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setByte(segments, getElementOffset(pos, 1), value);
    }

    public void setNullByte(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setByte(segments, getElementOffset(pos, 1), (byte) 0);
    }

    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getShort(segments, getElementOffset(pos, 2));
    }

    @Override
    public void setShort(int pos, short value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setShort(segments, getElementOffset(pos, 2), value);
    }

    public void setNullShort(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setShort(segments, getElementOffset(pos, 2), (short) 0);
    }

    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getFloat(segments, getElementOffset(pos, 4));
    }

    @Override
    public void setFloat(int pos, float value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setFloat(segments, getElementOffset(pos, 4), value);
    }

    public void setNullFloat(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setFloat(segments, getElementOffset(pos, 4), 0F);
    }

    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getDouble(segments, getElementOffset(pos, 8));
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return getString(pos);
    }

    @Override
    public void setDouble(int pos, double value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setDouble(segments, getElementOffset(pos, 8), value);
    }

    public void setNullDouble(int pos) {
        assertIndexIsValid(pos);
        BinarySegmentUtils.bitSet(segments, offset + 4, pos);
        BinarySegmentUtils.setDouble(segments, getElementOffset(pos, 8), 0.0);
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            // compact format
            setLong(pos, value.toUnscaledLong());
        } else {
            int fieldOffset = getElementOffset(pos, 8);
            int cursor = (int) (BinarySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;
            // zero-out the bytes
            BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
            BinarySegmentUtils.setLong(segments, offset + cursor + 8, 0L);

            if (value == null) {
                setNullAt(pos);
                // keep the offset for future update
                BinarySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {

                byte[] bytes = value.toUnscaledBytes();
                assert (bytes.length <= 16);

                // Write the bytes to the variable length portion.
                BinarySegmentUtils.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
                setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
            }
        }
    }

    @Override
    public void setTimestampNtz(int pos, TimestampNtz value, int precision) {
        assertIndexIsValid(pos);

        if (TimestampNtz.isCompact(precision)) {
            setLong(pos, value.getMillisecond());
        } else {
            int fieldOffset = getElementOffset(pos, 8);
            int cursor = (int) (BinarySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // zero-out the bytes
                BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
                // keep the offset for future update
                BinarySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {
                // write millisecond to the variable length portion.
                BinarySegmentUtils.setLong(segments, offset + cursor, value.getMillisecond());
                // write nanoOfMillisecond to the fixed-length portion.
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    @Override
    public void setTimestampLtz(int pos, TimestampLtz value, int precision) {
        assertIndexIsValid(pos);

        if (TimestampLtz.isCompact(precision)) {
            setLong(pos, value.getEpochMillisecond());
        } else {
            int fieldOffset = getElementOffset(pos, 8);
            int cursor = (int) (BinarySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // zero-out the bytes
                BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
                // keep the offset for future update
                BinarySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {
                // write epochMillisecond to the variable length portion.
                BinarySegmentUtils.setLong(segments, offset + cursor, value.getEpochMillisecond());
                // write nanoOfMillisecond to the fixed-length portion.
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    public boolean anyNull() {
        for (int i = offset + 4; i < elementOffset; i += 4) {
            if (BinarySegmentUtils.getInt(segments, i) != 0) {
                return true;
            }
        }
        return false;
    }

    private void checkNoNull() {
        if (anyNull()) {
            throw new RuntimeException("Primitive array must not contain a null value.");
        }
    }

    @Override
    public boolean[] toBooleanArray() {
        checkNoNull();
        boolean[] values = new boolean[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, BOOLEAN_ARRAY_OFFSET, size);
        return values;
    }

    @Override
    public byte[] toByteArray() {
        checkNoNull();
        byte[] values = new byte[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, BYTE_ARRAY_BASE_OFFSET, size);
        return values;
    }

    @Override
    public short[] toShortArray() {
        checkNoNull();
        short[] values = new short[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, SHORT_ARRAY_OFFSET, size * 2);
        return values;
    }

    @Override
    public int[] toIntArray() {
        checkNoNull();
        int[] values = new int[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, INT_ARRAY_OFFSET, size * 4);
        return values;
    }

    @Override
    public long[] toLongArray() {
        checkNoNull();
        long[] values = new long[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, LONG_ARRAY_OFFSET, size * 8);
        return values;
    }

    @Override
    public float[] toFloatArray() {
        checkNoNull();
        float[] values = new float[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, FLOAT_ARRAY_OFFSET, size * 4);
        return values;
    }

    @Override
    public double[] toDoubleArray() {
        checkNoNull();
        double[] values = new double[size];
        BinarySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, DOUBLE_ARRAY_OFFSET, size * 8);
        return values;
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toObjectArray(DataType elementType) {
        Class<T> elementClass = (Class<T>) InternalRow.getDataClass(elementType);
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(elementType);
        T[] values = (T[]) Array.newInstance(elementClass, size);
        for (int i = 0; i < size; i++) {
            if (!isNullAt(i)) {
                values[i] = (T) elementGetter.getElementOrNull(this, i);
            }
        }
        return values;
    }

    @Override
    public int hashCode() {
        return BinarySegmentUtils.hash(segments, offset, sizeInBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // override equals and only checks the other object is instance of BinaryArray
        if (!(o instanceof BinaryArray)) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        return sizeInBytes == that.sizeInBytes
                && BinarySegmentUtils.equals(
                        segments, offset, that.segments, that.offset, sizeInBytes);
    }

    // ------------------------------------------------------------------------------------------
    // Construction Utilities
    // ------------------------------------------------------------------------------------------

    public static BinaryArray fromPrimitiveArray(boolean[] arr) {
        return fromPrimitiveArray(arr, BOOLEAN_ARRAY_OFFSET, arr.length, 1);
    }

    public static BinaryArray fromPrimitiveArray(byte[] arr) {
        return fromPrimitiveArray(arr, BYTE_ARRAY_BASE_OFFSET, arr.length, 1);
    }

    public static BinaryArray fromPrimitiveArray(short[] arr) {
        return fromPrimitiveArray(arr, SHORT_ARRAY_OFFSET, arr.length, 2);
    }

    public static BinaryArray fromPrimitiveArray(int[] arr) {
        return fromPrimitiveArray(arr, INT_ARRAY_OFFSET, arr.length, 4);
    }

    public static BinaryArray fromPrimitiveArray(long[] arr) {
        return fromPrimitiveArray(arr, LONG_ARRAY_OFFSET, arr.length, 8);
    }

    public static BinaryArray fromPrimitiveArray(float[] arr) {
        return fromPrimitiveArray(arr, FLOAT_ARRAY_OFFSET, arr.length, 4);
    }

    public static BinaryArray fromPrimitiveArray(double[] arr) {
        return fromPrimitiveArray(arr, DOUBLE_ARRAY_OFFSET, arr.length, 8);
    }

    private static BinaryArray fromPrimitiveArray(
            Object arr, int offset, int length, int elementSize) {
        final long headerInBytes = calculateHeaderInBytes(length);
        final long valueRegionInBytes = ((long) elementSize) * length;

        // must align by 8 bytes
        long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
        if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
            throw new UnsupportedOperationException(
                    "Cannot convert this array to unsafe format as " + "it's too big.");
        }
        long totalSize = totalSizeInLongs * 8;

        final byte[] data = new byte[(int) totalSize];

        UNSAFE.putInt(data, (long) BYTE_ARRAY_BASE_OFFSET, length);
        UNSAFE.copyMemory(
                arr, offset, data, BYTE_ARRAY_BASE_OFFSET + headerInBytes, valueRegionInBytes);

        BinaryArray result = new PrimitiveBinaryArray();
        result.pointTo(MemorySegment.wrap(data), 0, (int) totalSize);
        return result;
    }

    public static BinaryArray fromLongArray(Long[] arr) {
        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, arr.length, 8);
        for (int i = 0; i < arr.length; i++) {
            Long v = arr[i];
            if (v == null) {
                writer.setNullLong(i);
            } else {
                writer.writeLong(i, v);
            }
        }
        writer.complete();
        return array;
    }

    public static BinaryArray fromLongArray(InternalArray arr) {
        if (arr instanceof BinaryArray) {
            return (BinaryArray) arr;
        }

        BinaryArray array = new PrimitiveBinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, arr.size(), 8);
        for (int i = 0; i < arr.size(); i++) {
            if (arr.isNullAt(i)) {
                writer.setNullLong(i);
            } else {
                writer.writeLong(i, arr.getLong(i));
            }
        }
        writer.complete();
        return array;
    }
}

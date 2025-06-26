/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.memory.MemorySegment;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Its memory storage structure is exactly the same with {@link BinaryRow}. The only different is
 * that, as {@link NestedRow} is used to store row value in the variable-length part of {@link
 * BinaryRow}, every field (including both fixed-length part and variable-length part) of {@link
 * NestedRow} has a possibility to cross the boundary of a segment, while the fixed-length part of
 * {@link BinaryRow} must fit into its first memory segment.
 */
public final class NestedRow extends BinarySection implements InternalRow, DataSetters {

    private static final long serialVersionUID = 1L;

    private final int arity;
    private final int nullBitsSizeInBytes;

    public NestedRow(int arity) {
        checkArgument(arity >= 0);
        this.arity = arity;
        this.nullBitsSizeInBytes = BinaryRow.calculateBitSetWidthInBytes(arity);
    }

    private int getFieldOffset(int pos) {
        return offset + nullBitsSizeInBytes + pos * 8;
    }

    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < arity : "index (" + index + ") should < " + arity;
    }

    @Override
    public int getFieldCount() {
        return arity;
    }

    private void setNotNullAt(int i) {
        assertIndexIsValid(i);
        BinarySegmentUtils.bitUnSet(segments, offset, i + 8);
    }

    @Override
    public void setNullAt(int i) {
        assertIndexIsValid(i);
        BinarySegmentUtils.bitSet(segments, offset, i + 8);
        BinarySegmentUtils.setLong(segments, getFieldOffset(i), 0);
    }

    @Override
    public void setInt(int pos, int value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setInt(segments, getFieldOffset(pos), value);
    }

    @Override
    public void setLong(int pos, long value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setLong(segments, getFieldOffset(pos), value);
    }

    @Override
    public void setDouble(int pos, double value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setDouble(segments, getFieldOffset(pos), value);
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            // compact format
            setLong(pos, value.toUnscaledLong());
        } else {
            int fieldOffset = getFieldOffset(pos);
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
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (BinarySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // zero-out the bytes
                BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
                BinarySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {
                // write millisecond to variable length portion.
                BinarySegmentUtils.setLong(segments, offset + cursor, value.getMillisecond());
                // write nanoOfMillisecond to fixed-length portion.
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    @Override
    public void setTimestampLtz(int pos, TimestampLtz value, int precision) {
        assertIndexIsValid(pos);

        if (TimestampLtz.isCompact(precision)) {
            setLong(pos, value.getNanoOfMillisecond());
        } else {
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (BinarySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // zero-out the bytes
                BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
                BinarySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {
                // write nanoOfMillisecond to variable length portion.
                BinarySegmentUtils.setLong(segments, offset + cursor, value.getNanoOfMillisecond());
                // write nanoOfMillisecond to fixed-length portion.
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setBoolean(segments, getFieldOffset(pos), value);
    }

    @Override
    public void setShort(int pos, short value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setShort(segments, getFieldOffset(pos), value);
    }

    @Override
    public void setByte(int pos, byte value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setByte(segments, getFieldOffset(pos), value);
    }

    @Override
    public void setFloat(int pos, float value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        BinarySegmentUtils.setFloat(segments, getFieldOffset(pos), value);
    }

    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.bitGet(segments, offset, pos + 8);
    }

    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getBoolean(segments, getFieldOffset(pos));
    }

    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getByte(segments, getFieldOffset(pos));
    }

    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getShort(segments, getFieldOffset(pos));
    }

    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getInt(segments, getFieldOffset(pos));
    }

    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getLong(segments, getFieldOffset(pos));
    }

    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getFloat(segments, getFieldOffset(pos));
    }

    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.getDouble(segments, getFieldOffset(pos));
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return getString(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readBinaryString(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(
                    BinarySegmentUtils.getLong(segments, getFieldOffset(pos)), precision, scale);
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndSize = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readDecimal(segments, offset, offsetAndSize, precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        assertIndexIsValid(pos);

        if (TimestampNtz.isCompact(precision)) {
            return TimestampNtz.fromMillis(
                    BinarySegmentUtils.getLong(segments, getFieldOffset(pos)));
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndNanoOfMilli = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readTimestampNtzData(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        assertIndexIsValid(pos);

        if (TimestampLtz.isCompact(precision)) {
            return TimestampLtz.fromEpochMillis(
                    BinarySegmentUtils.getLong(segments, getFieldOffset(pos)));
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndNanoOfMilli = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readTimestampLtzData(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public byte[] getBytes(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = BinarySegmentUtils.getLong(segments, fieldOffset);
        return BinarySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndLen);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.readRowData(segments, numFields, offset, getLong(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.readArrayData(segments, offset, getLong(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        assertIndexIsValid(pos);
        return BinarySegmentUtils.readMapData(segments, offset, getLong(pos));
    }

    public NestedRow copy() {
        return copy(new NestedRow(arity));
    }

    public NestedRow copy(InternalRow reuse) {
        return copyInternal((NestedRow) reuse);
    }

    private NestedRow copyInternal(NestedRow reuse) {
        byte[] bytes = BinarySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // both BinaryRow and NestedRow have the same memory format
        if (!(o instanceof NestedRow || o instanceof BinaryRow)) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        return sizeInBytes == that.sizeInBytes
                && BinarySegmentUtils.equals(
                        segments, offset, that.segments, that.offset, sizeInBytes);
    }

    @Override
    public int hashCode() {
        return BinarySegmentUtils.hash(segments, offset, sizeInBytes);
    }
}

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

/**
 * An {@link InternalRow} that pads another {@link InternalRow} with nulls up to a target field
 * count.
 */
public class PaddingRow implements InternalRow {

    private final int targetFieldCount;
    private InternalRow row;

    public PaddingRow(int targetFieldCount) {
        this.targetFieldCount = targetFieldCount;
    }

    /**
     * Replaces the underlying {@link InternalRow} backing this {@link PaddingRow}.
     *
     * <p>This method replaces the row data in place and does not return a new object. This is done
     * for performance reasons.
     */
    public PaddingRow replaceRow(InternalRow row) {
        this.row = row;
        return this;
    }

    @Override
    public int getFieldCount() {
        return targetFieldCount;
    }

    @Override
    public boolean isNullAt(int pos) {
        if (row.getFieldCount() > pos) {
            return row.isNullAt(pos);
        } else {
            // padding null if pos exceeds the underlying row's field count
            return true;
        }
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return row.getChar(pos, length);
    }

    @Override
    public BinaryString getString(int pos) {
        return row.getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(pos, precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        return row.getTimestampNtz(pos, precision);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        return row.getTimestampLtz(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return row.getBinary(pos, length);
    }

    @Override
    public byte[] getBytes(int pos) {
        return row.getBytes(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return row.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return row.getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return row.getRow(pos, numFields);
    }
}

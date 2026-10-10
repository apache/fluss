/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.compression.ColumnValueCodec;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** An {@link InternalRow} that lazily decodes self-describing BYTES column values. */
@Internal
public final class ColumnValueDecodingRow implements InternalRow {

    private static final ColumnValueCodec COLUMN_VALUE_CODEC = new ColumnValueCodec();

    private final InternalRow row;

    private ColumnValueDecodingRow(InternalRow row) {
        this.row = checkNotNull(row, "row must not be null");
    }

    /** Wraps a row unless it is already lazily decoding column values. */
    public static InternalRow wrap(InternalRow row) {
        if (row instanceof ColumnValueDecodingRow) {
            return row;
        }
        return new ColumnValueDecodingRow(row);
    }

    @Override
    public int getFieldCount() {
        return row.getFieldCount();
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos);
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
        if (row.isNullAt(pos)) {
            return null;
        }
        return COLUMN_VALUE_CODEC.decode(row.getBytes(pos));
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

    @Override
    public String toString() {
        return row.toString();
    }
}

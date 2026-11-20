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

package org.apache.fluss.lake.delta.source;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import io.delta.kernel.data.ColumnVector;

/**
 * Reads Fluss internal values from Delta Kernel column vectors. Subclasses tell which vector and
 * which row of it hold the value at a position, so rows, arrays and maps share the conversions.
 *
 * <p>Delta keeps dates as days and both timestamp types as microseconds since epoch, the same units
 * Fluss uses.
 */
abstract class DeltaVectorGetters implements DataGetters {

    /** Returns the vector that holds the value at the given position. */
    abstract ColumnVector vectorAt(int pos);

    /** Returns the row of {@link #vectorAt} that holds the value at the given position. */
    abstract int rowIdAt(int pos);

    @Override
    public boolean isNullAt(int pos) {
        return vectorAt(pos).isNullAt(rowIdAt(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return vectorAt(pos).getBoolean(rowIdAt(pos));
    }

    @Override
    public byte getByte(int pos) {
        return vectorAt(pos).getByte(rowIdAt(pos));
    }

    @Override
    public short getShort(int pos) {
        return vectorAt(pos).getShort(rowIdAt(pos));
    }

    @Override
    public int getInt(int pos) {
        return vectorAt(pos).getInt(rowIdAt(pos));
    }

    @Override
    public long getLong(int pos) {
        return vectorAt(pos).getLong(rowIdAt(pos));
    }

    @Override
    public float getFloat(int pos) {
        return vectorAt(pos).getFloat(rowIdAt(pos));
    }

    @Override
    public double getDouble(int pos) {
        return vectorAt(pos).getDouble(rowIdAt(pos));
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return getString(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromString(vectorAt(pos).getString(rowIdAt(pos)));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return Decimal.fromBigDecimal(vectorAt(pos).getDecimal(rowIdAt(pos)), precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        return TimestampNtz.fromMicros(getLong(pos));
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        return TimestampLtz.fromEpochMicros(getLong(pos));
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return getBytes(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return vectorAt(pos).getBinary(rowIdAt(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return new DeltaColumnarArray(vectorAt(pos).getArray(rowIdAt(pos)));
    }

    @Override
    public InternalMap getMap(int pos) {
        return new DeltaColumnarMap(vectorAt(pos).getMap(rowIdAt(pos)));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        ColumnVector struct = vectorAt(pos);
        ColumnVector[] fields = new ColumnVector[numFields];
        for (int i = 0; i < numFields; i++) {
            fields[i] = struct.getChild(i);
        }
        DeltaColumnarRow row = new DeltaColumnarRow(fields);
        row.setRowId(rowIdAt(pos));
        return row;
    }
}

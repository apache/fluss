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

package org.apache.fluss.row.columnar;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

import java.nio.charset.StandardCharsets;

/**
 * A VectorizedColumnBatch is a set of rows, organized with each column as a vector. It is the unit
 * of query execution, organized to minimize the cost per row.
 */
@Internal
public class VectorizedColumnBatch {

    public final ColumnVector[] columns;

    public VectorizedColumnBatch(ColumnVector[] vectors) {
        this.columns = vectors;
    }

    public int getFieldCount() {
        return columns.length;
    }

    public boolean isNullAt(int rowId, int colId) {
        return columns[colId].isNullAt(rowId);
    }

    public boolean getBoolean(int rowId, int colId) {
        return ((BooleanColumnVector) columns[colId]).getBoolean(rowId);
    }

    public byte getByte(int rowId, int colId) {
        return ((ByteColumnVector) columns[colId]).getByte(rowId);
    }

    public short getShort(int rowId, int colId) {
        return ((ShortColumnVector) columns[colId]).getShort(rowId);
    }

    public int getInt(int rowId, int colId) {
        return ((IntColumnVector) columns[colId]).getInt(rowId);
    }

    public long getLong(int rowId, int colId) {
        return ((LongColumnVector) columns[colId]).getLong(rowId);
    }

    public float getFloat(int rowId, int colId) {
        return ((FloatColumnVector) columns[colId]).getFloat(rowId);
    }

    public double getDouble(int rowId, int colId) {
        return ((DoubleColumnVector) columns[colId]).getDouble(rowId);
    }

    public BytesColumnVector.Bytes getByteArray(int rowId, int colId) {
        return ((BytesColumnVector) columns[colId]).getBytes(rowId);
    }

    public byte[] getBytes(int rowId, int colId) {
        BytesColumnVector.Bytes byteArray = getByteArray(rowId, colId);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            return byteArray.getBytes();
        }
    }

    public String getString(int rowId, int colId) {
        BytesColumnVector.Bytes byteArray = getByteArray(rowId, colId);
        return new String(byteArray.data, byteArray.offset, byteArray.len, StandardCharsets.UTF_8);
    }

    public Decimal getDecimal(int rowId, int colId, int precision, int scale) {
        return ((DecimalColumnVector) (columns[colId])).getDecimal(rowId, precision, scale);
    }

    public TimestampNtz getTimestampNtz(int rowId, int colId, int precision) {
        return ((TimestampNtzColumnVector) (columns[colId])).getTimestampNtz(rowId, precision);
    }

    public TimestampLtz getTimestampLtz(int rowId, int colId, int precision) {
        return ((TimestampLtzColumnVector) (columns[colId])).getTimestampLtz(rowId, precision);
    }

    public InternalArray getArray(int rowId, int colId) {
        return ((ArrayColumnVector) columns[colId]).getArray(rowId);
    }

    public InternalRow getRow(int rowId, int colId) {
        return ((RowColumnVector) columns[colId]).getRow(rowId);
    }
}

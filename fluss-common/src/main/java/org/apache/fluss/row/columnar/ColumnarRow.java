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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;

/**
 * Columnar row to support access to vector column data. It is a row view in {@link
 * VectorizedColumnBatch}.
 */
@PublicEvolving
public class ColumnarRow implements InternalRow {

    private VectorizedColumnBatch vectorizedColumnBatch;
    private int rowId;

    public ColumnarRow() {}

    public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch) {
        this(vectorizedColumnBatch, 0);
    }

    public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch, int rowId) {
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = rowId;
    }

    public void setVectorizedColumnBatch(VectorizedColumnBatch vectorizedColumnBatch) {
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = 0;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    @Override
    public boolean isNullAt(int pos) {
        return vectorizedColumnBatch.isNullAt(rowId, pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return vectorizedColumnBatch.getBoolean(rowId, pos);
    }

    @Override
    public byte getByte(int pos) {
        return vectorizedColumnBatch.getByte(rowId, pos);
    }

    @Override
    public short getShort(int pos) {
        return vectorizedColumnBatch.getShort(rowId, pos);
    }

    @Override
    public int getInt(int pos) {
        return vectorizedColumnBatch.getInt(rowId, pos);
    }

    @Override
    public long getLong(int pos) {
        return vectorizedColumnBatch.getLong(rowId, pos);
    }

    @Override
    public float getFloat(int pos) {
        return vectorizedColumnBatch.getFloat(rowId, pos);
    }

    @Override
    public double getDouble(int pos) {
        return vectorizedColumnBatch.getDouble(rowId, pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        // TODO check this?
        return getString(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        BytesColumnVector.Bytes byteArray = vectorizedColumnBatch.getByteArray(rowId, pos);
        return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return vectorizedColumnBatch.getDecimal(rowId, pos, precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        return vectorizedColumnBatch.getTimestampNtz(rowId, pos, precision);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        return vectorizedColumnBatch.getTimestampLtz(rowId, pos, precision);
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return getBytes(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return vectorizedColumnBatch.getBytes(rowId, pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return vectorizedColumnBatch.getArray(rowId, pos);
    }

    // TODO: getMap() will be added in Issue #1973
    // TODO: getRow() will be added in Issue #1974

    @Override
    public int getFieldCount() {
        return vectorizedColumnBatch.getFieldCount();
    }
}

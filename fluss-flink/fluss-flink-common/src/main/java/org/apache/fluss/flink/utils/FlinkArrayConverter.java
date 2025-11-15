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

package org.apache.fluss.flink.utils;

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import static org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter.createInternalConverter;

/** Flink Array Converter. */
public class FlinkArrayConverter implements ArrayData {
    private final ArrayData arrayData;

    FlinkArrayConverter(DataType flussDataType, Object flussField) {
        DataType eleType = ((ArrayType) flussDataType).getElementType();
        this.arrayData = copyArray((org.apache.fluss.row.InternalArray) flussField, eleType);
    }

    private ArrayData copyArray(org.apache.fluss.row.InternalArray from, DataType eleType) {
        FlussDeserializationConverter converter = createInternalConverter(eleType);
        if (!eleType.isNullable()) {
            switch (eleType.getTypeRoot()) {
                case BOOLEAN:
                    return new GenericArrayData(from.toBooleanArray());
                case TINYINT:
                    return new GenericArrayData(from.toByteArray());
                case SMALLINT:
                    return new GenericArrayData(from.toShortArray());
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                    return new GenericArrayData(from.toIntArray());
                case BIGINT:
                    return new GenericArrayData(from.toLongArray());
                case FLOAT:
                    return new GenericArrayData(from.toFloatArray());
                case DOUBLE:
                    return new GenericArrayData(from.toDoubleArray());
            }
        }

        Object[] newArray = new Object[from.size()];

        for (int i = 0; i < newArray.length; ++i) {
            if (!from.isNullAt(i)) {
                newArray[i] = converter.deserialize(getFieldValue(from, i, eleType));
            } else {
                newArray[i] = null;
            }
        }

        return new GenericArrayData(newArray);
    }

    @Override
    public int size() {
        return arrayData.size();
    }

    @Override
    public boolean isNullAt(int i) {
        return arrayData.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {
        return arrayData.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
        return arrayData.getByte(i);
    }

    @Override
    public short getShort(int i) {
        return arrayData.getShort(i);
    }

    @Override
    public int getInt(int i) {
        return arrayData.getInt(i);
    }

    @Override
    public long getLong(int i) {
        return arrayData.getLong(i);
    }

    @Override
    public float getFloat(int i) {
        return arrayData.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
        return arrayData.getDouble(i);
    }

    @Override
    public StringData getString(int i) {
        return arrayData.getString(i);
    }

    @Override
    public DecimalData getDecimal(int i, int i1, int i2) {
        return arrayData.getDecimal(i, i1, i2);
    }

    @Override
    public TimestampData getTimestamp(int i, int i1) {
        return arrayData.getTimestamp(i, i1);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        return arrayData.getRawValue(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return arrayData.getBinary(i);
    }

    @Override
    public ArrayData getArray(int i) {
        return arrayData.getArray(i);
    }

    @Override
    public MapData getMap(int i) {
        return arrayData.getMap(i);
    }

    @Override
    public RowData getRow(int i, int i1) {
        return arrayData.getRow(i, i1);
    }

    @Override
    public boolean[] toBooleanArray() {
        return arrayData.toBooleanArray();
    }

    @Override
    public byte[] toByteArray() {
        return arrayData.toByteArray();
    }

    @Override
    public short[] toShortArray() {
        return arrayData.toShortArray();
    }

    @Override
    public int[] toIntArray() {
        return arrayData.toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return arrayData.toLongArray();
    }

    @Override
    public float[] toFloatArray() {
        return arrayData.toFloatArray();
    }

    @Override
    public double[] toDoubleArray() {
        return arrayData.toDoubleArray();
    }

    public ArrayData getArrayData() {
        return arrayData;
    }

    public static ArrayData deserialize(DataType flussDataType, Object flussField) {
        return new FlinkArrayConverter(flussDataType, flussField).getArrayData();
    }

    // Helper method to replace InternalRowUtils functionality
    private static Object getFieldValue(InternalArray array, int pos, DataType dataType) {
        return FlinkMapConverter.getFieldValue(array, pos, dataType);
    }
}

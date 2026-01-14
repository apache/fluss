/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.source;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.utils.BytesUtils;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;

/** Adapter for Iceberg List as Fluss InternalArray. */
public class IcebergArrayAsFlussArray implements InternalArray {

    private final List<?> icebergList;

    public IcebergArrayAsFlussArray(List<?> icebergList) {
        this.icebergList = icebergList;
    }

    @Override
    public int size() {
        return icebergList.size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return icebergList.get(pos) == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return (boolean) icebergList.get(pos);
    }

    @Override
    public byte getByte(int pos) {
        Object value = icebergList.get(pos);
        return ((Integer) value).byteValue();
    }

    @Override
    public short getShort(int pos) {
        Object value = icebergList.get(pos);
        return ((Integer) value).shortValue();
    }

    @Override
    public int getInt(int pos) {
        return (Integer) icebergList.get(pos);
    }

    @Override
    public long getLong(int pos) {
        return (Long) icebergList.get(pos);
    }

    @Override
    public float getFloat(int pos) {
        return (float) icebergList.get(pos);
    }

    @Override
    public double getDouble(int pos) {
        return (double) icebergList.get(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        String value = (String) icebergList.get(pos);
        return BinaryString.fromBytes(value.getBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        String value = (String) icebergList.get(pos);
        return BinaryString.fromBytes(value.getBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        BigDecimal bigDecimal = (BigDecimal) icebergList.get(pos);
        return Decimal.fromBigDecimal(bigDecimal, precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        LocalDateTime localDateTime = (LocalDateTime) icebergList.get(pos);
        return TimestampNtz.fromLocalDateTime(localDateTime);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        OffsetDateTime offsetDateTime = (OffsetDateTime) icebergList.get(pos);
        return TimestampLtz.fromInstant(offsetDateTime.toInstant());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        ByteBuffer byteBuffer = (ByteBuffer) icebergList.get(pos);
        return BytesUtils.toArray(byteBuffer);
    }

    @Override
    public byte[] getBytes(int pos) {
        ByteBuffer byteBuffer = (ByteBuffer) icebergList.get(pos);
        return BytesUtils.toArray(byteBuffer);
    }

    @Override
    public InternalArray getArray(int pos) {
        List<?> nestedList = (List<?>) icebergList.get(pos);
        return nestedList == null ? null : new IcebergArrayAsFlussArray(nestedList);
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] result = new boolean[icebergList.size()];
        for (int i = 0; i < icebergList.size(); i++) {
            result[i] = (boolean) icebergList.get(i);
        }
        return result;
    }

    @Override
    public byte[] toByteArray() {
        byte[] result = new byte[icebergList.size()];
        for (int i = 0; i < icebergList.size(); i++) {
            result[i] = ((Integer) icebergList.get(i)).byteValue();
        }
        return result;
    }

    @Override
    public short[] toShortArray() {
        short[] result = new short[icebergList.size()];
        for (int i = 0; i < icebergList.size(); i++) {
            result[i] = ((Integer) icebergList.get(i)).shortValue();
        }
        return result;
    }

    @Override
    public int[] toIntArray() {
        int[] result = new int[icebergList.size()];
        for (int i = 0; i < icebergList.size(); i++) {
            result[i] = (int) icebergList.get(i);
        }
        return result;
    }

    @Override
    public long[] toLongArray() {
        long[] result = new long[icebergList.size()];
        for (int i = 0; i < icebergList.size(); i++) {
            result[i] = (long) icebergList.get(i);
        }
        return result;
    }

    @Override
    public float[] toFloatArray() {
        float[] result = new float[icebergList.size()];
        for (int i = 0; i < icebergList.size(); i++) {
            result[i] = (float) icebergList.get(i);
        }
        return result;
    }

    @Override
    public double[] toDoubleArray() {
        double[] result = new double[icebergList.size()];
        for (int i = 0; i < icebergList.size(); i++) {
            result[i] = (double) icebergList.get(i);
        }
        return result;
    }
}

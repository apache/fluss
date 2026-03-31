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

package org.apache.fluss.flink.row;

import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.variant.Variant;
import org.apache.fluss.utils.MapUtils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.TimestampData;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.fluss.flink.row.FlinkAsFlussRow.fromFlinkDecimal;

/** Wraps a Flink {@link ArrayData} as a Fluss {@link InternalArray}. */
public class FlinkAsFlussArray implements InternalArray {

    /**
     * Cache of reflected methods keyed by the concrete ArrayData class. The entry is a {@code
     * Method[3]} array {@code [getVariantMethod, getMetadataMethod, getValueMethod]}.
     *
     * @see FlinkAsFlussRow#VARIANT_METHOD_CACHE for why we use direct byte passthrough
     */
    private static final ConcurrentHashMap<Class<?>, Method[]> VARIANT_METHOD_CACHE =
            MapUtils.newConcurrentHashMap();

    private final ArrayData flinkArray;

    public FlinkAsFlussArray(ArrayData flinkArray) {
        this.flinkArray = flinkArray;
    }

    @Override
    public int size() {
        return flinkArray.size();
    }

    @Override
    public boolean[] toBooleanArray() {
        return flinkArray.toBooleanArray();
    }

    @Override
    public byte[] toByteArray() {
        return flinkArray.toByteArray();
    }

    @Override
    public short[] toShortArray() {
        return flinkArray.toShortArray();
    }

    @Override
    public int[] toIntArray() {
        return flinkArray.toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return flinkArray.toLongArray();
    }

    @Override
    public float[] toFloatArray() {
        return flinkArray.toFloatArray();
    }

    @Override
    public double[] toDoubleArray() {
        return flinkArray.toDoubleArray();
    }

    @Override
    public boolean isNullAt(int pos) {
        return flinkArray.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return flinkArray.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return flinkArray.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return flinkArray.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return flinkArray.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return flinkArray.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return flinkArray.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return flinkArray.getDouble(pos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        return BinaryString.fromBytes(flinkArray.getString(pos).toBytes());
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(flinkArray.getString(pos).toBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return fromFlinkDecimal(flinkArray.getDecimal(pos, precision, scale));
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        TimestampData timestamp = flinkArray.getTimestamp(pos, precision);
        return TimestampNtz.fromMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        TimestampData timestamp = flinkArray.getTimestamp(pos, precision);
        return TimestampLtz.fromEpochMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        return flinkArray.getBinary(pos);
    }

    @Override
    public byte[] getBytes(int pos) {
        return flinkArray.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return new FlinkAsFlussArray(flinkArray.getArray(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return new FlinkAsFlussMap(flinkArray.getMap(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return new FlinkAsFlussRow(flinkArray.getRow(pos, numFields));
    }

    @Override
    public Variant getVariant(int pos) {
        try {
            Method[] methods =
                    VARIANT_METHOD_CACHE.computeIfAbsent(
                            flinkArray.getClass(),
                            clazz -> {
                                try {
                                    Method getVariantMethod =
                                            clazz.getMethod("getVariant", int.class);
                                    Class<?> variantClass =
                                            Class.forName(
                                                    "org.apache.flink.types.variant.BinaryVariant");
                                    Method getMetadataMethod =
                                            variantClass.getMethod("getMetadata");
                                    Method getValueMethod = variantClass.getMethod("getValue");
                                    return new Method[] {
                                        getVariantMethod, getMetadataMethod, getValueMethod
                                    };
                                } catch (Exception e) {
                                    throw new UnsupportedOperationException(
                                            "Variant type requires Flink 2.1 or later.", e);
                                }
                            });
            Object flinkVariant = methods[0].invoke(flinkArray, pos);
            byte[] metadata = (byte[]) methods[1].invoke(flinkVariant);
            byte[] value = (byte[]) methods[2].invoke(flinkVariant);
            return new Variant(metadata, value);
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new UnsupportedOperationException("Variant type requires Flink 2.1 or later.", e);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toObjectArray(DataType elementType) {
        Class<T> elementClass = (Class<T>) InternalRow.getDataClass(elementType);
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(elementType);
        T[] values = (T[]) Array.newInstance(elementClass, size());
        for (int i = 0; i < size(); i++) {
            if (!isNullAt(i)) {
                values[i] = (T) elementGetter.getElementOrNull(this, i);
            }
        }
        return values;
    }
}

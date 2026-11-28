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

import org.apache.fluss.row.InternalArray;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;

/** A Fluss {@link InternalArray} view of a Delta Kernel array value. */
class DeltaColumnarArray extends DeltaVectorGetters implements InternalArray {

    private final ColumnVector elements;
    private final int size;

    DeltaColumnarArray(ArrayValue array) {
        this(array.getElements(), array.getSize());
    }

    DeltaColumnarArray(ColumnVector elements, int size) {
        this.elements = elements;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    ColumnVector vectorAt(int pos) {
        return elements;
    }

    @Override
    int rowIdAt(int pos) {
        return pos;
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] values = new boolean[size];
        for (int i = 0; i < size; i++) {
            values[i] = getBoolean(i);
        }
        return values;
    }

    @Override
    public byte[] toByteArray() {
        byte[] values = new byte[size];
        for (int i = 0; i < size; i++) {
            values[i] = getByte(i);
        }
        return values;
    }

    @Override
    public short[] toShortArray() {
        short[] values = new short[size];
        for (int i = 0; i < size; i++) {
            values[i] = getShort(i);
        }
        return values;
    }

    @Override
    public int[] toIntArray() {
        int[] values = new int[size];
        for (int i = 0; i < size; i++) {
            values[i] = getInt(i);
        }
        return values;
    }

    @Override
    public long[] toLongArray() {
        long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = getLong(i);
        }
        return values;
    }

    @Override
    public float[] toFloatArray() {
        float[] values = new float[size];
        for (int i = 0; i < size; i++) {
            values[i] = getFloat(i);
        }
        return values;
    }

    @Override
    public double[] toDoubleArray() {
        double[] values = new double[size];
        for (int i = 0; i < size; i++) {
            values[i] = getDouble(i);
        }
        return values;
    }
}

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

package org.apache.fluss.row;

import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.array.AlignedArray;
import org.apache.fluss.row.array.CompactedArray;
import org.apache.fluss.row.array.IndexedArray;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.types.DataType;

/** Utility methods for {@link InternalArray}. */
public final class InternalArrayUtils {

    private InternalArrayUtils() {}

    public static Object[] toObjectArray(InternalArray array, DataType elementType) {
        Object[] result = new Object[array.size()];
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(elementType);
        for (int i = 0; i < array.size(); i++) {
            Object element = elementGetter.getElementOrNull(array, i);
            if (element instanceof BinaryString) {
                element = element.toString();
            }
            result[i] = element;
        }
        return result;
    }

    public static BinaryArray toBinaryArray(
            InternalArray array, DataType elementType, BinaryRowFormat rowFormat) {
        ArraySerializer serializer = new ArraySerializer(elementType, rowFormat);
        return serializer.toBinaryArray(array);
    }

    public static BinaryArray concatToBinaryArray(
            InternalArray left,
            InternalArray right,
            DataType elementType,
            BinaryRowFormat rowFormat) {
        if (left == null && right == null) {
            return null;
        }
        if (left == null) {
            return toBinaryArray(right, elementType, rowFormat);
        }
        if (right == null) {
            return toBinaryArray(left, elementType, rowFormat);
        }
        if (left.size() == 0) {
            return toBinaryArray(right, elementType, rowFormat);
        }
        if (right.size() == 0) {
            return toBinaryArray(left, elementType, rowFormat);
        }

        int totalSize = left.size() + right.size();
        BinaryArray result;
        switch (rowFormat) {
            case COMPACTED:
                result = new CompactedArray(elementType);
                break;
            case INDEXED:
                result = new IndexedArray(elementType);
                break;
            case ALIGNED:
                result = new AlignedArray();
                break;
            default:
                throw new IllegalArgumentException("Unsupported row format: " + rowFormat);
        }
        BinaryArrayWriter writer =
                new BinaryArrayWriter(
                        result, totalSize, BinaryArray.calculateFixLengthPartSize(elementType));
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(elementType);
        BinaryWriter.ValueWriter valueWriter =
                BinaryWriter.createValueWriter(elementType, rowFormat);
        int leftSize = left.size();
        for (int i = 0; i < leftSize; i++) {
            if (left.isNullAt(i)) {
                writer.setNullAt(i, elementType);
            } else {
                valueWriter.writeValue(writer, i, elementGetter.getElementOrNull(left, i));
            }
        }
        for (int i = 0; i < right.size(); i++) {
            int pos = leftSize + i;
            if (right.isNullAt(i)) {
                writer.setNullAt(pos, elementType);
            } else {
                valueWriter.writeValue(writer, pos, elementGetter.getElementOrNull(right, i));
            }
        }
        writer.complete();
        return result;
    }
}

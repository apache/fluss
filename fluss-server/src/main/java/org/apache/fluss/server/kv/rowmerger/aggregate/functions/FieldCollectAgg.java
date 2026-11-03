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

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.array.BinaryArraySplicer;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ByteArrayWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Collects array elements, retaining their first occurrence order when distinct is enabled. */
public class FieldCollectAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final boolean distinct;
    private final DataType elementType;
    private final boolean spliceable;
    private final InternalArray.ElementGetter elementGetter;

    /** Creates a collector for the given array type and duplicate handling. */
    public FieldCollectAgg(ArrayType dataType, boolean distinct) {
        super(dataType);
        this.distinct = distinct;
        this.elementType = dataType.getElementType();
        this.spliceable = BinaryArraySplicer.supports(elementType);
        this.elementGetter = InternalArray.createElementGetter(elementType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (inputField == null) {
            return accumulator;
        }
        if (accumulator == null && !distinct) {
            return inputField;
        }

        InternalArray accumulatorArray = (InternalArray) accumulator;
        InternalArray inputArray = (InternalArray) inputField;
        int expectedSize =
                (accumulatorArray == null ? 0 : accumulatorArray.size()) + inputArray.size();
        if (expectedSize == 0) {
            return inputField;
        }
        if (!distinct) {
            if (inputArray.size() == 0) {
                return accumulator;
            }
            if (accumulatorArray.size() == 0) {
                return inputField;
            }
            if (spliceable && BinaryArraySplicer.canSplice(accumulatorArray, inputArray)) {
                BinaryArray older = (BinaryArray) accumulatorArray;
                BinaryArray newer = (BinaryArray) inputArray;
                return new BinaryArraySplicer(elementType)
                        .append(older, 0, older.size())
                        .append(newer, 0, newer.size())
                        .build();
            }
            Object[] result = new Object[expectedSize];
            int accumulatorSize = accumulatorArray.size();
            for (int i = 0; i < accumulatorSize; i++) {
                result[i] = elementGetter.getElementOrNull(accumulatorArray, i);
            }
            for (int i = 0; i < inputArray.size(); i++) {
                result[accumulatorSize + i] = elementGetter.getElementOrNull(inputArray, i);
            }
            return new GenericArray(result);
        }

        Set<Object> elements = new LinkedHashSet<>(expectedSize);
        collectDistinct(elements, accumulatorArray);
        int accumulatorDistinctSize = elements.size();
        collectDistinct(elements, inputArray);
        if (accumulatorArray != null
                && accumulatorDistinctSize == accumulatorArray.size()
                && elements.size() == accumulatorDistinctSize) {
            return accumulator;
        }
        Object[] result = elements.toArray();
        for (int i = 0; i < result.length; i++) {
            if (result[i] instanceof ByteArrayWrapper) {
                result[i] = ((ByteArrayWrapper) result[i]).getData();
            }
        }
        return new GenericArray(result);
    }

    @Override
    public boolean requiresAggOnFirstWrite() {
        return distinct;
    }

    @Override
    public boolean supportsRetract() {
        return true;
    }

    /**
     * Removes one matching accumulated occurrence for each supplied element, including null.
     * Missing occurrences are ignored; no pending retract or distinct contribution count is kept.
     */
    @Override
    public Object retract(Object accumulator, Object retractField) {
        if (accumulator == null || retractField == null) {
            return accumulator;
        }
        InternalArray input = (InternalArray) retractField;
        if (input.size() == 0) {
            return accumulator;
        }
        List<Object> pending = new ArrayList<>(input.size());
        for (int i = 0; i < input.size(); i++) {
            pending.add(elementGetter.getElementOrNull(input, i));
        }
        InternalArray accumulated = (InternalArray) accumulator;
        List<Object> result = new ArrayList<>(accumulated.size());
        for (int i = 0; i < accumulated.size(); i++) {
            Object element = elementGetter.getElementOrNull(accumulated, i);
            if (!retractOne(pending, element)) {
                result.add(element);
            }
        }
        return new GenericArray(result.toArray());
    }

    private boolean retractOne(List<Object> pending, Object element) {
        Iterator<Object> iterator = pending.iterator();
        while (iterator.hasNext()) {
            if (elementsEqual(iterator.next(), element, elementType)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    private static boolean elementsEqual(Object left, Object right, DataType type) {
        if (left == right) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
        // Compare logical contents across generic and binary representations, including binary
        // values nested in arrays, rows and maps. Object identity is not element equality.
        switch (type.getTypeRoot()) {
            case BINARY:
            case BYTES:
                return Arrays.equals((byte[]) left, (byte[]) right);
            case ARRAY:
                InternalArray a = (InternalArray) left;
                InternalArray b = (InternalArray) right;
                if (a.size() != b.size()) {
                    return false;
                }
                DataType nested = ((ArrayType) type).getElementType();
                InternalArray.ElementGetter getter = InternalArray.createElementGetter(nested);
                for (int i = 0; i < a.size(); i++) {
                    if (!elementsEqual(
                            getter.getElementOrNull(a, i), getter.getElementOrNull(b, i), nested)) {
                        return false;
                    }
                }
                return true;
            case ROW:
                RowType rowType = (RowType) type;
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    DataType fieldType = rowType.getTypeAt(i);
                    InternalRow.FieldGetter fieldGetter =
                            InternalRow.createFieldGetter(fieldType, i);
                    if (!elementsEqual(
                            fieldGetter.getFieldOrNull((InternalRow) left),
                            fieldGetter.getFieldOrNull((InternalRow) right),
                            fieldType)) {
                        return false;
                    }
                }
                return true;
            case MAP:
                InternalMap aMap = (InternalMap) left;
                InternalMap bMap = (InternalMap) right;
                if (aMap.size() != bMap.size()) {
                    return false;
                }
                MapType mapType = (MapType) type;
                InternalArray.ElementGetter keyGetter =
                        InternalArray.createElementGetter(mapType.getKeyType());
                InternalArray.ElementGetter valueGetter =
                        InternalArray.createElementGetter(mapType.getValueType());
                for (int i = 0; i < aMap.size(); i++) {
                    boolean matched = false;
                    for (int j = 0; j < bMap.size(); j++) {
                        if (elementsEqual(
                                keyGetter.getElementOrNull(aMap.keyArray(), i),
                                keyGetter.getElementOrNull(bMap.keyArray(), j),
                                mapType.getKeyType())) {
                            if (!elementsEqual(
                                    valueGetter.getElementOrNull(aMap.valueArray(), i),
                                    valueGetter.getElementOrNull(bMap.valueArray(), j),
                                    mapType.getValueType())) {
                                return false;
                            }
                            matched = true;
                            break;
                        }
                    }
                    if (!matched) {
                        return false;
                    }
                }
                return true;
            default:
                return Objects.equals(left, right);
        }
    }

    private void collectDistinct(Set<Object> elements, InternalArray array) {
        if (array == null) {
            return;
        }
        for (int i = 0; i < array.size(); i++) {
            Object element = elementGetter.getElementOrNull(array, i);
            elements.add(
                    element instanceof byte[] ? new ByteArrayWrapper((byte[]) element) : element);
        }
    }
}

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
import org.apache.fluss.row.array.BinaryArraySplicer;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.ByteArrayWrapper;

import java.util.LinkedHashSet;
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

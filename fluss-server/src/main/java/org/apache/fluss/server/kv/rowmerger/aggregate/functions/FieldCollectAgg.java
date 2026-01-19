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

import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalArrayUtils;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Collect elements into an ARRAY. */
public class FieldCollectAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final boolean distinct;
    private final DataType elementType;
    private final DataTypeRoot elementTypeRoot;
    private final InternalArray.ElementGetter elementGetter;

    public FieldCollectAgg(ArrayType dataType, boolean distinct) {
        super(dataType);
        this.distinct = distinct;
        this.elementType = dataType.getElementType();
        this.elementTypeRoot = dataType.getElementType().getTypeRoot();
        this.elementGetter = InternalArray.createElementGetter(elementType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null && inputField == null) {
            return null;
        }

        if (accumulator == null) {
            return inputField;
        }
        if (inputField == null) {
            return accumulator;
        }

        InternalArray accumulatorArray = (InternalArray) accumulator;
        InternalArray inputArray = (InternalArray) inputField;
        int expectedSize = accumulatorArray.size() + inputArray.size();

        if (distinct) {
            int capacity = (int) (expectedSize / 0.75f) + 1;
            if (elementTypeRoot == DataTypeRoot.BINARY || elementTypeRoot == DataTypeRoot.BYTES) {
                List<Object> collection = new ArrayList<>(expectedSize);
                Set<ByteArrayWrapper> seen = new HashSet<>(capacity);
                boolean seenNull = false;
                seenNull = collectDistinctBinary(collection, seen, seenNull, accumulatorArray);
                collectDistinctBinary(collection, seen, seenNull, inputArray);
                return new GenericArray(collection.toArray());
            } else {
                Set<Object> seen = new HashSet<>(capacity);
                collectDistinct(seen, accumulatorArray);
                collectDistinct(seen, inputArray);
                return new GenericArray(seen.toArray());
            }
        }

        // No distinct: encode directly to binary to avoid extra heap allocations/copies.
        return InternalArrayUtils.concatToBinaryArray(
                accumulatorArray, inputArray, elementType, rowFormat);
    }

    @Override
    public Object aggReversed(Object accumulator, Object inputField) {
        // For collect, order doesn't matter for distinct sets
        // For non-distinct, we just append, so reversed is same as normal
        return agg(accumulator, inputField);
    }

    private void collectDistinct(Set<Object> seen, InternalArray array) {
        for (int i = 0; i < array.size(); i++) {
            Object element = elementGetter.getElementOrNull(array, i);
            seen.add(element);
        }
    }

    private boolean collectDistinctBinary(
            List<Object> collection,
            Set<ByteArrayWrapper> seen,
            boolean seenNull,
            InternalArray array) {
        for (int i = 0; i < array.size(); i++) {
            Object element = elementGetter.getElementOrNull(array, i);
            if (element == null) {
                if (!seenNull) {
                    seenNull = true;
                    collection.add(null);
                }
                continue;
            }
            byte[] bytes = (byte[]) element;
            if (seen.add(new ByteArrayWrapper(bytes))) {
                collection.add(bytes);
            }
        }
        return seenNull;
    }

    private static final class ByteArrayWrapper {
        private final byte[] bytes;

        private ByteArrayWrapper(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ByteArrayWrapper)) {
                return false;
            }
            ByteArrayWrapper other = (ByteArrayWrapper) obj;
            return Arrays.equals(bytes, other.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}

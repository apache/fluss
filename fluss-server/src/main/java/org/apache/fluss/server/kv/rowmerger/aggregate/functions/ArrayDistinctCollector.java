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

import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.ByteArrayWrapper;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

/** Distinct collection using element equality, with byte arrays compared by content. */
final class ArrayDistinctCollector implements Serializable {
    private static final long serialVersionUID = 1L;

    private final InternalArray.ElementGetter valueGetter;

    ArrayDistinctCollector(DataType elementType) {
        valueGetter = InternalArray.createElementGetter(elementType);
    }

    Accumulator newAccumulator() {
        return new Accumulator();
    }

    /**
     * Owns the collection for one aggregation call; the collector retains only type configuration.
     */
    final class Accumulator {
        private final Set<Object> elements = new LinkedHashSet<>();

        void add(InternalArray source, int position) {
            Object value = valueGetter.getElementOrNull(source, position);
            elements.add(value instanceof byte[] ? new ByteArrayWrapper((byte[]) value) : value);
        }

        int size() {
            return elements.size();
        }

        InternalArray toArray() {
            Object[] result = elements.toArray();
            for (int i = 0; i < result.length; i++) {
                result[i] = unwrap(result[i]);
            }
            return new GenericArray(result);
        }

        InternalArray toReversedArray() {
            Object[] result = new Object[elements.size()];
            int position = result.length;
            for (Object value : elements) {
                result[--position] = unwrap(value);
            }
            return new GenericArray(result);
        }
    }

    private static Object unwrap(Object value) {
        return value instanceof ByteArrayWrapper ? ((ByteArrayWrapper) value).getData() : value;
    }
}

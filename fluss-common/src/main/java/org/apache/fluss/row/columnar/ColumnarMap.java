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

package org.apache.fluss.row.columnar;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;

import java.util.Objects;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Columnar map to support access to vector column data.
 *
 * @since 0.9
 */
@Internal
public class ColumnarMap implements InternalMap {

    private final InternalArray keyArray;
    private final InternalArray valueArray;

    public ColumnarMap(InternalArray keyArray, InternalArray valueArray) {
        checkNotNull(keyArray, "keyArray must not be null");
        checkNotNull(valueArray, "valueArray must not be null");
        if (keyArray.size() != valueArray.size()) {
            throw new IllegalArgumentException(
                    "Key array size and value array size must be equal. Key array size: "
                            + keyArray.size()
                            + ", value array size: "
                            + valueArray.size());
        }
        this.keyArray = keyArray;
        this.valueArray = valueArray;
    }

    public ColumnarMap(
            ColumnVector keyColumnVector,
            ColumnVector valueColumnVector,
            int offset,
            int numElements) {
        this(
                new ColumnarArray(keyColumnVector, offset, numElements),
                new ColumnarArray(valueColumnVector, offset, numElements));
    }

    @Override
    public int size() {
        return keyArray.size();
    }

    @Override
    public InternalArray keyArray() {
        return keyArray;
    }

    @Override
    public InternalArray valueArray() {
        return valueArray;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InternalMap)) {
            return false;
        }
        InternalMap that = (InternalMap) o;
        return Objects.equals(keyArray, that.keyArray())
                && Objects.equals(valueArray, that.valueArray());
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyArray, valueArray);
    }

    @Override
    public String toString() {
        return "ColumnarMap{" + "keyArray=" + keyArray + ", valueArray=" + valueArray + '}';
    }
}

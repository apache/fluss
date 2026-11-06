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

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.array.BinaryArraySplicer;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;

import javax.annotation.Nullable;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Appends input arrays to the accumulator and keeps the last {@code size} elements. */
public class FieldLastNAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final int size;
    private final DataType elementType;
    private final boolean spliceable;
    private final InternalArray.ElementGetter elementGetter;

    /**
     * Creates a LAST_N aggregator.
     *
     * @param dataType the array type of the aggregated column
     * @param size the number of elements to keep, must be positive
     */
    public FieldLastNAgg(ArrayType dataType, int size) {
        super(dataType);
        checkArgument(size > 0, "size must be positive, but was %s", size);
        this.size = size;
        this.elementType = dataType.getElementType();
        this.spliceable = BinaryArraySplicer.supports(elementType);
        this.elementGetter = InternalArray.createElementGetter(elementType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (inputField == null) {
            return accumulator;
        }
        InternalArray older = (InternalArray) accumulator;
        InternalArray newer = (InternalArray) inputField;
        if (older != null && newer.size() == 0) {
            return accumulator;
        }
        if (older == null && newer.size() <= size) {
            return inputField;
        }
        int olderSize = older == null ? 0 : older.size();
        int total = olderSize + newer.size();
        int skip = Math.max(0, total - size);
        if (spliceable && BinaryArraySplicer.canSplice(older, newer)) {
            return splice((BinaryArray) older, olderSize, (BinaryArray) newer, skip);
        }
        Object[] result = new Object[total - skip];
        for (int i = skip; i < total; i++) {
            result[i - skip] =
                    i < olderSize
                            ? elementGetter.getElementOrNull(older, i)
                            : elementGetter.getElementOrNull(newer, i - olderSize);
        }
        return new GenericArray(result);
    }

    /** Copies the kept elements as bytes, without reading each one. */
    private BinaryArray splice(
            @Nullable BinaryArray older, int olderSize, BinaryArray newer, int skip) {
        BinaryArraySplicer splicer = new BinaryArraySplicer(elementType);
        int skipOlder = Math.min(skip, olderSize);
        if (older != null) {
            splicer.append(older, skipOlder, olderSize - skipOlder);
        }
        int skipNewer = skip - skipOlder;
        return splicer.append(newer, skipNewer, newer.size() - skipNewer).build();
    }

    @Override
    public boolean requiresAggOnFirstWrite() {
        return true;
    }
}

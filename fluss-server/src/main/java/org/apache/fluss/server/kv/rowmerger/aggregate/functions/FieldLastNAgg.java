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
import org.apache.fluss.types.ArrayType;

/** Appends input arrays to the accumulator and keeps the last {@code size} elements. */
public class FieldLastNAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final int size;
    private final InternalArray.ElementGetter elementGetter;

    public FieldLastNAgg(ArrayType dataType, int size) {
        super(dataType);
        this.size = size;
        this.elementGetter = InternalArray.createElementGetter(dataType.getElementType());
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (inputField == null) {
            return accumulator;
        }
        InternalArray older = (InternalArray) accumulator;
        InternalArray newer = (InternalArray) inputField;
        int olderSize = older == null ? 0 : older.size();
        int total = olderSize + newer.size();
        int skip = Math.max(0, total - size);
        Object[] result = new Object[total - skip];
        for (int i = skip; i < total; i++) {
            result[i - skip] =
                    i < olderSize
                            ? elementGetter.getElementOrNull(older, i)
                            : elementGetter.getElementOrNull(newer, i - olderSize);
        }
        return new GenericArray(result);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.row.array;

import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryMap;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.map.IndexedMap;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

/**
 * A {@link BinaryArray} that uses {@link org.apache.fluss.row.indexed.IndexedRow} as the binary
 * format for arrays of nested row type.
 */
public class IndexedArray extends BinaryArray {

    private final DataType elementType;

    private transient DataType[] nestedFields;

    public IndexedArray(DataType elementType) {
        this.elementType = elementType;
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        assertIndexIsValid(pos);
        if (elementType instanceof RowType) {
            if (nestedFields == null) {
                nestedFields = ((RowType) elementType).getFieldTypes().toArray(new DataType[0]);
            }
            if (nestedFields.length != numFields) {
                throw new IllegalArgumentException(
                        "Unexpected number of fields " + numFields + " for " + elementType);
            }
            return BinarySegmentUtils.readIndexedRow(segments, offset, getLong(pos), nestedFields);
        } else {
            throw new IllegalArgumentException("Can not get row from Array of type " + elementType);
        }
    }

    @Override
    protected BinaryArray createNestedArrayInstance() {
        if (elementType instanceof ArrayType) {
            return new IndexedArray(((ArrayType) elementType).getElementType());
        } else {
            throw new IllegalArgumentException(
                    "Can not get nested array from Array of type " + elementType);
        }
    }

    @Override
    protected BinaryMap createNestedMapInstance() {
        if (elementType instanceof MapType) {
            MapType mapType = (MapType) elementType;
            return new IndexedMap(mapType.getKeyType(), mapType.getValueType());
        } else {
            throw new IllegalArgumentException(
                    "Can not get nested map from Array of type " + elementType);
        }
    }
}

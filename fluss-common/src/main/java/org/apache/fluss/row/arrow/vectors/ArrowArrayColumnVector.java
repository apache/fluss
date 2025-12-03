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

package org.apache.fluss.row.arrow.vectors;

import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.columnar.ArrayColumnVector;
import org.apache.fluss.row.columnar.ColumnVector;
import org.apache.fluss.row.columnar.ColumnarArray;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** ArrowArrayColumnVector is a wrapper class for Arrow ListVector. */
public class ArrowArrayColumnVector implements ArrayColumnVector {
    /** Container which is used to store the sequence of array values of a column to read. */
    private final ListVector listVector;

    private final ColumnVector elementVector;

    public ArrowArrayColumnVector(ListVector listVector, ColumnVector elementVector) {
        this.listVector = checkNotNull(listVector);
        this.elementVector = checkNotNull(elementVector);
    }

    @Override
    public InternalArray getArray(int index) {
        int start = listVector.getElementStartIndex(index);
        int end = listVector.getElementEndIndex(index);
        return new ColumnarArray(elementVector, start, end - start);
    }

    @Override
    public boolean isNullAt(int i) {
        return listVector.isNull(i);
    }
}

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
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.ArrowUtils;

/** ArrowArrayColumnVector is a wrapper class for Arrow ListVector. */
public class ArrowArrayColumnVector implements ArrayColumnVector {
    private boolean inited = false;
    private FieldVector vector;
    private final DataType elementType;
    private ColumnVector columnVector;

    public ArrowArrayColumnVector(FieldVector vector, DataType elementType) {
        this.vector = vector;
        this.elementType = elementType;
    }

    private void init() {
        if (!inited) {
            FieldVector child = ((ListVector) vector).getDataVector();
            this.columnVector = ArrowUtils.createArrowColumnVector(child, elementType);
            inited = true;
        }
    }

    @Override
    public InternalArray getArray(int index) {
        init();
        ListVector listVector = (ListVector) vector;
        int start = listVector.getElementStartIndex(index);
        int end = listVector.getElementEndIndex(index);
        return new ColumnarArray(columnVector, start, end - start);
    }

    @Override
    public ColumnVector getColumnVector() {
        init();
        return columnVector;
    }

    @Override
    public boolean isNullAt(int index) {
        return vector.isNull(index);
    }
}

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

import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.columnar.ColumnVector;
import org.apache.fluss.row.columnar.ColumnarMap;
import org.apache.fluss.row.columnar.MapColumnVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.MapVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import org.apache.fluss.types.DataType;
import org.apache.fluss.utils.ArrowUtils;

/** ArrowMapColumnVector is a wrapper class for Arrow MapVector. */
public class ArrowMapColumnVector implements MapColumnVector {
    private final MapVector mapVector;
    private final ColumnVector keyColumnVector;
    private final ColumnVector valueColumnVector;

    public ArrowMapColumnVector(FieldVector vector, DataType keyType, DataType valueType) {
        this.mapVector = (MapVector) vector;
        FieldVector dataVector = mapVector.getDataVector();
        StructVector structVector = (StructVector) dataVector;
        FieldVector keyVector = structVector.getChild(MapVector.KEY_NAME);
        FieldVector valueVector = structVector.getChild(MapVector.VALUE_NAME);
        this.keyColumnVector = ArrowUtils.createArrowColumnVector(keyVector, keyType);
        this.valueColumnVector = ArrowUtils.createArrowColumnVector(valueVector, valueType);
    }

    @Override
    public InternalMap getMap(int i) {
        int start = mapVector.getElementStartIndex(i);
        int end = mapVector.getElementEndIndex(i);

        return new ColumnarMap(keyColumnVector, valueColumnVector, start, end - start);
    }

    @Override
    public boolean isNullAt(int i) {
        return mapVector.isNull(i);
    }
}

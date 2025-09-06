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

import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.columnar.ColumnVector;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.row.columnar.RowColumnVector;
import org.apache.fluss.row.columnar.VectorizedColumnBatch;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import java.util.List;

/** ArrowRowColumnVector is a wrapper class for Arrow RowVector. */
public class ArrowRowColumnVector implements RowColumnVector {
    private boolean inited = false;
    private final FieldVector vector;
    private final RowType rowType;
    private VectorizedColumnBatch vectorizedColumnBatch;

    public ArrowRowColumnVector(FieldVector vector, RowType rowType) {
        this.vector = vector;
        this.rowType = rowType;
    }

    private void init() {
        if (!inited) {
            List<FieldVector> children = ((StructVector) vector).getChildrenFromFields();
            ColumnVector[] vectors = new ColumnVector[children.size()];
            for (int i = 0; i < children.size(); i++) {
                vectors[i] =
                        ArrowUtils.createArrowColumnVector(children.get(i), rowType.getTypeAt(i));
            }
            this.vectorizedColumnBatch = new VectorizedColumnBatch(vectors);
            inited = true;
        }
    }

    @Override
    public InternalRow getRow(int i) {
        init();
        return new ColumnarRow(vectorizedColumnBatch, i);
    }

    @Override
    public VectorizedColumnBatch getBatch() {
        init();
        return vectorizedColumnBatch;
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row.arrow.vectors;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.columnar.ArrayColumnVector;
import org.apache.fluss.row.columnar.ColumnarArray;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.Float4Vector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.FixedSizeListVector;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * {@link org.apache.fluss.row.columnar.ColumnVector} backed by a shaded Arrow {@code
 * FixedSizeListVector<Float32>} for reading {@link org.apache.fluss.types.VectorType} columns.
 *
 * <p>Each row {@code i} maps to child Float32 elements at indices {@code [i*dimension,
 * (i+1)*dimension)} in the child {@code Float4Vector}. The returned {@link InternalArray} is a
 * {@link ColumnarArray} view over an {@link ArrowFloatColumnVector} wrapping the child vector.
 */
@Internal
public class ArrowVectorColumnVector implements ArrayColumnVector {

    /** The FixedSizeListVector holding per-row validity and the stride-based child data. */
    private final FixedSizeListVector vector;

    /** The fixed number of float elements per row (equals the declared VECTOR dimension). */
    private final int dimension;

    /**
     * A ColumnVector view over the child Float4Vector, shared across all rows for zero-copy {@link
     * ColumnarArray} slicing.
     */
    private final ArrowFloatColumnVector elementVector;

    /**
     * Creates a new {@link ArrowVectorColumnVector}.
     *
     * @param vector the {@code FixedSizeListVector} to read from
     * @param dimension the declared VECTOR dimension (must equal {@code vector.getListSize()})
     */
    public ArrowVectorColumnVector(FixedSizeListVector vector, int dimension) {
        this.vector = checkNotNull(vector);
        this.dimension = dimension;
        this.elementVector =
                new ArrowFloatColumnVector((Float4Vector) checkNotNull(vector.getDataVector()));
    }

    /**
     * Returns the vector value at row {@code i} as an {@link InternalArray} of floats.
     *
     * <p>The returned array is a {@link ColumnarArray} window into the shared child vector,
     * starting at element index {@code i * dimension} with length {@code dimension}.
     *
     * @param i row index (0-based)
     * @return an {@link InternalArray} of {@code dimension} floats
     */
    @Override
    public InternalArray getArray(int i) {
        if (vector.getDataVector().getValueCount() == 0 && vector.getValueCount() > 0) {
            vector.getDataVector().setValueCount(vector.getValueCount() * dimension);
        }
        int start = i * dimension;
        return new ColumnarArray(elementVector, start, dimension);
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }
}

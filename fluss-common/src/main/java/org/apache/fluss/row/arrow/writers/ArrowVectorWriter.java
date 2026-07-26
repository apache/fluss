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

package org.apache.fluss.row.arrow.writers;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.FixedSizeListVector;

/**
 * {@link ArrowFieldWriter} for {@link org.apache.fluss.types.VectorType}, writing {@link
 * InternalArray} of float values into a shaded Arrow {@code FixedSizeListVector<Float32>}.
 *
 * <p>Unlike {@link ArrowArrayWriter} which uses {@code ListVector}'s {@code startNewValue/endValue}
 * protocol, {@code FixedSizeListVector} uses stride-based child offsets: row {@code i} occupies
 * child elements at indices {@code [i*listSize, (i+1)*listSize)}. Validity is set with {@code
 * setNotNull(rowIndex)} and child elements are written via the delegated element writer.
 */
@Internal
public class ArrowVectorWriter extends ArrowFieldWriter {

    /** Writer for the Float32 child vector elements. */
    private final ArrowFieldWriter elementWriter;

    /**
     * Running count of child element slots already written. Incremented by {@code listSize} for
     * each row (including null rows, since FixedSizeListVector still allocates child slots for null
     * rows).
     */
    private int offset;

    /**
     * Creates a new {@link ArrowVectorWriter}.
     *
     * @param vector the {@code FixedSizeListVector} to write into
     * @param elementWriter writer for the Float32 child vector
     */
    public ArrowVectorWriter(FixedSizeListVector vector, ArrowFieldWriter elementWriter) {
        super(vector);
        this.elementWriter = elementWriter;
        this.offset = 0;
    }

    @Override
    public void doWrite(int rowIndex, DataGetters getters, int ordinal, boolean handleSafe) {
        InternalArray array = getters.getArray(ordinal);
        FixedSizeListVector listVector = (FixedSizeListVector) fieldVector;
        int listSize = listVector.getListSize();
        if (array.size() != listSize) {
            throw new IllegalArgumentException(
                    String.format(
                            "VECTOR dimension mismatch: expected %d elements but got %d.",
                            listSize, array.size()));
        }
        listVector.setNotNull(rowIndex);
        for (int i = 0; i < listSize; i++) {
            int elementIndex = offset + i;
            // Use element-based index to determine handleSafe, not parent row count.
            // When row count < INITIAL_CAPACITY but total elements > INITIAL_CAPACITY,
            // we need safe mode for elements beyond the initial capacity.
            boolean elementHandleSafe = elementIndex >= ArrowWriter.INITIAL_CAPACITY;
            elementWriter.write(elementIndex, array, i, handleSafe || elementHandleSafe);
        }
        offset += listSize;
    }

    /**
     * Overrides the base {@link ArrowFieldWriter#write} to always advance the {@code offset}
     * counter by {@code listSize}, even for null rows.
     *
     * <p>This is required because {@code FixedSizeListVector} uses stride-based child indexing: row
     * {@code i}'s child elements always occupy positions {@code [i*listSize, (i+1)*listSize)},
     * regardless of whether the row is null. The base class short-circuits to {@code
     * setNull(rowIndex)} without calling {@code doWrite}, so {@code offset} would never be
     * incremented for null rows, causing subsequent non-null rows to write their child elements at
     * the wrong positions.
     */
    @Override
    public void write(int rowIndex, DataGetters getters, int ordinal, boolean handleSafe) {
        if (getters.isNullAt(ordinal)) {
            fieldVector.setNull(rowIndex);
            offset += ((FixedSizeListVector) fieldVector).getListSize();
        } else {
            doWrite(rowIndex, getters, ordinal, handleSafe);
        }
    }

    /**
     * Resets the writer state for reuse (e.g. after batch serialization). The child element writer
     * and offset counter are both reset to their initial state.
     */
    @Override
    public void reset() {
        super.reset();
        elementWriter.reset();
        offset = 0;
    }
}

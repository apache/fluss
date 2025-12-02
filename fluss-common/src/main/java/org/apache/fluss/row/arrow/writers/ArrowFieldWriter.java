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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Base class for arrow field writer which is used to convert a field to an Arrow format. */
@Internal
public abstract class ArrowFieldWriter {

    /** Container which is used to store the written sequence of values of a column. */
    protected final FieldVector fieldVector;

    public ArrowFieldWriter(FieldVector fieldVector) {
        this.fieldVector = checkNotNull(fieldVector);
    }

    /**
     * Sets the field value as the field at the specified ordinal of the specified row.
     *
     * <p>Note: The element at ordinal is already guaranteed to be not null.
     *
     * @param rowIndex The row index in the FieldVector to write to.
     * @param getters The DataGetters to get the value from, can be a {@link InternalRow} or {@link
     *     InternalArray}.
     * @param ordinal The ordinal of the field to write. It's the column index in case of {@link
     *     InternalRow}, and the element index in case of {@link InternalArray}.
     * @param handleSafe Whether to handle the safe write, see {@link FieldVector}'s setSafe method.
     */
    public abstract void doWrite(
            int rowIndex, DataGetters getters, int ordinal, boolean handleSafe);

    /** Writes the specified ordinal of the specified row. */
    public void write(int rowIndex, DataGetters getters, int ordinal, boolean handleSafe) {
        if (getters.isNullAt(ordinal)) {
            fieldVector.setNull(rowIndex);
        } else {
            doWrite(rowIndex, getters, ordinal, handleSafe);
        }
    }

    /** Resets the state of the writer to write the next batch of fields. */
    public void reset() {
        fieldVector.reset();
    }
}

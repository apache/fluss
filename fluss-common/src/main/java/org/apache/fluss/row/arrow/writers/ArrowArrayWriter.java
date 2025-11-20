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

package org.apache.fluss.row.arrow.writers;

import org.apache.fluss.row.DataGetters;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.ListVector;

/** {@link ArrowFieldWriter} for Array. */
public class ArrowArrayWriter extends ArrowFieldWriter {

    private final ArrowFieldWriter elementWriter;
    private int offset;

    public ArrowArrayWriter(FieldVector fieldVector, ArrowFieldWriter elementWriter) {
        super(fieldVector);
        this.elementWriter = elementWriter;
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        InternalArray array = row.getArray(ordinal);
        ListVector listVector = (ListVector) fieldVector;
        listVector.startNewValue(rowIndex);
        for (int arrIndex = 0; arrIndex < array.size(); arrIndex++) {
            int fieldIndex = offset + arrIndex;
            elementWriter.write(fieldIndex, array, arrIndex, handleSafe);
        }
        offset += array.size();
        listVector.endValue(rowIndex, array.size());
    }
}

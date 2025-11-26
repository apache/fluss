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
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;

/** {@link ArrowFieldWriter} for Row. */
public class ArrowRowWriter extends ArrowFieldWriter {

    private final ArrowFieldWriter[] fieldWriters;

    public ArrowRowWriter(FieldVector fieldVector, ArrowFieldWriter[] fieldWriters) {
        super(fieldVector);
        this.fieldWriters = fieldWriters;
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        InternalRow nestedRow = row.getRow(ordinal, fieldWriters.length);
        StructVector structVector = (StructVector) fieldVector;

        structVector.setIndexDefined(rowIndex);
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(rowIndex, nestedRow, i, handleSafe);
        }
    }
}

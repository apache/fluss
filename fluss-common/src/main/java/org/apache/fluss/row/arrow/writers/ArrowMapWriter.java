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
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.FieldVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.MapVector;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.complex.StructVector;

/** {@link ArrowFieldWriter} for Map. */
public class ArrowMapWriter extends ArrowFieldWriter {

    private final ArrowFieldWriter keyWriter;
    private final ArrowFieldWriter valueWriter;
    private int offset;

    public ArrowMapWriter(
            FieldVector fieldVector, ArrowFieldWriter keyWriter, ArrowFieldWriter valueWriter) {
        super(fieldVector);
        this.keyWriter = keyWriter;
        this.valueWriter = valueWriter;
    }

    @Override
    public void doWrite(int rowIndex, DataGetters row, int ordinal, boolean handleSafe) {
        InternalMap map = row.getMap(ordinal);
        MapVector mapVector = (MapVector) fieldVector;
        StructVector structVector = (StructVector) mapVector.getDataVector();

        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();

        mapVector.startNewValue(rowIndex);
        for (int i = 0; i < map.size(); i++) {
            int fieldIndex = offset + i;
            structVector.setIndexDefined(fieldIndex);
            // Use element-based index to determine handleSafe, not parent row count.
            // When row count < INITIAL_CAPACITY but total map entries > INITIAL_CAPACITY,
            // we need to use safe mode for entries beyond the initial capacity.
            boolean elementHandleSafe = fieldIndex >= ArrowWriter.INITIAL_CAPACITY;
            keyWriter.write(fieldIndex, keyArray, i, elementHandleSafe);
            valueWriter.write(fieldIndex, valueArray, i, elementHandleSafe);
        }
        offset += map.size();
        mapVector.endValue(rowIndex, map.size());
    }

    @Override
    public void reset() {
        super.reset();
        keyWriter.reset();
        valueWriter.reset();
        offset = 0;
    }
}

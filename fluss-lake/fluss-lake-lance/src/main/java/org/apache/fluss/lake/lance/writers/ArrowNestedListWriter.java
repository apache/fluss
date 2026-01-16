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

package org.apache.fluss.lake.lance.writers;

import org.apache.fluss.row.InternalArray;

import org.apache.arrow.vector.complex.ListVector;

/** {@link ArrowFieldWriter} for nested arrays (ARRAY&lt;ARRAY&lt;T&gt;&gt;). */
public class ArrowNestedListWriter extends ArrowFieldWriter<InternalArray> {

    private final ArrowFieldWriter<InternalArray> elementWriter;
    private int offset;

    public ArrowNestedListWriter(
            ListVector listVector, ArrowFieldWriter<InternalArray> elementWriter) {
        super(listVector);
        this.elementWriter = elementWriter;
    }

    @Override
    public void doWrite(InternalArray array, int ordinal, boolean handleSafe) {
        ListVector listVector = (ListVector) getValueVector();
        int rowIndex = getCount();

        if (array.isNullAt(ordinal)) {
            listVector.setNull(rowIndex);
        } else {
            InternalArray nestedArray = array.getArray(ordinal);
            listVector.startNewValue(rowIndex);
            for (int i = 0; i < nestedArray.size(); i++) {
                elementWriter.write(nestedArray, i, handleSafe);
            }
            listVector.endValue(rowIndex, nestedArray.size());
        }
    }
}

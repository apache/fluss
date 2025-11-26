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

package org.apache.fluss.testutils;

import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import org.assertj.core.api.AbstractAssert;

import static org.apache.fluss.types.DataTypeRoot.ARRAY;
import static org.apache.fluss.types.DataTypeRoot.MAP;
import static org.apache.fluss.types.DataTypeRoot.ROW;
import static org.assertj.core.api.Assertions.assertThat;

/** Extend assertj assertions to easily assert {@link InternalRow}. */
public class InternalRowAssert extends AbstractAssert<InternalRowAssert, InternalRow> {

    private RowType rowType;
    private InternalRow.FieldGetter[] fieldGetters;

    /** Creates assertions for {@link InternalRow}. */
    public static InternalRowAssert assertThatRow(InternalRow actual) {
        return new InternalRowAssert(actual);
    }

    private InternalRowAssert(InternalRow actual) {
        super(actual, InternalRowAssert.class);
    }

    public InternalRowAssert withSchema(RowType rowType) {
        this.rowType = rowType;
        this.fieldGetters = new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }
        return this;
    }

    public InternalRowAssert isEqualTo(InternalRow expected) {
        if ((actual instanceof IndexedRow && expected instanceof IndexedRow)
                || (actual instanceof GenericRow && expected instanceof GenericRow)) {
            assertThat(actual).isEqualTo(expected);
        }

        if (rowType == null) {
            throw new IllegalStateException(
                    "InternalRowAssert#isEqualTo(InternalRow) must be invoked after #withSchema(RowType).");
        }
        assertThat(actual.getFieldCount())
                .as("InternalRow#getFieldCount()")
                .isEqualTo(expected.getFieldCount());
        for (int i = 0; i < actual.getFieldCount(); i++) {
            assertThat(actual.isNullAt(i))
                    .as("InternalRow#isNullAt(" + i + ")")
                    .isEqualTo(expected.isNullAt(i));
            if (!actual.isNullAt(i)) {
                DataType fieldType = rowType.getTypeAt(i);
                Object actualField = fieldGetters[i].getFieldOrNull(actual);
                Object expectedField = fieldGetters[i].getFieldOrNull(expected);

                // Special handling for Array, Map, and Row types to compare content not instance
                if (fieldType.getTypeRoot() == ARRAY) {
                    InternalArrayAssert.assertThatArray((InternalArray) actualField)
                            .withElementType(((ArrayType) fieldType).getElementType())
                            .as("InternalRow#get" + fieldType.getTypeRoot() + "(" + i + ")")
                            .isEqualTo((InternalArray) expectedField);
                } else if (fieldType.getTypeRoot() == MAP) {
                    // TODO: Add Map type assertion support in future
                    throw new UnsupportedOperationException("Map type not supported yet");
                } else if (fieldType.getTypeRoot() == ROW) {
                    assertThatRow((InternalRow) actualField)
                            .withSchema((RowType) fieldType)
                            .as("InternalRow#get" + fieldType.getTypeRoot() + "(" + i + ")")
                            .isEqualTo((InternalRow) expectedField);
                } else {
                    assertThat(actualField)
                            .as("InternalRow#get" + fieldType.getTypeRoot() + "(" + i + ")")
                            .isEqualTo(expectedField);
                }
            }
        }
        return this;
    }
}

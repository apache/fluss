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

package com.alibaba.fluss.testutils;

import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.RowType;

import org.assertj.core.api.AbstractAssert;

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
                assertThat(fieldGetters[i].getFieldOrNull(actual))
                        .as("InternalRow#get" + rowType.getTypeAt(i).getTypeRoot() + "(" + i + ")")
                        .isEqualTo(fieldGetters[i].getFieldOrNull(expected));
            }
        }
        return this;
    }
}

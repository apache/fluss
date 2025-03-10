/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.testutils;

import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.RowType;

import org.assertj.core.api.AbstractAssert;

import java.util.List;

import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Extend assertj assertions to easily assert List of {@link InternalRow}. */
public class InternalRowListAssert
        extends AbstractAssert<InternalRowListAssert, List<? extends InternalRow>> {

    private RowType rowType;

    /** Creates assertions for List of {@link InternalRow}. */
    public static InternalRowListAssert assertThatRows(List<? extends InternalRow> actual) {
        return new InternalRowListAssert(actual);
    }

    private InternalRowListAssert(List<? extends InternalRow> actual) {
        super(actual, InternalRowListAssert.class);
    }

    public InternalRowListAssert withSchema(RowType rowType) {
        this.rowType = rowType;
        return this;
    }

    public InternalRowListAssert isEqualTo(List<? extends InternalRow> expected) {
        assertThat(actual.size()).as("List<InternalRow>#size()").isEqualTo(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            InternalRow actualRow = actual.get(i);
            InternalRow expectedRow = expected.get(i);
            assertThatRow(actualRow).withSchema(rowType).isEqualTo(expectedRow);
        }
        return this;
    }
}

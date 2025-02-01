/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.connector.flink.source.testutils;

import com.alibaba.fluss.connector.flink.source.reader.RecordAndPos;
import com.alibaba.fluss.types.RowType;

import org.assertj.core.api.AbstractAssert;

import static com.alibaba.fluss.testutils.InternalRowAssert.assertThatRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Extend assertj assertions to easily assert {@link RecordAndPos}. */
public class RecordAndPosAssert extends AbstractAssert<RecordAndPosAssert, RecordAndPos> {

    private RowType rowType;

    /** Creates assertions for {@link RecordAndPos}. */
    public static RecordAndPosAssert assertThatRecordAndPos(RecordAndPos actual) {
        return new RecordAndPosAssert(actual);
    }

    private RecordAndPosAssert(RecordAndPos actual) {
        super(actual, RecordAndPosAssert.class);
    }

    public RecordAndPosAssert withSchema(RowType rowType) {
        this.rowType = rowType;
        return this;
    }

    public RecordAndPosAssert isEqualTo(RecordAndPos expected) {
        assertThat(actual.readRecordsCount())
                .as("RecordAndPos#readRecordsCount()")
                .isEqualTo(expected.readRecordsCount());
        assertThat(actual.record().logOffset())
                .as("RecordAndPos#reocrd()#getOffset()")
                .isEqualTo(expected.record().logOffset());
        assertThat(actual.record().getRowKind())
                .as("RecordAndPos#reocrd()#getRowKind()")
                .isEqualTo(expected.record().getRowKind());
        assertThatRow(actual.record().getRow())
                .withSchema(rowType)
                .isEqualTo(expected.record().getRow());
        return this;
    }
}

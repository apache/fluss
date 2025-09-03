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

import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.types.RowType;

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

/** Extend assertj assertions to easily assert {@link LogRecord}. */
public class LogRecordAssert extends AbstractAssert<LogRecordAssert, LogRecord> {

    private RowType rowType;

    /** Creates assertions for {@link LogRecord}. */
    public static LogRecordAssert assertThatLogRecord(LogRecord actual) {
        return new LogRecordAssert(actual);
    }

    private LogRecordAssert(LogRecord actual) {
        super(actual, LogRecordAssert.class);
    }

    public LogRecordAssert withSchema(RowType rowType) {
        this.rowType = rowType;
        return this;
    }

    public LogRecordAssert isEqualTo(LogRecord expected) {
        assertThat(actual.logOffset()).as("LogRecord#logOffset()").isEqualTo(expected.logOffset());
        assertThat(actual.getChangeType())
                .as("LogRecord#getChangeType()")
                .isEqualTo(expected.getChangeType());
        InternalRow actualRow = actual.getRow();
        InternalRow expectedRow = expected.getRow();
        if (actualRow instanceof IndexedRow && expectedRow instanceof IndexedRow) {
            assertThat((actualRow)).isEqualTo(expectedRow);
        } else {
            // slow path
            InternalRowAssert.assertThatRow(actualRow).withSchema(rowType).isEqualTo(expectedRow);
        }
        return this;
    }
}

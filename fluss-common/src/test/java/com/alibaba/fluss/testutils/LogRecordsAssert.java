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

package com.alibaba.fluss.testutils;

import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.types.RowType;

import org.assertj.core.api.AbstractAssert;

import java.util.Iterator;

import static com.alibaba.fluss.testutils.LogRecordBatchAssert.assertThatLogRecordBatch;
import static org.assertj.core.api.Assertions.assertThat;

/** Extend assertj assertions to easily assert {@link LogRecords}. */
public class LogRecordsAssert extends AbstractAssert<LogRecordsAssert, LogRecords> {

    /** Creates assertions for {@link LogRecords}. */
    public static LogRecordsAssert assertThatLogRecords(LogRecords actual) {
        return new LogRecordsAssert(actual);
    }

    private RowType rowType;
    private boolean assertCheckSum = true;

    private LogRecordsAssert(LogRecords actual) {
        super(actual, LogRecordsAssert.class);
    }

    public LogRecordsAssert withSchema(RowType rowType) {
        this.rowType = rowType;
        return this;
    }

    public LogRecordsAssert assertCheckSum(boolean assertCheckSum) {
        this.assertCheckSum = assertCheckSum;
        return this;
    }

    public LogRecordsAssert hasBatchesCount(int batchesCount) {
        assertThat(actual.batches()).hasSize(batchesCount);
        return this;
    }

    public LogRecordsAssert isEqualTo(LogRecords expected) {
        if (rowType == null) {
            throw new IllegalStateException(
                    "LogRecordsAssert#isEqualTo(LogRecords) must be invoked after #withSchema(RowType).");
        }
        Iterator<LogRecordBatch> actualIter = actual.batches().iterator();
        for (LogRecordBatch expectedNext : expected.batches()) {
            assertThat(actualIter.hasNext()).isTrue();
            assertThatLogRecordBatch(actualIter.next())
                    .withSchema(rowType)
                    .assertCheckSum(assertCheckSum)
                    .isEqualTo(expectedNext);
        }
        assertThat(actualIter.hasNext()).isFalse();
        assertThat(actual.sizeInBytes())
                .as("LogRecords#sizeInBytes()")
                .isEqualTo(expected.sizeInBytes());
        return this;
    }
}

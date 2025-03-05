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

import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import org.assertj.core.api.AbstractAssert;

import static com.alibaba.fluss.testutils.LogRecordAssert.assertThatLogRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Extend assertj assertions to easily assert {@link LogRecordBatch}. */
public class LogRecordBatchAssert extends AbstractAssert<LogRecordBatchAssert, LogRecordBatch> {

    private RowType rowType;
    private LogFormat logFormat;
    private boolean assertCheckSum = true;

    /** Creates assertions for {@link LogRecordBatch}. */
    public static LogRecordBatchAssert assertThatLogRecordBatch(LogRecordBatch actual) {
        return new LogRecordBatchAssert(actual);
    }

    private LogRecordBatchAssert(LogRecordBatch actual) {
        super(actual, LogRecordBatchAssert.class);
    }

    public LogRecordBatchAssert withSchema(RowType rowType) {
        this.rowType = rowType;
        return this;
    }

    public LogRecordBatchAssert assertCheckSum(boolean assertCheckSum) {
        this.assertCheckSum = assertCheckSum;
        return this;
    }

    public LogRecordBatchAssert withLogFormat(LogFormat logFormat) {
        this.logFormat = logFormat;
        return this;
    }

    public LogRecordBatchAssert isEqualTo(LogRecordBatch expected) {
        if (rowType == null) {
            throw new IllegalStateException(
                    "LogRecordBatchAssert#isEqualTo(LogRecordBatch) must be invoked after #withSchema(RowType).");
        }
        assertThat(actual.schemaId())
                .as("LogRecordBatch#schemaId()")
                .isEqualTo(expected.schemaId());
        assertThat(actual.baseLogOffset())
                .as("LogRecordBatch#baseLogOffset()")
                .isEqualTo(expected.baseLogOffset());
        assertThat(actual.lastLogOffset())
                .as("LogRecordBatch#lastLogOffset()")
                .isEqualTo(expected.lastLogOffset());
        assertThat(actual.nextLogOffset())
                .as("LogRecordBatch#nextLogOffset()")
                .isEqualTo(expected.nextLogOffset());
        assertThat(actual.magic()).as("LogRecordBatch#magic()").isEqualTo(expected.magic());
        assertThat(actual.writerId())
                .as("LogRecordBatch#writerId()")
                .isEqualTo(expected.writerId());
        assertThat(actual.batchSequence())
                .as("LogRecordBatch#batchSequence()")
                .isEqualTo(expected.batchSequence());
        assertThat(actual.getRecordCount())
                .as("LogRecordBatch#getRecordCount()")
                .isEqualTo(expected.getRecordCount());
        try (LogRecordReadContext readContext = createReadContext(expected.schemaId());
                CloseableIterator<LogRecord> actualIter = actual.records(readContext);
                CloseableIterator<LogRecord> expectIter = expected.records(readContext)) {
            while (expectIter.hasNext()) {
                assertThat(actualIter.hasNext()).isTrue();
                assertThatLogRecord(actualIter.next())
                        .withSchema(rowType)
                        .isEqualTo(expectIter.next());
            }
            assertThat(actualIter.hasNext()).isFalse();
        }
        // put less readable assertions last
        assertThat(actual.sizeInBytes())
                .as("LogRecordBatch#sizeInBytes()")
                .isEqualTo(expected.sizeInBytes());

        if (assertCheckSum) {
            assertThat(actual.checksum())
                    .as("LogRecordBatch#checksum()")
                    .isEqualTo(expected.checksum());
        }
        return this;
    }

    private LogRecordReadContext createReadContext(int schemaId) {
        if (logFormat != null && logFormat == LogFormat.INDEXED) {
            return LogRecordReadContext.createIndexedReadContext(rowType, schemaId);
        } else {
            return LogRecordReadContext.createArrowReadContext(rowType, schemaId);
        }
    }
}

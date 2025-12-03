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

package org.apache.fluss.record;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.memory.MemorySegmentOutputView;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.row.TestInternalRowGenerator;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.testutils.ListLogRecords;
import org.apache.fluss.testutils.LogRecordsAssert;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** base test class for Log. */
public abstract class LogTestBase {

    protected final short schemaId = TestData.DEFAULT_SCHEMA_ID;
    protected final byte magic = TestData.DEFAULT_MAGIC;
    protected final long baseLogOffset = TestData.BASE_OFFSET;
    protected final int position = 0;
    protected final RowType baseRowType = TestData.DATA1_ROW_TYPE;
    protected final MemorySegmentOutputView outputView = new MemorySegmentOutputView(100);

    protected Configuration conf;

    @BeforeEach
    protected void before() throws IOException {
        conf = new Configuration();
    }

    protected List<IndexedRow> createAllTypeRowDataList() {
        List<IndexedRow> allTypeRows = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            IndexedRow row = TestInternalRowGenerator.genIndexedRowForAllType();
            allTypeRows.add(row);
        }
        return allTypeRows;
    }

    public static void assertLogRecordsListEquals(
            List<MemoryLogRecords> expected, LogRecords actual, RowType rowType) {
        LogRecordsAssert.assertThatLogRecords(actual)
                .withSchema(rowType)
                .isEqualTo(new ListLogRecords(expected));
    }

    protected void assertLogRecordsListEquals(List<MemoryLogRecords> expected, LogRecords actual) {
        assertLogRecordsListEquals(expected, actual, baseRowType);
    }

    protected void assertIndexedLogRecordBatchAndRowEquals(
            LogRecordBatch actual,
            LogRecordBatch expected,
            RowType rowType,
            List<IndexedRow> rows,
            SchemaGetter schemaGetter) {
        assertRecordBatchHeaderEquals(actual);

        LogRecordReadContext readContext =
                LogRecordReadContext.createIndexedReadContext(
                        rowType, TestData.DEFAULT_SCHEMA_ID, schemaGetter);
        try (CloseableIterator<LogRecord> actualIter = actual.records(readContext);
                CloseableIterator<LogRecord> expectIter = expected.records(readContext)) {
            int i = 0;
            while (actualIter.hasNext() && expectIter.hasNext()) {
                IndexedLogRecord actualRecord = (IndexedLogRecord) actualIter.next();
                IndexedLogRecord expectedRecord = (IndexedLogRecord) expectIter.next();
                assertIndexedRecordEquals(actualRecord, expectedRecord, rows.get(i), i);
                i++;
            }
        }
    }

    private void assertIndexedRecordEquals(
            LogRecord actualRecord, LogRecord expectedRecord, IndexedRow row, int offsetDelta) {
        assertThat(actualRecord.getChangeType())
                .isEqualTo(expectedRecord.getChangeType())
                .isEqualTo(ChangeType.APPEND_ONLY);
        assertThat(actualRecord.logOffset())
                .isEqualTo(expectedRecord.logOffset())
                .isEqualTo(baseLogOffset + offsetDelta);
        assertThat(actualRecord.getRow()).isEqualTo(expectedRecord.getRow()).isEqualTo(row);
    }

    private void assertRecordBatchHeaderEquals(LogRecordBatch recordBatch) {
        assertThat(recordBatch.baseLogOffset()).isEqualTo(baseLogOffset);
        assertThat(recordBatch.magic()).isEqualTo(magic);
        assertThat(recordBatch.isValid()).isTrue();
        assertThat(recordBatch.schemaId()).isEqualTo(schemaId);
    }
}

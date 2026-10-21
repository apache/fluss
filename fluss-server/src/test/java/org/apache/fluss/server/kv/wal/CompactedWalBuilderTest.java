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

package org.apache.fluss.server.kv.wal;

import org.apache.fluss.compression.LogCompressionInfo;
import org.apache.fluss.compression.LogCompressionType;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.DefaultLogRecordBatch;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactedWalBuilder}. */
class CompactedWalBuilderTest {

    static Collection<LogCompressionInfo> compressions() {
        return Arrays.asList(
                LogCompressionInfo.NO_COMPRESSION,
                new LogCompressionInfo(LogCompressionType.LZ4_FRAME, -1),
                new LogCompressionInfo(LogCompressionType.ZSTD, 3));
    }

    @ParameterizedTest
    @MethodSource("compressions")
    void testBuildChangelog(LogCompressionInfo compression) throws Exception {
        TestingMemorySegmentPool pool = new TestingMemorySegmentPool(256);
        CompactedWalBuilder walBuilder =
                new CompactedWalBuilder(DEFAULT_SCHEMA_ID, DATA1_ROW_TYPE, pool, compression);

        ChangeType[] changeTypes = {
            ChangeType.INSERT, ChangeType.UPDATE_BEFORE, ChangeType.UPDATE_AFTER, ChangeType.DELETE
        };
        List<ChangeType> expectedChangeTypes = new ArrayList<>();
        List<Object[]> expectedValues = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            ChangeType changeType = changeTypes[i % changeTypes.length];
            Object[] values = {i, "value-" + (i % 5)};
            walBuilder.append(changeType, row(values));
            expectedChangeTypes.add(changeType);
            expectedValues.add(values);
        }
        walBuilder.setWriterState(5L, 9);
        MemoryLogRecords logRecords = walBuilder.build();
        // the built records must stay readable after the pooled pages are returned
        walBuilder.deallocate();

        Iterator<LogRecordBatch> batches = logRecords.batches().iterator();
        DefaultLogRecordBatch batch = (DefaultLogRecordBatch) batches.next();
        assertThat(batches.hasNext()).isFalse();
        batch.ensureValid();
        assertThat(batch.compressionType()).isEqualTo(compression.getCompressionType());
        assertThat(batch.magic()).isEqualTo(CURRENT_LOG_MAGIC_VALUE);
        assertThat(batch.writerId()).isEqualTo(5L);
        assertThat(batch.batchSequence()).isEqualTo(9);
        assertThat(batch.getRecordCount()).isEqualTo(expectedValues.size());

        try (LogRecordReadContext readContext =
                        LogRecordReadContext.createCompactedRowReadContext(
                                DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER);
                CloseableIterator<LogRecord> records = batch.records(readContext)) {
            for (int i = 0; i < expectedValues.size(); i++) {
                assertThat(records.hasNext()).isTrue();
                LogRecord record = records.next();
                InternalRow row = record.getRow();
                assertThat(record.getChangeType()).isEqualTo(expectedChangeTypes.get(i));
                assertThat(row.getInt(0)).isEqualTo(expectedValues.get(i)[0]);
                assertThat(row.getString(1).toString()).isEqualTo(expectedValues.get(i)[1]);
            }
            assertThat(records.hasNext()).isFalse();
        }
    }
}

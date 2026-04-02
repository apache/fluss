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

import org.apache.fluss.testutils.DataTestUtils;

import org.junit.jupiter.api.Test;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_ROW_TYPE;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.record.TestData.TEST_SCHEMA_GETTER;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link LogRecordBatchIterator}. */
public class LogRecordBatchIteratorTest {

    @Test
    void testBasicLogRecordBatchIterator() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatchIterator<LogRecordBatch> iterator =
                new LogRecordBatchIterator<>(
                        new MemorySegmentLogInputStream(
                                memoryLogRecords.getMemorySegment(),
                                memoryLogRecords.getPosition(),
                                memoryLogRecords.sizeInBytes()));

        assertThat(iterator.hasNext()).isTrue();
        LogRecordBatch batch = iterator.next();
        assertThat(batch).isNotNull();
        assertThat(batch.getRecordCount()).isEqualTo(DATA1.size());
        assertThat(batch.baseLogOffset()).isEqualTo(0L);

        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            assertThat(batch.getStatistics(readContext)).isPresent();
        }
        assertThat(iterator.hasNext()).isFalse();
    }

    @Test
    void testGetStatisticsWithNullReadContext() throws Exception {
        MemoryLogRecords memoryLogRecords =
                LogRecordBatchStatisticsTestUtils.createLogRecordsWithStatistics(
                        DATA1, DATA1_ROW_TYPE, 0L, DEFAULT_SCHEMA_ID);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        assertThat(batch.getStatistics(null)).isEmpty();
    }

    @Test
    void testGetStatisticsWithV0Magic() throws Exception {
        MemoryLogRecords memoryLogRecords =
                DataTestUtils.genMemoryLogRecordsByObject((byte) 0, DATA1);

        LogRecordBatch batch = memoryLogRecords.batches().iterator().next();
        try (LogRecordReadContext readContext =
                LogRecordReadContext.createArrowReadContext(
                        DATA1_ROW_TYPE, DEFAULT_SCHEMA_ID, TEST_SCHEMA_GETTER)) {
            assertThat(batch.getStatistics(readContext)).isEmpty();
        }
    }
}

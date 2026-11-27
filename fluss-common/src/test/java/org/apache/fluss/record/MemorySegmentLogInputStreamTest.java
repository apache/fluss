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

import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.testutils.DataTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.TestData.DATA1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link MemorySegmentLogInputStream}. */
public class MemorySegmentLogInputStreamTest {

    @Test
    void testNextBatch() throws Exception {
        MemoryLogRecords records = DataTestUtils.genMemoryLogRecordsByObject(DATA1);
        assertThat(records.batches().iterator().hasNext()).isTrue();
        assertThat(MemoryLogRecords.EMPTY.batches().iterator().hasNext()).isFalse();

        for (int size : new int[] {LOG_OVERHEAD - 1, LOG_OVERHEAD, records.sizeInBytes() - 1}) {
            MemoryLogRecords partial = MemoryLogRecords.readableRecords(records, size);
            assertThat(partial.batches().iterator().hasNext()).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void testInvalidBatchLength(int length) {
        byte[] prefix = new byte[LOG_OVERHEAD];
        MemorySegment.wrap(prefix).putInt(LENGTH_OFFSET, length);
        MemoryLogRecords records = MemoryLogRecords.pointToBytes(prefix);
        assertThatThrownBy(() -> records.batches().iterator().hasNext())
                .isInstanceOf(CorruptMessageException.class);
    }
}

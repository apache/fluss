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

import java.util.Iterator;

import static org.apache.fluss.record.DefaultLogRecordBatch.LOG_OVERHEAD;
import static org.apache.fluss.record.TestData.DATA1;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MemorySegmentLogInputStream}. */
public class MemorySegmentLogInputStreamTest {

    @Test
    void testNextBatch() throws Exception {
        // gen normal batch.
        MemoryLogRecords memoryLogRecords = DataTestUtils.genMemoryLogRecordsByObject(DATA1);
        Iterator<LogRecordBatch> iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isTrue();

        // gen empty batch.
        memoryLogRecords = MemoryLogRecords.EMPTY;
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isFalse();

        // gen batch with invalid header size.
        memoryLogRecords = MemoryLogRecords.pointToBytes(new byte[LOG_OVERHEAD - 1]);
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isFalse();

        // gen batch with invalid header size.
        memoryLogRecords = MemoryLogRecords.pointToBytes(new byte[11]);
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isFalse();

        // gen batch with enough size.
        memoryLogRecords = MemoryLogRecords.pointToBytes(new byte[LOG_OVERHEAD]);
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isTrue();

        // gen batch with enough size.
        memoryLogRecords = MemoryLogRecords.pointToBytes(new byte[12]);
        iterator = getIterator(memoryLogRecords);
        assertThat(iterator.hasNext()).isTrue();
    }

    private Iterator<LogRecordBatch> getIterator(MemoryLogRecords memoryLogRecords) {
        return memoryLogRecords.batches().iterator();
    }
}

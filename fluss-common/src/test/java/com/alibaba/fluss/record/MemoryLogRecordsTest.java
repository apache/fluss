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

package com.alibaba.fluss.record;

import com.alibaba.fluss.testutils.DataTestUtils;
import com.alibaba.fluss.testutils.LogRecordsAssert;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static com.alibaba.fluss.record.TestData.DATA1;
import static com.alibaba.fluss.record.TestData.DATA1_ROW_TYPE;

/** Tests for {@link MemoryLogRecords}. */
class MemoryLogRecordsTest {

    @Test
    void testPointToDirectByteBuffer() throws Exception {
        verifyPointToByteBuffer(ByteBuffer::allocateDirect);
    }

    @Test
    void testPointToDirectByteBufferWithShift() throws Exception {
        verifyPointToByteBuffer(
                size -> {
                    ByteBuffer buffer = ByteBuffer.allocateDirect(size + 3);
                    buffer.put(new byte[] {1, 2, 3});
                    buffer.position(3);
                    return buffer;
                });
    }

    @Test
    void testPointToHeapByteBuffer() throws Exception {
        verifyPointToByteBuffer(ByteBuffer::allocate);
    }

    @Test
    void testPointToHeapByteBufferWithShift() throws Exception {
        verifyPointToByteBuffer(
                size -> {
                    ByteBuffer buffer = ByteBuffer.allocate(size + 3);
                    buffer.put(new byte[] {1, 2, 3});
                    buffer.position(3);
                    return buffer;
                });
    }

    void verifyPointToByteBuffer(Function<Integer, ByteBuffer> bufferSupplier) throws Exception {
        int writerId = 1000;
        int seqno = 6;
        long baseOffset = 123L;
        MemoryLogRecords records =
                DataTestUtils.genMemoryLogRecordsWithWriterId(DATA1, writerId, seqno, baseOffset);
        ByteBuffer buffer = bufferSupplier.apply(records.sizeInBytes());

        int originPos = buffer.position();
        records.getMemorySegment().get(records.getPosition(), buffer, records.sizeInBytes());
        buffer.position(originPos);

        MemoryLogRecords newRecords = MemoryLogRecords.pointToByteBuffer(buffer);
        LogRecordsAssert.assertThatLogRecords(newRecords)
                .hasBatchesCount(1)
                .withSchema(DATA1_ROW_TYPE)
                .isEqualTo(records);
    }
}

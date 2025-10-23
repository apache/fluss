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

import org.apache.fluss.memory.MemorySegment;

import static org.apache.fluss.record.LogRecordBatchFormat.LENGTH_OFFSET;
import static org.apache.fluss.record.LogRecordBatchFormat.LOG_OVERHEAD;
import static org.apache.fluss.record.LogRecordBatchFormat.V0_RECORD_BATCH_HEADER_SIZE;

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
class MemorySegmentLogInputStream implements LogInputStream<LogRecordBatch> {
    private final MemorySegment memorySegment;

    private int currentPosition;
    private int remaining;

    MemorySegmentLogInputStream(MemorySegment memorySegment, int basePosition, int sizeInBytes) {
        this.memorySegment = memorySegment;
        this.currentPosition = basePosition;
        this.remaining = sizeInBytes;
    }

    public LogRecordBatch nextBatch() {
        Integer batchSize = nextBatchSize();
        // should at-least larger than V0 header size, because V1 header is larger than V0.
        if (batchSize == null || remaining < batchSize || remaining < V0_RECORD_BATCH_HEADER_SIZE) {
            return null;
        }

        DefaultLogRecordBatch logRecords = new DefaultLogRecordBatch();
        logRecords.pointTo(memorySegment, currentPosition);

        currentPosition += batchSize;
        remaining -= batchSize;
        return logRecords;
    }

    /** Validates the header of the next batch and returns batch size. */
    private Integer nextBatchSize() {
        if (remaining < LOG_OVERHEAD) {
            return null;
        }

        int recordSize = memorySegment.getInt(currentPosition + LENGTH_OFFSET);
        return recordSize + LOG_OVERHEAD;
    }
}

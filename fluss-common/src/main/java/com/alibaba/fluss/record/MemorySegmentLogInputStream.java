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

package com.alibaba.fluss.record;

import com.alibaba.fluss.memory.MemorySegment;

import static com.alibaba.fluss.record.DefaultLogRecordBatch.LENGTH_OFFSET;
import static com.alibaba.fluss.record.DefaultLogRecordBatch.LOG_OVERHEAD;

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
        if (batchSize == null || remaining < batchSize) {
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

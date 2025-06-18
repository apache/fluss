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

package com.alibaba.fluss.server.log;

import com.alibaba.fluss.exception.OutOfOrderSequenceException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.LogRecordBatch;

/**
 * This class is used to validate the records appended by a given writer before they are written to
 * log. It's initialized with writer's state after the last successful append.
 */
public class WriterAppendInfo {
    private final long writerId;
    private final TableBucket tableBucket;
    private final WriterStateEntry currentEntry;
    private final WriterStateEntry updatedEntry;

    public WriterAppendInfo(long writerId, TableBucket tableBucket, WriterStateEntry currentEntry) {
        this.writerId = writerId;
        this.tableBucket = tableBucket;
        this.currentEntry = currentEntry;
        this.updatedEntry = currentEntry.withWriterIdAndBatchMetadata(writerId, null);
    }

    public long writerId() {
        return writerId;
    }

    public void append(LogRecordBatch batch) {
        LogOffsetMetadata firstOffsetMetadata = new LogOffsetMetadata(batch.baseLogOffset());
        appendDataBatch(
                batch.batchSequence(),
                firstOffsetMetadata,
                batch.lastLogOffset(),
                System.currentTimeMillis()); // TODO, add timestamp to record batch.
    }

    public void appendDataBatch(
            int batchSequence,
            LogOffsetMetadata firstOffsetMetadata,
            long lastOffset,
            long lastTimestamp) {
        maybeValidateDataBatch(batchSequence, lastOffset);
        updatedEntry.addBath(
                batchSequence,
                lastOffset,
                (int) (lastOffset - firstOffsetMetadata.getMessageOffset()),
                lastTimestamp);
    }

    private void maybeValidateDataBatch(int appendFirstSeq, long lastOffset) {
        int currentLastSeq =
                !updatedEntry.isEmpty()
                        ? updatedEntry.lastBatchSequence()
                        : currentEntry.lastBatchSequence();
        // must be in sequence, even for the first batch should start from 0
        if (!inSequence(currentLastSeq, appendFirstSeq)) {
            throw new OutOfOrderSequenceException(
                    String.format(
                            "Out of order batch sequence for writer %s at offset %s in "
                                    + "table-bucket %s : %s (incoming batch seq.), %s (current batch seq.)",
                            writerId, lastOffset, tableBucket, appendFirstSeq, currentLastSeq));
        }
    }

    public WriterStateEntry toEntry() {
        return updatedEntry;
    }

    private boolean inSequence(int lastBatchSeq, int nextBatchSeq) {
        return nextBatchSeq == lastBatchSeq + 1L
                || (nextBatchSeq == 0 && lastBatchSeq == Integer.MAX_VALUE);
    }
}
